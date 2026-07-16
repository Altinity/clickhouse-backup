//go:build integration

package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestRebalance verifies the `rebalance` command, https://github.com/Altinity/clickhouse-backup/issues/1024.
// Tables use the always-provisioned `hot_and_cold` policy (hot=default, cold=hdd1+hdd2, move_factor=0),
// so inserts deterministically land on the default disk. After `create`, live partitions are moved to
// hdd1/hdd2 and `rebalance` must hardlink them into the backup shadow on those disks (Rule 2),
// --dry-run and --tables are checked on the way, Rule 3 (invalid backup disk) is covered by unit tests
// in pkg/backup/rebalance_test.go.
func TestRebalance(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.1") < 0 {
		t.Skipf("Test requires ClickHouse >= 20.1 for storage policies and MOVE PARTITION, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	backupName := fmt.Sprintf("test_rebalance_%d", rand.Int())
	dbName := "test_rebalance_" + t.Name()
	backupCmd := "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml"

	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		dropSuffix = " SYNC"
	}

	// count part directories matching pattern inside the backup shadow of one disk
	countShadowDirs := func(diskPath, table, pattern string) int {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-c",
			fmt.Sprintf("find %s/backup/%s/shadow/%s/%s -type d -name '%s' 2>/dev/null | wc -l", diskPath, backupName, dbName, table, pattern))
		r.NoError(err, out)
		n, convErr := strconv.Atoi(strings.TrimSpace(out))
		r.NoError(convErr, out)
		return n
	}

	// Step 1: two tables on the hot_and_cold policy, merges stopped so part names stay stable,
	// all inserts land on the `default` disk (hot volume, move_factor=0)
	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	for _, table := range []string{"data1", "data2"} {
		env.queryWithNoError(t, r, fmt.Sprintf(
			"CREATE TABLE %s.%s (id UInt64, dt DateTime) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id SETTINGS storage_policy='hot_and_cold'",
			dbName, table,
		))
		env.queryWithNoError(t, r, fmt.Sprintf("SYSTEM STOP MERGES %s.%s", dbName, table))
		for _, month := range []string{"2024-01-01", "2024-02-01", "2024-03-01"} {
			env.queryWithNoError(t, r, fmt.Sprintf(
				"INSERT INTO %s.%s SELECT number, toDateTime('%s 00:00:00') + number FROM numbers(1000)",
				dbName, table, month,
			))
		}
	}
	env.checkCount(r, 1, 0, fmt.Sprintf(
		"SELECT count() FROM system.parts WHERE database='%s' AND table IN ('data1','data2') AND active AND disk_name!='default'", dbName,
	))

	// Step 2: create the backup, all parts recorded under disk `default`
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" create --tables="+dbName+".* "+backupName)

	// Step 3: move live partitions to hdd1/hdd2, the backup layout is stale now
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.data1 MOVE PARTITION ID '202402' TO DISK 'hdd1'", dbName))
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.data1 MOVE PARTITION ID '202403' TO DISK 'hdd2'", dbName))
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.data2 MOVE PARTITION ID '202402' TO DISK 'hdd1'", dbName))

	// Step 4: --dry-run logs the plan and changes nothing
	dryRunOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", backupCmd+" rebalance --dry-run "+backupName)
	log.Debug().Msg(dryRunOut)
	r.NoError(err, dryRunOut)
	r.Contains(dryRunOut, "would move part")
	r.Equal(0, countShadowDirs("/hdd1_data", "data1", "202402*"), "dry-run must not move parts")
	tableJSON, err := env.DockerExecOut("clickhouse-backup", "cat", fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata/%s/data1.json", backupName, dbName))
	r.NoError(err, tableJSON)
	r.NotContains(tableJSON, "hdd1", "dry-run must not rewrite table metadata")

	// Step 5: --tables filter moves only data1
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" rebalance --tables="+dbName+".data1 "+backupName)
	r.Greater(countShadowDirs("/hdd1_data", "data1", "202402*"), 0)
	r.Greater(countShadowDirs("/hdd2_data", "data1", "202403*"), 0)
	r.Equal(0, countShadowDirs("/hdd1_data", "data2", "202402*"))

	// Step 6: full rebalance aligns data2 too, Rule 2 hardlinks from the live parts
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" rebalance "+backupName)
	r.Greater(countShadowDirs("/hdd1_data", "data2", "202402*"), 0)
	r.Equal(0, countShadowDirs("/var/lib/clickhouse", "data1", "202402*"), "moved parts must be removed from the source shadow")
	r.Equal(0, countShadowDirs("/var/lib/clickhouse", "data1", "202403*"), "moved parts must be removed from the source shadow")
	r.Equal(0, countShadowDirs("/var/lib/clickhouse", "data2", "202402*"), "moved parts must be removed from the source shadow")
	r.Greater(countShadowDirs("/var/lib/clickhouse", "data1", "202401*"), 0, "unmoved parts must stay on the source disk")
	r.Greater(countShadowDirs("/var/lib/clickhouse", "data2", "202401*"), 0, "unmoved parts must stay on the source disk")
	linkCountOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		fmt.Sprintf("stat -c %%h $(find /hdd1_data/backup/%s/shadow/%s/data1 -name checksums.txt | head -1)", backupName, dbName))
	r.NoError(err, linkCountOut)
	linkCount, err := strconv.Atoi(strings.TrimSpace(linkCountOut))
	r.NoError(err, linkCountOut)
	r.GreaterOrEqual(linkCount, 2, "Rule 2 must hardlink from the live part, not copy")
	tableJSON, err = env.DockerExecOut("clickhouse-backup", "cat", fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata/%s/data1.json", backupName, dbName))
	r.NoError(err, tableJSON)
	r.Contains(tableJSON, "hdd1")
	r.Contains(tableJSON, "hdd2")

	// Step 7: the rebalanced backup restores correctly
	for _, table := range []string{"data1", "data2"} {
		env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE %s.%s%s", dbName, table, dropSuffix))
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" restore --tables="+dbName+".* "+backupName)
	for _, table := range []string{"data1", "data2"} {
		env.checkCount(r, 1, 3000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, table))
	}

	fullCleanup(t, r, env, []string{backupName}, []string{"local"}, []string{"test_rebalance"}, true, true, true, "config-s3.yml")
}
