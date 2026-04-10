//go:build integration

package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestForceRebalance verifies that the force_rebalance option distributes parts
// across JBOD disks even when the backup's disk name ("default") exists on the
// target. This covers the scenario where a backup taken on a single-disk machine
// is restored to a multi-disk JBOD target under the same storage policy name.
func TestForceRebalance(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.1") < 0 {
		t.Skipf("Test requires ClickHouse >= 20.1 for storage policies, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}

	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	backupName := fmt.Sprintf("test_force_rebalance_%d", rand.Int())
	dbName := "test_force_rebalance_" + t.Name()
	tableName := "data"

	// Step 1: Create a table on the default disk (no explicit storage_policy)
	// and insert enough data to produce multiple parts
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, dt DateTime) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id",
		dbName, tableName,
	))
	for _, month := range []string{"2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"} {
		env.queryWithNoError(r, fmt.Sprintf(
			"INSERT INTO %s.%s SELECT number, toDateTime('%s 00:00:00') + number FROM numbers(1000)",
			dbName, tableName, month,
		))
	}

	// Verify all data is on the default disk
	env.checkCount(r, 1, 4000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))

	// Step 2: Create and upload the backup — all parts will be under disk "default"
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml",
		"create", "--tables="+dbName+".*", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml",
		"upload", backupName)

	// Step 3: Delete local backup and drop the table
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml",
		"delete", "local", backupName)
	// SYNC keyword not supported before 21.x
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", dbName, tableName)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		dropSQL += " SYNC"
	}
	env.queryWithNoError(r, dropSQL)

	// Step 4: Reconfigure ClickHouse so the "default" storage policy includes
	// hdd1 and hdd2 as JBOD disks, simulating a multi-disk target
	env.DockerExecNoError(r, "clickhouse", "bash", "-xc", `
cat > /etc/clickhouse-server/config.d/force_rebalance_test.xml <<'XML'
<yandex>
  <storage_configuration>
    <policies>
      <default>
        <volumes>
          <default>
            <disk>hdd1</disk>
            <disk>hdd2</disk>
          </default>
        </volumes>
      </default>
    </policies>
  </storage_configuration>
</yandex>
XML
`)
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t.Context(), "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)

	// Step 5: Download with force_rebalance — parts should be distributed across hdd1 and hdd2
	downloadOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"CLICKHOUSE_FORCE_REBALANCE=true clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download "+backupName)
	log.Debug().Msg(downloadOut)
	r.NoError(err, downloadOut)
	// The rebalancing warning proves the code entered the rebalancing path
	r.Contains(downloadOut, "require disk 'default' that not found in system.disks")

	// Step 6: Verify backup data landed on both hdd1 and hdd2
	hdd1Out, err := env.DockerExecOut("clickhouse-backup", "find", "/hdd1_data/backup/"+backupName+"/shadow", "-type", "f")
	r.NoError(err)
	hdd2Out, err := env.DockerExecOut("clickhouse-backup", "find", "/hdd2_data/backup/"+backupName+"/shadow", "-type", "f")
	r.NoError(err)
	log.Debug().Msgf("hdd1 files:\n%s", hdd1Out)
	log.Debug().Msgf("hdd2 files:\n%s", hdd2Out)
	r.NotEmpty(hdd1Out, "hdd1 should contain some backup data")
	r.NotEmpty(hdd2Out, "hdd2 should contain some backup data")

	// Step 7: Restore and verify data integrity
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, dt DateTime) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id",
		dbName, tableName,
	))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce",
		"CLICKHOUSE_FORCE_REBALANCE=true clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore --data "+backupName)
	env.checkCount(r, 1, 4000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))

	// Step 8: Cleanup — remove the test storage policy override and restart
	env.DockerExecNoError(r, "clickhouse", "rm", "-f", "/etc/clickhouse-server/config.d/force_rebalance_test.xml")
	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{"test_force_rebalance"}, true, true, true, "config-s3.yml")
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t.Context(), "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
	env.Cleanup(t, r)
}
