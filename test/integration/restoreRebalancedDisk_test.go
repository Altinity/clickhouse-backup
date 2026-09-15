//go:build integration

package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestRestoreStaleDetachedPart covers item 2/3 of https://github.com/Altinity/clickhouse-backup/issues/1034:
// a same-named directory left in `detached/` (possibly on another disk of the storage policy) used to be
// silently merged into, and ClickHouse `ATTACH PART` then picked the stale copy on the first policy disk.
// Restore now removes `detached/<part>` from every data path of the table, hardlinks into a temporary
// directory and renames it into place.
func TestRestoreStaleDetachedPart(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.1") < 0 {
		t.Skipf("Test requires ClickHouse >= 20.1 for storage policies and MOVE PARTITION, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	backupName := fmt.Sprintf("test_stale_detached_%d", rand.Int())
	dbName := "test_stale_detached_" + t.Name()
	tableName := "data"
	backupCmd := "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml"

	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		dropSuffix = " SYNC"
	}

	// Step 1: two partitions on the hot_and_cold policy (move_factor=0, so inserts land on `default`)
	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, dt DateTime) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id SETTINGS storage_policy='hot_and_cold'",
		dbName, tableName,
	))
	env.queryWithNoError(t, r, fmt.Sprintf("SYSTEM STOP MERGES %s.%s", dbName, tableName))
	for _, month := range []string{"2024-01-01", "2024-02-01"} {
		env.queryWithNoError(t, r, fmt.Sprintf(
			"INSERT INTO %s.%s SELECT number, toDateTime('%s 00:00:00') + number FROM numbers(1000)",
			dbName, tableName, month,
		))
	}

	// Step 2: move the second partition to hdd1, so the restored part must land on a non-default disk
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.%s MOVE PARTITION ID '202402' TO DISK 'hdd1'", dbName, tableName))

	var movedPart string
	r.NoError(env.ch.SelectSingleRowNoCtx(&movedPart,
		"SELECT name FROM system.parts WHERE database=? AND `table`=? AND partition_id='202402' AND active LIMIT 1", dbName, tableName))
	r.NotEmpty(movedPart)

	var dataPathsRaw string
	r.NoError(env.ch.SelectSingleRowNoCtx(&dataPathsRaw,
		"SELECT arrayStringConcat(data_paths, ' ') FROM system.tables WHERE database=? AND name=?", dbName, tableName))
	dataPaths := strings.Fields(dataPathsRaw)
	r.GreaterOrEqual(len(dataPaths), 2, "hot_and_cold table must have a data path per policy disk, got %s", dataPathsRaw)
	hdd1Path := ""
	for i := range dataPaths {
		dataPaths[i] = strings.TrimRight(dataPaths[i], "/")
		if strings.HasPrefix(dataPaths[i], "/hdd1_data/") {
			hdd1Path = dataPaths[i]
		}
	}
	r.NotEmpty(hdd1Path, "no hdd1 data path in %s", dataPathsRaw)

	// Step 3: backup, then drop both partitions so `restore --data` re-attaches everything
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" create --tables="+dbName+".* "+backupName)
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.%s DROP PARTITION ID '202401'", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.%s DROP PARTITION ID '202402'", dbName, tableName))

	// Step 4: plant a stale `detached/<part>` on every disk of the policy plus a leftover temporary
	// directory of an interrupted restore on the disk where the part is going to land
	for _, dataPath := range dataPaths {
		env.DockerExecNoError(r, "clickhouse", "bash", "-ce", fmt.Sprintf(
			"mkdir -p %s/detached/%s && echo junk > %s/detached/%s/stale_junk.bin && echo stale > %s/detached/%s/count.txt && chown -R clickhouse:clickhouse %s/detached",
			dataPath, movedPart, dataPath, movedPart, dataPath, movedPart, dataPath,
		))
	}
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", fmt.Sprintf(
		"mkdir -p %s/detached/clickhouse_backup_tmp_%s && echo leftover > %s/detached/clickhouse_backup_tmp_%s/leftover.bin && chown -R clickhouse:clickhouse %s/detached",
		hdd1Path, movedPart, hdd1Path, movedPart, hdd1Path,
	))

	// Step 5: restore, before the fix ATTACH PART picked the stale directory on `default` and failed with code 226
	restoreOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", backupCmd+" restore --data --tables="+dbName+"."+tableName+" "+backupName)
	log.Debug().Msg(restoreOut)
	r.NoError(err, restoreOut)
	r.Contains(restoreOut, "remove stale detached part")

	// Step 6: all rows are back, the rebalanced partition sits on hdd1, nothing is left in any `detached/`
	env.checkCount(r, 1, 2000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))
	env.checkCount(r, 1, 1, fmt.Sprintf(
		"SELECT count() FROM system.parts WHERE database='%s' AND `table`='%s' AND partition_id='202402' AND active AND disk_name='hdd1' SETTINGS empty_result_for_aggregation_by_empty_set=0",
		dbName, tableName,
	))
	env.checkCount(r, 1, 0, fmt.Sprintf(
		"SELECT count() FROM system.detached_parts WHERE database='%s' AND `table`='%s' SETTINGS empty_result_for_aggregation_by_empty_set=0",
		dbName, tableName,
	))
	detachedDirs := make([]string, 0, len(dataPaths))
	for _, dataPath := range dataPaths {
		detachedDirs = append(detachedDirs, dataPath+"/detached")
	}
	leftOvers, err := env.DockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf(
		"find %s -mindepth 1 2>/dev/null | wc -l", strings.Join(detachedDirs, " "),
	))
	r.NoError(err, leftOvers)
	r.Equal("0", strings.TrimSpace(leftOvers), "detached dirs must be empty, found:\n%s", leftOvers)

	// Step 7: the same temporary directory + rename logic on the `restore_as_attach` path (toDetached=false)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") >= 0 {
		env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE %s.%s%s", dbName, tableName, dropSuffix))
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce",
			"CLICKHOUSE_RESTORE_AS_ATTACH=true "+backupCmd+" restore --tables="+dbName+"."+tableName+" "+backupName)
		env.checkCount(r, 1, 2000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))
	}

	fullCleanup(t, r, env, []string{backupName}, []string{"local"}, []string{"test_stale_detached"}, true, true, true, "config-s3.yml")
}

// TestRestoreDiskNotInStoragePolicyHint covers item 4 of https://github.com/Altinity/clickhouse-backup/issues/1034:
// restoring into an existing table whose storage policy does not contain the backup's disk used to fail with a
// raw Go map dump, now the error names the table, the part, the disk and points at system.storage_policies.
func TestRestoreDiskNotInStoragePolicyHint(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.1") < 0 {
		t.Skipf("Test requires ClickHouse >= 20.1 for storage policies and MOVE PARTITION, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	backupName := fmt.Sprintf("test_disk_hint_%d", rand.Int())
	dbName := "test_disk_hint_" + t.Name()
	tableName := "data"
	backupCmd := "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml"

	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		dropSuffix = " SYNC"
	}

	// Step 1: all data on hdd2, so the backup references exactly one disk
	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, dt DateTime) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id SETTINGS storage_policy='hot_and_cold'",
		dbName, tableName,
	))
	env.queryWithNoError(t, r, fmt.Sprintf("SYSTEM STOP MERGES %s.%s", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf(
		"INSERT INTO %s.%s SELECT number, toDateTime('2024-01-01 00:00:00') + number FROM numbers(1000)", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.%s MOVE PARTITION ID '202401' TO DISK 'hdd2'", dbName, tableName))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" create --tables="+dbName+".* "+backupName)

	// Step 2: recreate the destination table with a policy which has no hdd2, the disk itself still exists
	// on the server, so `download`/rebalance never kicks in and the restore reaches the hardlink step
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE %s.%s%s", dbName, tableName, dropSuffix))
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, dt DateTime) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id SETTINGS storage_policy='hdd1_only'",
		dbName, tableName,
	))

	// Step 3: the restore must fail with an actionable message
	restoreOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", backupCmd+" restore --data --tables="+dbName+"."+tableName+" "+backupName)
	log.Debug().Msg(restoreOut)
	r.Error(err, restoreOut)
	r.Contains(restoreOut, "system.storage_policies", restoreOut)
	r.Contains(restoreOut, "hdd2", restoreOut)
	r.NotContains(restoreOut, "dstDataPaths=map", restoreOut)

	fullCleanup(t, r, env, []string{backupName}, []string{"local"}, []string{"test_disk_hint"}, true, true, true, "config-s3.yml")
}

// TestDownloadIncrementRebalanceRequiredParts covers the download side of
// https://github.com/Altinity/clickhouse-backup/issues/1034: rebalancing an incremental backup aborted when the
// rebalanced disk carried only `required` parts, because such parts are uploaded with the base backup and
// therefore have no `files` entry in the increment. `force_rebalance` enters the very same branch as a
// missing disk, so the guard is exercised without removing a disk from the pooled test environment.
func TestDownloadIncrementRebalanceRequiredParts(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.1") < 0 {
		t.Skipf("Test requires ClickHouse >= 20.1 for storage policies and MOVE PARTITION, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	baseBackup := fmt.Sprintf("test_inc_rebalance_base_%d", rand.Int())
	incBackup := fmt.Sprintf("test_inc_rebalance_inc_%d", rand.Int())
	dbName := "test_inc_rebalance_" + t.Name()
	tableName := "data"
	backupCmd := "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml"

	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		dropSuffix = " SYNC"
	}

	// Step 1: the base backup holds one partition on the `default` disk
	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, dt DateTime) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id SETTINGS storage_policy='hot_and_cold'",
		dbName, tableName,
	))
	env.queryWithNoError(t, r, fmt.Sprintf("SYSTEM STOP MERGES %s.%s", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf(
		"INSERT INTO %s.%s SELECT number, toDateTime('2024-01-01 00:00:00') + number FROM numbers(1000)", dbName, tableName))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" create_remote --tables="+dbName+".* "+baseBackup)

	// Step 2: the increment adds a partition on hdd1, the `default` disk contributes only the required part
	env.queryWithNoError(t, r, fmt.Sprintf(
		"INSERT INTO %s.%s SELECT number, toDateTime('2024-02-01 00:00:00') + number FROM numbers(1000)", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.%s MOVE PARTITION ID '202402' TO DISK 'hdd1'", dbName, tableName))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce",
		backupCmd+" create_remote --diff-from-remote="+baseBackup+" --tables="+dbName+".* "+incBackup)

	// Step 3: drop the local copies, increment first, https://github.com/Altinity/clickhouse-backup/issues/1493
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" delete local "+incBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" delete local "+baseBackup)

	// Step 4: before the fix this aborted with "non empty `files` can't find disk: default"
	downloadOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"CLICKHOUSE_FORCE_REBALANCE=true "+backupCmd+" download "+incBackup)
	log.Debug().Msg(downloadOut)
	r.NoError(err, downloadOut)
	r.Contains(downloadOut, "require disk 'default' that not found in system.disks", downloadOut)
	r.NotContains(downloadOut, "can't find disk:", downloadOut)

	tableJSON, err := env.DockerExecOut("clickhouse-backup", "cat",
		fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata/%s/%s.json", incBackup, dbName, tableName))
	r.NoError(err, tableJSON)
	r.Contains(tableJSON, `"required":true`, tableJSON)
	r.Contains(tableJSON, "rebalanced_disk", tableJSON)
	env.DockerExecNoError(r, "clickhouse-backup", "test", "-f", fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata.json", incBackup))

	// Step 5: end to end restore of a rebalanced `required` part
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE %s.%s%s", dbName, tableName, dropSuffix))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce",
		"CLICKHOUSE_FORCE_REBALANCE=true "+backupCmd+" restore --tables="+dbName+"."+tableName+" "+incBackup)
	env.checkCount(r, 1, 2000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))

	fullCleanup(t, r, env, []string{incBackup, baseBackup}, []string{"remote", "local"}, []string{"test_inc_rebalance"}, true, true, true, "config-s3.yml")
}
