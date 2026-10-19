//go:build integration

package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestResumableStateConfigChange covers https://github.com/Altinity/clickhouse-backup/issues/1010
// upload.state2 keys are relative to the remote storage path, so after s3.path changed `upload --resume`
// used to trust the state written for the old path and skip every data file of the backup on the new path.
func TestResumableStateConfigChange(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	const configFile = "config-s3.yml"
	backupName := "test_resumable_config_change"
	dbName := "test_resumable_config_change_db"
	tableName := "t1"
	rowsCount := uint64(1000)
	backupCmd := "clickhouse-backup -c /etc/clickhouse-backup/" + configFile
	// another s3.path, keep {version} to isolate concurrent CI runs in the shared bucket
	const otherPathEnv = "S3_PATH=backup_1010/{cluster}/{shard}/{version} "

	cleanupOtherPath := func() {
		_, _ = env.DockerExecOut("clickhouse-backup", "bash", "-c", otherPathEnv+backupCmd+" delete remote "+backupName)
	}
	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, false, false, configFile)
	cleanupOtherPath()
	defer fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, false, false, configFile)
	defer cleanupOtherPath()

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.%s (id UInt64) ENGINE=MergeTree() ORDER BY id", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.%s SELECT number FROM numbers(%d)", dbName, tableName, rowsCount))

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" create --tables="+dbName+".* "+backupName)
	// a schema only copy on the other path, so the backup exists there and the #1492 stale state guard doesn't fire
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", otherPathEnv+backupCmd+" upload --resume --schema "+backupName)
	// full upload to the default path, upload.state2 now records every data file
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" upload --resume "+backupName)
	// same CLI params, only s3.path changed, the state must be discarded and data uploaded to the other path
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", otherPathEnv+backupCmd+" upload --resume "+backupName+" 2>&1")
	r.NoError(err, "%s", out)
	r.Contains(out, "config which defines the resumable state targets changed")

	// the only reliable check that data was really uploaded is a full restore from the other path
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" delete local "+backupName)
	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.11") >= 0 {
		dropSuffix = " SYNC"
	}
	env.queryWithNoError(t, r, "DROP DATABASE "+dbName+dropSuffix)
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", otherPathEnv+backupCmd+" restore_remote --rm "+backupName)

	var restoredRows uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&restoredRows, fmt.Sprintf("SELECT count() FROM %s.%s SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName, tableName)))
	r.Equal(rowsCount, restoredRows)
}
