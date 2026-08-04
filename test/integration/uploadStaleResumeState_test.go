//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestUploadStaleResumeStateAfterRemoteDelete covers https://github.com/Altinity/clickhouse-backup/issues/1492
// `create_remote --resume` leaves upload.state2 near the local backup, so after the remote backup is deleted
// the next `upload --resume` used to skip every file and upload a backup with metadata.json only.
func TestUploadStaleResumeStateAfterRemoteDelete(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	const configFile = "config-s3.yml"
	backupName := "test_stale_upload_state"
	dbName := "test_stale_upload_state_db"
	tableName := "t1"
	rowsCount := uint64(1000)

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, false, false, configFile)
	defer fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, false, false, configFile)

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.%s (id UInt64) ENGINE=MergeTree() ORDER BY id", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.%s SELECT number FROM numbers(%d)", dbName, tableName, rowsCount))

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "create_remote", "--resume", "--tables="+dbName+".*", backupName)

	// upload.state2 is not removed after a successful upload and create_remote keeps the local backup
	stateFile := fmt.Sprintf("/var/lib/clickhouse/backup/%s/upload.state2", backupName)
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("ls -la %s", stateFile))
	r.NoError(err, "%s\nexpected %s to survive successful create_remote --resume", out, stateFile)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "delete", "remote", backupName)

	// stale upload.state2 must not be trusted, the whole backup has to be uploaded again;
	// --tables must repeat create_remote, otherwise the state is dropped by cleanupStateIfParamsChange
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "upload", "--resume", "--tables="+dbName+".*", backupName)

	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "list", "remote")
	r.NoError(err, "%s", out)
	remoteListed := false
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, backupName) {
			r.NotContains(line, "broken", "re-uploaded backup is broken: %s", line)
			remoteListed = true
		}
	}
	r.True(remoteListed, "%s\n%s is missing in `list remote` after re-upload", out, backupName)

	// the only reliable check that data was really uploaded is a full restore from remote
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "delete", "local", backupName)
	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.4") >= 0 {
		dropSuffix = " SYNC"
	}
	env.queryWithNoError(t, r, "DROP DATABASE "+dbName+dropSuffix)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "restore_remote", "--rm", backupName)

	var restoredRows uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&restoredRows, fmt.Sprintf("SELECT count() FROM %s.%s SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName, tableName)))
	r.Equal(rowsCount, restoredRows)
}
