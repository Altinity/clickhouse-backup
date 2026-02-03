//go:build integration

package main

import (
	"testing"
	"time"
)

// TestS3NoDeletePermission - no parallel
func TestS3NoDeletePermission(t *testing.T) {
	if isTestShouldSkip("RUN_ADVANCED_TESTS") {
		t.Skip("Skipping Advanced integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	env.DockerExecNoError(r, "minio", "/bin/minio_nodelete.sh")
	r.NoError(env.DockerCP("configs/config-s3-nodelete.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))

	generateTestData(t, r, env, "S3", false, defaultTestData)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create_remote", "no_delete_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "no_delete_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore_remote", "no_delete_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "no_delete_backup")
	r.Error(env.DockerExec("clickhouse-backup", "clickhouse-backup", "delete", "remote", "no_delete_backup"))
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameReplicated, dbNameMySQL, dbNamePostgreSQL, Issue331Issue1091Atomic, Issue331Issue1091Ordinary}
	dropDatabasesFromTestDataDataSet(t, r, env, databaseList, true)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", "no_delete_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "list", "remote")
	env.checkObjectStorageIsEmpty(t, r, "S3")
	env.Cleanup(t, r)
}
