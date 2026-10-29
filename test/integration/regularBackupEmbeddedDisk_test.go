//go:build integration

package main

import (
	"os"
	"testing"
	"time"
)

// https://github.com/Altinity/clickhouse-backup/issues/1653
func TestDownloadRegularBackupWithEmbeddedBackupDisk(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.6") < 0 {
		t.Skip("backups_s3 disk is configured for 22.6+ only")
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	backupName := "test_regular_embedded_disk"
	dbName := "test_regular_embedded_disk_db"
	cfg := "/etc/clickhouse-backup/config-s3.yml"
	embeddedEnv := "CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE=true CLICKHOUSE_EMBEDDED_BACKUP_DISK=backups_s3 CLICKHOUSE_TIMEOUT=4h "

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t SELECT number FROM numbers(1000)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "create_remote", "--tables="+dbName+".*", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "local", backupName)

	// the backup is regular, embedded_backup_disk from config shall not be touched
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", embeddedEnv+"clickhouse-backup -c "+cfg+" download "+backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", embeddedEnv+"clickhouse-backup -c "+cfg+" restore --rm "+backupName)
	env.checkCount(r, 1, 1000, "SELECT count() FROM "+dbName+".t")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "local", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "remote", backupName)
	r.NoError(env.dropDatabase(dbName, false))
}
