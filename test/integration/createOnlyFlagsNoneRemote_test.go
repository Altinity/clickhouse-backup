//go:build integration

package main

import (
	"os"
	"testing"
	"time"
)

// TestCreateOnlyFlagsNoneRemote covers https://github.com/Altinity/clickhouse-backup/issues/1517:
// `create --schema --rbac-only` (and other *-only variants) copy no table data, so a local
// backup must succeed with remote_storage=none even when tables live on an object disk.
func TestCreateOnlyFlagsNoneRemote(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	if !isAdvancedMode() {
		t.Skip("requires advanced mode with storage policies")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") < 0 {
		t.Skip("requires ClickHouse >= 21.8 for s3_only storage policy")
	}
	dbName := "test_1517"
	backupName := "test_1517_schema_rbac"
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local "+backupName+" 2>/dev/null || true")
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t_s3 (id UInt64) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='s3_only'")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t_s3 SELECT number FROM numbers(100)")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "REMOTE_STORAGE=none ALLOW_EMPTY_BACKUPS=1 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create --schema --rbac-only "+backupName)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	r.NoError(env.dropDatabase(dbName, true))
}
