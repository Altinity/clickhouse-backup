//go:build integration

package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestRestoreSchemaOnClusterSafety verifies the safety check from
// https://github.com/Altinity/clickhouse-backup/issues/1325:
// when restore_schema_on_cluster is set via config, RESTORE_SCHEMA_ON_CLUSTER env var
// is empty, and target tables have data, restore must fail unless --rm is passed.
func TestRestoreSchemaOnClusterSafety(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.4") < 0 {
		t.Skipf("safety check requires system.tables.total_rows (ClickHouse >= 20.4), got %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	dbName := "test_restore_on_cluster_safety"
	tableName := "t"
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(r, "CREATE TABLE "+dbName+"."+tableName+" (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "INSERT INTO "+dbName+"."+tableName+" SELECT number FROM numbers(10)")

	backupName := "test_restore_on_cluster_safety_backup"
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+dbName+".*", backupName)

	// table still has data, no --rm, env var not set -> must fail with explicit message.
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--env", "USE_RESUMABLE_STATE=false", "--schema", backupName)
	r.Error(err, "restore must fail without --rm when cluster tables contain data, output: %s", out)
	r.True(strings.Contains(out, "issues/1325") || strings.Contains(out, "Re-run with --rm"),
		"error must mention --rm safety guidance, got: %s", out)

	// bypass via explicit empty env var (the documented escape hatch) -> succeeds.
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--env", "RESTORE_SCHEMA_ON_CLUSTER=", "--schema", backupName)

	// bypass via explicit non-empty env var (the documented escape hatch) -> succeeds.
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--env", "RESTORE_SCHEMA_ON_CLUSTER={cluster}", "--schema", backupName)

	// --rm explicitly confirms drop -> succeeds.
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", "--schema", backupName)

	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	env.Cleanup(t, r)
}
