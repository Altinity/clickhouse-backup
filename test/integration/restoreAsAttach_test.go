//go:build integration

package main

import (
	"os"
	"testing"
	"time"
)

func TestRestoreAsAttach(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") < 0 {
		t.Skipf("--restore-schema-as-attach not works in version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	// Create test database and table
	dbName := "test_restore_as_attach"
	tableName := "test_table"
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(r, "CREATE TABLE "+dbName+"."+tableName+" (id UInt64, value String) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "INSERT INTO "+dbName+"."+tableName+" SELECT number, toString(number) FROM numbers(100)")

	// Create backup
	backupName := "test_restore_as_attach_backup"
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+dbName+".*", backupName)

	// Get row count before dropping
	var rowCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCount, "SELECT count() FROM "+dbName+"."+tableName))
	r.Equal(uint64(100), rowCount)

	// Drop table and database
	env.queryWithNoError(r, "DROP TABLE "+dbName+"."+tableName+" SYNC")
	r.NoError(env.dropDatabase(dbName, false))

	// Restore using --restore-schema-as-attach + restore_schema_on_cluster
	r.Error(env.DockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--restore-schema-as-attach", backupName))
	// success Restore using --restore-schema-as-attach
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--env", "RESTORE_SCHEMA_ON_CLUSTER=", "--restore-schema-as-attach", backupName)

	// Verify data was restored correctly
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCount, "SELECT count() FROM "+dbName+"."+tableName))
	r.Equal(uint64(100), rowCount)

	// Clean up
	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)

	env.Cleanup(t, r)
}
