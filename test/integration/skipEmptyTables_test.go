//go:build integration

package main

import (
	"testing"
	"time"
)

// TestSkipEmptyTables tests the --skip-empty-tables flag for restore and restore_remote commands
// https://github.com/Altinity/clickhouse-backup/issues/1265
func TestSkipEmptyTables(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	// Create test database with tables - one with data and one empty
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS test_skip_empty")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_skip_empty.table_with_data (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_skip_empty.empty_table (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "INSERT INTO test_skip_empty.table_with_data SELECT number FROM numbers(100)")

	// Create backup
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "test_skip_empty_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "upload", "test_skip_empty_backup")

	// Drop tables
	r.NoError(env.dropDatabase("test_skip_empty", false))

	// Test restore without --skip-empty-tables (should restore both tables)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", "test_skip_empty_backup")

	// Verify both tables exist
	var tableCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&tableCount, "SELECT count() FROM system.tables WHERE database='test_skip_empty'"))
	r.Equal(uint64(2), tableCount, "Both tables should be restored without --skip-empty-tables")

	// Verify data in table_with_data
	var dataCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&dataCount, "SELECT count() FROM test_skip_empty.table_with_data"))
	r.Equal(uint64(100), dataCount, "table_with_data should have 100 rows")

	// Verify empty_table is empty
	var emptyCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&emptyCount, "SELECT count() FROM test_skip_empty.empty_table"))
	r.Equal(uint64(0), emptyCount, "empty_table should have 0 rows")

	// Drop tables and test with --skip-empty-tables
	r.NoError(env.dropDatabase("test_skip_empty", false))

	// Test restore with --skip-empty-tables (should skip empty_table completely - both schema and data)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", "--skip-empty-tables", "test_skip_empty_backup")

	// Verify only non-empty table exists (empty table should be skipped entirely)
	r.NoError(env.ch.SelectSingleRowNoCtx(&tableCount, "SELECT count() FROM system.tables WHERE database='test_skip_empty'"))
	r.Equal(uint64(1), tableCount, "Only table_with_data should be created with --skip-empty-tables")

	// Verify data in table_with_data
	r.NoError(env.ch.SelectSingleRowNoCtx(&dataCount, "SELECT count() FROM test_skip_empty.table_with_data"))
	r.Equal(uint64(100), dataCount, "table_with_data should have 100 rows with --skip-empty-tables")

	// Verify empty_table does NOT exist
	r.NoError(env.ch.SelectSingleRowNoCtx(&tableCount, "SELECT count() FROM system.tables WHERE database='test_skip_empty' AND name='empty_table'"))
	r.Equal(uint64(0), tableCount, "empty_table should NOT exist with --skip-empty-tables")

	// Test restore_remote with --skip-empty-tables
	r.NoError(env.dropDatabase("test_skip_empty", false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_skip_empty_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore_remote", "--rm", "--skip-empty-tables", "test_skip_empty_backup")

	// Verify only non-empty table exists
	r.NoError(env.ch.SelectSingleRowNoCtx(&tableCount, "SELECT count() FROM system.tables WHERE database='test_skip_empty'"))
	r.Equal(uint64(1), tableCount, "Only table_with_data should be created with restore_remote --skip-empty-tables")

	// Verify data in table_with_data
	r.NoError(env.ch.SelectSingleRowNoCtx(&dataCount, "SELECT count() FROM test_skip_empty.table_with_data"))
	r.Equal(uint64(100), dataCount, "table_with_data should have 100 rows with restore_remote --skip-empty-tables")

	// Clean up
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_skip_empty_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", "test_skip_empty_backup")
	r.NoError(env.dropDatabase("test_skip_empty", false))
	env.Cleanup(t, r)
}
