//go:build integration

package main

import (
	"os"
	"testing"
	"time"
)

// TestSparseSerialization reproduces https://github.com/Altinity/clickhouse-backup/issues/1372
//
// SerializationSparse (default since ClickHouse 23.8, PR ClickHouse/ClickHouse#49631)
// skips writing bytes for any column whose values are all defaults. When such a column
// lives on an object_storage disk, DiskObjectStorageTransaction calls
// createEmptyMetadataFile() and produces a Version 3 metadata file with
// StorageObjectCount=0, TotalSize=0 — a legitimately empty file, not corruption.
//
// Before the fix in pkg/backup/restore.go, restore_remote aborted with
// "invalid object_disk.Metadata" for every wide part that had at least one
// all-defaults sparse column (e.g. __is_deleted__ in any ReplacingMergeTree
// without soft-deletes).
func TestSparseSerialization(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.8") == -1 {
		t.Skipf("Test skipped: sparse serialization is default since 23.8, current %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	dbName := "test_sparse_serialization"
	tableName := "rmt_sparse"
	backupName := "test_sparse_serialization_backup"

	// Pre-clean state from any prior failed run.
	_ = env.dropDatabase(dbName, true)

	env.queryWithNoError(r, "CREATE DATABASE "+dbName)

	// ReplacingMergeTree carries an implicit `_row_exists` / `__is_deleted__` column
	// that is all-default for inserts without soft-deletes. We additionally add an
	// explicit always-default column to make the reproducer independent of CH
	// internals. Storage policy forces all parts onto the S3 object_storage disk.
	// ratio_of_defaults_for_sparse_serialization=0.9 + min_bytes_for_wide_part=0
	// guarantee a wide part using sparse serialization for the zero-valued column.
	createSQL := "CREATE TABLE " + dbName + "." + tableName + ` (
		id UInt64,
		v UInt64,
		zero_col UInt64 DEFAULT 0
	) ENGINE=ReplacingMergeTree() ORDER BY id
	SETTINGS storage_policy='s3_only',
		min_bytes_for_wide_part=0,
		min_rows_for_wide_part=0,
		ratio_of_defaults_for_sparse_serialization=0.9`
	env.queryWithNoError(r, createSQL)

	// Insert enough rows that zero_col is "mostly default" → sparse kicks in,
	// and offsets stream is empty → empty .bin metadata file on S3 disk.
	env.queryWithNoError(r, "INSERT INTO "+dbName+"."+tableName+" (id, v) SELECT number, number FROM numbers(50000)")
	env.queryWithNoError(r, "OPTIMIZE TABLE "+dbName+"."+tableName+" FINAL")

	// Sanity: column is actually serialized as Sparse.
	var sparseSubcolumns uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&sparseSubcolumns,
		"SELECT count() FROM system.parts_columns "+
			"WHERE database=? AND table=? AND column='zero_col' AND serialization_kind='Sparse' AND active",
		dbName, tableName))
	r.Greater(sparseSubcolumns, uint64(0), "expected at least one part with Sparse serialization for zero_col")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+dbName+".*", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "upload", backupName)

	// Drop local copy so restore_remote performs the full download+metadata
	// rewrite path that contains the buggy StorageObjectCount check.
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	r.NoError(env.dropDatabase(dbName, false))

	// Before the fix this aborted with:
	//   invalid object_disk.Metadata: {Version:3 StorageObjectCount:0 TotalSize:0 ...}
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore_remote", "--rm", backupName)

	env.checkCount(r, 1, 50000, "SELECT count() FROM "+dbName+"."+tableName)

	var zeroSum uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&zeroSum, "SELECT sum(zero_col) FROM "+dbName+"."+tableName))
	r.Equal(uint64(0), zeroSum, "zero_col must still be all zeros after restore")

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, true, true, true, "config-s3.yml")
	env.Cleanup(t, r)
}
