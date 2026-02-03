//go:build integration

package main

import (
	"testing"
	"time"
)

func TestInnerTablesMaterializedView(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 1*time.Second, 1*time.Second, 1*time.Minute)

	env.queryWithNoError(r, "CREATE DATABASE test_mv")
	env.queryWithNoError(r, "CREATE TABLE test_mv.src_table (v UInt64) ENGINE=MergeTree() ORDER BY v")
	env.queryWithNoError(r, "CREATE TABLE test_mv.dst_table (v UInt64) ENGINE=MergeTree() ORDER BY v")
	env.queryWithNoError(r, "CREATE MATERIALIZED VIEW test_mv.mv_with_inner (v UInt64) ENGINE=MergeTree() ORDER BY v AS SELECT v FROM test_mv.src_table")
	env.queryWithNoError(r, "CREATE MATERIALIZED VIEW test_mv.mv_with_dst TO test_mv.dst_table AS SELECT v FROM test_mv.src_table")
	env.queryWithNoError(r, "INSERT INTO test_mv.src_table SELECT number FROM numbers(100)")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "test_mv", "--tables=test_mv.mv_with*,test_mv.dst*")
	r.NoError(env.dropDatabase("test_mv", false))
	var rowCnt uint64

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "test_mv", "--tables=test_mv.mv_with*,test_mv.dst*")
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCnt, "SELECT count() FROM test_mv.mv_with_inner"))
	r.Equal(uint64(100), rowCnt)
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCnt, "SELECT count() FROM test_mv.mv_with_dst"))
	r.Equal(uint64(100), rowCnt)

	r.NoError(env.dropDatabase("test_mv", true))
	// https://github.com/Altinity/clickhouse-backup/issues/777
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "upload", "test_mv", "--delete-source", "--tables=test_mv.mv_with*,test_mv.dst*")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", "test_mv", "--tables=test_mv.mv_with*,test_mv.dst*")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "test_mv", "--tables=test_mv.mv_with*,test_mv.dst*")
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCnt, "SELECT count() FROM test_mv.mv_with_inner"))
	r.Equal(uint64(100), rowCnt)
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCnt, "SELECT count() FROM test_mv.mv_with_dst"))
	r.Equal(uint64(100), rowCnt)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_mv")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", "test_mv")
	r.NoError(env.dropDatabase("test_mv", true))
	env.Cleanup(t, r)
}
