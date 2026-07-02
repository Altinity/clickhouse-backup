//go:build integration

package main

import (
	"os"
	"testing"
	"time"
)

func TestSyncReplicaTimeout(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.11") == -1 {
		t.Skipf("Test skipped, SYNC REPLICA ignore receive_timeout for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Millisecond, 1*time.Second, 1*time.Minute)

	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+t.Name())
	dropReplTables := func() {
		for _, table := range []string{"repl1", "repl2"} {
			query := "DROP TABLE IF EXISTS " + t.Name() + "." + table
			if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") == 1 {
				query += " NO DELAY"
			}
			env.queryWithNoError(t, r, query)
		}
	}
	dropReplTables()
	env.queryWithNoError(t, r, "CREATE TABLE "+t.Name()+".repl1 (v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/"+t.Name()+"/repl','repl1') ORDER BY tuple()")
	env.queryWithNoError(t, r, "CREATE TABLE "+t.Name()+".repl2 (v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/"+t.Name()+"/repl','repl2') ORDER BY tuple()")

	env.queryWithNoError(t, r, "INSERT INTO "+t.Name()+".repl1 SELECT number FROM numbers(10)")

	env.queryWithNoError(t, r, "SYSTEM STOP REPLICATED SENDS "+t.Name()+".repl1")
	env.queryWithNoError(t, r, "SYSTEM STOP FETCHES "+t.Name()+".repl2")

	env.queryWithNoError(t, r, "INSERT INTO "+t.Name()+".repl1 SELECT number FROM numbers(100)")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+t.Name()+".repl*", "test_not_synced_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "upload", "test_not_synced_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_not_synced_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", "test_not_synced_backup")

	env.queryWithNoError(t, r, "SYSTEM START REPLICATED SENDS "+t.Name()+".repl1")
	env.queryWithNoError(t, r, "SYSTEM START FETCHES "+t.Name()+".repl2")

	dropReplTables()
	r.NoError(env.dropDatabase(t.Name(), false))
}
