//go:build integration

package main

import (
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

func TestPostgreSQLMaterialized(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.11") == -1 {
		t.Skipf("MaterializedPostgreSQL doens't support for clickhouse version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.10") == -1 {
		t.Skipf("Serial type, support in 23.10+, look https://github.com/ClickHouse/ClickHouse/issues/44250")
	}
	t.Skip("FREEZE don't support for MaterializedPostgreSQL, https://github.com/ClickHouse/ClickHouse/issues/32902")

	env, r := NewTestEnvironment(t)
	env.DockerExecNoError(r, "pgsql", "bash", "-ce", "echo 'CREATE DATABASE ch_pgsql_repl' | PGPASSWORD=root psql -v ON_ERROR_STOP=1 -U root")
	env.DockerExecNoError(r, "pgsql", "bash", "-ce", "echo \"CREATE TABLE t1 (id BIGINT PRIMARY KEY, s VARCHAR(255)); INSERT INTO t1(id, s) VALUES(1,'s1'),(2,'s2'),(3,'s3')\" | PGPASSWORD=root psql -v ON_ERROR_STOP=1 -U root -d ch_pgsql_repl")
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	env.queryWithNoError(r,
		"CREATE DATABASE ch_pgsql_repl ENGINE=MaterializedPostgreSQL('pgsql:5432','ch_pgsql_repl','root','root') "+
			"SETTINGS materialized_postgresql_schema = 'public'",
	)
	// time to initial snapshot
	count := uint64(0)
	for {
		err := env.ch.SelectSingleRowNoCtx(&count, "SELECT count() FROM system.tables WHERE database='ch_pgsql_repl'")
		r.NoError(err)
		if count > 0 {
			break
		}
		log.Debug().Msgf("ch_pgsql_repl contains %d tables, wait 5 seconds", count)
		time.Sleep(5 * time.Second)
	}

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "test_pgsql_materialized")
	r.NoError(env.dropDatabase("ch_pgsql_repl", false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "test_pgsql_materialized")

	result := 0
	r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM ch_pgsql_repl.t1"))
	r.Equal(3, result, "expect count=3")

	r.NoError(env.dropDatabase("ch_pgsql_repl", false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_pgsql_materialized")
	env.Cleanup(t, r)
}
