//go:build integration

package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// https://github.com/Altinity/clickhouse-backup/issues/1636
func TestRestoreFunctionsKeepExisting(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") == -1 {
		t.Skipf("Test requires ClickHouse >= 21.12 for CREATE OR REPLACE FUNCTION, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	backupName := "test_restore_functions_1636"
	dbName := "test_restore_functions_1636_db"
	incFunc := "test_1636_inc"
	doubleFunc := "test_1636_double"
	restrictedUser := "test_1636_user"
	cfg := "/etc/clickhouse-backup/config-s3.yml"
	functionQuery := func(name string) string {
		var query string
		r.NoError(env.ch.SelectSingleRowNoCtx(&query, fmt.Sprintf("SELECT create_query FROM system.functions WHERE name='%s'", name)))
		return query
	}
	functionExists := fmt.Sprintf("SELECT count() FROM system.functions WHERE name IN ('%s','%s') SETTINGS empty_result_for_aggregation_by_empty_set=0", incFunc, doubleFunc)
	cleanup := func() {
		r.NoError(env.dropDatabase(dbName, true))
		env.queryWithNoError(t, r, "DROP FUNCTION IF EXISTS "+incFunc)
		env.queryWithNoError(t, r, "DROP FUNCTION IF EXISTS "+doubleFunc)
		env.queryWithNoError(t, r, "DROP USER IF EXISTS "+restrictedUser)
	}
	cleanup()

	env.queryWithNoError(t, r, "CREATE FUNCTION "+incFunc+" AS (x) -> (x + 1)")
	env.queryWithNoError(t, r, "CREATE FUNCTION "+doubleFunc+" AS (x) -> (x * 2)")
	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t (x UInt64) ENGINE=MergeTree() ORDER BY x")
	// materialized view uses UDF, so the function shall be restored before the view
	env.queryWithNoError(t, r, "CREATE MATERIALIZED VIEW "+dbName+".mv ENGINE=MergeTree() ORDER BY x AS SELECT x, "+doubleFunc+"(x) AS y FROM "+dbName+".t")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t SELECT number FROM numbers(100)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "create_remote", "--tables="+dbName+".*", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "local", backupName)

	// function changed after backup and one function dropped together with database
	env.queryWithNoError(t, r, "CREATE OR REPLACE FUNCTION "+incFunc+" AS (x) -> (x + 100)")
	r.NoError(env.dropDatabase(dbName, false))
	env.queryWithNoError(t, r, "DROP FUNCTION "+doubleFunc)

	// restore_remote --dry-run shows plan for functions and changes nothing
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore_remote", "--dry-run", "--tables="+dbName+".*", backupName)
	log.Debug().Msg(out)
	r.NoError(err, out)
	r.Contains(out, fmt.Sprintf(`"%s":"keep_existing"`, incFunc))
	r.Contains(out, fmt.Sprintf(`"%s":"create"`, doubleFunc))
	env.checkCount(r, 1, 1, functionExists)

	// without --rm missing function is created before materialized view, changed function is kept
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore_remote", "--tables="+dbName+".*", backupName)
	log.Debug().Msg(out)
	r.NoError(err, out)
	r.Contains(out, "function already exists with different definition, keep existing")
	r.Contains(functionQuery(incFunc), "x + 100")
	r.Contains(functionQuery(doubleFunc), "x * 2")
	env.checkCount(r, 1, 100, "SELECT count() FROM "+dbName+".t")
	env.checkCount(r, 1, 100, "SELECT count() FROM "+dbName+".mv WHERE y = x * 2")

	// restore --dry-run from local backup, --rm replaces changed function
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore", "--dry-run", "--rm", "--tables="+dbName+".*", backupName)
	log.Debug().Msg(out)
	r.NoError(err, out)
	r.Contains(out, fmt.Sprintf(`"%s":"replace"`, incFunc))
	r.Contains(out, fmt.Sprintf(`"%s":"skip_identical"`, doubleFunc))
	r.Contains(functionQuery(incFunc), "x + 100")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore", "--rm", "--tables="+dbName+".*", backupName)
	r.Contains(functionQuery(incFunc), "x + 1)")
	r.Contains(functionQuery(doubleFunc), "x * 2")
	env.checkCount(r, 1, 100, "SELECT count() FROM "+dbName+".t")

	// functions are identical now, so restore doesn't need CREATE FUNCTION and DROP FUNCTION grants
	env.queryWithNoError(t, r, "CREATE USER "+restrictedUser+" IDENTIFIED WITH plaintext_password BY 'test_1636'")
	env.queryWithNoError(t, r, "GRANT ALL ON *.* TO "+restrictedUser)
	env.queryWithNoError(t, r, "REVOKE CREATE FUNCTION, DROP FUNCTION ON *.* FROM "+restrictedUser)
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "CLICKHOUSE_USERNAME="+restrictedUser+" CLICKHOUSE_PASSWORD=test_1636 clickhouse-backup -c "+cfg+" restore --rm --tables="+dbName+".* "+backupName)
	log.Debug().Msg(out)
	r.NoError(err, out)
	env.checkCount(r, 1, 100, "SELECT count() FROM "+dbName+".t")
	env.checkCount(r, 1, 100, "SELECT count() FROM "+dbName+".mv WHERE y = x * 2")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "local", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "remote", backupName)
	cleanup()
}
