//go:build integration

package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/rs/zerolog/log"
)

func TestCheckSystemPartsColumns(t *testing.T) {
	var err error
	var version int
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") == -1 {
		t.Skipf("Test skipped, system.parts_columns have inconsistency only in 23.3+, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	version, err = env.ch.GetVersion(t.Context())
	r.NoError(err)

	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+t.Name())

	// test compatible data types
	createSQL := "CREATE TABLE " + t.Name() + ".test_system_parts_columns(dt DateTime, v UInt64, e Enum('test' = 1)) ENGINE=MergeTree() ORDER BY tuple()"
	env.queryWithNoError(r, createSQL)
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_system_parts_columns SELECT today() - INTERVAL number DAY, number, 'test' FROM numbers(10)")

	env.queryWithNoError(r, "ALTER TABLE "+t.Name()+".test_system_parts_columns MODIFY COLUMN dt Nullable(DateTime('Europe/Moscow')), MODIFY COLUMN v Nullable(UInt64), MODIFY COLUMN e Enum16('test2'=1, 'test'=2)", t.Name())
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_system_parts_columns SELECT today() - INTERVAL number DAY, number, 'test2' FROM numbers(10)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create", "test_system_parts_columns")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_system_parts_columns")

	r.NoError(env.ch.DropOrDetachTable(clickhouse.Table{Database: t.Name(), Name: "test_system_parts_columns"}, createSQL, "", false, version, "", false, ""))

	// test incompatible data types
	env.queryWithNoError(r, "CREATE TABLE "+t.Name()+".test_system_parts_columns(dt Date, v String) ENGINE=MergeTree() PARTITION BY dt ORDER BY tuple()")
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_system_parts_columns SELECT today() - INTERVAL number DAY, if(number>0,'a',toString(number)) FROM numbers(2)")

	mutationSQL := "ALTER TABLE " + t.Name() + ".test_system_parts_columns MODIFY COLUMN v UInt64"
	err = env.ch.QueryContext(t.Context(), mutationSQL)
	if err != nil {
		errStr := strings.ToLower(err.Error())
		r.True(strings.Contains(errStr, "code: 341") || strings.Contains(errStr, "code: 517") || strings.Contains(errStr, "code: 524") || strings.Contains(errStr, "timeout"), "UNKNOWN ERROR: %s", err.Error())
		log.Debug().Msgf("%s RETURN EXPECTED ERROR=%#v", mutationSQL, err)
	}
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_system_parts_columns SELECT today() - INTERVAL number DAY, number FROM numbers(10)")
	r.Error(env.DockerExec("clickhouse-backup", "clickhouse-backup", "create", "test_system_parts_columns"))
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-lah", "/var/lib/clickhouse/backup/test_system_parts_columns"))
	r.Error(env.DockerExec("clickhouse-backup", "clickhouse-backup", "delete", "local", "test_system_parts_columns"))

	r.NoError(env.ch.DropOrDetachTable(clickhouse.Table{Database: t.Name(), Name: "test_system_parts_columns"}, createSQL, "", false, version, "", false, ""))
	r.NoError(env.dropDatabase(t.Name(), true))
	env.Cleanup(t, r)
}
