//go:build integration

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestSerialFirstFreezeOnFreshServer - parallel FREEZE on a server without shadow/increment.txt, or with an empty one
// left by a killed server, shall not fail with code 32 "File .../shadow/increment.txt is empty"
// https://github.com/Altinity/clickhouse-backup/issues/1651
func TestSerialFirstFreezeOnFreshServer(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))

	const dbName = "test_serial_first_freeze"
	const tablesCount = 8
	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	for i := 0; i < tablesCount; i++ {
		env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.t%d(id UInt64) ENGINE=MergeTree() ORDER BY id", dbName, i))
		env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.t%d SELECT number FROM numbers(100)", dbName, i))
	}
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE VIEW %s.v AS SELECT * FROM %s.t0", dbName, dbName))

	incrementFile := "/var/lib/clickhouse/shadow/increment.txt"
	for _, tc := range []struct {
		name, prepare, command string
	}{
		{"missing", "rm -f " + incrementFile, "create"},
		{"empty", "mkdir -p /var/lib/clickhouse/shadow && truncate -s 0 " + incrementFile + " && chown -R clickhouse:clickhouse /var/lib/clickhouse/shadow", "create"},
		{"missing_streaming", "rm -f " + incrementFile, "create_remote --streaming"},
		{"empty_streaming", "mkdir -p /var/lib/clickhouse/shadow && truncate -s 0 " + incrementFile + " && chown -R clickhouse:clickhouse /var/lib/clickhouse/shadow", "create_remote --streaming"},
	} {
		backupName := fmt.Sprintf("test_serial_first_freeze_%s_%d", tc.name, time.Now().UnixNano())
		log.Debug().Str("case", tc.name).Msg("FREEZE counter prepared, run backup with parallel FREEZE")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", tc.prepare)
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("CLICKHOUSE_MAX_CONNECTIONS=%d clickhouse-backup %s --tables='%s.*' %s", tablesCount, tc.command, dbName, backupName))
		// clickhouse-server versions which don't create the counter for FREEZE ... WITH NAME leave it missing, never empty
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("test ! -f %s || test -s %s", incrementFile, incrementFile))
		if tc.command == "create" {
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", backupName)
		} else {
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", backupName)
		}
	}
	env.queryWithNoError(t, r, "DROP DATABASE IF EXISTS "+dbName+" SYNC")
}
