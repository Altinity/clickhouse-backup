//go:build integration

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
)

func TestReplicatedCopyToDetached(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	version, versionErr := env.ch.GetVersion(t.Context())
	r.NoError(versionErr)
	// Create test database and table
	dbName := "test_replicated_copy_to_detached"
	tableName := "test_table"
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)

	// Create a replicated table
	zkPath := "/clickhouse/tables/{shard}/{database}/{table}"
	createSQL := fmt.Sprintf("CREATE TABLE %s.%s (id UInt64, value String) ENGINE=ReplicatedMergeTree('%s','{replica}') ORDER BY id", dbName, tableName, zkPath)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") < 0 {
		createSQL = strings.NewReplacer("{database}", dbName, "{table}", tableName).Replace(createSQL)
	}
	r.NoError(env.ch.CreateTable(clickhouse.Table{Database: dbName, Name: tableName}, createSQL, false, false, "", version, "/var/lib/clickhouse", false, ""))

	// Insert test data
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO %s.%s SELECT number, toString(number) FROM numbers(100)", dbName, tableName))

	// Create backup
	backupName := "test_replicated_copy_to_detached_backup"
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+dbName+".*", backupName)

	// Get row count before dropping
	var rowCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCount, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName)))
	r.Equal(uint64(100), rowCount)

	// Drop database
	r.NoError(env.dropDatabase(dbName, false))

	// Restore with --replicated-copy-to-detached flag, shall restore schema without data
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--replicated-copy-to-detached", backupName)

	// Check that detached folder contains parts,
	out, err := env.DockerExecOut("clickhouse", "bash", "-c", fmt.Sprintf("ls -la /var/lib/clickhouse/data/%s/%s/detached/ | grep -v 'total' | wc -l", dbName, tableName))
	r.NoError(err)
	detachedCount, parseErr := strconv.Atoi(strings.TrimSpace(out))
	r.NoError(parseErr)
	r.Greater(detachedCount, 0, "Detached folder should contain parts")

	// Verify no data was restored to the table
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCount, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName)))
	r.Equal(uint64(0), rowCount, "Table should have no data after restore with --replicated-copy-to-detached")

	// Clean up
	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)

	env.Cleanup(t, r)
}
