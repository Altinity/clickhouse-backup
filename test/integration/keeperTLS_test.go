//go:build integration

package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestKeeperTLS(t *testing.T) {
	if isTestShouldSkip("KEEPER_TLS_ENABLED") {
		t.Skip("KEEPER_TLS_ENABLED is not set or false, skipping TestKeeperTLS")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.9") < 0 {
		t.Skipf("ClickHouse version %s is too old for KEEPER_TLS_ENABLED tests", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 2*time.Minute)

	// create table using ZooKeeper
	dbName := "test_keeper_tls"
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	tableName := "test_table_tls"
	zkPath := "/clickhouse/tables/{shard}/{database}/{table}"
	onCluster := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.3") >= 0 {
		zkPath = "/clickhouse/tables/{shard}/{database}/{table}/{uuid}"
		onCluster = "ON CLUSTER '{cluster}'"
	}
	createSQL := fmt.Sprintf("CREATE TABLE %s.%s %s (id UInt64) ENGINE=ReplicatedMergeTree('%s', '{replica}') ORDER BY id",
		dbName, tableName, onCluster, zkPath)
	env.queryWithNoError(r, createSQL)
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO %s.%s SELECT number FROM numbers(100)", dbName, tableName))

	// create RBAC User
	rbacUser := "test_keeper_tls_user"
	env.queryWithNoError(r, fmt.Sprintf("DROP USER IF EXISTS %s %s", rbacUser, onCluster))
	env.queryWithNoError(r, fmt.Sprintf("CREATE USER %s %s IDENTIFIED WITH sha256_password BY '123'", rbacUser, onCluster))
	defer env.queryWithNoError(r, fmt.Sprintf("DROP USER IF EXISTS %s %s", rbacUser, onCluster))

	// backup with RBAC
	backupName := "test_keeper_tls_backup"
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--rbac", backupName)
	out, err := env.DockerExecOut("clickhouse-backup", "ls", "-laR", fmt.Sprintf("/var/lib/clickhouse/backup/%s/", backupName))
	r.NoError(err)
	t.Logf("Backup directory content:\n%s", out)

	// clean and restore
	r.NoError(env.dropDatabase(dbName, false))
	env.queryWithNoError(r, fmt.Sprintf("DROP USER IF EXISTS %s %s", rbacUser, onCluster))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rbac", backupName)

	// wait for restart after RBAC restore and check
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 2*time.Minute)
	var rowCount, userCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCount, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName)))
	r.Equal(uint64(100), rowCount)
	r.NoError(env.ch.SelectSingleRowNoCtx(&userCount, fmt.Sprintf("SELECT count() FROM system.users WHERE name='%s'", rbacUser)))
	r.Equal(uint64(1), userCount)

	// cleanup
	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
}
