//go:build integration

package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestRestoreDistributedCluster(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") < 0 {
		t.Skipf("system.clusters not cleanup properly in version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	xml := `
<yandex>
    <remote_servers>
        <new_cluster>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>127.0.0.1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </new_cluster>
    </remote_servers>
</yandex>
`

	env.DockerExecNoError(r, "clickhouse", "bash", "-c", fmt.Sprintf("echo -n '%s' > /etc/clickhouse-server/config.d/new-cluster.xml", xml))
	var clusterExists string
	for i := 0; i < 10 && clusterExists == ""; i++ {
		r.NoError(env.ch.SelectSingleRowNoCtx(&clusterExists, "SELECT cluster FROM system.clusters WHERE cluster='new_cluster'"))
		if clusterExists == "new_cluster" {
			break
		}
		time.Sleep(1 * time.Second)
	}
	r.Equal("new_cluster", clusterExists)

	// Create test database and table
	dbName := "test_restore_distributed_cluster"
	tableName := "test_table"
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(r, "CREATE TABLE "+dbName+"."+tableName+" (id UInt64, value String) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "CREATE TABLE "+dbName+"."+tableName+"_dist (id UInt64, value String) ENGINE=Distributed('new_cluster',"+dbName+","+tableName+")")
	env.queryWithNoError(r, "INSERT INTO "+dbName+"."+tableName+"_dist SELECT number, toString(number) FROM numbers(100)")

	// Create backup
	backupName := "test_restore_distributed_cluster"
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create", "-c", "/etc/clickhouse-backup/config-s3.yml", "--tables="+dbName+".*", backupName)

	testCases := []struct {
		RestoreDistributedCluster string
		RestoreSchemaOnCluster    string
	}{
		{"{cluster}", ""},
		{"", "{cluster}"},
	}
	for _, tc := range testCases {
		// Get row count before dropping
		var rowCount uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&rowCount, "SELECT count() FROM "+dbName+"."+tableName+"_dist"))
		r.Equal(uint64(100), rowCount)

		// Drop table and database
		r.NoError(env.dropDatabase(dbName, false))

		// remove cluster and wait configuration reload, overwrite with empty config instead of deleting
		// because some ClickHouse versions don't properly detect deleted config files during SYSTEM RELOAD CONFIG
		env.DockerExecNoError(r, "clickhouse", "bash", "-c", "echo '<yandex><remote_servers></remote_servers></yandex>' > /etc/clickhouse-server/config.d/new-cluster.xml")
		newClusterExists := uint64(1)
		for i := 0; i < 60 && newClusterExists == 1; i++ {
			env.queryWithNoError(r, "SYSTEM RELOAD CONFIG")
			r.NoError(env.ch.SelectSingleRowNoCtx(&newClusterExists, "SELECT count() FROM system.clusters WHERE cluster='new_cluster'"))
			if newClusterExists == 0 {
				break
			}
			time.Sleep(1 * time.Second)
		}
		r.Equal(uint64(0), newClusterExists, "new_cluster shall not present in system.clusters")

		// Restore using `CLICKHOUSE_RESTORE_DISTRIBUTED_CLUSTER` and `RESTORE_SCHEMA_ON_CLUSTER`
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", fmt.Sprintf("RESTORE_SCHEMA_ON_CLUSTER='%s' CLICKHOUSE_RESTORE_DISTRIBUTED_CLUSTER='%s' clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore %s", tc.RestoreSchemaOnCluster, tc.RestoreDistributedCluster, backupName))

		// Verify data was restored correctly
		r.NoError(env.ch.SelectSingleRowNoCtx(&rowCount, "SELECT count() FROM "+dbName+"."+tableName+"_dist"))
		r.Equal(uint64(100), rowCount)
		var tableDDL string
		r.NoError(env.ch.SelectSingleRowNoCtx(&tableDDL, "SHOW CREATE TABLE "+dbName+"."+tableName+"_dist"))
		r.NotContains(tableDDL, "new_cluster")
		r.Contains(tableDDL, "Distributed('{cluster}'")
	}

	// Clean up
	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	env.DockerExecNoError(r, "clickhouse", "bash", "-c", "rm -fv /etc/clickhouse-server/config.d/new-cluster.xml")

	env.Cleanup(t, r)
}
