//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TestSkipRestoreNonShardedReplicatedTables covers skip_restore_non_sharded_replicated_tables,
// https://github.com/Altinity/clickhouse-backup/issues/1181
// The node is a replica of the 2 shards `cluster_1181`, the other replica is 127.0.0.1 with an unused port,
// so system.clusters reports is_local=0 for it and the node is either the first replica of the first shard
// (restores the data of non-sharded replicated tables) or the first replica of the second shard (restores only their schema).
func TestSkipRestoreNonShardedReplicatedTables(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") < 0 {
		t.Skipf("system.clusters not cleanup properly in version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	const dbName = "test_skip_restore_1181"
	const backupName = "test_skip_restore_1181"
	const globalZkPath = "/clickhouse/tables/test_skip_restore_1181_global"
	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		dropSuffix = " NO DELAY"
	}

	setOwner := func(isOwner bool) {
		ownerPort, otherPort := 1, 9000
		if isOwner {
			ownerPort, otherPort = 9000, 1
		}
		xml := fmt.Sprintf(`<yandex><remote_servers><cluster_1181>
<shard><internal_replication>true</internal_replication><replica><host>127.0.0.1</host><port>%d</port></replica></shard>
<shard><internal_replication>true</internal_replication><replica><host>127.0.0.1</host><port>%d</port></replica></shard>
</cluster_1181></remote_servers></yandex>`, ownerPort, otherPort)
		env.DockerExecNoError(r, "clickhouse", "bash", "-c", fmt.Sprintf("echo -n '%s' > /etc/clickhouse-server/config.d/cluster_1181.xml", xml))
		expectedPort := uint16(ownerPort)
		port := uint16(0)
		for i := 0; i < 60 && port != expectedPort; i++ {
			env.queryWithNoError(t, r, "SYSTEM RELOAD CONFIG")
			r.NoError(env.ch.SelectSingleRowNoCtx(&port, "SELECT any(port) FROM system.clusters WHERE cluster='cluster_1181' AND shard_num=1 SETTINGS empty_result_for_aggregation_by_empty_set=0"))
			if port != expectedPort {
				time.Sleep(1 * time.Second)
			}
		}
		r.Equal(expectedPort, port, "cluster_1181 shard 1 shall use port %d", expectedPort)
	}
	restore := func(cluster string, expectErr bool, args ...string) string {
		cmd := fmt.Sprintf("CLICKHOUSE_SKIP_RESTORE_NON_SHARDED_REPLICATED_TABLES=1 CLICKHOUSE_SKIP_CHECK_NON_SHARDED_REPLICATED_TABLES_CLUSTER=%s clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore %s %s", cluster, strings.Join(args, " "), backupName)
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", cmd)
		level := zerolog.DebugLevel
		if (err != nil) != expectErr {
			level = zerolog.InfoLevel
		}
		log.WithLevel(level).Msg(out)
		if expectErr {
			r.Error(err, out)
		} else {
			r.NoError(err, out)
		}
		return out
	}
	count := func(table string) uint64 {
		result := uint64(0)
		r.NoError(env.ch.SelectSingleRowNoCtx(&result, fmt.Sprintf("SELECT count() FROM `%s`.`%s`", dbName, table)))
		return result
	}
	engineFull := func(table string) string {
		result := ""
		r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT engine_full FROM system.tables WHERE database=? AND name=?", dbName, table))
		return result
	}

	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	// a cluster wide table has no macro which differs between shards in its zookeeper path, a sharded one has {shard}
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.global (id UInt64) ENGINE=ReplicatedMergeTree('%s','{replica}') PARTITION BY id %% 2 ORDER BY id", dbName, globalZkPath))
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.sharded (id UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/test_skip_restore_1181_sharded','{replica}') ORDER BY id", dbName))
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.local (id UInt64) ENGINE=MergeTree() ORDER BY id", dbName))
	for _, table := range []string{"global", "sharded", "local"} {
		env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.%s SELECT number FROM numbers(100)", dbName, table))
	}
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+dbName+".*", backupName)

	// the second shard: the dry-run reports the skipped data, nothing changes
	setOwner(false)
	out := restore("cluster_1181", false, "--rm", "--dry-run")
	r.Contains(out, "skipped_data_tables")
	r.Contains(out, dbName+".global")
	r.Equal(uint64(100), count("global"))

	// the second shard: `restore --rm` executes DROP DATABASE, the non-sharded table is created again without data,
	// the sharded and the local tables are restored as usual
	out = restore("cluster_1181", false, "--rm", "--tables="+dbName+".*")
	r.Contains(out, "skip data restore of 1 non-sharded replicated tables")
	r.Contains(out, "127.0.0.1:1 (shard_num=1, replica_num=1)")
	r.Contains(engineFull("global"), globalZkPath)
	r.Equal(uint64(0), count("global"))
	r.Equal(uint64(100), count("sharded"))
	r.Equal(uint64(100), count("local"))

	// the first replica of the first shard restores the data of the non-sharded table
	setOwner(true)
	out = restore("cluster_1181", false, "--rm", "--tables="+dbName+".*")
	r.Contains(out, "restore data of 1 non-sharded replicated tables")
	r.Equal(uint64(100), count("global"))
	r.Equal(uint64(100), count("sharded"))
	r.Equal(uint64(100), count("local"))

	// the second shard with --data --partitions: DROP PARTITION of the non-sharded table would be replicated to the whole cluster
	setOwner(false)
	out = restore("cluster_1181", false, "--data", "--partitions=0", "--tables="+dbName+".global")
	r.NotContains(out, "DROP PARTITION")
	r.Equal(uint64(100), count("global"))

	// the cluster is read only when the selected tables contain a non-sharded replicated table
	restore("missing_cluster_1181", false, "--rm", "--tables="+dbName+".local")
	r.Equal(uint64(100), count("local"))
	// a missing cluster fails the restore before DROP DATABASE / DROP TABLE
	out = restore("missing_cluster_1181", true, "--rm", "--tables="+dbName+".*")
	r.Contains(out, "cluster 'missing_cluster_1181' not found in system.clusters")
	r.Equal(uint64(100), count("global"))
	r.Equal(uint64(100), count("sharded"))

	// a leftover replica of the non-sharded table is never rebound to default_replica_path with {shard}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.4") >= 0 {
		setOwner(true)
		env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE %s.global%s", dbName, dropSuffix))
		env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.global_leftover (id UInt64) ENGINE=ReplicatedMergeTree('%s','{replica}') ORDER BY id", dbName, globalZkPath))
		env.queryWithNoError(t, r, fmt.Sprintf("DETACH TABLE %s.global_leftover", dbName))
		out = restore("cluster_1181", true, "--tables="+dbName+".global")
		r.Contains(out, "skip_restore_non_sharded_replicated_tables=true => keep the original replica path")
		r.NotContains(out, "will replace to")
		out = restore("cluster_1181", false, "--drop-replica-if-exists", "--tables="+dbName+".global")
		r.Contains(out, "drop_replica_if_exists=true => will drop it and keep the original replica path")
		r.Contains(engineFull("global"), globalZkPath)
		r.Equal(uint64(100), count("global"))
		env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE %s.global%s", dbName, dropSuffix))
		env.queryWithNoError(t, r, fmt.Sprintf("ATTACH TABLE %s.global_leftover", dbName))
	}

	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	env.DockerExecNoError(r, "clickhouse", "bash", "-c", "echo '<yandex><remote_servers></remote_servers></yandex>' > /etc/clickhouse-server/config.d/cluster_1181.xml")
	env.queryWithNoError(t, r, "SYSTEM RELOAD CONFIG")
}
