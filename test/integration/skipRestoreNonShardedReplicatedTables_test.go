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
// The node is both shards of `cluster_1181`, one of them as 127.0.0.2 which ClickHouse never treats as a local address,
// so the node is either the first replica of the first shard (restores the data of non-sharded replicated tables) or the
// first replica of the second shard (restores only their schema). Both shards are the same server with the same macros,
// so {shard} has one value on the cluster, tables with different macro values per shard are covered by testflows.
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

	// firstShard is the host:port of the replica of the first shard, secondShard of the second one
	setCluster := func(firstShard, secondShard string) {
		replicaXML := func(hostPort string) string {
			parts := strings.Split(hostPort, ":")
			return fmt.Sprintf("<shard><internal_replication>true</internal_replication><replica><host>%s</host><port>%s</port></replica></shard>", parts[0], parts[1])
		}
		xml := fmt.Sprintf("<yandex><remote_servers><cluster_1181>%s%s</cluster_1181></remote_servers></yandex>", replicaXML(firstShard), replicaXML(secondShard))
		env.DockerExecNoError(r, "clickhouse", "bash", "-c", fmt.Sprintf("echo -n '%s' > /etc/clickhouse-server/config.d/cluster_1181.xml", xml))
		expected := firstShard + "," + secondShard
		actual := ""
		for i := 0; i < 60 && actual != expected; i++ {
			env.queryWithNoError(t, r, "SYSTEM RELOAD CONFIG")
			r.NoError(env.ch.SelectSingleRowNoCtx(&actual, "SELECT arrayStringConcat(arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((shard_num, concat(host_name, ':', toString(port)))))), ',') FROM system.clusters WHERE cluster='cluster_1181'"))
			if actual != expected {
				time.Sleep(1 * time.Second)
			}
		}
		r.Equal(expected, actual, "cluster_1181 shards")
	}
	setOwner := func(isOwner bool) {
		if isOwner {
			setCluster("127.0.0.1:9000", "127.0.0.2:9000")
		} else {
			setCluster("127.0.0.2:9000", "127.0.0.1:9000")
		}
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
	// `global` has no macros in zookeeper path, `shard_macro` has {shard} with the same value on both shards of cluster_1181
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.global (id UInt64) ENGINE=ReplicatedMergeTree('%s','{replica}') PARTITION BY id %% 2 ORDER BY id", dbName, globalZkPath))
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.shard_macro (id UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/test_skip_restore_1181_shard_macro','{replica}') ORDER BY id", dbName))
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.local (id UInt64) ENGINE=MergeTree() ORDER BY id", dbName))
	for _, table := range []string{"global", "shard_macro", "local"} {
		env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.%s SELECT number FROM numbers(100)", dbName, table))
	}
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+dbName+".*", backupName)

	// the second shard: the dry-run reports the skipped data, nothing changes
	setOwner(false)
	out := restore("cluster_1181", false, "--rm", "--dry-run")
	r.Contains(out, "skipped_data_tables")
	r.Contains(out, dbName+".global")
	r.Equal(uint64(100), count("global"))

	// the second shard: `restore --rm` executes DROP DATABASE, the non-sharded tables are created again without data,
	// the local table is restored as usual, {shard} has one value on both shards, so the table with it is non-sharded
	out = restore("cluster_1181", false, "--rm", "--tables="+dbName+".*")
	r.Contains(out, "skip data restore of 2 non-sharded replicated tables")
	r.Contains(out, "127.0.0.2:9000 (shard_num=1, replica_num=1)")
	r.Contains(out, "{shard} listed in always_sharded_macros have the same value on all 2 shards")
	r.Contains(engineFull("global"), globalZkPath)
	r.Equal(uint64(0), count("global"))
	r.Equal(uint64(0), count("shard_macro"))
	r.Equal(uint64(100), count("local"))

	// the first replica of the first shard restores the data of the non-sharded table
	setOwner(true)
	out = restore("cluster_1181", false, "--rm", "--tables="+dbName+".*")
	r.Contains(out, "restore data of 2 non-sharded replicated tables")
	r.Equal(uint64(100), count("global"))
	r.Equal(uint64(100), count("shard_macro"))
	r.Equal(uint64(100), count("local"))

	// the second shard with --data --partitions: DROP PARTITION of the non-sharded table would be replicated to the whole cluster
	setOwner(false)
	out = restore("cluster_1181", false, "--data", "--partitions=0", "--tables="+dbName+".global")
	r.NotContains(out, "DROP PARTITION")
	r.Equal(uint64(100), count("global"))

	// the cluster is read only when the selected tables contain a replicated table
	restore("missing_cluster_1181", false, "--rm", "--tables="+dbName+".local")
	r.Equal(uint64(100), count("local"))
	// a missing cluster fails the restore before DROP DATABASE / DROP TABLE
	out = restore("missing_cluster_1181", true, "--rm", "--tables="+dbName+".*")
	r.Contains(out, "cluster 'missing_cluster_1181' not found in system.clusters")
	r.Equal(uint64(100), count("global"))
	r.Equal(uint64(100), count("shard_macro"))
	// an unavailable shard fails the restore before DROP when system.macros of other shards is required
	setCluster("127.0.0.1:9000", "127.0.0.3:1")
	out = restore("cluster_1181", true, "--rm", "--tables="+dbName+".*")
	r.Contains(out, "can't read hosts of cluster 'cluster_1181'")
	r.Equal(uint64(100), count("global"))
	r.Equal(uint64(100), count("shard_macro"))

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
