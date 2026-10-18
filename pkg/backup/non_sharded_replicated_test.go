package backup

import (
	"context"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicatedZookeeperPath(t *testing.T) {
	noDefault := func() (string, error) {
		t.Fatal("default_replica_path shall not be requested")
		return "", nil
	}
	defaultPath := func() (string, error) { return "/clickhouse/tables/{uuid}/{shard}", nil }
	testCases := []struct {
		name     string
		query    string
		resolve  func() (string, error)
		expected string
		ok       bool
	}{
		{"long syntax", "CREATE TABLE db.t (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db/t', '{replica}') ORDER BY id", noDefault, "/clickhouse/tables/{shard}/db/t", true},
		{"engine arguments after replica", "CREATE TABLE db.t (id UInt64, v UInt64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/global/t', '{shard}-{replica}', v) ORDER BY id", noDefault, "/clickhouse/tables/global/t", true},
		{"no spaces", "CREATE TABLE db.t (id UInt64) ENGINE=ReplicatedMergeTree('/p/t','{replica}') ORDER BY id", noDefault, "/p/t", true},
		{"short syntax with parentheses", "CREATE TABLE db.t (id UInt64) ENGINE = ReplicatedMergeTree() ORDER BY id", defaultPath, "/clickhouse/tables/{uuid}/{shard}", true},
		{"short syntax without parentheses", "CREATE TABLE db.t (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id", defaultPath, "/clickhouse/tables/{uuid}/{shard}", true},
		{"short syntax with engine arguments", "CREATE TABLE db.t (id UInt64, v UInt64) ENGINE = ReplicatedReplacingMergeTree(v) ORDER BY id", defaultPath, "/clickhouse/tables/{uuid}/{shard}", true},
		{"not replicated", "CREATE TABLE db.t (id UInt64) ENGINE = MergeTree ORDER BY id", noDefault, "", false},
		{"replicated database engine is not a table engine", "CREATE DATABASE db ENGINE = Replicated('/clickhouse/databases/db', '{shard}', '{replica}')", noDefault, "", false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			zkPath, ok, err := replicatedZookeeperPath(tc.query, tc.resolve)
			require.NoError(t, err)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.expected, zkPath)
		})
	}
	_, _, err := replicatedZookeeperPath("CREATE TABLE db.t (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id", func() (string, error) {
		return "", errors.New("settings unavailable")
	})
	assert.ErrorContains(t, err, "settings unavailable")
}

func TestIsNonShardedReplicatedPath(t *testing.T) {
	// 2 shards, {shard} is the same on both like in a cluster where shards are distinguished by another macro
	macros := shardMacros{
		"ch1": {"cluster": "c", "shard": "01", "layer": "01", "replica": "ch1"},
		"ch2": {"cluster": "c", "shard": "01", "layer": "02", "replica": "ch2"},
	}
	testCases := []struct {
		zkPath     string
		nonSharded bool
	}{
		{"/clickhouse/tables/global/db/t", true},
		{"/clickhouse/tables/{database}/{table}", true},
		{"/clickhouse/tables/{uuid}", true},
		{"/clickhouse/tables/{cluster}/{database}/{table}", true},
		{"/clickhouse/tables/{shard}/{database}/{table}", true},
		{"/clickhouse/tables/{cluster}/{layer}/{database}/{table}", false},
		{"/clickhouse/tables/{shard}-{layer}/t", false},
		{"/clickhouse/tables/{replica}/t", false},
	}
	for _, tc := range testCases {
		nonSharded, err := isNonShardedReplicatedPath(tc.zkPath, macros)
		require.NoError(t, err, tc.zkPath)
		assert.Equal(t, tc.nonSharded, nonSharded, tc.zkPath)
	}
	// only table identity macros, system.macros of other shards is not needed
	nonSharded, err := isNonShardedReplicatedPath("/clickhouse/tables/{uuid}/{database}/{table}", nil)
	require.NoError(t, err)
	assert.True(t, nonSharded)

	_, err = isNonShardedReplicatedPath("/clickhouse/tables/{undefined}/t", macros)
	assert.ErrorContains(t, err, "macro {undefined} from zookeeper path /clickhouse/tables/{undefined}/t is not defined in system.macros on ch1")
}

func TestPathClusterMacros(t *testing.T) {
	assert.Nil(t, pathClusterMacros("/clickhouse/tables/{uuid}/{database}/{table}"))
	assert.Equal(t, []string{"cluster", "shard"}, pathClusterMacros("/clickhouse/tables/{cluster}/{shard}/{database}/{table}"))
}

func TestShardedMacrosWithSameValues(t *testing.T) {
	macros := shardMacros{
		"ch1": {"shard": "01", "layer": "01"},
		"ch2": {"shard": "01", "layer": "02"},
	}
	used := map[string]struct{}{"shard": {}, "layer": {}}
	assert.Equal(t, []string{"{shard}"}, shardedMacrosWithSameValues([]string{"shard", "{layer}", "unused"}, used, macros))
	assert.Equal(t, []string{"{shard}"}, shardedMacrosWithSameValues([]string{" {shard} "}, used, macros))
	assert.Nil(t, shardedMacrosWithSameValues([]string{"layer"}, used, macros))
	assert.Nil(t, shardedMacrosWithSameValues([]string{"shard"}, map[string]struct{}{}, macros), "macro isn't used in zookeeper paths")
}

func TestNonShardedReplicatedOwner(t *testing.T) {
	// 2 shards x 2 replicas, system.clusters order is not guaranteed
	replicas := func(localShard, localReplica uint32) []clusterReplica {
		result := []clusterReplica{
			{ShardNum: 2, ReplicaNum: 2, HostName: "s2r2", Port: 9000},
			{ShardNum: 1, ReplicaNum: 2, HostName: "s1r2", Port: 9000},
			{ShardNum: 2, ReplicaNum: 1, HostName: "s2r1", Port: 9000},
			{ShardNum: 1, ReplicaNum: 1, HostName: "s1r1", Port: 9000},
		}
		for i := range result {
			if result[i].ShardNum == localShard && result[i].ReplicaNum == localReplica {
				result[i].IsLocal = 1
			}
		}
		return result
	}
	for _, tc := range []struct {
		shard, replica uint32
		isOwner        bool
	}{
		{1, 1, true},
		{1, 2, false},
		{2, 1, false},
		{2, 2, false},
	} {
		owner, isOwner, err := nonShardedReplicatedOwner("c", replicas(tc.shard, tc.replica))
		require.NoError(t, err)
		assert.Equal(t, tc.isOwner, isOwner, "shard=%d replica=%d", tc.shard, tc.replica)
		assert.Equal(t, "s1r1", owner.HostName)
	}

	_, _, err := nonShardedReplicatedOwner("missing", nil)
	assert.ErrorContains(t, err, "cluster 'missing' not found in system.clusters")

	_, _, err = nonShardedReplicatedOwner("other", replicas(0, 0))
	assert.ErrorContains(t, err, "current node is not a replica of cluster 'other'")
}

func TestExcludeTables(t *testing.T) {
	tables := ListOfTables{
		{Database: "db", Table: "g"},
		{Database: "db", Table: "s"},
		{Database: "db", Table: "v"},
	}
	assert.Equal(t, tables, excludeTables(tables, nil))

	got := excludeTables(tables, map[metadata.TableTitle]struct{}{{Database: "db", Table: "g"}: {}, {Database: "db", Table: "missing"}: {}})
	require.Len(t, got, 2)
	assert.Equal(t, "s", got[0].Table)
	assert.Equal(t, "v", got[1].Table)
	// the schema of the skipped table is still restored, the input list must stay unchanged
	require.Len(t, tables, 3)
	assert.Equal(t, "g", tables[0].Table)
}

func TestGetNonShardedReplicatedTablesSkipDataWithoutSQL(t *testing.T) {
	// b.ch is nil, any SQL query panics
	tables := ListOfTables{
		{Database: "db", Table: "g", Query: "CREATE TABLE db.g (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/global/g', '{shard}-{replica}') ORDER BY id"},
		{Database: "db", Table: "s", Query: "CREATE TABLE db.s (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/s', '{replica}') ORDER BY id"},
		{Database: "db", Table: "m", Query: "CREATE TABLE db.m (id UInt64) ENGINE = MergeTree ORDER BY id"},
	}
	b := &Backuper{cfg: &config.Config{ClickHouse: config.ClickHouseConfig{SkipRestoreNonShardedReplicatedTables: false, SkipCheckNonShardedReplicatedTablesCluster: "{cluster}"}}}
	skip, err := b.getNonShardedReplicatedTablesSkipData(context.Background(), tables)
	require.NoError(t, err)
	assert.Nil(t, skip, "flag is off")

	// the cluster is queried only when the selection contains a replicated table
	b.cfg.ClickHouse.SkipRestoreNonShardedReplicatedTables = true
	skip, err = b.getNonShardedReplicatedTablesSkipData(context.Background(), tables[2:])
	require.NoError(t, err)
	assert.Nil(t, skip, "no replicated tables")

	b.cfg.ClickHouse.SkipCheckNonShardedReplicatedTablesCluster = " \t "
	_, err = b.getNonShardedReplicatedTablesSkipData(context.Background(), tables)
	assert.ErrorContains(t, err, "skip_check_non_sharded_replicated_tables_cluster is empty")
}

func TestSkipRestoreNonShardedReplicatedTablesUnsupportedByRestoreCloud(t *testing.T) {
	b := &Backuper{cfg: &config.Config{ClickHouse: config.ClickHouseConfig{SkipRestoreNonShardedReplicatedTables: true}}}
	err := b.RestoreCloud(RestoreCloudOptions{Prefix: "backup"}, -1)
	assert.ErrorContains(t, err, "skip_restore_non_sharded_replicated_tables is not supported by restore_cloud")
}
