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
	testCases := []struct {
		zkPath     string
		nonSharded bool
	}{
		{"/clickhouse/tables/global/db/t", true},
		{"/clickhouse/tables/{database}/{table}", true},
		{"/clickhouse/tables/{cluster}/{database}/{table}", true},
		{"/clickhouse/tables/{cluster}/{shard}/{database}/{table}", false},
		{"/clickhouse/tables/{layer}-{shard}/t", false},
		{"/clickhouse/tables/{uuid}", false},
		{"/clickhouse/tables/{custom}/t", false},
		{"/clickhouse/tables/{replica}/t", false},
		{"", false},
	}
	for _, tc := range testCases {
		assert.Equal(t, tc.nonSharded, isNonShardedReplicatedPath(tc.zkPath), tc.zkPath)
	}
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

	// the cluster is queried only when the selection contains a non-sharded replicated table
	b.cfg.ClickHouse.SkipRestoreNonShardedReplicatedTables = true
	skip, err = b.getNonShardedReplicatedTablesSkipData(context.Background(), tables[1:])
	require.NoError(t, err)
	assert.Nil(t, skip, "no non-sharded replicated tables")

	b.cfg.ClickHouse.SkipCheckNonShardedReplicatedTablesCluster = " \t "
	_, err = b.getNonShardedReplicatedTablesSkipData(context.Background(), tables)
	assert.ErrorContains(t, err, "skip_check_non_sharded_replicated_tables_cluster is empty")
}

func TestSkipRestoreNonShardedReplicatedTablesUnsupportedByRestoreCloud(t *testing.T) {
	b := &Backuper{cfg: &config.Config{ClickHouse: config.ClickHouseConfig{SkipRestoreNonShardedReplicatedTables: true}}}
	err := b.RestoreCloud(RestoreCloudOptions{Prefix: "backup"}, -1)
	assert.ErrorContains(t, err, "skip_restore_non_sharded_replicated_tables is not supported by restore_cloud")
}
