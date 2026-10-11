package backup

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/keeper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const kafkaKeeperTestQuery = "CREATE TABLE default.kafka UUID 'a1b2c3d4-0000-4000-8000-000000000001' (`id` UInt64) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'topic1', kafka_group_name = 'g', kafka_format = 'JSONEachRow', kafka_keeper_path = '/clickhouse/default/kafka/{uuid}', kafka_replica_name = '{replica}'"

func TestParseKafkaSetting(t *testing.T) {
	testCases := []struct {
		name, query, setting, expected string
		found                          bool
	}{
		{"keeper path", kafkaKeeperTestQuery, kafkaKeeperPathSetting, "/clickhouse/default/kafka/{uuid}", true},
		{"replica name", kafkaKeeperTestQuery, "kafka_replica_name", "{replica}", true},
		{"reversed order", "ENGINE = Kafka SETTINGS kafka_replica_name='r1',kafka_keeper_path='/p/1', kafka_topic_list = 't'", kafkaKeeperPathSetting, "/p/1", true},
		{"escaped quote and backslash", `ENGINE = Kafka SETTINGS kafka_keeper_path = '/p/it\'s\\x', kafka_replica_name = 'r'`, kafkaKeeperPathSetting, `/p/it's\x`, true},
		{"missing", "ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092'", kafkaKeeperPathSetting, "", false},
		{"prefix of other setting", "ENGINE = Kafka SETTINGS xkafka_keeper_path = '/wrong'", kafkaKeeperPathSetting, "", false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value, found := parseKafkaSetting(tc.query, tc.setting)
			assert.Equal(t, tc.found, found)
			assert.Equal(t, tc.expected, value)
		})
	}
	_, found := kafkaKeeperPathFromQuery("MergeTree", kafkaKeeperTestQuery)
	assert.False(t, found, "only Kafka engine is supported")
	_, found = kafkaKeeperPathFromQuery("Kafka", kafkaKeeperTestQuery)
	assert.True(t, found)
}

func TestReplaceKafkaSetting(t *testing.T) {
	query := replaceKafkaSetting(kafkaKeeperTestQuery, kafkaKeeperPathSetting, func(rawValue string) string {
		return strings.ReplaceAll(rawValue, "{uuid}", "u1")
	})
	assert.Contains(t, query, "kafka_keeper_path = '/clickhouse/default/kafka/u1', kafka_replica_name = '{replica}'")
	assert.Equal(t, "no settings", replaceKafkaSetting("no settings", kafkaKeeperPathSetting, func(string) string { return "x" }))
}

func TestResolveKafkaKeeperPath(t *testing.T) {
	assert.Equal(t, "/clickhouse/db1/t1/u1/{shard}", resolveKafkaKeeperPath("/clickhouse/{database}/{table}/{uuid}/{shard}", "u1", "db1", "t1"))
}

func TestFilterKafkaKeeperNodes(t *testing.T) {
	dump := []keeper.DumpNode{
		{Path: "", Value: nil},
		{Path: "dropped", Value: nil},
		{Path: "dropped/lock", Value: nil},
		{Path: "replicas", Value: nil},
		{Path: "replicas/r1", Value: []byte("1")},
		{Path: "topic_partition_locks", Value: nil},
		{Path: "topics", Value: nil},
		{Path: "topics/t1/partitions/0/committed", Value: []byte("42")},
		{Path: "topics/t1/partitions/0/intention", Value: []byte("5")},
	}
	backupNodes := filterKafkaKeeperNodesForBackup(dump)
	assert.Equal(t, []metadata.KafkaKeeperNode{
		{Path: "replicas", Value: ""},
		{Path: "replicas/r1", Value: "1"},
		{Path: "topic_partition_locks", Value: ""},
		{Path: "topics", Value: ""},
		{Path: "topics/t1/partitions/0/committed", Value: "42"},
		{Path: "topics/t1/partitions/0/intention", Value: "5"},
	}, backupNodes)

	restoreNodes := filterKafkaKeeperNodesForRestore(append(backupNodes, metadata.KafkaKeeperNode{Path: "topic_partition_locks/t1_0.lock"}, metadata.KafkaKeeperNode{Path: "dropped"}), kafkaKeeperTestVersion)
	assert.Equal(t, []metadata.KafkaKeeperNode{
		{Path: "replicas", Value: ""},
		{Path: "topic_partition_locks", Value: ""},
		{Path: "topics", Value: ""},
		{Path: "topics/t1/partitions/0/committed", Value: "42"},
		{Path: "topics/t1/partitions/0/intention", Value: "5"},
	}, restoreNodes)
	assert.Equal(t, "t1/0=42", formatKafkaKeeperOffsets(restoreNodes))
}

func TestParseKafkaKeeperOffsetNode(t *testing.T) {
	topic, partition, kind, ok := parseKafkaKeeperOffsetNode("topics/t.1-x/partitions/12/committed")
	assert.True(t, ok)
	assert.Equal(t, []string{"t.1-x", "12", "committed"}, []string{topic, partition, kind})
	for _, p := range []string{"topics/t1/partitions/12", "topics/t1/partitions/12/other", "replicas/t1/partitions/12/committed", "topics/t1/partitions/12/committed/x"} {
		_, _, _, ok = parseKafkaKeeperOffsetNode(p)
		assert.False(t, ok, p)
	}
}

func TestDecideKafkaKeeperRestore(t *testing.T) {
	assert.Equal(t, kafkaKeeperRestoreCreate, decideKafkaKeeperRestore(false, nil, nil))
	assert.Equal(t, kafkaKeeperRestoreOverwrite, decideKafkaKeeperRestore(true, nil, nil))
	assert.Equal(t, kafkaKeeperRestoreSkip, decideKafkaKeeperRestore(true, []string{"r1"}, nil))
	assert.Equal(t, kafkaKeeperRestoreSkip, decideKafkaKeeperRestore(true, nil, []string{"t1_0.lock"}))
}

type fakeKeeperNode struct {
	value     string
	ephemeral bool
}

// fakeKeeper - in-memory znodes tree, keys are absolute paths
type fakeKeeper struct {
	nodes map[string]fakeKeeperNode
}

func newFakeKeeper(nodes map[string]fakeKeeperNode) *fakeKeeper {
	k := &fakeKeeper{nodes: map[string]fakeKeeperNode{"/": {}}}
	for p, n := range nodes {
		k.nodes[p] = n
	}
	return k
}

func (k *fakeKeeper) Exists(nodePath string) (bool, error) {
	_, exists := k.nodes[nodePath]
	return exists, nil
}

func (k *fakeKeeper) Get(nodePath string) ([]byte, bool, error) {
	n, exists := k.nodes[nodePath]
	if !exists {
		return nil, false, nil
	}
	return []byte(n.value), true, nil
}

func (k *fakeKeeper) Create(nodePath string, value []byte) (bool, error) {
	if _, exists := k.nodes[nodePath]; exists {
		return false, nil
	}
	if _, exists := k.nodes[path.Dir(nodePath)]; !exists {
		return false, errors.Errorf("parent of %s doesn't exist", nodePath)
	}
	k.nodes[nodePath] = fakeKeeperNode{value: string(value)}
	return true, nil
}

func (k *fakeKeeper) Children(nodePath string) ([]string, error) {
	children := make([]string, 0)
	for p := range k.nodes {
		if p != "/" && path.Dir(p) == nodePath {
			children = append(children, path.Base(p))
		}
	}
	sort.Strings(children)
	return children, nil
}

func (k *fakeKeeper) DumpTree(prefix string) ([]keeper.DumpNode, error) {
	paths := make([]string, 0)
	for p, n := range k.nodes {
		if (p == prefix || strings.HasPrefix(p, prefix+"/")) && !n.ephemeral {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	result := make([]keeper.DumpNode, 0, len(paths))
	for _, p := range paths {
		result = append(result, keeper.DumpNode{Path: strings.TrimPrefix(strings.TrimPrefix(p, prefix), "/"), Value: []byte(k.nodes[p].value)})
	}
	return result, nil
}

func (k *fakeKeeper) EnsureNode(nodePath string) error {
	if _, exists := k.nodes[nodePath]; exists {
		return nil
	}
	if err := k.EnsureNode(path.Dir(nodePath)); err != nil {
		return err
	}
	k.nodes[nodePath] = fakeKeeperNode{}
	return nil
}

func (k *fakeKeeper) Upsert(nodePath string, value []byte) error {
	if _, exists := k.nodes[path.Dir(nodePath)]; !exists {
		return errors.Errorf("parent of %s doesn't exist", nodePath)
	}
	k.nodes[nodePath] = fakeKeeperNode{value: string(value)}
	return nil
}

func (k *fakeKeeper) Delete(nodePath string) error {
	delete(k.nodes, nodePath)
	return nil
}

const kafkaKeeperTestVersion = 26008000

var kafkaKeeperTestBackupNodes = []metadata.KafkaKeeperNode{
	{Path: "replicas"},
	{Path: "replicas/r1", Value: "1"},
	{Path: "topic_partition_locks"},
	{Path: "topics"},
	{Path: "topics/t1"},
	{Path: "topics/t1/partitions"},
	{Path: "topics/t1/partitions/0"},
	{Path: "topics/t1/partitions/0/committed", Value: "42"},
	{Path: "topics/t1/partitions/1"},
	{Path: "topics/t1/partitions/1/committed", Value: "7"},
	{Path: "topics/t1/partitions/1/intention", Value: "3"},
}

func TestApplyKafkaKeeperOffsetsCreate(t *testing.T) {
	k := newFakeKeeper(nil)
	heldLocks, err := applyKafkaKeeperOffsets(k, "/clickhouse/kafka/t", "default.kafka", kafkaKeeperTestBackupNodes, []string{"t1"}, true, kafkaKeeperTestVersion)
	require.NoError(t, err)
	assert.Equal(t, []string{"/clickhouse/kafka/t/topic_partition_locks/t1_0.lock", "/clickhouse/kafka/t/topic_partition_locks/t1_1.lock"}, heldLocks)
	for _, lockPath := range heldLocks {
		assert.Equal(t, fakeKeeperNode{value: kafkaKeeperHoldOwner}, k.nodes[lockPath], "hold lock shall be persistent")
	}
	assert.Equal(t, "42", k.nodes["/clickhouse/kafka/t/topics/t1/partitions/0/committed"].value)
	assert.Equal(t, "7", k.nodes["/clickhouse/kafka/t/topics/t1/partitions/1/committed"].value)
	assert.Equal(t, "3", k.nodes["/clickhouse/kafka/t/topics/t1/partitions/1/intention"].value)
	assert.Contains(t, k.nodes, "/clickhouse/kafka/t/replicas", "replicas node is required to skip keeper skeleton creation")
	assert.Contains(t, k.nodes, "/clickhouse/kafka/t/topic_partition_locks")
	assert.NotContains(t, k.nodes, "/clickhouse/kafka/t/replicas/r1", "source replicas shall not be restored")
}

func TestApplyKafkaKeeperOffsetsOverwrite(t *testing.T) {
	k := newFakeKeeper(map[string]fakeKeeperNode{
		"/p":                                  {},
		"/p/replicas":                         {},
		"/p/replicas/r2":                      {value: "1"},
		"/p/topic_partition_locks":            {},
		"/p/topics":                           {},
		"/p/topics/t1":                        {},
		"/p/topics/t1/partitions":             {},
		"/p/topics/t1/partitions/0":           {},
		"/p/topics/t1/partitions/0/committed": {value: "100"},
		"/p/topics/t1/partitions/0/intention": {value: "10"},
		"/p/topics/t1/partitions/2":           {},
		"/p/topics/t1/partitions/2/committed": {value: "500"},
		// own hold lock left by previous `restore --schema` is not an active replica
		"/p/topic_partition_locks/t1_0.lock": {value: kafkaKeeperHoldOwner},
	})
	heldLocks, err := applyKafkaKeeperOffsets(k, "/p", "default.kafka", kafkaKeeperTestBackupNodes, []string{"t1"}, true, kafkaKeeperTestVersion)
	require.NoError(t, err)
	assert.Equal(t, []string{"/p/topic_partition_locks/t1_0.lock", "/p/topic_partition_locks/t1_1.lock"}, heldLocks)
	assert.Equal(t, "42", k.nodes["/p/topics/t1/partitions/0/committed"].value)
	assert.NotContains(t, k.nodes, "/p/topics/t1/partitions/0/intention", "intention absent in backup shall be removed")
	assert.Equal(t, "7", k.nodes["/p/topics/t1/partitions/1/committed"].value)
	assert.Equal(t, "3", k.nodes["/p/topics/t1/partitions/1/intention"].value)
	assert.Equal(t, "500", k.nodes["/p/topics/t1/partitions/2/committed"].value, "partitions absent in backup are untouched")
	assert.Equal(t, "1", k.nodes["/p/replicas/r2"].value, "existing replicas are untouched")
	assert.NotContains(t, k.nodes, "/p/replicas/r1")
}

func TestApplyKafkaKeeperOffsetsSkipActive(t *testing.T) {
	activeNodes := map[string]struct {
		path string
		node fakeKeeperNode
	}{
		"is_active":               {"/p/replicas/r2/is_active", fakeKeeperNode{ephemeral: true}},
		"partition lock":          {"/p/topic_partition_locks/t1_0.lock", fakeKeeperNode{value: "r2", ephemeral: true}},
		"persistent foreign lock": {"/p/topic_partition_locks/t1_0.lock", fakeKeeperNode{value: "r2"}},
	}
	for name, active := range activeNodes {
		t.Run(name, func(t *testing.T) {
			k := newFakeKeeper(map[string]fakeKeeperNode{
				"/p":                                  {},
				"/p/replicas":                         {},
				"/p/replicas/r2":                      {},
				"/p/topic_partition_locks":            {},
				"/p/topics":                           {},
				"/p/topics/t1":                        {},
				"/p/topics/t1/partitions":             {},
				"/p/topics/t1/partitions/0":           {},
				"/p/topics/t1/partitions/0/committed": {value: "100"},
				active.path:                           active.node,
			})
			heldLocks, err := applyKafkaKeeperOffsets(k, "/p", "default.kafka", kafkaKeeperTestBackupNodes, []string{"t1"}, true, kafkaKeeperTestVersion)
			require.NoError(t, err)
			assert.Empty(t, heldLocks, "active replicas already consume, nothing to hold")
			assert.Equal(t, "100", k.nodes["/p/topics/t1/partitions/0/committed"].value)
			assert.NotContains(t, k.nodes, "/p/topics/t1/partitions/1/committed")
			assert.NotContains(t, k.nodes, "/p/topic_partition_locks/t1_1.lock")
		})
	}
}

func TestChangeTableQueryToAdjustMappingKafkaKeeperPath(t *testing.T) {
	newTables := func(query string) ListOfTables {
		return ListOfTables{{Database: "default", Table: "kafka", Query: query}}
	}
	tables := newTables(kafkaKeeperTestQuery)
	require.NoError(t, changeTableQueryToAdjustDatabaseMapping(&tables, map[string]string{"default": "db2"}))
	keeperPath, _ := parseKafkaSetting(tables[0].Query, kafkaKeeperPathSetting)
	assert.Equal(t, "/clickhouse/db2/kafka/{uuid}", keeperPath)
	assert.NotContains(t, tables[0].Query, "a1b2c3d4-0000-4000-8000-000000000001", "new UUID shall be generated")

	tables = newTables(kafkaKeeperTestQuery)
	require.NoError(t, changeTableQueryToAdjustTableMapping(&tables, map[string]string{"kafka": "kafka2"}))
	keeperPath, _ = parseKafkaSetting(tables[0].Query, kafkaKeeperPathSetting)
	assert.Equal(t, "/clickhouse/default/kafka2/{uuid}", keeperPath)
	assert.Equal(t, "kafka2", tables[0].Table)

	// only a whole path segment is replaced, `t` doesn't match `/tmp`
	query := strings.Replace(kafkaKeeperTestQuery, "default.kafka", "default.t", 1)
	query = strings.Replace(query, "/clickhouse/default/kafka/{uuid}", "/tmp/t", 1)
	tables = ListOfTables{{Database: "default", Table: "t", Query: query}}
	require.NoError(t, changeTableQueryToAdjustTableMapping(&tables, map[string]string{"t": "t2"}))
	keeperPath, _ = parseKafkaSetting(tables[0].Query, kafkaKeeperPathSetting)
	assert.Equal(t, "/tmp/t2", keeperPath)
	assert.Equal(t, "/x/t2/y", parseKafkaKeeperPathOf(adjustKafkaKeeperPathForMapping("ENGINE = Kafka SETTINGS kafka_keeper_path = '/x/t/y'", "t", "t2", "default.t")))
	assert.Equal(t, "/tmp/x", parseKafkaKeeperPathOf(adjustKafkaKeeperPathForMapping("ENGINE = Kafka SETTINGS kafka_keeper_path = '/tmp/x'", "t", "t2", "default.t")))

	// path without database segment stays as is
	query = strings.Replace(kafkaKeeperTestQuery, "/clickhouse/default/kafka/{uuid}", "/static/path", 1)
	tables = newTables(query)
	require.NoError(t, changeTableQueryToAdjustDatabaseMapping(&tables, map[string]string{"default": "db2"}))
	keeperPath, _ = parseKafkaSetting(tables[0].Query, kafkaKeeperPathSetting)
	assert.Equal(t, "/static/path", keeperPath)
}

func parseKafkaKeeperPathOf(query string) string {
	keeperPath, _ := parseKafkaSetting(query, kafkaKeeperPathSetting)
	return keeperPath
}

func TestKafkaKeeperHoldLockPaths(t *testing.T) {
	assert.Equal(t, []string{"topic_partition_locks/t1_0.lock", "topic_partition_locks/t1_1.lock"}, kafkaKeeperHoldLockPaths(kafkaKeeperTestBackupNodes, 25006000))
	assert.Equal(t, []string{"topics/t1/partitions/0/lock", "topics/t1/partitions/1/lock"}, kafkaKeeperHoldLockPaths(kafkaKeeperTestBackupNodes, 25005000))
	assert.Empty(t, kafkaKeeperHoldLockPaths([]metadata.KafkaKeeperNode{{Path: "replicas"}, {Path: "topics/t1/partitions/0/intention", Value: "1"}}, kafkaKeeperTestVersion))
}

func TestReleaseOwnKafkaKeeperLocks(t *testing.T) {
	k := newFakeKeeper(map[string]fakeKeeperNode{
		"/p":                                 {},
		"/p/topic_partition_locks":           {},
		"/p/topic_partition_locks/t1_0.lock": {value: kafkaKeeperHoldOwner},
		"/p/topic_partition_locks/t1_1.lock": {value: "r2", ephemeral: true},
		"/p/topic_partition_locks/t2_0.lock": {value: kafkaKeeperHoldOwner},
	})
	ownLocks, err := ownKafkaKeeperLocks(k, "/p")
	require.NoError(t, err)
	assert.Equal(t, []string{"/p/topic_partition_locks/t1_0.lock", "/p/topic_partition_locks/t2_0.lock"}, ownLocks)
	require.NoError(t, releaseOwnKafkaKeeperLocks(k, "/p", "default.kafka"))
	assert.NotContains(t, k.nodes, "/p/topic_partition_locks/t1_0.lock")
	assert.NotContains(t, k.nodes, "/p/topic_partition_locks/t2_0.lock")
	assert.Equal(t, "r2", k.nodes["/p/topic_partition_locks/t1_1.lock"].value, "locks of other owners are never touched")
	// path without topic_partition_locks
	require.NoError(t, releaseOwnKafkaKeeperLocks(newFakeKeeper(nil), "/missing", "default.kafka"))
}

func TestApplyKafkaKeeperOffsetsSkipHold(t *testing.T) {
	// `restore --skip-kafka-keeper-hold` unblocks a table created by previous `restore --schema`, even with active replicas
	k := newFakeKeeper(map[string]fakeKeeperNode{
		"/p":                                  {},
		"/p/replicas":                         {},
		"/p/replicas/r1":                      {},
		"/p/replicas/r1/is_active":            {ephemeral: true},
		"/p/topic_partition_locks":            {},
		"/p/topic_partition_locks/t1_0.lock":  {value: kafkaKeeperHoldOwner},
		"/p/topics":                           {},
		"/p/topics/t1":                        {},
		"/p/topics/t1/partitions":             {},
		"/p/topics/t1/partitions/0":           {},
		"/p/topics/t1/partitions/0/committed": {value: "42"},
	})
	heldLocks, err := applyKafkaKeeperOffsets(k, "/p", "default.kafka", kafkaKeeperTestBackupNodes, []string{"t1"}, false, kafkaKeeperTestVersion)
	require.NoError(t, err)
	assert.Empty(t, heldLocks)
	assert.NotContains(t, k.nodes, "/p/topic_partition_locks/t1_0.lock")

	k = newFakeKeeper(nil)
	heldLocks, err = applyKafkaKeeperOffsets(k, "/p", "default.kafka", kafkaKeeperTestBackupNodes, []string{"t1"}, false, kafkaKeeperTestVersion)
	require.NoError(t, err)
	assert.Empty(t, heldLocks)
	assert.Equal(t, "42", k.nodes["/p/topics/t1/partitions/0/committed"].value)
	locks, _ := k.Children("/p/topic_partition_locks")
	assert.Empty(t, locks)
}

func TestFilterKafkaKeeperLockNodes(t *testing.T) {
	dump := []keeper.DumpNode{
		{Path: "topic_partition_locks"},
		{Path: "topic_partition_locks/t1_0.lock", Value: []byte(kafkaKeeperHoldOwner)},
		{Path: "topics/t1/partitions/0"},
		{Path: "topics/t1/partitions/0/committed", Value: []byte("42")},
		{Path: "topics/t1/partitions/0/lock", Value: []byte(kafkaKeeperHoldOwner)},
		{Path: "topics/t1/partitions/1/lock", Value: []byte("r2")},
	}
	backupNodes := filterKafkaKeeperNodesForBackup(dump)
	assert.Equal(t, []metadata.KafkaKeeperNode{
		{Path: "topic_partition_locks"},
		{Path: "topics/t1/partitions/0"},
		{Path: "topics/t1/partitions/0/committed", Value: "42"},
		{Path: "topics/t1/partitions/1/lock", Value: "r2"},
	}, backupNodes, "own hold locks of both layouts shall not be backed up")
	assert.Equal(t, []metadata.KafkaKeeperNode{
		{Path: "topic_partition_locks"},
		{Path: "topics/t1/partitions/0"},
		{Path: "topics/t1/partitions/0/committed", Value: "42"},
	}, filterKafkaKeeperNodesForRestore(backupNodes, kafkaKeeperTestVersion), "partition locks of any layout shall not be restored")
}

func TestApplyKafkaKeeperOffsetsLegacyLocks(t *testing.T) {
	// ClickHouse 24.8-25.5 locks partitions with topics/<topic>/partitions/<partition>/lock
	k := newFakeKeeper(nil)
	heldLocks, err := applyKafkaKeeperOffsets(k, "/p", "default.kafka", kafkaKeeperTestBackupNodes, []string{"t1"}, true, 25003000)
	require.NoError(t, err)
	assert.Equal(t, []string{"/p/topics/t1/partitions/0/lock", "/p/topics/t1/partitions/1/lock"}, heldLocks)
	for _, lockPath := range heldLocks {
		assert.Equal(t, fakeKeeperNode{value: kafkaKeeperHoldOwner}, k.nodes[lockPath])
	}
	assert.NotContains(t, k.nodes, "/p/topic_partition_locks/t1_0.lock")

	// own legacy locks are not active replicas, second run overwrites offsets and holds again
	heldLocks, err = applyKafkaKeeperOffsets(k, "/p", "default.kafka", kafkaKeeperTestBackupNodes, []string{"t1"}, true, 25003000)
	require.NoError(t, err)
	assert.Len(t, heldLocks, 2)

	// foreign ephemeral legacy lock is an active consumer
	k.nodes["/p/topics/t1/partitions/0/lock"] = fakeKeeperNode{value: "r2", ephemeral: true}
	k.nodes["/p/topics/t1/partitions/0/committed"] = fakeKeeperNode{value: "100"}
	activeReplicas, partitionLocks, err := kafkaKeeperActivity(k, "/p")
	require.NoError(t, err)
	assert.Empty(t, activeReplicas)
	assert.Equal(t, []string{"topics/t1/partitions/0/lock"}, partitionLocks)
	heldLocks, err = applyKafkaKeeperOffsets(k, "/p", "default.kafka", kafkaKeeperTestBackupNodes, []string{"t1"}, true, 25003000)
	require.NoError(t, err)
	assert.Empty(t, heldLocks)
	assert.Equal(t, "100", k.nodes["/p/topics/t1/partitions/0/committed"].value)
}

func TestReleaseOwnKafkaKeeperLocksBothLayouts(t *testing.T) {
	// server could be upgraded between schema and data restore, own locks of both layouts are released
	k := newFakeKeeper(map[string]fakeKeeperNode{
		"/p":                                  {},
		"/p/topic_partition_locks":            {},
		"/p/topic_partition_locks/t1_0.lock":  {value: kafkaKeeperHoldOwner},
		"/p/topics":                           {},
		"/p/topics/t1":                        {},
		"/p/topics/t1/partitions":             {},
		"/p/topics/t1/partitions/0":           {},
		"/p/topics/t1/partitions/0/committed": {value: "42"},
		"/p/topics/t1/partitions/0/lock":      {value: kafkaKeeperHoldOwner},
		"/p/topics/t1/partitions/1":           {},
		"/p/topics/t1/partitions/1/lock":      {value: "r2", ephemeral: true},
	})
	require.NoError(t, releaseOwnKafkaKeeperLocks(k, "/p", "default.kafka"))
	assert.NotContains(t, k.nodes, "/p/topic_partition_locks/t1_0.lock")
	assert.NotContains(t, k.nodes, "/p/topics/t1/partitions/0/lock")
	assert.Equal(t, "42", k.nodes["/p/topics/t1/partitions/0/committed"].value)
	assert.Equal(t, "r2", k.nodes["/p/topics/t1/partitions/1/lock"].value, "locks of other owners are never touched")
}

func TestParseKafkaTopicList(t *testing.T) {
	assert.Equal(t, []string{"topic1"}, parseKafkaTopicList(kafkaKeeperTestQuery))
	assert.Equal(t, []string{"a", "b.c", "d-e"}, parseKafkaTopicList("ENGINE = Kafka SETTINGS kafka_topic_list = 'a, b.c ,d-e,'"))
	assert.Empty(t, parseKafkaTopicList("ENGINE = Kafka SETTINGS kafka_broker_list = 'k:9092'"))
}

func TestApplyKafkaKeeperOffsetsCrossVersionSkeleton(t *testing.T) {
	// backup from ClickHouse 24.8-25.5 has no topic_partition_locks, ClickHouse 25.6+ doesn't consume without it
	legacyBackupNodes := []metadata.KafkaKeeperNode{
		{Path: "replicas"},
		{Path: "topics"},
		{Path: "topics/t1"},
		{Path: "topics/t1/partitions"},
		{Path: "topics/t1/partitions/0"},
		{Path: "topics/t1/partitions/0/committed", Value: "42"},
	}
	k := newFakeKeeper(nil)
	heldLocks, err := applyKafkaKeeperOffsets(k, "/p", "default.kafka", legacyBackupNodes, []string{"t1", "t2"}, false, 26008000)
	require.NoError(t, err)
	assert.Empty(t, heldLocks)
	for _, nodePath := range []string{"/p/topic_partition_locks", "/p/replicas", "/p/topics/t1/partitions", "/p/topics/t2/partitions"} {
		assert.Contains(t, k.nodes, nodePath)
	}
	assert.Equal(t, "42", k.nodes["/p/topics/t1/partitions/0/committed"].value)

	// backup from ClickHouse 25.6+ restored on older ClickHouse keeps the native layout without topic_partition_locks
	k = newFakeKeeper(nil)
	heldLocks, err = applyKafkaKeeperOffsets(k, "/p", "default.kafka", kafkaKeeperTestBackupNodes, []string{"t1"}, false, 24008000)
	require.NoError(t, err)
	assert.Empty(t, heldLocks)
	assert.NotContains(t, k.nodes, "/p/topic_partition_locks")
	assert.Contains(t, k.nodes, "/p/replicas")
	assert.Equal(t, "42", k.nodes["/p/topics/t1/partitions/0/committed"].value)

	// existing znodes are never overwritten by skeleton creation
	k = newFakeKeeper(map[string]fakeKeeperNode{
		"/p":                        {},
		"/p/replicas":               {value: "keep"},
		"/p/topics":                 {value: "keep"},
		"/p/topics/t1":              {},
		"/p/topics/t1/partitions":   {value: "keep"},
		"/p/topic_partition_locks":  {value: "keep"},
		"/p/topics/t1/partitions/0": {},
	})
	_, err = applyKafkaKeeperOffsets(k, "/p", "default.kafka", legacyBackupNodes, []string{"t1"}, false, 26008000)
	require.NoError(t, err)
	for _, nodePath := range []string{"/p/replicas", "/p/topics", "/p/topics/t1/partitions", "/p/topic_partition_locks"} {
		assert.Equal(t, "keep", k.nodes[nodePath].value, nodePath)
	}
	assert.Equal(t, "42", k.nodes["/p/topics/t1/partitions/0/committed"].value)
}

func TestParseKafkaSettingAnchoredToKafkaEngine(t *testing.T) {
	// column DEFAULT before ENGINE and other engines shall not match
	query := `CREATE TABLE default.kafka (s String DEFAULT 'kafka_keeper_path = ''/wrong''') ENGINE = Kafka SETTINGS kafka_topic_list = 't1', kafka_keeper_path = '/right' COMMENT 'c'`
	keeperPath, ok := parseKafkaSetting(query, kafkaKeeperPathSetting)
	assert.True(t, ok)
	assert.Equal(t, "/right", keeperPath)
	mergeTreeQuery := `CREATE TABLE default.t (id UInt64) ENGINE = MergeTree ORDER BY id COMMENT 'kafka_keeper_path = ''/clickhouse/default/t'''`
	_, ok = parseKafkaSetting(mergeTreeQuery, kafkaKeeperPathSetting)
	assert.False(t, ok)
	assert.Equal(t, mergeTreeQuery, adjustKafkaKeeperPathForMapping(mergeTreeQuery, "default", "db2", "default.t"), "only Kafka engine queries shall be changed")
	assert.Equal(t, mergeTreeQuery, replaceKafkaSetting(mergeTreeQuery, kafkaKeeperPathSetting, func(string) string { return "x" }))
}

func TestKafkaDetachAttachErrorCodes(t *testing.T) {
	unknownTable := fmt.Errorf("code: 60, message: Table default.kafka does not exist")
	alreadyExists := fmt.Errorf("code: 57, message: Table default.kafka already exists")
	assert.True(t, isKafkaDetachedConcurrently(unknownTable, "Replicated"))
	assert.False(t, isKafkaDetachedConcurrently(unknownTable, "Atomic"), "only DETACH PERMANENTLY in a Replicated database is cluster-wide")
	assert.False(t, isKafkaDetachedConcurrently(alreadyExists, "Replicated"))
	assert.False(t, isKafkaDetachedConcurrently(fmt.Errorf("code: 600, message: x"), "Replicated"))
	assert.True(t, isKafkaAttachedConcurrently(alreadyExists))
	assert.False(t, isKafkaAttachedConcurrently(unknownTable))
	assert.False(t, isKafkaAttachedConcurrently(nil))
}

func TestRestoreKafkaKeeperOffsetsPinsUUIDWithoutNodes(t *testing.T) {
	// schema-only, embedded or old backups have no kafka_keeper metadata, but {uuid} shall be pinned anyway, CREATE without ON CLUSTER fails with code 36 otherwise
	b := &Backuper{cfg: &config.Config{}}
	var k *keeper.Keeper
	schema := &metadata.TableMetadata{Database: "default", Table: "kafka", Query: kafkaKeeperTestQuery}
	require.NoError(t, b.restoreKafkaKeeperOffsets(context.Background(), schema, "Atomic", kafkaKeeperTestVersion, &k))
	keeperPath, _ := parseKafkaSetting(schema.Query, kafkaKeeperPathSetting)
	assert.Equal(t, "/clickhouse/default/kafka/a1b2c3d4-0000-4000-8000-000000000001", keeperPath)
	assert.Nil(t, k, "keeper shall not be connected without kafka_keeper metadata")

	// ON CLUSTER resolves {uuid} by itself
	b.cfg.General.RestoreSchemaOnCluster = "cluster"
	schema = &metadata.TableMetadata{Database: "default", Table: "kafka", Query: kafkaKeeperTestQuery, KafkaKeeper: &metadata.KafkaKeeperMetadata{}}
	require.NoError(t, b.restoreKafkaKeeperOffsets(context.Background(), schema, "Atomic", kafkaKeeperTestVersion, &k))
	assert.Equal(t, kafkaKeeperTestQuery, schema.Query)
	assert.Nil(t, k)
}

func TestMarkKafkaKeeperRestored(t *testing.T) {
	restored := make(map[metadata.TableTitle]struct{})
	schema := &metadata.TableMetadata{Database: "default", Table: "kafka"}
	assert.True(t, markKafkaKeeperRestored(restored, schema))
	assert.False(t, markKafkaKeeperRestored(restored, schema), "CREATE retry shall not restore offsets again")
	assert.True(t, markKafkaKeeperRestored(restored, &metadata.TableMetadata{Database: "default", Table: "kafka2"}))
}

func TestPlaceKafkaKeeperHoldLocksCreateOnly(t *testing.T) {
	k := newFakeKeeper(map[string]fakeKeeperNode{
		"/p":                                 {},
		"/p/topic_partition_locks":           {},
		"/p/topic_partition_locks/t1_0.lock": {value: "r2", ephemeral: true},
		"/p/topic_partition_locks/t1_1.lock": {value: kafkaKeeperHoldOwner},
	})
	nodes := append(append([]metadata.KafkaKeeperNode{}, kafkaKeeperTestBackupNodes...), metadata.KafkaKeeperNode{Path: "topics/t1/partitions/2/committed", Value: "1"})
	heldLocks, err := placeKafkaKeeperHoldLocks(k, "/p", "default.kafka", nodes, kafkaKeeperTestVersion)
	require.NoError(t, err)
	assert.Equal(t, []string{"/p/topic_partition_locks/t1_1.lock", "/p/topic_partition_locks/t1_2.lock"}, heldLocks, "foreign lock shall be skipped, own lock reused")
	assert.Equal(t, fakeKeeperNode{value: "r2", ephemeral: true}, k.nodes["/p/topic_partition_locks/t1_0.lock"], "foreign lock shall not be overwritten")
	assert.Equal(t, kafkaKeeperHoldOwner, k.nodes["/p/topic_partition_locks/t1_2.lock"].value)
}
