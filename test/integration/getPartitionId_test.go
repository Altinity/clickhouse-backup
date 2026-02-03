//go:build integration

package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/partition"
	"github.com/stretchr/testify/assert"
)

func TestGetPartitionId(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") == -1 {
		t.Skipf("Test skipped, is_in_partition_key not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	type testData struct {
		CreateTableSQL string
		Database       string
		Table          string
		Partition      string
		ExpectedId     string
		ExpectedName   string
	}
	testCases := []testData{
		{
			"CREATE TABLE default.test_part_id_1 UUID 'b45e751f-6c06-42a3-ab4a-f5bb9ac3716e' (dt Date, version DateTime, category String, name String) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}','{replica}',version) ORDER BY dt PARTITION BY (toYYYYMM(dt),category)",
			"default",
			"test_part_id_1",
			"('2023-01-01','category1')",
			"cc1ad6ede2e7f708f147e132cac7a590",
			"(202301,'category1')",
		},
		{
			"CREATE TABLE default.test_part_id_2 (dt Date, version DateTime, name String) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}','{replica}',version) ORDER BY dt PARTITION BY toYYYYMM(dt)",
			"default",
			"test_part_id_2",
			"'2023-01-01'",
			"202301",
			"202301",
		},
		{
			"CREATE TABLE default.test_part_id_3 ON CLUSTER '{cluster}' (i UInt32, name String) ENGINE = ReplicatedMergeTree() ORDER BY i PARTITION BY i",
			"default",
			"test_part_id_3",
			"202301",
			"202301",
			"202301",
		},
		{
			"CREATE TABLE default.test_part_id_4 (dt String, name String) ENGINE = MergeTree ORDER BY dt PARTITION BY dt",
			"default",
			"test_part_id_4",
			"'2023-01-01'",
			"c487903ebbb25a533634d6ec3485e3a9",
			"2023-01-01",
		},
		{
			"CREATE TABLE default.test_part_id_5 (dt String, name String) ENGINE = Memory",
			"default",
			"test_part_id_5",
			"'2023-01-01'",
			"",
			"",
		},
		{
			// Test case for pre-evaluated partition values (ClickHouse 21.8+)
			// User provides already-computed partition values from system.parts
			"CREATE TABLE default.test_part_id_6 (dt Date, user_id UInt32, name String) ENGINE = MergeTree ORDER BY dt PARTITION BY (toYYYYMM(dt), user_id % 12)",
			"default",
			"test_part_id_6",
			"(202601,1)", // Pre-evaluated values: toYYYYMM result and modulo result
			"202601-1",   // Expected partition_id for (202601, 1)
			"(202601,1)", // Expected partition name
		},
		{
			// Test case for LowCardinality(String) field
			"CREATE TABLE default.test_part_id_7 (dt Date, status LowCardinality(String), name String) ENGINE = MergeTree ORDER BY dt PARTITION BY status",
			"default",
			"test_part_id_7",
			"'active'",
			"871e1f9034cd074d71ed2545c1691db0",
			"active",
		},
		{
			// Test case for intHash64 function in PARTITION BY (raw value)
			"CREATE TABLE default.test_part_id_8 (dt Date, user_name String, value UInt32) ENGINE = MergeTree ORDER BY dt PARTITION BY intHash64(user_name)",
			"default",
			"test_part_id_8",
			"'john_doe'",
			"8cc4ef9ac9147c0663072caac7a64601",
			"john_doe",
		},
		{
			// Test case for intHash64 with pre-evaluated partition value (no quotes)
			"CREATE TABLE default.test_part_id_9 (dt Date, user_name String, value UInt32) ENGINE = MergeTree ORDER BY dt PARTITION BY intHash64(user_name)",
			"default",
			"test_part_id_9",
			"john_doe", // Pre-evaluated: pass partition value directly from system.parts
			"8cc4ef9ac9147c0663072caac7a64601",
			"john_doe",
		},
	}
	if isAtomicOrReplicated, _ := env.ch.IsDbAtomicOrReplicated("default"); !isAtomicOrReplicated {
		testCases[0].CreateTableSQL = strings.Replace(testCases[0].CreateTableSQL, "UUID 'b45e751f-6c06-42a3-ab4a-f5bb9ac3716e'", "", 1)
	}
	for _, tc := range testCases {
		// Skip new test cases for ClickHouse < 21.8
		if (tc.Table == "test_part_id_6" || tc.Table == "test_part_id_7" || tc.Table == "test_part_id_8" || tc.Table == "test_part_id_9") &&
			compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") < 0 {
			t.Logf("Skipping %s.%s - test requires ClickHouse 21.8+", tc.Database, tc.Table)
			continue
		}

		partitionId, partitionName, err := partition.GetPartitionIdAndName(t.Context(), env.ch, tc.Database, tc.Table, tc.CreateTableSQL, tc.Partition)
		assert.NoError(t, err)
		if tc.ExpectedId != "" {
			assert.Equal(t, tc.ExpectedId, partitionId)
		} else {
			t.Logf("Test %s.%s with partition %s: partitionId=%s, partitionName=%s", tc.Database, tc.Table, tc.Partition, partitionId, partitionName)
		}
		if tc.ExpectedName != "" {
			assert.Equal(t, tc.ExpectedName, partitionName)
		}
	}
	env.Cleanup(t, r)
}
