package clickhouse

import (
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
)

func TestCheckTypesConsistency(t *testing.T) {
	ch := ClickHouse{}
	table := &Table{
		Database: "mydb",
		Name:     "mytable",
	}
	expectedErr := errors.Errorf("`mydb`.`mytable` have inconsistent data types for active data part in system.parts_columns")

	testCases := []struct {
		Name            string
		PartColumnsData []ColumnDataTypes
		ExpectedError   error
	}{
		{
			Name:            "No partColumnsData",
			PartColumnsData: []ColumnDataTypes{},
			ExpectedError:   nil,
		},
		{
			Name: "Consistent data types",
			PartColumnsData: []ColumnDataTypes{
				{
					Column: "agg_col",
					Types:  []string{"AggregateFunction(1, sumMap, Array(UInt16), Array(UInt64))", "AggregateFunction(sumMap, Array(UInt16), Array(UInt64))"},
				},
				{
					Column: "simple_agg_col",
					Types:  []string{"SimpleAggregateFunction(1, sum, UInt16)", "SimpleAggregateFunction(sum, UInt16)"},
				},

				{
					Column: "col3",
					Types:  []string{"Nullable(Int32)", "Int32"},
				},
				{
					Column: "col4",
					Types:  []string{"LowCardinality(String)", "String"},
				},
				{
					Column: "col5",
					Types:  []string{"DateTime", "DateTime('Meteor/Chelyabinsk')"},
				},
				{
					Column: "col6",
					Types:  []string{"LowCardinality(Nullable(String))", "String"},
				},
			},
			ExpectedError: nil,
		},
		{
			Name: "Inconsistent data types",
			PartColumnsData: []ColumnDataTypes{
				{
					Column: "col1",
					Types:  []string{"Int32", "String"},
				},
			},
			ExpectedError: expectedErr,
		},
		{
			Name: "Inconsistent AggregateFunction",
			PartColumnsData: []ColumnDataTypes{
				{
					Column: "agg_col",
					Types:  []string{"AggregateFunction(1, avg, Array(UInt16), Array(UInt64))", "AggregateFunction(sumMap, Array(UInt16), Array(UInt64))"},
				},
			},
			ExpectedError: expectedErr,
		},
		{
			Name: "Inconsistent SimpleAggregateFunction",
			PartColumnsData: []ColumnDataTypes{
				{
					Column: "simple_agg_col",
					Types:  []string{"SimpleAggregateFunction(1, sum, UInt16)", "SimpleAggregateFunction(sumMap, Array(UInt16))"},
				},
			},
			ExpectedError: expectedErr,
		},
		{
			Name: "Inconsistent Types #2",
			PartColumnsData: []ColumnDataTypes{
				{
					Column: "col2",
					Types:  []string{"DateTime(6)", "Date"},
				},
			},
			ExpectedError: expectedErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			err := ch.CheckTypesConsistency(table, tc.PartColumnsData)
			if tc.ExpectedError == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.ExpectedError.Error())
			}
		})
	}
}

func TestExtractStoragePolicy(t *testing.T) {
	ch := ClickHouse{}

	testCases := map[string]string{
		"CREATE TABLE `_test.ДБ_atomic__TestIntegrationS3`.test_s3_TestIntegrationS3 UUID '8135780b-0c9a-46a7-94fd-2aebb701eff6' (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/_test.ДБ_atomic__TestIntegrationS3/test_s3_TestIntegrationS3', '{replica}') ORDER BY id SETTINGS storage_policy = 's3_only', index_granularity = 8192": "s3_only",
		"CREATE TABLE test2 SETTINGS storage_policy = 'default'": "default",
		"CREATE TABLE test3": "default",
	}
	for query, policy := range testCases {
		assert.Equal(t, policy, ch.ExtractStoragePolicy(query))
	}
}

func TestEnrichQueryWithOnCluster(t *testing.T) {
	ch := ClickHouse{}

	testCases := []struct {
		Name          string
		Query         string
		OnCluster     string
		Version       int
		ExpectedQuery string
	}{
		{
			Name:          "No OnCluster provided, version < 19000000",
			Query:         "CREATE TABLE test (id UInt64) ENGINE = MergeTree ORDER BY id",
			OnCluster:     "my_cluster",
			Version:       19000000,
			ExpectedQuery: "CREATE TABLE test (id UInt64) ENGINE = MergeTree ORDER BY id",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, CREATE TABLE",
			Query:         "CREATE TABLE test (id UInt64) ENGINE = MergeTree ORDER BY id",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "CREATE TABLE test  ON CLUSTER 'my_cluster' (id UInt64) ENGINE = MergeTree ORDER BY id",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, CREATE MATERIALIZED VIEW with TO clause",
			Query:         "CREATE MATERIALIZED VIEW test_view TO test_table AS SELECT * FROM test_table",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "CREATE MATERIALIZED VIEW test_view ON CLUSTER 'my_cluster'  TO test_table AS SELECT * FROM test_table",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, CREATE VIEW with AS SELECT",
			Query:         "CREATE VIEW test_view AS SELECT * FROM test_table",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "CREATE VIEW test_view ON CLUSTER 'my_cluster'  AS SELECT * FROM test_table",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, CREATE MATERIALIZED VIEW with ENGINE clause",
			Query:         "CREATE MATERIALIZED VIEW test_view ENGINE = AggregatingMergeTree() ORDER BY t AS SELECT * FROM test_table",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "CREATE MATERIALIZED VIEW test_view ON CLUSTER 'my_cluster'  ENGINE = AggregatingMergeTree() ORDER BY t AS SELECT * FROM test_table",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, ATTACH VIEW with AS SELECT",
			Query:         "ATTACH VIEW test_view AS SELECT * FROM test_table",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "ATTACH VIEW test_view ON CLUSTER 'my_cluster'  AS SELECT * FROM test_table",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, ATTACH LIVE VIEW with columns",
			Query:         "ATTACH LIVE VIEW test.daily_sales_live UUID '8190d585-1111-2222-3333-444444444444' (`event_date` UInt32, `sku_id` String, `total_sales` Decimal(38, 2)) AS SELECT event_date FROM test.sales",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "ATTACH LIVE VIEW test.daily_sales_live UUID '8190d585-1111-2222-3333-444444444444' ON CLUSTER 'my_cluster' (`event_date` UInt32, `sku_id` String, `total_sales` Decimal(38, 2)) AS SELECT event_date FROM test.sales",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, ATTACH WINDOW VIEW with engine and settings",
			Query:         "ATTACH WINDOW VIEW test.wv UUID '6b87827c-1111-2222-3333-444444444444' ENGINE AggregatingMergeTree() ORDER BY total SETTINGS index_granularity = 8192 AS SELECT total FROM test.src",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "ATTACH WINDOW VIEW test.wv UUID '6b87827c-1111-2222-3333-444444444444' ON CLUSTER 'my_cluster'  ENGINE AggregatingMergeTree() ORDER BY total SETTINGS index_granularity = 8192 AS SELECT total FROM test.src",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, CREATE VIEW with columns",
			Query:         "CREATE VIEW test_view (`id` UInt64) AS SELECT id FROM test_table",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "CREATE VIEW test_view ON CLUSTER 'my_cluster' (`id` UInt64) AS SELECT id FROM test_table",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, ATTACH VIEW without columns regression",
			Query:         "ATTACH VIEW test_view AS SELECT id FROM test_table",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "ATTACH VIEW test_view ON CLUSTER 'my_cluster'  AS SELECT id FROM test_table",
		},
		{
			Name:          "OnCluster already present leaves query unchanged",
			Query:         "ATTACH VIEW test_view ON CLUSTER 'my_cluster' AS SELECT * FROM test_table",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "ATTACH VIEW test_view ON CLUSTER 'my_cluster' AS SELECT * FROM test_table",
		},
		{
			Name:          "Version before ON CLUSTER support leaves ATTACH LIVE VIEW unchanged",
			Query:         "ATTACH LIVE VIEW test.daily_sales_live UUID '8190d585-1111-2222-3333-444444444444' (`event_date` UInt32, `sku_id` String, `total_sales` Decimal(38, 2)) AS SELECT event_date FROM test.sales",
			OnCluster:     "my_cluster",
			Version:       18999999,
			ExpectedQuery: "ATTACH LIVE VIEW test.daily_sales_live UUID '8190d585-1111-2222-3333-444444444444' (`event_date` UInt32, `sku_id` String, `total_sales` Decimal(38, 2)) AS SELECT event_date FROM test.sales",
		},
		{
			Name:          "Empty OnCluster leaves ATTACH WINDOW VIEW unchanged",
			Query:         "ATTACH WINDOW VIEW test.wv UUID '6b87827c-1111-2222-3333-444444444444' (`total` Decimal(38, 2), `w_start` DateTime) ENGINE = MergeTree ORDER BY total SETTINGS index_granularity = 8192 AS SELECT total FROM test.src",
			OnCluster:     "",
			Version:       19000001,
			ExpectedQuery: "ATTACH WINDOW VIEW test.wv UUID '6b87827c-1111-2222-3333-444444444444' (`total` Decimal(38, 2), `w_start` DateTime) ENGINE = MergeTree ORDER BY total SETTINGS index_granularity = 8192 AS SELECT total FROM test.src",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, CREATE DICTIONARY",
			Query:         "CREATE DICTIONARY test_dict (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(HOST 'localhost')) LAYOUT(HASHED())",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "CREATE DICTIONARY test_dict  ON CLUSTER 'my_cluster' (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(HOST 'localhost')) LAYOUT(HASHED())",
		},
		{
			Name:          "OnCluster provided, version >= 19000000, CREATE VIEW ...AS WITH AS..., https://github.com/Altinity/clickhouse-backup/issues/1075",
			Query:         "CREATE VIEW feature_data.insert_price_history_it_az UUID '12a40994-ff52-4a14-8f0b-bccdf1a3a4ba' AS WITH a AS (SELECT {input_timestamp:DateTime} AS entity_timestamp, concat(it.provider, '_', z.name, '_', it.instance_type) AS entity_id_cloud_az_it, it.provider AS cloud, z.name AS availability_zone, it.instance_type AS instance_type, ph.is_spot AS is_spot, CAST(ph.price, 'float') AS price, now() AS created_timestamp, row_number() OVER (PARTITION BY entity_id_cloud_az_it ORDER BY ph.effective_from DESC) AS rn FROM raw_data.price_history AS ph INNER JOIN raw_data.zones AS z ON z.id = ph.availability_zone INNER JOIN raw_data.instance_types AS it ON it.id = ph.instance_type WHERE (ph.effective_to < {input_timestamp:DateTime}) AND z.latest_value AND it.latest_value) SELECT entity_timestamp, entity_id_cloud_az_it, cloud, availability_zone, instance_type, is_spot, price, created_timestamp FROM a WHERE rn = 1 SETTINGS final = 1",
			OnCluster:     "my_cluster",
			Version:       19000001,
			ExpectedQuery: "CREATE VIEW feature_data.insert_price_history_it_az UUID '12a40994-ff52-4a14-8f0b-bccdf1a3a4ba' ON CLUSTER 'my_cluster'  AS WITH a AS (SELECT {input_timestamp:DateTime} AS entity_timestamp, concat(it.provider, '_', z.name, '_', it.instance_type) AS entity_id_cloud_az_it, it.provider AS cloud, z.name AS availability_zone, it.instance_type AS instance_type, ph.is_spot AS is_spot, CAST(ph.price, 'float') AS price, now() AS created_timestamp, row_number() OVER (PARTITION BY entity_id_cloud_az_it ORDER BY ph.effective_from DESC) AS rn FROM raw_data.price_history AS ph INNER JOIN raw_data.zones AS z ON z.id = ph.availability_zone INNER JOIN raw_data.instance_types AS it ON it.id = ph.instance_type WHERE (ph.effective_to < {input_timestamp:DateTime}) AND z.latest_value AND it.latest_value) SELECT entity_timestamp, entity_id_cloud_az_it, cloud, availability_zone, instance_type, is_spot, price, created_timestamp FROM a WHERE rn = 1 SETTINGS final = 1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			result := ch.enrichQueryWithOnCluster(tc.Query, tc.OnCluster, tc.Version, "Atomic")
			assert.Equal(t, tc.ExpectedQuery, result)
		})
	}
}

// TestGroupMutationsByTable covers the per-backup batch mutation lookup that replaced the
// per-table system.mutations query (O(N^2) fix). Verifies that mutations from a single
// server-wide scan are bucketed to the correct database.table and never leak across tables.
func TestGroupMutationsByTable(t *testing.T) {
	rows := []inProgressMutationRow{
		{Database: "db1", Table: "t1", MutationId: "0000000001", Command: "MODIFY COLUMN a UInt64"},
		{Database: "db1", Table: "t1", MutationId: "0000000002", Command: "DROP COLUMN b"},
		{Database: "db1", Table: "t2", MutationId: "0000000003", Command: "MODIFY COLUMN c String"},
	}

	got := groupMutationsByTable(rows)

	assert.Len(t, got, 2, "two distinct tables expected")
	assert.Equal(t, []metadata.MutationMetadata{
		{MutationId: "0000000001", Command: "MODIFY COLUMN a UInt64"},
		{MutationId: "0000000002", Command: "DROP COLUMN b"},
	}, got[metadata.TableTitle{Database: "db1", Table: "t1"}], "t1 must keep both of its mutations, in order")
	assert.Equal(t, []metadata.MutationMetadata{
		{MutationId: "0000000003", Command: "MODIFY COLUMN c String"},
	}, got[metadata.TableTitle{Database: "db1", Table: "t2"}], "t2 must get only its own mutation (no cross-table leak)")
}

// TestGroupMutationsByTableDottedNames pins the reason for the metadata.TableTitle struct key:
// dots are legal in database/table names, so a "database.table" string key would collapse
// {db="a.b", table="c"} and {db="a", table="b.c"} into the same "a.b.c" bucket. The struct key
// keeps them separate.
func TestGroupMutationsByTableDottedNames(t *testing.T) {
	rows := []inProgressMutationRow{
		{Database: "a.b", Table: "c", MutationId: "0000000001", Command: "DROP COLUMN x"},
		{Database: "a", Table: "b.c", MutationId: "0000000002", Command: "DROP COLUMN y"},
	}

	got := groupMutationsByTable(rows)

	assert.Len(t, got, 2, "ambiguous string key would have merged these into one bucket")
	assert.Equal(t, []metadata.MutationMetadata{
		{MutationId: "0000000001", Command: "DROP COLUMN x"},
	}, got[metadata.TableTitle{Database: "a.b", Table: "c"}])
	assert.Equal(t, []metadata.MutationMetadata{
		{MutationId: "0000000002", Command: "DROP COLUMN y"},
	}, got[metadata.TableTitle{Database: "a", Table: "b.c"}])
}

func TestGroupMutationsByTableEmpty(t *testing.T) {
	assert.Empty(t, groupMutationsByTable(nil))
}
