package clickhouse

import (
	"fmt"
	apexLog "github.com/apex/log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckTypesConsistency(t *testing.T) {
	ch := ClickHouse{
		Log: apexLog.WithField("logger", "test"),
	}
	table := &Table{
		Database: "mydb",
		Name:     "mytable",
	}
	expectedErr := fmt.Errorf("`mydb`.`mytable` have inconsistent data types for active data part in system.parts_columns")

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
			assert.Equal(t, tc.ExpectedError, err)
		})
	}
}

func TestExtractStoragePolicy(t *testing.T) {
	ch := ClickHouse{
		Log: apexLog.WithField("logger", "test"),
	}

	testCases := map[string]string{
		"CREATE TABLE `_test.ДБ_atomic__TestIntegrationS3`.test_s3_TestIntegrationS3 UUID '8135780b-0c9a-46a7-94fd-2aebb701eff6' (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/_test.ДБ_atomic__TestIntegrationS3/test_s3_TestIntegrationS3', '{replica}') ORDER BY id SETTINGS storage_policy = 's3_only', index_granularity = 8192": "s3_only",
		"CREATE TABLE test2 SETTINGS storage_policy = 'default'": "default",
		"CREATE TABLE test3": "default",
	}
	for query, policy := range testCases {
		assert.Equal(t, policy, ch.ExtractStoragePolicy(query))
	}
}
