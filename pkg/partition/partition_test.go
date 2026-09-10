package partition

import (
	"context"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/stretchr/testify/require"
)

// https://github.com/Altinity/clickhouse-backup/issues/1547
// skipped tables must not reach GetPartitionIdAndName (ch == nil would panic there)
func TestConvertPartitionsToIdsMapAndNamesListSkipTables(t *testing.T) {
	tables := []clickhouse.Table{
		{
			Database:         "system",
			Name:             "metric_log",
			Skip:             true,
			CreateTableQuery: "CREATE TABLE system.metric_log (`event_date` Date, `event_time` DateTime) ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time)",
		},
	}
	idMap, nameList, err := ConvertPartitionsToIdsMapAndNamesList(context.Background(), nil, tables, nil, []string{"(202504,'2025-04-16')"})
	require.NoError(t, err)
	title := metadata.TableTitle{Database: "system", Table: "metric_log"}
	require.Empty(t, idMap[title])
	require.Empty(t, nameList[title])
}

func TestExtractPartitionByExpr(t *testing.T) {
	require.Equal(t, "(year_month, toString(event_date))", ExtractPartitionByExpr("CREATE TABLE vs.t (`a` Int64) ENGINE = MergeTree PARTITION BY (year_month, toString(event_date)) ORDER BY (a, b, c) SETTINGS index_granularity = 8192"))
	require.Equal(t, "toYYYYMM(event_date)", ExtractPartitionByExpr("CREATE TABLE vs.t (`a` Int64) ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY a"))
	require.Equal(t, "", ExtractPartitionByExpr("CREATE TABLE vs.t (`a` Int64) ENGINE = MergeTree ORDER BY a"))
}
