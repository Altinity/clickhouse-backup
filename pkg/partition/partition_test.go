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
	idMap, nameList := ConvertPartitionsToIdsMapAndNamesList(context.Background(), nil, tables, nil, []string{"(202504,'2025-04-16')"})
	title := metadata.TableTitle{Database: "system", Table: "metric_log"}
	require.Empty(t, idMap[title])
	require.Empty(t, nameList[title])
}
