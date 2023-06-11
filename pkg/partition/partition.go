package partition

import (
	"context"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/google/uuid"
	"regexp"
	"strconv"
	"strings"
)

// https://regex101.com/r/k4Zxs9/1
var replicatedMergeTreeRE = regexp.MustCompile(`(?m)Replicated(MergeTree|ReplacingMergeTree|SummingMergeTree|AggregatingMergeTree|CollapsingMergeTree|VersionedCollapsingMergeTree|GraphiteMergeTree)\s*\(('[^']+'\s*,\s*'[^']+'(\s*,\s*|.*?)([^)]*)|)\)(.*)`)
var uuidRE = regexp.MustCompile(`UUID '[^']+'`)

func splitAndParsePartition(partition string) []interface{} {
	values := strings.Split(partition, ",")
	parsedValues := make([]interface{}, len(values))
	for i, v := range values {
		if strings.HasPrefix(v, "(") {
			v = strings.TrimPrefix(v, "(")
		}
		if strings.HasSuffix(v, ")") {
			v = strings.TrimPrefix(v, ")")
		}
		v = strings.TrimSpace(v)
		if strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'") {
			v = strings.TrimSuffix(strings.TrimPrefix(v, "'"), "'")
		}
		if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
			parsedValues[i] = intVal
		} else if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
			parsedValues[i] = floatVal
		} else {
			parsedValues[i] = v
		}
	}
	return parsedValues
}

func GetPartitionId(ctx context.Context, ch *clickhouse.ClickHouse, database, table, createQuery string, partition string) (error, string) {
	if !strings.Contains(createQuery, "MergeTree") {
		return nil, ""
	}
	sql := replicatedMergeTreeRE.ReplaceAllString(createQuery, "$1($4)$5")
	if len(uuidRE.FindAllString(sql, -1)) > 0 {
		newUUID, _ := uuid.NewUUID()
		sql = uuidRE.ReplaceAllString(sql, fmt.Sprintf("UUID '%s'", newUUID.String()))
	}
	dbAndTableNameRE := regexp.MustCompile(
		fmt.Sprintf("%s.%s|`%s`.%s|%s.`%s`|`%s`.`%s`",
			regexp.QuoteMeta(database), regexp.QuoteMeta(table),
			regexp.QuoteMeta(database), regexp.QuoteMeta(table),
			regexp.QuoteMeta(database), regexp.QuoteMeta(table),
			regexp.QuoteMeta(database), regexp.QuoteMeta(table),
		))
	partitionIdTable := "__partition_id_" + table
	sql = dbAndTableNameRE.ReplaceAllString(sql, fmt.Sprintf("`%s`.`%s`", database, partitionIdTable))
	if err := ch.Query(sql); err != nil {
		return err, ""
	}
	columns := make([]struct {
		Name string `ch:"name"`
	}, 0)
	sql = "SELECT name FROM system.columns WHERE database=? AND table=? AND is_in_partition_key"
	if err := ch.SelectContext(ctx, &columns, sql, database, partitionIdTable); err != nil {
		if err = dropPartitionIdTable(ch, database, partitionIdTable); err != nil {
			return err, ""
		}
		return fmt.Errorf("can't get is_in_partition_key column names from for table `%s`.`%s`: %v", database, partitionIdTable, err), ""
	}
	if len(columns) == 0 {
		return fmt.Errorf("is_in_partition_key=1 fields not found in system.columns for table `%s`.`%s`", database, partitionIdTable), ""
	}
	partitionInsert := splitAndParsePartition(partition)
	columnNames := make([]string, len(columns))
	for i, c := range columns {
		columnNames[i] = c.Name
	}

	sql = fmt.Sprintf(
		"INSERT INTO `%s`.`%s`(`%s`) VALUES (%s)",
		database, partitionIdTable, strings.Join(columnNames, "`,`"),
		strings.TrimSuffix(strings.Repeat("?,", len(partitionInsert)), ","),
	)
	err := ch.QueryContext(ctx, sql, partitionInsert...)
	if err != nil {
		return err, ""
	}
	partitionIds := make([]struct {
		Id string `ch:"partition_id"`
	}, 0)
	sql = "SELECT partition_id FROM system.parts WHERE active AND database=? AND table=?"
	if err = ch.SelectContext(ctx, &partitionIds, sql, database, partitionIdTable); err != nil {
		return fmt.Errorf("can't SELECT partition_id for PARTITION BY fields(%#v) FROM `%s`.`%s`: %v", partitionInsert, database, partitionIdTable, err), ""
	}
	if len(partitionIds) != 1 {
		return fmt.Errorf("wrong partitionsIds=%#v found system.parts for table `%s`.`%s`", partitionIds, database, partitionIdTable), ""
	}

	if err = dropPartitionIdTable(ch, database, partitionIdTable); err != nil {
		return err, ""
	}
	return nil, partitionIds[0].Id
}

func dropPartitionIdTable(ch *clickhouse.ClickHouse, database string, partitionIdTable string) error {
	sql := fmt.Sprintf("DROP TABLE `%s`.`%s`", database, partitionIdTable)
	if isAtomic, err := ch.IsAtomic(database); isAtomic {
		sql += " SYNC"
	} else if err != nil {
		return err
	}
	if err := ch.Query(sql); err != nil {
		return err
	}
	return nil
}
