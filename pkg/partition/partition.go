package partition

import (
	"context"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/common"
	"github.com/Altinity/clickhouse-backup/pkg/metadata"
	apexLog "github.com/apex/log"
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
		v = strings.TrimSpace(v)
		if strings.HasPrefix(v, "(") {
			v = strings.TrimPrefix(v, "(")
		}
		if strings.HasSuffix(v, ")") {
			v = strings.TrimSuffix(v, ")")
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

var PartitionByRE = regexp.MustCompile(`(?mi)\s*PARTITION BY\s*(.+)`)
var SettingsRE = regexp.MustCompile(`(?mi)\s*SETTINGS.*`)
var OrderByRE = regexp.MustCompile(`(?mi)\s*ORDER BY.*`)
var FunctionsRE = regexp.MustCompile(`(?i)\w+\(`)
var StringsRE = regexp.MustCompile(`(?i)'[^']+'`)
var SpecialCharsRE = regexp.MustCompile(`(?i)[)*+\-/\\]+`)
var FieldsNamesRE = regexp.MustCompile("(?i)\\w+|`[^`]+`\\.`[^`]+`|\"[^\"]+\"")

func extractPartitionByFieldNames(s string) []struct {
	Name string `ch:"name"`
} {
	s = SettingsRE.ReplaceAllString(s, "")
	s = OrderByRE.ReplaceAllString(s, "")
	s = FunctionsRE.ReplaceAllString(s, "")
	s = StringsRE.ReplaceAllString(s, "")
	s = SpecialCharsRE.ReplaceAllString(s, "")
	matches := FieldsNamesRE.FindStringSubmatch(s)
	columns := make([]struct {
		Name string `ch:"name"`
	}, len(matches))
	for i := range matches {
		columns[i].Name = matches[i]
	}
	return columns
}

func GetPartitionIdAndName(ctx context.Context, ch *clickhouse.ClickHouse, database, table, createQuery string, partition string) (error, string, string) {
	if !strings.Contains(createQuery, "MergeTree") || !PartitionByRE.MatchString(createQuery) {
		return nil, "", ""
	}
	createQuery = replicatedMergeTreeRE.ReplaceAllString(createQuery, "$1($4)$5")
	if len(uuidRE.FindAllString(createQuery, -1)) > 0 {
		newUUID, _ := uuid.NewUUID()
		createQuery = uuidRE.ReplaceAllString(createQuery, fmt.Sprintf("UUID '%s'", newUUID.String()))
	}
	dbAndTableNameRE := regexp.MustCompile(
		fmt.Sprintf("%s.%s|`%s`.%s|%s.`%s`|`%s`.`%s`",
			regexp.QuoteMeta(database), regexp.QuoteMeta(table),
			regexp.QuoteMeta(database), regexp.QuoteMeta(table),
			regexp.QuoteMeta(database), regexp.QuoteMeta(table),
			regexp.QuoteMeta(database), regexp.QuoteMeta(table),
		))
	partitionIdTable := "__partition_id_" + table
	createQuery = dbAndTableNameRE.ReplaceAllString(createQuery, fmt.Sprintf("`%s`.`%s`", database, partitionIdTable))
	if err := ch.Query(createQuery); err != nil {
		return err, "", ""
	}
	columns := make([]struct {
		Name string `ch:"name"`
	}, 0)
	sql := "SELECT name FROM system.columns WHERE database=? AND table=? AND is_in_partition_key"
	oldVersion := false
	if err := ch.SelectContext(ctx, &columns, sql, database, partitionIdTable); err != nil {
		matches := PartitionByRE.FindStringSubmatch(createQuery)
		if len(matches) == 0 {
			if dropErr := dropPartitionIdTable(ch, database, partitionIdTable); dropErr != nil {
				return dropErr, "", ""
			}
			return fmt.Errorf("can't get is_in_partition_key column names from for table `%s`.`%s`: %v", database, partitionIdTable, err), "", ""
		}
		columns = extractPartitionByFieldNames(matches[1])
		oldVersion = true
	}
	defer func() {
		if dropErr := dropPartitionIdTable(ch, database, partitionIdTable); dropErr != nil {
			apexLog.Warnf("partition.GetPartitionId can't drop `%s`.`%s`: %v", database, partitionIdTable, dropErr)
		}
	}()
	if len(columns) == 0 {
		apexLog.Warnf("is_in_partition_key=1 fields not found in system.columns for table `%s`.`%s`", database, partitionIdTable)
		return nil, "", ""
	}
	partitionInsert := splitAndParsePartition(partition)
	columnNames := make([]string, len(columns))
	for i, c := range columns {
		columnNames[i] = c.Name
	}
	var err error
	if !oldVersion {
		sql = fmt.Sprintf(
			"INSERT INTO `%s`.`%s`(`%s`) VALUES (%s)",
			database, partitionIdTable, strings.Join(columnNames, "`,`"),
			strings.TrimSuffix(strings.Repeat("?,", len(partitionInsert)), ","),
		)
		err = ch.QueryContext(ctx, sql, partitionInsert...)
	} else {
		sql = fmt.Sprintf(
			"INSERT INTO `%s`.`%s`(`%s`)",
			database, partitionIdTable, strings.Join(columnNames, "`,`"),
		)
		batch, err := ch.GetConn().PrepareBatch(ctx, sql)
		if err != nil {
			return err, "", ""
		}
		if err = batch.Append(partitionInsert...); err != nil {
			return err, "", ""
		}
		if err = batch.Send(); err != nil {
			return err, "", ""
		}
	}
	if err != nil {
		return err, "", ""
	}
	partitions := make([]struct {
		Id   string `ch:"partition_id"`
		Name string `ch:"partition"`
	}, 0)
	sql = "SELECT partition_id, partition FROM system.parts WHERE active AND database=? AND table=?"
	if err = ch.SelectContext(ctx, &partitions, sql, database, partitionIdTable); err != nil {
		return fmt.Errorf("can't SELECT partition_id for PARTITION BY fields(%#v) FROM `%s`.`%s`: %v", partitionInsert, database, partitionIdTable, err), "", ""
	}
	if len(partitions) != 1 {
		return fmt.Errorf("wrong partitionsIds=%#v found system.parts for table `%s`.`%s`", partitions, database, partitionIdTable), "", ""
	}

	return nil, partitions[0].Id, partitions[0].Name
}

func dropPartitionIdTable(ch *clickhouse.ClickHouse, database string, partitionIdTable string) error {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", database, partitionIdTable)
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

var partitionTupleRE = regexp.MustCompile(`\)\s*,\s*\(`)

// ConvertPartitionsToIdsMapAndNamesList - get partitions from CLI/API params and convert it for NameList and IdMap for each table
func ConvertPartitionsToIdsMapAndNamesList(ctx context.Context, ch *clickhouse.ClickHouse, tablesFromClickHouse []clickhouse.Table, tablesFromMetadata []metadata.TableMetadata, partitions []string) (map[metadata.TableTitle]common.EmptyMap, map[metadata.TableTitle][]string) {
	partitionsIdMap := map[metadata.TableTitle]common.EmptyMap{}
	partitionsNameList := map[metadata.TableTitle][]string{}
	if len(partitions) == 0 {
		for _, t := range tablesFromClickHouse {
			createIdMapAndNameListIfNotExists(t.Database, t.Name, partitionsIdMap, partitionsNameList)
		}
		for _, t := range tablesFromMetadata {
			createIdMapAndNameListIfNotExists(t.Database, t.Table, partitionsIdMap, partitionsNameList)
		}
		return partitionsIdMap, partitionsNameList
	}

	// to allow use --partitions val1 --partitions val2, https://github.com/Altinity/clickhouse-backup/issues/425#issuecomment-1149855063
	for _, partitionArg := range partitions {
		partitionArg = strings.Trim(partitionArg, " \t")
		// when PARTITION BY clause return partition_id field as hash, https://github.com/Altinity/clickhouse-backup/issues/602
		if strings.HasPrefix(partitionArg, "(") {
			partitionArg = strings.TrimSuffix(strings.TrimPrefix(partitionArg, "("), ")")
			for _, partitionTuple := range partitionTupleRE.Split(partitionArg, -1) {
				for _, t := range tablesFromClickHouse {
					createIdMapAndNameListIfNotExists(t.Database, t.Name, partitionsIdMap, partitionsNameList)
					if err, partitionId, partitionName := GetPartitionIdAndName(ctx, ch, t.Database, t.Name, t.CreateTableQuery, partitionTuple); err != nil {
						apexLog.Fatalf("partition.GetPartitionIdAndName error: %v", err)
					} else if partitionId != "" {
						addItemToIdMapAndNameListIfNotExists(partitionId, partitionName, t.Database, t.Name, partitionsIdMap, partitionsNameList)
					}
				}
				for _, t := range tablesFromMetadata {
					createIdMapAndNameListIfNotExists(t.Database, t.Table, partitionsIdMap, partitionsNameList)
					if err, partitionId, partitionName := GetPartitionIdAndName(ctx, ch, t.Database, t.Table, t.Query, partitionTuple); err != nil {
						apexLog.Fatalf("partition.GetPartitionIdAndName error: %v", err)
					} else if partitionId != "" {
						addItemToIdMapAndNameListIfNotExists(partitionId, partitionName, t.Database, t.Table, partitionsIdMap, partitionsNameList)
					}
				}
			}
		} else {
			// when partitionId == partitionName
			for _, item := range strings.Split(partitionArg, ",") {
				item = strings.Trim(item, " \t")
				for _, t := range tablesFromClickHouse {
					createIdMapAndNameListIfNotExists(t.Database, t.Name, partitionsIdMap, partitionsNameList)
					addItemToIdMapAndNameListIfNotExists(item, item, t.Database, t.Name, partitionsIdMap, partitionsNameList)
				}
				for _, t := range tablesFromMetadata {
					createIdMapAndNameListIfNotExists(t.Database, t.Table, partitionsIdMap, partitionsNameList)
					addItemToIdMapAndNameListIfNotExists(item, item, t.Database, t.Table, partitionsIdMap, partitionsNameList)
				}
			}
		}
	}
	return partitionsIdMap, partitionsNameList
}

func addItemToIdMapAndNameListIfNotExists(partitionId, partitionName, database string, table string, partitionsIdMap map[metadata.TableTitle]common.EmptyMap, partitionsNameList map[metadata.TableTitle][]string) {
	if partitionId != "" {
		partitionsIdMap[metadata.TableTitle{
			Database: database, Table: table,
		}][partitionId] = struct{}{}
	}
	if partitionName != "" {
		partitionsNameList[metadata.TableTitle{
			Database: database, Table: table,
		}] = common.AddStringToSliceIfNotExists(partitionsNameList[metadata.TableTitle{
			Database: database, Table: table,
		}], partitionName)
	}
}

func createIdMapAndNameListIfNotExists(database, table string, partitionsIdsMap map[metadata.TableTitle]common.EmptyMap, partitionsNameList map[metadata.TableTitle][]string) {
	if _, exists := partitionsIdsMap[metadata.TableTitle{Database: database, Table: table}]; !exists {
		partitionsIdsMap[metadata.TableTitle{Database: database, Table: table}] = make(common.EmptyMap, 0)
	}
	if _, exists := partitionsNameList[metadata.TableTitle{Database: database, Table: table}]; !exists {
		partitionsNameList[metadata.TableTitle{Database: database, Table: table}] = make([]string, 0)
	}
}
