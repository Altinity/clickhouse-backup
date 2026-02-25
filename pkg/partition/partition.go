package partition

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
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
var SpecialCharsRE = regexp.MustCompile(`(?i)[)(*+\-/\\,]+`)
var FieldsNamesRE = regexp.MustCompile("(?i)\\w+|`[^`]+`\\.`[^`]+`|\"[^\"]+\"")

func extractPartitionComponents(partitionExpr string) []string {
	partitionExpr = strings.TrimSpace(partitionExpr)
	log.Debug().Msgf("extractPartitionComponents(partitionExpr=%s)", partitionExpr)
	if strings.HasPrefix(partitionExpr, "(") && strings.HasSuffix(partitionExpr, ")") {
		inner := strings.TrimSpace(partitionExpr[1 : len(partitionExpr)-1])
		return splitPartitionTuple(inner)
	}
	return []string{partitionExpr}
}

func splitPartitionTuple(s string) []string {
	var components []string
	var current strings.Builder
	depth := 0
	inQuote := false
	var quoteChar rune

	for _, ch := range s {
		switch ch {
		case '\'', '"':
			if !inQuote {
				inQuote = true
				quoteChar = ch
			} else if ch == quoteChar {
				inQuote = false
			}
			current.WriteRune(ch)
		case '(':
			if !inQuote {
				depth++
			}
			current.WriteRune(ch)
		case ')':
			if !inQuote {
				depth--
			}
			current.WriteRune(ch)
		case ',':
			if !inQuote && depth == 0 {
				// Top-level comma, split here
				components = append(components, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		components = append(components, strings.TrimSpace(current.String()))
	}

	return components
}

func extractPartitionByFieldNames(s string) []struct {
	Name string `ch:"name"`
} {
	s = SettingsRE.ReplaceAllString(s, "")
	s = OrderByRE.ReplaceAllString(s, "")
	s = FunctionsRE.ReplaceAllString(s, "")
	s = StringsRE.ReplaceAllString(s, "")
	s = SpecialCharsRE.ReplaceAllString(s, " ")
	matches := FieldsNamesRE.FindAllString(s, -1)
	columns := make([]struct {
		Name string `ch:"name"`
	}, len(matches))
	for i := range matches {
		columns[i].Name = strings.TrimSpace(matches[i])
	}
	return columns
}

func GetPartitionIdAndName(ctx context.Context, ch *clickhouse.ClickHouse, database, table, createQuery, partition string) (string, string, error) {
	if !strings.Contains(createQuery, "MergeTree") || !PartitionByRE.MatchString(createQuery) {
		return "", "", nil
	}

	partitionByMatches := PartitionByRE.FindStringSubmatch(createQuery)
	if len(partitionByMatches) < 2 || partitionByMatches[1] == "" {
		return "", "", nil
	}
	partitionExpr := strings.TrimSpace(partitionByMatches[1])
	s := partitionExpr
	s = SettingsRE.ReplaceAllString(s, "")
	s = OrderByRE.ReplaceAllString(s, "")
	partitionExpr = s

	version, err := ch.GetVersion(ctx)
	if err != nil {
		log.Warn().Msgf("can't get ClickHouse version, falling back to temporary table: %v", err)
	}

	if version >= 21008000 {
		partitionId, partitionName, err := getPartitionIdWithFunction(ctx, ch, createQuery, partitionExpr, partition)
		if err != nil {
			log.Warn().Msgf("partitionId function failed, using temporary table: %v", err)
			return getPartitionIdWithTempTable(ctx, ch, database, table, createQuery, partition, partitionByMatches)
		}
		return partitionId, partitionName, nil
	}

	return getPartitionIdWithTempTable(ctx, ch, database, table, createQuery, partition, partitionByMatches)
}

func getPartitionIdWithFunction(ctx context.Context, ch *clickhouse.ClickHouse, createQuery, partitionExpr, partition string) (string, string, error) {
	partitionValues := splitAndParsePartition(partition)
	partitionComponents := extractPartitionComponents(partitionExpr)

	if len(partitionValues) != len(partitionComponents) {
		return "", "", fmt.Errorf("partition values count (%d) doesn't match components count (%d)", len(partitionValues), len(partitionComponents))
	}

	evaluatedId, evaluatedName, evaluatedErr := tryEvaluatedPartitionId(ctx, ch, createQuery, partitionExpr, partitionComponents, partitionValues)
	if evaluatedErr == nil {
		return evaluatedId, evaluatedName, nil
	}

	directId, directName, directErr := tryDirectPartitionId(ctx, ch, partitionValues)
	if directErr == nil {
		return directId, directName, nil
	}

	return "", "", fmt.Errorf("both approaches failed: evaluated=%v, direct=%v", evaluatedErr, directErr)
}

func tryDirectPartitionId(ctx context.Context, ch *clickhouse.ClickHouse, partitionValues []interface{}) (string, string, error) {
	placeholders := make([]string, len(partitionValues))
	var args []interface{}

	for i := range partitionValues {
		placeholders[i] = "?"
		args = append(args, partitionValues[i])
	}

	var partitionNameExpr string
	if len(partitionValues) == 1 {
		partitionNameExpr = "toString(?)"
		args = append(args, partitionValues[0])
	} else {
		partitionNameExpr = fmt.Sprintf("toString((%s))", strings.Join(placeholders, ", "))
		args = append(args, partitionValues...)
	}

	sql := fmt.Sprintf("SELECT partitionId(%s) AS partition_id, %s AS partition_name",
		strings.Join(placeholders, ", "), partitionNameExpr)

	var result []struct {
		PartitionId   string `ch:"partition_id"`
		PartitionName string `ch:"partition_name"`
	}

	if err := ch.SelectContext(ctx, &result, sql, args...); err != nil {
		return "", "", err
	}

	if len(result) != 1 {
		return "", "", fmt.Errorf("unexpected result count: %d", len(result))
	}

	return result[0].PartitionId, result[0].PartitionName, nil
}

func tryEvaluatedPartitionId(ctx context.Context, ch *clickhouse.ClickHouse, createQuery, partitionExpr string, partitionComponents []string, partitionValues []interface{}) (string, string, error) {
	columns := extractPartitionByFieldNames(partitionExpr)
	if len(columns) == 0 {
		return "", "", fmt.Errorf("can't extract column names from partition expression: %s", partitionExpr)
	}

	if len(partitionValues) != len(columns) {
		return "", "", fmt.Errorf("partition values count (%d) doesn't match columns count (%d)", len(partitionValues), len(columns))
	}

	var args []interface{}
	withItems := make([]string, len(columns))
	for i, col := range columns {
		columnType := extractFieldType(createQuery, col.Name, partitionValues[i])
		withItems[i] = fmt.Sprintf("CAST(? AS %s) AS %s", columnType, col.Name)
		args = append(args, partitionValues[i])
	}

	partitionIdArgs := partitionExpr
	if len(partitionComponents) > 1 {
		partitionIdArgs = strings.Join(partitionComponents, ", ")
	}

	// Use ClickHouse's native partition formatting by calling toString() on the full expression
	partitionNameExpr := fmt.Sprintf("toString(%s)", partitionExpr)

	sql := fmt.Sprintf("WITH %s SELECT partitionId(%s) AS partition_id, %s AS partition_name",
		strings.Join(withItems, ", "), partitionIdArgs, partitionNameExpr)

	var result []struct {
		PartitionId   string `ch:"partition_id"`
		PartitionName string `ch:"partition_name"`
	}

	if err := ch.SelectContext(ctx, &result, sql, args...); err != nil {
		return "", "", errors.Wrapf(err, "can't execute partitionId query: sql=%s, args=%#v", sql, args)
	}

	if len(result) != 1 {
		return "", "", fmt.Errorf("unexpected result: %#v", result)
	}

	return result[0].PartitionId, result[0].PartitionName, nil
}

func extractFieldType(createQuery, columnName string, value interface{}) string {
	fieldNamePattern := fmt.Sprintf(`(?:[\s,\(])\x60?%s\x60?\s+`, regexp.QuoteMeta(columnName))
	re := regexp.MustCompile(fieldNamePattern)

	loc := re.FindStringIndex(createQuery)
	if loc != nil {
		remaining := createQuery[loc[1]:]

		var parsedType strings.Builder
		nesting := 0
		inQuotes := false

		for _, ch := range remaining {
			// We process strings inside the type (for example, DateTime('Europe/Moscow'))
			if ch == '\'' {
				inQuotes = !inQuotes
				parsedType.WriteRune(ch)
				continue
			}

			// If we are inside quotes, we just take everything as it is
			if inQuotes {
				parsedType.WriteRune(ch)
				continue
			}

			// Accounting for nesting of parentheses
			if ch == '(' {
				nesting++
				parsedType.WriteRune(ch)
				continue
			}
			if ch == ')' {
				if nesting == 0 {
					break // Дошли до закрывающей скобки списка колонок (конец таблицы)
				}
				nesting--
				parsedType.WriteRune(ch)
				continue
			}

			// A comma OUTSIDE parentheses means the end of the current column definition
			if ch == ',' {
				if nesting == 0 {
					break
				}
				parsedType.WriteRune(ch)
				continue
			}

			// A space OUTSIDE the parentheses means the end of the type and the beginning of the properties (e.g. DEFAULT, COMMENT)
			// Spaces INSIDE parentheses (e.g. Decimal(64, 3)) are preserved
			if ch == ' ' || ch == '\t' || ch == '\n' {
				if nesting == 0 {
					if parsedType.Len() > 0 {
						break // Space after type
					}
					continue // Skip leading spaces if present
				}
				parsedType.WriteRune(ch)
				continue
			}

			// Any other character is added to the result
			parsedType.WriteRune(ch)
		}

		return parsedType.String()
	}

	switch value.(type) {
	case int64:
		return "Int64"
	case float64:
		return "Float64"
	default:
		return "String"
	}
}

func getPartitionIdWithTempTable(ctx context.Context, ch *clickhouse.ClickHouse, database, table, createQuery, partition string, partitionByMatches []string) (string, string, error) {
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
		return "", "", err
	}
	columns := make([]struct {
		Name string `ch:"name"`
	}, 0)
	sql := "SELECT name FROM system.columns WHERE database=? AND table=? AND is_in_partition_key"
	oldVersion := false
	if err := ch.SelectContext(ctx, &columns, sql, database, partitionIdTable); err != nil {
		if len(partitionByMatches) == 0 {
			if dropErr := dropPartitionIdTable(ch, database, partitionIdTable); dropErr != nil {
				return "", "", dropErr
			}
			return "", "", errors.Wrapf(err, "can't get is_in_partition_key column names from for table `%s`.`%s`", database, partitionIdTable)
		}
		columns = extractPartitionByFieldNames(partitionByMatches[1])
		oldVersion = true
	}
	// to the same order of fields as described in PARTITION BY clause, https://github.com/Altinity/clickhouse-backup/issues/791
	if len(partitionByMatches) == 2 && partitionByMatches[1] != "" {
		sort.Slice(columns, func(i int, j int) bool {
			return strings.Index(partitionByMatches[1], columns[i].Name) < strings.Index(partitionByMatches[1], columns[j].Name)
		})
	}
	defer func() {
		if dropErr := dropPartitionIdTable(ch, database, partitionIdTable); dropErr != nil {
			log.Warn().Msgf("partition.GetPartitionId can't drop `%s`.`%s`: %v", database, partitionIdTable, dropErr)
		}
	}()
	if len(columns) == 0 {
		log.Warn().Msgf("is_in_partition_key=1 fields not found in system.columns for table `%s`.`%s`", database, partitionIdTable)
		return "", "", nil
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
			return "", "", errors.Wrapf(err, "PrepareBatch sql=%s partitionInsert=%#v", sql, partitionInsert)
		}
		if err = batch.Append(partitionInsert...); err != nil {
			return "", "", errors.Wrapf(err, "batch.Append sql=%s partitionInsert=%#v", sql, partitionInsert)
		}
		if err = batch.Send(); err != nil {
			return "", "", errors.Wrapf(err, "batch.Send sql=%s partitionInsert=%#v", sql, partitionInsert)
		}
	}
	if err != nil {
		return "", "", err
	}
	partitions := make([]struct {
		Id   string `ch:"partition_id"`
		Name string `ch:"partition"`
	}, 0)
	sql = "SELECT partition_id, partition FROM system.parts WHERE active AND database=? AND table=?"
	if err = ch.SelectContext(ctx, &partitions, sql, database, partitionIdTable); err != nil {
		return "", "", errors.Wrapf(err, "can't SELECT partition_id for PARTITION BY fields(%#v) FROM `%s`.`%s`", partitionInsert, database, partitionIdTable)
	}
	if len(partitions) != 1 {
		return "", "", fmt.Errorf("wrong partitionsIds=%#v found system.parts for table `%s`.`%s`", partitions, database, partitionIdTable)
	}

	return partitions[0].Id, partitions[0].Name, nil
}

func dropPartitionIdTable(ch *clickhouse.ClickHouse, database string, partitionIdTable string) error {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", database, partitionIdTable)
	if isAtomicOrReplicated, err := ch.IsDbAtomicOrReplicated(database); isAtomicOrReplicated {
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
func ConvertPartitionsToIdsMapAndNamesList(ctx context.Context, ch *clickhouse.ClickHouse, tablesFromClickHouse []clickhouse.Table, tablesFromMetadata []*metadata.TableMetadata, partitions []string) (map[metadata.TableTitle]common.EmptyMap, map[metadata.TableTitle][]string) {
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
		tablePattern := "*.*"
		// when PARTITION BY is table specific, https://github.com/Altinity/clickhouse-backup/issues/916
		if tablePatternDelimiterIndex := strings.Index(partitionArg, ":"); tablePatternDelimiterIndex != -1 {
			tablePattern = partitionArg[:tablePatternDelimiterIndex]
			partitionArg = partitionArg[tablePatternDelimiterIndex+1:]
		}
		// when PARTITION BY clause return partition_id field as hash, https://github.com/Altinity/clickhouse-backup/issues/602
		if strings.HasPrefix(partitionArg, "(") {
			partitionArg = strings.TrimSuffix(strings.TrimPrefix(partitionArg, "("), ")")
			for _, partitionTuple := range partitionTupleRE.Split(partitionArg, -1) {
				for _, t := range tablesFromClickHouse {
					createIdMapAndNameListIfNotExists(t.Database, t.Name, partitionsIdMap, partitionsNameList)
					if partitionId, partitionName, err := GetPartitionIdAndName(ctx, ch, t.Database, t.Name, t.CreateTableQuery, partitionTuple); err != nil {
						log.Fatal().Stack().Msgf("partition.GetPartitionIdAndName error: %v", err)
					} else if partitionId != "" {
						addItemToIdMapAndNameListIfNotExists(partitionId, partitionName, t.Database, t.Name, partitionsIdMap, partitionsNameList, tablePattern)
					}
				}
				for _, t := range tablesFromMetadata {
					createIdMapAndNameListIfNotExists(t.Database, t.Table, partitionsIdMap, partitionsNameList)
					if partitionId, partitionName, err := GetPartitionIdAndName(ctx, ch, t.Database, t.Table, t.Query, partitionTuple); err != nil {
						log.Fatal().Stack().Msgf("partition.GetPartitionIdAndName error: %v", err)
					} else if partitionId != "" {
						addItemToIdMapAndNameListIfNotExists(partitionId, partitionName, t.Database, t.Table, partitionsIdMap, partitionsNameList, tablePattern)
					}
				}
			}
		} else {
			// when partitionId == partitionName
			for _, item := range strings.Split(partitionArg, ",") {
				item = strings.Trim(item, " \t")
				for _, t := range tablesFromClickHouse {
					createIdMapAndNameListIfNotExists(t.Database, t.Name, partitionsIdMap, partitionsNameList)
					addItemToIdMapAndNameListIfNotExists(item, item, t.Database, t.Name, partitionsIdMap, partitionsNameList, tablePattern)
				}
				for _, t := range tablesFromMetadata {
					createIdMapAndNameListIfNotExists(t.Database, t.Table, partitionsIdMap, partitionsNameList)
					addItemToIdMapAndNameListIfNotExists(item, item, t.Database, t.Table, partitionsIdMap, partitionsNameList, tablePattern)
				}
			}
		}
	}
	return partitionsIdMap, partitionsNameList
}

func addItemToIdMapAndNameListIfNotExists(partitionId, partitionName, database, table string, partitionsIdMap map[metadata.TableTitle]common.EmptyMap, partitionsNameList map[metadata.TableTitle][]string, tablePattern string) {
	// https://github.com/Altinity/clickhouse-backup/issues/1091
	replacer := strings.NewReplacer(`\`, "_", `/`, "_")
	if matched, err := filepath.Match(replacer.Replace(tablePattern), replacer.Replace(database+"."+table)); err == nil && matched || tablePattern == "*" {
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
	} else if err != nil {
		log.Error().Msgf("wrong --partitions table specific pattern matching: %v", err)
	}
}

func createIdMapAndNameListIfNotExists(database, table string, partitionsIdsMap map[metadata.TableTitle]common.EmptyMap, partitionsNameList map[metadata.TableTitle][]string) {
	if _, exists := partitionsIdsMap[metadata.TableTitle{Database: database, Table: table}]; !exists {
		partitionsIdsMap[metadata.TableTitle{Database: database, Table: table}] = make(common.EmptyMap)
	}
	if _, exists := partitionsNameList[metadata.TableTitle{Database: database, Table: table}]; !exists {
		partitionsNameList[metadata.TableTitle{Database: database, Table: table}] = make([]string, 0)
	}
}
