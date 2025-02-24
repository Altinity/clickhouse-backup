package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/partition"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type ListOfTables []metadata.TableMetadata

// Sort - sorting ListOfTables slice orderly by engine priority
func (lt ListOfTables) Sort(dropTable bool) {
	sort.Slice(lt, func(i, j int) bool {
		return getOrderByEngine(lt[i].Query, dropTable) < getOrderByEngine(lt[j].Query, dropTable)
	})
}

func addTableToListIfNotExistsOrEnrichQueryAndParts(tables ListOfTables, table metadata.TableMetadata) ListOfTables {
	for i, t := range tables {
		if (t.Database == table.Database) && (t.Table == table.Table) {
			if t.Query == "" && table.Query != "" {
				tables[i].Query = table.Query
			}
			if len(t.Parts) == 0 && len(table.Parts) > 0 {
				tables[i].Parts = table.Parts
			}
			return tables
		}
	}
	return append(tables, table)
}

func (b *Backuper) getTableListByPatternLocal(ctx context.Context, metadataPath string, tablePattern string, dropTable bool, partitions []string) (ListOfTables, map[metadata.TableTitle][]string, error) {
	result := ListOfTables{}
	resultPartitionNames := map[metadata.TableTitle][]string{}
	tablePatterns := []string{"*"}
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	var err error
	// https://github.com/Altinity/clickhouse-backup/issues/613
	if !b.isEmbedded {
		if tablePatterns, err = b.enrichTablePatternsByInnerDependencies(metadataPath, tablePatterns); err != nil {
			return nil, nil, err
		}
	}
	// https://github.com/Altinity/clickhouse-backup/issues/1091
	replacer := strings.NewReplacer(`/`, "_", `\`, "_")

	if err := filepath.Walk(metadataPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(filePath, ".sql") &&
			!strings.HasSuffix(filePath, ".json") &&
			!info.Mode().IsRegular() {
			return nil
		}
		p := filepath.ToSlash(filePath)
		isEmbeddedMetadata := false
		if strings.HasSuffix(p, ".sql") {
			isEmbeddedMetadata = true
			p = strings.TrimSuffix(p, ".sql")
		} else {
			p = strings.TrimSuffix(p, ".json")
		}
		names, database, table, tableName, shallSkipped, continueProcessing := b.checkShallSkipped(p, metadataPath)
		if !continueProcessing || shallSkipped {
			return nil
		}
		for _, pattern := range tablePatterns {
			// https://github.com/Altinity/clickhouse-backup/issues/1091
			if matched, _ := filepath.Match(replacer.Replace(strings.Trim(pattern, " \t\r\n")), replacer.Replace(tableName)); !matched {
				continue
			}
			data, err := os.ReadFile(filePath)
			if err != nil {
				return err
			}
			if isEmbeddedMetadata {
				// embedded backup to s3 disk could contain only s3 key names inside .sql file
				t, err := prepareTableMetadataFromSQL(data, metadataPath, names, b.cfg, database, table)
				if err != nil {
					return err
				}
				// .sql file will enrich Query
				partitionsIdMap, _ := partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, nil, []metadata.TableMetadata{t}, partitions)
				filterPartsAndFilesByPartitionsFilter(t, partitionsIdMap[metadata.TableTitle{Database: t.Database, Table: t.Table}])
				result = addTableToListIfNotExistsOrEnrichQueryAndParts(result, t)
				return nil
			}
			var t metadata.TableMetadata
			if err := json.Unmarshal(data, &t); err != nil {
				return err
			}
			partitionsIdMap, partitionsNameList := partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, nil, []metadata.TableMetadata{t}, partitions)
			filterPartsAndFilesByPartitionsFilter(t, partitionsIdMap[metadata.TableTitle{Database: t.Database, Table: t.Table}])
			result = addTableToListIfNotExistsOrEnrichQueryAndParts(result, t)
			for tt := range partitionsNameList {
				if _, exists := resultPartitionNames[tt]; !exists {
					resultPartitionNames[tt] = []string{}
				}
				resultPartitionNames[tt] = common.AddSliceToSliceIfNotExists(
					resultPartitionNames[tt],
					partitionsNameList[tt],
				)
			}
			return nil
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	result.Sort(dropTable)
	for i := 0; i < len(result); i++ {
		if b.shouldSkipByTableEngine(result[i]) {
			t := result[i]
			delete(resultPartitionNames, metadata.TableTitle{Database: t.Database, Table: t.Table})
			result = append(result[:i], result[i+1:]...)
			if i > 0 {
				i = i - 1
			}
		}
	}
	return result, resultPartitionNames, nil
}

func (b *Backuper) shouldSkipByTableName(tableFullName string) bool {
	shallSkipped := false
	for _, skipPattern := range b.cfg.ClickHouse.SkipTables {
		if shallSkipped, _ = filepath.Match(skipPattern, tableFullName); shallSkipped {
			break
		}
	}
	return shallSkipped
}
func (b *Backuper) shouldSkipByTableEngine(t metadata.TableMetadata) bool {
	for _, engine := range b.cfg.ClickHouse.SkipTableEngines {
		//b.log.Debugf("engine=%s query=%s", engine, t.Query)
		if strings.ToLower(engine) == "dictionary" && (strings.HasPrefix(t.Query, "ATTACH DICTIONARY") || strings.HasPrefix(t.Query, "CREATE DICTIONARY")) {
			log.Warn().Msgf("shouldSkipByTableEngine engine=%s found in : %s", engine, t.Query)
			return true
		}
		if strings.ToLower(engine) == "materializedview" && (strings.HasPrefix(t.Query, "ATTACH MATERIALIZED VIEW") || strings.HasPrefix(t.Query, "CREATE MATERIALIZED VIEW")) {
			log.Warn().Msgf("shouldSkipByTableEngine engine=%s found in : %s", engine, t.Query)
			return true
		}
		if strings.ToLower(engine) == "view" && (strings.HasPrefix(t.Query, "ATTACH VIEW") || strings.HasPrefix(t.Query, "CREATE VIEW")) {
			log.Warn().Msgf("shouldSkipByTableEngine engine=%s found in : %s", engine, t.Query)
			return true
		}
		if strings.ToLower(engine) == "liveview" && (strings.HasPrefix(t.Query, "ATTACH LIVE") || strings.HasPrefix(t.Query, "CREATE LIVE")) {
			log.Warn().Msgf("shouldSkipByTableEngine engine=%s found in : %s", engine, t.Query)
			return true
		}
		if strings.ToLower(engine) == "windowview" && (strings.HasPrefix(t.Query, "ATTACH WINDOW") || strings.HasPrefix(t.Query, "CREATE WINDOW")) {
			log.Warn().Msgf("shouldSkipByTableEngine engine=%s found in : %s", engine, t.Query)
			return true
		}
		if engine != "" {
			if shouldSkip, err := regexp.MatchString(fmt.Sprintf("(?mi)ENGINE\\s*=\\s*%s([\\(\\s]|\\s*)", engine), t.Query); err == nil && shouldSkip {
				log.Warn().Msgf("shouldSkipByTableEngine engine=%s found in : %s", engine, t.Query)
				return true
			} else if err != nil {
				log.Warn().Msgf("shouldSkipByTableEngine engine=%s return error: %v", engine, err)
			}
		}
	}
	return false
}

func (b *Backuper) checkShallSkipped(p string, metadataPath string) ([]string, string, string, string, bool, bool) {
	p = strings.Trim(strings.TrimPrefix(p, metadataPath), "/")
	names := strings.Split(p, "/")
	if len(names) != 2 {
		return nil, "", "", "", true, false
	}
	database, _ := url.PathUnescape(names[0])
	if IsInformationSchema(database) {
		return nil, "", "", "", true, false
	}
	table, _ := url.PathUnescape(names[1])
	tableFullName := fmt.Sprintf("%s.%s", database, table)
	shallSkipped := b.shouldSkipByTableName(tableFullName)
	return names, database, table, tableFullName, shallSkipped, true
}

func prepareTableMetadataFromSQL(data []byte, metadataPath string, names []string, cfg *config.Config, database string, table string) (metadata.TableMetadata, error) {
	query := string(data)
	if strings.HasPrefix(query, "ATTACH") || strings.HasPrefix(query, "CREATE") {
		query = strings.Replace(query, "ATTACH", "CREATE", 1)
	} else {
		query = ""
	}
	dataPartsPath := strings.Replace(metadataPath, "/metadata", "/data", 1)
	dataPartsPath = path.Join(dataPartsPath, path.Join(names...))
	if _, err := os.Stat(dataPartsPath); err != nil && !os.IsNotExist(err) {
		return metadata.TableMetadata{}, err
	}
	dataParts, err := os.ReadDir(dataPartsPath)
	if err != nil && !os.IsNotExist(err) {
		log.Warn().Err(err).Send()
	}
	parts := map[string][]metadata.Part{
		cfg.ClickHouse.EmbeddedBackupDisk: make([]metadata.Part, len(dataParts)),
	}
	for i := range dataParts {
		parts[cfg.ClickHouse.EmbeddedBackupDisk][i].Name = dataParts[i].Name()
	}
	var t metadata.TableMetadata
	t = metadata.TableMetadata{
		Database: database,
		Table:    table,
		Query:    query,
		Parts:    parts,
	}
	return t, nil
}

func (b *Backuper) enrichTablePatternsByInnerDependencies(metadataPath string, tablePatterns []string) ([]string, error) {
	innerTablePatterns := make([]string, 0)
	// https://github.com/Altinity/clickhouse-backup/issues/1091
	replacer := strings.NewReplacer("/", "_", `\`, "_")

	if err := filepath.Walk(metadataPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(filePath, ".json") {
			return nil
		}
		names, database, table, tableName, shallSkipped, continueProcessing := b.checkShallSkipped(strings.TrimSuffix(filepath.ToSlash(filePath), ".json"), metadataPath)
		if !continueProcessing || shallSkipped {
			return nil
		}
		for _, pattern := range tablePatterns {
			// https://github.com/Altinity/clickhouse-backup/issues/1091
			if matched, _ := filepath.Match(replacer.Replace(strings.Trim(pattern, " \t\r\n")), replacer.Replace(tableName)); !matched {
				continue
			}
			data, err := os.ReadFile(filePath)
			if err != nil {
				return err
			}
			var t metadata.TableMetadata
			if err := json.Unmarshal(data, &t); err != nil {
				return err
			}
			if strings.HasPrefix(t.Query, "ATTACH MATERIALIZED") || strings.HasPrefix(t.Query, "CREATE MATERIALIZED") {
				if strings.Contains(t.Query, " TO ") && !strings.Contains(t.Query, " TO INNER UUID") {
					continue
				}
				innerTableFile := path.Join(names[:len(names)-1]...)
				innerTableName := fmt.Sprintf("%s.", database)
				if matches := uuidRE.FindStringSubmatch(t.Query); len(matches) > 0 {
					innerTableFile = path.Join(innerTableFile, common.TablePathEncode(fmt.Sprintf(".inner_id.%s", matches[1])))
					innerTableName += fmt.Sprintf(".inner_id.%s", matches[1])
				} else {
					innerTableFile = path.Join(innerTableFile, common.TablePathEncode(fmt.Sprintf(".inner.%s", table)))
					innerTableName += fmt.Sprintf(".inner.%s", table)
				}
				// https://github.com/Altinity/clickhouse-backup/issues/765, .inner. table could be dropped manually, .inner. table is required for ATTACH
				if _, err := os.Stat(path.Join(metadataPath, innerTableFile+".json")); err != nil {
					return err
				}
				innerPatternExists := false
				for _, existsP := range tablePatterns {
					if innerPatternExists, _ = filepath.Match(strings.Trim(existsP, " \t\r\n"), innerTableName); innerPatternExists {
						break
					}
				}
				if !innerPatternExists {
					innerTablePatterns = append(innerTablePatterns, innerTableName)
				}
			}
			return nil
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(innerTablePatterns) > 0 {
		tablePatterns = append(innerTablePatterns, tablePatterns...)
	}
	return tablePatterns, nil
}

var queryRE = regexp.MustCompile(`(?m)^(CREATE|ATTACH) (TABLE|VIEW|LIVE VIEW|MATERIALIZED VIEW|DICTIONARY|FUNCTION) (\x60?)([^\s\x60.]*)(\x60?)\.\x60?([^\s\x60.]*)\x60?( UUID '[^']+')?(?:( TO )(\x60?)([^\s\x60.]*)(\x60?)(\.)(\x60?)([^\s\x60.]*)(\x60?))?(?:(.+FROM )(\x60?)([^\s\x60.]*)(\x60?)(\.)(\x60?)([^\s\x60.]*)(\x60?))?`)
var createOrAttachRE = regexp.MustCompile(`(?m)^(CREATE|ATTACH)`)
var uuidRE = regexp.MustCompile(`UUID '([a-f\d\-]+)'`)

var usualIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
var replicatedRE = regexp.MustCompile(`(Replicated[a-zA-Z]*MergeTree)\('([^']+)'([^)]+)\)`)
var distributedRE = regexp.MustCompile(`(Distributed)\(([^,]+),([^,]+),([^,]+)([^)]+)\)`)

func changeTableQueryToAdjustDatabaseMapping(originTables *ListOfTables, dbMapRule map[string]string) error {
	for i := 0; i < len(*originTables); i++ {
		originTable := (*originTables)[i]
		if targetDB, isMapped := dbMapRule[originTable.Database]; isMapped {
			// substitute database in the table create query
			var substitution string

			if createOrAttachRE.MatchString(originTable.Query) {
				matches := queryRE.FindAllStringSubmatch(originTable.Query, -1)
				if matches[0][4] != originTable.Database {
					return fmt.Errorf("invalid SQL: %s for restore-database-mapping[%s]=%s", originTable.Query, originTable.Database, targetDB)
				}
				setMatchedDb := func(clauseTargetDb string) string {
					if clauseMappedDb, isClauseMapped := dbMapRule[clauseTargetDb]; isClauseMapped {
						clauseTargetDb = clauseMappedDb
						if !usualIdentifier.MatchString(clauseTargetDb) {
							clauseTargetDb = "`" + clauseTargetDb + "`"
						}
					}
					return clauseTargetDb
				}
				createTargetDb := targetDB
				if !usualIdentifier.MatchString(createTargetDb) {
					createTargetDb = "`" + createTargetDb + "`"
				}
				toClauseTargetDb := setMatchedDb(matches[0][10])
				fromClauseTargetDb := setMatchedDb(matches[0][18])
				// matching CREATE|ATTACH ... TO .. SELECT ... FROM ... command
				substitution = fmt.Sprintf("${1} ${2} ${3}%v${5}.${6}${7}${8}${9}%v${11}${12}${13}${14}${15}${16}${17}%v${19}${20}${21}${22}${23}", createTargetDb, toClauseTargetDb, fromClauseTargetDb)
			} else {
				if originTable.Query == "" {
					continue
				}
				return fmt.Errorf("error when try to replace database `%s` to `%s` in query: %s", originTable.Database, targetDB, originTable.Query)
			}
			originTable.Query = queryRE.ReplaceAllString(originTable.Query, substitution)
			if uuidRE.MatchString(originTable.Query) {
				newUUID, _ := uuid.NewUUID()
				substitution = fmt.Sprintf("UUID '%s'", newUUID.String())
				originTable.Query = uuidRE.ReplaceAllString(originTable.Query, substitution)
			}
			// https://github.com/Altinity/clickhouse-backup/issues/547
			if replicatedRE.MatchString(originTable.Query) {
				matches := replicatedRE.FindAllStringSubmatch(originTable.Query, -1)
				originPath := matches[0][2]
				dbReplicatedPattern := "/" + originTable.Database + "/"
				if strings.Contains(originPath, dbReplicatedPattern) {
					substitution = fmt.Sprintf("${1}('%s'${3})", strings.Replace(originPath, dbReplicatedPattern, "/"+targetDB+"/", 1))
					originTable.Query = replicatedRE.ReplaceAllString(originTable.Query, substitution)
				}
			}
			// https://github.com/Altinity/clickhouse-backup/issues/547
			if distributedRE.MatchString(originTable.Query) {
				matches := distributedRE.FindAllStringSubmatch(originTable.Query, -1)
				underlyingDB := matches[0][3]
				underlyingDBClean := strings.NewReplacer(" ", "", "'", "").Replace(underlyingDB)
				if underlyingTargetDB, isUnderlyingMapped := dbMapRule[underlyingDBClean]; isUnderlyingMapped {
					substitution = fmt.Sprintf("${1}(${2},%s,${4}${5})", strings.Replace(underlyingDB, underlyingDBClean, underlyingTargetDB, 1))
					originTable.Query = distributedRE.ReplaceAllString(originTable.Query, substitution)
				}
			}
			originTable.Database = targetDB
			(*originTables)[i] = originTable
		}
	}
	return nil
}

func changeTableQueryToAdjustTableMapping(originTables *ListOfTables, tableMapRule map[string]string) error {
	for i := 0; i < len(*originTables); i++ {
		originTable := (*originTables)[i]
		if targetTable, isMapped := tableMapRule[originTable.Table]; isMapped {
			// substitute table in the table create query
			var substitution string

			if createOrAttachRE.MatchString(originTable.Query) {
				matches := queryRE.FindAllStringSubmatch(originTable.Query, -1)
				if matches[0][6] != originTable.Table {
					return fmt.Errorf("invalid SQL: %s for restore-table-mapping[%s]=%s", originTable.Query, originTable.Table, targetTable)
				}
				setMatchedDb := func(clauseTargetTable string) string {
					if clauseMappedTable, isClauseMapped := tableMapRule[clauseTargetTable]; isClauseMapped {
						clauseTargetTable = clauseMappedTable
						if !usualIdentifier.MatchString(clauseTargetTable) {
							clauseTargetTable = "`" + clauseTargetTable + "`"
						}
					}
					return clauseTargetTable
				}
				createTargetTable := targetTable
				if !usualIdentifier.MatchString(createTargetTable) {
					createTargetTable = "`" + createTargetTable + "`"
				}
				toClauseTargetTable := setMatchedDb(matches[0][14])
				fromClauseTargetTable := setMatchedDb(matches[0][22])
				// matching CREATE|ATTACH ... TO .. SELECT ... FROM ... command
				substitution = fmt.Sprintf("${1} ${2} ${3}${4}${5}.%v${7}${8}${9}${10}${11}${12}${13}%v${15}${16}${17}${18}${19}${20}${21}%v${23}", createTargetTable, toClauseTargetTable, fromClauseTargetTable)
			} else {
				if originTable.Query == "" {
					continue
				}
				return fmt.Errorf("error when try to replace table `%s` to `%s` in query: %s", originTable.Table, targetTable, originTable.Query)
			}
			originTable.Query = queryRE.ReplaceAllString(originTable.Query, substitution)
			if uuidRE.MatchString(originTable.Query) {
				newUUID, _ := uuid.NewUUID()
				substitution = fmt.Sprintf("UUID '%s'", newUUID.String())
				originTable.Query = uuidRE.ReplaceAllString(originTable.Query, substitution)
			}
			// https://github.com/Altinity/clickhouse-backup/issues/547
			if replicatedRE.MatchString(originTable.Query) {
				matches := replicatedRE.FindAllStringSubmatch(originTable.Query, -1)
				originPath := matches[0][2]
				tableReplicatedPattern := "/" + originTable.Table
				if strings.Contains(originPath, tableReplicatedPattern) {
					substitution = fmt.Sprintf("${1}('%s'${3})", strings.Replace(originPath, tableReplicatedPattern, "/"+targetTable, 1))
					originTable.Query = replicatedRE.ReplaceAllString(originTable.Query, substitution)
				}
			}
			// https://github.com/Altinity/clickhouse-backup/issues/547
			if distributedRE.MatchString(originTable.Query) {
				matches := distributedRE.FindAllStringSubmatch(originTable.Query, -1)
				underlyingTable := matches[0][4]
				underlyingTableClean := strings.NewReplacer(" ", "", "'", "").Replace(underlyingTable)
				if underlyingTargetTable, isUnderlyingMapped := tableMapRule[underlyingTableClean]; isUnderlyingMapped {
					substitution = fmt.Sprintf("${1}(${2},${3},%s${5})", strings.Replace(underlyingTable, underlyingTableClean, underlyingTargetTable, 1))
					originTable.Query = distributedRE.ReplaceAllString(originTable.Query, substitution)
				}
			}
			originTable.Table = targetTable
			(*originTables)[i] = originTable
		}
	}
	return nil
}

func changePartitionsToAdjustDatabaseMapping(partitionsNames map[metadata.TableTitle][]string, databaseMapping map[string]string) (map[metadata.TableTitle][]string, error) {
	adjustedPartitionsNames := map[metadata.TableTitle][]string{}
	for tableTitle, partitions := range partitionsNames {
		if targetDb, isMapped := databaseMapping[tableTitle.Database]; isMapped {
			tableTitle.Database = targetDb
		}
		adjustedPartitionsNames[tableTitle] = partitions
	}
	return adjustedPartitionsNames, nil
}

func changePartitionsToAdjustTableMapping(partitionsNames map[metadata.TableTitle][]string, tableMapping map[string]string) (map[metadata.TableTitle][]string, error) {
	adjustedPartitionsNames := map[metadata.TableTitle][]string{}
	for tableTitle, partitions := range partitionsNames {
		if targetTable, isMapped := tableMapping[tableTitle.Table]; isMapped {
			tableTitle.Table = targetTable
		}
		adjustedPartitionsNames[tableTitle] = partitions
	}
	return adjustedPartitionsNames, nil
}

func filterPartsAndFilesByPartitionsFilter(tableMetadata metadata.TableMetadata, partitionsFilter common.EmptyMap) {
	if len(partitionsFilter) > 0 {
		for disk, parts := range tableMetadata.Parts {
			filteredParts := make([]metadata.Part, 0)
			for _, part := range parts {
				if filesystemhelper.IsPartInPartition(part.Name, partitionsFilter) {
					filteredParts = append(filteredParts, part)
				}
			}
			tableMetadata.Parts[disk] = filteredParts
		}
		for disk, files := range tableMetadata.Files {
			filteredFiles := make([]string, 0)
			for _, file := range files {
				if filesystemhelper.IsFileInPartition(disk, file, partitionsFilter) {
					filteredFiles = append(filteredFiles, file)
				}
			}
			tableMetadata.Files[disk] = filteredFiles
		}
	}
}

func getTableListByPatternRemote(ctx context.Context, b *Backuper, remoteBackupMetadata *metadata.BackupMetadata, tablePattern string, dropTable bool) (ListOfTables, error) {
	result := ListOfTables{}
	tablePatterns := []string{"*"}

	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	metadataPath := path.Join(remoteBackupMetadata.BackupName, "metadata")
	// https://github.com/Altinity/clickhouse-backup/issues/1091
	replacer := strings.NewReplacer(`/`, "_", `\`, "_")
	for _, t := range remoteBackupMetadata.Tables {
		if IsInformationSchema(t.Database) {
			continue
		}
		tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
		if shallSkipped := b.shouldSkipByTableName(tableName); shallSkipped {
			continue
		}
	tablePatterns:
		for _, pattern := range tablePatterns {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				// https://github.com/Altinity/clickhouse-backup/issues/1091
				if matched, _ := filepath.Match(replacer.Replace(strings.Trim(pattern, " \t\r\n")), replacer.Replace(tableName)); !matched {
					continue
				}
				tmReader, err := b.dst.GetFileReader(ctx, path.Join(metadataPath, common.TablePathEncode(t.Database), fmt.Sprintf("%s.json", common.TablePathEncode(t.Table))))
				if err != nil {
					return nil, err
				}
				data, err := io.ReadAll(tmReader)
				if err != nil {
					return nil, err
				}
				err = tmReader.Close()
				if err != nil {
					return nil, err
				}
				var t metadata.TableMetadata
				if err = json.Unmarshal(data, &t); err != nil {
					return nil, err
				}
				result = addTableToListIfNotExistsOrEnrichQueryAndParts(result, t)
				break tablePatterns
			}
		}
	}
	result.Sort(dropTable)
	return result, nil
}

var lowPriorityEnginesRE = regexp.MustCompile(`ENGINE = (Distributed|Dictionary|Merge)\(`)
var streamingEnginesRE = regexp.MustCompile(`ENGINE = (Kafka|NATS|RabbitMQ|S3Queue)`)

func getOrderByEngine(query string, dropTable bool) int64 {
	if lowPriorityEnginesRE.MatchString(query) {
		return 5
	}

	if streamingEnginesRE.MatchString(query) {
		return 4
	}
	if strings.HasPrefix(query, "CREATE DICTIONARY") {
		return 3
	}
	if strings.HasPrefix(query, "CREATE VIEW") ||
		strings.HasPrefix(query, "CREATE LIVE VIEW") ||
		strings.HasPrefix(query, "CREATE WINDOW VIEW") ||
		strings.HasPrefix(query, "ATTACH WINDOW VIEW") ||
		strings.HasPrefix(query, "CREATE MATERIALIZED VIEW") ||
		strings.HasPrefix(query, "ATTACH MATERIALIZED VIEW") {
		if dropTable {
			return 1
		} else {
			return 2
		}
	}

	if strings.HasPrefix(query, "CREATE TABLE") &&
		(strings.Contains(query, ".inner_id.") || strings.Contains(query, ".inner.")) {
		if dropTable {
			return 2
		} else {
			return 1
		}
	}
	return 0
}

func parseTablePatternForDownload(tables []metadata.TableTitle, tablePattern string) []metadata.TableTitle {
	tablePatterns := []string{"*"}
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	var result []metadata.TableTitle
	// https://github.com/Altinity/clickhouse-backup/issues/1091
	replacer := strings.NewReplacer("/", "_", `\`, "_")

	for _, t := range tables {
		for _, pattern := range tablePatterns {
			tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
			// https://github.com/Altinity/clickhouse-backup/issues/1091
			if matched, _ := filepath.Match(replacer.Replace(strings.Trim(pattern, " \t\r\n")), replacer.Replace(tableName)); matched {
				result = append(result, t)
				break
			}
		}
	}
	return result
}

func IsInformationSchema(database string) bool {
	for _, skipDatabase := range []string{"INFORMATION_SCHEMA", "information_schema", "_temporary_and_external_tables"} {
		if database == skipDatabase {
			return true
		}
	}
	return false
}

func ShallSkipDatabase(cfg *config.Config, targetDB, tablePattern string) bool {
	if tablePattern != "" {
		var bypassTablePatterns []string
		bypassTablePatterns = append(bypassTablePatterns, strings.Split(tablePattern, ",")...)
		for _, pattern := range bypassTablePatterns {
			pattern = strings.Trim(pattern, " \r\t\n")
			if strings.HasSuffix(pattern, ".*") && strings.TrimSuffix(pattern, ".*") == targetDB {
				return false
			}
			// https://github.com/Altinity/clickhouse-backup/issues/663
			if matched, err := filepath.Match(pattern, targetDB+"."); err == nil && matched {
				return false
			}
		}
		return true
	}

	if len(cfg.ClickHouse.SkipTables) > 0 {
		var skipTablesPatterns []string
		skipTablesPatterns = append(skipTablesPatterns, cfg.ClickHouse.SkipTables...)
		for _, pattern := range skipTablesPatterns {
			pattern = strings.Trim(pattern, " \r\t\n")
			// https://github.com/Altinity/clickhouse-backup/issues/663
			if matched, err := filepath.Match(pattern, targetDB+"."); err == nil && matched {
				return true
			}
		}
	}
	return false
}
