package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/partition"
	apexLog "github.com/apex/log"
	"github.com/google/uuid"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/Altinity/clickhouse-backup/pkg/common"
	"github.com/Altinity/clickhouse-backup/pkg/filesystemhelper"

	"github.com/Altinity/clickhouse-backup/pkg/metadata"
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

func getTableListByPatternLocal(ctx context.Context, cfg *config.Config, ch *clickhouse.ClickHouse, metadataPath string, tablePattern string, dropTable bool, partitions []string) (ListOfTables, map[metadata.TableTitle][]string, error) {
	result := ListOfTables{}
	resultPartitionNames := map[metadata.TableTitle][]string{}
	tablePatterns := []string{"*"}
	log := apexLog.WithField("logger", "getTableListByPatternLocal")
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
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
		p = strings.Trim(strings.TrimPrefix(p, metadataPath), "/")
		names := strings.Split(p, "/")
		if len(names) != 2 {
			return nil
		}
		database, _ := url.PathUnescape(names[0])
		if IsInformationSchema(database) {
			return nil
		}
		table, _ := url.PathUnescape(names[1])
		tableName := fmt.Sprintf("%s.%s", database, table)
		shallSkipped := false
		for _, skipPattern := range cfg.ClickHouse.SkipTables {
			if shallSkipped, _ = filepath.Match(skipPattern, tableName); shallSkipped {
				break
			}
		}
		for _, p := range tablePatterns {
			if matched, _ := filepath.Match(strings.Trim(p, " \t\r\n"), tableName); !matched || shallSkipped {
				continue
			}
			data, err := os.ReadFile(filePath)
			if err != nil {
				return err
			}
			if isEmbeddedMetadata {
				// embedded backup to s3 disk could contain only s3 key names inside .sql file
				query := string(data)
				if strings.HasPrefix(query, "ATTACH") || strings.HasPrefix(query, "CREATE") {
					query = strings.Replace(query, "ATTACH", "CREATE", 1)
				} else {
					query = ""
				}
				dataPartsPath := strings.Replace(metadataPath, "/metadata", "/data", 1)
				dataPartsPath = path.Join(dataPartsPath, path.Join(names...))
				if _, err := os.Stat(dataPartsPath); err != nil && !os.IsNotExist(err) {
					return err
				}
				dataParts, err := os.ReadDir(dataPartsPath)
				if err != nil {
					log.Warn(err.Error())
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
				// .sql file will enrich Query
				partitionsIdMap, _ := partition.ConvertPartitionsToIdsMapAndNamesList(ctx, ch, nil, []metadata.TableMetadata{t}, partitions)
				filterPartsAndFilesByPartitionsFilter(t, partitionsIdMap[metadata.TableTitle{Database: t.Database, Table: t.Table}])
				result = addTableToListIfNotExistsOrEnrichQueryAndParts(result, t)
				return nil
			}
			var t metadata.TableMetadata
			if err := json.Unmarshal(data, &t); err != nil {
				return err
			}
			partitionsIdMap, partitionsNameList := partition.ConvertPartitionsToIdsMapAndNamesList(ctx, ch, nil, []metadata.TableMetadata{t}, partitions)
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
	return result, resultPartitionNames, nil
}

var queryRE = regexp.MustCompile(`(?m)^(CREATE|ATTACH) (TABLE|VIEW|LIVE VIEW|MATERIALIZED VIEW|DICTIONARY|FUNCTION) (\x60?)([^\s\x60.]*)(\x60?)\.([^\s\x60.]*)(?:( UUID '[^']+'))?(?:( TO )(\x60?)([^\s\x60.]*)(\x60?)(\.))?(?:(.+FROM )(\x60?)([^\s\x60.]*)(\x60?)(\.))?`)
var createOrAttachRE = regexp.MustCompile(`(?m)^(CREATE|ATTACH)`)
var uuidRE = regexp.MustCompile(`UUID '[a-f\d\-]+'`)

var replicatedRE = regexp.MustCompile(`(Replicated[a-zA-Z]*MergeTree)\('([^']+)'([^)]+)\)`)
var distributedRE = regexp.MustCompile(`(Distributed)\(([^,]+),([^,]+),([^)]+)\)`)

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
					}
					return clauseTargetDb
				}
				toClauseTargetDb := setMatchedDb(matches[0][10])
				fromClauseTargetDb := setMatchedDb(matches[0][15])
				// matching CREATE|ATTACH ... TO .. SELECT ... FROM ... command
				substitution = fmt.Sprintf("${1} ${2} ${3}%v${5}.${6}${7}${8}${9}%v${11}${12}${13}${14}%v${16}${17}", targetDB, toClauseTargetDb, fromClauseTargetDb)
			} else {
				if originTable.Query == "" {
					continue
				}
				return fmt.Errorf("error when try to replace database `%s` to `%s` in query: %s", originTable.Database, targetDB, originTable.Query)
			}
			originTable.Query = queryRE.ReplaceAllString(originTable.Query, substitution)
			if len(uuidRE.FindAllString(originTable.Query, -1)) > 0 {
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
					substitution = fmt.Sprintf("${1}(${2},%s,${4})", strings.Replace(underlyingDB, underlyingDBClean, underlyingTargetDB, 1))
					originTable.Query = distributedRE.ReplaceAllString(originTable.Query, substitution)
				}
			}
			originTable.Database = targetDB
			(*originTables)[i] = originTable
		}
	}
	return nil
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
	for _, t := range remoteBackupMetadata.Tables {
		if IsInformationSchema(t.Database) {
			continue
		}
		tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
		shallSkipped := false
		for _, skipPattern := range b.cfg.ClickHouse.SkipTables {
			if shallSkipped, _ = filepath.Match(skipPattern, tableName); shallSkipped {
				break
			}
		}
	tablePatterns:
		for _, p := range tablePatterns {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				if matched, _ := filepath.Match(strings.Trim(p, " \t\r\n"), tableName); !matched || shallSkipped {
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

func getOrderByEngine(query string, dropTable bool) int64 {
	if strings.Contains(query, "ENGINE = Distributed") || strings.Contains(query, "ENGINE = Kafka") || strings.Contains(query, "ENGINE = RabbitMQ") {
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
	for _, t := range tables {
		for _, pattern := range tablePatterns {
			tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
			if matched, _ := filepath.Match(strings.Trim(pattern, " \t\r\n"), tableName); matched {
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
		}
		return true
	}

	if len(cfg.ClickHouse.SkipTables) > 0 {
		var skipTablesPatterns []string
		skipTablesPatterns = append(skipTablesPatterns, cfg.ClickHouse.SkipTables...)
		for _, pattern := range skipTablesPatterns {
			pattern = strings.Trim(pattern, " \r\t\n")
			if strings.HasSuffix(pattern, ".*") && strings.TrimSuffix(pattern, ".*") == targetDB {
				return true
			}
		}
	}
	return false
}
