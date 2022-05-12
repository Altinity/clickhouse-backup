package backup

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/pkg/common"
	"github.com/AlexAkulov/clickhouse-backup/pkg/filesystemhelper"

	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
)

type ListOfTables []metadata.TableMetadata

// Sort - sorting ListOfTables slice orderly by engine priority
func (lt ListOfTables) Sort(dropTable bool) {
	sort.Slice(lt, func(i, j int) bool {
		return getOrderByEngine(lt[i].Query, dropTable) < getOrderByEngine(lt[j].Query, dropTable)
	})
}

func addTableToListIfNotExists(tables ListOfTables, table metadata.TableMetadata) ListOfTables {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Table == table.Table) {
			return tables
		}
	}
	return append(tables, table)
}

func getTableListByPatternLocal(metadataPath string, tablePattern string, skipTables []string, dropTable bool, partitionsFilter common.EmptyMap) (ListOfTables, error) {
	result := ListOfTables{}
	tablePatterns := []string{"*"}

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
		legacy := false
		if strings.HasSuffix(p, ".sql") {
			legacy = true
			p = strings.TrimSuffix(p, ".sql")
		} else {
			p = strings.TrimSuffix(p, ".json")
		}
		p = strings.Trim(strings.TrimPrefix(p, metadataPath), "/")
		parts := strings.Split(p, "/")
		if len(parts) != 2 {
			return nil
		}
		database, _ := url.PathUnescape(parts[0])
		if IsInformationSchema(database) {
			return nil
		}
		table, _ := url.PathUnescape(parts[1])
		tableName := fmt.Sprintf("%s.%s", database, table)
		shallSkipped := false
		for _, skipPattern := range skipTables {
			if shallSkipped, _ = filepath.Match(skipPattern, tableName); shallSkipped {
				break
			}
		}
		for _, p := range tablePatterns {
			if matched, _ := filepath.Match(strings.Trim(p, " \t\r\n"), tableName); !matched || shallSkipped {
				continue
			}
			data, err := ioutil.ReadFile(filePath)
			if err != nil {
				return err
			}
			if legacy {
				result = addTableToListIfNotExists(result, metadata.TableMetadata{
					Database: database,
					Table:    table,
					Query:    strings.Replace(string(data), "ATTACH", "CREATE", 1),
					// Path:     filePath,
				})
				return nil
			}
			var t metadata.TableMetadata
			if err := json.Unmarshal(data, &t); err != nil {
				return err
			}
			filterPartsByPartitionsFilter(t, partitionsFilter)
			result = addTableToListIfNotExists(result, t)
			return nil
		}
		return nil
	}); err != nil {
		return nil, err
	}
	result.Sort(dropTable)
	return result, nil
}

func getTableListByRestoreDatabaseMappingRule(originTables *ListOfTables, dbMapRule map[string]string) error {
	result := ListOfTables{}
	dbMetaMap := make(map[string]metadata.TableMetadata, 0)
	for i := 0; i < len(*originTables); i++ {
		table := (*originTables)[i]
		dbMetaMap[table.Database] = table
	}
	for i := 0; i < len(*originTables); i++ {
		originDBMeta := (*originTables)[i]
		if targetDB, ok := dbMapRule[originDBMeta.Database]; ok {
			regExp := regexp.MustCompile(`(?m)^(CREATE|ATTACH) (TABLE|VIEW|MATERIALIZED VIEW|DICTiONARY|FUNCTION) ([\x60]?)([^\s\x60\.]*)([\x60]?)\.([^\s\x60\.]*)(?:( TO )([\x60]?)([^\s\x60\.]*)([\x60]?)(\.))?`)
			var substitution string

			if len(regexp.MustCompile(`(?m)^CREATE`).FindAllString(originDBMeta.Query, -1)) > 0 {
				substitution = fmt.Sprintf("${1} ${2} ${3}%v${5}.${6}", targetDB)
			} else if len(regexp.MustCompile(`(?m)^ATTACH`).FindAllString(originDBMeta.Query, -1)) > 0 {
				substitution = fmt.Sprintf("${1} ${2} ${3}%v${5}.${6}${7}${8}%v${11}", targetDB, targetDB)
			} else {
				return fmt.Errorf("there is an error when substituting the default database in query, try to open an issue on Github and we will fix it :)")
			}

			originDBMeta.Query = regExp.ReplaceAllString(originDBMeta.Query, substitution)
			originDBMeta.Database = targetDB
			result = append(result, originDBMeta)
		}
	}
	if len(result) == 0 {
		return fmt.Errorf("no datababse/schema matched in restore-database-mapping rules")
	}

	*originTables = result
	return nil
}

func filterPartsByPartitionsFilter(tableMetadata metadata.TableMetadata, partitionsFilter common.EmptyMap) {
	if len(partitionsFilter) > 0 {
		for disk, parts := range tableMetadata.Parts {
			for i, part := range parts {
				if !filesystemhelper.IsPartInPartition(part.Name, partitionsFilter) {
					parts = append(parts[:i], parts[i+1:]...)
				}
			}
			tableMetadata.Parts[disk] = parts
		}
	}
}

func getTableListByPatternRemote(b *Backuper, remoteBackupMetadata *metadata.BackupMetadata, tablePattern string, skipTables []string, dropTable bool) (ListOfTables, error) {
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
		for _, skipPattern := range skipTables {
			if shallSkipped, _ = filepath.Match(skipPattern, tableName); shallSkipped {
				break
			}
		}
		for _, p := range tablePatterns {
			if matched, _ := filepath.Match(strings.Trim(p, " \t\r\n"), tableName); !matched || shallSkipped {
				continue
			}
			tmReader, err := b.dst.GetFileReader(path.Join(metadataPath, common.TablePathEncode(t.Database), fmt.Sprintf("%s.json", common.TablePathEncode(t.Table))))
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
			result = addTableToListIfNotExists(result, t)
			break
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
	for _, skipDatabase := range []string{"INFORMATION_SCHEMA", "information_schema"} {
		if database == skipDatabase {
			return true
		}
	}
	return false
}
