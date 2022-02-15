package backup

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
)

func addRestoreTable(tables RestoreTables, table metadata.TableMetadata) RestoreTables {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Table == table.Table) {
			return tables
		}
	}
	return append(tables, table)
}

func parseSchemaPattern(metadataPath string, tablePattern string, skipTables []string, dropTable bool) (RestoreTables, error) {
	result := RestoreTables{}
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
				result = addRestoreTable(result, metadata.TableMetadata{
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
			result = addRestoreTable(result, t)
			return nil
		}
		return nil
	}); err != nil {
		return nil, err
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
	result := []metadata.TableTitle{}
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
