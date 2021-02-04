package backup

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
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

func parseSchemaPattern(metadataPath string, tablePattern string) (RestoreTables, error) {
	result := RestoreTables{}
	tablePatterns := []string{"*"}

	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	if err := filepath.Walk(metadataPath, func(filePath string, info os.FileInfo, err error) error {
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
		table, _ := url.PathUnescape(parts[1])
		tableName := fmt.Sprintf("%s.%s", database, table)
		for _, p := range tablePatterns {
			if matched, _ := filepath.Match(p, tableName); !matched {
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
	result.Sort()
	return result, nil
}

func getOrderByEngine(query string) int64 {
	if strings.Contains(query, "ENGINE = Distributed") {
		return 1
	}
	if strings.HasPrefix(query, "CREATE VIEW") ||
		strings.HasPrefix(query, "CREATE MATERIALIZED VIEW") {
		return 2
	}
	return 0
}

func parseTablePatternForRestoreData(tables map[string]metadata.TableMetadata, tablePattern string) clickhouse.BackupTables {
	tablePatterns := []string{"*"}
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	result := clickhouse.BackupTables{}
	for _, t := range tables {
		for _, pattern := range tablePatterns {
			tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
			if matched, _ := filepath.Match(pattern, tableName); matched {
				result = addBackupTable(result, t)
			}
		}
	}
	result.Sort()
	return result
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
			if matched, _ := filepath.Match(pattern, tableName); matched {
				result = append(result, t)
				break
			}
		}
	}
	return result
}
