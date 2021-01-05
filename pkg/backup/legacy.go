package backup

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func parseSchemaPattern(metadataPath string, tablePattern string) (RestoreTables, error) {
	regularTables := RestoreTables{}
	distributedTables := RestoreTables{}
	viewTables := RestoreTables{}
	tablePatterns := []string{"*"}

	//log.Printf("tp1 = %s", tablePattern)
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	if err := filepath.Walk(metadataPath, func(filePath string, info os.FileInfo, err error) error {
		if !strings.HasSuffix(filePath, ".sql") || !info.Mode().IsRegular() {
			return nil
		}
		p := filepath.ToSlash(filePath)
		//log.Printf("p1 = %s", p)
		p = strings.Trim(strings.TrimPrefix(strings.TrimSuffix(p, ".sql"), metadataPath), "/")
		//log.Printf("p2 = %s", p)
		parts := strings.Split(p, "/")
		if len(parts) != 2 {
			return nil
		}
		database, _ := url.PathUnescape(parts[0])
		table, _ := url.PathUnescape(parts[1])
		tableName := fmt.Sprintf("%s.%s", database, table)
		for _, p := range tablePatterns {
			//log.Printf("p3 = %s", p)
			if matched, _ := filepath.Match(p, tableName); matched {
				data, err := ioutil.ReadFile(filePath)
				if err != nil {
					return err
				}
				restoreTable := RestoreTable{
					Database: database,
					Table:    table,
					Query:    strings.Replace(string(data), "ATTACH", "CREATE", 1),
					Path:     filePath,
				}
				if strings.Contains(restoreTable.Query, "ENGINE = Distributed") {
					distributedTables = addRestoreTable(distributedTables, restoreTable)
					return nil
				}
				if strings.HasPrefix(restoreTable.Query, "CREATE VIEW") ||
					strings.HasPrefix(restoreTable.Query, "CREATE MATERIALIZED VIEW") {
					viewTables = addRestoreTable(viewTables, restoreTable)
					return nil
				}
				regularTables = addRestoreTable(regularTables, restoreTable)
				return nil
			}
			/*else {
				log.Printf("No match %s %s", p, tableName)
			}*/
		}
		return nil
	}); err != nil {
		return nil, err
	}
	regularTables.Sort()
	distributedTables.Sort()
	viewTables.Sort()
	result := append(regularTables, distributedTables...)
	result = append(result, viewTables...)
	return result, nil
}
