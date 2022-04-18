package clickhouse

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
)

// GetBackupTablesLegacy - return list of backups of tables that can be restored
func (ch *ClickHouse) GetBackupTablesLegacy(backupName string, disks []Disk) ([]metadata.TableMetadata, error) {
	dataPath, err := ch.GetDefaultPath(disks)
	if err != nil {
		return nil, err
	}
	backupShadowPath := filepath.Join(dataPath, "backup", backupName, "shadow")
	dbNum := 0
	tableNum := 1
	partNum := 2
	totalNum := 3
	if IsClickhouseShadow(backupShadowPath) {
		dbNum = 2
		tableNum = 3
		partNum = 4
		totalNum = 5
	}
	fi, err := os.Stat(backupShadowPath)
	if err != nil {
		return nil, fmt.Errorf("can't get tables, %v", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("can't get tables, %s is not a dir", backupShadowPath)
	}

	var allpartsBackup map[string][]metadata.Part
	// TODO: we don't need anymore fill Partitions from file, we can get it from `system.detached_parts` table
	tables := make(map[string]metadata.TableMetadata)
	err = filepath.Walk(backupShadowPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			filePath = filepath.ToSlash(filePath) // fix fucking Windows slashes
			relativePath := strings.Trim(strings.TrimPrefix(filePath, backupShadowPath), "/")
			parts := strings.Split(relativePath, "/")
			if len(parts) != totalNum {
				return nil
			}

			tDB, _ := url.PathUnescape(parts[dbNum])
			tName, _ := url.PathUnescape(parts[tableNum])
			fullTableName := fmt.Sprintf("%s.%s", tDB, tName)

			allparthash := allpartsBackup[fullTableName]
			var hoaf, houf, uhocf string
			for _, parthash := range allparthash {
				if parthash.Name == parts[partNum] {
					hoaf = parthash.HashOfAllFiles
					houf = parthash.HashOfUncompressedFiles
					uhocf = parthash.UncompressedHashOfCompressedFiles
				}
			}

			partition := metadata.Part{
				Name: parts[partNum],
				// Path:                              filePath,
				HashOfAllFiles:                    hoaf,
				HashOfUncompressedFiles:           houf,
				UncompressedHashOfCompressedFiles: uhocf,
			}

			if t, ok := tables[fullTableName]; ok {
				t.Parts["default"] = append(t.Parts["default"], partition)
				tables[fullTableName] = t
				return nil
			}
			tables[fullTableName] = metadata.TableMetadata{
				Database: tDB,
				Table:    tName,
				Parts:    map[string][]metadata.Part{"default": {partition}},
			}
			return nil
		}
		return nil
	})
	result := []metadata.TableMetadata{}
	for i := range tables {
		result = append(result, tables[i])
	}
	return result, err
}
