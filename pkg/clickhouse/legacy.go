package clickhouse

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/apex/log"
)

// // GetBackupTables - return list of backups of tables that can be restored
// func (ch *ClickHouse) GetBackupTables(backupName string) (map[string]BackupTable, error) {
// 	dataPath, err := ch.GetDefaultPath()
// 	if err != nil {
// 		return nil, err
// 	}
// 	metadataDir := filepath.Join(dataPath, "backup", backupName, "metadata")
// 	result := map[string]BackupTable{}

// 	err = filepath.Walk(metadataDir, func(filePath string, info os.FileInfo, err error) error {
// 		if err != nil {
// 			return err
// 		}
// 		if !strings.HasSuffix(info.Name(), ".json") || info.IsDir() {
// 			return nil
// 		}
// 		if info.IsDir() {
// 			filePath = filepath.ToSlash(filePath) // fix fucking Windows slashes
// 			relativePath := strings.Trim(strings.TrimPrefix(filePath, backupShadowPath), "/")
// 			parts := strings.Split(relativePath, "/")
// 			if len(parts) != totalNum {
// 				return nil
// 			}

// 			tDB, _ := url.PathUnescape(parts[dbNum])
// 			tName, _ := url.PathUnescape(parts[tableNum])
// 			fullTableName := fmt.Sprintf("%s.%s", tDB, tName)

// 			allparthash := allpartsBackup[fullTableName]
// 			var hoaf, houf, uhocf string
// 			for _, parthash := range allparthash {
// 				if parthash.Name == parts[partNum] {
// 					hoaf = parthash.HashOfAllFiles
// 					houf = parthash.HashOfUncompressedFiles
// 					uhocf = parthash.UncompressedHashOfCompressedFiles
// 				}
// 			}

// 			partition := BackupPartition{
// 				Name:                              parts[partNum],
// 				Path:                              filePath,
// 				HashOfAllFiles:                    hoaf,
// 				HashOfUncompressedFiles:           houf,
// 				UncompressedHashOfCompressedFiles: uhocf,
// 			}

// 			if t, ok := result[fullTableName]; ok {
// 				t.Partitions["default"] = append(t.Partitions["default"], partition)
// 				result[fullTableName] = t
// 				return nil
// 			}
// 			result[fullTableName] = BackupTable{
// 				Database:   tDB,
// 				Name:       tName,
// 				Partitions: map[string][]BackupPartition{"default": {partition}},
// 			}
// 			return nil
// 		}
// 		return nil
// 	})
// 	return result, err
// }

// GetBackupTablesLegacy - return list of backups of tables that can be restored
func (ch *ClickHouse) GetBackupTablesLegacy(backupName string) (map[string]metadata.TableMetadata, error) {
	dataPath, err := ch.GetDefaultPath()
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
	// hashPath := path.Join(dataPath, "backup", backupName, hashfile)
	// log.Printf("Reading part hashes %s", hashPath)
	// bytes, err := ioutil.ReadFile(hashPath)
	// if err != nil {
	// 	log.Printf("Unable to read hash file %s", hashPath)
	// 	//return nil, fmt.Errorf("Unable to read hash file %s", hashPath)
	// } else {
	// 	err = json.Unmarshal(bytes, &allpartsBackup)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("issue occurred while reading hash file %s", hashPath)
	// 	}
	// }
	// TODO: нам больше не нужно заполнять Partitions из файла теперь их можно взять из таблицы detached
	result := make(map[string]metadata.TableMetadata)
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
				Name:                              parts[partNum],
				Path:                              filePath,
				HashOfAllFiles:                    hoaf,
				HashOfUncompressedFiles:           houf,
				UncompressedHashOfCompressedFiles: uhocf,
			}

			if t, ok := result[fullTableName]; ok {
				t.Parts["default"] = append(t.Parts["default"], partition)
				result[fullTableName] = t
				return nil
			}
			result[fullTableName] = metadata.TableMetadata{
				Database: tDB,
				Table:    tName,
				Parts:    map[string][]metadata.Part{"default": {partition}},
			}
			return nil
		}
		return nil
	})
	return result, err
}

// CopyData - copy partitions for specific table to detached folder
func (ch *ClickHouse) CopyDataLegacy(table metadata.TableMetadata) error {
	dataPath, err := ch.GetDefaultPath()
	if err != nil {
		return err
	}
	detachedParentDir := filepath.Join(dataPath, "data", TablePathEncode(table.Database), TablePathEncode(table.Table), "detached")
	os.MkdirAll(detachedParentDir, 0750)
	ch.Chown(detachedParentDir)

	for _, partition := range table.Parts["default"] {
		// log.Printf("partition name is %s (%s)", partition.Name, partition.Path)
		detachedPath := filepath.Join(detachedParentDir, partition.Name)
		info, err := os.Stat(detachedPath)
		if err != nil {
			if os.IsNotExist(err) {
				// partition dir does not exist, creating
				os.MkdirAll(detachedPath, 0750)
			} else {
				return err
			}
		} else if !info.IsDir() {
			return fmt.Errorf("'%s' should be directory or absent", detachedPath)
		}
		ch.Chown(detachedPath)

		if err := filepath.Walk(partition.Path, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			filePath = filepath.ToSlash(filePath) // fix Windows slashes
			filename := strings.Trim(strings.TrimPrefix(filePath, partition.Path), "/")
			dstFilePath := filepath.Join(detachedPath, filename)
			if info.IsDir() {
				os.MkdirAll(dstFilePath, 0750)
				return ch.Chown(dstFilePath)
			}
			if !info.Mode().IsRegular() {
				log.Debugf("'%s' is not a regular file, skipping.", filePath)
				return nil
			}
			if err := os.Link(filePath, dstFilePath); err != nil {
				return fmt.Errorf("failed to crete hard link '%s' -> '%s': %v", filePath, dstFilePath, err)
			}
			return ch.Chown(dstFilePath)
		}); err != nil {
			return fmt.Errorf("error during filepath.Walk for partition '%s': %v", partition.Path, err)
		}
	}
	return nil
}
