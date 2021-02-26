package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/pkg/new_storage"

	apexLog "github.com/apex/log"
)

func Upload(cfg *config.Config, backupName string, tablePattern string, diffFrom string, schemaOnly bool) error {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("Upload aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		PrintLocalBackups(cfg, "all")
		return fmt.Errorf("select backup for upload")
	}
	if diffFrom != "" {
		return fmt.Errorf("diff-from is not supported yet")
	}
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "upload",
	})
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	bd, err := new_storage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}
	if err := bd.Connect(); err != nil {
		return fmt.Errorf("can't connect to %s: %v", bd.Kind(), err)
	}

	if _, err := getLocalBackup(cfg, backupName); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}
	defaulDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	diskMap := map[string]string{}
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
	}
	// TODO: проверяем существует ли бэкап на удалённом сторадже
	metadataPath := path.Join(defaulDataPath, "backup", backupName, "metadata")
	if _, err := os.Stat(metadataPath); err != nil {
		return err
	}
	tablesForUpload, err := parseSchemaPattern(metadataPath, tablePattern)

	for _, table := range tablesForUpload {
		uuid := path.Join(clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table))
		// if table.UUID != "" {
		// 	uuid = path.Join(table.UUID[0:3], table.UUID)
		// }
		metdataFiles := map[string][]string{}
		if !schemaOnly {
			for disk := range table.Parts {
				backupPath := path.Join(diskMap[disk], "backup", backupName, "shadow", uuid, disk)
				parts, err := separateParts(backupPath, table.Parts[disk], cfg.General.MaxFileSize)
				if err != nil {
					return err
				}
				for i, p := range parts {
					remoteDataPath := path.Join(backupName, "shadow", clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table))
					var err error
					if cfg.GetCompressionFormat() == "none" {
						err = bd.UploadPath(0, backupPath, p, path.Join(remoteDataPath, disk))
					} else {
						fileName := fmt.Sprintf("%s_%d.%s", disk, i+1, cfg.GetArchiveExtension())
						metdataFiles[disk] = append(metdataFiles[disk], fileName)
						remoteDataFile := path.Join(remoteDataPath, fileName)
						err = bd.CompressedStreamUpload(backupPath, p, remoteDataFile)
					}
					if err != nil {
						return fmt.Errorf("can't upload: %v", err)
					}
				}
			}
		}
		// заливаем метадату для таблицы
		tableMetafile := table
		tableMetafile.Files = metdataFiles
		content, err := json.MarshalIndent(&tableMetafile, "", "\t")
		if err != nil {
			return fmt.Errorf("can't marshal json: %v", err)
		}
		remoteTableMetaFile := path.Join(backupName, "metadata", clickhouse.TablePathEncode(table.Database), fmt.Sprintf("%s.%s", clickhouse.TablePathEncode(table.Table), "json"))
		if err := bd.PutFile(remoteTableMetaFile,
			ioutil.NopCloser(bytes.NewReader(content))); err != nil {
			return fmt.Errorf("can't upload: %v", err)
		}
		log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).Info("done")
	}
	t := []metadata.TableTitle{}
	for i := range tablesForUpload {
		t = append(t, metadata.TableTitle{
			Database: tablesForUpload[i].Database,
			Table:    tablesForUpload[i].Table,
		})
	}
	// заливаем метадату для бэкапа
	backupMetadataPath := path.Join(defaulDataPath, "backup", backupName, "metadata.json")
	backupMetadataBody, err := ioutil.ReadFile(backupMetadataPath)
	if err != nil {
		return err
	}
	// TODO: тут нужно менять размер если заливаем только схему или часть таблиц
	backupMetadata := metadata.BackupMetadata{}
	if err := json.Unmarshal(backupMetadataBody, &backupMetadata); err != nil {
		return err
	}
	if cfg.GetCompressionFormat() != "none" {
		backupMetadata.DataFormat = cfg.GetCompressionFormat()
	} else {
		backupMetadata.DataFormat = "directory"
	}
	newBackupMetadataBody, err := json.MarshalIndent(backupMetadata, "", "\t")
	if err != nil {
		return err
	}
	remoteBackupMetaFile := path.Join(backupName, "metadata.json")
	if err := bd.PutFile(remoteBackupMetaFile,
		ioutil.NopCloser(bytes.NewReader(newBackupMetadataBody))); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}

	if err := bd.RemoveOldBackups(bd.BackupsToKeep()); err != nil {
		return fmt.Errorf("can't remove old backups: %v", err)
	}
	log.Infof("done")
	return nil
}

func separateParts(basePath string, parts []metadata.Part, maxSize int64) ([][]string, error) {
	var size int64
	files := []string{}
	result := [][]string{}
	for i := range parts {
		partPath := path.Join(basePath, parts[i].Name)
		filepath.Walk(partPath, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			if (size + info.Size()) > maxSize {
				result = append(result, files)
				files = []string{}
				size = 0
			}
			relativePath := strings.TrimPrefix(filePath, basePath)
			files = append(files, relativePath)
			size += info.Size()
			return nil
		})
	}
	if len(files) > 0 {
		result = append(result, files)
	}
	return result, nil
}
