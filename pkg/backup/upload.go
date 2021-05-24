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

	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"

	apexLog "github.com/apex/log"
)

func (b *Backuper) Upload(backupName string, tablePattern string, diffFrom string, schemaOnly bool) error {
	if b.cfg.General.RemoteStorage == "none" {
		fmt.Println("Upload aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		PrintLocalBackups(b.cfg, "all")
		return fmt.Errorf("select backup for upload")
	}
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "upload",
	})
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()
	if err := b.init(); err != nil {
		return err
	}
	if _, err := getLocalBackup(b.cfg, backupName); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}
	remoteBackups, err := b.dst.BackupList()
	if err != nil {
		return err
	}
	for i := range remoteBackups {
		if backupName == remoteBackups[i].BackupName {
			return fmt.Errorf("'%s' already exists on remote", backupName)
		}
	}
	backupMetadata, err := b.ReadBackupMetadata(backupName)
	if err != nil {
		return err
	}
	var tablesForUpload RestoreTables
	if len(backupMetadata.Tables) != 0 {
		metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
		tablesForUpload, err = parseSchemaPattern(metadataPath, tablePattern, false)
		if err != nil {
			return err
		}
	}
	dataSize := int64(0)
	metadataSize := int64(0)
	var diffFromBackup *metadata.BackupMetadata
	tablesForUploadFromDiff := map[metadata.TableTitle]metadata.TableMetadata{}
	if diffFrom != "" {
		diffFromBackup, err = b.ReadBackupMetadata(diffFrom)
		if err != nil {
			return err
		}
		if len(diffFromBackup.Tables) != 0 {
			metadataPath := path.Join(b.DefaultDataPath, "backup", diffFrom, "metadata")
			diffTablesList, err := parseSchemaPattern(metadataPath, tablePattern, false)
			if err != nil {
				return err
			}
			for _, t := range diffTablesList {
				tablesForUploadFromDiff[metadata.TableTitle{
					Database: t.Database,
					Table:    t.Table,
				}] = t
			}
		}
	}
	for _, table := range tablesForUpload {
		// if table.UUID != "" {
		// 	uuid = path.Join(table.UUID[0:3], table.UUID)
		// }
		if !schemaOnly {
			if diffTable, ok := tablesForUploadFromDiff[metadata.TableTitle{
				Database: table.Database,
				Table:    table.Table,
			}]; ok {
				markDuplicatedDiskParts(diffTable.Parts, table.Parts)
			}
			dataSize += table.TotalBytes
			metadataFiles, err := b.uploadTableData(backupName, table)
			if err != nil {
				return err
			}
			table.Files = metadataFiles
		}
		tableMetadataSize, err := b.uploadTableMetadata(backupName, table)
		if err != nil {
			return err
		}
		metadataSize += tableMetadataSize
		log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).Info("done")
	}

	// заливаем метадату для бэкапа
	backupMetadata.DataSize = dataSize
	backupMetadata.MetadataSize = metadataSize
	tt := []metadata.TableTitle{}
	for i := range tablesForUpload {
		tt = append(tt, metadata.TableTitle{
			Database: tablesForUpload[i].Database,
			Table:    tablesForUpload[i].Table,
		})
	}
	backupMetadata.Tables = tt
	backupMetadata.RequiredBackup = diffFrom
	if b.cfg.GetCompressionFormat() != "none" {
		backupMetadata.DataFormat = b.cfg.GetCompressionFormat()
	} else {
		backupMetadata.DataFormat = "directory"
	}
	newBackupMetadataBody, err := json.MarshalIndent(backupMetadata, "", "\t")
	if err != nil {
		return err
	}
	remoteBackupMetaFile := path.Join(backupName, "metadata.json")
	if err := b.dst.PutFile(remoteBackupMetaFile,
		ioutil.NopCloser(bytes.NewReader(newBackupMetadataBody))); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}

	if err := b.dst.RemoveOldBackups(b.dst.BackupsToKeep()); err != nil {
		return fmt.Errorf("can't remove old backups: %v", err)
	}
	if err := RemoveOldBackupsLocal(b.cfg, false); err != nil {
		return fmt.Errorf("can't remove old local backups: %v", err)
	}
	log.Infof("done")
	return nil
}

func (b *Backuper) uploadTableData(backupName string, table metadata.TableMetadata) (map[string][]string, error) {
	uuid := path.Join(clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table))
	metdataFiles := map[string][]string{}
	for disk := range table.Parts {
		backupPath := path.Join(b.DiskMap[disk], "backup", backupName, "shadow", uuid, disk)
		parts, err := separateParts(backupPath, table.Parts[disk], b.cfg.General.MaxFileSize)
		if err != nil {
			return nil, err
		}
		for i, p := range parts {
			remoteDataPath := path.Join(backupName, "shadow", clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table))
			// Disabled temporary
			// if b.cfg.GetCompressionFormat() == "none" {
			// 	err = b.dst.UploadPath(0, backupPath, p, path.Join(remoteDataPath, disk))
			// } else {
			fileName := fmt.Sprintf("%s_%d.%s", disk, i+1, b.cfg.GetArchiveExtension())
			metdataFiles[disk] = append(metdataFiles[disk], fileName)
			remoteDataFile := path.Join(remoteDataPath, fileName)
			if err := b.dst.CompressedStreamUpload(backupPath, p, remoteDataFile); err != nil {
				return nil, fmt.Errorf("can't upload: %v", err)
			}
		}
	}
	return metdataFiles, nil
}

func (b *Backuper) uploadTableMetadata(backupName string, table metadata.TableMetadata) (int64, error) {
	// заливаем метадату для таблицы
	tableMetafile := table
	content, err := json.MarshalIndent(&tableMetafile, "", "\t")
	if err != nil {
		return 0, fmt.Errorf("can't marshal json: %v", err)
	}
	remoteTableMetaFile := path.Join(backupName, "metadata", clickhouse.TablePathEncode(table.Database), fmt.Sprintf("%s.%s", clickhouse.TablePathEncode(table.Table), "json"))
	if err := b.dst.PutFile(remoteTableMetaFile,
		ioutil.NopCloser(bytes.NewReader(content))); err != nil {
		return 0, fmt.Errorf("can't upload: %v", err)
	}
	return int64(len(content)), nil
}

func markDuplicatedDiskParts(partsFromExistsBackup, partsFromNewBackup map[string][]metadata.Part) {
	for disk := range partsFromNewBackup {
		if _, ok := partsFromExistsBackup[disk]; ok {
			markDuplicatedParts(partsFromExistsBackup[disk], partsFromNewBackup[disk])
		}
	}
}

func markDuplicatedParts(old, new []metadata.Part) {
	if len(old) == 0 {
		return
	}
	oldMap := map[string]struct{}{}
	for i := range old {
		oldMap[old[i].Name] = struct{}{}
	}
	for i := range new {
		_, ok := oldMap[new[i].Name]
		new[i].Required = ok
	}
}

func (b *Backuper) ReadBackupMetadata(backupName string) (*metadata.BackupMetadata, error) {
	backupMetadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
	backupMetadataBody, err := ioutil.ReadFile(backupMetadataPath)
	if err != nil {
		return nil, err
	}
	backupMetadata := metadata.BackupMetadata{}
	if err := json.Unmarshal(backupMetadataBody, &backupMetadata); err != nil {
		return nil, err
	}
	if len(backupMetadata.Tables) == 0 && !b.cfg.General.AllowEmptyBackups {
		return nil, fmt.Errorf("'%s' is empty backup", backupName)
	}
	return &backupMetadata, nil
}

func separateParts(basePath string, parts []metadata.Part, maxSize int64) ([][]string, error) {
	var size int64
	files := []string{}
	result := [][]string{}
	for i := range parts {
		if parts[i].Required {
			continue
		}
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
