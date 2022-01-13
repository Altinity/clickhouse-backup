package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/utils"
	apexLog "github.com/apex/log"
	"github.com/yargevad/filepathx"
)

func (b *Backuper) Upload(backupName, tablePattern, diffFrom string, schemaOnly bool) error {
	if b.cfg.General.RemoteStorage == "none" {
		fmt.Println("Upload aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		_ = PrintLocalBackups(b.cfg, "all")
		return fmt.Errorf("select backup for upload")
	}
	if backupName == diffFrom {
		return fmt.Errorf("you cannot upload diff from the same backup")
	}
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "upload",
	})
	startUpload := time.Now()
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
	remoteBackups, err := b.dst.BackupList(false, "")
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
	var tablesForUpload ListOfTables
	if len(backupMetadata.Tables) != 0 {
		metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
		tablesForUpload, err = parseSchemaPattern(metadataPath, tablePattern, false, false)
		if err != nil {
			return err
		}
	}
	var diffFromBackup *metadata.BackupMetadata
	tablesForUploadFromDiff := map[metadata.TableTitle]metadata.TableMetadata{}
	if diffFrom != "" {
		diffFromBackup, err = b.ReadBackupMetadata(diffFrom)
		if err != nil {
			return err
		}
		if len(diffFromBackup.Tables) != 0 {
			backupMetadata.RequiredBackup = diffFrom
			metadataPath := path.Join(b.DefaultDataPath, "backup", diffFrom, "metadata")
			diffTablesList, err := parseSchemaPattern(metadataPath, tablePattern, false, false)
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
	compressedDataSize := int64(0)
	metadataSize := int64(0)
	log.Debugf("prepare table concurrent semaphore with concurrency=%d len(tablesForUpload)=%d", b.cfg.General.UploadConcurrency, len(tablesForUpload))
	s := semaphore.NewWeighted(int64(b.cfg.General.UploadConcurrency))
	g, ctx := errgroup.WithContext(context.Background())

	for i, table := range tablesForUpload {
		if err := s.Acquire(ctx, 1); err != nil {
			log.Errorf("can't acquire semaphore during Upload: %v", err)
			break
		}
		start := time.Now()
		if !schemaOnly {
			if diffTable, ok := tablesForUploadFromDiff[metadata.TableTitle{
				Database: table.Database,
				Table:    table.Table,
			}]; ok {
				b.markDuplicatedParts(backupMetadata, &diffTable, &table)
			}
		}
		idx := i
		g.Go(func() error {
			defer s.Release(1)
			var uploadedBytes int64
			if !schemaOnly {
				var files map[string][]string
				var err error
				files, uploadedBytes, err = b.uploadTableData(backupName, tablesForUpload[idx])
				if err != nil {
					return err
				}
				atomic.AddInt64(&compressedDataSize, uploadedBytes)
				tablesForUpload[idx].Files = files
			}
			tableMetadataSize, err := b.uploadTableMetadata(backupName, tablesForUpload[idx])
			if err != nil {
				return err
			}
			atomic.AddInt64(&metadataSize, tableMetadataSize)
			log.
				WithField("table", fmt.Sprintf("%s.%s", tablesForUpload[idx].Database, tablesForUpload[idx].Table)).
				WithField("duration", utils.HumanizeDuration(time.Since(start))).
				WithField("size", utils.FormatBytes(uint64(uploadedBytes+tableMetadataSize))).
				Info("done")
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("one of upload go-routine return error: %v", err)
	}

	// upload rbac for backup
	if backupMetadata.RBACSize, err = b.uploadRBACData(backupName); err != nil {
		return err
	}

	// upload configs for backup
	if backupMetadata.ConfigSize, err = b.uploadConfigData(backupName); err != nil {
		return err
	}

	// upload metadata for backup
	backupMetadata.CompressedSize = uint64(compressedDataSize)
	backupMetadata.MetadataSize = uint64(metadataSize)
	tt := make([]metadata.TableTitle, len(tablesForUpload))
	for i := range tablesForUpload {
		tt[i] = metadata.TableTitle{
			Database: tablesForUpload[i].Database,
			Table:    tablesForUpload[i].Table,
		}
	}
	backupMetadata.Tables = tt
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
	log.
		WithField("duration", utils.HumanizeDuration(time.Since(startUpload))).
		WithField("size", utils.FormatBytes(uint64(compressedDataSize)+uint64(metadataSize)+uint64(len(newBackupMetadataBody))+backupMetadata.RBACSize+backupMetadata.ConfigSize)).
		Info("done")

	// Clean
	if err := b.dst.RemoveOldBackups(b.cfg.General.BackupsToKeepRemote); err != nil {
		return fmt.Errorf("can't remove old backups on remote storage: %v", err)
	}
	return nil
}

func (b *Backuper) uploadConfigData(backupName string) (uint64, error) {
	configBackupPath := path.Join(b.DefaultDataPath, "backup", backupName, "configs")
	configFilesGlobPattern := path.Join(configBackupPath, "**/*.*")
	remoteConfigsArchive := path.Join(backupName, fmt.Sprintf("configs.%s", b.cfg.GetArchiveExtension()))
	return b.uploadAndArchiveBackupRelatedDir(configBackupPath, configFilesGlobPattern, remoteConfigsArchive)

}

func (b *Backuper) uploadRBACData(backupName string) (uint64, error) {
	rbacBackupPath := path.Join(b.DefaultDataPath, "backup", backupName, "access")
	accessFilesGlobPattern := path.Join(rbacBackupPath, "*.*")
	remoteRBACArchive := path.Join(backupName, fmt.Sprintf("access.%s", b.cfg.GetArchiveExtension()))
	return b.uploadAndArchiveBackupRelatedDir(rbacBackupPath, accessFilesGlobPattern, remoteRBACArchive)
}

func (b *Backuper) uploadAndArchiveBackupRelatedDir(localBackupRelatedDir, localFilesGlobPattern, remoteFile string) (uint64, error) {
	if _, err := os.Stat(localBackupRelatedDir); os.IsNotExist(err) {
		return 0, nil
	}
	var localFiles []string
	var err error
	if localFiles, err = filepathx.Glob(localFilesGlobPattern); err != nil || localFiles == nil || len(localFiles) == 0 {
		return 0, fmt.Errorf("list %s return list=%v with err=%v", localFilesGlobPattern, localFiles, err)
	}
	for i := range localFiles {
		localFiles[i] = strings.Replace(localFiles[i], localBackupRelatedDir, "", 1)
	}

	if err := b.dst.CompressedStreamUpload(localBackupRelatedDir, localFiles, remoteFile); err != nil {
		return 0, fmt.Errorf("can't RBAC upload: %v", err)
	}
	remoteUploaded, err := b.dst.StatFile(remoteFile)
	if err != nil {
		return 0, fmt.Errorf("can't check uploaded %s file: %v", remoteFile, err)
	}
	return uint64(remoteUploaded.Size()), nil
}

func (b *Backuper) uploadTableData(backupName string, table metadata.TableMetadata) (map[string][]string, int64, error) {
	uuid := path.Join(clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table))
	metdataFiles := map[string][]string{}
	capacity := 0
	for disk := range table.Parts {
		capacity += len(table.Parts[disk])
	}
	apexLog.Debugf("start uploadTableData %s.%s with concurrency=%d len(table.Parts[...])=%d", table.Database, table.Table, b.cfg.General.UploadConcurrency, capacity)
	s := semaphore.NewWeighted(int64(b.cfg.General.UploadConcurrency))
	g, ctx := errgroup.WithContext(context.Background())
	var uploadedBytes int64
	for disk := range table.Parts {
		backupPath := path.Join(b.DiskMap[disk], "backup", backupName, "shadow", uuid, disk)
		parts, err := separateParts(backupPath, table.Parts[disk], b.cfg.General.MaxFileSize)
		if err != nil {
			return nil, 0, err
		}
		for i, p := range parts {
			if err := s.Acquire(ctx, 1); err != nil {
				apexLog.Errorf("can't acquire semaphore during Upload: %v", err)
				break
			}
			remoteDataPath := path.Join(backupName, "shadow", clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table))
			// Disabled temporary
			// if b.cfg.GetCompressionFormat() == "none" {
			// 	err = b.dst.UploadPath(0, backupPath, p, path.Join(remoteDataPath, disk))
			// } else {
			fileName := fmt.Sprintf("%s_%d.%s", disk, i+1, b.cfg.GetArchiveExtension())
			metdataFiles[disk] = append(metdataFiles[disk], fileName)
			remoteDataFile := path.Join(remoteDataPath, fileName)
			localFiles := p
			g.Go(func() error {
				apexLog.Debugf("start upload %d files to %s", len(localFiles), remoteDataFile)
				defer s.Release(1)
				if err := b.dst.CompressedStreamUpload(backupPath, localFiles, remoteDataFile); err != nil {
					apexLog.Errorf("CompressedStreamUpload return error: %v", err)
					return fmt.Errorf("can't upload: %v", err)
				}
				remoteFile, err := b.dst.StatFile(remoteDataFile)
				if err != nil {
					return fmt.Errorf("can't check uploaded file: %v", err)
				}
				atomic.AddInt64(&uploadedBytes, remoteFile.Size())
				apexLog.Debugf("finish upload to %s", remoteDataFile)
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return nil, 0, fmt.Errorf("one of uploadTableData go-routine return error: %v", err)
	}
	apexLog.Debugf("finish uploadTableData %s.%s with concurrency=%d len(table.Parts[...])=%d metadataFiles=%v, uploadedBytes=%v", table.Database, table.Table, b.cfg.General.UploadConcurrency, capacity, metdataFiles, uploadedBytes)
	return metdataFiles, uploadedBytes, nil
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

func (b *Backuper) markDuplicatedParts(backup *metadata.BackupMetadata, existsTable *metadata.TableMetadata, newTable *metadata.TableMetadata) {
	for disk, newParts := range newTable.Parts {
		if _, ok := existsTable.Parts[disk]; ok {
			if len(existsTable.Parts[disk]) == 0 {
				continue
			}
			existsPartsMap := map[string]struct{}{}
			existsPartitionMap := map[string]struct{}{}
			for _, p := range existsTable.Parts[disk] {
				existsPartsMap[p.Name] = struct{}{}
				//
				// ClickHouse part name: partition_start_end_level, the first part is partition name.
				//
				//
				existsPartitionMap[strings.Split(p.Name, "_")[0]] = struct{}{}
			}
			// TODO If more codes here, split these codes into two functions. It will be neat and clean.
			if b.cfg.General.BackUpPartsBasedIncremental {
				for i := range newParts {
					if _, ok := existsPartsMap[newParts[i].Name]; !ok {
						continue
					}
					uuid := path.Join(clickhouse.TablePathEncode(existsTable.Database), clickhouse.TablePathEncode(existsTable.Table))
					existsPath := path.Join(b.DiskMap[disk], "backup", backup.RequiredBackup, "shadow", uuid, disk, newParts[i].Name)
					newPath := path.Join(b.DiskMap[disk], "backup", backup.BackupName, "shadow", uuid, disk, newParts[i].Name)

					if err := isDuplicatedParts(existsPath, newPath); err != nil {
						apexLog.Debugf("part '%s' and '%s' must be the same: %v", existsPath, newPath, err)
						continue
					}
					newParts[i].Required = true
				}
			} else {
				for i := 0; i < len(newParts); {
					// Mark to remove duplicated parts under this policy.
					// newParts[i].RemovePartsBasedOnPartitionIncrementalPolicy = true
					if _, ok := existsPartitionMap[strings.Split(newParts[i].Name, "_")[0]]; ok {
						// Required = Not upload.
						newParts = append(newParts[:i], newParts[i+1:]...)
					} else {
						i++
						// We should remove this part.
					}
				}
				newTable.Parts[disk] = newParts
			}
		}
	}
}

func isDuplicatedParts(part1, part2 string) error {
	p1, err := os.Open(part1)
	if err != nil {
		return err
	}
	defer func() {
		if err = p1.Close(); err != nil {
			apexLog.Warnf("Can't close %s", part1)
		}
	}()
	p2, err := os.Open(part2)
	if err != nil {
		return err
	}
	defer func() {
		if err = p2.Close(); err != nil {
			apexLog.Warnf("Can't close %s", part2)
		}
	}()
	pf1, err := p1.Readdirnames(-1)
	if err != nil {
		return err
	}
	pf2, err := p2.Readdirnames(-1)
	if err != nil {
		return err
	}
	if len(pf1) != len(pf2) {
		return fmt.Errorf("files count in parts is different")
	}
	for _, f := range pf1 {
		part1File, err := os.Stat(path.Join(part1, f))
		if err != nil {
			return err
		}
		part2File, err := os.Stat(path.Join(part2, f))
		if err != nil {
			return err
		}
		if !os.SameFile(part1File, part2File) {
			return fmt.Errorf("file '%s' is different", f)
		}
	}
	return nil
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
	// if len(backupMetadata.Tables) == 0 && !b.cfg.General.AllowEmptyBackups {
	// 	return nil, fmt.Errorf("'%s' is empty backup", backupName)
	// }
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
		err := filepath.Walk(partPath, func(filePath string, info os.FileInfo, err error) error {
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
		if err != nil {
			apexLog.Warnf("filepath.Walk return error: %v", err)
		}
	}
	if len(files) > 0 {
		result = append(result, files)
	}
	return result, nil
}
