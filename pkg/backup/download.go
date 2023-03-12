package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/custom"
	"github.com/AlexAkulov/clickhouse-backup/pkg/filesystemhelper"
	"github.com/AlexAkulov/clickhouse-backup/pkg/resumable"
	"github.com/AlexAkulov/clickhouse-backup/pkg/status"
	"github.com/eapache/go-resiliency/retrier"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/AlexAkulov/clickhouse-backup/pkg/common"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/pkg/storage"
	"github.com/AlexAkulov/clickhouse-backup/pkg/utils"

	apexLog "github.com/apex/log"
)

var (
	ErrBackupIsAlreadyExists = errors.New("backup is already exists")
)

func (b *Backuper) legacyDownload(ctx context.Context, backupName string) error {
	log := b.log.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "download_legacy",
	})
	bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, true, "")
	if err != nil {
		return err
	}
	if err := bd.Connect(ctx); err != nil {
		return err
	}
	defer func() {
		if err := bd.Close(ctx); err != nil {
			b.log.Warnf("can't close BackupDestination error: %v", err)
		}
	}()
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return bd.DownloadCompressedStream(ctx, backupName, path.Join(b.DefaultDataPath, "backup", backupName))
	})
	if err != nil {
		return err
	}
	log.Info("done")
	return nil
}

func (b *Backuper) Download(backupName string, tablePattern string, partitions []string, schemaOnly, resume bool, commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	log := b.log.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "download",
	})
	if b.cfg.General.RemoteStorage == "none" {
		return fmt.Errorf("general->remote_storage shall not be \"none\" for download, change you config or use REMOTE_STORAGE environment variable")
	}
	if !resume && b.cfg.General.UseResumableState {
		resume = true
	}
	b.resume = resume
	if backupName == "" {
		_ = b.PrintRemoteBackups(ctx, "all")
		return fmt.Errorf("select backup for download")
	}
	localBackups, disks, err := b.GetLocalBackups(ctx, nil)
	if err != nil {
		return err
	}
	for i := range localBackups {
		if backupName == localBackups[i].BackupName {
			if !b.resume {
				return ErrBackupIsAlreadyExists
			} else {
				if strings.Contains(localBackups[i].Tags, "embedded") || b.cfg.General.RemoteStorage == "custom" {
					return ErrBackupIsAlreadyExists
				}
				log.Warnf("%s already exists will try to resume download", backupName)
			}
		}
	}
	startDownload := time.Now()
	if b.cfg.General.RemoteStorage == "custom" {
		return custom.Download(ctx, b.cfg, backupName, tablePattern, partitions, schemaOnly)
	}
	if err := b.init(ctx, disks, ""); err != nil {
		return err
	}
	defer func() {
		if err := b.dst.Close(ctx); err != nil {
			b.log.Warnf("can't close BackupDestination error: %v", err)
		}
	}()

	remoteBackups, err := b.dst.BackupList(ctx, true, backupName)
	if err != nil {
		return err
	}
	found := false
	var remoteBackup storage.Backup
	for _, r := range remoteBackups {
		if backupName == r.BackupName {
			remoteBackup = r
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("'%s' is not found on remote storage", backupName)
	}
	//look https://github.com/AlexAkulov/clickhouse-backup/discussions/266 need download legacy before check for empty backup
	if remoteBackup.Legacy {
		if tablePattern != "" {
			return fmt.Errorf("'%s' is old format backup and doesn't supports download of specific tables", backupName)
		}
		if schemaOnly {
			return fmt.Errorf("'%s' is old format backup and doesn't supports download of schema only", backupName)
		}
		log.Warnf("'%s' is old-format backup", backupName)
		return b.legacyDownload(ctx, backupName)
	}
	if len(remoteBackup.Tables) == 0 && !b.cfg.General.AllowEmptyBackups {
		return fmt.Errorf("'%s' is empty backup", backupName)
	}
	tablesForDownload := parseTablePatternForDownload(remoteBackup.Tables, tablePattern)
	tableMetadataAfterDownload := make([]metadata.TableMetadata, len(tablesForDownload))

	if !schemaOnly && !b.cfg.General.DownloadByPart && remoteBackup.RequiredBackup != "" {
		err := b.Download(remoteBackup.RequiredBackup, tablePattern, partitions, schemaOnly, b.resume, commandId)
		if err != nil && err != ErrBackupIsAlreadyExists {
			return err
		}
	}

	dataSize := uint64(0)
	metadataSize := uint64(0)
	b.isEmbedded = strings.Contains(remoteBackup.Tags, "embedded")
	localBackupDir := path.Join(b.DefaultDataPath, "backup", backupName)
	if b.isEmbedded {
		localBackupDir = path.Join(b.EmbeddedBackupDataPath, backupName)
	}
	err = os.MkdirAll(localBackupDir, 0750)
	if err != nil && !resume {
		return err
	}
	if b.resume {
		b.resumableState = resumable.NewState(b.DefaultDataPath, backupName, "download")
	}

	log.Debugf("prepare table METADATA concurrent semaphore with concurrency=%d len(tablesForDownload)=%d", b.cfg.General.DownloadConcurrency, len(tablesForDownload))
	downloadSemaphore := semaphore.NewWeighted(int64(b.cfg.General.DownloadConcurrency))
	metadataGroup, metadataCtx := errgroup.WithContext(ctx)
	for i, t := range tablesForDownload {
		if err := downloadSemaphore.Acquire(metadataCtx, 1); err != nil {
			log.Errorf("can't acquire semaphore during Download metadata: %v", err)
			break
		}
		log := log.WithField("table_metadata", fmt.Sprintf("%s.%s", t.Database, t.Table))
		idx := i
		tableTitle := t
		metadataGroup.Go(func() error {
			defer downloadSemaphore.Release(1)
			downloadedMetadata, size, err := b.downloadTableMetadata(metadataCtx, backupName, disks, log, tableTitle, schemaOnly, partitions)
			if err != nil {
				return err
			}
			tableMetadataAfterDownload[idx] = *downloadedMetadata
			atomic.AddUint64(&metadataSize, size)
			return nil
		})
	}
	if err := metadataGroup.Wait(); err != nil {
		return fmt.Errorf("one of Download Metadata go-routine return error: %v", err)
	}
	if !schemaOnly {
		for _, t := range tableMetadataAfterDownload {
			for disk := range t.Parts {
				if _, diskExists := b.DiskToPathMap[disk]; !diskExists && disk != b.cfg.ClickHouse.EmbeddedBackupDisk {
					b.DiskToPathMap[disk] = b.DiskToPathMap["default"]
					log.Warnf("table '%s.%s' require disk '%s' that not found in clickhouse table system.disks, you can add nonexistent disks to `disk_mapping` in  `clickhouse` config section, data will download to %s", t.Database, t.Table, disk, b.DiskToPathMap["default"])
				}
			}
		}
		log.Debugf("prepare table SHADOW concurrent semaphore with concurrency=%d len(tableMetadataAfterDownload)=%d", b.cfg.General.DownloadConcurrency, len(tableMetadataAfterDownload))
		dataGroup, dataCtx := errgroup.WithContext(ctx)

		for i, tableMetadata := range tableMetadataAfterDownload {
			if tableMetadata.MetadataOnly {
				continue
			}
			if err := downloadSemaphore.Acquire(dataCtx, 1); err != nil {
				log.Errorf("can't acquire semaphore during Download table data: %v", err)
				break
			}
			dataSize += tableMetadata.TotalBytes
			idx := i
			dataGroup.Go(func() error {
				defer downloadSemaphore.Release(1)
				start := time.Now()
				if err := b.downloadTableData(dataCtx, remoteBackup.BackupMetadata, tableMetadataAfterDownload[idx]); err != nil {
					return err
				}
				log.
					WithField("operation", "download_data").
					WithField("table", fmt.Sprintf("%s.%s", tableMetadataAfterDownload[idx].Database, tableMetadataAfterDownload[idx].Table)).
					WithField("duration", utils.HumanizeDuration(time.Since(start))).
					WithField("size", utils.FormatBytes(tableMetadataAfterDownload[idx].TotalBytes)).
					Info("done")
				return nil
			})
		}
		if err := dataGroup.Wait(); err != nil {
			return fmt.Errorf("one of Download go-routine return error: %v", err)
		}
	}
	var rbacSize, configSize uint64
	if !b.isEmbedded {
		rbacSize, err = b.downloadRBACData(ctx, remoteBackup)
		if err != nil {
			return fmt.Errorf("download RBAC error: %v", err)
		}

		configSize, err = b.downloadConfigData(ctx, remoteBackup)
		if err != nil {
			return fmt.Errorf("download CONFIGS error: %v", err)
		}
	}

	backupMetadata := remoteBackup.BackupMetadata
	backupMetadata.Tables = tablesForDownload
	backupMetadata.DataSize = dataSize
	backupMetadata.MetadataSize = metadataSize

	if b.isEmbedded {
		localClickHouseBackupFile := path.Join(b.EmbeddedBackupDataPath, backupName, ".backup")
		remoteClickHouseBackupFile := path.Join(backupName, ".backup")
		if err = b.downloadSingleBackupFile(ctx, remoteClickHouseBackupFile, localClickHouseBackupFile, disks); err != nil {
			return err
		}
	}

	backupMetadata.CompressedSize = 0
	backupMetadata.DataFormat = ""
	backupMetadata.RequiredBackup = ""
	backupMetadata.ConfigSize = configSize
	backupMetadata.RBACSize = rbacSize

	backupMetafileLocalPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
	if b.isEmbedded {
		backupMetafileLocalPath = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata.json")
	}
	if err := backupMetadata.Save(backupMetafileLocalPath); err != nil {
		return err
	}
	for _, disk := range disks {
		if disk.IsBackup {
			if err = filesystemhelper.Chown(path.Join(disk.Path, backupName), b.ch, disks, true); err != nil {
				return err
			}
		}
	}

	if b.resume {
		b.resumableState.Close()
	}

	log.
		WithField("duration", utils.HumanizeDuration(time.Since(startDownload))).
		WithField("size", utils.FormatBytes(dataSize+metadataSize+rbacSize+configSize)).
		Info("done")
	return nil
}

func (b *Backuper) downloadTableMetadataIfNotExists(ctx context.Context, backupName string, log *apexLog.Entry, tableTitle metadata.TableTitle) (*metadata.TableMetadata, error) {
	metadataLocalFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	tm := &metadata.TableMetadata{}
	if _, err := tm.Load(metadataLocalFile); err == nil {
		return tm, nil
	}
	tm, _, err := b.downloadTableMetadata(ctx, backupName, nil, log.WithFields(apexLog.Fields{"operation": "downloadTableMetadataIfNotExists", "backupName": backupName, "table_metadata_diff": fmt.Sprintf("%s.%s", tableTitle.Database, tableTitle.Table)}), tableTitle, false, nil)
	return tm, err
}

func (b *Backuper) downloadTableMetadata(ctx context.Context, backupName string, disks []clickhouse.Disk, log *apexLog.Entry, tableTitle metadata.TableTitle, schemaOnly bool, partitions []string) (*metadata.TableMetadata, uint64, error) {
	start := time.Now()
	size := uint64(0)
	metadataFiles := map[string]string{}
	remoteMedataPrefix := path.Join(backupName, "metadata", common.TablePathEncode(tableTitle.Database), common.TablePathEncode(tableTitle.Table))
	metadataFiles[fmt.Sprintf("%s.json", remoteMedataPrefix)] = path.Join(b.DefaultDataPath, "backup", backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))

	if b.isEmbedded {
		metadataFiles[fmt.Sprintf("%s.sql", remoteMedataPrefix)] = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.sql", common.TablePathEncode(tableTitle.Table)))
		metadataFiles[fmt.Sprintf("%s.json", remoteMedataPrefix)] = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	}
	var tableMetadata metadata.TableMetadata
	for remoteMetadataFile, localMetadataFile := range metadataFiles {
		if b.resume {
			isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(localMetadataFile)
			if isProcessed && strings.HasSuffix(localMetadataFile, ".json") {
				tmBody, err := os.ReadFile(localMetadataFile)
				if err != nil {
					return nil, 0, err
				}
				if err = json.Unmarshal(tmBody, &tableMetadata); err != nil {
					return nil, 0, err
				}
				partitionsFilter, _ := filesystemhelper.CreatePartitionsToBackupMap(b.ch, nil, []metadata.TableMetadata{tableMetadata}, partitions)
				apexLog.Errorf("SUKA!!! partitionsFilter=%#v", partitionsFilter)
				filterPartsAndFilesByPartitionsFilter(tableMetadata, partitionsFilter)
			}
			if isProcessed {
				size += uint64(processedSize)
				continue
			}
		}
		var tmBody []byte
		retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
		err := retry.RunCtx(ctx, func(ctx context.Context) error {
			tmReader, err := b.dst.GetFileReader(ctx, remoteMetadataFile)
			if err != nil {
				return err
			}
			tmBody, err = io.ReadAll(tmReader)
			if err != nil {
				return err
			}
			err = tmReader.Close()
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return nil, 0, err
		}

		if err = os.MkdirAll(path.Dir(localMetadataFile), 0755); err != nil {
			return nil, 0, err
		}
		var written int64
		if strings.HasSuffix(localMetadataFile, ".sql") {
			if err = os.WriteFile(localMetadataFile, tmBody, 0640); err != nil {
				return nil, 0, err
			}
			if err = filesystemhelper.Chown(localMetadataFile, b.ch, disks, false); err != nil {
				return nil, 0, err
			}
			written = int64(len(tmBody))
			size += uint64(len(tmBody))
		} else {
			if err = json.Unmarshal(tmBody, &tableMetadata); err != nil {
				return nil, 0, err
			}
			partitionsFilter, _ := filesystemhelper.CreatePartitionsToBackupMap(b.ch, nil, []metadata.TableMetadata{tableMetadata}, partitions)
			apexLog.Errorf("SUKA2!!! partitions=%#v, partitionsFilter=%#v", partitions, partitionsFilter)
			filterPartsAndFilesByPartitionsFilter(tableMetadata, partitionsFilter)
			// save metadata
			jsonSize := uint64(0)
			jsonSize, err = tableMetadata.Save(localMetadataFile, schemaOnly)
			if err != nil {
				return nil, 0, err
			}
			written = int64(jsonSize)
			size += jsonSize
		}
		if b.resume {
			b.resumableState.AppendToState(localMetadataFile, written)
		}
	}
	log.
		WithField("duration", utils.HumanizeDuration(time.Since(start))).
		WithField("size", utils.FormatBytes(size)).
		Info("done")
	return &tableMetadata, size, nil
}

func (b *Backuper) downloadRBACData(ctx context.Context, remoteBackup storage.Backup) (uint64, error) {
	return b.downloadBackupRelatedDir(ctx, remoteBackup, "access")
}

func (b *Backuper) downloadConfigData(ctx context.Context, remoteBackup storage.Backup) (uint64, error) {
	return b.downloadBackupRelatedDir(ctx, remoteBackup, "configs")
}

func (b *Backuper) downloadBackupRelatedDir(ctx context.Context, remoteBackup storage.Backup, prefix string) (uint64, error) {
	log := b.log.WithField("logger", "downloadBackupRelatedDir")
	archiveFile := fmt.Sprintf("%s.%s", prefix, b.cfg.GetArchiveExtension())
	remoteFile := path.Join(remoteBackup.BackupName, archiveFile)
	if b.resume {
		if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(remoteFile); isProcessed {
			return uint64(processedSize), nil
		}
	}
	localDir := path.Join(b.DefaultDataPath, "backup", remoteBackup.BackupName, prefix)
	remoteFileInfo, err := b.dst.StatFile(ctx, remoteFile)
	if err != nil {
		log.Debugf("%s not exists on remote storage, skip download", remoteFile)
		return 0, nil
	}
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.DownloadCompressedStream(ctx, remoteFile, localDir)
	})
	if err != nil {
		return 0, err
	}
	if b.resume {
		b.resumableState.AppendToState(remoteFile, remoteFileInfo.Size())
	}
	return uint64(remoteFileInfo.Size()), nil
}

func (b *Backuper) downloadTableData(ctx context.Context, remoteBackup metadata.BackupMetadata, table metadata.TableMetadata) error {
	log := b.log.WithField("logger", "downloadTableData")
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))

	s := semaphore.NewWeighted(int64(b.cfg.General.DownloadConcurrency))
	g, dataCtx := errgroup.WithContext(ctx)

	if remoteBackup.DataFormat != "directory" {
		capacity := 0
		downloadOffset := make(map[string]int, 0)
		for disk := range table.Files {
			capacity += len(table.Files[disk])
			downloadOffset[disk] = 0
		}
		log.Debugf("start %s.%s with concurrency=%d len(table.Files[...])=%d", table.Database, table.Table, b.cfg.General.DownloadConcurrency, capacity)
	breakByErrorArchive:
		for common.SumMapValuesInt(downloadOffset) < capacity {
			for disk := range table.Files {
				if downloadOffset[disk] >= len(table.Files[disk]) {
					continue
				}
				archiveFile := table.Files[disk][downloadOffset[disk]]
				if err := s.Acquire(dataCtx, 1); err != nil {
					log.Errorf("can't acquire semaphore %s archive: %v", archiveFile, err)
					break breakByErrorArchive
				}
				tableLocalDir := b.getLocalBackupDataPathForTable(remoteBackup.BackupName, disk, dbAndTableDir)
				downloadOffset[disk] += 1
				tableRemoteFile := path.Join(remoteBackup.BackupName, "shadow", common.TablePathEncode(table.Database), common.TablePathEncode(table.Table), archiveFile)
				g.Go(func() error {
					defer s.Release(1)
					log.Debugf("start download %s", tableRemoteFile)
					if b.resume && b.resumableState.IsAlreadyProcessedBool(tableRemoteFile) {
						return nil
					}
					retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
					err := retry.RunCtx(dataCtx, func(dataCtx context.Context) error {
						return b.dst.DownloadCompressedStream(dataCtx, tableRemoteFile, tableLocalDir)
					})
					if err != nil {
						return err
					}
					if b.resume {
						b.resumableState.AppendToState(tableRemoteFile, 0)
					}
					log.Debugf("finish download %s", tableRemoteFile)
					return nil
				})
			}
		}
	} else {
		capacity := 0
		for disk := range table.Parts {
			capacity += len(table.Parts[disk])
		}
		log.Debugf("start %s.%s with concurrency=%d len(table.Parts[...])=%d", table.Database, table.Table, b.cfg.General.DownloadConcurrency, capacity)

	breakByErrorDirectory:
		for disk, parts := range table.Parts {
			tableRemotePath := path.Join(remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
			diskPath := b.DiskToPathMap[disk]
			tableLocalPath := path.Join(diskPath, "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
			if b.isEmbedded {
				tableLocalPath = path.Join(diskPath, remoteBackup.BackupName, "data", dbAndTableDir)
			}
			for _, part := range parts {
				if part.Required {
					continue
				}
				partRemotePath := path.Join(tableRemotePath, part.Name)
				if err := s.Acquire(dataCtx, 1); err != nil {
					log.Errorf("can't acquire semaphore %s directory: %v", partRemotePath, err)
					break breakByErrorDirectory
				}
				partLocalPath := path.Join(tableLocalPath, part.Name)
				g.Go(func() error {
					defer s.Release(1)
					log.Debugf("start %s -> %s", partRemotePath, partLocalPath)
					if b.resume && b.resumableState.IsAlreadyProcessedBool(partRemotePath) {
						return nil
					}
					if err := b.dst.DownloadPath(dataCtx, 0, partRemotePath, partLocalPath, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration); err != nil {
						return err
					}
					if b.resume {
						b.resumableState.AppendToState(partRemotePath, 0)
					}
					log.Debugf("finish %s -> %s", partRemotePath, partLocalPath)
					return nil
				})
			}
		}
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("one of downloadTableData go-routine return error: %v", err)
	}

	if !b.isEmbedded {
		err := b.downloadDiffParts(ctx, remoteBackup, table, dbAndTableDir)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Backuper) downloadDiffParts(ctx context.Context, remoteBackup metadata.BackupMetadata, table metadata.TableMetadata, dbAndTableDir string) error {
	log := b.log.WithField("operation", "downloadDiffParts")
	log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).Debug("start")
	start := time.Now()
	downloadedDiffParts := uint32(0)
	s := semaphore.NewWeighted(int64(b.cfg.General.DownloadConcurrency))
	downloadDiffGroup, downloadDiffCtx := errgroup.WithContext(ctx)

	diffRemoteFilesCache := map[string]*sync.Mutex{}
	diffRemoteFilesLock := &sync.Mutex{}

breakByError:
	for disk, parts := range table.Parts {
		for _, part := range parts {
			newPath := path.Join(b.DiskToPathMap[disk], "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk, part.Name)
			if err := b.checkNewPath(newPath, part); err != nil {
				return err
			}
			if !part.Required {
				continue
			}
			existsPath := path.Join(b.DiskToPathMap[disk], "backup", remoteBackup.RequiredBackup, "shadow", dbAndTableDir, disk, part.Name)
			_, err := os.Stat(existsPath)
			if err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("%s stat return error: %v", existsPath, err)
			}
			if err != nil && os.IsNotExist(err) {
				if err := s.Acquire(downloadDiffCtx, 1); err != nil {
					log.Errorf("can't acquire semaphore during downloadDiffParts: %v", err)
					break breakByError
				}
				partForDownload := part
				diskForDownload := disk
				downloadDiffGroup.Go(func() error {
					defer s.Release(1)
					tableRemoteFiles, err := b.findDiffBackupFilesRemote(downloadDiffCtx, remoteBackup, table, diskForDownload, partForDownload, log)
					if err != nil {
						return err
					}

					for tableRemoteFile, tableLocalDir := range tableRemoteFiles {
						err = b.downloadDiffRemoteFile(downloadDiffCtx, diffRemoteFilesLock, diffRemoteFilesCache, tableRemoteFile, tableLocalDir)
						if err != nil {
							return err
						}
						downloadedPartPath := path.Join(tableLocalDir, partForDownload.Name)
						if downloadedPartPath != existsPath {
							info, err := os.Stat(downloadedPartPath)
							if err == nil {
								if !info.IsDir() {
									return fmt.Errorf("after downloadDiffRemoteFile %s exists but is not directory", downloadedPartPath)
								}
								if err = b.makePartHardlinks(downloadedPartPath, existsPath); err != nil {
									return fmt.Errorf("can't to add link to exists part %s -> %s error: %v", newPath, existsPath, err)
								}
							}
							if err != nil && !os.IsNotExist(err) {
								return fmt.Errorf("after downloadDiffRemoteFile %s stat return error: %v", downloadedPartPath, err)
							}
						}
						atomic.AddUint32(&downloadedDiffParts, 1)
					}
					if err = b.makePartHardlinks(existsPath, newPath); err != nil {
						return fmt.Errorf("can't to add link to exists part %s -> %s error: %v", newPath, existsPath, err)
					}
					if b.resume {
						b.resumableState.AppendToState(existsPath, 0)
					}
					return nil
				})
			} else {
				if !b.resume || (b.resume && !b.resumableState.IsAlreadyProcessedBool(existsPath)) {
					if err = b.makePartHardlinks(existsPath, newPath); err != nil {
						return fmt.Errorf("can't to add exists part: %v", err)
					}
				}
			}
		}
	}
	if err := downloadDiffGroup.Wait(); err != nil {
		return fmt.Errorf("one of downloadDiffParts go-routine return error: %v", err)
	}
	log.WithField("duration", utils.HumanizeDuration(time.Since(start))).WithField("diff_parts", strconv.Itoa(int(downloadedDiffParts))).Info("done")
	return nil
}

func (b *Backuper) downloadDiffRemoteFile(ctx context.Context, diffRemoteFilesLock *sync.Mutex, diffRemoteFilesCache map[string]*sync.Mutex, tableRemoteFile string, tableLocalDir string) error {
	log := b.log.WithField("logger", "downloadDiffRemoteFile")
	diffRemoteFilesLock.Lock()
	namedLock, isCached := diffRemoteFilesCache[tableRemoteFile]
	if isCached {
		log.Debugf("wait download begin %s", tableRemoteFile)
		namedLock.Lock()
		diffRemoteFilesLock.Unlock()
		namedLock.Unlock()
		log.Debugf("wait download end %s", tableRemoteFile)
	} else {
		log.Debugf("start download from %s", tableRemoteFile)
		namedLock = &sync.Mutex{}
		diffRemoteFilesCache[tableRemoteFile] = namedLock
		namedLock.Lock()
		diffRemoteFilesLock.Unlock()
		if path.Ext(tableRemoteFile) != "" {
			retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
			err := retry.RunCtx(ctx, func(ctx context.Context) error {
				return b.dst.DownloadCompressedStream(ctx, tableRemoteFile, tableLocalDir)
			})
			if err != nil {
				log.Warnf("DownloadCompressedStream %s -> %s return error: %v", tableRemoteFile, tableLocalDir, err)
				return err
			}
		} else {
			// remoteFile could be a directory
			if err := b.dst.DownloadPath(ctx, 0, tableRemoteFile, tableLocalDir, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration); err != nil {
				log.Warnf("DownloadPath %s -> %s return error: %v", tableRemoteFile, tableLocalDir, err)
				return err
			}
		}
		namedLock.Unlock()
		log.Debugf("finish download from %s", tableRemoteFile)
	}
	return nil
}

func (b *Backuper) checkNewPath(newPath string, part metadata.Part) error {
	info, err := os.Stat(newPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("%s stat return error: %v", newPath, err)
	}
	if os.IsNotExist(err) && !part.Required {
		return fmt.Errorf("%s not found after download backup", newPath)
	}
	if err == nil && !info.IsDir() {
		return fmt.Errorf("%s is not directory after download backup", newPath)
	}
	return nil
}

func (b *Backuper) findDiffBackupFilesRemote(ctx context.Context, backup metadata.BackupMetadata, table metadata.TableMetadata, disk string, part metadata.Part, log *apexLog.Entry) (map[string]string, error) {
	var requiredTable *metadata.TableMetadata
	log.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffBackupFilesRemote"}).Debugf("start")
	requiredBackup, err := b.ReadBackupMetadataRemote(ctx, backup.RequiredBackup)
	if err != nil {
		return nil, err
	}
	requiredTable, err = b.downloadTableMetadataIfNotExists(ctx, requiredBackup.BackupName, log, metadata.TableTitle{Database: table.Database, Table: table.Table})
	if err != nil {
		log.Warnf("downloadTableMetadataIfNotExists %s / %s.%s return error", requiredBackup.BackupName, table.Database, table.Table)
		return nil, err
	}

	// recursive find if part in RequiredBackup also Required
	tableRemoteFiles, found, err := b.findDiffRecursive(ctx, requiredBackup, log, table, requiredTable, part, disk)
	if found {
		return tableRemoteFiles, nil
	}

	found = false
	// try to find part on the same disk
	tableRemoteFiles, err, found = b.findDiffOnePart(ctx, requiredBackup, table, disk, disk, part)
	if found {
		return tableRemoteFiles, nil
	}

	// try to find part on other disks
	for requiredDisk := range requiredBackup.Disks {
		if requiredDisk != disk {
			tableRemoteFiles, err, found = b.findDiffOnePart(ctx, requiredBackup, table, disk, requiredDisk, part)
			if found {
				return tableRemoteFiles, nil
			}
		}
	}
	// find one or multiple big files, disk_X.tar files by part.Name
	tableRemoteFiles = make(map[string]string)
	for requiredDisk, requiredParts := range requiredTable.Parts {
		for _, requiredPart := range requiredParts {
			if part.Name == requiredPart.Name {
				localTableDir := path.Join(b.DiskToPathMap[disk], "backup", requiredBackup.BackupName, "shadow", common.TablePathEncode(table.Database), common.TablePathEncode(table.Table), disk)
				for _, remoteFile := range requiredTable.Files[requiredDisk] {
					remoteFile = path.Join(requiredBackup.BackupName, "shadow", common.TablePathEncode(table.Database), common.TablePathEncode(table.Table), remoteFile)
					tableRemoteFiles[remoteFile] = localTableDir
				}
			}
		}
	}
	if len(tableRemoteFiles) > 0 {
		return tableRemoteFiles, nil
	}
	// not found
	return nil, fmt.Errorf("%s.%s %s not found on %s and all required backups sequence", table.Database, table.Table, part.Name, requiredBackup.BackupName)
}

func (b *Backuper) findDiffRecursive(ctx context.Context, requiredBackup *metadata.BackupMetadata, log *apexLog.Entry, table metadata.TableMetadata, requiredTable *metadata.TableMetadata, part metadata.Part, disk string) (map[string]string, bool, error) {
	log.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffRecursive"}).Debugf("start")
	found := false
	for _, requiredParts := range requiredTable.Parts {
		for _, requiredPart := range requiredParts {
			if requiredPart.Name == part.Name {
				found = true
				if requiredPart.Required {
					tableRemoteFiles, err := b.findDiffBackupFilesRemote(ctx, *requiredBackup, table, disk, part, log)
					if err != nil {
						found = false
						log.Warnf("try find %s.%s %s recursive return err: %v", table.Database, table.Table, part.Name, err)
					}
					return tableRemoteFiles, found, err
				}
				break
			}
		}
		if found {
			break
		}
	}
	return nil, false, nil
}

func (b *Backuper) findDiffOnePart(ctx context.Context, requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, localDisk, remoteDisk string, part metadata.Part) (map[string]string, error, bool) {
	log := apexLog.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffOnePart"})
	log.Debugf("start")
	tableRemoteFiles := make(map[string]string)
	// find same disk and part name archive
	if requiredBackup.DataFormat != "directory" {
		if tableRemoteFile, tableLocalDir, err := b.findDiffOnePartArchive(ctx, requiredBackup, table, localDisk, remoteDisk, part); err == nil {
			tableRemoteFiles[tableRemoteFile] = tableLocalDir
			return tableRemoteFiles, nil, true
		}
	} else {
		// find same disk and part name directory
		if tableRemoteFile, tableLocalDir, err := b.findDiffOnePartDirectory(ctx, requiredBackup, table, localDisk, remoteDisk, part); err == nil {
			tableRemoteFiles[tableRemoteFile] = tableLocalDir
			return tableRemoteFiles, nil, true
		}
	}
	return nil, nil, false
}

func (b *Backuper) findDiffOnePartDirectory(ctx context.Context, requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, localDisk, remoteDisk string, part metadata.Part) (string, string, error) {
	log := apexLog.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffOnePartDirectory"})
	log.Debugf("start")
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	tableRemotePath := path.Join(requiredBackup.BackupName, "shadow", dbAndTableDir, remoteDisk, part.Name)
	tableRemoteFile := path.Join(tableRemotePath, "checksums.txt")
	return b.findDiffFileExist(ctx, requiredBackup, tableRemoteFile, tableRemotePath, localDisk, dbAndTableDir, part)
}

func (b *Backuper) findDiffOnePartArchive(ctx context.Context, requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, localDisk, remoteDisk string, part metadata.Part) (string, string, error) {
	log := apexLog.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffOnePartArchive"})
	log.Debugf("start")
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	remoteExt := config.ArchiveExtensions[requiredBackup.DataFormat]
	tableRemotePath := path.Join(requiredBackup.BackupName, "shadow", dbAndTableDir, fmt.Sprintf("%s_%s.%s", remoteDisk, common.TablePathEncode(part.Name), remoteExt))
	tableRemoteFile := tableRemotePath
	return b.findDiffFileExist(ctx, requiredBackup, tableRemoteFile, tableRemotePath, localDisk, dbAndTableDir, part)
}

func (b *Backuper) findDiffFileExist(ctx context.Context, requiredBackup *metadata.BackupMetadata, tableRemoteFile string, tableRemotePath string, localDisk string, dbAndTableDir string, part metadata.Part) (string, string, error) {
	_, err := b.dst.StatFile(ctx, tableRemoteFile)
	log := b.log.WithField("logger", "findDiffFileExist")
	if err != nil {
		log.WithFields(apexLog.Fields{"tableRemoteFile": tableRemoteFile, "tableRemotePath": tableRemotePath, "part": part.Name}).Debugf("findDiffFileExist not found")
		return "", "", err
	}
	if tableLocalDir, diskExists := b.DiskToPathMap[localDisk]; !diskExists {
		return "", "", fmt.Errorf("`%s` is not found in system.disks", localDisk)
	} else {
		if path.Ext(tableRemoteFile) == ".txt" {
			tableLocalDir = path.Join(tableLocalDir, "backup", requiredBackup.BackupName, "shadow", dbAndTableDir, localDisk, part.Name)
		} else {
			tableLocalDir = path.Join(tableLocalDir, "backup", requiredBackup.BackupName, "shadow", dbAndTableDir, localDisk)
		}
		log.WithFields(apexLog.Fields{"tableRemoteFile": tableRemoteFile, "tableRemotePath": tableRemotePath, "part": part.Name}).Debugf("findDiffFileExist found")
		return tableRemotePath, tableLocalDir, nil
	}
}

func (b *Backuper) ReadBackupMetadataRemote(ctx context.Context, backupName string) (*metadata.BackupMetadata, error) {
	backupList, err := b.dst.BackupList(ctx, true, backupName)
	if err != nil {
		return nil, err
	}
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			return &backup.BackupMetadata, nil
		}
	}
	return nil, fmt.Errorf("%s not found on remote storage", backupName)
}

func (b *Backuper) makePartHardlinks(exists, new string) error {
	log := apexLog.WithField("logger", "makePartHardlinks")
	ex, err := os.Open(exists)
	if err != nil {
		return err
	}
	defer func() {
		if err = ex.Close(); err != nil {
			log.Warnf("Can't close %s", exists)
		}
	}()
	files, err := ex.Readdirnames(-1)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(new, 0750); err != nil {
		log.Warnf("MkDirAll(%s) error: %v", new, err)
		return err
	}
	for _, f := range files {
		existsF := path.Join(exists, f)
		newF := path.Join(new, f)
		if err := os.Link(existsF, newF); err != nil {
			existsFInfo, existsStatErr := os.Stat(existsF)
			newFInfo, newStatErr := os.Stat(newF)
			if existsStatErr != nil || newStatErr != nil || !os.SameFile(existsFInfo, newFInfo) {
				log.Warnf("Link %s -> %s error: %v", newF, existsF, err)
				return err
			}
		}
	}
	return nil
}

func (b *Backuper) downloadSingleBackupFile(ctx context.Context, remoteFile string, localFile string, disks []clickhouse.Disk) error {
	if b.resume && b.resumableState.IsAlreadyProcessedBool(remoteFile) {
		return nil
	}
	log := b.log.WithField("logger", "downloadSingleBackupFile")
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err := retry.RunCtx(ctx, func(ctx context.Context) error {
		remoteReader, err := b.dst.GetFileReader(ctx, remoteFile)
		if err != nil {
			return err
		}
		defer func() {
			err = remoteReader.Close()
			if err != nil {
				log.Warnf("can't close remoteReader %s", remoteFile)
			}
		}()
		localWriter, err := os.OpenFile(localFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
		if err != nil {
			return err
		}

		defer func() {
			err = localWriter.Close()
			if err != nil {
				log.Warnf("can't close localWriter %s", localFile)
			}
		}()

		_, err = io.CopyBuffer(localWriter, remoteReader, nil)
		if err != nil {
			return err
		}

		if err = filesystemhelper.Chown(localFile, b.ch, disks, false); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if b.resume {
		b.resumableState.AppendToState(remoteFile, 0)
	}
	return nil
}
