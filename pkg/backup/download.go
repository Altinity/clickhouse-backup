package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/filesystemhelper"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/AlexAkulov/clickhouse-backup/pkg/common"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/pkg/new_storage"
	legacyStorage "github.com/AlexAkulov/clickhouse-backup/pkg/storage"
	"github.com/AlexAkulov/clickhouse-backup/pkg/utils"

	apexLog "github.com/apex/log"
)

var (
	ErrBackupIsAlreadyExists = errors.New("backup is already exists")
)

func legacyDownload(ctx context.Context, cfg *config.Config, defaultDataPath, backupName string) error {
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "download_legacy",
	})
	bd, err := legacyStorage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}
	if err := bd.Connect(); err != nil {
		return err
	}
	if err := bd.DownloadCompressedStream(ctx, backupName,
		path.Join(defaultDataPath, "backup", backupName)); err != nil {
		return err
	}
	log.Info("done")
	return nil
}

func (b *Backuper) Download(backupName string, tablePattern string, partitions []string, schemaOnly bool) error {
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "download",
	})
	if b.cfg.General.RemoteStorage == "none" {
		return fmt.Errorf("remote storage is 'none'")
	}
	if backupName == "" {
		_ = PrintRemoteBackups(b.cfg, "all")
		return fmt.Errorf("select backup for download")
	}
	localBackups, disks, err := GetLocalBackups(b.cfg, nil)
	if err != nil {
		return err
	}
	for i := range localBackups {
		if backupName == localBackups[i].BackupName {
			return ErrBackupIsAlreadyExists
		}
	}
	startDownload := time.Now()
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()
	if err := b.init(disks); err != nil {
		return err
	}
	remoteBackups, err := b.dst.BackupList(true, backupName)
	if err != nil {
		return err
	}
	found := false
	var remoteBackup new_storage.Backup
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
		return legacyDownload(context.Background(), b.cfg, b.DefaultDataPath, backupName)
	}
	if len(remoteBackup.Tables) == 0 && !b.cfg.General.AllowEmptyBackups {
		return fmt.Errorf("'%s' is empty backup", backupName)
	}
	tablesForDownload := parseTablePatternForDownload(remoteBackup.Tables, tablePattern)
	tableMetadataForDownload := make([]metadata.TableMetadata, len(tablesForDownload))

	if !schemaOnly && !b.cfg.General.DownloadByPart && remoteBackup.RequiredBackup != "" {
		err := b.Download(remoteBackup.RequiredBackup, tablePattern, partitions, schemaOnly)
		if err != nil && err != ErrBackupIsAlreadyExists {
			return err
		}
	}

	dataSize := uint64(0)
	metadataSize := uint64(0)
	err = os.MkdirAll(path.Join(b.DefaultDataPath, "backup", backupName), 0750)
	if err != nil {
		return err
	}
	partitionsToDownloadMap := filesystemhelper.CreatePartitionsToBackupMap(partitions)

	log.Debugf("prepare table METADATA concurrent semaphore with concurrency=%d len(tableMetadataForDownload)=%d", b.cfg.General.DownloadConcurrency, len(tableMetadataForDownload))
	s := semaphore.NewWeighted(int64(b.cfg.General.DownloadConcurrency))
	g, ctx := errgroup.WithContext(context.Background())
	for i, t := range tablesForDownload {
		if err := s.Acquire(ctx, 1); err != nil {
			log.Errorf("can't acquire semaphore during Download: %v", err)
			break
		}
		log := log.WithField("table_metadata", fmt.Sprintf("%s.%s", t.Database, t.Table))
		idx := i
		tableTitle := t
		g.Go(func() error {
			defer s.Release(1)
			downloadedMetadata, size, err := b.downloadTableMetadata(backupName, log, tableTitle, schemaOnly, partitionsToDownloadMap)
			if err != nil {
				return err
			}
			tableMetadataForDownload[idx] = *downloadedMetadata
			atomic.AddUint64(&metadataSize, size)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("one of Download Metadata go-routine return error: %v", err)
	}
	if !schemaOnly {
		for _, t := range tableMetadataForDownload {
			for disk := range t.Parts {
				if _, diskExists := b.DiskToPathMap[disk]; !diskExists {
					return fmt.Errorf("table '%s.%s' require disk '%s' that not found in clickhouse table system.disks, you can add nonexistent disks to disk_mapping config", t.Database, t.Table, disk)
				}
			}
		}
		log.Debugf("prepare table SHADOW concurrent semaphore with concurrency=%d len(tableMetadataForDownload)=%d", b.cfg.General.DownloadConcurrency, len(tableMetadataForDownload))
		s := semaphore.NewWeighted(int64(b.cfg.General.DownloadConcurrency))
		g, ctx := errgroup.WithContext(context.Background())

		for i, tableMetadata := range tableMetadataForDownload {
			if tableMetadata.MetadataOnly {
				continue
			}
			if err := s.Acquire(ctx, 1); err != nil {
				log.Errorf("can't acquire semaphore during Download: %v", err)
				break
			}
			dataSize += tableMetadata.TotalBytes
			idx := i
			g.Go(func() error {
				defer s.Release(1)
				start := time.Now()
				if err := b.downloadTableData(remoteBackup.BackupMetadata, tableMetadataForDownload[idx]); err != nil {
					return err
				}
				log.
					WithField("operation", "download_data").
					WithField("table", fmt.Sprintf("%s.%s", tableMetadataForDownload[idx].Database, tableMetadataForDownload[idx].Table)).
					WithField("duration", utils.HumanizeDuration(time.Since(start))).
					WithField("size", utils.FormatBytes(tableMetadataForDownload[idx].TotalBytes)).
					Info("done")
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return fmt.Errorf("one of Download go-routine return error: %v", err)
		}
	}
	rbacSize, err := b.downloadRBACData(remoteBackup)
	if err != nil {
		return fmt.Errorf("download RBAC error: %v", err)
	}

	configSize, err := b.downloadConfigData(remoteBackup)
	if err != nil {
		return fmt.Errorf("download CONFIGS error: %v", err)
	}

	backupMetadata := remoteBackup.BackupMetadata
	backupMetadata.Tables = tablesForDownload
	backupMetadata.DataSize = dataSize
	backupMetadata.MetadataSize = metadataSize
	backupMetadata.CompressedSize = 0
	backupMetadata.DataFormat = ""
	backupMetadata.RequiredBackup = ""
	backupMetadata.ConfigSize = configSize
	backupMetadata.RBACSize = rbacSize

	backupMetafileLocalPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
	if err := backupMetadata.Save(backupMetafileLocalPath); err != nil {
		return err
	}
	log.
		WithField("duration", utils.HumanizeDuration(time.Since(startDownload))).
		WithField("size", utils.FormatBytes(dataSize+metadataSize+rbacSize+configSize)).
		Info("done")
	return nil
}

func (b *Backuper) downloadTableMetadataIfNotExists(backupName string, log *apexLog.Entry, tableTitle metadata.TableTitle) (*metadata.TableMetadata, error) {
	metadataLocalFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	tm := &metadata.TableMetadata{}
	if _, err := tm.Load(metadataLocalFile); err == nil {
		return tm, nil
	}
	tm, _, err := b.downloadTableMetadata(backupName, log.WithFields(apexLog.Fields{"operation": "downloadTableMetadataIfNotExists", "table_metadata_diff": fmt.Sprintf("%s.%s", tableTitle.Database, tableTitle.Table)}), tableTitle, false, nil)
	return tm, err
}

func (b *Backuper) downloadTableMetadata(backupName string, log *apexLog.Entry, tableTitle metadata.TableTitle, schemaOnly bool, partitionsFilter common.EmptyMap) (*metadata.TableMetadata, uint64, error) {
	start := time.Now()
	size := uint64(0)
	remoteTableMetadata := path.Join(backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	tmReader, err := b.dst.GetFileReader(remoteTableMetadata)
	if err != nil {
		return nil, 0, err
	}
	tmBody, err := ioutil.ReadAll(tmReader)
	if err != nil {
		return nil, 0, err
	}
	err = tmReader.Close()
	if err != nil {
		return nil, 0, err
	}
	var tableMetadata metadata.TableMetadata
	if err = json.Unmarshal(tmBody, &tableMetadata); err != nil {
		return nil, 0, err
	}
	filterPartsByPartitionsFilter(tableMetadata, partitionsFilter)
	// save metadata
	metadataLocalFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	size, err = tableMetadata.Save(metadataLocalFile, schemaOnly)
	if err != nil {
		return nil, 0, err
	}
	log.
		WithField("duration", utils.HumanizeDuration(time.Since(start))).
		WithField("size", utils.FormatBytes(size)).
		Info("done")
	return &tableMetadata, size, nil
}

func (b *Backuper) downloadRBACData(remoteBackup new_storage.Backup) (uint64, error) {
	return b.downloadBackupRelatedDir(remoteBackup, "access")
}

func (b *Backuper) downloadConfigData(remoteBackup new_storage.Backup) (uint64, error) {
	return b.downloadBackupRelatedDir(remoteBackup, "configs")
}

func (b *Backuper) downloadBackupRelatedDir(remoteBackup new_storage.Backup, prefix string) (uint64, error) {
	archiveFile := fmt.Sprintf("%s.%s", prefix, b.cfg.GetArchiveExtension())
	remoteFile := path.Join(remoteBackup.BackupName, archiveFile)
	localDir := path.Join(b.DefaultDataPath, "backup", remoteBackup.BackupName, prefix)
	remoteFileInfo, err := b.dst.StatFile(remoteFile)
	if err != nil {
		apexLog.Debugf("%s not exists on remote storage, skip download", remoteFile)
		return 0, nil
	}
	if err = b.dst.DownloadCompressedStream(context.Background(), remoteFile, localDir); err != nil {
		return 0, err
	}
	return uint64(remoteFileInfo.Size()), nil
}

func (b *Backuper) downloadTableData(remoteBackup metadata.BackupMetadata, table metadata.TableMetadata) error {
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))

	s := semaphore.NewWeighted(int64(b.cfg.General.DownloadConcurrency))
	g, ctx := errgroup.WithContext(context.Background())

	if remoteBackup.DataFormat != "directory" {
		capacity := 0
		downloadOffset := make(map[string]int, 0)
		for disk := range table.Files {
			capacity += len(table.Files[disk])
			downloadOffset[disk] = 0
		}
		apexLog.Debugf("start downloadTableData %s.%s with concurrency=%d len(table.Files[...])=%d", table.Database, table.Table, b.cfg.General.DownloadConcurrency, capacity)
		for common.SumMapValuesInt(downloadOffset) < capacity {
			for disk := range table.Files {
				if downloadOffset[disk] >= len(table.Files[disk]) {
					continue
				}
				if err := s.Acquire(ctx, 1); err != nil {
					apexLog.Errorf("can't acquire semaphore during downloadTableData: %v", err)
					break
				}
				backupPath := b.DiskToPathMap[disk]
				tableLocalDir := path.Join(backupPath, "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
				archiveFile := table.Files[disk][downloadOffset[disk]]
				downloadOffset[disk] += 1
				tableRemoteFile := path.Join(remoteBackup.BackupName, "shadow", common.TablePathEncode(table.Database), common.TablePathEncode(table.Table), archiveFile)
				g.Go(func() error {
					apexLog.Debugf("start download from %s", tableRemoteFile)
					defer s.Release(1)
					if err := b.dst.DownloadCompressedStream(ctx, tableRemoteFile, tableLocalDir); err != nil {
						return err
					}
					apexLog.Debugf("finish download from %s", tableRemoteFile)
					return nil
				})
			}
		}
	} else {
		capacity := 0
		for disk := range table.Parts {
			capacity += len(table.Parts[disk])
		}
		apexLog.Debugf("start downloadTableData %s.%s with concurrency=%d len(table.Parts[...])=%d", table.Database, table.Table, b.cfg.General.DownloadConcurrency, capacity)
		for disk := range table.Parts {
			if err := s.Acquire(ctx, 1); err != nil {
				apexLog.Errorf("can't acquire semaphore during downloadTableData: %v", err)
				break
			}
			tableRemotePath := path.Join(remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
			diskPath := b.DiskToPathMap[disk]
			tableLocalDir := path.Join(diskPath, "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
			g.Go(func() error {
				apexLog.Debugf("start download from %s to %s", tableLocalDir, tableRemotePath)
				defer s.Release(1)
				if err := b.dst.DownloadPath(0, tableRemotePath, tableLocalDir); err != nil {
					return err
				}
				apexLog.Debugf("finish download from %s to %s", tableLocalDir, tableRemotePath)
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("one of downloadTableData go-routine return error: %v", err)
	}

	err := b.downloadDiffParts(remoteBackup, table, dbAndTableDir)
	if err != nil {
		return err
	}

	return nil
}

func (b *Backuper) downloadDiffParts(remoteBackup metadata.BackupMetadata, table metadata.TableMetadata, dbAndTableDir string) error {
	log := apexLog.WithField("operation", "downloadDiffParts")
	log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).Debug("start")
	start := time.Now()
	downloadedDiffParts := uint32(0)
	s := semaphore.NewWeighted(int64(b.cfg.General.DownloadConcurrency))
	g, ctx := errgroup.WithContext(context.Background())

	diffRemoteFilesCache := map[string]*sync.Mutex{}
	diffRemoteFilesLock := &sync.Mutex{}

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
				if err := s.Acquire(ctx, 1); err != nil {
					log.Errorf("can't acquire semaphore during downloadDiffParts: %v", err)
					break
				}
				partForDownload := part
				diskForDownload := disk
				g.Go(func() error {
					defer s.Release(1)
					tableRemoteFiles, err := b.findDiffBackupFilesRemote(remoteBackup, table, diskForDownload, partForDownload, log)
					if err != nil {
						return err
					}

					for tableRemoteFile, tableLocalDir := range tableRemoteFiles {
						err = b.downloadDiffRemoteFile(ctx, diffRemoteFilesLock, diffRemoteFilesCache, tableRemoteFile, tableLocalDir)
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
								if err = makePartHardlinks(downloadedPartPath, existsPath); err != nil {
									return fmt.Errorf("can't to add link to exists part %s -> %s error: %v", newPath, existsPath, err)
								}
							}
							if err != nil && !os.IsNotExist(err) {
								return fmt.Errorf("after downloadDiffRemoteFile %s stat return error: %v", downloadedPartPath, err)
							}
						}
						atomic.AddUint32(&downloadedDiffParts, 1)
					}
					if err = makePartHardlinks(existsPath, newPath); err != nil {
						return fmt.Errorf("can't to add link to exists part %s -> %s error: %v", newPath, existsPath, err)
					}
					return nil
				})
			} else {
				if err = makePartHardlinks(existsPath, newPath); err != nil {
					return fmt.Errorf("can't to add exists part: %v", err)
				}
			}
		}
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("one of downloadDiffParts go-routine return error: %v", err)
	}
	log.WithField("duration", utils.HumanizeDuration(time.Since(start))).WithField("diff_parts", strconv.Itoa(int(downloadedDiffParts))).Info("done")
	return nil
}

func (b *Backuper) downloadDiffRemoteFile(ctx context.Context, diffRemoteFilesLock *sync.Mutex, diffRemoteFilesCache map[string]*sync.Mutex, tableRemoteFile string, tableLocalDir string) error {
	diffRemoteFilesLock.Lock()
	namedLock, isCached := diffRemoteFilesCache[tableRemoteFile]
	log := apexLog.WithField("operation", "downloadDiffRemoteFile")
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
			if err := b.dst.DownloadCompressedStream(ctx, tableRemoteFile, tableLocalDir); err != nil {
				log.Warnf("DownloadCompressedStream %s -> %s return error: %v", tableRemoteFile, tableLocalDir, err)
				return err
			}
		} else {
			// remoteFile could be a directory
			if err := b.dst.DownloadPath(0, tableRemoteFile, tableLocalDir); err != nil {
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

func (b *Backuper) findDiffBackupFilesRemote(backup metadata.BackupMetadata, table metadata.TableMetadata, disk string, part metadata.Part, log *apexLog.Entry) (map[string]string, error) {
	var requiredTable *metadata.TableMetadata
	log.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name}).Debugf("findDiffBackupFilesRemote")
	requiredBackup, err := b.ReadBackupMetadataRemote(backup.RequiredBackup)
	if err != nil {
		return nil, err
	}
	requiredTable, err = b.downloadTableMetadataIfNotExists(requiredBackup.BackupName, log, metadata.TableTitle{Database: table.Database, Table: table.Table})
	if err != nil {
		log.Warnf("downloadTableMetadataIfNotExists %s / %s.%s return error", requiredBackup.BackupName, table.Database, table.Table)
		return nil, err
	}

	// recursive find if part in RequiredBackup also Required
	tableRemoteFiles, found, err := b.findDiffRecursive(requiredBackup, log, table, requiredTable, part, disk)
	if found {
		return tableRemoteFiles, nil
	}

	found = false
	// try to find part on the same disk
	tableRemoteFiles, err, found = b.findDiffOnePart(requiredBackup, table, disk, disk, part)
	if found {
		return tableRemoteFiles, nil
	}

	// try to find part on other disks
	for requiredDisk := range requiredBackup.Disks {
		if requiredDisk != disk {
			tableRemoteFiles, err, found = b.findDiffOnePart(requiredBackup, table, disk, requiredDisk, part)
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

func (b *Backuper) findDiffRecursive(requiredBackup *metadata.BackupMetadata, log *apexLog.Entry, table metadata.TableMetadata, requiredTable *metadata.TableMetadata, part metadata.Part, disk string) (map[string]string, bool, error) {
	log.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name}).Debugf("findDiffRecursive")
	found := false
	for _, requiredParts := range requiredTable.Parts {
		for _, requiredPart := range requiredParts {
			if requiredPart.Name == part.Name {
				found = true
				if requiredPart.Required {
					tableRemoteFiles, err := b.findDiffBackupFilesRemote(*requiredBackup, table, disk, part, log)
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

func (b *Backuper) findDiffOnePart(requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, localDisk, remoteDisk string, part metadata.Part) (map[string]string, error, bool) {
	apexLog.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name}).Debugf("findDiffOnePart")
	tableRemoteFiles := make(map[string]string)
	// find same disk and part name archive
	if requiredBackup.DataFormat != "directory" {
		if tableRemoteFile, tableLocalDir, err := b.findDiffOnePartArchive(requiredBackup, table, localDisk, remoteDisk, part); err == nil {
			tableRemoteFiles[tableRemoteFile] = tableLocalDir
			return tableRemoteFiles, nil, true
		}
	} else {
		// find same disk and part name directory
		if tableRemoteFile, tableLocalDir, err := b.findDiffOnePartDirectory(requiredBackup, table, localDisk, remoteDisk, part); err == nil {
			tableRemoteFiles[tableRemoteFile] = tableLocalDir
			return tableRemoteFiles, nil, true
		}
	}
	return nil, nil, false
}

func (b *Backuper) findDiffOnePartDirectory(requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, localDisk, remoteDisk string, part metadata.Part) (string, string, error) {
	apexLog.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name}).Debugf("findDiffOnePartDirectory")
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	tableRemotePath := path.Join(requiredBackup.BackupName, "shadow", dbAndTableDir, remoteDisk, part.Name)
	tableRemoteFile := path.Join(tableRemotePath, "checksums.txt")
	return b.findDiffFileExist(requiredBackup, tableRemoteFile, tableRemotePath, localDisk, dbAndTableDir, part)
}

func (b *Backuper) findDiffOnePartArchive(requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, localDisk, remoteDisk string, part metadata.Part) (string, string, error) {
	apexLog.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name}).Debugf("findDiffOnePartArchive")
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	remoteExt := config.ArchiveExtensions[requiredBackup.DataFormat]
	tableRemotePath := path.Join(requiredBackup.BackupName, "shadow", dbAndTableDir, fmt.Sprintf("%s_%s.%s", remoteDisk, part.Name, remoteExt))
	tableRemoteFile := tableRemotePath
	return b.findDiffFileExist(requiredBackup, tableRemoteFile, tableRemotePath, localDisk, dbAndTableDir, part)
}

func (b *Backuper) findDiffFileExist(requiredBackup *metadata.BackupMetadata, tableRemoteFile string, tableRemotePath string, localDisk string, dbAndTableDir string, part metadata.Part) (string, string, error) {
	//apexLog.WithFields(apexLog.Fields{"tableRemoteFile": tableRemoteFile, "tableRemotePath": tableRemotePath, "part": part.Name}).Debugf("findDiffFileExist start")
	_, err := b.dst.StatFile(tableRemoteFile)
	if err != nil {
		apexLog.WithFields(apexLog.Fields{"tableRemoteFile": tableRemoteFile, "tableRemotePath": tableRemotePath, "part": part.Name}).Debugf("findDiffFileExist not found")
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
		apexLog.WithFields(apexLog.Fields{"tableRemoteFile": tableRemoteFile, "tableRemotePath": tableRemotePath, "part": part.Name}).Debugf("findDiffFileExist found")
		return tableRemotePath, tableLocalDir, nil
	}
}

func (b *Backuper) ReadBackupMetadataRemote(backupName string) (*metadata.BackupMetadata, error) {
	backupList, err := b.dst.BackupList(true, backupName)
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

func makePartHardlinks(exists, new string) error {
	ex, err := os.Open(exists)
	if err != nil {
		return err
	}
	defer func() {
		if err = ex.Close(); err != nil {
			apexLog.Warnf("Can't close %s", exists)
		}
	}()
	files, err := ex.Readdirnames(-1)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(new, 0750); err != nil {
		apexLog.Warnf("makePartHardlinks::MkDirAll %v", err)
		return err
	}
	for _, f := range files {
		existsF := path.Join(exists, f)
		newF := path.Join(new, f)
		if err := os.Link(existsF, newF); err != nil {
			apexLog.Warnf("makePartHardlinks::Link %s -> %s: %v", newF, existsF, err)
			return err
		}
	}
	return nil
}
