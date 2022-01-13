package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/pkg/new_storage"
	legacyStorage "github.com/AlexAkulov/clickhouse-backup/pkg/storage"
	"github.com/AlexAkulov/clickhouse-backup/utils"

	apexLog "github.com/apex/log"
)

var (
	ErrBackupIsAlreadyExists = errors.New("backup is already exists")
)

func legacyDownload(cfg *config.Config, defaultDataPath, backupName string) error {
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "download",
	})
	bd, err := legacyStorage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}
	if err := bd.Connect(); err != nil {
		return err
	}
	if err := bd.CompressedStreamDownload(backupName,
		path.Join(defaultDataPath, "backup", backupName)); err != nil {
		return err
	}
	log.Info("done")
	return nil
}

func (b *Backuper) Download(backupName string, tablePattern string, schemaOnly bool) error {
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
	localBackups, err := GetLocalBackups(b.cfg)
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
	if err := b.init(); err != nil {
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
		return legacyDownload(b.cfg, b.DefaultDataPath, backupName)
	}
	// if len(remoteBackup.Tables) == 0 && !b.cfg.General.AllowEmptyBackups {
	// 	return fmt.Errorf("'%s' is empty backup", backupName)
	// }
	tablesForDownload := parseTablePatternForDownload(remoteBackup.Tables, tablePattern)
	tableMetadataForDownload := make([]metadata.TableMetadata, len(tablesForDownload))

	if !schemaOnly && remoteBackup.RequiredBackup != "" {
		err := b.Download(remoteBackup.RequiredBackup, tablePattern, schemaOnly)
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
	for i, t := range tablesForDownload {
		start := time.Now()
		log := log.WithField("table_metadata", fmt.Sprintf("%s.%s", t.Database, t.Table))
		remoteTableMetadata := path.Join(backupName, "metadata", clickhouse.TablePathEncode(t.Database), fmt.Sprintf("%s.json", clickhouse.TablePathEncode(t.Table)))
		tmReader, err := b.dst.GetFileReader(remoteTableMetadata)
		if err != nil {
			return err
		}
		tmBody, err := ioutil.ReadAll(tmReader)
		if err != nil {
			return err
		}
		err = tmReader.Close()
		if err != nil {
			return err
		}
		var tableMetadata metadata.TableMetadata
		if err = json.Unmarshal(tmBody, &tableMetadata); err != nil {
			return err
		}
		tableMetadataForDownload[i] = tableMetadata

		// save metadata
		metadataLocalFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata", clickhouse.TablePathEncode(t.Database), fmt.Sprintf("%s.json", clickhouse.TablePathEncode(t.Table)))
		size, err := tableMetadata.Save(metadataLocalFile, schemaOnly)
		if err != nil {
			return err
		}
		metadataSize += size
		log.
			WithField("duration", utils.HumanizeDuration(time.Since(start))).
			WithField("size", utils.FormatBytes(size)).
			Info("done")
	}
	if !schemaOnly {
		for _, t := range tableMetadataForDownload {
			for disk := range t.Parts {
				if _, ok := b.DiskMap[disk]; !ok {
					return fmt.Errorf("table '%s.%s' require disk '%s' that not found in clickhouse, you can add nonexistent disks to disk_mapping config", t.Database, t.Table, disk)
				}
			}
		}
		log.Debugf("prepare table concurrent semaphore with concurrency=%d len(tableMetadataForDownload)=%d", b.cfg.General.UploadConcurrency, len(tableMetadataForDownload))
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
	if err = b.dst.CompressedStreamDownload(remoteFile, localDir); err != nil {
		return 0, err
	}
	return uint64(remoteFileInfo.Size()), nil
}

func (b *Backuper) downloadTableData(remoteBackup metadata.BackupMetadata, table metadata.TableMetadata) error {
	dbAndTableDir := path.Join(clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table))

	s := semaphore.NewWeighted(int64(b.cfg.General.DownloadConcurrency))
	g, ctx := errgroup.WithContext(context.Background())

	if remoteBackup.DataFormat != "directory" {
		capacity := 0
		for disk := range table.Files {
			capacity += len(table.Files[disk])
		}
		apexLog.Debugf("start downloadTableData %s.%s with concurrency=%d len(table.Files[...])=%d", table.Database, table.Table, b.cfg.General.DownloadConcurrency, capacity)

		for disk := range table.Files {
			diskPath := b.DiskMap[disk]
			tableLocalDir := path.Join(diskPath, "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
			for _, archiveFile := range table.Files[disk] {
				if err := s.Acquire(ctx, 1); err != nil {
					apexLog.Errorf("can't acquire semaphore during downloadTableData: %v", err)
					break
				}
				tableRemoteFile := path.Join(remoteBackup.BackupName, "shadow", clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table), archiveFile)
				g.Go(func() error {
					apexLog.Debugf("start download from %s", tableRemoteFile)
					defer s.Release(1)
					if err := b.dst.CompressedStreamDownload(tableRemoteFile, tableLocalDir); err != nil {
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
			diskPath := b.DiskMap[disk]
			tableLocalDir := path.Join(diskPath, "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
			g.Go(func() error {
				apexLog.Debugf("start download from %s", tableRemotePath)
				defer s.Release(1)
				if err := b.dst.DownloadPath(0, tableRemotePath, tableLocalDir); err != nil {
					return err
				}
				apexLog.Debugf("finish download from %s", tableRemotePath)
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("one of downloadTableData go-routine return error: %v", err)
	}

	// Create hardlink for exists parts
	for disk, parts := range table.Parts {
		for _, p := range parts {
			if !p.Required {
				continue
			}
			existsPath := path.Join(b.DiskMap[disk], "backup", remoteBackup.RequiredBackup, "shadow", dbAndTableDir, disk, p.Name)
			newPath := path.Join(b.DiskMap[disk], "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk, p.Name)
			if err := duplicatePart(existsPath, newPath); err != nil {
				return fmt.Errorf("can't to add exists part: %s", err)
			}
		}
	}
	return nil
}

func duplicatePart(exists, new string) error {
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
		return err
	}
	for _, f := range files {
		if err := os.Link(path.Join(exists, f), path.Join(new, f)); err != nil {
			return err
		}
	}
	return nil
}
