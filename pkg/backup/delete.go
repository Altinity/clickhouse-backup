package backup

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/custom"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

	apexLog "github.com/apex/log"
	"github.com/pkg/errors"
)

// Clean - removed all data in shadow folder
func (b *Backuper) Clean(ctx context.Context) error {
	log := b.log.WithField("logger", "Clean")
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		return err
	}
	for _, disk := range disks {
		if disk.IsBackup {
			continue
		}
		shadowDir := path.Join(disk.Path, "shadow")
		if err := b.cleanDir(shadowDir); err != nil {
			return fmt.Errorf("can't clean '%s': %v", shadowDir, err)
		}
		log.Info(shadowDir)
	}
	return nil
}

func (b *Backuper) cleanDir(dirName string) error {
	items, err := os.ReadDir(dirName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, item := range items {
		if err = os.RemoveAll(path.Join(dirName, item.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Delete - remove local or remote backup
func (b *Backuper) Delete(backupType, backupName string, commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	switch backupType {
	case "local":
		return b.RemoveBackupLocal(ctx, backupName, nil)
	case "remote":
		return b.RemoveBackupRemote(ctx, backupName)
	default:
		return fmt.Errorf("unknown backup type")
	}
}

func (b *Backuper) RemoveOldBackupsLocal(ctx context.Context, keepLastBackup bool, disks []clickhouse.Disk) error {
	keep := b.cfg.General.BackupsToKeepLocal
	if keep == 0 {
		return nil
	}
	// fix https://github.com/Altinity/clickhouse-backup/issues/698
	if keep < 0 {
		keep = 0
		if keepLastBackup {
			keep = 1
		}
	}

	backupList, disks, err := b.GetLocalBackups(ctx, disks)
	if err != nil {
		return err
	}
	backupsToDelete := GetBackupsToDelete(backupList, keep)
	for _, backup := range backupsToDelete {
		if err := b.RemoveBackupLocal(ctx, backup.BackupName, disks); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backuper) RemoveBackupLocal(ctx context.Context, backupName string, disks []clickhouse.Disk) error {
	log := b.log.WithField("logger", "RemoveBackupLocal")
	var err error
	start := time.Now()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	if err = b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()
	if disks == nil {
		disks, err = b.ch.GetDisks(ctx, true)
		if err != nil {
			return err
		}
	}
	backupList, disks, err := b.GetLocalBackups(ctx, disks)
	if err != nil {
		return err
	}

	if b.hasObjectDisks(backupList, backupName, disks) {
		bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, false, backupName)
		if err != nil {
			return err
		}
		err = bd.Connect(ctx)
		if err != nil {
			return fmt.Errorf("can't connect to remote storage: %v", err)
		}
		defer func() {
			if err := bd.Close(ctx); err != nil {
				b.log.Warnf("can't close BackupDestination error: %v", err)
			}
		}()
		b.dst = bd
	}

	for _, backup := range backupList {
		if backup.BackupName == backupName {
			var skip bool
			skip, err = b.skipIfTheSameRemoteBackupPresent(ctx, backup.BackupName, backup.Tags)
			if err != nil {
				return err
			}
			if !skip && strings.Contains(backup.Tags, "embedded") {
				if err = b.cleanLocalEmbedded(ctx, backup, disks); err != nil {
					log.Warnf("b.cleanRemoteEmbedded return error: %v", err)
					return err
				}
			}

			for _, disk := range disks {
				backupPath := path.Join(disk.Path, "backup", backupName)
				if disk.IsBackup {
					backupPath = path.Join(disk.Path, backupName)
				}
				if !skip && !disk.IsBackup && (disk.Type == "s3" || disk.Type == "azure_blob_storage") && !strings.Contains(backup.Tags, "embedded") {
					if err = b.cleanLocalBackupObjectDisk(ctx, backupName, backupPath, disk.Name); err != nil {
						return err
					}
				}
				log.Debugf("remove '%s'", backupPath)
				if err = os.RemoveAll(backupPath); err != nil {
					return err
				}
			}
			log.WithField("operation", "delete").
				WithField("location", "local").
				WithField("backup", backupName).
				WithField("duration", utils.HumanizeDuration(time.Since(start))).
				Info("done")
			return nil
		}
	}
	return fmt.Errorf("'%s' is not found on local storage", backupName)
}

func (b *Backuper) hasObjectDisks(backupList []LocalBackup, backupName string, disks []clickhouse.Disk) bool {
	for _, backup := range backupList {
		if backup.BackupName == backupName && !strings.Contains(backup.Tags, "embedded") {
			for _, disk := range disks {
				if !disk.IsBackup && (disk.Type == "s3" || disk.Type == "azure_blob_storage") {
					backupExists, err := os.ReadDir(path.Join(disk.Path, "backup", backup.BackupName))
					if err == nil && len(backupExists) > 0 {
						return true
					}
				}
			}
		}
	}
	return false
}

func (b *Backuper) cleanLocalBackupObjectDisk(ctx context.Context, backupName string, backupPath, diskName string) error {
	_, err := os.Stat(backupPath)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	err = filepath.Walk(backupPath, func(fPath string, fInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fInfo.IsDir() {
			return nil
		}
		objMeta, err := object_disk.ReadMetadataFromFile(fPath)
		if err != nil {
			return err
		}
		for _, storageObject := range objMeta.StorageObjects {
			if err = b.dst.DeleteFileFromObjectDiskBackup(ctx, path.Join(backupName, diskName, storageObject.ObjectRelativePath)); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (b *Backuper) cleanLocalEmbedded(ctx context.Context, backup LocalBackup, disks []clickhouse.Disk) error {
	for _, disk := range disks {
		if disk.Name == b.cfg.ClickHouse.EmbeddedBackupDisk {
			if err := object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, disk.Name); err != nil {
				return err
			}
			backupPath := path.Join(disk.Path, backup.BackupName)
			if err := filepath.Walk(backupPath, func(filePath string, info fs.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() && !strings.HasSuffix(filePath, ".json") {
					apexLog.Debugf("object_disk.ReadMetadataFromFile(%s)", filePath)
					meta, err := object_disk.ReadMetadataFromFile(filePath)
					if err != nil {
						return err
					}
					for _, o := range meta.StorageObjects {
						err = object_disk.DeleteFile(ctx, b.cfg.ClickHouse.EmbeddedBackupDisk, o.ObjectRelativePath)
						if err != nil {
							return err
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Backuper) skipIfTheSameRemoteBackupPresent(ctx context.Context, backupName, tags string) (bool, error) {
	if b.cfg.General.RemoteStorage != "custom" && b.cfg.General.RemoteStorage != "none" {
		if remoteList, err := b.GetRemoteBackups(ctx, true); err != nil {
			return true, err
		} else {
			for _, remoteBackup := range remoteList {
				if remoteBackup.BackupName == backupName {
					if tags == "" || (tags != "" && strings.Contains(remoteBackup.Tags, tags)) {
						return true, nil
					}
				}
			}
		}
	}
	return false, nil
}

func (b *Backuper) RemoveBackupRemote(ctx context.Context, backupName string) error {
	log := b.log.WithField("logger", "RemoveBackupRemote")
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	start := time.Now()
	if b.cfg.General.RemoteStorage == "none" {
		err := errors.New("aborted: RemoteStorage set to \"none\"")
		log.Error(err.Error())
		return err
	}
	if b.cfg.General.RemoteStorage == "custom" {
		return custom.DeleteRemote(ctx, b.cfg, backupName)
	}
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, false, "")
	if err != nil {
		return err
	}
	err = bd.Connect(ctx)
	if err != nil {
		return fmt.Errorf("can't connect to remote storage: %v", err)
	}
	defer func() {
		if err := bd.Close(ctx); err != nil {
			b.log.Warnf("can't close BackupDestination error: %v", err)
		}
	}()

	b.dst = bd

	backupList, err := bd.BackupList(ctx, true, backupName)
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			if skip, err := b.skipIfSameLocalBackupPresent(ctx, backup.BackupName, backup.Tags); err != nil {
				return err
			} else if !skip {
				if strings.Contains(backup.Tags, "embedded") {
					if err = b.cleanRemoteEmbedded(ctx, backup, bd); err != nil {
						log.Warnf("b.cleanRemoteEmbedded return error: %v", err)
						return err
					}
				} else if err = b.cleanRemoteBackupObjectDisks(ctx, backup); err != nil {
					log.Warnf("b.cleanRemoteBackupObjectDisks return error: %v", err)
					return err
				}
			}

			if err = bd.RemoveBackup(ctx, backup); err != nil {
				log.Warnf("bd.RemoveBackup return error: %v", err)
				return err
			}
			log.WithFields(apexLog.Fields{
				"backup":    backupName,
				"location":  "remote",
				"operation": "delete",
				"duration":  utils.HumanizeDuration(time.Since(start)),
			}).Info("done")
			return nil
		}
	}
	return fmt.Errorf("'%s' is not found on remote storage", backupName)
}

func (b *Backuper) cleanRemoteBackupObjectDisks(ctx context.Context, backup storage.Backup) error {
	if b.dst.Kind() != "azblob" && b.dst.Kind() != "s3" && b.dst.Kind() != "gcs" {
		return nil
	}
	if !backup.Legacy && len(backup.Disks) > 0 && backup.DiskTypes != nil && len(backup.DiskTypes) < len(backup.Disks) {
		return fmt.Errorf("RemoveRemoteBackupObjectDisks: invalid backup.DiskTypes=%#v, not correlated with backup.Disks=%#v", backup.DiskTypes, backup.Disks)
	}
	return b.dst.Walk(ctx, backup.BackupName+"/", true, func(ctx context.Context, f storage.RemoteFile) error {
		fName := path.Join(backup.BackupName, f.Name())
		if !strings.HasPrefix(fName, path.Join(backup.BackupName, "/shadow/")) {
			return nil
		}
		for diskName, diskType := range backup.DiskTypes {
			if diskType == "s3" || diskType == "azure_blob_storage" {
				compressedRE := regexp.MustCompile(`/shadow/([^/]+/[^/]+)/` + diskName + `_[^/]+$`)
				if matches := compressedRE.FindStringSubmatch(fName); len(matches) > 0 {
					// compressed remote object disk part
					localPath := path.Join(backup.Disks[diskName], "backup", backup.BackupName, "shadow", matches[1], diskName)
					if err := b.dst.DownloadCompressedStream(ctx, fName, localPath); err != nil {
						return err
					}
					walkErr := filepath.Walk(localPath, func(fPath string, fInfo fs.FileInfo, err error) error {
						if err != nil {
							return err
						}
						if fInfo.IsDir() {
							return nil
						}
						objMeta, err := object_disk.ReadMetadataFromFile(fPath)
						if err != nil {
							return err
						}
						for _, storageObject := range objMeta.StorageObjects {
							err = b.dst.DeleteFileFromObjectDiskBackup(ctx, path.Join(backup.BackupName, diskName, storageObject.ObjectRelativePath))
							if err != nil {
								return err
							}
						}
						return nil
					})
					if walkErr != nil {
						b.log.Warnf("filepath.Walk(%s) return error: %v", localPath, walkErr)
					}
					if err := os.RemoveAll(localPath); err != nil {
						return err
					}
				} else if regexp.MustCompile(`/shadow/[^/]+/[^/]+/` + diskName + `/.+$`).MatchString(fName) {
					// non compressed remote object disk part
					objMetaReader, err := b.dst.GetFileReader(ctx, fName)
					if err != nil {
						return err
					}
					objMeta, err := object_disk.ReadMetadataFromReader(objMetaReader, fName)
					if err != nil {
						return err
					}
					for _, storageObject := range objMeta.StorageObjects {
						err = b.dst.DeleteFileFromObjectDiskBackup(ctx, path.Join(backup.BackupName, diskName, storageObject.ObjectRelativePath))
						if err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	})
}

func (b *Backuper) cleanRemoteEmbedded(ctx context.Context, backup storage.Backup, bd *storage.BackupDestination) error {
	if err := object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, b.cfg.ClickHouse.EmbeddedBackupDisk); err != nil {
		return err
	}
	return bd.Walk(ctx, backup.BackupName+"/", true, func(ctx context.Context, f storage.RemoteFile) error {
		if !strings.HasSuffix(f.Name(), ".json") {
			r, err := bd.GetFileReader(ctx, path.Join(backup.BackupName, f.Name()))
			if err != nil {
				return err
			}
			apexLog.Debugf("object_disk.ReadMetadataFromReader(%s)", f.Name())
			meta, err := object_disk.ReadMetadataFromReader(r, f.Name())
			if err != nil {
				return err
			}
			for _, o := range meta.StorageObjects {
				if err = object_disk.DeleteFile(ctx, b.cfg.ClickHouse.EmbeddedBackupDisk, o.ObjectRelativePath); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (b *Backuper) skipIfSameLocalBackupPresent(ctx context.Context, backupName, tags string) (bool, error) {
	if localList, _, err := b.GetLocalBackups(ctx, nil); err != nil {
		return true, err
	} else {
		for _, localBackup := range localList {
			if localBackup.BackupName == backupName && strings.Contains(localBackup.Tags, tags) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (b *Backuper) CleanRemoteBroken(commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	remoteBackups, err := b.GetRemoteBackups(ctx, true)
	if err != nil {
		return err
	}
	for _, backup := range remoteBackups {
		if backup.Broken != "" {
			if err = b.RemoveBackupRemote(ctx, backup.BackupName); err != nil {
				return err
			}
		}
	}
	return nil
}
