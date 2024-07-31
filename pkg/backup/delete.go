package backup

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
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
	backupsToDelete := GetBackupsToDeleteLocal(backupList, keep)
	for _, backup := range backupsToDelete {
		if deleteErr := b.RemoveBackupLocal(ctx, backup.BackupName, disks); deleteErr != nil {
			return deleteErr
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
	hasObjectDisks := b.hasObjectDisksLocal(backupList, backupName, disks)

	for _, backup := range backupList {
		if backup.BackupName == backupName {
			b.isEmbedded = strings.Contains(backup.Tags, "embedded")
			if hasObjectDisks || (b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk == "") {
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
			err = b.cleanEmbeddedAndObjectDiskLocalIfSameRemoteNotPresent(ctx, backupName, disks, backup, hasObjectDisks, log)
			if err != nil {
				return err
			}
			for _, disk := range disks {
				backupPath := path.Join(disk.Path, "backup", backupName)
				if disk.IsBackup {
					backupPath = path.Join(disk.Path, backupName)
				}
				log.Infof("remove '%s'", backupPath)
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

func (b *Backuper) cleanEmbeddedAndObjectDiskLocalIfSameRemoteNotPresent(ctx context.Context, backupName string, disks []clickhouse.Disk, backup LocalBackup, hasObjectDisks bool, log *apexLog.Entry) error {
	skip, err := b.skipIfTheSameRemoteBackupPresent(ctx, backup.BackupName, backup.Tags)
	log.Debugf("b.skipIfTheSameRemoteBackupPresent return skip=%v", skip)
	if err != nil {
		return err
	}
	if !skip && (hasObjectDisks || (b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk == "")) {
		startTime := time.Now()
		if deletedKeys, deleteErr := b.cleanBackupObjectDisks(ctx, backupName); deleteErr != nil {
			log.Warnf("b.cleanBackupObjectDisks return error: %v", deleteErr)
			return err
		} else {
			log.WithField("backup", backupName).WithField("duration", utils.HumanizeDuration(time.Since(startTime))).Infof("cleanBackupObjectDisks deleted %d keys", deletedKeys)
		}
	}
	if !skip && (b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "") {
		if err = b.cleanLocalEmbedded(ctx, backup, disks); err != nil {
			log.Warnf("b.cleanLocalEmbedded return error: %v", err)
			return err
		}
	}
	return nil
}

func (b *Backuper) hasObjectDisksLocal(backupList []LocalBackup, backupName string, disks []clickhouse.Disk) bool {
	for _, backup := range backupList {
		if backup.BackupName == backupName && !b.isEmbedded {
			for _, disk := range disks {
				if !disk.IsBackup && (b.isDiskTypeObject(disk.Type) || b.isDiskTypeEncryptedObject(disk, disks)) {
					backupExists, err := os.ReadDir(path.Join(disk.Path, "backup", backup.BackupName))
					if err == nil && len(backupExists) > 0 {
						apexLog.Debugf("hasObjectDisksLocal: found object disk %s", disk.Name)
						return true
					}
				}
			}
		}
	}
	return false
}

func (b *Backuper) cleanLocalEmbedded(ctx context.Context, backup LocalBackup, disks []clickhouse.Disk) error {
	for _, disk := range disks {
		if disk.Name == b.cfg.ClickHouse.EmbeddedBackupDisk && disk.Type != "local" {
			if err := object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, disk.Name); err != nil {
				return err
			}
			backupPath := path.Join(disk.Path, backup.BackupName)
			if err := filepath.Walk(backupPath, func(filePath string, info fs.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() && !strings.HasSuffix(filePath, ".json") && !strings.HasPrefix(filePath, path.Join(backupPath, "access")) {
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
			err = b.cleanEmbeddedAndObjectDiskRemoteIfSameLocalNotPresent(ctx, backup, log)
			if err != nil {
				return err
			}

			if err = bd.RemoveBackupRemote(ctx, backup); err != nil {
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

func (b *Backuper) cleanEmbeddedAndObjectDiskRemoteIfSameLocalNotPresent(ctx context.Context, backup storage.Backup, log *apexLog.Entry) error {
	var skip bool
	var err error
	if skip, err = b.skipIfSameLocalBackupPresent(ctx, backup.BackupName, backup.Tags); err != nil {
		return err
	}
	if !skip {
		if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
			if err = b.cleanRemoteEmbedded(ctx, backup); err != nil {
				log.Warnf("b.cleanRemoteEmbedded return error: %v", err)
				return err
			}
			return nil
		}
		if b.hasObjectDisksRemote(backup) || (b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk == "") {
			startTime := time.Now()
			if deletedKeys, deleteErr := b.cleanBackupObjectDisks(ctx, backup.BackupName); deleteErr != nil {
				log.Warnf("b.cleanBackupObjectDisks return error: %v", deleteErr)
			} else {
				log.WithField("backup", backup.BackupName).WithField("duration", utils.HumanizeDuration(time.Since(startTime))).Infof("cleanBackupObjectDisks deleted %d keys", deletedKeys)
			}
			return nil
		}
	}
	return nil
}

func (b *Backuper) hasObjectDisksRemote(backup storage.Backup) bool {
	for _, diskType := range backup.DiskTypes {
		if b.isDiskTypeObject(diskType) {
			return true
		}
	}
	return false
}

func (b *Backuper) cleanRemoteEmbedded(ctx context.Context, backup storage.Backup) error {
	if err := object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, b.cfg.ClickHouse.EmbeddedBackupDisk); err != nil {
		return err
	}
	return b.dst.Walk(ctx, backup.BackupName+"/", true, func(ctx context.Context, f storage.RemoteFile) error {
		if !strings.HasSuffix(f.Name(), ".json") {
			r, err := b.dst.GetFileReader(ctx, path.Join(backup.BackupName, f.Name()))
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

// cleanBackupObjectDisks - recursive delete <object_disks_path>/<backupName>
func (b *Backuper) cleanBackupObjectDisks(ctx context.Context, backupName string) (uint, error) {
	objectDiskPath, err := b.getObjectDiskPath()
	if err != nil {
		return 0, err
	}
	//walk absolute path, delete relative
	deletedKeys := uint(0)
	walkErr := b.dst.WalkAbsolute(ctx, path.Join(objectDiskPath, backupName), true, func(ctx context.Context, f storage.RemoteFile) error {
		if b.dst.Kind() == "azblob" {
			if f.Size() > 0 || !f.LastModified().IsZero() {
				deletedKeys += 1
				return b.dst.DeleteFileFromObjectDiskBackup(ctx, path.Join(backupName, f.Name()))
			} else {
				return nil
			}
		}
		deletedKeys += 1
		return b.dst.DeleteFileFromObjectDiskBackup(ctx, path.Join(backupName, f.Name()))
	})
	return deletedKeys, walkErr
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

func (b *Backuper) cleanPartialRequiredBackup(ctx context.Context, disks []clickhouse.Disk, currentBackupName string) error {
	if localBackups, _, err := b.GetLocalBackups(ctx, disks); err == nil {
		for _, localBackup := range localBackups {
			if localBackup.BackupName != currentBackupName && localBackup.DataSize+localBackup.CompressedSize+localBackup.MetadataSize+localBackup.RBACSize == 0 {
				if err = b.RemoveBackupLocal(ctx, localBackup.BackupName, disks); err != nil {
					return fmt.Errorf("CleanPartialRequiredBackups %s -> RemoveBackupLocal cleaning error: %v", localBackup.BackupName, err)
				} else {
					b.log.Infof("CleanPartialRequiredBackups %s deleted", localBackup.BackupName)
				}
			}
		}
	} else {
		return fmt.Errorf("CleanPartialRequiredBackups -> GetLocalBackups cleaning error: %v", err)
	}
	return nil
}
