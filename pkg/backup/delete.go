package backup

import (
	"context"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/custom"
	"github.com/Altinity/clickhouse-backup/pkg/status"
	"github.com/Altinity/clickhouse-backup/pkg/utils"
	"github.com/pkg/errors"
	"os"
	"path"
	"time"

	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/storage"

	apexLog "github.com/apex/log"
)

// Clean - removed all data in shadow folder
func (b *Backuper) Clean(ctx context.Context) error {
	log := b.log.WithField("logger", "Clean")
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	disks, err := b.ch.GetDisks(ctx)
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
	if items, err := os.ReadDir(dirName); err != nil {
		return err
	} else {
		for _, item := range items {
			if err = os.RemoveAll(path.Join(dirName, item.Name())); err != nil {
				return err
			}
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
	if keepLastBackup && keep < 0 {
		keep = 1
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
		disks, err = b.ch.GetDisks(ctx)
		if err != nil {
			return err
		}
	}
	backupList, disks, err := b.GetLocalBackups(ctx, disks)
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			for _, disk := range disks {
				backupPath := path.Join(disk.Path, "backup", backupName)
				if disk.IsBackup {
					backupPath = path.Join(disk.Path, backupName)
				}
				log.Debugf("remove '%s'", backupPath)
				err = os.RemoveAll(backupPath)
				if err != nil {
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

	backupList, err := bd.BackupList(ctx, true, backupName)
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			if err := bd.RemoveBackup(ctx, backup); err != nil {
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
