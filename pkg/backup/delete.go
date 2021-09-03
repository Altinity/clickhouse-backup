package backup

import (
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/utils"
	"os"
	"path"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/new_storage"

	apexLog "github.com/apex/log"
)

//
func RemoveOldBackupsLocal(cfg *config.Config, keepLastBackup bool) error {
	keep := cfg.General.BackupsToKeepLocal
	if keep == 0 {
		return nil
	}
	if keepLastBackup && keep < 0 {
		keep = 1
	}
	backupList, err := GetLocalBackups(cfg)
	if err != nil {
		return err
	}
	backupsToDelete := GetBackupsToDelete(backupList, keep)
	for _, backup := range backupsToDelete {
		if err := RemoveBackupLocal(cfg, backup.BackupName); err != nil {
			return err
		}
	}
	return nil
}

func RemoveBackupLocal(cfg *config.Config, backupName string) error {
	start := time.Now()
	backupList, err := GetLocalBackups(cfg)
	if err != nil {
		return err
	}
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			for _, disk := range disks {
				apexLog.WithField("path", disk.Path).Debugf("remove '%s'", backupName)
				err := os.RemoveAll(path.Join(disk.Path, "backup", backupName))
				if err != nil {
					return err
				}
			}
			apexLog.WithField("operation", "delete").
				WithField("location", "local").
				WithField("backup", backupName).
				WithField("duration", utils.HumanizeDuration(time.Since(start))).
				Info("done")
			return nil
		}
	}
	return fmt.Errorf("'%s' is not found on local storage", backupName)
}

func RemoveBackupRemote(cfg *config.Config, backupName string) error {
	start := time.Now()
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("RemoveBackupRemote aborted: RemoteStorage set to \"none\"")
		return nil
	}

	bd, err := new_storage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}
	err = bd.Connect()
	if err != nil {
		return fmt.Errorf("can't connect to remote storage: %v", err)
	}
	backupList, err := bd.BackupList()
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			if err := bd.RemoveBackup(backup); err != nil {
				return err
			}
			apexLog.WithFields(apexLog.Fields{
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
