package backup

import (
	"fmt"
	"os"
	"path"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/new_storage"
)

//
func RemoveOldBackupsLocal(cfg *config.Config) error {
	if cfg.General.BackupsToKeepLocal < 1 {
		return nil
	}
	backupList, err := GetLocalBackups(cfg)
	if err != nil {
		return err
	}
	backupsToDelete := new_storage.GetBackupsToDelete(backupList, cfg.General.BackupsToKeepLocal)
	for _, backup := range backupsToDelete {
		if err := RemoveBackupLocal(cfg, backup.BackupName); err != nil {
			return err
		}
	}
	return nil
}

func RemoveBackupLocal(cfg *config.Config, backupName string) error {
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
				err := os.RemoveAll(path.Join(disk.Path, "backup", backupName))
				if err != nil {
					return err
				}
			}
			return nil
		}
	}
	return fmt.Errorf("'%s' is not found on local storage", backupName)
}

func RemoveBackupRemote(cfg *config.Config, backupName string) error {
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
			if backup.Legacy {
				archiveName := fmt.Sprintf("%s.%s", backup.BackupName, backup.FileExtension)
				return bd.DeleteFile(archiveName)
			}
			return bd.RemoveBackup(backupName)
		}
	}
	return fmt.Errorf("'%s' is not found on remote storage", backupName)
}
