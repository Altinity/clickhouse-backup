package backup

import "fmt"

func (b *Backuper) CreateToRemote(backupName, diffFrom, diffFromRemote, tablePattern string, partitions []string, schemaOnly, rbac, backupConfig bool, version string) error {
	if backupName == "" {
		backupName = NewBackupName()
	}
	if err := CreateBackup(b.cfg, backupName, tablePattern, partitions, schemaOnly, rbac, backupConfig, version); err != nil {
		return err
	}
	if err := b.Upload(backupName, diffFrom, diffFromRemote, tablePattern, partitions, schemaOnly); err != nil {
		return err
	}
	if err := RemoveOldBackupsLocal(b.cfg, false, nil); err != nil {
		return fmt.Errorf("can't remove old local backups: %v", err)
	}
	return nil
}
