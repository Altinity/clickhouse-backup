package backup

import "github.com/AlexAkulov/clickhouse-backup/config"

func CreateToRemote(cfg *config.Config, backupName, tablePattern, diffFrom string, schemaOnly bool, version string) error {
	if backupName == "" {
		backupName = NewBackupName()
	}
	if err := CreateBackup(cfg, backupName, tablePattern, schemaOnly, version); err != nil {
		return err
	}
	return Upload(cfg, backupName, tablePattern, diffFrom, schemaOnly)
}
