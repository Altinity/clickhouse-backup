package backup

import "github.com/AlexAkulov/clickhouse-backup/config"

func RestoreFromRemote(cfg *config.Config, backupName string, tablePattern string, schemaOnly bool, dataOnly bool, dropTable bool) error {
	if err := Download(cfg, backupName, tablePattern, schemaOnly); err != nil {
		return err
	}
	return Restore(cfg, backupName, tablePattern, schemaOnly, dataOnly, dropTable)
}
