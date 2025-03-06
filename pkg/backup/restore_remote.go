package backup

import "errors"

func (b *Backuper) RestoreFromRemote(backupName, tablePattern string, databaseMapping, tableMapping, partitions, skipProjections []string, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, resume, schemaAsAttach bool, version string, commandId int) error {
	if err := b.Download(backupName, tablePattern, partitions, schemaOnly, rbacOnly, configsOnly, resume, version, commandId); err != nil {
		// https://github.com/Altinity/clickhouse-backup/issues/625
		if !errors.Is(err, ErrBackupIsAlreadyExists) {
			return err
		}
	}
	return b.Restore(backupName, tablePattern, databaseMapping, tableMapping, partitions, skipProjections, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, resume, schemaAsAttach, version, commandId)
}
