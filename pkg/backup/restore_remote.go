package backup

import (
	"errors"
	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
)

func (b *Backuper) RestoreFromRemote(backupName, tablePattern string, databaseMapping, tableMapping, partitions, skipProjections []string, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, resume, schemaAsAttach, replicatedCopyToDetached bool, version string, commandId int) error {
	// don't need to create pid separately because we combine Download+Restore
	defer pidlock.RemovePidFile(backupName)
	if err := b.Download(backupName, tablePattern, partitions, schemaOnly, rbacOnly, configsOnly, resume, version, commandId); err != nil {
		// https://github.com/Altinity/clickhouse-backup/issues/625
		if !errors.Is(err, ErrBackupIsAlreadyExists) {
			return err
		}
	}
	pidlock.RemovePidFile(backupName)
	return b.Restore(backupName, tablePattern, databaseMapping, tableMapping, partitions, skipProjections, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, resume, schemaAsAttach, replicatedCopyToDetached, version, commandId)
}
