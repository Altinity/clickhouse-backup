package backup

import (
	"errors"

	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
	pkgerrors "github.com/pkg/errors"
)

func (b *Backuper) RestoreFromRemote(backupName, tablePattern string, databaseMapping, tableMapping, partitions, skipProjections []string, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, replicatedCopyToDetached, skipEmptyTables, hardlinkExistsFiles bool, version string, commandId int) error {
	// don't need to create pid separately because we combine Download+Restore
	defer pidlock.RemovePidFile(backupName)
	if err := b.Download(backupName, tablePattern, partitions, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, resume, hardlinkExistsFiles, version, commandId); err != nil {
		// https://github.com/Altinity/clickhouse-backup/issues/625
		if !errors.Is(err, ErrBackupIsAlreadyExists) {
			return pkgerrors.Wrap(err, "RestoreFromRemote Download")
		}
	}
	pidlock.RemovePidFile(backupName)
	// the restored volume equals the volume the download would bring and nothing is materialized locally,
	// so the download report is re-labeled instead of being estimated twice,
	// https://github.com/Altinity/clickhouse-backup/issues/1012
	if b.DryRun && b.DryRunResult != nil {
		b.DryRunResult.Command = "restore_remote"
		b.setDryRunResult(b.DryRunResult)
		return nil
	}
	if err := b.Restore(backupName, tablePattern, databaseMapping, tableMapping, partitions, skipProjections, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, replicatedCopyToDetached, skipEmptyTables, version, commandId); err != nil {
		return err
	}
	// the backup already existed locally, so the download produced no report and the local restore
	// dry-run above gave the exact numbers
	if b.DryRun && b.DryRunResult != nil {
		b.DryRunResult.Command = "restore_remote"
		b.setDryRunResult(b.DryRunResult)
	}
	return nil
}
