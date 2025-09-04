package backup

import (
	"errors"

	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
	"github.com/rs/zerolog/log"
)

func (b *Backuper) RestoreFromRemote(backupName, tablePattern string, databaseMapping, tableMapping, partitions, skipProjections []string, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, replicatedCopyToDetached, hardlinkExistsFiles, dropIfSchemaChanged bool, version string, commandId int) error {
	// don't need to create pid separately because we combine Download+Restore
	defer pidlock.RemovePidFile(backupName)

	// Check if in-place restore is enabled for remote restore and we're doing data-only restore
	log.Debug().
		Bool("restore_in_place_config", b.cfg.General.RestoreInPlace).
		Bool("data_only", dataOnly).
		Bool("schema_only", schemaOnly).
		Bool("rbac_only", rbacOnly).
		Bool("configs_only", configsOnly).
		Bool("named_collections_only", namedCollectionsOnly).
		Bool("drop_exists", dropExists).
		Msg("RestoreFromRemote: Checking in-place restore routing conditions")

	if b.cfg.General.RestoreInPlace && !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly && !dropExists {
		log.Info().Msg("RestoreFromRemote: Triggering in-place restore")
		return b.RestoreInPlaceFromRemote(backupName, tablePattern, commandId, dropIfSchemaChanged)
	}

	log.Info().Msg("RestoreFromRemote: Using standard download+restore process")

	if err := b.Download(backupName, tablePattern, partitions, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, resume, hardlinkExistsFiles, version, commandId); err != nil {
		// https://github.com/Altinity/clickhouse-backup/issues/625
		if !errors.Is(err, ErrBackupIsAlreadyExists) {
			return err
		}
	}
	pidlock.RemovePidFile(backupName)
	return b.Restore(backupName, tablePattern, databaseMapping, tableMapping, partitions, skipProjections, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, replicatedCopyToDetached, version, commandId)
}
