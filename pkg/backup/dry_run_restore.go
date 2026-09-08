package backup

import (
	"context"
	"path"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/pkg/errors"
)

// dryRunRestore reports how many tables and bytes `restore` would copy from an existing local backup,
// without creating or dropping databases, without restoring RBAC, configs and named collections, without
// restarting clickhouse-server and without downloading object disk data,
// https://github.com/Altinity/clickhouse-backup/issues/1012
func (b *Backuper) dryRunRestore(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, disks []clickhouse.Disk, tablePattern string, partitions []string, schemaOnly, dataOnly, dropExists, rbacOnly, configsOnly, namedCollectionsOnly, restoreRBAC, restoreConfigs, restoreNamedCollections, skipEmptyTables bool) error {
	report := &DryRunReport{Command: "restore", BackupName: backupName}
	if rbacOnly || restoreRBAC {
		report.RBACSize = backupMetadata.RBACSize
	}
	if configsOnly || restoreConfigs {
		report.ConfigSize = backupMetadata.ConfigSize
	}
	if namedCollectionsOnly || restoreNamedCollections {
		report.NamedCollectionsSize = backupMetadata.NamedCollectionsSize
	}
	setTotalAndReport := func() {
		report.TotalSize = report.DataSize + report.ObjectDiskSize + report.RBACSize + report.ConfigSize + report.NamedCollectionsSize
		b.setDryRunResult(report)
	}
	// the real restore returns right after RBAC, configs and named collections with these flags
	if rbacOnly || configsOnly || namedCollectionsOnly {
		setTotalAndReport()
		return nil
	}
	if len(backupMetadata.Tables) == 0 {
		setTotalAndReport()
		return nil
	}
	if tablePattern == "" {
		tablePattern = "*"
	}
	metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
	if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		metadataPath = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata")
	}
	tablesForRestore, partitionsNames, err := b.getTablesForRestoreLocal(ctx, backupName, metadataPath, tablePattern, dropExists, partitions)
	if err != nil {
		return errors.Wrap(err, "getTablesForRestoreLocal")
	}
	if skipEmptyTables {
		tablesForRestore = b.filterEmptyTables(tablesForRestore)
	}
	if len(partitions) > 0 {
		tablesForRestore = b.filterTablesWithoutPartitions(tablesForRestore, partitionsNames)
	}
	report.TableCount = len(tablesForRestore)
	// rbacOnly, configsOnly and namedCollectionsOnly are always false here, the early return above handles them
	doRestoreData := !schemaOnly || dataOnly

	if doRestoreData && b.isEmbedded {
		// embedded RESTORE is executed by clickhouse-server from a single .backup, per table sizes are unavailable
		report.DataSize = backupMetadata.DataSize
	} else if doRestoreData {
		diskTypeByName := make(map[string]string, len(disks))
		for _, d := range disks {
			diskTypeByName[d.Name] = d.Type
		}
		for _, t := range tablesForRestore {
			if t == nil || t.MetadataOnly {
				continue
			}
			for diskName, parts := range t.Parts {
				if b.shouldDiskNameSkipByNameOrType(diskName, disks) {
					continue
				}
				diskType, exists := backupMetadata.DiskTypes[diskName]
				if !exists {
					diskType = diskTypeByName[diskName]
				}
				isObjectDisk := b.isDiskTypeObject(diskType) || backupMetadata.IsPlainDisk(diskName)
				diskSize := uint64(0)
				unknownSizeParts := 0
				for _, part := range parts {
					report.PartsCount++
					if part.Size == 0 {
						unknownSizeParts++
						continue
					}
					diskSize += part.Size
				}
				if isObjectDisk {
					report.ObjectDiskSize += diskSize
					report.UnknownSizeParts += unknownSizeParts
					continue
				}
				// backups created before per-part `size` (issue #1268) carry the size only per disk in table
				// metadata, it covers exactly the parts stored in the backup, so it is usable as long as
				// --partitions doesn't narrow them down
				if diskSize == 0 && len(partitions) == 0 && t.Size[diskName] > 0 {
					diskSize = uint64(t.Size[diskName])
					unknownSizeParts = 0
				}
				report.DataSize += diskSize
				report.UnknownSizeParts += unknownSizeParts
			}
		}
	}
	setTotalAndReport()
	return nil
}
