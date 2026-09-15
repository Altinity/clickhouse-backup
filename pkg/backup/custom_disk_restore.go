package backup

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// customDiskAlias binds a disk name recorded in the backup to the disk a `SETTINGS disk = disk(...)` table
// actually lives on after schema restore, https://github.com/Altinity/clickhouse-backup/issues/943.
// ClickHouse names such a disk `__tmp_internal_<hash of the declaration AST>`, the hash changes between
// server versions, so the name in the backup and the name on the restore target are often different.
type customDiskAlias struct {
	// Disk - entry appended to the live disks list, Path is the path of the target disk, so that
	// clickhouse.GetDisksByPaths maps the table data path to the backup disk name
	Disk clickhouse.Disk
	// LocalPath - directory which holds `backup/<name>/shadow/...` of this disk, the path recorded in the
	// backup, create and download both put the data there
	LocalPath string
}

// tableDiskNames - disk names referenced by the parts and files of a backed up table, sorted for determinism
func tableDiskNames(t *metadata.TableMetadata) []string {
	names := make([]string, 0, len(t.Parts)+len(t.Files))
	seen := make(map[string]struct{}, len(t.Parts)+len(t.Files))
	for diskName := range t.Parts {
		if _, exists := seen[diskName]; !exists {
			seen[diskName] = struct{}{}
			names = append(names, diskName)
		}
	}
	for diskName := range t.Files {
		if _, exists := seen[diskName]; !exists {
			seen[diskName] = struct{}{}
			names = append(names, diskName)
		}
	}
	sort.Strings(names)
	return names
}

// customDiskAliases - aliases for every backup disk name which is not the disk the table lives on now,
// all of them point to target. A backup disk name which is still registered on the server under another path
// (the disk of a dropped table stays registered until restart) is aliased too, the restored table doesn't
// live there.
func customDiskAliases(backupDiskNames []string, backupDiskPaths map[string]string, liveDisks []clickhouse.Disk, target clickhouse.Disk) []customDiskAlias {
	targetNames := map[string]struct{}{target.Name: {}}
	liveNames := make(map[string]struct{}, len(liveDisks))
	for _, d := range liveDisks {
		liveNames[d.Name] = struct{}{}
		if d.Path == target.Path {
			targetNames[d.Name] = struct{}{}
		}
	}
	aliases := make([]customDiskAlias, 0, len(backupDiskNames))
	for _, backupDiskName := range backupDiskNames {
		if backupDiskName == "" {
			continue
		}
		if _, isTarget := targetNames[backupDiskName]; isTarget {
			continue
		}
		// a regular disk (`default`, a config disk) which exists on the server is left alone, only a generated
		// name registered under another path is a leftover of a dropped table and is redirected to target
		if _, isLive := liveNames[backupDiskName]; isLive && !strings.HasPrefix(backupDiskName, clickhouse.TmpInternalDiskPrefix) {
			continue
		}
		localPath := backupDiskPaths[backupDiskName]
		if localPath == "" {
			localPath = target.Path
		}
		aliases = append(aliases, customDiskAlias{
			Disk: clickhouse.Disk{
				Name:            backupDiskName,
				Path:            target.Path,
				Type:            target.Type,
				MetadataType:    target.MetadataType,
				FreeSpace:       target.FreeSpace,
				TotalSpace:      target.TotalSpace,
				StoragePolicies: target.StoragePolicies,
				RawPath:         target.RawPath,
			},
			LocalPath: localPath,
		})
	}
	return aliases
}

// registerCustomDiskPaths - a `SETTINGS disk = disk(...)` disk doesn't exist locally until the schema is
// restored, so the download has no live path for it, take the path recorded in the backup.
// The table DDL is replayed verbatim during restore, so clickhouse-server registers the disk with the same
// `metadata_path` and the downloaded data is already where the restore expects it.
// The disk root is created here with the clickhouse-server owner, otherwise the download creates it as the
// clickhouse-backup user and clickhouse-server can't register the disk during schema restore.
func (b *Backuper) registerCustomDiskPaths(t *metadata.TableMetadata, disks []clickhouse.Disk, remoteBackup storage.Backup) error {
	if b.DiskToPathMap == nil {
		return nil
	}
	for _, diskName := range tableDiskNames(t) {
		if _, diskExists := b.DiskToPathMap[diskName]; diskExists {
			continue
		}
		backupDiskPath := remoteBackup.Disks[diskName]
		if backupDiskPath == "" {
			log.Warn().Msgf("table `%s`.`%s` declares `disk = disk(...)`, but disk %s has no path in %s/metadata.json", t.Database, t.Table, diskName, remoteBackup.BackupName)
			continue
		}
		if err := filesystemhelper.MkdirAll(backupDiskPath, b.ch, disks); err != nil {
			return errors.Wrapf(err, "registerCustomDiskPaths: %s", backupDiskPath)
		}
		b.DiskToPathMap[diskName] = backupDiskPath
		log.Debug().Msgf("table `%s`.`%s` declares `disk = disk(...)`, download %s to %s", t.Database, t.Table, diskName, backupDiskPath)
	}
	return nil
}

// customDiskRawPath - system.disks.path of diskName as clickhouse-server reports it. GetDisks groups the rows
// which share one path (a `cache` wrapper and the disk it wraps) into a single entry named after the wrapped
// disk, so the outer name taken from system.tables.storage_policy may be absent from the GetDisks result.
func (b *Backuper) customDiskRawPath(ctx context.Context, diskName string) (string, error) {
	var diskPath string
	if err := b.ch.SelectSingleRow(ctx, &diskPath, "SELECT path FROM system.disks WHERE name=?", diskName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", errors.Wrapf(err, "customDiskRawPath: %s", diskName)
	}
	return diskPath, nil
}

// resolveCustomDiskAliases - make the disk names recorded in the backup usable for tables declared with
// `SETTINGS disk = disk(...)`, https://github.com/Altinity/clickhouse-backup/issues/943.
// Such a disk is registered by clickhouse-server only when the table is created, so it appears after the
// schema restore and usually under a different generated name than the one in the backup. For every table
// the actual disk is resolved via system.tables.storage_policy and the missing backup disk names are added
// to disks/diskMap/diskTypes as aliases of it, object storage credentials and connection are aliased too,
// so downloadObjectDiskParts keeps looking everything up by the backup disk name.
func (b *Backuper) resolveCustomDiskAliases(ctx context.Context, tablesForRestore ListOfTables, backupDiskPaths map[string]string, disks []clickhouse.Disk, diskMap, diskTypes map[string]string) ([]clickhouse.Disk, error) {
	customTables := make(ListOfTables, 0, len(tablesForRestore))
	for _, t := range tablesForRestore {
		if t != nil && clickhouse.HasCustomDisk(t.Query) {
			customTables = append(customTables, t)
		}
	}
	if len(customTables) == 0 {
		return disks, nil
	}
	liveDisks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		return nil, errors.Wrap(err, "resolveCustomDiskAliases: ch.GetDisks")
	}
	// disks were listed before the schema restore, the disks registered together with the restored tables
	// are missing there, so restoreDataRegular can't map the table data paths to them
	knownDisks := make(map[string]struct{}, len(disks))
	for _, d := range disks {
		knownDisks[d.Name] = struct{}{}
	}
	for _, d := range liveDisks {
		if _, exists := knownDisks[d.Name]; exists {
			continue
		}
		disks = append(disks, d)
		if _, exists := diskMap[d.Name]; !exists {
			diskMap[d.Name] = d.Path
		}
		diskTypes[d.Name] = d.Type
		if b.DiskToPathMap != nil {
			if _, exists := b.DiskToPathMap[d.Name]; !exists {
				b.DiskToPathMap[d.Name] = d.Path
			}
		}
		log.Debug().Msgf("resolveCustomDiskAliases: disk %s (%s) registered after schema restore", d.Name, d.Path)
	}
	for _, t := range customTables {
		outerDiskName, policyErr := b.ch.GetTableDiskNameFromStoragePolicy(ctx, t.Database, t.Table)
		if policyErr != nil {
			return nil, errors.Wrap(policyErr, "resolveCustomDiskAliases")
		}
		if outerDiskName == "" {
			log.Warn().Msgf("table `%s`.`%s` declares `disk = disk(...)` but doesn't use a `%s` storage policy, custom disk aliases skipped", t.Database, t.Table, clickhouse.TmpStoragePolicyPrefix)
			continue
		}
		target := b.findDiskByName(liveDisks, outerDiskName)
		if target == nil {
			outerDiskPath, pathErr := b.customDiskRawPath(ctx, outerDiskName)
			if pathErr != nil {
				return nil, pathErr
			}
			for i := range liveDisks {
				if liveDisks[i].Path == outerDiskPath {
					target = &liveDisks[i]
					break
				}
			}
		}
		if target == nil {
			return nil, errors.Errorf("resolveCustomDiskAliases: disk %s of table `%s`.`%s` not found in system.disks", outerDiskName, t.Database, t.Table)
		}
		aliases := customDiskAliases(tableDiskNames(t), backupDiskPaths, liveDisks, *target)
		if len(aliases) == 0 {
			continue
		}
		if b.isDiskTypeObject(target.Type) || b.isDiskTypeEncryptedObject(*target, liveDisks) {
			if err = object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, target.Name); err != nil {
				return nil, errors.Wrapf(err, "resolveCustomDiskAliases: InitCredentialsAndConnections %s", target.Name)
			}
		}
		for _, alias := range aliases {
			disks = append(disks, alias.Disk)
			diskMap[alias.Disk.Name] = alias.LocalPath
			diskTypes[alias.Disk.Name] = alias.Disk.Type
			if b.DiskToPathMap != nil {
				b.DiskToPathMap[alias.Disk.Name] = alias.LocalPath
			}
			aliasObjectDiskRegistries(target.Name, alias.Disk)
			log.Info().Msgf("table `%s`.`%s` disk %s from backup resolved to `disk = disk(...)` %s (%s)", t.Database, t.Table, alias.Disk.Name, target.Name, target.Path)
		}
	}
	return disks, nil
}

// aliasObjectDiskRegistries - copy credentials, connection and system disk of targetDiskName to the backup
// disk name, object_disk lookups during restore are keyed by the disk name stored in the backup
func aliasObjectDiskRegistries(targetDiskName string, alias clickhouse.Disk) {
	if credentials, exists := object_disk.DisksCredentials.Load(targetDiskName); exists {
		object_disk.DisksCredentials.Store(alias.Name, credentials)
	}
	if connection, exists := object_disk.DisksConnections.Load(targetDiskName); exists {
		object_disk.DisksConnections.Store(alias.Name, connection)
	}
	object_disk.SystemDisks.Store(alias.Name, alias)
}
