package backup

import (
	"context"
	"fmt"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/partition"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/pkg/errors"
)

// isTablePatternMatchAll reports whether the --tables pattern selects everything, so the whole backup
// numbers from metadata.json are exact and don't need to be estimated from per-part sizes
func isTablePatternMatchAll(tablePattern string) bool {
	return tablePattern == "" || tablePattern == "*" || tablePattern == "*.*"
}

// collectRemoteTablesForDryRun reads table metadata straight from remote storage and applies the same
// skip and --partitions filters as downloadTableMetadata, without writing anything to the local disk,
// so `download --dry-run` leaves no traces
func (b *Backuper) collectRemoteTablesForDryRun(ctx context.Context, backupName string, tablesForDownload []metadata.TableTitle, partitions []string) (ListOfTables, uint64, error) {
	tables := make(ListOfTables, 0, len(tablesForDownload))
	metadataSize := uint64(0)
	for _, tableTitle := range tablesForDownload {
		tableMetadata, remoteSize, err := b.readRemoteTableMetadata(ctx, backupName, tableTitle)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "readRemoteTableMetadata %s.%s", tableTitle.Database, tableTitle.Table)
		}
		if b.shouldSkipByTableEngine(*tableMetadata) || b.shouldSkipByTableName(fmt.Sprintf("%s.%s", tableMetadata.Database, tableMetadata.Table)) {
			continue
		}
		partitionsIdMap, _ := partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, nil, ListOfTables{tableMetadata}, partitions)
		filterPartsAndFilesByPartitionsFilter(*tableMetadata, partitionsIdMap[metadata.TableTitle{Database: tableMetadata.Database, Table: tableMetadata.Table}])
		// the remote json is what travels over the network, the locally saved copy could be smaller
		// with --schema, so this is an upper bound of the real metadata download size
		metadataSize += uint64(remoteSize)
		tables = append(tables, tableMetadata)
	}
	// a materialized view without a `TO db.table` clause keeps its data in a hidden .inner table which the
	// real download fetches as well, see downloadMissedInnerTablesMetadata
	downloaded := make(map[metadata.TableTitle]struct{}, len(tables))
	for _, t := range tables {
		downloaded[metadata.TableTitle{Database: t.Database, Table: t.Table}] = struct{}{}
	}
	for i := 0; i < len(tables); i++ {
		if !strings.HasPrefix(tables[i].Query, "ATTACH MATERIALIZED") && !strings.HasPrefix(tables[i].Query, "CREATE MATERIALIZED") {
			continue
		}
		if strings.Contains(tables[i].Query, " TO ") && !strings.Contains(tables[i].Query, " TO INNER UUID") {
			continue
		}
		innerTableTitle := metadata.TableTitle{Database: tables[i].Database, Table: fmt.Sprintf(".inner.%s", tables[i].Table)}
		if matches := uuidRE.FindStringSubmatch(tables[i].Query); len(matches) > 0 {
			innerTableTitle.Table = fmt.Sprintf(".inner_id.%s", matches[1])
		}
		if _, exists := downloaded[innerTableTitle]; exists {
			continue
		}
		innerTableMetadata, remoteSize, err := b.readRemoteTableMetadata(ctx, backupName, innerTableTitle)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "readRemoteTableMetadata %s.%s", innerTableTitle.Database, innerTableTitle.Table)
		}
		partitionsIdMap, _ := partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, nil, ListOfTables{innerTableMetadata}, partitions)
		filterPartsAndFilesByPartitionsFilter(*innerTableMetadata, partitionsIdMap[innerTableTitle])
		metadataSize += uint64(remoteSize)
		downloaded[innerTableTitle] = struct{}{}
		tables = append(tables, innerTableMetadata)
	}
	return tables, metadataSize, nil
}

// dryRunDownload reports how many tables and bytes `download` would transfer, without creating the local
// backup directory, without writing table metadata and without resumable state,
// https://github.com/Altinity/clickhouse-backup/issues/1012
func (b *Backuper) dryRunDownload(ctx context.Context, remoteBackup storage.Backup, disks []clickhouse.Disk, tablesForDownload []metadata.TableTitle, tablePattern string, partitions []string, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, hardlinkExistsFiles bool) error {
	report := &DryRunReport{Command: "download", BackupName: remoteBackup.BackupName}
	// same conditions as the real download below
	if rbacOnly || rbacOnly == configsOnly == namedCollectionsOnly == false {
		report.RBACSize = remoteBackup.RBACSize
	}
	if configsOnly || rbacOnly == configsOnly == namedCollectionsOnly == false {
		report.ConfigSize = remoteBackup.ConfigSize
	}
	if namedCollectionsOnly || rbacOnly == configsOnly == namedCollectionsOnly == false {
		report.NamedCollectionsSize = remoteBackup.NamedCollectionsSize
	}
	doDownloadData := !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly
	// an embedded backup keeps all data inside a single .backup, there are no per-part sizes to filter,
	// so report the exact whole backup numbers
	if strings.Contains(remoteBackup.Tags, "embedded") {
		report.TableCount = len(tablesForDownload)
		if doDownloadData || schemaOnly {
			report.MetadataSize = remoteBackup.MetadataSize
		}
		if doDownloadData {
			report.DataSize = remoteBackup.DataSize
			report.CompressedSize = remoteBackup.CompressedSize
		}
		report.TotalSize = remoteBackup.GetFullSize()
		b.setDryRunResult(report)
		return nil
	}
	useWholeBackupSizes := false
	if doDownloadData || schemaOnly {
		tables, metadataSize, err := b.collectRemoteTablesForDryRun(ctx, remoteBackup.BackupName, tablesForDownload, partitions)
		if err != nil {
			return errors.Wrap(err, "collectRemoteTablesForDryRun")
		}
		report.TableCount = len(tables)
		report.MetadataSize = metadataSize
		if doDownloadData {
			b.filterPartsAndFilesByDisk(tables, disks)
			estimate := b.computeDownloadSizeEstimate(ctx, remoteBackup, tables, disks, hardlinkExistsFiles)
			report.PartsCount = estimate.PartsCount
			report.UnknownSizeParts = estimate.UnknownSizeParts
			report.ObjectDiskSize = estimate.ObjectDiskSize
			report.DataSize = estimate.RequiredSize - estimate.ObjectDiskSize
			// without --tables, --partitions and hardlink reuse the whole backup is downloaded, and
			// metadata.json carries exact numbers including the compressed archive size, which can't be
			// derived from per-part sizes
			useWholeBackupSizes = !hardlinkExistsFiles && isTablePatternMatchAll(tablePattern) && len(partitions) == 0
			if useWholeBackupSizes {
				report.DataSize = remoteBackup.DataSize
				report.CompressedSize = remoteBackup.CompressedSize
				report.ObjectDiskSize = remoteBackup.ObjectDiskSize
				report.MetadataSize = remoteBackup.MetadataSize
			}
		}
	}
	if useWholeBackupSizes {
		report.TotalSize = remoteBackup.GetFullSize()
	} else {
		report.TotalSize = report.DataSize + report.ObjectDiskSize + report.MetadataSize + report.RBACSize + report.ConfigSize + report.NamedCollectionsSize
	}
	b.setDryRunResult(report)
	return nil
}
