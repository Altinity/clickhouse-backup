package backup

import (
	"context"
	"fmt"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/pkg/errors"
)

// partsSizeRow - `system.parts` sizes aggregated per table and disk, used only for dry-run estimation
type partsSizeRow struct {
	Database    string `ch:"database"`
	Table       string `ch:"table"`
	DiskName    string `ch:"disk_name"`
	PartitionID string `ch:"partition_id"`
	PartsCount  uint64 `ch:"parts_count"`
	TotalBytes  uint64 `ch:"total_bytes"`
	Bytes1d     uint64 `ch:"bytes_1d"`
	Bytes7d     uint64 `ch:"bytes_7d"`
	Bytes30d    uint64 `ch:"bytes_30d"`
}

// buildCreateDryRunReport estimates how many tables and bytes `create` would back up, without any side effect,
// sizes come from `system.parts` because the backup doesn't exist yet. https://github.com/Altinity/clickhouse-backup/issues/1012
func (b *Backuper) buildCreateDryRunReport(ctx context.Context, backupName string, tables []clickhouse.Table, disks []clickhouse.Disk, partitionsIdMap map[metadata.TableTitle]common.EmptyMap, doBackupData, rbacOnly, configsOnly, namedCollectionsOnly bool) (*DryRunReport, error) {
	report := &DryRunReport{Command: "create", BackupName: backupName}
	// rbac-only/configs-only/named-collections-only copy no tables at all
	if rbacOnly || configsOnly || namedCollectionsOnly {
		return report, nil
	}
	// --schema still processes every table schema, but copies no data
	report.TableCount = b.CalculateNonSkipTables(tables)
	if !doBackupData {
		return report, nil
	}

	backupTables := make(map[metadata.TableTitle]bool, len(tables))
	partitionsFilterActive := false
	for _, table := range tables {
		if table.Skip {
			continue
		}
		title := metadata.TableTitle{Database: table.Database, Table: table.Name}
		backupTables[title] = true
		if len(partitionsIdMap[title]) > 0 {
			partitionsFilterActive = true
		}
	}

	version, err := b.ch.GetVersion(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "buildCreateDryRunReport get version")
	}
	// system.parts.disk_name appeared in 19.15 with multi-disk support, older servers always have a single `default` disk
	diskNameExpr, diskNameGroupBy := "disk_name", ", disk_name"
	if version < 19015000 {
		diskNameExpr, diskNameGroupBy = "'default' AS disk_name", ""
	}
	// group by partition_id only when a --partitions filter is active, to keep the result set small otherwise
	partitionIdExpr, groupBy := "'' AS partition_id", "database, table"+diskNameGroupBy
	if partitionsFilterActive {
		partitionIdExpr, groupBy = "partition_id", groupBy+", partition_id"
	}
	query := fmt.Sprintf(
		"SELECT database, table, "+diskNameExpr+", %s, count() AS parts_count, sum(bytes_on_disk) AS total_bytes, "+
			"sumIf(bytes_on_disk, modification_time >= now() - INTERVAL 1 DAY) AS bytes_1d, "+
			"sumIf(bytes_on_disk, modification_time >= now() - INTERVAL 7 DAY) AS bytes_7d, "+
			"sumIf(bytes_on_disk, modification_time >= now() - INTERVAL 30 DAY) AS bytes_30d "+
			"FROM `system`.`parts` WHERE active GROUP BY %s SETTINGS empty_result_for_aggregation_by_empty_set=0",
		partitionIdExpr, groupBy,
	)
	var rows []partsSizeRow
	if err := b.ch.SelectContext(ctx, &rows, query); err != nil {
		return nil, errors.Wrap(err, "buildCreateDryRunReport select system.parts")
	}

	isObjectDisk := make(map[string]bool, len(disks))
	for _, disk := range disks {
		isObjectDisk[disk.Name] = b.isDiskTypeObject(disk.Type) || b.isDiskTypeEncryptedObject(disk, disks) || b.isDiskPlain(disk)
	}
	for _, row := range rows {
		title := metadata.TableTitle{Database: row.Database, Table: row.Table}
		if !backupTables[title] {
			continue
		}
		if partitionsFilter := partitionsIdMap[title]; len(partitionsFilter) != 0 && !filesystemhelper.IsPartInPartition(row.PartitionID, partitionsFilter) {
			continue
		}
		report.PartsCount += int(row.PartsCount)
		// object disk data is copied to the remote storage, not hardlinked, so it never contributes to the forecast
		if isObjectDisk[row.DiskName] {
			report.ObjectDiskSize += row.TotalBytes
			continue
		}
		report.DataSize += row.TotalBytes
		// hardlinks cost 0 bytes right after create, parts younger than N days are the ones likely to be
		// rewritten by merges within the next ~N days, and from that moment the backup owns their bytes
		report.HardlinkEstimate1d += row.Bytes1d
		report.HardlinkEstimate7d += row.Bytes7d
		report.HardlinkEstimate30d += row.Bytes30d
	}
	report.HardlinkMaxSize = report.DataSize
	report.TotalSize = report.DataSize + report.ObjectDiskSize
	return report, nil
}
