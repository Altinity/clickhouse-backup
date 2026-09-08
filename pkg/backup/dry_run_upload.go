package backup

import (
	"encoding/json"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/rs/zerolog/log"
)

// estimateUploadDataSize sums the on-disk bytes of the parts which would be uploaded for one table.
// Parts marked Required belong to the `--diff-from`/`--diff-from-remote` base and are not uploaded,
// plain/plain_rewritable disks have no local files in the backup at all, their objects were already
// copied to object_disk_path during create. Parts without a recorded size (backups created before
// https://github.com/Altinity/clickhouse-backup/issues/1268) are counted in unknownParts and, when a
// disk has no usable per-part sizes at all, the per-disk TableMetadata.Size total is used instead,
// it also covers the Required parts of that disk, so such an estimate is an upper bound.
func (b *Backuper) estimateUploadDataSize(table *metadata.TableMetadata, disks []clickhouse.Disk) (dataSize uint64, partsCount int, unknownParts int) {
	for diskName, parts := range table.Parts {
		if b.shouldDiskNameSkipByNameOrType(diskName, disks) {
			continue
		}
		if disk := b.findDiskByName(disks, diskName); disk != nil && b.isDiskPlain(*disk) {
			continue
		}
		diskSize := uint64(0)
		diskUnknown := 0
		diskParts := 0
		for i := range parts {
			if parts[i].Required {
				continue
			}
			diskParts++
			if parts[i].Size == 0 {
				diskUnknown++
				continue
			}
			diskSize += parts[i].Size
		}
		if diskSize == 0 && diskUnknown > 0 {
			if total, exists := table.Size[diskName]; exists && total > 0 {
				log.Warn().Str("database", table.Database).Str("table", table.Table).Str("disk", diskName).
					Msgf("dry-run: %d parts have no size in metadata, fall back to the per-disk total %d bytes, `required` parts are included", diskUnknown, total)
				diskSize = uint64(total)
			}
		}
		dataSize += diskSize
		partsCount += diskParts
		unknownParts += diskUnknown
	}
	return dataSize, partsCount, unknownParts
}

// estimateUploadMetadataSize returns the size of the table metadata json which would be uploaded,
// it mirrors uploadTableMetadataRegular, so it honors `--tables`/`--partitions` filtering
func estimateUploadMetadataSize(table *metadata.TableMetadata) uint64 {
	content, err := json.MarshalIndent(table, "", "\t")
	if err != nil {
		log.Warn().Err(err).Msgf("dry-run: can't marshal %s.%s metadata, size not counted", table.Database, table.Table)
		return 0
	}
	return uint64(len(content))
}
