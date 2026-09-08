package backup

import (
	"encoding/json"

	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/rs/zerolog/log"
)

// DryRunReport describes what a command would do, without doing it. issues/1012
type DryRunReport struct {
	Command              string `json:"command"`
	BackupName           string `json:"backup_name,omitempty"`
	DryRun               bool   `json:"dry_run"` // always true
	TableCount           int    `json:"table_count"`
	PartsCount           int    `json:"parts_count,omitempty"`
	DataSize             uint64 `json:"data_size"`
	CompressedSize       uint64 `json:"compressed_size,omitempty"`
	ObjectDiskSize       uint64 `json:"object_disk_size,omitempty"`
	MetadataSize         uint64 `json:"metadata_size,omitempty"`
	RBACSize             uint64 `json:"rbac_size,omitempty"`
	ConfigSize           uint64 `json:"config_size,omitempty"`
	NamedCollectionsSize uint64 `json:"named_collections_size,omitempty"`
	TotalSize            uint64 `json:"total_size"`
	// UnknownSizeParts counts parts whose size is not recorded in metadata (older backups)
	UnknownSizeParts int `json:"unknown_size_parts,omitempty"`
	// create only: hardlinks cost nothing now, but become owned as merges remove originals
	HardlinkMaxSize     uint64 `json:"hardlink_max_size,omitempty"`
	HardlinkEstimate1d  uint64 `json:"hardlink_estimate_1d,omitempty"`
	HardlinkEstimate7d  uint64 `json:"hardlink_estimate_7d,omitempty"`
	HardlinkEstimate30d uint64 `json:"hardlink_estimate_30d,omitempty"`
	// DependentBackups is delete only, backups which reference the deleted one via required_backup
	DependentBackups []string `json:"dependent_backups,omitempty"`
}

// JSONString marshals the report for the `result` field of a status row, it returns
// an empty string for a nil report so callers don't need to check
func (r *DryRunReport) JSONString() string {
	if r == nil {
		return ""
	}
	js, err := json.Marshal(r)
	if err != nil {
		log.Warn().Err(err).Msgf("can't marshal dry-run report for %s", r.Command)
		return ""
	}
	return string(js)
}

// setDryRunResult stores the report on the Backuper, so REST API handlers can return it, and logs one summary line
func (b *Backuper) setDryRunResult(r *DryRunReport) {
	if r == nil {
		return
	}
	r.DryRun = true
	b.DryRunResult = r
	fields := map[string]interface{}{
		"operation":  r.Command,
		"dry_run":    true,
		"tables":     r.TableCount,
		"total_size": utils.FormatBytes(r.TotalSize),
	}
	if r.BackupName != "" {
		fields["backup"] = r.BackupName
	}
	if r.PartsCount > 0 {
		fields["parts"] = r.PartsCount
	}
	if r.UnknownSizeParts > 0 {
		fields["unknown_size_parts"] = r.UnknownSizeParts
	}
	for name, size := range map[string]uint64{
		"data_size":              r.DataSize,
		"compressed_size":        r.CompressedSize,
		"object_disk_size":       r.ObjectDiskSize,
		"metadata_size":          r.MetadataSize,
		"rbac_size":              r.RBACSize,
		"config_size":            r.ConfigSize,
		"named_collections_size": r.NamedCollectionsSize,
		"hardlink_max_size":      r.HardlinkMaxSize,
		"hardlink_estimate_1d":   r.HardlinkEstimate1d,
		"hardlink_estimate_7d":   r.HardlinkEstimate7d,
		"hardlink_estimate_30d":  r.HardlinkEstimate30d,
	} {
		if size > 0 {
			fields[name] = utils.FormatBytes(size)
		}
	}
	if len(r.DependentBackups) > 0 {
		fields["dependent_backups"] = r.DependentBackups
	}
	log.Info().Fields(fields).Msgf("dry-run: would process %d tables, %s data", r.TableCount, utils.FormatBytes(r.TotalSize))
}
