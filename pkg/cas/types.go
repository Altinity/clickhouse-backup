package cas

const (
	// LayoutVersion is the schema version of the CAS layout itself. Persisted
	// per backup in BackupMetadata.CAS.LayoutVersion. Bumps are major/breaking;
	// tools encountering a higher version refuse with a clear error.
	LayoutVersion uint8 = 1

	// MinInline / MaxInline bound the persisted InlineThreshold. ValidateBackup
	// rejects backups outside this range. See docs/cas-design.md §6.2.1.
	MinInline uint64 = 1
	MaxInline uint64 = 1 << 30 // 1 GiB
)

// TableInfo is a minimal description of a ClickHouse table used by
// DetectObjectDiskTables. The caller (e.g. cas-upload) populates this from
// clickhouse.Table values; keeping it here avoids an import cycle between
// pkg/cas and pkg/clickhouse.
type TableInfo struct {
	Database  string
	Name      string
	DataPaths []string
}

// DiskInfo is a minimal description of a ClickHouse disk from system.disks,
// used by DetectObjectDiskTables.
type DiskInfo struct {
	Name string
	Path string
	Type string
}

// Triplet is a (filename, size, hash) tuple extracted from a part's
// checksums.txt. The CAS upload planner classifies each Triplet as inline
// (size <= InlineThreshold; goes into per-table tar.zstd) or blob
// (size > InlineThreshold; uploaded to cas/.../blob/<aa>/<rest>).
type Triplet struct {
	Filename string
	Size     uint64
	HashLow  uint64
	HashHigh uint64
}

// InProgressMarker is the JSON body of cas/<cluster>/inprogress/<backup>.marker.
// Written at upload start, deleted at commit. Used by cas-prune for
// abandoned-upload cleanup and by cas-delete to detect uploads in flight.
type InProgressMarker struct {
	Backup    string `json:"backup"`
	Host      string `json:"host"`
	StartedAt string `json:"started_at"` // RFC3339 UTC
	Tool      string `json:"tool"`       // e.g. "clickhouse-backup v2.7.0"
}

// PruneMarker is the JSON body of cas/<cluster>/prune.marker. Written at the
// start of cas-prune; the run-id is read back to detect concurrent prunes.
// Released via deferred call so panics/errors don't strand it.
type PruneMarker struct {
	Host      string `json:"host"`
	StartedAt string `json:"started_at"` // RFC3339 UTC
	RunID     string `json:"run_id"`     // 16 hex chars from crypto/rand
	Tool      string `json:"tool"`
}
