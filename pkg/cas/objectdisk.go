package cas

import (
	"strings"
)

// objectDiskTypes lists ClickHouse system.disks.type values that mean the
// underlying storage is object-based and therefore not supported by CAS v1.
// See docs/cas-design.md §3 (object-disk parts NOT supported in v1).
var objectDiskTypes = map[string]bool{
	"s3":                 true,
	"s3_plain":           true,
	"azure_blob_storage": true,
	"azure":              true, // legacy type emitted by older ClickHouse versions; pkg/backup/backuper.go:225 treats it as object disk too
	"hdfs":               true,
	"web":                true,
}

// IsObjectDiskType reports whether a system.disks.type value indicates an
// object disk (vs. a local-filesystem disk).
func IsObjectDiskType(t string) bool { return objectDiskTypes[t] }

// ObjectDiskHit identifies one (database, table, disk, disk-type) combination
// where a CAS upload would refuse (or, with --skip-object-disks, skip).
type ObjectDiskHit struct {
	Database string
	Table    string
	Disk     string
	DiskType string
}

// DetectObjectDiskTables walks tables and returns all (db, table, disk) where
// the table has at least one DataPath that lives under an object-disk.
//
// Mapping a DataPath to a disk uses the disk's Path prefix from system.disks.
// A DataPath is considered "on disk D" if it has D.Path as a prefix. The
// longest-matching prefix wins (so a disk at "/var/lib/clickhouse/disks/s3/"
// is matched before one at "/var/lib/clickhouse/").
func DetectObjectDiskTables(tables []TableInfo, disks []DiskInfo) []ObjectDiskHit {
	// Pre-sort disks by Path length descending so we can do longest-prefix
	// matching with a simple loop.
	sorted := make([]DiskInfo, len(disks))
	copy(sorted, disks)
	// Insertion sort is fine for typical len(disks) ~ small.
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && len(sorted[j-1].Path) < len(sorted[j].Path); j-- {
			sorted[j-1], sorted[j] = sorted[j], sorted[j-1]
		}
	}

	var hits []ObjectDiskHit
	seen := make(map[ObjectDiskHit]struct{})
	for _, t := range tables {
		for _, dp := range t.DataPaths {
			d, ok := matchDisk(dp, sorted)
			if !ok {
				continue
			}
			if !objectDiskTypes[d.Type] {
				continue
			}
			h := ObjectDiskHit{Database: t.Database, Table: t.Name, Disk: d.Name, DiskType: d.Type}
			if _, dup := seen[h]; dup {
				continue
			}
			seen[h] = struct{}{}
			hits = append(hits, h)
		}
	}
	return hits
}

// matchDisk returns the disk whose Path is the longest prefix of dataPath, or
// (DiskInfo{}, false) if none matches. Caller must pass disks sorted by Path
// length descending.
func matchDisk(dataPath string, sortedDisks []DiskInfo) (DiskInfo, bool) {
	for _, d := range sortedDisks {
		if d.Path == "" {
			continue
		}
		// Normalize: ensure trailing separator on the disk path so a dir
		// boundary is required (avoid "/var/lib/foo" matching "/var/lib/foobar/...").
		prefix := d.Path
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		if strings.HasPrefix(dataPath, prefix) || dataPath == strings.TrimSuffix(prefix, "/") {
			return d, true
		}
	}
	return DiskInfo{}, false
}
