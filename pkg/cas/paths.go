package cas

import "github.com/Altinity/clickhouse-backup/v2/pkg/common"

// All helpers take clusterPrefix, which must end with "/".

func MetadataDir(clusterPrefix, backup string) string {
	return clusterPrefix + "metadata/" + backup + "/"
}

func MetadataJSONPath(clusterPrefix, backup string) string {
	return MetadataDir(clusterPrefix, backup) + "metadata.json"
}

func TableMetaPath(clusterPrefix, backup, db, table string) string {
	return MetadataDir(clusterPrefix, backup) + "metadata/" +
		common.TablePathEncode(db) + "/" + common.TablePathEncode(table) + ".json"
}

// PartArchivePath returns the per-(disk, db, table) tar.zstd archive key.
// disk is intentionally NOT TablePathEncode'd: ClickHouse disk names are
// constrained at config-load time to alphanumeric + dash/underscore, so they
// are path-safe by construction. db and table can be arbitrary user input
// and must be encoded.
func PartArchivePath(clusterPrefix, backup, disk, db, table string) string {
	return MetadataDir(clusterPrefix, backup) + "parts/" + disk + "/" +
		common.TablePathEncode(db) + "/" + common.TablePathEncode(table) + ".tar.zstd"
}

func InProgressMarkerPath(clusterPrefix, backup string) string {
	return clusterPrefix + "inprogress/" + backup + ".marker"
}

func PruneMarkerPath(clusterPrefix string) string {
	return clusterPrefix + "prune.marker"
}
