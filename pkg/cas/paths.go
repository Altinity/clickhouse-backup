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
