package clickhouse

import (
	"database/sql"
	"sort"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
)

// Table - ClickHouse table struct
type Table struct {
	// common fields for all `clickhouse-server` versions
	Database string `db:"database"`
	Name     string `db:"name"`
	Engine   string `db:"engine"`
	// fields depends on `clickhouse-server` version
	DataPath         string        `db:"data_path,omitempty"` // For legacy support
	DataPaths        []string      `db:"data_paths,omitempty"`
	UUID             string        `db:"uuid,omitempty"`
	CreateTableQuery string        `db:"create_table_query,omitempty"`
	TotalBytes       sql.NullInt64 `db:"total_bytes,omitempty"`
	Skip             bool
}

// IsSystemTablesFieldPresent - ClickHouse `system.tables` varius field flags
type IsSystemTablesFieldPresent struct {
	IsDataPathPresent         int `db:"is_data_path_present"`
	IsDataPathsPresent        int `db:"is_data_paths_present"`
	IsUUIDPresent             int `db:"is_uuid_present"`
	IsCreateTableQueryPresent int `db:"is_create_table_query_present"`
	IsTotalBytesPresent       int `db:"is_total_bytes_present"`
}

type Disk struct {
	Name string `db:"name"`
	Path string `db:"path"`
	Type string `db:"type"`
}

// Database - Clickhouse system.databases struct
type Database struct {
	Name   string `db:"name"`
	Engine string `db:"engine"`
	Query  string `db:"query"`
}

// BackupPartition - struct representing Clickhouse partition
// type BackupPartition struct {
// 	Partition                         string `json:"partition"`
// 	Name                              string `json:"name"`
// 	Path                              string `json:"Path"`
// 	HashOfAllFiles                    string `json:"hash_of_all_files"`
// 	HashOfUncompressedFiles           string `json:"hash_of_uncompressed_files"`
// 	UncompressedHashOfCompressedFiles string `json:"uncompressed_hash_of_compressed_files"`
// 	Active                            uint8  `json:"active"`
// 	DiskName                          string `json:"disk_name"`
// 	PartitionID                       string `json:"partition_id"`
// 	DataUncompressedBytes             int64  `json:"data_uncompressed_bytes"`
// }

// // BackupTable - struct to store additional information on partitions
// type BackupTable struct {
// 	Database   string
// 	Name       string
// 	Partitions map[string][]metadata.Part
// 	DataPaths  map[string]string
// }

// BackupTables - slice of BackupTable
type BackupTables []metadata.TableMetadata

// Sort - sorting BackupTables slice orderly by name
func (bt BackupTables) Sort() {
	sort.Slice(bt, func(i, j int) bool {
		return (bt[i].Database < bt[j].Database) || (bt[i].Database == bt[j].Database && bt[i].Table < bt[j].Table)
	})
}

// Partition - partition info from system.parts
type partition struct {
	Partition                         string    `db:"partition"`
	PartitionID                       string    `db:"partition_id"`
	Name                              string    `db:"name"`
	Path                              string    `db:"path"`
	HashOfAllFiles                    string    `db:"hash_of_all_files"`
	HashOfUncompressedFiles           string    `db:"hash_of_uncompressed_files"`
	UncompressedHashOfCompressedFiles string    `db:"uncompressed_hash_of_compressed_files"`
	Active                            uint8     `db:"active"`
	DiskName                          string    `db:"disk_name"`
	ModificationTime                  time.Time `db:"modification_time"`
	DataUncompressedBytes             int64     `db:"data_uncompressed_bytes"`
}

// PartDiff - Data part discrepancies infos
type PartDiff struct {
	BTable           metadata.TableMetadata
	PartitionsAdd    []metadata.Part
	PartitionsRemove []metadata.Part
}
