package metadata

import (
	"time"
)

type TableTitle struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

type BackupMetadata struct {
	BackupName              string            `json:"backup_name"`
	Disks                   map[string]string `json:"disks"` // "default": "/var/lib/clickhouse"
	ClickhouseBackupVersion string            `json:"version"`
	CreationDate            time.Time         `json:"creation_date"`
	Tags                    string            `json:"tags,omitempty"` // "type=manual", "type=sheduled", "hostname": "", "shard="
	ClickHouseVersion       string            `json:"clickhouse_version,omitempty"`
	DataSize                uint64            `json:"data_size,omitempty"`
	MetadataSize            uint64            `json:"metadata_size"`
	RBACSize                uint64            `json:"rbac_size,omitempty"`
	ConfigSize              uint64            `json:"config_size,omitempty"`
	CompressedSize          uint64            `json:"compressed_size,omitempty"`
	Databases               []DatabasesMeta   `json:"databases,omitempty"`
	Tables                  []TableTitle      `json:"tables"`
	Functions               []FunctionsMeta   `json:"functions"`
	DataFormat              string            `json:"data_format"`
	RequiredBackup          string            `json:"required_backup,omitempty"`
}

type DatabasesMeta struct {
	Name   string `json:"name"`
	Engine string `json:"engine"`
	Query  string `json:"query"`
}

type FunctionsMeta struct {
	Name        string `json:"name"`
	CreateQuery string `json:"create_query"`
}

type TableMetadata struct {
	Files map[string][]string `json:"files,omitempty"`
	// Disks       map[string]string   `json:"disks"` // "default": "/var/lib/clickhouse"
	Table       string            `json:"table"`
	Database    string            `json:"database"`
	IncrementOf string            `json:"increment_of,omitempty"`
	Parts       map[string][]Part `json:"parts"`
	Query       string            `json:"query"`
	// UUID        string            `json:"uuid,omitempty"`
	// Macros ???
	Size                 map[string]int64 `json:"size"`                  // how much size on each disk
	TotalBytes           uint64           `json:"total_bytes,omitempty"` // total table size
	DependenciesTable    string           `json:"dependencies_table,omitempty"`
	DependenciesDatabase string           `json:"dependencies_database,omitempty"`
	MetadataOnly         bool             `json:"metadata_only"`
}

type Part struct {
	Partition string `json:"partition,omitempty"`
	Name      string `json:"name"`
	Required  bool   `json:"required,omitempty"`
	// Path                              string    `json:"path"`              // TODO: make it relative? look like useless now, can be calculated from Name
	HashOfAllFiles                    string     `json:"hash_of_all_files,omitempty"` // ???
	HashOfUncompressedFiles           string     `json:"hash_of_uncompressed_files,omitempty"`
	UncompressedHashOfCompressedFiles string     `json:"uncompressed_hash_of_compressed_files,omitempty"` // ???
	PartitionID                       string     `json:"partition_id,omitempty"`
	ModificationTime                  *time.Time `json:"modification_time,omitempty"`
	Size                              int64      `json:"size,omitempty"`
	// bytes_on_disk, data_compressed_bytes, data_uncompressed_bytes
}

type PartFilesSplitted struct {
	Prefix string
	Files  []string
}
