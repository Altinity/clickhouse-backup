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
	Disks                   map[string]string `json:"disks"`      // "default": "/var/lib/clickhouse"
	DiskTypes               map[string]string `json:"disk_types"` // "default": "local"
	ClickhouseBackupVersion string            `json:"version"`
	CreationDate            time.Time         `json:"creation_date"`
	Tags                    string            `json:"tags,omitempty"` // "regular,embedded"
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
	Files                map[string][]string `json:"files,omitempty"`
	RebalancedFiles      map[string]string   `json:"rebalanced_files,omitempty"`
	Table                string              `json:"table"`
	Database             string              `json:"database"`
	Parts                map[string][]Part   `json:"parts"`
	Query                string              `json:"query"`
	Size                 map[string]int64    `json:"size"`                  // how much size on each disk
	TotalBytes           uint64              `json:"total_bytes,omitempty"` // total table size
	DependenciesTable    string              `json:"dependencies_table,omitempty"`
	DependenciesDatabase string              `json:"dependencies_database,omitempty"`
	Mutations            []MutationMetadata  `json:"mutations,omitempty"`
	MetadataOnly         bool                `json:"metadata_only"`
	LocalFile            string              `json:"local_file,omitempty"`
}

type MutationMetadata struct {
	MutationId string `json:"mutation_id" ch:"mutation_id"`
	Command    string `json:"command" ch:"command"`
}

type Part struct {
	Name           string `json:"name"`
	Required       bool   `json:"required,omitempty"`
	RebalancedDisk string `json:"rebalanced_disk,omitempty"`
}

type SplitPartFiles struct {
	Prefix string
	Files  []string
}
