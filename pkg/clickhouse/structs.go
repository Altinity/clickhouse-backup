package clickhouse

import (
	"time"
)

type ShardBackupType string

const (
	ShardBackupFull   = "full"
	ShardBackupNone   = "none"
	ShardBackupSchema = "schema-only"
)

// Table - ClickHouse table struct
type Table struct {
	// common fields for all `clickhouse-server` versions
	Database string `ch:"database"`
	Name     string `ch:"name"`
	Engine   string `ch:"engine"`
	// fields depends on `clickhouse-server` version
	DataPath         string   `ch:"data_path"` // For legacy support
	DataPaths        []string `ch:"data_paths"`
	UUID             string   `ch:"uuid"`
	CreateTableQuery string   `ch:"create_table_query"`
	TotalBytes       uint64   `ch:"total_bytes"`
	Skip             bool
	BackupType       ShardBackupType
}

// IsSystemTablesFieldPresent - ClickHouse `system.tables` varius field flags
type IsSystemTablesFieldPresent struct {
	IsDataPathPresent         uint64 `ch:"is_data_path_present"`
	IsDataPathsPresent        uint64 `ch:"is_data_paths_present"`
	IsUUIDPresent             uint64 `ch:"is_uuid_present"`
	IsCreateTableQueryPresent uint64 `ch:"is_create_table_query_present"`
	IsTotalBytesPresent       uint64 `ch:"is_total_bytes_present"`
}

type Disk struct {
	Name     string `ch:"name"`
	Path     string `ch:"path"`
	Type     string `ch:"type"`
	IsBackup bool
}

// Database - Clickhouse system.databases struct
type Database struct {
	Name   string `ch:"name"`
	Engine string `ch:"engine"`
	Query  string `ch:"query"`
}

// Function - Clickhouse system.functions struct
type Function struct {
	Name        string `ch:"name"`
	CreateQuery string `ch:"create_query"`
}

// Macro - info from system.macros
type Macro struct {
	Macro        string `ch:"macro"`
	Substitution string `ch:"substitution"`
}

// SystemBackups - info from system.backups
type SystemBackups struct {
	Id                string    `ch:"id"`
	UUID              string    `ch:"uuid"`
	BackupName        string    `ch:"backup_name"`
	Name              string    `ch:"name"`
	Status            string    `ch:"status"`
	StatusChangedTime time.Time `ch:"status_changed_time"`
	Error             string    `ch:"error"`
	Internal          bool      `ch:"internal"`
	StartTime         time.Time `ch:"start_time"`
	EndTime           time.Time `ch:"end_time"`
	CompressedSize    uint64    `ch:"compressed_size"`
	UncompressedSize  uint64    `ch:"uncompressed_size"`
	NumFiles          uint64    `ch:"num_files"`
}
