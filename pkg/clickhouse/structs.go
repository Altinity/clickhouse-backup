package clickhouse

import (
	"time"
)

// Table - ClickHouse table struct
type Table struct {
	// common fields for all `clickhouse-server` versions
	Database string `db:"database"`
	Name     string `db:"name"`
	Engine   string `db:"engine"`
	// fields depends on `clickhouse-server` version
	DataPath         string   `db:"data_path,omitempty"` // For legacy support
	DataPaths        []string `db:"data_paths,omitempty"`
	UUID             string   `db:"uuid,omitempty"`
	CreateTableQuery string   `db:"create_table_query,omitempty"`
	TotalBytes       uint64   `db:"total_bytes,omitempty"`
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
	Name     string `db:"name"`
	Path     string `db:"path"`
	Type     string `db:"type"`
	IsBackup bool
}

// Database - Clickhouse system.databases struct
type Database struct {
	Name   string `db:"name"`
	Engine string `db:"engine"`
	Query  string `db:"query"`
}

// Function - Clickhouse system.functions struct
type Function struct {
	Name        string `db:"name"`
	CreateQuery string `db:"create_query"`
}

// macro - info from system.macros
type macro struct {
	Macro        string `db:"macro"`
	Substitution string `db:"substitution"`
}

// SystemBackups - info from system.backups
type SystemBackups struct {
	UUID              string    `db:"uuid"`
	BackupName        string    `db:"backup_name"`
	Status            string    `db:"status"`
	StatusChangedTime time.Time `db:"status_changed_time"`
	Error             string    `db:"error"`
	Internal          bool      `db:"internal"`
}
