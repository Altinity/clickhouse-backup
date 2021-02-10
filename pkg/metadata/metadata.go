package metadata

import "time"

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
	Size                    int64             `json:"size"`
	CompressedSize          int64             `json:"compressed_size,omitempty"`
	Tables                  []TableTitle      `json:"tables"`
}

type TableMetadata struct {
	Files       map[string][]string `json:"files,omitempty"`
	// Disks       map[string]string   `json:"disks"` // "default": "/var/lib/clickhouse"
	Table       string              `json:"table"`
	Database    string              `json:"database"`
	IncrementOf string              `json:"increment_of,omitempty"`
	Parts       map[string][]Part   `json:"parts"`
	Query       string              `json:"query"`
	UUID        string              `json:"uuid,omitempty"`
	// Macros ???
	Size                 map[string]int64 `json:"size"`                  // сколько занимает бэкап на каждом диске
	TotalBytes           int64            `json:"total_bytes,omitempty"` // общий объём бэкапа
	RealSize int64
	DependencesTable     string           `json:"dependencies_table,omitempty"`
	DependenciesDatabase string           `json:"dependencies_database,omitempty"`
}

type Part struct {
	Partition                         string    `json:"partition"`
	Name                              string    `json:"name"`
	Path                              string    `json:"path"`              // TODO: должен быть относительный путь вообще непонятно зачем он, его можно из name получить
	HashOfAllFiles                    string    `json:"hash_of_all_files"` // ???
	HashOfUncompressedFiles           string    `json:"hash_of_uncompressed_files"`
	UncompressedHashOfCompressedFiles string    `json:"uncompressed_hash_of_compressed_files"` // ???
	PartitionID                       string    `json:"partition_id"`
	ModificationTime                  time.Time `json:"modification_time"`
	Size                              int64     `json:"size"`
	// bytes_on_disk, data_compressed_bytes, data_uncompressed_bytes
}
