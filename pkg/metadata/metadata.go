package metadata

type BackupMetadata struct {
	BackupName              string            `json:"backup_name"`
	Disks                   map[string]string `json:"disks"` // "default": "/var/lib/clickhouse"
	ClickhouseBackupVersion string            `json:"version"`
	CreationDate            string
	Tags                    string // "type=manual", "type=sheduled", "hostname": "", "shard="
	ClickHouseVersion       string `json:"clickhouse_version,omitempty"`
}

type TableMetadata struct {
	Files       map[string][]string `json:"files,omitempty"` // "default": ["default_table1_0", "default_table1_0"] - архивы
	Disks       map[string]string   `json:"disks"`           // "default": "/var/lib/clickhouse"
	Table       string              `json:"table"`
	Database    string              `json:"database"`
	Tags        string              // "type=manual", "type=sheduled", "hostname": "", "shard="
	IncrementOf string
	Parts       map[string][]Part // "default": [] `json:"parts"`
	Query       string            `json:"query"`
	UUID        string            `json:"uuid"`
	// Macros ???
	Size                 map[string]int64 `json:"size"` // сколько занимает бэкап на каждом диске
	TotalBytes           int64            `json:"total_bytes,omitempty"` // общий объём бэкапа
	DependencesTable     string           `json:"dependencies_table"`
	DependenciesDatabase string           `json:"dependencies_database"`
}

type Part struct {
	Partition                         string `json:"partition"`
	Name                              string `json:"name"`
	Path                              string `json:"path"` // TODO: должен быть относительный путь вообще непонятно зачем он, его можно из name получить
	HashOfAllFiles                    string `json:"hash_of_all_files"`
	HashOfUncompressedFiles           string `json:"hash_of_uncompressed_files"`
	UncompressedHashOfCompressedFiles string `json:"uncompressed_hash_of_compressed_files"`
	PartitionID                       string `json:"partition_id"`
}
