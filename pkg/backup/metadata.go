package backup

type Metadata struct {
	BackupName              string              `json:"backup_name"`
	Files                   map[string][]string `json:"files,omitempty"` // "default": ["default_table1_0", "default_table1_0"] - архивы
	Disks                   map[string]string   `json:"disks"`           // "default": "/var/lib/clickhouse"
	Table                   string              `json:"table"`
	Database                string              `json:"database"`
	ClickhouseBackupVersion string              `json:"version"`
	CreationDate            string
	Tags                    string // "type=manual", "type=sheduled", "hostname": "", "shard="
	IncrementOf             string
	Parts                   map[string][]Part // "default": []
	Query                   string            `json:"query"`
	// Macros ???
	ClickHouseVersion string           `json:"clickhouse_version,omitempty"`
	Size              map[string]int64 `json:"size"`
}

type Part struct {
	Partition                         string `json:"partition"`
	Name                              string `json:"name"`
	Path                              string `json:"path"`
	HashOfAllFiles                    string `json:"hash_of_all_files"`
	HashOfUncompressedFiles           string `json:"hash_of_uncompressed_files"`
	UncompressedHashOfCompressedFiles string `json:"uncompressed_hash_of_compressed_files"`
	DiskName                          string `json:"disk_name"`
	PartitionID                       string `json:"partition_id"`
}
