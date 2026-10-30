package metadata

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"os"
	"path"
)

type TableMetadata struct {
	Files           map[string][]string `json:"files,omitempty"`
	RebalancedFiles map[string]string   `json:"rebalanced_files,omitempty"`
	Table           string              `json:"table"`
	Database        string              `json:"database"`
	UUID            string              `json:"uuid,omitempty"`
	Parts           map[string][]Part   `json:"parts"`
	// BrokenParts - per-disk data parts skipped as broken during backup creation because
	// general.max_broken_part_ratio > 0 tolerated them, see https://github.com/Altinity/clickhouse-backup/issues/1418
	BrokenParts          map[string][]Part  `json:"broken_parts,omitempty"`
	Query                string             `json:"query"`
	Size                 map[string]int64   `json:"size"`                  // how much size on each disk
	TotalBytes           uint64             `json:"total_bytes,omitempty"` // total table size
	DependenciesTable    string             `json:"dependencies_table,omitempty"`
	DependenciesDatabase string             `json:"dependencies_database,omitempty"`
	Mutations            []MutationMetadata `json:"mutations,omitempty"`
	MetadataOnly         bool               `json:"metadata_only"`
	LocalFile            string             `json:"local_file,omitempty"`
	Checksums            map[string]uint64  `json:"checksums,omitempty"`
	HashOfAllFiles       map[string]string  `json:"hash_of_all_files,omitempty"`
	// KafkaKeeper - keeper state of Kafka engine table with `kafka_keeper_path` setting, see https://github.com/Altinity/clickhouse-backup/issues/1120
	KafkaKeeper *KafkaKeeperMetadata `json:"kafka_keeper,omitempty"`
    // OriginalDatabase / OriginalTable hold the pre-mapping names as they exist INSIDE the
    // backup, captured when restore-database-mapping / restore-table-mapping rewrites the
    // in-memory table list (Database/Table are overwritten with the target names at that
    // point). The embedded RESTORE path needs the originals to emit `RESTORE TABLE <origin>
    // AS <target> FROM Disk(...)`; without them it asks ClickHouse for the target name inside
    // the backup and fails with "code: 605 Table ... not found in backup". Restore-time only;
    // never serialized into backup metadata.
    OriginalDatabase string `json:"-"`
    OriginalTable    string `json:"-"`
}

// KafkaKeeperMetadata - persistent znodes under `kafka_keeper_path` dumped while the Kafka table was detached
type KafkaKeeperMetadata struct {
	// Path - resolved source keeper path, informational only, restore resolves the path from the restored query
	Path string `json:"path"`
	// Nodes - znodes with paths relative to Path in pre-order, Path itself is not included
	Nodes []KafkaKeeperNode `json:"nodes"`
}

type KafkaKeeperNode struct {
	Path  string `json:"path"`
	Value string `json:"value"`
}

func (tm *TableMetadata) Save(location string, metadataOnly bool) (uint64, error) {
	newTM := TableMetadata{
		Table:                tm.Table,
		Database:             tm.Database,
		UUID:                 tm.UUID,
		Query:                tm.Query,
		DependenciesTable:    tm.DependenciesTable,
		DependenciesDatabase: tm.DependenciesDatabase,
		KafkaKeeper:          tm.KafkaKeeper,
		MetadataOnly:         true,
	}

	if !metadataOnly {
		newTM.Files = tm.Files
		newTM.Parts = tm.Parts
		newTM.BrokenParts = tm.BrokenParts
		newTM.Checksums = tm.Checksums
		newTM.HashOfAllFiles = tm.HashOfAllFiles
		newTM.Size = tm.Size
		newTM.TotalBytes = tm.TotalBytes
		newTM.RebalancedFiles = tm.RebalancedFiles
		newTM.MetadataOnly = false
	}
	if err := os.MkdirAll(path.Dir(location), 0750); err != nil {
		return 0, err
	}
	body, err := json.MarshalIndent(&newTM, "", "\t")
	if err != nil {
		return 0, err
	}
	return uint64(len(body)), os.WriteFile(location, body, 0640)
}

func (tm *TableMetadata) Load(location string) (uint64, error) {
	data, err := os.ReadFile(location)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(data, tm); err != nil {
		return 0, err
	}
	log.Debug().Msgf("success %s", location)
	return uint64(len(data)), nil
}
