package metadata

import (
	"encoding/json"
	"os"
	"path"

	"github.com/rs/zerolog/log"
)

type TableMetadata struct {
	Files                map[string][]string `json:"files,omitempty"`
	RebalancedFiles      map[string]string   `json:"rebalanced_files,omitempty"`
	Table                string              `json:"table"`
	Database             string              `json:"database"`
	UUID                 string              `json:"uuid,omitempty"`
	Parts                map[string][]Part   `json:"parts"` // Parts in the backup (represents DB state at backup time)
	Query                string              `json:"query"`
	Size                 map[string]int64    `json:"size"`                  // how much size on each disk
	TotalBytes           uint64              `json:"total_bytes,omitempty"` // total table size
	DependenciesTable    string              `json:"dependencies_table,omitempty"`
	DependenciesDatabase string              `json:"dependencies_database,omitempty"`
	Mutations            []MutationMetadata  `json:"mutations,omitempty"`
	MetadataOnly         bool                `json:"metadata_only"`
	LocalFile            string              `json:"local_file,omitempty"`
	Checksums            map[string]uint64   `json:"checksums,omitempty"`
}

func (tm *TableMetadata) Save(location string, metadataOnly bool) (uint64, error) {
	newTM := TableMetadata{
		Table:                tm.Table,
		Database:             tm.Database,
		UUID:                 tm.UUID,
		Query:                tm.Query,
		DependenciesTable:    tm.DependenciesTable,
		DependenciesDatabase: tm.DependenciesDatabase,
		MetadataOnly:         true,
	}

	if !metadataOnly {
		newTM.Files = tm.Files
		newTM.Parts = tm.Parts
		newTM.Checksums = tm.Checksums
		newTM.Size = tm.Size
		newTM.TotalBytes = tm.TotalBytes
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
