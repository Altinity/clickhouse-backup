package metadata

import (
	"encoding/json"
	apexLog "github.com/apex/log"
	"os"
	"path"
)

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

func (tm *TableMetadata) Save(location string, metadataOnly bool) (uint64, error) {
	newTM := TableMetadata{
		Table:                tm.Table,
		Database:             tm.Database,
		Query:                tm.Query,
		DependenciesTable:    tm.DependenciesTable,
		DependenciesDatabase: tm.DependenciesDatabase,
		MetadataOnly:         true,
	}

	if !metadataOnly {
		newTM.Files = tm.Files
		newTM.Parts = tm.Parts
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
	log := apexLog.WithField("logger", "metadata.Load")
	data, err := os.ReadFile(location)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(data, tm); err != nil {
		return 0, err
	}
	log.Debugf("success %s", location)
	return uint64(len(data)), nil
}
