package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

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

func (bm *BackupMetadata) Save(location string) error {
	tbBody, err := json.MarshalIndent(bm, "", "\t")
	if err != nil {
		return fmt.Errorf("can't marshall backup metadata: %v", err)
	}
	if err := os.WriteFile(location, tbBody, 0640); err != nil {
		return fmt.Errorf("can't save backup metadata: %v", err)
	}
	return nil
}
