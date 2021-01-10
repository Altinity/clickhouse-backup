package backup

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"

	"github.com/apex/log"
)

// CopyPartHashes - Copy data parts hashes by tablePattern
func CopyPartHashes(cfg config.Config, tablePattern string, backupName string) error {
	allparts := map[string][]metadata.Part{}
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	dataPath, err := ch.GetDefaultPath()
	if err != nil || dataPath == "" {
		return fmt.Errorf("can't get data path from clickhouse: %v\nyou can set data_path in config file", err)
	}

	allTables, err := ch.GetTables()
	if err != nil {
		return fmt.Errorf("can't get tables from clickhouse: %v", err)
	}
	backupTables := filterTablesByPattern(allTables, tablePattern)
	if len(backupTables) == 0 {
		return fmt.Errorf("there are no tables in clickhouse, create something to freeze")
	}
	for _, table := range backupTables {
		if table.Skip {
			log.Infof("Skip '%s.%s'", table.Database, table.Name)
			continue
		}

		parts, err := ch.GetPartitions(table)
		if err != nil {
			return err
		}
		allparts[table.Database+"."+table.Name] = parts["default"]

	}
	log.Debug("Writing part hashes")
	byteArray, err := json.MarshalIndent(allparts, "", " ")
	if err != nil {
		return err
	}
	hashPartsPath := path.Join(dataPath, "backup", backupName, hashfile)
	return ioutil.WriteFile(hashPartsPath, byteArray, 0644)
}

// Flashback - restore tables matched by tablePattern from backupName by restroing only modified parts.
func Flashback(cfg config.Config, backupName string, tablePattern string) error {
	/*if schemaOnly || (schemaOnly == dataOnly) {
		err := restoreSchema(config, backupName, tablePattern)
		if err != nil {
			return err
		}
	}
	if dataOnly || (schemaOnly == dataOnly) {
		err := RestoreData(config, backupName, tablePattern)
		if err != nil {
			return err
		}
	}*/

	err := FlashBackData(cfg, backupName, tablePattern)
	if err != nil {
		return err
	}
	return nil
}

// FlashBackData - restore data for tables matched by tablePattern from backupName
func FlashBackData(cfg config.Config, backupName string, tablePattern string) error {
	if backupName == "" {
		PrintLocalBackups(cfg, "all")
		return fmt.Errorf("select backup for restore")
	}

	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	allBackupTables, err := ch.GetBackupTablesLegacy(backupName)
	if err != nil {
		return err
	}

	restoreTables := parseTablePatternForRestoreData(allBackupTables, tablePattern)

	liveTables, err := ch.GetTables()

	if err != nil {
		return err
	}
	if len(restoreTables) == 0 {
		return fmt.Errorf("backup doesn't have tables to restore")
	}

	missingTables := []string{}

	for _, restoreTable := range restoreTables {
		found := false
		for _, liveTable := range liveTables {
			if (restoreTable.Database == liveTable.Database) && (restoreTable.Table == liveTable.Name) {
				found = true
				break
			}
		}
		if !found {
			missingTables = append(missingTables, fmt.Sprintf("%s.%s", restoreTable.Database, restoreTable.Table))

			for _, newtable := range missingTables {
				//log.Printf("newtable=%s", newtable)
				if err := RestoreSchema(cfg, backupName, newtable, true); err != nil {
					return err
				}
			}

			FlashBackData(cfg, backupName, tablePattern)
			return nil
		}
	}

	diffInfos, _ := ch.ComputePartitionsDelta(restoreTables, liveTables)
	for _, tableDiff := range diffInfos {

		if err := ch.CopyDataDiff(tableDiff); err != nil {
			return fmt.Errorf("can't restore '%s.%s': %v", tableDiff.BTable.Database, tableDiff.BTable.Table, err)
		}

		if err := ch.ApplyPartitionsChanges(tableDiff); err != nil {
			return fmt.Errorf("can't attach partitions for table '%s.%s': %v", tableDiff.BTable.Database, tableDiff.BTable.Table, err)
		}
	}
	return nil
}
