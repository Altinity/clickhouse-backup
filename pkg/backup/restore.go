package backup

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	apexLog "github.com/apex/log"
)

// RestoreTables - slice of RestoreTable
type RestoreTables []metadata.TableMetadata

// Sort - sorting BackupTables slice orderly by name
func (rt RestoreTables) Sort(dropTable bool) {
	sort.Slice(rt, func(i, j int) bool {
		return getOrderByEngine(rt[i].Query, dropTable) < getOrderByEngine(rt[j].Query, dropTable)
	})
}

// Restore - restore tables matched by tablePattern from backupName
func Restore(cfg *config.Config, backupName string, tablePattern string, schemaOnly bool, dataOnly bool, dropTable bool) error {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()
	defaultDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	backupMetafileLocalPath := path.Join(defaultDataPath, "backup", backupName, "metadata.json")
	backupMetadataBody, err := ioutil.ReadFile(backupMetafileLocalPath)
	if err == nil {
		backupMetadata := metadata.BackupMetadata{}
		if err := json.Unmarshal(backupMetadataBody, &backupMetadata); err != nil {
			return err
		}
		for _, database := range backupMetadata.Databases {
			if err := ch.CreateDatabaseFromQuery(database.Query); err != nil {
				return err
			}
		}
		if len(backupMetadata.Tables) == 0 {
			apexLog.Infof("'%s' is empty backup, nothing to do", backupName)
			return nil
		}
	} else if !os.IsNotExist(err) { // Legacy backups don't contain metadata.json
		return err
	}

	if schemaOnly || (schemaOnly == dataOnly) {
		if err := RestoreSchema(cfg, backupName, tablePattern, dropTable); err != nil {
			return err
		}
	}
	if dataOnly || (schemaOnly == dataOnly) {
		if err := RestoreData(cfg, backupName, tablePattern); err != nil {
			return err
		}
	}
	return nil
}

// RestoreSchema - restore schemas matched by tablePattern from backupName
func RestoreSchema(cfg *config.Config, backupName string, tablePattern string, dropTable bool) error {
	if backupName == "" {
		_ = PrintLocalBackups(cfg, "all")
		return fmt.Errorf("select backup for restore")
	}
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	defaultDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	metadataPath := path.Join(defaultDataPath, "backup", backupName, "metadata")
	info, err := os.Stat(metadataPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", metadataPath)
	}
	if tablePattern == "" {
		tablePattern = "*"
	}
	tablesForRestore, err := parseSchemaPattern(metadataPath, tablePattern, dropTable)
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("no have found schemas by %s in %s", tablePattern, backupName)
	}

	totalRetries := len(tablesForRestore)
	restoreRetries := 0
	var notRestoredTables RestoreTables
	var restoreErr error
	for restoreRetries < totalRetries {
		for _, schema := range tablesForRestore {
			// if metadata.json doesn't contains "databases", we will re-create tables with default engine
			if err = ch.CreateDatabase(schema.Database); err != nil {
				return fmt.Errorf("can't create database '%s': %v", schema.Database, err)
			}
			//materialized views should restore via ATTACH
			schema.Query = strings.Replace(
				schema.Query, "CREATE MATERIALIZED VIEW", "ATTACH MATERIALIZED VIEW", 1,
			)
			restoreErr = ch.CreateTable(clickhouse.Table{
				Database: schema.Database,
				Name:     schema.Table,
			}, schema.Query, dropTable)

			if restoreErr != nil {
				restoreRetries++
				if restoreRetries >= totalRetries {
					return fmt.Errorf(
						"can't create table `%s`.`%s`: %v after %d times, please check your schema depencncies",
						schema.Database, schema.Table, restoreErr, restoreRetries,
					)
				} else {
					apexLog.Warnf(
						"can't create table '%s.%s': %v, will try again", schema.Database, schema.Table, err,
					)
				}
				notRestoredTables = append(notRestoredTables, schema)
			}
		}
		tablesForRestore = notRestoredTables
		if len(tablesForRestore) == 0 {
			break
		}
	}
	return nil
}

// RestoreData - restore data for tables matched by tablePattern from backupName
func RestoreData(cfg *config.Config, backupName string, tablePattern string) error {
	if backupName == "" {
		_ = PrintLocalBackups(cfg, "all")
		return fmt.Errorf("select backup for restore")
	}
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "restore",
	})
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	defaulDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	if clickhouse.IsClickhouseShadow(path.Join(defaulDataPath, "backup", backupName, "shadow")) {
		return fmt.Errorf("backups created in v0.0.1 is not supported now")
	}
	backup, err := getLocalBackup(cfg, backupName)
	if err != nil {
		return fmt.Errorf("can't restore: %v", err)
	}
	var tablesForRestore RestoreTables
	if backup.Legacy {
		tablesForRestore, err = ch.GetBackupTablesLegacy(backupName)
	} else {
		metadataPath := path.Join(defaulDataPath, "backup", backupName, "metadata")
		tablesForRestore, err = parseSchemaPattern(metadataPath, tablePattern, false)
	}
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("no have found schemas by %s in %s", tablePattern, backupName)
	}
	log.Debugf("found %d tables with data in backup", len(tablesForRestore))
	chTables, err := ch.GetTables()
	if err != nil {
		return err
	}
	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	diskMap := map[string]string{}
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
	}
	for _, t := range tablesForRestore {
		for disk := range t.Parts {
			if _, ok := diskMap[disk]; !ok {
				return fmt.Errorf("table '%s.%s' require disk '%s' that not found in clickhouse, you can add nonexistent disks to disk_mapping config", t.Database, t.Table, disk)
			}
		}
	}
	dstTablesMap := map[metadata.TableTitle]clickhouse.Table{}
	for i := range chTables {
		dstTablesMap[metadata.TableTitle{
			Database: chTables[i].Database,
			Table:    chTables[i].Name,
		}] = chTables[i]
	}

	var missingTables []string
	for _, restoreTable := range tablesForRestore {
		found := false
		for _, chTable := range chTables {
			if (restoreTable.Database == chTable.Database) && (restoreTable.Table == chTable.Name) {
				found = true
				break
			}
		}
		if !found {
			missingTables = append(missingTables, fmt.Sprintf("'%s.%s'", restoreTable.Database, restoreTable.Table))
		}
	}
	if len(missingTables) > 0 {
		return fmt.Errorf("%s is not created. Restore schema first or create missing tables manually", strings.Join(missingTables, ", "))
	}

	for _, table := range tablesForRestore {
		log := log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Table))
		dstTableDataPaths := dstTablesMap[metadata.TableTitle{
			Database: table.Database,
			Table:    table.Table}].DataPaths
		if err := ch.CopyData(backupName, table, disks, dstTableDataPaths); err != nil {
			return fmt.Errorf("can't restore '%s.%s': %v", table.Database, table.Table, err)
		}
		log.Debugf("copied data to 'detached'")
		if err := ch.AttachPartitions(table, disks); err != nil {
			return fmt.Errorf("can't attach partitions for table '%s.%s': %v", table.Database, table.Table, err)
		}
		log.Debugf("attached parts")
		log.Info("done")
	}
	log.Info("done")
	return nil
}
