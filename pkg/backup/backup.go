package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/pkg/storage"

	"github.com/apex/log"
)

const (
	// BackupTimeFormat - default backup name format
	BackupTimeFormat = "2006-01-02T15-04-05"
	hashfile         = "parts.hash"
	MetaFileName     = "metadata.json"
)

var (
	// ErrUnknownClickhouseDataPath -
	ErrUnknownClickhouseDataPath = errors.New("clickhouse data path is unknown, you can set data_path in config file")
)

// RestoreTables - slice of RestoreTable
type RestoreTables []metadata.TableMetadata

// Sort - sorting BackupTables slice orderly by name
func (rt RestoreTables) Sort() {
	sort.Slice(rt, func(i, j int) bool {
		if getOrderByEngine(rt[i].Table) < getOrderByEngine(rt[j].Table) {
			return true
		}
		return (rt[i].Database < rt[j].Database) || (rt[i].Database == rt[j].Database && rt[i].Table < rt[j].Table)
	})
}

func addTable(tables []clickhouse.Table, table clickhouse.Table) []clickhouse.Table {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Name == table.Name) {
			return tables
		}
	}
	return append(tables, table)
}

func addBackupTable(tables clickhouse.BackupTables, table metadata.TableMetadata) clickhouse.BackupTables {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Table == table.Table) {
			return tables
		}
	}
	return append(tables, table)
}

func addRestoreTable(tables RestoreTables, table metadata.TableMetadata) RestoreTables {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Table == table.Table) {
			return tables
		}
	}
	return append(tables, table)
}

func filterTablesByPattern(tables []clickhouse.Table, tablePattern string) []clickhouse.Table {
	if tablePattern == "" {
		return tables
	}
	tablePatterns := strings.Split(tablePattern, ",")
	var result []clickhouse.Table
	for _, t := range tables {
		for _, pattern := range tablePatterns {
			if matched, _ := filepath.Match(pattern, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				result = addTable(result, t)
			}
		}
	}
	return result
}

func RestoreSchema(cfg config.Config, backupName string, tablePattern string, dropTable bool) error {
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

	defaulDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}

	metadataPath := path.Join(defaulDataPath, "backup", backupName, "metadata")
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
	tablesForRestore, err := parseSchemaPattern(metadataPath, tablePattern)
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("no have found schemas by %s in %s", tablePattern, backupName)
	}

	for _, schema := range tablesForRestore {
		if err := ch.CreateDatabase(schema.Database); err != nil {
			return fmt.Errorf("can't create database '%s': %v", schema.Database, err)
		}
		if err := ch.CreateTable(clickhouse.Table{
			Database: schema.Database,
			Name:     schema.Table,
		}, schema.Query, dropTable); err != nil {
			return fmt.Errorf("can't create table '%s.%s': %v", schema.Database, schema.Table, err)
		}
	}
	return nil
}

// Freeze - freeze tables by tablePattern
func Freeze(cfg config.Config, tablePattern string) error {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	for _, disk := range disks {
		shadowPath := filepath.Join(disk.Path, "shadow")
		files, err := ioutil.ReadDir(shadowPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("can't read '%s': %v", shadowPath, err)
			}
		}
		if len(files) > 0 {
			return fmt.Errorf("'%s' is not empty, execute 'clean' command first", shadowPath)
		}
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
		if err := ch.FreezeTable(&table); err != nil {
			return err
		}
	}
	return nil
}

// NewBackupName - return default backup name
func NewBackupName() string {
	return time.Now().UTC().Format(BackupTimeFormat)
}

// CreateBackup - create new backup of all tables matched by tablePattern
// If backupName is empty string will use default backup name
func CreateBackup(cfg config.Config, backupName, tablePattern string) error {
	if backupName == "" {
		backupName = NewBackupName()
	}
	ctx := log.WithFields(log.Fields{
		"backup":    backupName,
		"operation": "create",
	})
	log.SetLevel(log.DebugLevel)
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	allTables, err := ch.GetTables()
	if err != nil {
		return fmt.Errorf("cat't get tables from clickhouse: %v", err)
	}
	tables := filterTablesByPattern(allTables, tablePattern)
	i := 0
	for _, table := range tables {
		if table.Skip {
			continue
		}
		i++
	}
	if i == 0 {
		return fmt.Errorf("no tables for backup")
	}

	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	for _, disk := range disks {
		if err := ch.Mkdir(path.Join(disk.Path, "backup")); err != nil {
			return err
		}
		backupPath := path.Join(disk.Path, "backup", backupName)
		if _, err := os.Stat(backupPath); err == nil || !os.IsNotExist(err) {
			return fmt.Errorf("'%s' already exists", backupPath)
		}
	}

	for _, table := range tables {
		if table.Skip {
			continue
		}
		ctx.Infof("%s.%s", table.Database, table.Name)
		if err := AddTableToBackup(ch, backupName, &table); err != nil {
			ctx.Errorf("error=\"%v\"", err)
			continue
		}
	}

	if err := RemoveOldBackupsLocal(cfg); err != nil {
		return err
	}
	ctx.Info("done")
	return nil
}

func AddTableToBackup(ch *clickhouse.ClickHouse, backupName string, table *clickhouse.Table) error {
	ctx := log.WithFields(log.Fields{
		"backup":    backupName,
		"operation": "create",
		"table":     fmt.Sprintf("%s.%s", table.Database, table.Name),
	})
	if backupName == "" {
		return fmt.Errorf("backupName is not defined")
	}
	defaultPath, err := ch.GetDefaultPath()
	if err != nil {
		return fmt.Errorf("can't get default data path: %v", err)
	}
	diskList, err := ch.GetDisks()
	if err != nil {
		return fmt.Errorf("can't get clickhouse disk list: %v", err)
	}
	relevantBackupPath := path.Join("backup", backupName)

	diskPathList := []string{defaultPath}
	for _, dataPath := range table.DataPaths {
		for _, disk := range diskList {
			if disk.Path == defaultPath {
				continue
			}
			if strings.HasPrefix(dataPath, disk.Path) {
				diskPathList = append(diskPathList, disk.Path)
				break
			}
		}
	}

	for _, diskPath := range diskPathList {
		backupPath := path.Join(diskPath, relevantBackupPath)
		if err := ch.Mkdir(backupPath); err != nil {
			return err
		}
	}
	ctx.Debug("create metadata")
	backupPath := path.Join(defaultPath, "backup", backupName)
	if err := createMetadata(ch, backupPath, table); err != nil {
		return err
	}
	// backup data
	if !strings.HasSuffix(table.Engine, "MergeTree") {
		return nil
	}
	ctx.Debug("freeze")
	if err := ch.FreezeTable(table); err != nil {
		for _, diskPath := range diskPathList {
			// Remove failed backup
			os.RemoveAll(path.Join(diskPath, relevantBackupPath))
		}
		return err
	}

	// log.Printf("Copy part hashes")
	// if err := CopyPartHashes(cfg, tablePattern, backupName); err != nil {
	// 	log.Println(err)
	// }

	// log.Println("Copy metadata")
	// schemaList, err := parseSchemaPattern(path.Join(dataPath, "metadata"), tablePattern)
	// if err != nil {
	// 	return err
	// }
	// for _, schema := range schemaList {
	// 	skip := false
	// 	for _, filter := range cfg.ClickHouse.SkipTables {
	// 		if matched, _ := filepath.Match(filter, fmt.Sprintf("%s.%s", schema.Database, schema.Table)); matched {
	// 			skip = true
	// 			break
	// 		}
	// 	}
	// 	if skip {
	// 		continue
	// 	}
	// 	relativePath := strings.Trim(strings.TrimPrefix(schema.Path, path.Join(dataPath, "metadata")), "/")
	// 	newPath := path.Join(backupPath, "metadata", relativePath)
	// 	if err := copyFile(schema.Path, newPath); err != nil {
	// 		return fmt.Errorf("can't backup metadata: %v", err)
	// 	}
	// }
	// log.Println("  Done.")

	ctx.Debug("move shadow")
	for _, diskPath := range diskPathList {
		// backupPath := path.Join(diskPath, "backup", backupName, table.Database, table.Name)
		backupPath := path.Join(diskPath, "backup", backupName)

		shadowPath := path.Join(diskPath, "shadow")
		backupShadowPath := path.Join(backupPath, "shadow")
		if err := moveShadow(shadowPath, backupShadowPath); err != nil {
			return err
		}
	}
	// if err := RemoveOldBackupsLocal(cfg); err != nil {
	// 	return err
	// }
	return nil
}

func createMetadata(ch *clickhouse.ClickHouse, backupPath string, table *clickhouse.Table) error {
	diskList, err := ch.GetDisks()
	if err != nil {
		return fmt.Errorf("can't get clickhouse disk list: %v", err)
	}
	diskMap := map[string]string{}
	for _, disk := range diskList {
		diskMap[disk.Name] = disk.Path
	}
	parts, err := ch.GetPartitions(*table)
	if err != nil {
		return err
	}
	metadata := &metadata.TableMetadata{
		Table:      table.Name,
		Database:   table.Database,
		Query:      table.CreateTableQuery,
		Disks:      clickhouse.GetDisksByPaths(diskList,table.DataPaths),
		UUID:       table.UUID,
		TotalBytes: *table.TotalBytes,
		Parts:      parts,
	}
	metadataPath := path.Join(backupPath, "metadata")
	if err := ch.Mkdir(metadataPath); err != nil {
		return err
	}
	metadataDatabasePath := path.Join(metadataPath, table.Database)
	if err := ch.Mkdir(metadataDatabasePath); err != nil {
		return err
	}
	metadataFile := path.Join(metadataDatabasePath, fmt.Sprintf("%s.json", table.Name))
	metadataBody, err := json.MarshalIndent(metadata, "", " ")
	if err != nil {
		return fmt.Errorf("can't marshal %s: %v", MetaFileName, err)
	}
	if err := ioutil.WriteFile(metadataFile, metadataBody, 0644); err != nil {
		return fmt.Errorf("can't create %s: %v", MetaFileName, err)
	}
	if err := ch.Chown(metadataFile); err != nil {
		return err
	}
	return nil
}

// Restore - restore tables matched by tablePattern from backupName
func Restore(cfg config.Config, backupName string, tablePattern string, schemaOnly bool, dataOnly bool, dropTable bool) error {
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

// RestoreData - restore data for tables matched by tablePattern from backupName
func RestoreData(cfg config.Config, backupName string, tablePattern string) error {
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

	defaulDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}

	metadataPath := path.Join(defaulDataPath, "backup", backupName, "metadata")
	tablesForRestore, err := parseSchemaPattern(metadataPath, tablePattern)
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("no have found schemas by %s in %s", tablePattern, backupName)
	}

	// allBackupTables, err := ch.GetBackupTables(backupName)
	// if err != nil {
	// 	return err
	// }
	// restoreTables := parseTablePatternForRestoreData(allBackupTables, tablePattern)
	chTables, err := ch.GetTables()
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("backup doesn't have tables to restore")
	}
	missingTables := []string{}
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
	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	for _, table := range tablesForRestore {
		if err := ch.CopyData(backupName, table, disks); err != nil {
			return fmt.Errorf("can't restore '%s.%s': %v", table.Database, table.Table, err)
		}
		if err := ch.AttachPartitions(table, disks); err != nil {
			return fmt.Errorf("can't attach partitions for table '%s.%s': %v", table.Database, table.Table, err)
		}
	}
	return nil
}

func Upload(cfg config.Config, backupName string, diffFrom string) error {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("Upload aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		PrintLocalBackups(cfg, "all")
		return fmt.Errorf("select backup for upload")
	}
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	dataPath, err := ch.GetDefaultPath()
	if err != nil {
		return err
	}

	bd, err := storage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}
	if err := bd.Connect(); err != nil {
		return fmt.Errorf("can't connect to %s: %v", bd.Kind(), err)
	}

	if err := GetLocalBackup(cfg, backupName); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}
	backupPath := path.Join(dataPath, "backup", backupName)
	log.Infof("Upload backup '%s'", backupName)
	diffFromPath := ""
	if diffFrom != "" {
		diffFromPath = path.Join(dataPath, "backup", diffFrom)
	}
	if clickhouse.IsClickhouseShadow(filepath.Join(diffFromPath, "shadow")) {
		return fmt.Errorf("'%s' is old format backup and doesn't supports diff", filepath.Base(diffFromPath))
	}
	if err := bd.CompressedStreamUpload(backupPath, backupName, diffFromPath); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}
	if err := bd.RemoveOldBackups(bd.BackupsToKeep()); err != nil {
		return fmt.Errorf("can't remove old backups: %v", err)
	}
	log.Infof("  Done.")
	return nil
}

func Download(cfg config.Config, backupName string) error {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("Download aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		PrintRemoteBackups(cfg, "all")
		return fmt.Errorf("select backup for download")
	}

	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()
	dataPath, err := ch.GetDefaultPath()
	if err != nil {
		return err
	}
	bd, err := storage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}

	if err := bd.Connect(); err != nil {
		return err
	}
	if err := bd.CompressedStreamDownload(backupName,
		path.Join(dataPath, "backup", backupName)); err != nil {
		return err
	}
	log.Info("  Done.")
	return nil
}

// Clean - removed all data in shadow folder
func Clean(cfg config.Config) error {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	for _, disk := range disks {
		shadowDir := path.Join(disk.Path, "shadow")
		log.Infof("Clean %s", shadowDir)
		if err := cleanDir(shadowDir); err != nil {
			return fmt.Errorf("can't clean '%s': %v", shadowDir, err)
		}
	}
	return nil
}

//
func RemoveOldBackupsLocal(cfg config.Config) error {
	if cfg.General.BackupsToKeepLocal < 1 {
		return nil
	}
	backupList, err := ListLocalBackups(cfg)
	if err != nil {
		return err
	}
	backupsToDelete := storage.GetBackupsToDelete(backupList, cfg.General.BackupsToKeepLocal)
	for _, backup := range backupsToDelete {
		if err := RemoveBackupLocal(cfg, backup.Name); err != nil {
			return err
		}
	}
	return nil
}

func RemoveBackupLocal(cfg config.Config, backupName string) error {
	backupList, err := ListLocalBackups(cfg)
	if err != nil {
		return err
	}
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}

	for _, backup := range backupList {
		if backup.Name == backupName {
			for _, disk := range disks {
				err := os.RemoveAll(path.Join(disk.Path, "backup", backupName))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func RemoveBackupRemote(cfg config.Config, backupName string) error {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("RemoveBackupRemote aborted: RemoteStorage set to \"none\"")
		return nil
	}

	bd, err := storage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}
	err = bd.Connect()
	if err != nil {
		return fmt.Errorf("can't connect to remote storage: %v", err)
	}
	backupList, err := bd.BackupList()
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.Name == backupName {
			return bd.RemoveBackup(backupName)
		}
	}
	return fmt.Errorf("backup '%s' not found on remote storage", backupName)
}
