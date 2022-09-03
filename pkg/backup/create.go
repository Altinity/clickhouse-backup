package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/common"
	"github.com/AlexAkulov/clickhouse-backup/pkg/filesystemhelper"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/pkg/utils"
	apexLog "github.com/apex/log"
	"github.com/google/uuid"
	recursiveCopy "github.com/otiai10/copy"
)

const (
	// TimeFormatForBackup - default backup name format
	TimeFormatForBackup = "2006-01-02T15-04-05"
	MetaFileName        = "metadata.json"
)

var (
	// ErrUnknownClickhouseDataPath -
	ErrUnknownClickhouseDataPath = errors.New("clickhouse data path is unknown, you can set data_path in config file")
)

type BackupLocal struct {
	metadata.BackupMetadata
	Legacy bool
	Broken string
}

func addTable(tables []clickhouse.Table, table clickhouse.Table) []clickhouse.Table {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Name == table.Name) {
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
			tableName := fmt.Sprintf("%s.%s", t.Database, t.Name)
			if matched, _ := filepath.Match(strings.Trim(pattern, " \t\n\r"), tableName); matched {
				result = addTable(result, t)
			} else {
				apexLog.Debugf("%s not matched with %s", tableName, pattern)
			}
		}
	}
	return result
}

// NewBackupName - return default backup name
func NewBackupName() string {
	return time.Now().UTC().Format(TimeFormatForBackup)
}

// CreateBackup - create new backup of all tables matched by tablePattern
// If backupName is empty string will use default backup name
func CreateBackup(cfg *config.Config, backupName, tablePattern string, partitions []string, schemaOnly, rbacOnly, configsOnly bool, version string) error {

	startBackup := time.Now()
	doBackupData := !schemaOnly
	if backupName == "" {
		backupName = NewBackupName()
	}
	backupName = CleanBackupNameRE.ReplaceAllString(backupName, "")
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "create",
	})
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	allDatabases, err := ch.GetDatabases()
	if err != nil {
		return fmt.Errorf("can't get database engines from clickhouse: %v", err)
	}
	allTables, err := ch.GetTables(tablePattern)
	if err != nil {
		return fmt.Errorf("can't get tables from clickhouse: %v", err)
	}
	tables := filterTablesByPattern(allTables, tablePattern)
	i := 0
	for _, table := range tables {
		if table.Skip {
			continue
		}
		i++
	}
	if i == 0 && !cfg.General.AllowEmptyBackups {
		return fmt.Errorf("no tables for backup")
	}

	allFunctions, err := ch.GetUserDefinedFunctions()
	if err != nil {
		return fmt.Errorf("GetUserDefinedFunctions return error: %v", err)
	}

	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}

	diskMap := map[string]string{}
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
	}
	partitionsToBackupMap, partitions := filesystemhelper.CreatePartitionsToBackupMap(partitions)
	// create
	if cfg.ClickHouse.UseEmbeddedBackupRestore {
		err = createBackupEmbedded(cfg, ch, backupName, tablePattern, partitions, partitionsToBackupMap, schemaOnly, rbacOnly, configsOnly, tables, allDatabases, allFunctions, disks, diskMap, log, startBackup, version)
	} else {
		err = createBackupLocal(cfg, ch, backupName, partitionsToBackupMap, tables, doBackupData, schemaOnly, rbacOnly, configsOnly, version, disks, diskMap, allDatabases, allFunctions, log, startBackup)
	}
	if err != nil {
		return err
	}

	// Clean
	if err := RemoveOldBackupsLocal(cfg, true, disks); err != nil {
		return err
	}
	return nil
}

func createBackupLocal(cfg *config.Config, ch *clickhouse.ClickHouse, backupName string, partitionsToBackupMap common.EmptyMap, tables []clickhouse.Table, doBackupData bool, schemaOnly bool, rbacOnly bool, configsOnly bool, version string, disks []clickhouse.Disk, diskMap map[string]string, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, log *apexLog.Entry, startBackup time.Time) error {
	// Create backup dir on all clickhouse disks
	for _, disk := range disks {
		if err := filesystemhelper.Mkdir(path.Join(disk.Path, "backup"), ch, disks); err != nil {
			return err
		}
	}
	defaultPath, err := ch.GetDefaultPath(disks)
	if err != nil {
		return err
	}
	backupPath := path.Join(defaultPath, "backup", backupName)
	if _, err := os.Stat(path.Join(backupPath, "metadata.json")); err == nil || !os.IsNotExist(err) {
		return fmt.Errorf("'%s' medatata.json already exists", backupName)
	}
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		if err = filesystemhelper.Mkdir(backupPath, ch, disks); err != nil {
			log.Errorf("can't create directory %s: %v", backupPath, err)
			return err
		}
	}
	var backupDataSize, backupMetadataSize uint64

	var tableMetas []metadata.TableTitle
	for _, table := range tables {
		log := log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Name))
		if table.Skip {
			continue
		}
		var realSize map[string]int64
		var disksToPartsMap map[string][]metadata.Part
		if doBackupData {
			log.Debug("create data")
			shadowBackupUUID := strings.ReplaceAll(uuid.New().String(), "-", "")
			disksToPartsMap, realSize, err = AddTableToBackup(ch, backupName, shadowBackupUUID, disks, &table, partitionsToBackupMap)
			if err != nil {
				log.Error(err.Error())
				if removeBackupErr := RemoveBackupLocal(cfg, backupName, disks); removeBackupErr != nil {
					log.Error(removeBackupErr.Error())
				}
				// fix corner cases after https://github.com/AlexAkulov/clickhouse-backup/issues/379
				if cleanShadowErr := Clean(cfg); cleanShadowErr != nil {
					log.Error(cleanShadowErr.Error())
				}
				return err
			}
			// more precise data size calculation
			for _, size := range realSize {
				backupDataSize += uint64(size)
			}
		}
		log.Debug("create metadata")
		metadataSize, err := createTableMetadata(ch, path.Join(backupPath, "metadata"), metadata.TableMetadata{
			Table:        table.Name,
			Database:     table.Database,
			Query:        table.CreateTableQuery,
			TotalBytes:   table.TotalBytes,
			Size:         realSize,
			Parts:        disksToPartsMap,
			MetadataOnly: schemaOnly,
		}, disks)
		if err != nil {
			if removeBackupErr := RemoveBackupLocal(cfg, backupName, disks); removeBackupErr != nil {
				log.Error(removeBackupErr.Error())
			}
			return err
		}
		backupMetadataSize += metadataSize
		tableMetas = append(tableMetas, metadata.TableTitle{
			Database: table.Database,
			Table:    table.Name,
		})
		log.Infof("done")
	}
	backupRBACSize, backupConfigSize := uint64(0), uint64(0)

	if rbacOnly {
		if backupRBACSize, err = createRBACBackup(ch, backupPath, disks); err != nil {
			log.Errorf("error during do RBAC backup: %v", err)
		} else {
			log.WithField("size", utils.FormatBytes(backupRBACSize)).Info("done createRBACBackup")
		}
	}
	if configsOnly {
		if backupConfigSize, err = createConfigBackup(cfg, backupPath); err != nil {
			log.Errorf("error during do CONFIG backup: %v", err)
		} else {
			log.WithField("size", utils.FormatBytes(backupConfigSize)).Info("done createConfigBackup")
		}
	}

	backupMetaFile := path.Join(defaultPath, "backup", backupName, "metadata.json")
	if err := createBackupMetadata(cfg, ch, backupMetaFile, backupName, version, "regular", diskMap, disks, backupDataSize, backupMetadataSize, backupRBACSize, backupConfigSize, tableMetas, allDatabases, allFunctions, log); err != nil {
		return err
	}
	log.WithField("duration", utils.HumanizeDuration(time.Since(startBackup))).Info("done")
	return nil
}

func createBackupEmbedded(cfg *config.Config, ch *clickhouse.ClickHouse, backupName, tablePattern string, partitions []string, partitionsToBackupMap common.EmptyMap, schemaOnly, rbacOnly, configsOnly bool, tables []clickhouse.Table, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, disks []clickhouse.Disk, diskMap map[string]string, log *apexLog.Entry, startBackup time.Time, backupVersion string) error {
	if _, isBackupDiskExists := diskMap[cfg.ClickHouse.EmbeddedBackupDisk]; !isBackupDiskExists {
		return fmt.Errorf("backup disk `%s` not exists in system.disks", cfg.ClickHouse.EmbeddedBackupDisk)
	}
	if rbacOnly || configsOnly {
		return fmt.Errorf("`use_embedded_backup_restore: true` doesn't support --rbac, --configs parameters")
	}
	l := 0
	for _, table := range tables {
		if !table.Skip {
			l += 1
		}
	}
	if l == 0 {
		return fmt.Errorf("`use_embedded_backup_restore: true` allow doesn't empty backups, check your parameter --tables=%v", tablePattern)
	}
	tableMetas := make([]metadata.TableTitle, l)
	tablesSQL := ""
	tableSizeSQL := ""
	i := 0
	backupMetadataSize := uint64(0)
	backupPath := path.Join(diskMap[cfg.ClickHouse.EmbeddedBackupDisk], backupName)
	for _, table := range tables {
		if table.Skip {
			continue
		}
		tableMetas[i] = metadata.TableTitle{
			Database: table.Database,
			Table:    table.Name,
		}
		i += 1

		tablesSQL += "TABLE `" + table.Database + "`.`" + table.Name + "`"
		tableSizeSQL += "'" + table.Database + "." + table.Name + "'"
		if len(partitions) > 0 {
			tablesSQL += fmt.Sprintf(" PARTITIONS '%s'", strings.Join(partitions, "','"))
		}
		if i < l {
			tablesSQL += ", "
			tableSizeSQL += ", "
		}
	}
	backupDataSize := make([]uint64, 0)
	chVersion, err := ch.GetVersion()
	if err != nil {
		return err
	}
	if !schemaOnly {
		backupSizeSQL := fmt.Sprintf("SELECT sum(bytes_on_disk) AS backup_data_size FROM system.parts WHERE active AND concat(database,'.',table) IN (%s)", tableSizeSQL)
		if chVersion >= 20005000 {
			backupSizeSQL = fmt.Sprintf("SELECT sum(total_bytes) AS backup_data_size FROM system.tables WHERE concat(database,'.',name) IN (%s)", tableSizeSQL)
		}
		if err := ch.Select(&backupDataSize, backupSizeSQL); err != nil {
			return err
		}
	} else {
		backupDataSize = append(backupDataSize, 0)
	}
	backupSQL := fmt.Sprintf("BACKUP %s TO Disk(?,?)", tablesSQL)
	if schemaOnly {
		backupSQL += " SETTINGS structure_only=true"
	}
	backupResult := make([]clickhouse.SystemBackups, 0)
	if err := ch.Select(&backupResult, backupSQL, cfg.ClickHouse.EmbeddedBackupDisk, backupName); err != nil {
		return fmt.Errorf("backup error: %v", err)
	}
	if len(backupResult) != 1 || backupResult[0].Status != "BACKUP_COMPLETE" {
		return fmt.Errorf("backup return wrong results: %v", backupResult)
	}

	log.Debug("calculate parts list from embedded backup disk")
	for _, table := range tables {
		if table.Skip {
			continue
		}
		disksToPartsMap, err := getPartsFromBackupDisk(cfg, backupPath, table, partitionsToBackupMap)
		if err != nil {
			if removeBackupErr := RemoveBackupLocal(cfg, backupName, disks); removeBackupErr != nil {
				log.Error(removeBackupErr.Error())
			}
			return err
		}
		metadataSize, err := createTableMetadata(ch, path.Join(backupPath, "metadata"), metadata.TableMetadata{
			Table:        table.Name,
			Database:     table.Database,
			Query:        table.CreateTableQuery,
			TotalBytes:   table.TotalBytes,
			Size:         map[string]int64{cfg.ClickHouse.EmbeddedBackupDisk: 0},
			Parts:        disksToPartsMap,
			MetadataOnly: schemaOnly,
		}, disks)
		if err != nil {
			if removeBackupErr := RemoveBackupLocal(cfg, backupName, disks); removeBackupErr != nil {
				log.Error(removeBackupErr.Error())
			}
			return err
		}
		backupMetadataSize += metadataSize
	}
	backupMetaFile := path.Join(diskMap[cfg.ClickHouse.EmbeddedBackupDisk], backupName, "metadata.json")
	if err := createBackupMetadata(cfg, ch, backupMetaFile, backupName, backupVersion, "embedded", diskMap, disks, backupDataSize[0], backupMetadataSize, 0, 0, tableMetas, allDatabases, allFunctions, log); err != nil {
		return err
	}

	log.WithFields(apexLog.Fields{
		"operation": "create_embedded",
		"duration":  utils.HumanizeDuration(time.Since(startBackup)),
	}).Info("done")

	return nil
}

func getPartsFromBackupDisk(cfg *config.Config, backupPath string, table clickhouse.Table, partitionsToBackupMap common.EmptyMap) (map[string][]metadata.Part, error) {
	parts := map[string][]metadata.Part{}
	dirList, err := os.ReadDir(path.Join(backupPath, "data", common.TablePathEncode(table.Database), common.TablePathEncode(table.Name)))
	if err != nil {
		if os.IsNotExist(err) {
			return parts, nil
		}
		return nil, err
	}
	if len(partitionsToBackupMap) == 0 {
		parts[cfg.ClickHouse.EmbeddedBackupDisk] = make([]metadata.Part, len(dirList))
		for i, d := range dirList {
			parts[cfg.ClickHouse.EmbeddedBackupDisk][i] = metadata.Part{
				Name: d.Name(),
			}
		}
	} else {
		parts[cfg.ClickHouse.EmbeddedBackupDisk] = make([]metadata.Part, 0)
		for _, d := range dirList {
			found := false
			for prefix := range partitionsToBackupMap {
				if strings.HasPrefix(d.Name(), prefix+"_") {
					found = true
					break
				}
			}
			if found {
				parts[cfg.ClickHouse.EmbeddedBackupDisk] = append(parts[cfg.ClickHouse.EmbeddedBackupDisk], metadata.Part{
					Name: d.Name(),
				})
			}
		}
	}
	return parts, nil
}

func createConfigBackup(cfg *config.Config, backupPath string) (uint64, error) {
	backupConfigSize := uint64(0)
	configBackupPath := path.Join(backupPath, "configs")
	apexLog.Debugf("copy %s -> %s", cfg.ClickHouse.ConfigDir, configBackupPath)
	copyErr := recursiveCopy.Copy(cfg.ClickHouse.ConfigDir, configBackupPath, recursiveCopy.Options{
		Skip: func(src string) (bool, error) {
			if fileInfo, err := os.Stat(src); err == nil {
				backupConfigSize += uint64(fileInfo.Size())
			}
			return false, nil
		},
	})
	return backupConfigSize, copyErr
}

func createRBACBackup(ch *clickhouse.ClickHouse, backupPath string, disks []clickhouse.Disk) (uint64, error) {
	rbacDataSize := uint64(0)
	rbacBackup := path.Join(backupPath, "access")
	accessPath, err := ch.GetAccessManagementPath(disks)
	if err != nil {
		return 0, err
	}
	apexLog.Debugf("copy %s -> %s", accessPath, rbacBackup)
	copyErr := recursiveCopy.Copy(accessPath, rbacBackup, recursiveCopy.Options{
		Skip: func(src string) (bool, error) {
			if fileInfo, err := os.Stat(src); err == nil {
				rbacDataSize += uint64(fileInfo.Size())
			}
			return false, nil
		},
	})
	return rbacDataSize, copyErr
}

func AddTableToBackup(ch *clickhouse.ClickHouse, backupName, shadowBackupUUID string, diskList []clickhouse.Disk, table *clickhouse.Table, partitionsToBackupMap common.EmptyMap) (map[string][]metadata.Part, map[string]int64, error) {
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "create",
		"table":     fmt.Sprintf("%s.%s", table.Database, table.Name),
	})
	if backupName == "" {
		return nil, nil, fmt.Errorf("backupName is not defined")
	}

	// backup data
	if !strings.HasSuffix(table.Engine, "MergeTree") && table.Engine != "MaterializedMySQL" && table.Engine != "MaterializedPostgreSQL" {
		log.WithField("engine", table.Engine).Debug("skip table backup")
		return nil, nil, nil
	}
	if err := ch.FreezeTable(table, shadowBackupUUID); err != nil {
		return nil, nil, err
	}
	log.Debug("freezed")
	realSize := map[string]int64{}
	disksToPartsMap := map[string][]metadata.Part{}
	for _, disk := range diskList {
		shadowPath := path.Join(disk.Path, "shadow", shadowBackupUUID)
		if _, err := os.Stat(shadowPath); err != nil && os.IsNotExist(err) {
			continue
		}
		backupPath := path.Join(disk.Path, "backup", backupName)
		encodedTablePath := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Name))
		backupShadowPath := path.Join(backupPath, "shadow", encodedTablePath, disk.Name)
		if err := filesystemhelper.MkdirAll(backupShadowPath, ch, diskList); err != nil && !os.IsExist(err) {
			return nil, nil, err
		}
		// If partitionsToBackupMap is not empty, only parts in this partition will back up.
		parts, size, err := filesystemhelper.MoveShadow(shadowPath, backupShadowPath, partitionsToBackupMap)
		if err != nil {
			return nil, nil, err
		}
		realSize[disk.Name] = size
		disksToPartsMap[disk.Name] = parts
		log.WithField("disk", disk.Name).Debug("shadow moved")

		// Clean all the files under the shadowPath.
		if err := os.RemoveAll(shadowPath); err != nil {
			return disksToPartsMap, realSize, err
		}
	}
	log.Debug("done")
	return disksToPartsMap, realSize, nil
}

func createBackupMetadata(cfg *config.Config, ch *clickhouse.ClickHouse, backupMetaFile, backupName, version, tags string, diskMap map[string]string, disks []clickhouse.Disk, backupDataSize, backupMetadataSize, backupRBACSize, backupConfigSize uint64, tableMetas []metadata.TableTitle, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, log *apexLog.Entry) error {
	backupMetadata := metadata.BackupMetadata{
		BackupName:              backupName,
		Disks:                   diskMap,
		ClickhouseBackupVersion: version,
		CreationDate:            time.Now().UTC(),
		Tags:                    tags,
		ClickHouseVersion:       ch.GetVersionDescribe(),
		DataSize:                backupDataSize,
		MetadataSize:            backupMetadataSize,
		RBACSize:                backupRBACSize,
		ConfigSize:              backupConfigSize,
		Tables:                  tableMetas,
		Databases:               []metadata.DatabasesMeta{},
		Functions:               []metadata.FunctionsMeta{},
	}
	for _, database := range allDatabases {
		backupMetadata.Databases = append(backupMetadata.Databases, metadata.DatabasesMeta(database))
	}
	for _, function := range allFunctions {
		backupMetadata.Functions = append(backupMetadata.Functions, metadata.FunctionsMeta(function))
	}
	content, err := json.MarshalIndent(&backupMetadata, "", "\t")
	if err != nil {
		_ = RemoveBackupLocal(cfg, backupName, disks)
		return fmt.Errorf("can't marshal backup metafile json: %v", err)
	}
	if err := os.WriteFile(backupMetaFile, content, 0640); err != nil {
		_ = RemoveBackupLocal(cfg, backupName, disks)
		return err
	}
	if err := filesystemhelper.Chown(backupMetaFile, ch, disks, false); err != nil {
		log.Warnf("can't chown %s: %v", backupMetaFile, err)
	}
	return nil
}

func createTableMetadata(ch *clickhouse.ClickHouse, metadataPath string, table metadata.TableMetadata, disks []clickhouse.Disk) (uint64, error) {
	if err := filesystemhelper.Mkdir(metadataPath, ch, disks); err != nil {
		return 0, err
	}
	metadataDatabasePath := path.Join(metadataPath, common.TablePathEncode(table.Database))
	if err := filesystemhelper.Mkdir(metadataDatabasePath, ch, disks); err != nil {
		return 0, err
	}
	metadataFile := path.Join(metadataDatabasePath, fmt.Sprintf("%s.json", common.TablePathEncode(table.Table)))
	metadataBody, err := json.MarshalIndent(&table, "", " ")
	if err != nil {
		return 0, fmt.Errorf("can't marshal %s: %v", MetaFileName, err)
	}
	if err := os.WriteFile(metadataFile, metadataBody, 0644); err != nil {
		return 0, fmt.Errorf("can't create %s: %v", MetaFileName, err)
	}
	if err := filesystemhelper.Chown(metadataFile, ch, disks, false); err != nil {
		return 0, err
	}
	return uint64(len(metadataBody)), nil
}
