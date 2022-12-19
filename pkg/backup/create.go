package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/status"
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

type LocalBackup struct {
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
	log := apexLog.WithField("logger", "filterTablesByPattern")
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
				log.Debugf("%s not matched with %s", tableName, pattern)
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
func (b *Backuper) CreateBackup(backupName, tablePattern string, partitions []string, schemaOnly, rbacOnly, configsOnly bool, version string, commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	startBackup := time.Now()
	doBackupData := !schemaOnly
	if backupName == "" {
		backupName = NewBackupName()
	}
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	log := b.log.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "create",
	})
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	allDatabases, err := b.ch.GetDatabases(ctx, b.cfg, tablePattern)
	if err != nil {
		return fmt.Errorf("can't get database engines from clickhouse: %v", err)
	}
	allTables, err := b.ch.GetTables(ctx, tablePattern)
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
	if i == 0 && !b.cfg.General.AllowEmptyBackups {
		return fmt.Errorf("no tables for backup")
	}

	allFunctions, err := b.ch.GetUserDefinedFunctions(ctx)
	if err != nil {
		return fmt.Errorf("GetUserDefinedFunctions return error: %v", err)
	}

	disks, err := b.ch.GetDisks(ctx)
	if err != nil {
		return err
	}

	diskMap := map[string]string{}
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
	}
	partitionsToBackupMap, partitions := filesystemhelper.CreatePartitionsToBackupMap(partitions)
	// create
	if b.cfg.ClickHouse.UseEmbeddedBackupRestore {
		err = b.createBackupEmbedded(ctx, backupName, tablePattern, partitions, partitionsToBackupMap, schemaOnly, rbacOnly, configsOnly, tables, allDatabases, allFunctions, disks, diskMap, log, startBackup, version)
	} else {
		err = b.createBackupLocal(ctx, backupName, partitionsToBackupMap, tables, doBackupData, schemaOnly, rbacOnly, configsOnly, version, disks, diskMap, allDatabases, allFunctions, log, startBackup)
	}
	if err != nil {
		return err
	}

	// Clean
	if err := b.RemoveOldBackupsLocal(ctx, true, disks); err != nil {
		return err
	}
	return nil
}

func (b *Backuper) createBackupLocal(ctx context.Context, backupName string, partitionsToBackupMap common.EmptyMap, tables []clickhouse.Table, doBackupData bool, schemaOnly bool, rbacOnly bool, configsOnly bool, version string, disks []clickhouse.Disk, diskMap map[string]string, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, log *apexLog.Entry, startBackup time.Time) error {
	// Create backup dir on all clickhouse disks
	for _, disk := range disks {
		if err := filesystemhelper.Mkdir(path.Join(disk.Path, "backup"), b.ch, disks); err != nil {
			return err
		}
	}
	defaultPath, err := b.ch.GetDefaultPath(disks)
	if err != nil {
		return err
	}
	backupPath := path.Join(defaultPath, "backup", backupName)
	if _, err := os.Stat(path.Join(backupPath, "metadata.json")); err == nil || !os.IsNotExist(err) {
		return fmt.Errorf("'%s' medatata.json already exists", backupName)
	}
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		if err = filesystemhelper.Mkdir(backupPath, b.ch, disks); err != nil {
			log.Errorf("can't create directory %s: %v", backupPath, err)
			return err
		}
	}
	var backupDataSize, backupMetadataSize uint64

	var tableMetas []metadata.TableTitle
	for _, table := range tables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			log := log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Name))
			if table.Skip {
				continue
			}
			var realSize map[string]int64
			var disksToPartsMap map[string][]metadata.Part
			if doBackupData {
				log.Debug("create data")
				shadowBackupUUID := strings.ReplaceAll(uuid.New().String(), "-", "")
				disksToPartsMap, realSize, err = b.AddTableToBackup(ctx, backupName, shadowBackupUUID, disks, &table, partitionsToBackupMap)
				if err != nil {
					log.Error(err.Error())
					if removeBackupErr := b.RemoveBackupLocal(ctx, backupName, disks); removeBackupErr != nil {
						log.Error(removeBackupErr.Error())
					}
					// fix corner cases after https://github.com/AlexAkulov/clickhouse-backup/issues/379
					if cleanShadowErr := b.Clean(ctx); cleanShadowErr != nil {
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
			metadataSize, err := b.createTableMetadata(path.Join(backupPath, "metadata"), metadata.TableMetadata{
				Table:        table.Name,
				Database:     table.Database,
				Query:        table.CreateTableQuery,
				TotalBytes:   table.TotalBytes,
				Size:         realSize,
				Parts:        disksToPartsMap,
				MetadataOnly: schemaOnly,
			}, disks)
			if err != nil {
				if removeBackupErr := b.RemoveBackupLocal(ctx, backupName, disks); removeBackupErr != nil {
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
	}
	backupRBACSize, backupConfigSize := uint64(0), uint64(0)

	if rbacOnly {
		if backupRBACSize, err = b.createRBACBackup(ctx, backupPath, disks); err != nil {
			log.Errorf("error during do RBAC backup: %v", err)
		} else {
			log.WithField("size", utils.FormatBytes(backupRBACSize)).Info("done createRBACBackup")
		}
	}
	if configsOnly {
		if backupConfigSize, err = b.createConfigBackup(ctx, backupPath); err != nil {
			log.Errorf("error during do CONFIG backup: %v", err)
		} else {
			log.WithField("size", utils.FormatBytes(backupConfigSize)).Info("done createConfigBackup")
		}
	}

	backupMetaFile := path.Join(defaultPath, "backup", backupName, "metadata.json")
	if err := b.createBackupMetadata(ctx, backupMetaFile, backupName, version, "regular", diskMap, disks, backupDataSize, backupMetadataSize, backupRBACSize, backupConfigSize, tableMetas, allDatabases, allFunctions, log); err != nil {
		return err
	}
	log.WithField("duration", utils.HumanizeDuration(time.Since(startBackup))).Info("done")
	return nil
}

func (b *Backuper) createBackupEmbedded(ctx context.Context, backupName, tablePattern string, partitions []string, partitionsToBackupMap common.EmptyMap, schemaOnly, rbacOnly, configsOnly bool, tables []clickhouse.Table, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, disks []clickhouse.Disk, diskMap map[string]string, log *apexLog.Entry, startBackup time.Time, backupVersion string) error {
	if _, isBackupDiskExists := diskMap[b.cfg.ClickHouse.EmbeddedBackupDisk]; !isBackupDiskExists {
		return fmt.Errorf("backup disk `%s` not exists in system.disks", b.cfg.ClickHouse.EmbeddedBackupDisk)
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
		return fmt.Errorf("`use_embedded_backup_restore: true` doesn't allow empty backups, check your parameter --tables=%v", tablePattern)
	}
	tableMetas := make([]metadata.TableTitle, l)
	tablesSQL := ""
	tableSizeSQL := ""
	i := 0
	backupMetadataSize := uint64(0)
	backupPath := path.Join(diskMap[b.cfg.ClickHouse.EmbeddedBackupDisk], backupName)
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
	backupSQL := fmt.Sprintf("BACKUP %s TO Disk(?,?)", tablesSQL)
	if schemaOnly {
		backupSQL += " SETTINGS structure_only=true"
	}
	backupResult := make([]clickhouse.SystemBackups, 0)
	if err := b.ch.SelectContext(ctx, &backupResult, backupSQL, b.cfg.ClickHouse.EmbeddedBackupDisk, backupName); err != nil {
		return fmt.Errorf("backup error: %v", err)
	}
	if len(backupResult) != 1 || (backupResult[0].Status != "BACKUP_COMPLETE" && backupResult[0].Status != "BACKUP_CREATED") {
		return fmt.Errorf("backup return wrong results: %+v", backupResult)
	}
	backupDataSize := make([]uint64, 0)
	if !schemaOnly {
		if backupResult[0].CompressedSize == 0 {
			chVersion, err := b.ch.GetVersion(ctx)
			if err != nil {
				return err
			}
			backupSizeSQL := fmt.Sprintf("SELECT sum(bytes_on_disk) AS backup_data_size FROM system.parts WHERE active AND concat(database,'.',table) IN (%s)", tableSizeSQL)
			if chVersion >= 20005000 {
				backupSizeSQL = fmt.Sprintf("SELECT sum(total_bytes) AS backup_data_size FROM system.tables WHERE concat(database,'.',name) IN (%s)", tableSizeSQL)
			}
			if err := b.ch.SelectContext(ctx, &backupDataSize, backupSizeSQL); err != nil {
				return err
			}
		} else {
			backupDataSize = append(backupDataSize, backupResult[0].CompressedSize)
		}
	} else {
		backupDataSize = append(backupDataSize, 0)
	}

	log.Debug("calculate parts list from embedded backup disk")
	for _, table := range tables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if table.Skip {
				continue
			}
			disksToPartsMap, err := b.getPartsFromBackupDisk(backupPath, table, partitionsToBackupMap)
			if err != nil {
				if removeBackupErr := b.RemoveBackupLocal(ctx, backupName, disks); removeBackupErr != nil {
					log.Error(removeBackupErr.Error())
				}
				return err
			}
			metadataSize, err := b.createTableMetadata(path.Join(backupPath, "metadata"), metadata.TableMetadata{
				Table:        table.Name,
				Database:     table.Database,
				Query:        table.CreateTableQuery,
				TotalBytes:   table.TotalBytes,
				Size:         map[string]int64{b.cfg.ClickHouse.EmbeddedBackupDisk: 0},
				Parts:        disksToPartsMap,
				MetadataOnly: schemaOnly,
			}, disks)
			if err != nil {
				if removeBackupErr := b.RemoveBackupLocal(ctx, backupName, disks); removeBackupErr != nil {
					log.Error(removeBackupErr.Error())
				}
				return err
			}
			backupMetadataSize += metadataSize
		}
	}
	backupMetaFile := path.Join(diskMap[b.cfg.ClickHouse.EmbeddedBackupDisk], backupName, "metadata.json")
	if err := b.createBackupMetadata(ctx, backupMetaFile, backupName, backupVersion, "embedded", diskMap, disks, backupDataSize[0], backupMetadataSize, 0, 0, tableMetas, allDatabases, allFunctions, log); err != nil {
		return err
	}

	log.WithFields(apexLog.Fields{
		"operation": "create_embedded",
		"duration":  utils.HumanizeDuration(time.Since(startBackup)),
	}).Info("done")

	return nil
}

func (b *Backuper) getPartsFromBackupDisk(backupPath string, table clickhouse.Table, partitionsToBackupMap common.EmptyMap) (map[string][]metadata.Part, error) {
	parts := map[string][]metadata.Part{}
	dirList, err := os.ReadDir(path.Join(backupPath, "data", common.TablePathEncode(table.Database), common.TablePathEncode(table.Name)))
	if err != nil {
		if os.IsNotExist(err) {
			return parts, nil
		}
		return nil, err
	}
	if len(partitionsToBackupMap) == 0 {
		parts[b.cfg.ClickHouse.EmbeddedBackupDisk] = make([]metadata.Part, len(dirList))
		for i, d := range dirList {
			parts[b.cfg.ClickHouse.EmbeddedBackupDisk][i] = metadata.Part{
				Name: d.Name(),
			}
		}
	} else {
		parts[b.cfg.ClickHouse.EmbeddedBackupDisk] = make([]metadata.Part, 0)
		for _, d := range dirList {
			found := false
			for prefix := range partitionsToBackupMap {
				if strings.HasPrefix(d.Name(), prefix+"_") {
					found = true
					break
				}
			}
			if found {
				parts[b.cfg.ClickHouse.EmbeddedBackupDisk] = append(parts[b.cfg.ClickHouse.EmbeddedBackupDisk], metadata.Part{
					Name: d.Name(),
				})
			}
		}
	}
	return parts, nil
}

func (b *Backuper) createConfigBackup(ctx context.Context, backupPath string) (uint64, error) {
	log := b.log.WithField("logger", "createConfigBackup")
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		backupConfigSize := uint64(0)
		configBackupPath := path.Join(backupPath, "configs")
		log.Debugf("copy %s -> %s", b.cfg.ClickHouse.ConfigDir, configBackupPath)
		copyErr := recursiveCopy.Copy(b.cfg.ClickHouse.ConfigDir, configBackupPath, recursiveCopy.Options{
			Skip: func(srcinfo os.FileInfo, src, dest string) (bool, error) {
				backupConfigSize += uint64(srcinfo.Size())
				return false, nil
			},
		})
		return backupConfigSize, copyErr
	}
}

func (b *Backuper) createRBACBackup(ctx context.Context, backupPath string, disks []clickhouse.Disk) (uint64, error) {
	log := b.log.WithField("logger", "createRBACBackup")
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		rbacDataSize := uint64(0)
		rbacBackup := path.Join(backupPath, "access")
		accessPath, err := b.ch.GetAccessManagementPath(ctx, disks)
		if err != nil {
			return 0, err
		}
		log.Debugf("copy %s -> %s", accessPath, rbacBackup)
		copyErr := recursiveCopy.Copy(accessPath, rbacBackup, recursiveCopy.Options{
			Skip: func(srcinfo os.FileInfo, src, dest string) (bool, error) {
				rbacDataSize += uint64(srcinfo.Size())
				return false, nil
			},
		})
		return rbacDataSize, copyErr
	}
}

func (b *Backuper) AddTableToBackup(ctx context.Context, backupName, shadowBackupUUID string, diskList []clickhouse.Disk, table *clickhouse.Table, partitionsToBackupMap common.EmptyMap) (map[string][]metadata.Part, map[string]int64, error) {
	log := b.log.WithFields(apexLog.Fields{
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
	if err := b.ch.FreezeTable(ctx, table, shadowBackupUUID); err != nil {
		return nil, nil, err
	}
	log.Debug("frozen")
	realSize := map[string]int64{}
	disksToPartsMap := map[string][]metadata.Part{}
	for _, disk := range diskList {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
			shadowPath := path.Join(disk.Path, "shadow", shadowBackupUUID)
			if _, err := os.Stat(shadowPath); err != nil && os.IsNotExist(err) {
				continue
			}
			backupPath := path.Join(disk.Path, "backup", backupName)
			encodedTablePath := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Name))
			backupShadowPath := path.Join(backupPath, "shadow", encodedTablePath, disk.Name)
			if err := filesystemhelper.MkdirAll(backupShadowPath, b.ch, diskList); err != nil && !os.IsExist(err) {
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
	}
	log.Debug("done")
	return disksToPartsMap, realSize, nil
}

func (b *Backuper) createBackupMetadata(ctx context.Context, backupMetaFile, backupName, version, tags string, diskMap map[string]string, disks []clickhouse.Disk, backupDataSize, backupMetadataSize, backupRBACSize, backupConfigSize uint64, tableMetas []metadata.TableTitle, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, log *apexLog.Entry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		backupMetadata := metadata.BackupMetadata{
			BackupName:              backupName,
			Disks:                   diskMap,
			ClickhouseBackupVersion: version,
			CreationDate:            time.Now().UTC(),
			Tags:                    tags,
			ClickHouseVersion:       b.ch.GetVersionDescribe(ctx),
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
			_ = b.RemoveBackupLocal(ctx, backupName, disks)
			return fmt.Errorf("can't marshal backup metafile json: %v", err)
		}
		if err := os.WriteFile(backupMetaFile, content, 0640); err != nil {
			_ = b.RemoveBackupLocal(ctx, backupName, disks)
			return err
		}
		if err := filesystemhelper.Chown(backupMetaFile, b.ch, disks, false); err != nil {
			log.Warnf("can't chown %s: %v", backupMetaFile, err)
		}
		return nil
	}
}

func (b *Backuper) createTableMetadata(metadataPath string, table metadata.TableMetadata, disks []clickhouse.Disk) (uint64, error) {
	if err := filesystemhelper.Mkdir(metadataPath, b.ch, disks); err != nil {
		return 0, err
	}
	metadataDatabasePath := path.Join(metadataPath, common.TablePathEncode(table.Database))
	if err := filesystemhelper.Mkdir(metadataDatabasePath, b.ch, disks); err != nil {
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
	if err := filesystemhelper.Chown(metadataFile, b.ch, disks, false); err != nil {
		return 0, err
	}
	return uint64(len(metadataBody)), nil
}
