package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"golang.org/x/sync/errgroup"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/keeper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/partition"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

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

// NewBackupName - return default backup name
func NewBackupName() string {
	return time.Now().UTC().Format(TimeFormatForBackup)
}

// CreateBackup - create new backup of all tables matched by tablePattern
// If backupName is empty string will use default backup name
func (b *Backuper) CreateBackup(backupName, diffFromRemote, tablePattern string, partitions []string, schemaOnly, createRBAC, rbacOnly, createConfigs, configsOnly, skipCheckPartsColumns bool, version string, commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	startBackup := time.Now()
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

	if skipCheckPartsColumns && b.cfg.ClickHouse.CheckPartsColumns {
		b.cfg.ClickHouse.CheckPartsColumns = false
	}

	allDatabases, err := b.ch.GetDatabases(ctx, b.cfg, tablePattern)
	if err != nil {
		return fmt.Errorf("can't get database engines from clickhouse: %v", err)
	}
	tables, err := b.GetTables(ctx, tablePattern)
	if err != nil {
		return fmt.Errorf("can't get tables from clickhouse: %v", err)
	}
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

	disks, err := b.ch.GetDisks(ctx, false)
	if err != nil {
		return err
	}
	b.DefaultDataPath, err = b.ch.GetDefaultPath(disks)
	if err != nil {
		return err
	}

	diskMap := make(map[string]string, len(disks))
	diskTypes := make(map[string]string, len(disks))
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
		diskTypes[disk.Name] = disk.Type
	}
	partitionsIdMap, partitionsNameList := partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, tables, nil, partitions)
	doBackupData := !schemaOnly && !rbacOnly && !configsOnly
	backupRBACSize, backupConfigSize := b.createRBACAndConfigsIfNecessary(ctx, backupName, createRBAC, rbacOnly, createConfigs, configsOnly, disks, diskMap, log)

	if b.cfg.ClickHouse.UseEmbeddedBackupRestore {
		err = b.createBackupEmbedded(ctx, backupName, diffFromRemote, doBackupData, schemaOnly, version, tablePattern, partitionsNameList, partitionsIdMap, tables, allDatabases, allFunctions, disks, diskMap, diskTypes, backupRBACSize, backupConfigSize, log, startBackup)
	} else {
		err = b.createBackupLocal(ctx, backupName, diffFromRemote, doBackupData, schemaOnly, rbacOnly, configsOnly, version, partitionsIdMap, tables, tablePattern, disks, diskMap, diskTypes, allDatabases, allFunctions, backupRBACSize, backupConfigSize, log, startBackup)
	}
	if err != nil {
		// delete local backup if can't create
		if removeBackupErr := b.RemoveBackupLocal(ctx, backupName, disks); removeBackupErr != nil {
			log.Errorf("creating failed -> b.RemoveBackupLocal error: %v", removeBackupErr)
		}
		// fix corner cases after https://github.com/Altinity/clickhouse-backup/issues/379
		if cleanShadowErr := b.Clean(ctx); cleanShadowErr != nil {
			log.Errorf("creating failed -> b.Clean error: %v", cleanShadowErr)
			log.Error(cleanShadowErr.Error())
		}

		return err
	}

	// Clean
	if err := b.RemoveOldBackupsLocal(ctx, true, disks); err != nil {
		return err
	}
	return nil
}

func (b *Backuper) createRBACAndConfigsIfNecessary(ctx context.Context, backupName string, createRBAC bool, rbacOnly bool, createConfigs bool, configsOnly bool, disks []clickhouse.Disk, diskMap map[string]string, log *apexLog.Entry) (uint64, uint64) {
	backupRBACSize, backupConfigSize := uint64(0), uint64(0)
	backupPath := path.Join(b.DefaultDataPath, "backup")
	if b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		backupPath = diskMap[b.cfg.ClickHouse.EmbeddedBackupDisk]
	}
	backupPath = path.Join(backupPath, backupName)
	if createRBAC || rbacOnly {
		var createRBACErr error
		if backupRBACSize, createRBACErr = b.createBackupRBAC(ctx, backupPath, disks); createRBACErr != nil {
			log.Fatalf("error during do RBAC backup: %v", createRBACErr)
		} else {
			log.WithField("size", utils.FormatBytes(backupRBACSize)).Info("done createBackupRBAC")
		}
	}
	if createConfigs || configsOnly {
		var createConfigsErr error
		if backupConfigSize, createConfigsErr = b.createBackupConfigs(ctx, backupPath); createConfigsErr != nil {
			log.Fatalf("error during do CONFIG backup: %v", createConfigsErr)
		} else {
			log.WithField("size", utils.FormatBytes(backupConfigSize)).Info("done createBackupConfigs")
		}
	}
	return backupRBACSize, backupConfigSize
}

func (b *Backuper) createBackupLocal(ctx context.Context, backupName, diffFromRemote string, doBackupData, schemaOnly, rbacOnly, configsOnly bool, backupVersion string, partitionsIdMap map[metadata.TableTitle]common.EmptyMap, tables []clickhouse.Table, tablePattern string, disks []clickhouse.Disk, diskMap, diskTypes map[string]string, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, backupRBACSize, backupConfigSize uint64, log *apexLog.Entry, startBackup time.Time) error {
	// Create backup dir on all clickhouse disks
	for _, disk := range disks {
		if err := filesystemhelper.Mkdir(path.Join(disk.Path, "backup"), b.ch, disks); err != nil {
			return err
		}
	}
	backupPath := path.Join(b.DefaultDataPath, "backup", backupName)
	if _, err := os.Stat(path.Join(backupPath, "metadata.json")); err == nil || !os.IsNotExist(err) {
		return fmt.Errorf("'%s' medatata.json already exists", backupName)
	}
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		if err = filesystemhelper.Mkdir(backupPath, b.ch, disks); err != nil {
			log.Errorf("can't create directory %s: %v", backupPath, err)
			return err
		}
	}
	isObjectDiskContainsTables := false
	for _, disk := range disks {
		if b.isDiskTypeObject(disk.Type) || b.isDiskTypeEncryptedObject(disk, disks) {
			for _, table := range tables {
				sort.Slice(table.DataPaths, func(i, j int) bool { return len(table.DataPaths[i]) > len(table.DataPaths[j]) })
				for _, tableDataPath := range table.DataPaths {
					if strings.HasPrefix(tableDataPath, disk.Path) {
						isObjectDiskContainsTables = true
						break
					}
				}
			}
		}
	}
	if isObjectDiskContainsTables || diffFromRemote != "" {
		var err error
		if err = config.ValidateObjectDiskConfig(b.cfg); err != nil {
			return err
		}
		b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, false, backupName)
		if err != nil {
			return err
		}
		if err := b.dst.Connect(ctx); err != nil {
			return fmt.Errorf("can't connect to %s: %v", b.dst.Kind(), err)
		}
		defer func() {
			if closeErr := b.dst.Close(ctx); closeErr != nil {
				log.Warnf("can't close connection to %s: %v", b.dst.Kind(), closeErr)
			}
		}()
	}
	var tablesDiffFromRemote map[metadata.TableTitle]metadata.TableMetadata
	if diffFromRemote != "" {
		var diffFromRemoteErr error
		tablesDiffFromRemote, diffFromRemoteErr = b.getTablesDiffFromRemote(ctx, diffFromRemote, tablePattern)
		if diffFromRemoteErr != nil {
			return fmt.Errorf("b.getTablesDiffFromRemote return error: %v", diffFromRemoteErr)
		}
	}

	var backupDataSize, backupMetadataSize uint64
	var metaMutex sync.Mutex
	createBackupWorkingGroup, createCtx := errgroup.WithContext(ctx)
	createBackupWorkingGroup.SetLimit(int(b.cfg.General.UploadConcurrency))

	var tableMetas []metadata.TableTitle
	for _, tableItem := range tables {
		//to avoid race condition
		table := tableItem
		if table.Skip {
			continue
		}
		createBackupWorkingGroup.Go(func() error {
			log := log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Name))
			var realSize map[string]int64
			var disksToPartsMap map[string][]metadata.Part
			if doBackupData && table.BackupType == clickhouse.ShardBackupFull {
				log.Debug("create data")
				shadowBackupUUID := strings.ReplaceAll(uuid.New().String(), "-", "")
				var addTableToBackupErr error
				disksToPartsMap, realSize, addTableToBackupErr = b.AddTableToLocalBackup(createCtx, backupName, tablesDiffFromRemote, shadowBackupUUID, disks, &table, partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Name}])
				if addTableToBackupErr != nil {
					log.Errorf("b.AddTableToLocalBackup error: %v", addTableToBackupErr)
					return addTableToBackupErr
				}
				// more precise data size calculation
				for _, size := range realSize {
					atomic.AddUint64(&backupDataSize, uint64(size))
				}
			}
			// https://github.com/Altinity/clickhouse-backup/issues/529
			log.Debug("get in progress mutations list")
			inProgressMutations := make([]metadata.MutationMetadata, 0)
			if b.cfg.ClickHouse.BackupMutations && !schemaOnly && !rbacOnly && !configsOnly {
				var inProgressMutationsErr error
				inProgressMutations, inProgressMutationsErr = b.ch.GetInProgressMutations(createCtx, table.Database, table.Name)
				if inProgressMutationsErr != nil {
					log.Errorf("b.ch.GetInProgressMutations error: %v", inProgressMutationsErr)
					return inProgressMutationsErr
				}
			}
			log.Debug("create metadata")
			if schemaOnly || doBackupData {
				metadataSize, createTableMetadataErr := b.createTableMetadata(path.Join(backupPath, "metadata"), metadata.TableMetadata{
					Table:        table.Name,
					Database:     table.Database,
					Query:        table.CreateTableQuery,
					TotalBytes:   table.TotalBytes,
					Size:         realSize,
					Parts:        disksToPartsMap,
					Mutations:    inProgressMutations,
					MetadataOnly: schemaOnly || table.BackupType == clickhouse.ShardBackupSchema,
				}, disks)
				if createTableMetadataErr != nil {
					log.Errorf("b.createTableMetadata error: %v", createTableMetadataErr)
					return createTableMetadataErr
				}
				atomic.AddUint64(&backupMetadataSize, metadataSize)
				metaMutex.Lock()
				tableMetas = append(tableMetas, metadata.TableTitle{
					Database: table.Database,
					Table:    table.Name,
				})
				metaMutex.Unlock()
			}
			log.Infof("done")
			return nil
		})
	}
	if wgWaitErr := createBackupWorkingGroup.Wait(); wgWaitErr != nil {
		return fmt.Errorf("one of createBackupLocal go-routine return error: %v", wgWaitErr)
	}

	backupMetaFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
	if err := b.createBackupMetadata(ctx, backupMetaFile, backupName, diffFromRemote, backupVersion, "regular", diskMap, diskTypes, disks, backupDataSize, backupMetadataSize, backupRBACSize, backupConfigSize, tableMetas, allDatabases, allFunctions, log); err != nil {
		return fmt.Errorf("createBackupMetadata return error: %v", err)
	}
	log.WithField("duration", utils.HumanizeDuration(time.Since(startBackup))).Info("done")
	return nil
}

func (b *Backuper) createBackupEmbedded(ctx context.Context, backupName, baseBackup string, doBackupData, schemaOnly bool, backupVersion, tablePattern string, partitionsNameList map[metadata.TableTitle][]string, partitionsIdMap map[metadata.TableTitle]common.EmptyMap, tables []clickhouse.Table, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, disks []clickhouse.Disk, diskMap, diskTypes map[string]string, backupRBACSize, backupConfigSize uint64, log *apexLog.Entry, startBackup time.Time) error {
	// TODO: Implement sharded backup operations for embedded backups
	if doesShard(b.cfg.General.ShardedOperationMode) {
		return fmt.Errorf("cannot perform embedded backup: %w", errShardOperationUnsupported)
	}
	backupPath := path.Join(b.DefaultDataPath, "backup")
	if b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		backupPath = diskMap[b.cfg.ClickHouse.EmbeddedBackupDisk]
	}
	backupPath = path.Join(backupPath, backupName)

	backupMetadataSize := uint64(0)
	backupDataSize := make([]clickhouse.BackupDataSize, 0)
	if !schemaOnly && !doBackupData {
		backupDataSize = append(backupDataSize, clickhouse.BackupDataSize{Size: 0})
	}
	l := 0
	for _, table := range tables {
		if !table.Skip {
			l += 1
		}
	}
	if l == 0 && (schemaOnly || doBackupData) {
		return fmt.Errorf("`use_embedded_backup_restore: true` not found tables for backup, check your parameter --tables=%v", tablePattern)
	}
	tablesTitle := make([]metadata.TableTitle, l)

	if schemaOnly || doBackupData {
		if _, isBackupDiskExists := diskMap[b.cfg.ClickHouse.EmbeddedBackupDisk]; !isBackupDiskExists && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
			return fmt.Errorf("backup disk `%s` not exists in system.disks", b.cfg.ClickHouse.EmbeddedBackupDisk)
		}
		if b.cfg.ClickHouse.EmbeddedBackupDisk == "" {
			if err := config.ValidateObjectDiskConfig(b.cfg); err != nil {
				return err
			}
		}

		tableSizeSQL, backupSQL, err := b.generateEmbeddedBackupSQL(ctx, backupName, schemaOnly, tables, tablesTitle, partitionsNameList, l, baseBackup)
		if err != nil {
			return err
		}
		backupResult := make([]clickhouse.SystemBackups, 0)
		if err := b.ch.SelectContext(ctx, &backupResult, backupSQL); err != nil {
			return fmt.Errorf("backup error: %v", err)
		}
		if len(backupResult) != 1 || (backupResult[0].Status != "BACKUP_COMPLETE" && backupResult[0].Status != "BACKUP_CREATED") {
			return fmt.Errorf("backup return wrong results: %+v", backupResult)
		}

		if schemaOnly {
			backupDataSize = append(backupDataSize, clickhouse.BackupDataSize{Size: 0})
		} else {
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
				backupDataSize = append(backupDataSize, clickhouse.BackupDataSize{Size: backupResult[0].CompressedSize})
			}
		}

		if doBackupData && b.cfg.ClickHouse.EmbeddedBackupDisk == "" {
			var err error
			if b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, false, backupName); err != nil {
				return err
			}
			if err = b.dst.Connect(ctx); err != nil {
				return fmt.Errorf("createBackupEmbedded: can't connect to %s: %v", b.dst.Kind(), err)
			}
			defer func() {
				if closeErr := b.dst.Close(ctx); closeErr != nil {
					log.Warnf("createBackupEmbedded: can't close connection to %s: %v", b.dst.Kind(), closeErr)
				}
			}()
		}

		for _, table := range tables {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if table.Skip {
					continue
				}
				var disksToPartsMap map[string][]metadata.Part
				if doBackupData {
					if b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
						log.Debugf("calculate parts list `%s`.`%s` from embedded backup disk `%s`", table.Database, table.Name, b.cfg.ClickHouse.EmbeddedBackupDisk)
						disksToPartsMap, err = b.getPartsFromLocalEmbeddedBackupDisk(backupPath, table, partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Name}])
					} else {
						log.Debugf("calculate parts list `%s`.`%s` from embedded backup remote destination", table.Database, table.Name)
						disksToPartsMap, err = b.getPartsFromRemoteEmbeddedBackup(ctx, backupName, table, partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Name}], log)
					}
				}
				if err != nil {
					return err
				}
				if schemaOnly || doBackupData {
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
						return err
					}
					backupMetadataSize += metadataSize
				}
			}
		}
	}
	backupMetaFile := path.Join(backupPath, "metadata.json")
	if err := b.createBackupMetadata(ctx, backupMetaFile, backupName, baseBackup, backupVersion, "embedded", diskMap, diskTypes, disks, backupDataSize[0].Size, backupMetadataSize, backupRBACSize, backupConfigSize, tablesTitle, allDatabases, allFunctions, log); err != nil {
		return err
	}

	log.WithFields(apexLog.Fields{
		"operation": "create_embedded",
		"duration":  utils.HumanizeDuration(time.Since(startBackup)),
	}).Info("done")

	return nil
}

func (b *Backuper) generateEmbeddedBackupSQL(ctx context.Context, backupName string, schemaOnly bool, tables []clickhouse.Table, tablesTitle []metadata.TableTitle, partitionsNameList map[metadata.TableTitle][]string, tablesListLen int, baseBackup string) (string, string, error) {
	tablesSQL := ""
	tableSizeSQL := ""
	i := 0
	for _, table := range tables {
		if table.Skip {
			continue
		}
		tablesTitle[i] = metadata.TableTitle{
			Database: table.Database,
			Table:    table.Name,
		}
		i += 1

		tablesSQL += "TABLE `" + table.Database + "`.`" + table.Name + "`"
		tableSizeSQL += "'" + table.Database + "." + table.Name + "'"
		if nameList, exists := partitionsNameList[metadata.TableTitle{Database: table.Database, Table: table.Name}]; exists && len(nameList) > 0 {
			partitionsSQL := ""
			for _, partitionName := range nameList {
				if strings.HasPrefix(partitionName, "(") {
					partitionsSQL += partitionName + ","
				} else {
					partitionsSQL += "'" + partitionName + "',"
				}
			}
			tablesSQL += fmt.Sprintf(" PARTITIONS %s", partitionsSQL[:len(partitionsSQL)-1])
		}
		if i < tablesListLen {
			tablesSQL += ", "
			tableSizeSQL += ", "
		}
	}
	embeddedBackupLocation, err := b.getEmbeddedBackupLocation(ctx, backupName)
	if err != nil {
		return "", "", err
	}
	backupSQL := fmt.Sprintf("BACKUP %s TO %s", tablesSQL, embeddedBackupLocation)
	var backupSettings []string
	if schemaOnly {
		backupSettings = append(backupSettings, "structure_only=1")
	}
	if b.cfg.ClickHouse.EmbeddedBackupThreads > 0 {
		backupSettings = append(backupSettings, fmt.Sprintf("backup_threads=%d", b.cfg.ClickHouse.EmbeddedBackupThreads))
	}
	if baseBackup != "" {
		backupSettings = append(backupSettings, fmt.Sprintf("base_backup='%s'", baseBackup))
	}
	if len(backupSettings) > 0 {
		backupSQL += " SETTINGS " + strings.Join(backupSettings, ", ")
	}
	return tableSizeSQL, backupSQL, nil
}

func (b *Backuper) getPartsFromRemoteEmbeddedBackup(ctx context.Context, backupName string, table clickhouse.Table, partitionsIdsMap common.EmptyMap, log *apexLog.Entry) (map[string][]metadata.Part, error) {
	dirListStr := make([]string, 0)
	remoteEmbeddedBackupPath := ""
	if b.cfg.General.RemoteStorage == "s3" {
		remoteEmbeddedBackupPath = b.cfg.S3.ObjectDiskPath
	} else if b.cfg.General.RemoteStorage == "gcs" {
		remoteEmbeddedBackupPath = b.cfg.GCS.ObjectDiskPath
	} else if b.cfg.General.RemoteStorage == "azblob" {
		remoteEmbeddedBackupPath = b.cfg.AzureBlob.ObjectDiskPath
	} else {
		return nil, fmt.Errorf("getPartsFromRemoteEmbeddedBackup: unsupported remote_storage: %s", b.cfg.General.RemoteStorage)
	}
	remoteEmbeddedBackupPath = path.Join(remoteEmbeddedBackupPath, backupName, "data", common.TablePathEncode(table.Database), common.TablePathEncode(table.Name))
	if walkErr := b.dst.WalkAbsolute(ctx, remoteEmbeddedBackupPath, false, func(ctx context.Context, fInfo storage.RemoteFile) error {
		dirListStr = append(dirListStr, fInfo.Name())
		return nil
	}); walkErr != nil {
		return nil, walkErr
	}
	log.Debugf("getPartsFromRemoteEmbeddedBackup from %s found %d parts", remoteEmbeddedBackupPath, len(dirListStr))
	return b.fillEmbeddedPartsFromDirList(partitionsIdsMap, dirListStr, "default")
}

func (b *Backuper) getPartsFromLocalEmbeddedBackupDisk(backupPath string, table clickhouse.Table, partitionsIdsMap common.EmptyMap) (map[string][]metadata.Part, error) {
	dirList, err := os.ReadDir(path.Join(backupPath, "data", common.TablePathEncode(table.Database), common.TablePathEncode(table.Name)))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string][]metadata.Part{}, nil
		}
		return nil, err
	}
	dirListStr := make([]string, len(dirList))
	for i, d := range dirList {
		dirListStr[i] = d.Name()
	}
	return b.fillEmbeddedPartsFromDirList(partitionsIdsMap, dirListStr, b.cfg.ClickHouse.EmbeddedBackupDisk)
}

func (b *Backuper) fillEmbeddedPartsFromDirList(partitionsIdsMap common.EmptyMap, dirList []string, diskName string) (map[string][]metadata.Part, error) {
	parts := map[string][]metadata.Part{}
	if len(partitionsIdsMap) == 0 {
		parts[diskName] = make([]metadata.Part, len(dirList))
		for i, dirName := range dirList {
			parts[diskName][i] = metadata.Part{
				Name: dirName,
			}
		}
		return parts, nil
	}

	parts[diskName] = make([]metadata.Part, 0)
	for _, dirName := range dirList {
		found := false
		for prefix := range partitionsIdsMap {
			if strings.HasPrefix(dirName, prefix+"_") {
				found = true
				break
			}
		}
		if found {
			parts[diskName] = append(parts[b.cfg.ClickHouse.EmbeddedBackupDisk], metadata.Part{
				Name: dirName,
			})
		}
	}
	return parts, nil
}

func (b *Backuper) createBackupConfigs(ctx context.Context, backupPath string) (uint64, error) {
	log := b.log.WithField("logger", "createBackupConfigs")
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

func (b *Backuper) createBackupRBAC(ctx context.Context, backupPath string, disks []clickhouse.Disk) (uint64, error) {
	log := b.log.WithField("logger", "createBackupRBAC")
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
		accessPathInfo, err := os.Stat(accessPath)
		if err != nil && !os.IsNotExist(err) {
			return 0, err
		}
		if err == nil && !accessPathInfo.IsDir() {
			return 0, fmt.Errorf("%s is not directory", accessPath)
		}
		if err == nil {
			log.Debugf("copy %s -> %s", accessPath, rbacBackup)
			copyErr := recursiveCopy.Copy(accessPath, rbacBackup, recursiveCopy.Options{
				Skip: func(srcinfo os.FileInfo, src, dest string) (bool, error) {
					rbacDataSize += uint64(srcinfo.Size())
					return false, nil
				},
			})
			if copyErr != nil {
				return 0, copyErr
			}
		} else {
			if err = os.MkdirAll(rbacBackup, 0755); err != nil {
				return 0, err
			}
		}
		replicatedRBACDataSize, err := b.createBackupRBACReplicated(ctx, rbacBackup)
		if err != nil {
			return 0, err
		}
		return rbacDataSize + replicatedRBACDataSize, nil
	}
}

func (b *Backuper) createBackupRBACReplicated(ctx context.Context, rbacBackup string) (replicatedRBACDataSize uint64, err error) {
	replicatedRBAC := make([]struct {
		Name string `ch:"name"`
	}, 0)
	rbacDataSize := uint64(0)
	if err = b.ch.SelectContext(ctx, &replicatedRBAC, "SELECT name FROM system.user_directories WHERE type='replicated'"); err == nil && len(replicatedRBAC) > 0 {
		k := keeper.Keeper{Log: b.log.WithField("logger", "keeper")}
		if err = k.Connect(ctx, b.ch, b.cfg); err != nil {
			return 0, err
		}
		defer k.Close()
		for _, userDirectory := range replicatedRBAC {
			replicatedAccessPath, err := k.GetReplicatedAccessPath(userDirectory.Name)
			if err != nil {
				return 0, err
			}
			dumpFile := path.Join(rbacBackup, userDirectory.Name+".jsonl")
			b.log.WithField("logger", "createBackupRBACReplicated").Infof("keeper.Dump %s -> %s", replicatedAccessPath, dumpFile)
			dumpRBACSize, dumpErr := k.Dump(replicatedAccessPath, dumpFile)
			if dumpErr != nil {
				return 0, dumpErr
			}
			rbacDataSize += uint64(dumpRBACSize)
		}
	}
	return rbacDataSize, nil
}

func (b *Backuper) AddTableToLocalBackup(ctx context.Context, backupName string, tablesDiffFromRemote map[metadata.TableTitle]metadata.TableMetadata, shadowBackupUUID string, diskList []clickhouse.Disk, table *clickhouse.Table, partitionsIdsMap common.EmptyMap) (map[string][]metadata.Part, map[string]int64, error) {
	log := b.log.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "create",
		"table":     fmt.Sprintf("%s.%s", table.Database, table.Name),
	})
	if backupName == "" {
		return nil, nil, fmt.Errorf("backupName is not defined")
	}

	if !strings.HasSuffix(table.Engine, "MergeTree") && table.Engine != "MaterializedMySQL" && table.Engine != "MaterializedPostgreSQL" {
		if table.Engine != "MaterializedView" {
			log.WithField("engine", table.Engine).Warnf("supports only schema backup")
		}
		return nil, nil, nil
	}
	if b.cfg.ClickHouse.CheckPartsColumns {
		if err := b.ch.CheckSystemPartsColumns(ctx, table); err != nil {
			return nil, nil, err
		}
	}
	// backup data
	if err := b.ch.FreezeTable(ctx, table, shadowBackupUUID); err != nil {
		return nil, nil, err
	}
	log.Debug("frozen")
	version, err := b.ch.GetVersion(ctx)
	if err != nil {
		return nil, nil, err
	}
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
			// If partitionsIdsMap is not empty, only parts in this partition will back up.
			parts, size, err := filesystemhelper.MoveShadowToBackup(shadowPath, backupShadowPath, partitionsIdsMap, tablesDiffFromRemote[metadata.TableTitle{Database: table.Database, Table: table.Name}], disk, version)
			if err != nil {
				return nil, nil, err
			}
			realSize[disk.Name] = size
			disksToPartsMap[disk.Name] = parts
			log.WithField("disk", disk.Name).Debug("shadow moved")
			if len(parts) > 0 && (b.isDiskTypeObject(disk.Type) || b.isDiskTypeEncryptedObject(disk, diskList)) {
				start := time.Now()
				if size, err = b.uploadObjectDiskParts(ctx, backupName, tablesDiffFromRemote[metadata.TableTitle{Database: table.Database, Table: table.Name}], backupShadowPath, disk); err != nil {
					return disksToPartsMap, realSize, err
				}
				realSize[disk.Name] += size
				log.WithField("disk", disk.Name).WithField("duration", utils.HumanizeDuration(time.Since(start))).WithField("size", utils.FormatBytes(uint64(size))).Info("object_disk data uploaded")
			}
			// Clean all the files under the shadowPath, cause UNFREEZE unavailable
			if version < 21004000 {
				if err := os.RemoveAll(shadowPath); err != nil {
					return disksToPartsMap, realSize, err
				}
			}
		}
	}
	// Unfreeze to unlock data on S3 disks, https://github.com/Altinity/clickhouse-backup/issues/423
	if version > 21004000 {
		if err := b.ch.QueryContext(ctx, fmt.Sprintf("ALTER TABLE `%s`.`%s` UNFREEZE WITH NAME '%s'", table.Database, table.Name, shadowBackupUUID)); err != nil {
			if (strings.Contains(err.Error(), "code: 60") || strings.Contains(err.Error(), "code: 81") || strings.Contains(err.Error(), "code: 218")) && b.cfg.ClickHouse.IgnoreNotExistsErrorDuringFreeze {
				b.ch.Log.Warnf("can't unfreeze table: %v", err)
			} else {
				return disksToPartsMap, realSize, err
			}

		}
	}
	log.Debug("done")
	return disksToPartsMap, realSize, nil
}

func (b *Backuper) uploadObjectDiskParts(ctx context.Context, backupName string, tableDiffFromRemote metadata.TableMetadata, backupShadowPath string, disk clickhouse.Disk) (int64, error) {
	var size int64
	var err error
	if err = object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, disk.Name); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	uploadObjectDiskPartsWorkingGroup, ctx := errgroup.WithContext(ctx)
	uploadObjectDiskPartsWorkingGroup.SetLimit(int(b.cfg.General.UploadConcurrency * b.cfg.General.UploadConcurrency))
	srcDiskConnection, exists := object_disk.DisksConnections.Load(disk.Name)
	if !exists {
		return 0, fmt.Errorf("uploadObjectDiskParts: %s not present in object_disk.DisksConnections", disk.Name)
	}
	srcBucket := srcDiskConnection.GetRemoteBucket()
	walkErr := filepath.Walk(backupShadowPath, func(fPath string, fInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fInfo.IsDir() {
			return nil
		}
		// fix https://github.com/Altinity/clickhouse-backup/issues/826
		if strings.Contains(fInfo.Name(), "frozen_metadata") {
			return nil
		}
		var realSize, objSize int64
		// upload only not required parts, https://github.com/Altinity/clickhouse-backup/issues/865
		if tableDiffFromRemote.Database != "" && tableDiffFromRemote.Table != "" && len(tableDiffFromRemote.Parts[disk.Name]) > 0 {
			partPaths := strings.SplitN(strings.TrimPrefix(fPath, backupShadowPath), "/", 2)
			for _, part := range tableDiffFromRemote.Parts[disk.Name] {
				if part.Name == partPaths[0] {
					return nil
				}
			}
		}
		uploadObjectDiskPartsWorkingGroup.Go(func() error {
			objPartFileMeta, readMetadataErr := object_disk.ReadMetadataFromFile(fPath)
			if readMetadataErr != nil {
				return readMetadataErr
			}
			for _, storageObject := range objPartFileMeta.StorageObjects {
				if storageObject.ObjectSize == 0 {
					continue
				}
				if objSize, err = b.dst.CopyObject(
					ctx,
					storageObject.ObjectSize,
					srcBucket,
					path.Join(srcDiskConnection.GetRemotePath(), storageObject.ObjectRelativePath),
					path.Join(backupName, disk.Name, storageObject.ObjectRelativePath),
				); err != nil {
					return err
				}
				realSize += objSize
			}
			if realSize > objPartFileMeta.TotalSize {
				atomic.AddInt64(&size, realSize)
			} else {
				atomic.AddInt64(&size, objPartFileMeta.TotalSize)
			}
			return nil
		})
		return nil
	})
	if walkErr != nil {
		return 0, err
	}

	if wgWaitErr := uploadObjectDiskPartsWorkingGroup.Wait(); wgWaitErr != nil {
		return 0, fmt.Errorf("one of uploadObjectDiskParts go-routine return error: %v", wgWaitErr)
	}
	return size, nil
}

func (b *Backuper) createBackupMetadata(ctx context.Context, backupMetaFile, backupName, requiredBackup, version, tags string, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, backupDataSize, backupMetadataSize, backupRBACSize, backupConfigSize uint64, tableMetas []metadata.TableTitle, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, log *apexLog.Entry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		backupMetadata := metadata.BackupMetadata{
			BackupName:              backupName,
			RequiredBackup:          requiredBackup,
			Disks:                   diskMap,
			DiskTypes:               diskTypes,
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
			return fmt.Errorf("can't marshal backup metafile json: %v", err)
		}
		if err := os.WriteFile(backupMetaFile, content, 0640); err != nil {
			return err
		}
		if err := filesystemhelper.Chown(backupMetaFile, b.ch, disks, false); err != nil {
			log.Warnf("can't chown %s: %v", backupMetaFile, err)
		}
		b.log.Debugf("%s created", backupMetaFile)
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
	b.log.Debugf("%s created", metadataFile)
	return uint64(len(metadataBody)), nil
}
