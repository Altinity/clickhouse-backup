package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
	"github.com/Altinity/clickhouse-backup/v2/pkg/resumable"
	"github.com/eapache/go-resiliency/retrier"

	"github.com/google/uuid"
	recursiveCopy "github.com/otiai10/copy"
	"golang.org/x/sync/errgroup"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/keeper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/partition"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/rs/zerolog/log"
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
	Broken string
}

// NewBackupName - return default backup name
func NewBackupName() string {
	return time.Now().Format(TimeFormatForBackup)
}

// CreateBackup - create new backup of all tables matched by tablePattern
// If backupName is empty string will use default backup name
func (b *Backuper) CreateBackup(backupName, diffFromRemote, tablePattern string, partitions []string, schemaOnly, createRBAC, rbacOnly, createConfigs, configsOnly, createNamedCollections, namedCollectionsOnly, skipCheckPartsColumns bool, skipProjections []string, resume bool, backupVersion string, commandId int) error {
	if pidCheckErr := pidlock.CheckAndCreatePidFile(backupName, "create"); pidCheckErr != nil {
		return pidCheckErr
	}
	defer pidlock.RemovePidFile(backupName)
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

	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	clickHouseVersion, versionErr := b.ch.GetVersion(ctx)
	if versionErr != nil {
		return versionErr
	}
	if clickHouseVersion < 24003000 && skipProjections != nil && len(skipProjections) > 0 {
		log.Warn().Msg("backup with skip-projections can restore only in 24.3+")
	}

	if skipCheckPartsColumns && b.cfg.ClickHouse.CheckPartsColumns {
		b.cfg.ClickHouse.CheckPartsColumns = false
	}
	if b.cfg.General.RBACBackupAlways {
		createRBAC = true
	}
	if b.cfg.General.ConfigBackupAlways {
		createConfigs = true
	}
	if b.cfg.General.NamedCollectionsBackupAlways {
		createNamedCollections = true
	}
	b.adjustResumeFlag(resume)

	allDatabases, err := b.ch.GetDatabases(ctx, b.cfg, tablePattern)
	if err != nil {
		return fmt.Errorf("can't get database engines from clickhouse: %v", err)
	}
	tables, err := b.GetTables(ctx, tablePattern)
	if err != nil {
		return fmt.Errorf("can't get tables from clickhouse: %v", err)
	}

	if b.CalculateNonSkipTables(tables) == 0 && !b.cfg.General.AllowEmptyBackups {
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
	doBackupData := !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly
	backupRBACSize, backupConfigSize, backupNamedCollectionsSize, rbacConfigsNamedCollectionsErr := b.createConfigsNamedCollectionsAndRBACIfNecessary(ctx, backupName, createRBAC, rbacOnly, createConfigs, configsOnly, createNamedCollections, namedCollectionsOnly, disks, diskMap)
	if rbacConfigsNamedCollectionsErr != nil {
		return rbacConfigsNamedCollectionsErr
	}
	if b.cfg.ClickHouse.UseEmbeddedBackupRestore {
		err = b.createBackupEmbedded(ctx, backupName, diffFromRemote, doBackupData, schemaOnly, backupVersion, tablePattern, partitionsNameList, partitionsIdMap, tables, allDatabases, allFunctions, disks, diskMap, diskTypes, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, startBackup, clickHouseVersion)
	} else {
		err = b.createBackupLocal(ctx, backupName, diffFromRemote, doBackupData, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, backupVersion, partitions, partitionsIdMap, tables, tablePattern, skipProjections, disks, diskMap, diskTypes, allDatabases, allFunctions, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, startBackup, clickHouseVersion)
	}
	if err != nil {
		log.Error().Msgf("backup failed error: %v", err)
		// delete local backup when creation failure
		if removeBackupErr := b.RemoveBackupLocal(ctx, backupName, disks); removeBackupErr != nil {
			log.Error().Msgf("creating failed -> b.RemoveBackupLocal error: %v", removeBackupErr)
		}
		// fix corner cases after https://github.com/Altinity/clickhouse-backup/issues/379
		if cleanShadowErr := b.Clean(ctx); cleanShadowErr != nil {
			log.Error().Msgf("creating failed -> b.Clean error: %v", cleanShadowErr)
		}
		return err
	}

	// Clean
	if err := b.RemoveOldBackupsLocal(ctx, true, disks); err != nil {
		return err
	}
	return nil
}

func (b *Backuper) CalculateNonSkipTables(tables []clickhouse.Table) int {
	i := 0
	for _, table := range tables {
		if table.Skip {
			continue
		}
		i++
	}
	return i
}

func (b *Backuper) createConfigsNamedCollectionsAndRBACIfNecessary(ctx context.Context, backupName string, createRBAC bool, rbacOnly bool, createConfigs bool, configsOnly bool, createNamedCollections bool, namedCollectionsOnly bool, disks []clickhouse.Disk, diskMap map[string]string) (uint64, uint64, uint64, error) {
	backupRBACSize, backupConfigSize, backupNamedCollectionsSize := uint64(0), uint64(0), uint64(0)
	backupPath := path.Join(b.DefaultDataPath, "backup")
	if b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		backupPath = diskMap[b.cfg.ClickHouse.EmbeddedBackupDisk]
	}
	backupPath = path.Join(backupPath, backupName)
	if createRBAC || rbacOnly {
		var createRBACErr error
		if backupRBACSize, createRBACErr = b.createBackupRBAC(ctx, backupPath, disks); createRBACErr != nil {
			log.Fatal().Msgf("error during do RBAC backup: %v", createRBACErr)
		} else {
			log.Info().Str("size", utils.FormatBytes(backupRBACSize)).Msg("done createBackupRBAC")
		}
	}
	if createConfigs || configsOnly {
		var createConfigsErr error
		if backupConfigSize, createConfigsErr = b.createBackupConfigs(ctx, backupPath); createConfigsErr != nil {
			log.Fatal().Msgf("error during do CONFIG backup: %v", createConfigsErr)
		} else {
			log.Info().Str("size", utils.FormatBytes(backupConfigSize)).Msg("done createBackupConfigs")
		}
	}
	if createNamedCollections || namedCollectionsOnly {
		var createNamedCollectionsErr error
		if backupNamedCollectionsSize, createNamedCollectionsErr = b.createBackupNamedCollections(ctx, backupPath); createNamedCollectionsErr != nil {
			log.Fatal().Msgf("error during do NamedCollections backup: %v", createNamedCollectionsErr)
		} else {
			log.Info().Str("size", utils.FormatBytes(backupNamedCollectionsSize)).Msg("done createBackupNamedCollections")
		}
	}
	if backupRBACSize > 0 || backupConfigSize > 0 || backupNamedCollectionsSize > 0 {
		if chownErr := filesystemhelper.Chown(backupPath, b.ch, disks, true); chownErr != nil {
			return backupRBACSize, backupConfigSize, backupNamedCollectionsSize, chownErr
		}
	}
	return backupRBACSize, backupConfigSize, backupNamedCollectionsSize, nil
}

func (b *Backuper) createBackupLocal(ctx context.Context, backupName, diffFromRemote string, doBackupData, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly bool, backupVersion string, partitions []string, partitionsIdMap map[metadata.TableTitle]common.EmptyMap, tables []clickhouse.Table, tablePattern string, skipProjections []string, disks []clickhouse.Disk, diskMap, diskTypes map[string]string, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, backupRBACSize, backupConfigSize, backupNamedCollectionsSize uint64, startBackup time.Time, version int) error {
	// Create backup dir on all clickhouse disks
	for _, disk := range disks {
		if err := filesystemhelper.Mkdir(path.Join(disk.Path, "backup"), b.ch, disks); err != nil {
			return err
		}
	}
	backupPath := path.Join(b.DefaultDataPath, "backup", backupName)
	if _, err := os.Stat(path.Join(backupPath, "metadata.json")); err == nil || !os.IsNotExist(err) {
		if !b.resume {
			return fmt.Errorf("'%s' medatata.json already exists", backupName)
		}
		log.Warn().Msgf("'%s' medatata.json already exists, will overwrite and resume object disk data upload", backupName)
	}
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		if err = filesystemhelper.Mkdir(backupPath, b.ch, disks); err != nil {
			log.Error().Msgf("can't create directory %s: %v", backupPath, err)
			return err
		}
	}
	isObjectDiskContainsTables := false
	for _, disk := range disks {
		if b.shouldSkipByDiskNameOrType(disk) {
			continue
		}
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

	var err error
	// https://github.com/Altinity/clickhouse-backup/issues/910
	if isObjectDiskContainsTables {
		if err = config.ValidateObjectDiskConfig(b.cfg); err != nil {
			return err
		}
	}

	if isObjectDiskContainsTables || (diffFromRemote != "" && b.cfg.General.RemoteStorage != "custom") {
		if err = b.CalculateMaxSize(ctx); err != nil {
			return err
		}
		b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, backupName)
		if err != nil {
			return err
		}
		if err = b.dst.Connect(ctx); err != nil {
			return fmt.Errorf("can't connect to %s: %v", b.dst.Kind(), err)
		}
		defer func() {
			if closeErr := b.dst.Close(ctx); closeErr != nil {
				log.Warn().Msgf("can't close connection to %s: %v", b.dst.Kind(), closeErr)
			}
		}()
		if b.resume {
			b.resumableState = resumable.NewState(b.GetStateDir(), backupName, "create", map[string]interface{}{
				"diffFromRemote": diffFromRemote,
				"tablePattern":   tablePattern,
				"partitions":     partitions,
				"schemaOnly":     schemaOnly,
			})
			defer b.resumableState.Close()
		}

	}
	var tablesDiffFromRemote map[metadata.TableTitle]metadata.TableMetadata
	if diffFromRemote != "" && b.cfg.General.RemoteStorage != "custom" {
		var diffFromRemoteErr error
		tablesDiffFromRemote, diffFromRemoteErr = b.getTablesDiffFromRemote(ctx, diffFromRemote, tablePattern)
		if diffFromRemoteErr != nil {
			return fmt.Errorf("b.getTablesDiffFromRemote return error: %v", diffFromRemoteErr)
		}
	}

	var backupDataSize, backupObjectDiskSize, backupMetadataSize uint64
	var metaMutex sync.Mutex

	// Note: The Parts field in metadata will contain the parts that were backed up,
	// which represents the database state at backup time for in-place restore planning

	createBackupWorkingGroup, createCtx := errgroup.WithContext(ctx)
	createBackupWorkingGroup.SetLimit(max(b.cfg.ClickHouse.MaxConnections, 1))

	var tableMetas []metadata.TableTitle
	for tableIdx, tableItem := range tables {
		//to avoid race condition
		table := tableItem
		if table.Skip {
			continue
		}
		idx := tableIdx
		createBackupWorkingGroup.Go(func() error {
			logger := log.With().Str("table", fmt.Sprintf("%s.%s", table.Database, table.Name)).Logger()
			var realSize, objectDiskSize map[string]int64
			var disksToPartsMap map[string][]metadata.Part
			var checksums map[string]uint64
			var addTableToBackupErr error

			if doBackupData && table.BackupType == clickhouse.ShardBackupFull {
				logger.Debug().Msg("begin data backup")
				shadowBackupUUID := strings.ReplaceAll(uuid.New().String(), "-", "")
				disksToPartsMap, realSize, objectDiskSize, checksums, addTableToBackupErr = b.AddTableToLocalBackup(createCtx, backupName, tablesDiffFromRemote, shadowBackupUUID, disks, &table, partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Name}], skipProjections, version)
				if addTableToBackupErr != nil {
					logger.Error().Msgf("b.AddTableToLocalBackup error: %v", addTableToBackupErr)
					return addTableToBackupErr
				}
				// more precise data size calculation
				for _, size := range realSize {
					atomic.AddUint64(&backupDataSize, uint64(size))
				}
				for _, size := range objectDiskSize {
					atomic.AddUint64(&backupObjectDiskSize, uint64(size))
				}
			}
			// https://github.com/Altinity/clickhouse-backup/issues/529
			logger.Debug().Msg("get in progress mutations list")
			inProgressMutations := make([]metadata.MutationMetadata, 0)
			if b.cfg.ClickHouse.BackupMutations && !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly {
				var inProgressMutationsErr error
				inProgressMutations, inProgressMutationsErr = b.ch.GetInProgressMutations(createCtx, table.Database, table.Name)
				if inProgressMutationsErr != nil {
					logger.Error().Msgf("b.ch.GetInProgressMutations error: %v", inProgressMutationsErr)
					return inProgressMutationsErr
				}
			}
			logger.Debug().Msg("create metadata")
			if schemaOnly || doBackupData {
				metadataSize, createTableMetadataErr := b.createTableMetadata(path.Join(backupPath, "metadata"), metadata.TableMetadata{
					Table:        table.Name,
					Database:     table.Database,
					UUID:         table.UUID,
					Query:        table.CreateTableQuery,
					TotalBytes:   table.TotalBytes,
					Size:         realSize,
					Parts:        disksToPartsMap,
					Checksums:    checksums,
					Mutations:    inProgressMutations,
					MetadataOnly: schemaOnly || table.BackupType == clickhouse.ShardBackupSchema,
				}, disks)
				if createTableMetadataErr != nil {
					logger.Error().Msgf("b.createTableMetadata error: %v", createTableMetadataErr)
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
			logger.Info().Str("progress", fmt.Sprintf("%d/%d", idx+1, len(tables))).Msg("done")
			return nil
		})
	}
	if wgWaitErr := createBackupWorkingGroup.Wait(); wgWaitErr != nil {
		return fmt.Errorf("one of createBackupLocal go-routine return error: %v", wgWaitErr)
	}

	backupMetaFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
	if err := b.createBackupMetadata(ctx, backupMetaFile, backupName, diffFromRemote, backupVersion, "regular", diskMap, diskTypes, disks, backupDataSize, backupObjectDiskSize, backupMetadataSize, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, tableMetas, allDatabases, allFunctions); err != nil {
		return fmt.Errorf("createBackupMetadata return error: %v", err)
	}
	log.Info().Str("version", backupVersion).Str("operation", "createBackupLocal").Str("duration", utils.HumanizeDuration(time.Since(startBackup))).Msg("done")
	return nil
}

func (b *Backuper) createBackupEmbedded(ctx context.Context, backupName, baseBackup string, doBackupData, schemaOnly bool, backupVersion, tablePattern string, partitionsNameList map[metadata.TableTitle][]string, partitionsIdMap map[metadata.TableTitle]common.EmptyMap, tables []clickhouse.Table, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, disks []clickhouse.Disk, diskMap, diskTypes map[string]string, backupRBACSize, backupConfigSize, backupNamedCollectionsSize uint64, startBackup time.Time, version int) error {
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
	var tablesTitle []metadata.TableTitle

	if schemaOnly || doBackupData {
		l := b.CalculateNonSkipTables(tables)
		if l == 0 {
			return fmt.Errorf("`use_embedded_backup_restore: true` not found tables for backup, check your parameter --tables=%v", tablePattern)
		}
		tablesTitle = make([]metadata.TableTitle, l)

		if _, isBackupDiskExists := diskMap[b.cfg.ClickHouse.EmbeddedBackupDisk]; b.cfg.ClickHouse.EmbeddedBackupDisk != "" && !isBackupDiskExists {
			return fmt.Errorf("backup disk `%s` not exists in system.disks", b.cfg.ClickHouse.EmbeddedBackupDisk)
		}
		if b.cfg.ClickHouse.EmbeddedBackupDisk == "" {
			if err := config.ValidateObjectDiskConfig(b.cfg); err != nil {
				return err
			}
		}

		backupSQL, tablesSizeSQL, err := b.generateEmbeddedBackupSQL(ctx, backupName, schemaOnly, tables, tablesTitle, partitionsNameList, l, baseBackup, version)
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
			if backupResult[0].CompressedSize == 0 && backupResult[0].Id != "" {
				systemBackupResult := make([]clickhouse.SystemBackups, 0)
				backupSizeSQL := fmt.Sprintf("SELECT * FROM system.backups WHERE id='%s'", backupResult[0].Id)
				if sizeErr := b.ch.SelectContext(ctx, &systemBackupResult, backupSizeSQL); sizeErr != nil {
					return sizeErr
				}
				if len(systemBackupResult) == 0 && len(systemBackupResult) > 1 {
					return fmt.Errorf("wrong system.backup results: %v", systemBackupResult)
				}
				backupDataSize = append(backupDataSize, clickhouse.BackupDataSize{Size: systemBackupResult[0].CompressedSize})
			} else if backupResult[0].CompressedSize == 0 {
				backupSizeSQL := "SELECT sum(bytes_on_disk) AS backup_data_size FROM system.parts WHERE active AND ("
				for _, t := range tables {
					if oneTableSizeSQL, exists := tablesSizeSQL[metadata.TableTitle{Database: t.Database, Table: t.Name}]; exists {
						if strings.HasPrefix(oneTableSizeSQL, fmt.Sprintf("'%s::%s::", t.Database, t.Name)) {
							backupSizeSQL += fmt.Sprintf(" concat(database,'::',table,'::',partition) IN (%s) OR ", oneTableSizeSQL)
						} else {
							backupSizeSQL += fmt.Sprintf(" concat(database,'::',table) IN (%s) OR ", tablesSizeSQL[metadata.TableTitle{Database: t.Database, Table: t.Name}])
						}
					}
				}
				backupSizeSQL = backupSizeSQL[:len(backupSizeSQL)-4] + ")"
				if sizeErr := b.ch.SelectContext(ctx, &backupDataSize, backupSizeSQL); sizeErr != nil {
					return sizeErr
				}
			} else {
				backupDataSize = append(backupDataSize, clickhouse.BackupDataSize{Size: backupResult[0].CompressedSize})
			}
		}

		if doBackupData && b.cfg.ClickHouse.EmbeddedBackupDisk == "" {
			var err error
			if b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, backupName); err != nil {
				return err
			}
			if err = b.dst.Connect(ctx); err != nil {
				return fmt.Errorf("createBackupEmbedded: can't connect to %s: %v", b.dst.Kind(), err)
			}
			defer func() {
				if closeErr := b.dst.Close(ctx); closeErr != nil {
					log.Warn().Msgf("createBackupEmbedded: can't close connection to %s: %v", b.dst.Kind(), closeErr)
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
						log.Debug().Msgf("calculate parts list `%s`.`%s` from embedded backup disk `%s`", table.Database, table.Name, b.cfg.ClickHouse.EmbeddedBackupDisk)
						disksToPartsMap, err = b.getPartsFromLocalEmbeddedBackupDisk(backupPath, table, partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Name}])
					} else {
						log.Debug().Msgf("calculate parts list `%s`.`%s` from embedded backup remote destination", table.Database, table.Name)
						disksToPartsMap, err = b.getPartsFromRemoteEmbeddedBackup(ctx, backupName, table, partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Name}])
					}
				}
				if err != nil {
					return err
				}
				if schemaOnly || doBackupData {
					metadataSize, err := b.createTableMetadata(path.Join(backupPath, "metadata"), metadata.TableMetadata{
						Table:        table.Name,
						Database:     table.Database,
						UUID:         table.UUID,
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
	if err := b.createBackupMetadata(ctx, backupMetaFile, backupName, baseBackup, backupVersion, "embedded", diskMap, diskTypes, disks, backupDataSize[0].Size, 0, backupMetadataSize, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, tablesTitle, allDatabases, allFunctions); err != nil {
		return err
	}

	log.Info().Fields(map[string]interface{}{
		"operation": "create_embedded",
		"duration":  utils.HumanizeDuration(time.Since(startBackup)),
	}).Msg("done")

	return nil
}

func (b *Backuper) generateEmbeddedBackupSQL(ctx context.Context, backupName string, schemaOnly bool, tables []clickhouse.Table, tablesTitle []metadata.TableTitle, partitionsNameList map[metadata.TableTitle][]string, tablesListLen int, baseBackup string, version int) (string, map[metadata.TableTitle]string, error) {
	tablesSQL := ""
	tableSizeSQL := map[metadata.TableTitle]string{}
	i := 0
	for _, table := range tables {
		if table.Skip {
			continue
		}

		if version > 24004000 && (strings.HasPrefix(table.Name, ".inner.") || strings.HasPrefix(table.Name, ".inner_id.")) {
			log.Warn().Msgf("`%s`.`%s` skipped, 24.4+ version contain bug for EMBEDDED BACKUP/RESTORE for `.inner.` and `.inner_id.` look details in https://github.com/ClickHouse/ClickHouse/issues/67669", table.Database, table.Name)
			tablesListLen -= 1
			continue
		}
		tablesTitle[i] = metadata.TableTitle{
			Database: table.Database,
			Table:    table.Name,
		}
		i += 1

		tablesSQL += "TABLE `" + table.Database + "`.`" + table.Name + "`"
		fullTableNameForTableSizeSQL := table.Database + "::" + table.Name
		tableSizeSQLOneTable := ""
		if nameList, exists := partitionsNameList[metadata.TableTitle{Database: table.Database, Table: table.Name}]; exists && len(nameList) > 0 {
			partitionsSQL := ""
			for _, partitionName := range nameList {
				if partitionName != "*" {
					tableSizeSQLOneTable += "'" + fullTableNameForTableSizeSQL + "::" + partitionName + "',"
					if strings.HasPrefix(partitionName, "(") {
						partitionsSQL += partitionName + ","
					} else {
						partitionsSQL += "'" + partitionName + "',"
					}
				}
			}
			tablesSQL += fmt.Sprintf(" PARTITIONS %s", partitionsSQL[:len(partitionsSQL)-1])
			if tableSizeSQLOneTable != "" {
				tableSizeSQLOneTable = tableSizeSQLOneTable[:len(tableSizeSQLOneTable)-1]
			}
		}
		if tableSizeSQLOneTable == "" {
			tableSizeSQLOneTable = "'" + fullTableNameForTableSizeSQL + "'"
		}
		tableSizeSQL[metadata.TableTitle{Database: table.Database, Table: table.Name}] = tableSizeSQLOneTable
		if i < tablesListLen {
			tablesSQL += ", "
		}
	}
	backupSettings := b.getEmbeddedBackupSettings(version)
	embeddedBackupLocation, err := b.getEmbeddedBackupLocation(ctx, backupName)
	if err != nil {
		return "", nil, err
	}
	backupSQL := fmt.Sprintf("BACKUP %s TO %s", tablesSQL, embeddedBackupLocation)
	if schemaOnly {
		backupSettings = append(backupSettings, "structure_only=1")
	}
	// incremental native backup https://github.com/Altinity/clickhouse-backup/issues/735
	if baseBackup != "" {
		baseBackup, err = b.getEmbeddedBackupLocation(ctx, baseBackup)
		if err != nil {
			return "", nil, err
		}
		backupSettings = append(backupSettings, "base_backup="+baseBackup)
	}
	if len(backupSettings) > 0 {
		backupSQL += " SETTINGS " + strings.Join(backupSettings, ", ")
	}
	return backupSQL, tableSizeSQL, nil
}

func (b *Backuper) getPartsFromRemoteEmbeddedBackup(ctx context.Context, backupName string, table clickhouse.Table, partitionsIdsMap common.EmptyMap) (map[string][]metadata.Part, error) {
	dirListStr := make([]string, 0)
	remoteEmbeddedBackupPath, err := b.getObjectDiskPath()
	if err != nil {
		return nil, err
	}
	remoteEmbeddedBackupPath = path.Join(remoteEmbeddedBackupPath, backupName, "data", common.TablePathEncode(table.Database), common.TablePathEncode(table.Name))
	if walkErr := b.dst.WalkAbsolute(ctx, remoteEmbeddedBackupPath, false, func(ctx context.Context, fInfo storage.RemoteFile) error {
		dirListStr = append(dirListStr, fInfo.Name())
		return nil
	}); walkErr != nil {
		return nil, walkErr
	}
	log.Debug().Msgf("getPartsFromRemoteEmbeddedBackup from %s found %d parts", remoteEmbeddedBackupPath, len(dirListStr))
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
			if strings.Contains(prefix, "*") || strings.Contains(prefix, "?") {
				if matched, err := filepath.Match(dirName, prefix); err == nil && matched {
					found = true
					break
				}
			}
		}
		if found {
			parts[diskName] = append(parts[diskName], metadata.Part{
				Name: dirName,
			})
		}
	}
	return parts, nil
}

func (b *Backuper) createBackupConfigs(ctx context.Context, backupPath string) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		backupConfigSize := uint64(0)
		configBackupPath := path.Join(backupPath, "configs")
		log.Debug().Msgf("copy %s -> %s", b.cfg.ClickHouse.ConfigDir, configBackupPath)
		copyErr := recursiveCopy.Copy(b.cfg.ClickHouse.ConfigDir, configBackupPath, recursiveCopy.Options{
			Skip: func(srcinfo os.FileInfo, src, dest string) (bool, error) {
				backupConfigSize += uint64(srcinfo.Size())
				return false, nil
			},
		})
		return backupConfigSize, copyErr
	}
}

func (b *Backuper) backupSQLFiles(fromPath, toPath string) (uint64, error) {
	log.Debug().Msgf("copy %s -> %s", fromPath, toPath)
	copySize := uint64(0)
	copyErr := recursiveCopy.Copy(fromPath, toPath, recursiveCopy.Options{
		OnDirExists: func(src, dst string) recursiveCopy.DirExistsAction {
			return recursiveCopy.Replace
		},
		Skip: func(srcinfo os.FileInfo, src, dst string) (bool, error) {
			if strings.HasSuffix(src, ".sql") {
				copySize += uint64(srcinfo.Size())
				return false, nil
			} else {
				return true, nil
			}
		},
	})
	return copySize, copyErr
}

func (b *Backuper) createBackupRBAC(ctx context.Context, backupPath string, disks []clickhouse.Disk) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		rbacDataSize := uint64(0)
		rbacBackup := path.Join(backupPath, "access")
		replicatedRBACDataSize, err := b.createBackupRBACReplicated(ctx, rbacBackup)
		if err != nil {
			return 0, fmt.Errorf("b.createBackupRBACReplicated error: %v", err)
		}
		accessPath, err := b.ch.GetAccessManagementPath(ctx, disks)
		if err != nil {
			return 0, fmt.Errorf("b.ch.GetAccessManagementPath error: %v", err)
		}
		accessPathInfo, err := os.Stat(accessPath)
		if err != nil && !os.IsNotExist(err) {
			return rbacDataSize + replicatedRBACDataSize, err
		}
		if err == nil && !accessPathInfo.IsDir() {
			return rbacDataSize + replicatedRBACDataSize, fmt.Errorf("%s is not directory", accessPath)
		}
		if os.IsNotExist(err) {
			return rbacDataSize + replicatedRBACDataSize, nil
		}
		rbacSQLFiles, err := filepath.Glob(path.Join(accessPath, "*.sql"))
		if err != nil {
			return rbacDataSize + replicatedRBACDataSize, err
		}
		if len(rbacSQLFiles) != 0 {
			if copySize, copyErr := b.backupSQLFiles(accessPath, rbacBackup); copyErr != nil {
				return 0, copyErr
			} else {
				rbacDataSize += copySize
			}
		}
		return rbacDataSize + replicatedRBACDataSize, nil
	}
}

func (b *Backuper) createBackupNamedCollections(ctx context.Context, backupPath string) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		namedCollectionsDataSize := uint64(0)
		namedCollectionsBackup := path.Join(backupPath, "named_collections")

		// Parse named_collections_storage from config.xml
		settingsXPath := map[string]string{
			"type":      "//named_collections_storage/type",
			"path":      "//named_collections_storage/path",
			"key_hex":   "//named_collections_storage/key_hex",
			"algorithm": "//named_collections_storage/algorithm",
		}
		settings, err := b.ch.GetPreprocessedXMLSettings(ctx, settingsXPath, "config.xml")
		if err != nil {
			log.Warn().Msgf("can't get named_collections_storage settings from config.xml: %v", err)
			return 0, nil
		}
		settingsFile := path.Join(namedCollectionsBackup, "settings.json")
		settingsJSON, marshalErr := json.Marshal(settings)
		if marshalErr != nil {
			return 0, marshalErr
		}
		if mkDirErr := os.MkdirAll(namedCollectionsBackup, 0755); mkDirErr != nil {
			return 0, mkDirErr
		}
		if writeErr := os.WriteFile(settingsFile, settingsJSON, 0644); writeErr != nil {
			return 0, writeErr
		}

		// Check if type contains "keeper"
		if strings.Contains(strings.ToLower(settings["type"]), "keeper") {
			// Use keeper.Dump from the path
			keeperPath := settings["path"]
			if keeperPath == "" {
				log.Warn().Str("//named_collections_storage/type", settings["type"]).Msg("named_collections_storage path is empty")
				return 0, nil
			}

			k := keeper.Keeper{}
			if err = k.Connect(ctx, b.ch); err != nil {
				return 0, err
			}
			defer k.Close()

			dumpFile := path.Join(namedCollectionsBackup, "named_collections.jsonl")
			log.Info().Str("logger", "createBackupNamedCollections").Msgf("keeper.Dump %s -> %s", keeperPath, dumpFile)
			dumpSize, dumpErr := k.Dump(keeperPath, dumpFile)
			if dumpErr != nil {
				return 0, dumpErr
			}
			namedCollectionsDataSize += uint64(dumpSize)
		} else {
			// Copy *.sql files from {DefaultDataPath}/named_collections/
			namedCollectionsPath := path.Join(b.DefaultDataPath, "named_collections")
			namedCollectionsPathInfo, err := os.Stat(namedCollectionsPath)
			if err != nil && !os.IsNotExist(err) {
				return 0, err
			}
			if err == nil && !namedCollectionsPathInfo.IsDir() {
				return 0, fmt.Errorf("%s is not directory", namedCollectionsPath)
			}
			if os.IsNotExist(err) {
				return 0, nil
			}

			namedCollectionsSQLFiles, err := filepath.Glob(path.Join(namedCollectionsPath, "*.sql"))
			if err != nil {
				return 0, err
			}

			if len(namedCollectionsSQLFiles) != 0 {
				if copySize, copyErr := b.backupSQLFiles(namedCollectionsPath, namedCollectionsBackup); copyErr != nil {
					return 0, copyErr
				} else {
					namedCollectionsDataSize += copySize
				}
			}
		}
		return namedCollectionsDataSize, nil
	}
}

func (b *Backuper) createBackupRBACReplicated(ctx context.Context, rbacBackup string) (replicatedRBACDataSize uint64, err error) {
	replicatedRBAC := make([]struct {
		Name string `ch:"name"`
	}, 0)
	rbacDataSize := uint64(0)
	if err = b.ch.SelectContext(ctx, &replicatedRBAC, "SELECT name FROM system.user_directories WHERE type='replicated'"); err == nil && len(replicatedRBAC) > 0 {
		k := keeper.Keeper{}
		if err = k.Connect(ctx, b.ch); err != nil {
			return 0, err
		}
		defer k.Close()
		for _, userDirectory := range replicatedRBAC {
			replicatedAccessPath, err := k.GetReplicatedAccessPath(userDirectory.Name)
			if err != nil {
				return 0, fmt.Errorf("k.GetReplicatedAccessPath(%s) error: %v", userDirectory.Name, err)
			}
			rbacUUIDObjectsCount, err := k.ChildCount(replicatedAccessPath, "uuid")
			if err != nil {
				return 0, err
			}
			if rbacUUIDObjectsCount == 0 {
				log.Warn().Str("logger", "createBackupRBACReplicated").Msgf("%s/%s have no children, skip Dump", replicatedAccessPath, "uuid")
				continue
			}
			if err = os.MkdirAll(rbacBackup, 0755); err != nil {
				return 0, err
			}
			dumpFile := path.Join(rbacBackup, userDirectory.Name+".jsonl")
			log.Info().Str("logger", "createBackupRBACReplicated").Msgf("keeper.Dump %s -> %s", replicatedAccessPath, dumpFile)
			dumpRBACSize, dumpErr := k.Dump(replicatedAccessPath, dumpFile)
			if dumpErr != nil {
				return 0, dumpErr
			}
			rbacDataSize += uint64(dumpRBACSize)
		}
	}
	return rbacDataSize, nil
}

func (b *Backuper) AddTableToLocalBackup(ctx context.Context, backupName string, tablesDiffFromRemote map[metadata.TableTitle]metadata.TableMetadata, shadowBackupUUID string, diskList []clickhouse.Disk, table *clickhouse.Table, partitionsIdsMap common.EmptyMap, skipProjections []string, version int) (map[string][]metadata.Part, map[string]int64, map[string]int64, map[string]uint64, error) {
	logger := log.With().Fields(map[string]interface{}{
		"backup":    backupName,
		"operation": "create",
		"table":     fmt.Sprintf("%s.%s", table.Database, table.Name),
	}).Logger()
	if backupName == "" {
		return nil, nil, nil, nil, fmt.Errorf("backupName is not defined")
	}

	if !strings.HasSuffix(table.Engine, "MergeTree") && table.Engine != "MaterializedMySQL" && table.Engine != "MaterializedPostgreSQL" {
		if table.Engine != "MaterializedView" {
			logger.Warn().Str("engine", table.Engine).Msg("supports only schema backup")
		}
		return nil, nil, nil, nil, nil
	}
	if b.cfg.ClickHouse.CheckPartsColumns {
		if err := b.ch.CheckSystemPartsColumns(ctx, table); err != nil {
			return nil, nil, nil, nil, err
		}
	}
	// backup data
	if err := b.ch.FreezeTable(ctx, table, shadowBackupUUID); err != nil {
		return nil, nil, nil, nil, err
	}
	log.Debug().Str("database", table.Database).Str("table", table.Name).Msg("frozen")
	realSize := map[string]int64{}
	objectDiskSize := map[string]int64{}
	disksToPartsMap := map[string][]metadata.Part{}
	checksums := make(map[string]uint64)

	for _, disk := range diskList {
		if b.shouldSkipByDiskNameOrType(disk) {
			log.Warn().Str("database", table.Database).Str("table", table.Name).Str("disk.Name", disk.Name).Msg("skipped")
			continue
		}
		select {
		case <-ctx.Done():
			return nil, nil, nil, nil, ctx.Err()
		default:
			shadowPath := path.Join(disk.Path, "shadow", shadowBackupUUID)
			if _, err := os.Stat(shadowPath); err != nil && os.IsNotExist(err) {
				continue
			}
			backupPath := path.Join(disk.Path, "backup", backupName)
			encodedTablePath := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Name))
			backupShadowPath := path.Join(backupPath, "shadow", encodedTablePath, disk.Name)
			if b.resume {
				if dir, err := os.Lstat(backupShadowPath); err == nil && dir.IsDir() {
					log.Warn().Msgf("%s will clean to properly handle resume parameter", backupShadowPath)
					if err = os.RemoveAll(backupShadowPath); err != nil {
						return nil, nil, nil, nil, err
					}
				}
			}
			if err := filesystemhelper.MkdirAll(backupShadowPath, b.ch, diskList); err != nil && !os.IsExist(err) {
				return nil, nil, nil, nil, err
			}
			var diffTableMetadata metadata.TableMetadata
			if tablesDiffFromRemote != nil {
				diffTableMetadata = tablesDiffFromRemote[metadata.TableTitle{Database: table.Database, Table: table.Name}]
			}
			// If partitionsIdsMap is not empty, only parts in this partition will back up.
			parts, size, newChecksums, err := filesystemhelper.MoveShadowToBackup(shadowPath, backupShadowPath, partitionsIdsMap, table, diffTableMetadata, disk, skipProjections, version)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			realSize[disk.Name] = size

			disksToPartsMap[disk.Name] = parts
			for pName, c := range newChecksums {
				checksums[pName] = c
			}

			logger.Debug().Str("disk", disk.Name).Msg("shadow moved")
			if len(parts) > 0 && (b.isDiskTypeObject(disk.Type) || b.isDiskTypeEncryptedObject(disk, diskList)) {
				start := time.Now()
				if size, err = b.uploadObjectDiskParts(ctx, backupName, diffTableMetadata, backupShadowPath, disk); err != nil {
					return nil, nil, nil, nil, err
				}
				objectDiskSize[disk.Name] = size
				if size > 0 {
					log.Info().Str("duration", utils.HumanizeDuration(time.Since(start))).
						Str("database", table.Database).Str("table", table.Name).
						Str("disk", disk.Name).Str("size", utils.FormatBytes(uint64(size))).
						Msg("upload object_disk finish")
				}
			}
			// Clean all the files under the shadowPath, cause UNFREEZE unavailable
			if version < 21004000 {
				if err := os.RemoveAll(shadowPath); err != nil {
					return nil, nil, nil, nil, err
				}
			}
		}
	}
	// Unfreeze to unlock data on S3 disks, https://github.com/Altinity/clickhouse-backup/issues/423
	if version > 21004000 {
		if err := b.ch.QueryContext(ctx, fmt.Sprintf("ALTER TABLE `%s`.`%s` UNFREEZE WITH NAME '%s'", table.Database, table.Name, shadowBackupUUID)); err != nil {
			if (strings.Contains(err.Error(), "code: 60") || strings.Contains(err.Error(), "code: 81") || strings.Contains(err.Error(), "code: 218")) && b.cfg.ClickHouse.IgnoreNotExistsErrorDuringFreeze {
				logger.Warn().Msgf("can't unfreeze table: %v", err)
			}
		}
	}
	log.Debug().Fields(map[string]interface{}{
		"disksToPartsMap": disksToPartsMap, "realSize": realSize, "objectDiskSize": objectDiskSize, "checksums": checksums,
		"operation": "AddTableToLocalBackup",
		"table":     table.Name,
		"database":  table.Database,
	}).Msg("done")
	return disksToPartsMap, realSize, objectDiskSize, checksums, nil
}

func (b *Backuper) uploadObjectDiskParts(ctx context.Context, backupName string, tableDiffFromRemote metadata.TableMetadata, backupShadowPath string, disk clickhouse.Disk) (int64, error) {
	var size int64
	var err error
	if err = object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, disk.Name); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	uploadObjectDiskPartsWorkingGroup, uploadCtx := errgroup.WithContext(ctx)
	// Unify with main upload concurrency to eliminate bottlenecks
	objectDiskConcurrency := b.cfg.GetOptimalObjectDiskConcurrency()
	uploadObjectDiskPartsWorkingGroup.SetLimit(objectDiskConcurrency)
	srcDiskConnection, exists := object_disk.DisksConnections.Load(disk.Name)
	if !exists {
		return 0, fmt.Errorf("uploadObjectDiskParts: %s not present in object_disk.DisksConnections", disk.Name)
	}
	srcBucket := srcDiskConnection.GetRemoteBucket()
	var objectDiskPath string
	if objectDiskPath, err = b.getObjectDiskPath(); err != nil {
		return 0, err
	}
	var isCopyFailed atomic.Bool
	isCopyFailed.Store(false)
	walkErr := filepath.Walk(backupShadowPath, func(fPath string, fInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fInfo.IsDir() {
			return nil
		}
		// fix https://github.com/Altinity/clickhouse-backup/issues/826
		if strings.Contains(fInfo.Name(), "frozen_metadata.txt") {
			return nil
		}
		var realSize, objSize int64
		// upload only not required parts, https://github.com/Altinity/clickhouse-backup/issues/865
		if tableDiffFromRemote.Database != "" && tableDiffFromRemote.Table != "" && len(tableDiffFromRemote.Parts[disk.Name]) > 0 {
			partPaths := strings.SplitN(strings.TrimPrefix(fPath, backupShadowPath), "/", 2)
			for _, part := range tableDiffFromRemote.Parts[disk.Name] {
				if part.Name == partPaths[0] {
					log.Debug().Msgf("%s exists in diff-from-remote backup", part.Name)
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
				var copyObjectErr error
				srcKey := path.Join(srcDiskConnection.GetRemotePath(), storageObject.ObjectRelativePath)
				if b.resume {
					isAlreadyProcesses := false
					isAlreadyProcesses, objSize = b.resumableState.IsAlreadyProcessed(path.Join(srcBucket, srcKey))
					if isAlreadyProcesses {
						continue
					}
				}
				dstKey := path.Join(backupName, disk.Name, storageObject.ObjectRelativePath)
				if !b.cfg.General.AllowObjectDiskStreaming {
					retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
					copyObjectErr = retry.RunCtx(uploadCtx, func(ctx context.Context) error {
						if objSize, err = b.dst.CopyObject(ctx, storageObject.ObjectSize, srcBucket, srcKey, dstKey); err != nil {
							return err
						}
						return nil
					})
					if copyObjectErr != nil {
						return fmt.Errorf("b.dst.CopyObject in %s error: %v", backupShadowPath, copyObjectErr)
					}
				} else {
					if !isCopyFailed.Load() {
						objSize, copyObjectErr = b.dst.CopyObject(ctx, storageObject.ObjectSize, srcBucket, srcKey, dstKey)
						if copyObjectErr != nil {
							log.Warn().Msgf("b.dst.CopyObject in %s error: %v, will try upload via streaming (possible high network traffic)", backupShadowPath, copyObjectErr)
							isCopyFailed.Store(true)
						}
					}
					if isCopyFailed.Load() {
						retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
						copyObjectErr = retry.RunCtx(uploadCtx, func(ctx context.Context) error {
							return object_disk.CopyObjectStreaming(uploadCtx, srcDiskConnection.GetRemoteStorage(), b.dst, srcKey, path.Join(objectDiskPath, dstKey))
						})
						if copyObjectErr != nil {
							return fmt.Errorf("object_disk.CopyObjectStreaming in %s error: %v", backupShadowPath, copyObjectErr)
						}
					}
					objSize = storageObject.ObjectSize
					if b.resume {
						b.resumableState.AppendToState(path.Join(srcBucket, srcKey), objSize)
					}
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

func (b *Backuper) createBackupMetadata(ctx context.Context, backupMetaFile, backupName, requiredBackup, version, tags string, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, backupDataSize, backupObjectDiskSize, backupMetadataSize, backupRBACSize, backupConfigSize, backupNamedCollectionsSize uint64, tableMetas []metadata.TableTitle, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function) error {
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
			CreationDate:            time.Now(),
			Tags:                    tags,
			ClickHouseVersion:       b.ch.GetVersionDescribe(ctx),
			DataSize:                backupDataSize,
			ObjectDiskSize:          backupObjectDiskSize,
			MetadataSize:            backupMetadataSize,
			RBACSize:                backupRBACSize,
			ConfigSize:              backupConfigSize,
			NamedCollectionsSize:    backupNamedCollectionsSize,
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
			log.Warn().Msgf("can't chown %s: %v", backupMetaFile, err)
		}
		log.Debug().Msgf("%s created", backupMetaFile)
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
	log.Debug().Msgf("%s created", metadataFile)
	return uint64(len(metadataBody)), nil
}
