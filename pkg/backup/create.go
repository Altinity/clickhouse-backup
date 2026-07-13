package backup

import (
	"context"
	"encoding/json"
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
	"github.com/pkg/errors"

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
	"github.com/Altinity/clickhouse-backup/v2/pkg/server/metrics"
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
		return errors.Wrap(err, "status.Current.GetContextWithCancel")
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	startBackup := time.Now()
	if backupName == "" {
		backupName = NewBackupName()
	}
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")

	if err := b.ch.Connect(); err != nil {
		return errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()

	clickHouseVersion, versionErr := b.ch.GetVersion(ctx)
	if versionErr != nil {
		return errors.Wrap(versionErr, "b.ch.GetVersion")
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
		return errors.Wrap(err, "can't get database engines from clickhouse")
	}
	tables, err := b.GetTables(ctx, tablePattern)
	if err != nil {
		return errors.Wrap(err, "can't get tables from clickhouse")
	}

	if b.CalculateNonSkipTables(tables) == 0 && !b.cfg.General.AllowEmptyBackups {
		return errors.New("no tables for backup")
	}

	allFunctions, err := b.ch.GetUserDefinedFunctions(ctx)
	if err != nil {
		return errors.Wrap(err, "GetUserDefinedFunctions return error")
	}

	disks, err := b.ch.GetDisks(ctx, false)
	if err != nil {
		return errors.Wrap(err, "b.ch.GetDisks")
	}

	b.DefaultDataPath, err = b.ch.GetDefaultPath(disks)
	if err != nil {
		return errors.Wrap(err, "b.ch.GetDefaultPath")
	}

	diskMap := make(map[string]string, len(disks))
	diskTypes := make(map[string]string, len(disks))
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
		diskTypes[disk.Name] = disk.Type
	}
	partitionsIdMap, partitionsNameList := partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, tables, nil, partitions)
	doBackupData := !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly
	if doBackupData {
		if err = b.checkDisksConsistency(disks); err != nil {
			return err
		}
	}
	backupRBACSize, backupConfigSize, backupNamedCollectionsSize, rbacConfigsNamedCollectionsErr := b.createConfigsNamedCollectionsAndRBACIfNecessary(ctx, backupName, createRBAC, rbacOnly, createConfigs, configsOnly, createNamedCollections, namedCollectionsOnly, disks, diskMap)
	if rbacConfigsNamedCollectionsErr != nil {
		return errors.Wrap(rbacConfigsNamedCollectionsErr, "createConfigsNamedCollectionsAndRBACIfNecessary")
	}
	if b.cfg.ClickHouse.UseEmbeddedBackupRestore {
		if err = b.resolveEmbeddedClusterShardReplica(ctx); err != nil {
			return errors.Wrap(err, "resolveEmbeddedClusterShardReplica")
		}
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
		// fix https://github.com/Altinity/clickhouse-backup/issues/1345 only clean shadow UUIDs created by this backup, don't touch other shadows
		if cleanShadowErr := b.CleanShadowUUIDs(disks); cleanShadowErr != nil {
			log.Error().Msgf("creating failed -> b.CleanShadowUUIDs error: %v", cleanShadowErr)
		}
		return errors.Wrap(err, "createBackup failed")
	}

	// fix https://github.com/Altinity/clickhouse-backup/issues/1345 clean only shadow UUIDs created by this backup
	if cleanShadowErr := b.CleanShadowUUIDs(disks); cleanShadowErr != nil {
		log.Warn().Msgf("b.CleanShadowUUIDs error: %v", cleanShadowErr)
	}
	// Clean
	if err := b.RemoveOldBackupsLocal(ctx, true, disks); err != nil {
		return errors.Wrap(err, "b.RemoveOldBackupsLocal")
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
			log.Fatal().Stack().Msgf("error during do RBAC backup: %v", createRBACErr)
		} else {
			log.Info().Str("size", utils.FormatBytes(backupRBACSize)).Msg("done createBackupRBAC")
		}
	}
	if createConfigs || configsOnly {
		var createConfigsErr error
		if backupConfigSize, createConfigsErr = b.createBackupConfigs(ctx, backupPath); createConfigsErr != nil {
			log.Fatal().Stack().Msgf("error during do CONFIG backup: %v", createConfigsErr)
		} else {
			log.Info().Str("size", utils.FormatBytes(backupConfigSize)).Msg("done createBackupConfigs")
		}
	}
	if createNamedCollections || namedCollectionsOnly {
		var createNamedCollectionsErr error
		if backupNamedCollectionsSize, createNamedCollectionsErr = b.createBackupNamedCollections(ctx, backupPath); createNamedCollectionsErr != nil {
			log.Fatal().Stack().Msgf("error during do NamedCollections backup: %v", createNamedCollectionsErr)
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
			return errors.Wrap(err, "filesystemhelper.Mkdir")
		}
	}
	backupPath := path.Join(b.DefaultDataPath, "backup", backupName)
	if _, err := os.Stat(path.Join(backupPath, "metadata.json")); err == nil || !os.IsNotExist(err) {
		if !b.resume {
			return errors.Errorf("'%s' medatata.json already exists", backupName)
		}
		log.Warn().Msgf("'%s' medatata.json already exists, will overwrite and resume object disk data upload", backupName)
	}
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		if err = filesystemhelper.Mkdir(backupPath, b.ch, disks); err != nil {
			log.Error().Msgf("can't create directory %s: %v", backupPath, err)
			return errors.Wrap(err, "filesystemhelper.Mkdir backupPath")
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
			return errors.Wrap(err, "config.ValidateObjectDiskConfig")
		}
		// Warn if encryption key is set for GCS - object disk files won't be encrypted
		if b.cfg.General.RemoteStorage == "gcs" && b.cfg.GCS.EncryptionKey != "" {
			log.Warn().Msg("GCS_ENCRYPTION_KEY is configured, but files in object_disk path will NOT be encrypted. ClickHouse needs direct unencrypted access to these files for BACKUP/RESTORE operations.")
		}
	}

	if isObjectDiskContainsTables || (diffFromRemote != "" && b.cfg.General.RemoteStorage != "custom") {
		if err = b.CalculateMaxSize(ctx); err != nil {
			return errors.Wrap(err, "b.CalculateMaxSize")
		}
		b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, backupName)
		if err != nil {
			return errors.Wrap(err, "storage.NewBackupDestination")
		}
		if err = b.dst.Connect(ctx); err != nil {
			return errors.Wrapf(err, "can't connect to %s", b.dst.Kind())
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
			return errors.Wrap(diffFromRemoteErr, "b.getTablesDiffFromRemote return error")
		}
	}

	if b.cfg.ClickHouse.CheckPartsColumns && doBackupData {
		tablesToCheck := make([]clickhouse.Table, 0, len(tables))
		for _, table := range tables {
			if !table.Skip && table.BackupType == clickhouse.ShardBackupFull {
				tablesToCheck = append(tablesToCheck, table)
			}
		}
		if err := b.ch.CheckSystemPartsColumnsForTables(ctx, tablesToCheck); err != nil {
			return errors.Wrap(err, "CheckSystemPartsColumnsForTables failed")
		}
		log.Debug().Msgf("CheckSystemPartsColumnsForTables passed for %d tables", len(tablesToCheck))
	}

	// Fetch in-progress mutations ONCE for the whole backup. system.mutations scans all tables on
	// every query, so the previous per-table GetInProgressMutations call was O(N^2) and dominated
	// create time on installations with many tables. We now do a single scan and look up per table.
	var allInProgressMutations map[metadata.TableTitle][]metadata.MutationMetadata
	if b.cfg.ClickHouse.BackupMutations && !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly {
		var allInProgressMutationsErr error
		allInProgressMutations, allInProgressMutationsErr = b.ch.GetInProgressMutationsBatch(ctx)
		if allInProgressMutationsErr != nil {
			return errors.Wrap(allInProgressMutationsErr, "b.ch.GetInProgressMutationsBatch")
		}
	}

	var backupDataSize, backupObjectDiskSize, backupMetadataSize uint64
	// brokenParts/totalParts accumulate the broken vs. total data parts across all tables so that, when
	// general.max_broken_part_ratio > 0, a backup that hit broken parts within the configured threshold is
	// still completed as a partial backup, see https://github.com/Altinity/clickhouse-backup/issues/1418
	var brokenParts, totalParts int64
	var metaMutex sync.Mutex
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
			var hashOfAllFiles map[string]string
			var addTableToBackupErr error
			var tableBrokenParts map[string][]metadata.Part
			if doBackupData && table.BackupType == clickhouse.ShardBackupFull {
				logger.Debug().Msg("begin data backup")
				shadowBackupUUID := strings.ReplaceAll(uuid.New().String(), "-", "")
				b.addShadowBackupUUID(shadowBackupUUID)
				disksToPartsMap, realSize, objectDiskSize, checksums, hashOfAllFiles, tableBrokenParts, addTableToBackupErr = b.AddTableToLocalBackup(createCtx, backupName, tablesDiffFromRemote, shadowBackupUUID, disks, &table, partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Name}], skipProjections, version)
				if addTableToBackupErr != nil {
					logger.Error().Msgf("b.AddTableToLocalBackup error: %v", addTableToBackupErr)
					return errors.Wrap(addTableToBackupErr, "b.AddTableToLocalBackup")
				}
				// account broken and total data parts for the max_broken_part_ratio decision below
				tableBrokenCount := 0
				for _, diskParts := range tableBrokenParts {
					tableBrokenCount += len(diskParts)
				}
				tableTotalParts := tableBrokenCount
				for _, parts := range disksToPartsMap {
					tableTotalParts += len(parts)
				}
				atomic.AddInt64(&brokenParts, int64(tableBrokenCount))
				atomic.AddInt64(&totalParts, int64(tableTotalParts))
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
				// looked up from the single GetInProgressMutationsBatch query above — avoids the
				// O(N^2) per-table system.mutations scan.
				inProgressMutations = allInProgressMutations[metadata.TableTitle{Database: table.Database, Table: table.Name}]
			}
			logger.Debug().Msg("create metadata")
			if schemaOnly || doBackupData {
				metadataSize, createTableMetadataErr := b.createTableMetadata(path.Join(backupPath, "metadata"), metadata.TableMetadata{
					Table:          table.Name,
					Database:       table.Database,
					UUID:           table.UUID,
					Query:          table.CreateTableQuery,
					TotalBytes:     table.TotalBytes,
					Size:           realSize,
					Parts:          disksToPartsMap,
					BrokenParts:    tableBrokenParts,
					Checksums:      checksums,
					HashOfAllFiles: hashOfAllFiles,
					Mutations:      inProgressMutations,
					MetadataOnly:   schemaOnly || table.BackupType == clickhouse.ShardBackupSchema,
				}, disks)
				if createTableMetadataErr != nil {
					logger.Error().Msgf("b.createTableMetadata error: %v", createTableMetadataErr)
					return errors.Wrap(createTableMetadataErr, "b.createTableMetadata")
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
		return errors.Wrap(wgWaitErr, "one of createBackupLocal go-routine return error")
	}

	// max_broken_part_ratio enforcement: when broken parts were tolerated above, fail the whole backup
	// if their share of all data parts exceeds the configured ratio; otherwise complete as a partial
	// backup with a warning, see https://github.com/Altinity/clickhouse-backup/issues/1418
	if broken := atomic.LoadInt64(&brokenParts); broken > 0 {
		total := atomic.LoadInt64(&totalParts)
		if !b.cfg.General.AllowPartialBackup(int(broken), int(total)) {
			return errors.Errorf("backup aborted: %d of %d data parts are broken, ratio %.4f exceeds max_broken_part_ratio %.4f", broken, total, float64(broken)/float64(total), b.cfg.General.MaxBrokenPartRatio)
		}
		// surfaces silent data loss to monitoring in server mode, exposed as clickhouse_backup_failed_parts_count
		metrics.FailedPartsCount.Add(float64(broken))
		log.Warn().Int64("broken_parts", broken).Int64("total_parts", total).Float64("max_broken_part_ratio", b.cfg.General.MaxBrokenPartRatio).Msg("partial backup: some data parts were broken but stayed within max_broken_part_ratio")
	}

	backupMetaFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
	if err := b.createBackupMetadata(ctx, backupMetaFile, backupName, diffFromRemote, backupVersion, "regular", diskMap, diskTypes, disks, backupDataSize, backupObjectDiskSize, backupMetadataSize, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, tableMetas, allDatabases, allFunctions); err != nil {
		return errors.Wrap(err, "createBackupMetadata return error")
	}
	log.Info().Str("version", backupVersion).Str("operation", "createBackupLocal").Str("duration", utils.HumanizeDuration(time.Since(startBackup))).Msg("done")
	return nil
}

func (b *Backuper) createBackupEmbedded(ctx context.Context, backupName, baseBackup string, doBackupData, schemaOnly bool, backupVersion, tablePattern string, partitionsNameList map[metadata.TableTitle][]string, partitionsIdMap map[metadata.TableTitle]common.EmptyMap, tables []clickhouse.Table, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function, disks []clickhouse.Disk, diskMap, diskTypes map[string]string, backupRBACSize, backupConfigSize, backupNamedCollectionsSize uint64, startBackup time.Time, version int) error {
	// TODO: Implement sharded backup operations for embedded backups
	if doesShard(b.cfg.General.ShardedOperationMode) {
		return errors.Wrap(errShardOperationUnsupported, "cannot perform embedded backup")
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
		if l == 0 && !b.cfg.General.AllowEmptyBackups {
			return errors.Errorf("`use_embedded_backup_restore: true` not found tables for backup, check your parameter --tables=%v", tablePattern)
		}
		tablesTitle = make([]metadata.TableTitle, l)
		if l == 0 {
			log.Warn().Msg("empty backup: no tables found, skip embedded backup SQL generation")
			backupDataSize = append(backupDataSize, clickhouse.BackupDataSize{Size: 0})
		}

		if l > 0 {
			if _, isBackupDiskExists := diskMap[b.cfg.ClickHouse.EmbeddedBackupDisk]; b.cfg.ClickHouse.EmbeddedBackupDisk != "" && !isBackupDiskExists {
				return errors.Errorf("backup disk `%s` not exists in system.disks", b.cfg.ClickHouse.EmbeddedBackupDisk)
			}
			if b.cfg.ClickHouse.EmbeddedBackupDisk == "" {
				if err := config.ValidateObjectDiskConfig(b.cfg); err != nil {
					return errors.Wrap(err, "config.ValidateObjectDiskConfig")
				}
				// Warn if encryption key is set for GCS - object disk files won't be encrypted
				if b.cfg.General.RemoteStorage == "gcs" && b.cfg.GCS.EncryptionKey != "" {
					log.Warn().Msg("GCS_ENCRYPTION_KEY is configured, but files in object_disk path will NOT be encrypted. ClickHouse needs direct unencrypted access to these files for BACKUP/RESTORE operations.")
				}
			}

			backupSQL, tablesSizeSQL, err := b.generateEmbeddedBackupSQL(ctx, backupName, schemaOnly, tables, tablesTitle, partitionsNameList, l, baseBackup, version)
			if err != nil {
				return errors.Wrap(err, "b.generateEmbeddedBackupSQL")
			}
			backupResult := make([]clickhouse.SystemBackups, 0)
			if err := b.ch.SelectContext(ctx, &backupResult, backupSQL); err != nil {
				return errors.Wrap(err, "backup error")
			}
			if len(backupResult) != 1 || (backupResult[0].Status != "BACKUP_COMPLETE" && backupResult[0].Status != "BACKUP_CREATED") {
				return errors.Errorf("backup return wrong results: %+v", backupResult)
			}

			if schemaOnly {
				backupDataSize = append(backupDataSize, clickhouse.BackupDataSize{Size: 0})
			} else {
				if backupResult[0].CompressedSize == 0 && backupResult[0].Id != "" {
					systemBackupResult := make([]clickhouse.SystemBackups, 0)
					backupSizeSQL := fmt.Sprintf("SELECT * FROM system.backups WHERE id='%s'", backupResult[0].Id)
					if sizeErr := b.ch.SelectContext(ctx, &systemBackupResult, backupSizeSQL); sizeErr != nil {
						return errors.Wrap(sizeErr, "system.backups query")
					}
					if len(systemBackupResult) == 0 && len(systemBackupResult) > 1 {
						return errors.Errorf("wrong system.backup results: %v", systemBackupResult)
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
						return errors.Wrap(sizeErr, "backup size query")
					}
				} else {
					backupDataSize = append(backupDataSize, clickhouse.BackupDataSize{Size: backupResult[0].CompressedSize})
				}
			}

			if doBackupData && b.cfg.ClickHouse.EmbeddedBackupDisk == "" {
				var err error
				if b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, backupName); err != nil {
					return errors.Wrap(err, "storage.NewBackupDestination")
				}
				if err = b.dst.Connect(ctx); err != nil {
					return errors.Wrapf(err, "createBackupEmbedded: can't connect to %s", b.dst.Kind())
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
							disksToPartsMap, err = b.getPartsFromLocalEmbeddedBackupDisk(path.Join(backupPath, b.embeddedClusterPrefix), table, partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Name}])
						} else {
							log.Debug().Msgf("calculate parts list `%s`.`%s` from embedded backup remote destination", table.Database, table.Name)
							disksToPartsMap, err = b.getPartsFromRemoteEmbeddedBackup(ctx, backupName, table, partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Name}])
						}
					}
					if err != nil {
						return errors.Wrap(err, "getPartsFromEmbeddedBackup")
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
							return errors.Wrap(err, "b.createTableMetadata")
						}
						backupMetadataSize += metadataSize
					}
				}
			}
		} // end if l > 0
	}
	backupMetaFile := path.Join(backupPath, "metadata.json")
	if err := b.createBackupMetadata(ctx, backupMetaFile, backupName, baseBackup, backupVersion, "embedded", diskMap, diskTypes, disks, backupDataSize[0].Size, 0, backupMetadataSize, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, tablesTitle, allDatabases, allFunctions); err != nil {
		return errors.Wrap(err, "b.createBackupMetadata")
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
		return "", nil, errors.Wrap(err, "b.getEmbeddedBackupLocation")
	}
	onCluster := ""
	if b.cfg.ClickHouse.UseEmbeddedBackupRestoreCluster != "" {
		onCluster = " ON CLUSTER '" + b.cfg.ClickHouse.UseEmbeddedBackupRestoreCluster + "'"
	}
	backupSQL := fmt.Sprintf("BACKUP %s %s TO %s", tablesSQL, onCluster, embeddedBackupLocation)
	if schemaOnly {
		backupSettings = append(backupSettings, "structure_only=1")
	}
	// incremental native backup https://github.com/Altinity/clickhouse-backup/issues/735
	if baseBackup != "" {
		baseBackup, err = b.getEmbeddedBackupLocation(ctx, baseBackup)
		if err != nil {
			return "", nil, errors.Wrap(err, "b.getEmbeddedBackupLocation baseBackup")
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
		return nil, errors.Wrap(err, "b.getObjectDiskPath")
	}
	remoteEmbeddedBackupPath = path.Join(remoteEmbeddedBackupPath, backupName, b.embeddedClusterPrefix, "data", common.TablePathEncode(table.Database), common.TablePathEncode(table.Name))
	if walkErr := b.dst.WalkAbsolute(ctx, remoteEmbeddedBackupPath, false, func(ctx context.Context, fInfo storage.RemoteFile) error {
		dirListStr = append(dirListStr, fInfo.Name())
		return nil
	}); walkErr != nil {
		return nil, errors.Wrap(walkErr, "b.dst.WalkAbsolute")
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
		return nil, errors.Wrap(err, "os.ReadDir")
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
			}
			return true, nil
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
			return 0, errors.Wrap(err, "b.createBackupRBACReplicated error")
		}
		accessPath, err := b.ch.GetAccessManagementPath(ctx, disks)
		if err != nil {
			return 0, errors.Wrap(err, "b.ch.GetAccessManagementPath error")
		}
		accessPathInfo, err := os.Stat(accessPath)
		if err != nil && !os.IsNotExist(err) {
			return rbacDataSize + replicatedRBACDataSize, errors.Wrap(err, "os.Stat accessPath")
		}
		if err == nil && !accessPathInfo.IsDir() {
			return rbacDataSize + replicatedRBACDataSize, errors.Errorf("%s is not directory", accessPath)
		}
		if os.IsNotExist(err) {
			return rbacDataSize + replicatedRBACDataSize, nil
		}
		rbacSQLFiles, err := filepath.Glob(path.Join(accessPath, "*.sql"))
		if err != nil {
			return rbacDataSize + replicatedRBACDataSize, errors.Wrap(err, "filepath.Glob accessPath")
		}
		if len(rbacSQLFiles) != 0 {
			if copySize, copyErr := b.backupSQLFiles(accessPath, rbacBackup); copyErr != nil {
				return 0, errors.Wrap(copyErr, "backupSQLFiles rbac")
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
			return 0, errors.Wrap(marshalErr, "json.Marshal namedCollections settings")
		}
		if mkDirErr := os.MkdirAll(namedCollectionsBackup, 0755); mkDirErr != nil {
			return 0, errors.Wrap(mkDirErr, "os.MkdirAll namedCollectionsBackup")
		}
		if writeErr := os.WriteFile(settingsFile, settingsJSON, 0644); writeErr != nil {
			return 0, errors.Wrap(writeErr, "os.WriteFile namedCollections settings")
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
				return 0, errors.Wrap(err, "keeper.Connect")
			}
			defer k.Close()

			dumpFile := path.Join(namedCollectionsBackup, "named_collections.jsonl")
			log.Info().Str("logger", "createBackupNamedCollections").Msgf("keeper.Dump %s -> %s", keeperPath, dumpFile)
			dumpSize, dumpErr := k.Dump(keeperPath, dumpFile)
			if dumpErr != nil {
				return 0, errors.Wrap(dumpErr, "keeper.Dump namedCollections")
			}
			namedCollectionsDataSize += uint64(dumpSize)
		} else {
			// Copy *.sql files from {DefaultDataPath}/named_collections/
			namedCollectionsPath := path.Join(b.DefaultDataPath, "named_collections")
			namedCollectionsPathInfo, err := os.Stat(namedCollectionsPath)
			if err != nil && !os.IsNotExist(err) {
				return 0, errors.Wrap(err, "os.Stat namedCollectionsPath")
			}
			if err == nil && !namedCollectionsPathInfo.IsDir() {
				return 0, errors.Errorf("%s is not directory", namedCollectionsPath)
			}
			if os.IsNotExist(err) {
				return 0, nil
			}

			namedCollectionsSQLFiles, err := filepath.Glob(path.Join(namedCollectionsPath, "*.sql"))
			if err != nil {
				return 0, errors.Wrap(err, "filepath.Glob namedCollections")
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
			return 0, errors.Wrap(err, "keeper.Connect")
		}
		defer k.Close()
		for _, userDirectory := range replicatedRBAC {
			replicatedAccessPath, err := k.GetReplicatedAccessPath(userDirectory.Name)
			if err != nil {
				return 0, errors.Wrapf(err, "k.GetReplicatedAccessPath(%s) error", userDirectory.Name)
			}
			rbacUUIDObjectsCount, err := k.ChildCount(replicatedAccessPath, "uuid")
			if err != nil {
				return 0, errors.Wrap(err, "keeper.ChildCount")
			}
			if rbacUUIDObjectsCount == 0 {
				log.Warn().Str("logger", "createBackupRBACReplicated").Msgf("%s/%s have no children, skip Dump", replicatedAccessPath, "uuid")
				continue
			}
			if err = os.MkdirAll(rbacBackup, 0755); err != nil {
				return 0, errors.Wrap(err, "os.MkdirAll rbacBackup")
			}
			dumpFile := path.Join(rbacBackup, userDirectory.Name+".jsonl")
			log.Info().Str("logger", "createBackupRBACReplicated").Msgf("keeper.Dump %s -> %s", replicatedAccessPath, dumpFile)
			dumpRBACSize, dumpErr := k.Dump(replicatedAccessPath, dumpFile)
			if dumpErr != nil {
				return 0, errors.Wrap(dumpErr, "keeper.Dump rbac")
			}
			rbacDataSize += uint64(dumpRBACSize)
		}
	}
	return rbacDataSize, nil
}

// AddTableToLocalBackup freezes a table and moves its parts into the local backup. It returns the
// per-disk parts/sizes/checksums plus the per-disk data parts that were skipped as broken. Broken parts
// are only tolerated (skipped instead of aborting) when general.max_broken_part_ratio > 0; the caller
// aggregates the broken/total counts across all tables, enforces the configured ratio and stores the
// broken parts in the table metadata, see https://github.com/Altinity/clickhouse-backup/issues/1418
func (b *Backuper) AddTableToLocalBackup(ctx context.Context, backupName string, tablesDiffFromRemote map[metadata.TableTitle]metadata.TableMetadata, shadowBackupUUID string, diskList []clickhouse.Disk, table *clickhouse.Table, partitionsIdsMap common.EmptyMap, skipProjections []string, version int) (map[string][]metadata.Part, map[string]int64, map[string]int64, map[string]uint64, map[string]string, map[string][]metadata.Part, error) {
	logger := log.With().Fields(map[string]interface{}{
		"backup":    backupName,
		"operation": "create",
		"table":     fmt.Sprintf("%s.%s", table.Database, table.Name),
	}).Logger()
	if backupName == "" {
		return nil, nil, nil, nil, nil, nil, errors.New("backupName is not defined")
	}

	if !strings.HasSuffix(table.Engine, "MergeTree") && table.Engine != "MaterializedMySQL" && table.Engine != "MaterializedPostgreSQL" {
		if table.Engine != "MaterializedView" {
			logger.Warn().Str("engine", table.Engine).Msg("supports only schema backup")
		}
		return nil, nil, nil, nil, nil, nil, nil
	}
	if err := b.ch.FreezeTable(ctx, table, shadowBackupUUID); err != nil {
		return nil, nil, nil, nil, nil, nil, errors.Wrap(err, "b.ch.FreezeTable")
	}
	// brokenParts collects, per disk, the data parts that could not be moved/uploaded into the backup
	// but were tolerated because general.max_broken_part_ratio > 0; they end up in the broken_parts
	// section of the table metadata, see https://github.com/Altinity/clickhouse-backup/issues/1418
	brokenParts := map[string][]metadata.Part{}
	allowBrokenParts := b.cfg.General.MaxBrokenPartRatio > 0
	log.Debug().Str("database", table.Database).Str("table", table.Name).Msg("frozen")
	realSize := map[string]int64{}
	objectDiskSize := map[string]int64{}
	disksToPartsMap := map[string][]metadata.Part{}
	checksums := make(map[string]uint64)
	hashOfAllFiles := make(map[string]string)

	for _, disk := range diskList {
		if b.shouldSkipByDiskNameOrType(disk) {
			log.Warn().Str("database", table.Database).Str("table", table.Name).Str("disk.Name", disk.Name).Msg("skipped")
			continue
		}
		select {
		case <-ctx.Done():
			return nil, nil, nil, nil, nil, nil, ctx.Err()
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
						return nil, nil, nil, nil, nil, nil, errors.Wrap(err, "os.RemoveAll backupShadowPath")
					}
				}
			}
			if err := filesystemhelper.MkdirAll(backupShadowPath, b.ch, diskList); err != nil && !os.IsExist(err) {
				return nil, nil, nil, nil, nil, nil, errors.Wrap(err, "filesystemhelper.MkdirAll backupShadowPath")
			}
			var diffTableMetadata metadata.TableMetadata
			if tablesDiffFromRemote != nil {
				diffTableMetadata = tablesDiffFromRemote[metadata.TableTitle{Database: table.Database, Table: table.Name}]
			}
			// If partitionsIdsMap is not empty, only parts in this partition will back up.
			start := time.Now()
			useHashOfAllFiles := version >= 19011000
			// Old CH still gets the legacy CRC64-of-checksums.txt path. For modern
			// CH we let MoveShadowToBackup skip CRC64 entirely and pull
			// hash_of_all_files from system.parts after the shadow tree is walked
			// (see post-loop SELECT below).
			parts, size, newChecksums, err := filesystemhelper.MoveShadowToBackup(shadowPath, backupShadowPath, partitionsIdsMap, table, diffTableMetadata, disk, skipProjections, version, !useHashOfAllFiles)
			if err != nil {
				if !allowBrokenParts {
					return nil, nil, nil, nil, nil, nil, errors.Wrap(err, "filesystemhelper.MoveShadowToBackup")
				}
				// max_broken_part_ratio > 0: attribute every part frozen on this disk as broken, skip the
				// disk and let the caller decide whether the aggregate ratio is acceptable,
				// see https://github.com/Altinity/clickhouse-backup/issues/1418
				diskBroken := listShadowParts(shadowPath, partitionsIdsMap)
				if len(diskBroken) == 0 {
					// best-effort: the shadow tree is unreadable, still count at least one broken part
					diskBroken = []metadata.Part{{Name: "unknown"}}
				}
				brokenParts[disk.Name] = append(brokenParts[disk.Name], diskBroken...)
				logger.Warn().Err(err).Str("disk", disk.Name).Int("broken_parts", len(diskBroken)).Msg("filesystemhelper.MoveShadowToBackup failed, counting parts as broken (max_broken_part_ratio > 0)")
				// remove partially moved files so the backup keeps no orphan parts absent from the
				// table metadata (shadow files are hardlinks, table data is untouched)
				if removeErr := os.RemoveAll(backupShadowPath); removeErr != nil {
					return nil, nil, nil, nil, nil, nil, errors.Wrap(removeErr, "os.RemoveAll backupShadowPath after MoveShadowToBackup failure")
				}
				continue
			}
			realSize[disk.Name] = size

			disksToPartsMap[disk.Name] = parts
			for pName, c := range newChecksums {
				checksums[pName] = c
			}
			if useHashOfAllFiles && len(parts) > 0 {
				partNames := make([]string, 0, len(parts))
				for _, p := range parts {
					partNames = append(partNames, p.Name)
				}
				diskHashes, hashErr := b.fetchHashOfAllFiles(ctx, table.Database, table.Name, disk.Name, partNames)
				if hashErr != nil {
					return nil, nil, nil, nil, nil, nil, errors.Wrap(hashErr, "fetchHashOfAllFiles")
				}
				for pName, h := range diskHashes {
					hashOfAllFiles[pName] = h
				}
			}

			// Validate parts marked Required by name against the diff source's
			// content fingerprint. If hash_of_all_files (or, when absent on
			// either side, checksums) disagree, demote the part and link its
			// files from shadow so it is backed up locally.
			// See https://github.com/Altinity/clickhouse-backup/issues/1307
			if diffTableMetadata.Database != "" && len(diffTableMetadata.Parts[disk.Name]) > 0 {
				for idx := range parts {
					if !parts[idx].Required {
						continue
					}
					name := parts[idx].Name
					matches := true
					if diffHash, ok := diffTableMetadata.HashOfAllFiles[name]; ok {
						liveHash := hashOfAllFiles[name]
						matches = liveHash != "" && diffHash == liveHash
					} else if diffCksum, ok := diffTableMetadata.Checksums[name]; ok {
						liveCksum, liveOk := newChecksums[name]
						matches = liveOk && diffCksum == liveCksum
					}
					if matches {
						continue
					}
					logger.Info().Str("disk", disk.Name).Str("part", name).Msg("part name matched diff source but content fingerprint mismatch, backing up locally")
					parts[idx].Required = false
					linkedSize, linkErr := filesystemhelper.LinkPartFromShadow(shadowPath, backupShadowPath, name, table, skipProjections, version)
					if linkErr != nil {
						return nil, nil, nil, nil, nil, nil, errors.Wrapf(linkErr, "LinkPartFromShadow part %s", name)
					}
					realSize[disk.Name] += linkedSize
				}
			}

			logger.Debug().Str("disk", disk.Name).Str("duration", utils.HumanizeDuration(time.Since(start))).Msg("shadow moved")
			if len(parts) > 0 && (b.isDiskTypeObject(disk.Type) || b.isDiskTypeEncryptedObject(disk, diskList)) {
				start = time.Now()
				log.Info().
					Str("database", table.Database).Str("table", table.Name).
					Str("disk", disk.Name).Str("size", utils.FormatBytes(uint64(size))).
					Msg("upload object_disk start")
				var brokenObjectDiskParts map[string]struct{}
				if size, brokenObjectDiskParts, err = b.uploadObjectDiskParts(ctx, backupName, parts, backupShadowPath, disk); err != nil {
					return nil, nil, nil, nil, nil, nil, errors.Wrap(err, "b.uploadObjectDiskParts")
				}
				if len(brokenObjectDiskParts) > 0 {
					// max_broken_part_ratio > 0: drop only the parts whose object storage blobs could not be
					// copied and count them as broken; parts marked Required are never copied here (restore
					// pulls their unchanged data from the diff base backup) so they are never dropped,
					// see https://github.com/Altinity/clickhouse-backup/issues/1418
					keptParts := make([]metadata.Part, 0, len(parts))
					for _, p := range parts {
						if _, broken := brokenObjectDiskParts[p.Name]; broken {
							brokenParts[disk.Name] = append(brokenParts[disk.Name], p)
							continue
						}
						keptParts = append(keptParts, p)
					}
					// remove the broken parts' local metadata files so the backup contains no orphan
					// parts absent from the table metadata, and drop their fingerprints
					for partName := range brokenObjectDiskParts {
						if removeErr := os.RemoveAll(path.Join(backupShadowPath, partName)); removeErr != nil {
							return nil, nil, nil, nil, nil, nil, errors.Wrapf(removeErr, "os.RemoveAll broken part %s", partName)
						}
						delete(checksums, partName)
						delete(hashOfAllFiles, partName)
					}
					logger.Warn().Str("disk", disk.Name).Int("broken_parts", len(brokenObjectDiskParts)).Int("kept_parts", len(keptParts)).Msg("some object disk parts are broken and skipped (max_broken_part_ratio > 0)")
					parts = keptParts
					if len(parts) > 0 {
						disksToPartsMap[disk.Name] = parts
					} else {
						delete(disksToPartsMap, disk.Name)
					}
				}
				objectDiskSize[disk.Name] = size
				if size > 0 {
					log.Info().
						Str("duration", utils.HumanizeDuration(time.Since(start))).
						Str("database", table.Database).Str("table", table.Name).
						Str("disk", disk.Name).Str("size", utils.FormatBytes(uint64(size))).
						Msg("upload object_disk finish")
				}
			}
			// Clean all the files under the shadowPath, cause UNFREEZE unavailable
			if version < 21004000 {
				if err := os.RemoveAll(shadowPath); err != nil {
					return nil, nil, nil, nil, nil, nil, errors.Wrap(err, "os.RemoveAll shadowPath")
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
	return disksToPartsMap, realSize, objectDiskSize, checksums, hashOfAllFiles, brokenParts, nil
}

// listShadowParts walks a frozen shadow directory and lists data-part directories, mirroring the
// depth-4 part detection (and --partitions filter) in filesystemhelper.MoveShadowToBackup. It is a
// best-effort list used to attribute broken parts when a shadow move fails and
// general.max_broken_part_ratio allows a partial backup. partitionsBackupMap, when non-empty, restricts
// the list to the requested partitions so unrequested parts are not reported as broken,
// see https://github.com/Altinity/clickhouse-backup/issues/1418
func listShadowParts(shadowPath string, partitionsBackupMap common.EmptyMap) []metadata.Part {
	parts := make([]metadata.Part, 0)
	_ = filepath.Walk(shadowPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil || info == nil || !info.IsDir() {
			return nil
		}
		relativePath := strings.Trim(strings.TrimPrefix(filePath, shadowPath), "/")
		pathParts := strings.SplitN(relativePath, "/", 4)
		if len(pathParts) != 4 || strings.Contains(pathParts[3], "/") {
			return nil
		}
		if pathParts[3] == "detached" {
			return filepath.SkipDir
		}
		if strings.HasSuffix(pathParts[3], ".proj") {
			return nil
		}
		if len(partitionsBackupMap) != 0 && !filesystemhelper.IsPartInPartition(pathParts[3], partitionsBackupMap) {
			return filepath.SkipDir
		}
		parts = append(parts, metadata.Part{Name: pathParts[3]})
		return filepath.SkipDir
	})
	metadata.SortPartsByMinBlock(parts)
	return parts
}

// fetchHashOfAllFiles returns name → hash_of_all_files for the given parts of
// `database`.`table` (frozen from disk `diskName`). The value is whatever
// ClickHouse prints in system.parts.hash_of_all_files (already lowercased); the
// storage and compare paths are version-agnostic because both ends use the
// server's formatting.
//
// We deliberately do NOT filter by disk_name: when a cache disk wraps an object
// disk, both share the same metadata path, and system.parts reports the part
// under the cache disk name (the disk named in the storage policy), not the
// underlying disk that clickhouse-backup walks the shadow tree from. Filtering
// by diskName would miss those parts (see issue #1396). Part names are unique
// within a table's active set, and inactive copies left by a move carry
// identical content (hence identical hash_of_all_files), so matching by
// database+table+name is unambiguous.
//
// Called right after FREEZE so the active-set delta is essentially zero —
// inactive parts also remain visible in system.parts for ~480s, which makes
// the race window practically impossible to hit. If a frozen part is missing
// from the result we surface a hard error rather than silently dropping it,
// unless the table itself was dropped/detached concurrently (the documented
// IgnoreNotExistsErrorDuringFreeze race), in which case we skip the part.
func (b *Backuper) fetchHashOfAllFiles(ctx context.Context, database, table, diskName string, partNames []string) (map[string]string, error) {
	var rows []struct {
		Name string `ch:"name"`
		Hash string `ch:"hash_of_all_files"`
	}
	// https://github.com/Altinity/clickhouse-backup/issues/1408
	q := "SELECT name, lower(hash_of_all_files) AS hash_of_all_files FROM system.parts WHERE database=? AND `table`=?"
	if err := b.ch.SelectContext(ctx, &rows, q, database, table); err != nil {
		return nil, errors.Wrap(err, "SELECT hash_of_all_files FROM system.parts")
	}
	hashByName := make(map[string]string, len(rows))
	for _, r := range rows {
		hashByName[r.Name] = r.Hash
	}
	for _, name := range partNames {
		if _, ok := hashByName[name]; !ok {
			// A part can vanish from system.parts after FREEZE for several reasons:
			// the table was dropped/detached concurrently, or a background merge/move
			// (common on object-storage tiers like s3_cold) replaced the part right
			// after we froze it. In every case the frozen files are already safe in
			// shadow, so this is not a backup failure — we just lack a
			// hash_of_all_files fingerprint for this part. Skip it with a warning;
			// download/restore fall back to checksums and then to name-only
			// comparison when no fingerprint is available.
			log.Warn().Msgf("part %q not found in system.parts (database=%s, table=%s, disk_name=%s) after FREEZE, skip hash_of_all_files (fall back to checksums/name comparison)", name, database, table, diskName)
			continue
		}
	}
	return hashByName, nil
}

// uploadObjectDiskParts server-side copies the object storage blobs referenced by the frozen part
// metadata files under backupShadowPath into the backup bucket. It returns the copied size plus the
// set of data parts whose blobs could not be copied: with general.max_broken_part_ratio > 0 a per-part
// copy failure is tolerated (the part is reported broken instead of aborting), otherwise any failure
// returns an error, see https://github.com/Altinity/clickhouse-backup/issues/1418
func (b *Backuper) uploadObjectDiskParts(ctx context.Context, backupName string, localParts []metadata.Part, backupShadowPath string, disk clickhouse.Disk) (int64, map[string]struct{}, error) {
	var err error
	if err = object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, disk.Name); err != nil {
		return 0, nil, errors.Wrap(err, "object_disk.InitCredentialsAndConnections")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	uploadObjectDiskPartsWorkingGroup, uploadCtx := errgroup.WithContext(ctx)
	uploadObjectDiskPartsWorkingGroup.SetLimit(int(b.cfg.General.ObjectDiskServerSideCopyConcurrency))
	srcDiskConnection, exists := object_disk.DisksConnections.Load(disk.Name)
	if !exists {
		return 0, nil, errors.Errorf("uploadObjectDiskParts: %s not present in object_disk.DisksConnections", disk.Name)
	}
	srcBucket := srcDiskConnection.GetRemoteBucket()
	var objectDiskPath string
	if objectDiskPath, err = b.getObjectDiskPath(); err != nil {
		return 0, nil, errors.Wrap(err, "b.getObjectDiskPath")
	}
	var isCopyFailed atomic.Bool
	isCopyFailed.Store(false)
	// sizes and broken markers are tracked per data part so that, when
	// general.max_broken_part_ratio > 0 tolerates copy failures, only the affected part is dropped
	// and its already-copied size is excluded, see https://github.com/Altinity/clickhouse-backup/issues/1418
	allowBrokenParts := b.cfg.General.MaxBrokenPartRatio > 0
	var partsMu sync.Mutex
	partSizes := make(map[string]int64)
	brokenParts := make(map[string]struct{})
	isPartBroken := func(partName string) bool {
		partsMu.Lock()
		defer partsMu.Unlock()
		_, broken := brokenParts[partName]
		return broken
	}
	markPartBroken := func(partName string, cause error) {
		partsMu.Lock()
		brokenParts[partName] = struct{}{}
		partsMu.Unlock()
		log.Warn().Err(cause).Str("disk", disk.Name).Str("part", partName).Msg("can't copy object disk data, part marked as broken (max_broken_part_ratio > 0)")
	}
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
		// first path element under backupShadowPath is the data part directory name
		partName := strings.SplitN(strings.Trim(strings.TrimPrefix(fPath, backupShadowPath), "/"), "/", 2)[0]
		var realSize, objSize int64
		// upload only not required parts, https://github.com/Altinity/clickhouse-backup/issues/865
		// localParts already reflects the post-demotion Required flag from the
		// hash_of_all_files / checksums comparison done in AddTableToLocalBackup
		// (see https://github.com/Altinity/clickhouse-backup/issues/1307), so a
		// Required=true entry here means the part's content also matched the
		// diff source and the diff backup will provide the object-disk blobs.
		if len(localParts) > 0 {
			for _, part := range localParts {
				if part.Required && part.Name == partName {
					log.Debug().Msgf("%s exists in diff-from-remote backup", part.Name)
					return nil
				}
			}
		}
		uploadObjectDiskPartsWorkingGroup.Go(func() error {
			if allowBrokenParts && isPartBroken(partName) {
				return nil
			}
			objMeta, readMetadataErr := object_disk.ReadMetadataFromFile(fPath)
			if readMetadataErr != nil {
				if allowBrokenParts {
					markPartBroken(partName, errors.Wrap(readMetadataErr, "object_disk.ReadMetadataFromFile"))
					return nil
				}
				return errors.Wrap(readMetadataErr, "object_disk.ReadMetadataFromFile")
			}
			for _, storageObject := range objMeta.StorageObjects {
				if storageObject.ObjectSize == 0 {
					continue
				}
				var copyObjectErr error

				srcKey := path.Join(srcDiskConnection.GetRemotePath(), storageObject.ObjectPath)

				if b.resume {
					isAlreadyProcesses := false
					var resumeErr error
					isAlreadyProcesses, objSize, resumeErr = b.resumableState.IsAlreadyProcessed(path.Join(srcBucket, srcKey))
					if resumeErr != nil {
						return errors.Wrap(resumeErr, "resumableState.IsAlreadyProcessed")
					}
					if isAlreadyProcesses {
						continue
					}
				}
				dstKey := path.Join(objectDiskPath, backupName, disk.Name, storageObject.ObjectPath)
				if !b.cfg.General.AllowObjectDiskStreaming {
					retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
					copyObjectErr = retry.RunCtx(uploadCtx, func(ctx context.Context) error {
						if objSize, err = b.dst.CopyObject(ctx, storageObject.ObjectSize, srcBucket, srcKey, dstKey); err != nil {
							return err
						}
						return nil
					})
					if copyObjectErr != nil {
						if allowBrokenParts {
							markPartBroken(partName, errors.Wrapf(copyObjectErr, "b.dst.CopyObject in %s for srcKey=%s error", fPath, srcKey))
							return nil
						}
						return errors.Wrapf(copyObjectErr, "b.dst.CopyObject in %s for srcKey=%s error", fPath, srcKey)
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
							return object_disk.CopyObjectStreaming(uploadCtx, srcDiskConnection.GetRemoteStorage(), b.dst, srcKey, dstKey, b.dst.UploadLimiter(b.cfg.General.UploadMaxBytesPerSecond))
						})
						if copyObjectErr != nil {
							if allowBrokenParts {
								markPartBroken(partName, errors.Wrapf(copyObjectErr, "object_disk.CopyObjectStreaming in %s for srcKey=%s error", fPath, srcKey))
								return nil
							}
							return errors.Wrapf(copyObjectErr, "object_disk.CopyObjectStreaming in %s for srcKey=%s error", fPath, srcKey)
						}
					}
					objSize = storageObject.ObjectSize
					if b.resume {
						if appendErr := b.resumableState.AppendToState(path.Join(srcBucket, srcKey), objSize); appendErr != nil {
							return errors.Wrap(appendErr, "resumableState.AppendToState")
						}
					}
				}
				realSize += objSize
			}
			partsMu.Lock()
			if realSize > objMeta.TotalSize {
				partSizes[partName] += realSize
			} else {
				partSizes[partName] += objMeta.TotalSize
			}
			partsMu.Unlock()
			return nil
		})
		return nil
	})
	if walkErr != nil {
		return 0, nil, errors.Wrap(walkErr, "filepath.Walk backupShadowPath")
	}

	if wgWaitErr := uploadObjectDiskPartsWorkingGroup.Wait(); wgWaitErr != nil {
		return 0, nil, errors.Wrap(wgWaitErr, "one of uploadObjectDiskParts go-routine return error")
	}
	var size int64
	for partName, partSize := range partSizes {
		if _, broken := brokenParts[partName]; !broken {
			size += partSize
		}
	}
	return size, brokenParts, nil
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
			return errors.Wrap(err, "can't marshal backup metafile json")
		}
		if err := os.WriteFile(backupMetaFile, content, 0640); err != nil {
			return errors.Wrap(err, "os.WriteFile backupMetaFile")
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
		return 0, errors.Wrap(err, "filesystemhelper.Mkdir metadataPath")
	}
	metadataDatabasePath := path.Join(metadataPath, common.TablePathEncode(table.Database))
	if err := filesystemhelper.Mkdir(metadataDatabasePath, b.ch, disks); err != nil {
		return 0, errors.Wrap(err, "filesystemhelper.Mkdir metadataDatabasePath")
	}
	metadataFile := path.Join(metadataDatabasePath, fmt.Sprintf("%s.json", common.TablePathEncode(table.Table)))
	metadataBody, err := json.MarshalIndent(&table, "", " ")
	if err != nil {
		return 0, errors.Wrapf(err, "can't marshal %s", MetaFileName)
	}
	if err := os.WriteFile(metadataFile, metadataBody, 0644); err != nil {
		return 0, errors.Wrapf(err, "can't create %s", MetaFileName)
	}
	if err := filesystemhelper.Chown(metadataFile, b.ch, disks, false); err != nil {
		return 0, errors.Wrap(err, "filesystemhelper.Chown metadataFile")
	}
	log.Debug().Msgf("%s created", metadataFile)
	return uint64(len(metadataBody)), nil
}
