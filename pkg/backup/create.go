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
	if err := b.validateEmbeddedOnClusterWorker(); err != nil {
		return err
	}
	if b.EmbeddedOnClusterWorker {
		// rbac, configs and named collections are backed up by the initiator node only, https://github.com/Altinity/clickhouse-backup/issues/928
		createRBAC, createConfigs, createNamedCollections = false, false, false
	}
	b.adjustResumeFlag(resume)

	p, err := b.createPrologue(ctx, tablePattern, partitions, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly)
	if err != nil {
		return err
	}
	allDatabases, allFunctions, tables, disks := p.allDatabases, p.allFunctions, p.tables, p.disks
	diskMap, diskTypes, partitionsIdMap, partitionsNameList, doBackupData := p.diskMap, p.diskTypes, p.partitionsIdMap, p.partitionsNameList, p.doBackupData
	if b.DryRun {
		dryRunReport, dryRunErr := b.buildCreateDryRunReport(ctx, backupName, tables, disks, partitionsIdMap, doBackupData, rbacOnly, configsOnly, namedCollectionsOnly)
		if dryRunErr != nil {
			return dryRunErr
		}
		b.setDryRunResult(dryRunReport)
		return nil
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
		// fix corner cases after https://github.com/Altinity/clickhouse-backup/issues/379
		// fix https://github.com/Altinity/clickhouse-backup/issues/1345 only clean shadow UUIDs created by this backup, don't touch other shadows
		// runs before RemoveBackupLocal, which deletes <backup_name>/freezes.tmp together with the backup directory
		if cleanShadowErr := b.cleanOwnFreezes(disks); cleanShadowErr != nil {
			log.Error().Msgf("creating failed -> b.cleanOwnFreezes error: %v", cleanShadowErr)
		}
		// delete local backup when creation failure
		if removeBackupErr := b.RemoveBackupLocal(context.WithoutCancel(ctx), backupName, disks, true); removeBackupErr != nil {
			log.Error().Msgf("creating failed -> b.RemoveBackupLocal error: %v", removeBackupErr)
		}
		return errors.Wrap(err, "createBackup failed")
	}
	// BACKUP ... ON CLUSTER already wrote the shards/N/replicas/M data of every node, now each worker node describes its
	// own part with clickhouse-backup metadata, https://github.com/Altinity/clickhouse-backup/issues/928
	if b.embeddedClusterInitiator() && !b.skipEmbeddedClusterFanOut && !rbacOnly && !configsOnly && !namedCollectionsOnly {
		if err := b.runEmbeddedClusterWorkers(ctx, embeddedClusterCreateWorkerCommand("create", backupName, diffFromRemote, tablePattern, partitions, schemaOnly, skipCheckPartsColumns, false)); err != nil {
			return errors.Wrap(err, "embedded ON CLUSTER workers")
		}
	}

	// fix https://github.com/Altinity/clickhouse-backup/issues/1345 clean only shadow UUIDs created by this backup
	if cleanShadowErr := b.cleanOwnFreezes(disks); cleanShadowErr != nil {
		log.Warn().Msgf("b.cleanOwnFreezes error: %v", cleanShadowErr)
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

// createPrologueResult holds everything CreateBackup collects from ClickHouse before the backup is created
type createPrologueResult struct {
	allDatabases       []clickhouse.Database
	allFunctions       []clickhouse.Function
	tables             []clickhouse.Table
	disks              []clickhouse.Disk
	diskMap            map[string]string
	diskTypes          map[string]string
	partitionsIdMap    map[metadata.TableTitle]common.EmptyMap
	partitionsNameList map[metadata.TableTitle][]string
	doBackupData       bool
}

// createPrologue collects databases, tables, functions, disks and partitions for the backup,
// sets b.DefaultDataPath and checks disks consistency when table data is going to be backed up
func (b *Backuper) createPrologue(ctx context.Context, tablePattern string, partitions []string, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly bool) (*createPrologueResult, error) {
	allDatabases, err := b.ch.GetDatabases(ctx, b.cfg, tablePattern)
	if err != nil {
		return nil, errors.Wrap(err, "can't get database engines from clickhouse")
	}
	tables, err := b.GetTables(ctx, tablePattern)
	if err != nil {
		return nil, errors.Wrap(err, "can't get tables from clickhouse")
	}

	if b.CalculateNonSkipTables(tables) == 0 && !b.cfg.General.AllowEmptyBackups {
		return nil, errors.New("no tables for backup")
	}

	allFunctions, err := b.ch.GetUserDefinedFunctions(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "GetUserDefinedFunctions return error")
	}

	disks, err := b.ch.GetDisks(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "b.ch.GetDisks")
	}

	b.DefaultDataPath, err = b.ch.GetDefaultPath(disks)
	if err != nil {
		return nil, errors.Wrap(err, "b.ch.GetDefaultPath")
	}

	diskMap := make(map[string]string, len(disks))
	diskTypes := make(map[string]string, len(disks))
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
		diskTypes[disk.Name] = disk.Type
	}
	partitionsIdMap, partitionsNameList, err := partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, tables, nil, partitions)
	if err != nil {
		return nil, err
	}
	doBackupData := !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly
	if doBackupData {
		if err = b.checkDisksConsistency(disks); err != nil {
			return nil, err
		}
	}
	return &createPrologueResult{
		allDatabases:       allDatabases,
		allFunctions:       allFunctions,
		tables:             tables,
		disks:              disks,
		diskMap:            diskMap,
		diskTypes:          diskTypes,
		partitionsIdMap:    partitionsIdMap,
		partitionsNameList: partitionsNameList,
		doBackupData:       doBackupData,
	}, nil
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
	backupPath, err := b.createBackupDirs(backupName, disks)
	if err != nil {
		return err
	}
	isObjectDiskContainsTables := b.isObjectDiskContainsTables(tables, disks, doBackupData)
	if err = b.validateObjectDiskConfigIfNecessary(isObjectDiskContainsTables); err != nil {
		return err
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
				"configHash":     resumableConfigHash(b.cfg, "create"),
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

	allInProgressMutations, err := b.checkPartsColumnsAndGetInProgressMutations(ctx, tables, doBackupData, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly)
	if err != nil {
		return err
	}

	// https://github.com/Altinity/clickhouse-backup/issues/1120
	kafkaKeeper, attachKafkaTables, err := b.backupKafkaKeeperOffsets(ctx, tables, doBackupData)
	if err != nil {
		return errors.Wrap(err, "b.backupKafkaKeeperOffsets")
	}
	defer func() {
		// errors are already logged with the ATTACH statement for manual run
		_ = attachKafkaTables()
	}()

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
			tableMeta, tableBackupSize, createOneTableErr := b.createOneTable(createCtx, backupName, backupPath, &table, tablesDiffFromRemote, partitionsIdMap, allInProgressMutations, kafkaKeeper, skipProjections, disks, doBackupData, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, version)
			if createOneTableErr != nil {
				return createOneTableErr
			}
			atomic.AddInt64(&brokenParts, tableBackupSize.brokenParts)
			atomic.AddInt64(&totalParts, tableBackupSize.totalParts)
			atomic.AddUint64(&backupDataSize, tableBackupSize.dataSize)
			atomic.AddUint64(&backupObjectDiskSize, tableBackupSize.objectDiskSize)
			atomic.AddUint64(&backupMetadataSize, tableBackupSize.metadataSize)
			if tableMeta != nil {
				metaMutex.Lock()
				tableMetas = append(tableMetas, metadata.TableTitle{
					Database: tableMeta.Database,
					Table:    tableMeta.Table,
				})
				metaMutex.Unlock()
			}
			log.Info().Str("table", fmt.Sprintf("%s.%s", table.Database, table.Name)).Str("progress", fmt.Sprintf("%d/%d", idx+1, len(tables))).Msg("done")
			return nil
		})
	}
	wgWaitErr := createBackupWorkingGroup.Wait()
	// all tables are frozen, Kafka tables can consume again
	attachErr := attachKafkaTables()
	if wgWaitErr != nil {
		return errors.Wrap(wgWaitErr, "one of createBackupLocal go-routine return error")
	}
	if attachErr != nil {
		return errors.Wrapf(attachErr, "can't attach back Kafka tables with %s", kafkaKeeperPathSetting)
	}

	if err := b.checkMaxBrokenPartRatio(atomic.LoadInt64(&brokenParts), atomic.LoadInt64(&totalParts)); err != nil {
		return err
	}

	backupMetaFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
	if err := b.createBackupMetadata(ctx, backupMetaFile, backupName, diffFromRemote, backupVersion, "regular", diskMap, diskTypes, disks, backupDataSize, backupObjectDiskSize, backupMetadataSize, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, tableMetas, allDatabases, allFunctions); err != nil {
		return errors.Wrap(err, "createBackupMetadata return error")
	}
	log.Info().Str("version", backupVersion).Str("operation", "createBackupLocal").Str("duration", utils.HumanizeDuration(time.Since(startBackup))).Msg("done")
	return nil
}

// createBackupDirs creates `backup` dir on every disk and the backup dir on the default disk,
// fails when the backup metadata.json already exists unless --resume is active
func (b *Backuper) createBackupDirs(backupName string, disks []clickhouse.Disk) (string, error) {
	// Create backup dir on all clickhouse disks
	for _, disk := range disks {
		if err := filesystemhelper.Mkdir(path.Join(disk.Path, "backup"), b.ch, disks); err != nil {
			return "", errors.Wrap(err, "filesystemhelper.Mkdir")
		}
	}
	backupPath := path.Join(b.DefaultDataPath, "backup", backupName)
	if _, err := os.Stat(path.Join(backupPath, "metadata.json")); err == nil || !os.IsNotExist(err) {
		if !b.resume {
			return "", errors.Errorf("'%s' medatata.json already exists", backupName)
		}
		log.Warn().Msgf("'%s' medatata.json already exists, will overwrite and resume object disk data upload", backupName)
	}
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		if err = filesystemhelper.Mkdir(backupPath, b.ch, disks); err != nil {
			log.Error().Msgf("can't create directory %s: %v", backupPath, err)
			return "", errors.Wrap(err, "filesystemhelper.Mkdir backupPath")
		}
	}
	return backupPath, nil
}

// isObjectDiskContainsTables reports whether any table to back up has data on an object, encrypted object or plain disk,
// object disk data is copied to remote storage only when table data is backed up,
// --schema / *-only backups shall not require remote storage, https://github.com/Altinity/clickhouse-backup/issues/1517
func (b *Backuper) isObjectDiskContainsTables(tables []clickhouse.Table, disks []clickhouse.Disk, doBackupData bool) bool {
	if !doBackupData {
		return false
	}
	for _, disk := range disks {
		if b.shouldSkipByDiskNameOrType(disk) {
			continue
		}
		if b.isDiskTypeObject(disk.Type) || b.isDiskTypeEncryptedObject(disk, disks) || b.isDiskPlain(disk) {
			for _, table := range tables {
				sort.Slice(table.DataPaths, func(i, j int) bool { return len(table.DataPaths[i]) > len(table.DataPaths[j]) })
				for _, tableDataPath := range table.DataPaths {
					if strings.HasPrefix(tableDataPath, disk.Path) {
						return true
					}
				}
			}
		}
	}
	return false
}

// validateObjectDiskConfigIfNecessary https://github.com/Altinity/clickhouse-backup/issues/910
func (b *Backuper) validateObjectDiskConfigIfNecessary(isObjectDiskContainsTables bool) error {
	if !isObjectDiskContainsTables {
		return nil
	}
	if err := config.ValidateObjectDiskConfig(b.cfg); err != nil {
		return errors.Wrap(err, "config.ValidateObjectDiskConfig")
	}
	// Warn if encryption key is set for GCS - object disk files won't be encrypted
	if b.cfg.General.RemoteStorage == "gcs" && b.cfg.GCS.EncryptionKey != "" {
		log.Warn().Msg("GCS_ENCRYPTION_KEY is configured, but files in object_disk path will NOT be encrypted. ClickHouse needs direct unencrypted access to these files for BACKUP/RESTORE operations.")
	}
	return nil
}

// checkPartsColumnsAndGetInProgressMutations validates system.parts_columns for full backup tables when configured and
// fetches in-progress mutations ONCE for the whole backup. system.mutations scans all tables on
// every query, so the previous per-table GetInProgressMutations call was O(N^2) and dominated
// create time on installations with many tables. We now do a single scan and look up per table.
func (b *Backuper) checkPartsColumnsAndGetInProgressMutations(ctx context.Context, tables []clickhouse.Table, doBackupData, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly bool) (map[metadata.TableTitle][]metadata.MutationMetadata, error) {
	if b.cfg.ClickHouse.CheckPartsColumns && doBackupData {
		tablesToCheck := make([]clickhouse.Table, 0, len(tables))
		for _, table := range tables {
			if !table.Skip && table.BackupType == clickhouse.ShardBackupFull {
				tablesToCheck = append(tablesToCheck, table)
			}
		}
		if err := b.ch.CheckSystemPartsColumnsForTables(ctx, tablesToCheck); err != nil {
			return nil, errors.Wrap(err, "CheckSystemPartsColumnsForTables failed")
		}
		log.Debug().Msgf("CheckSystemPartsColumnsForTables passed for %d tables", len(tablesToCheck))
	}

	var allInProgressMutations map[metadata.TableTitle][]metadata.MutationMetadata
	if b.cfg.ClickHouse.BackupMutations && !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly {
		var allInProgressMutationsErr error
		allInProgressMutations, allInProgressMutationsErr = b.ch.GetInProgressMutationsBatch(ctx)
		if allInProgressMutationsErr != nil {
			return nil, errors.Wrap(allInProgressMutationsErr, "b.ch.GetInProgressMutationsBatch")
		}
	}
	return allInProgressMutations, nil
}

// checkMaxBrokenPartRatio enforces max_broken_part_ratio: when broken parts were tolerated, fail the whole backup
// if their share of all data parts exceeds the configured ratio; otherwise complete as a partial
// backup with a warning, see https://github.com/Altinity/clickhouse-backup/issues/1418
func (b *Backuper) checkMaxBrokenPartRatio(broken, total int64) error {
	if broken <= 0 {
		return nil
	}
	if !b.cfg.General.AllowPartialBackup(int(broken), int(total)) {
		return errors.Errorf("backup aborted: %d of %d data parts are broken, ratio %.4f exceeds max_broken_part_ratio %.4f", broken, total, float64(broken)/float64(total), b.cfg.General.MaxBrokenPartRatio)
	}
	// surfaces silent data loss to monitoring in server mode, exposed as clickhouse_backup_failed_parts_count
	metrics.FailedPartsCount.Add(float64(broken))
	log.Warn().Int64("broken_parts", broken).Int64("total_parts", total).Float64("max_broken_part_ratio", b.cfg.General.MaxBrokenPartRatio).Msg("partial backup: some data parts were broken but stayed within max_broken_part_ratio")
	return nil
}

// createOneTableSize accumulates per-table counters that createBackupLocal sums into backup-level totals
type createOneTableSize struct {
	dataSize       uint64
	objectDiskSize uint64
	metadataSize   uint64
	brokenParts    int64
	totalParts     int64
}

// createOneTable backs up data (when requested) and writes table metadata for a single table,
// returns the written metadata (nil when no metadata was written) and per-table size counters
func (b *Backuper) createOneTable(ctx context.Context, backupName, backupPath string, table *clickhouse.Table, tablesDiffFromRemote map[metadata.TableTitle]metadata.TableMetadata, partitionsIdMap map[metadata.TableTitle]common.EmptyMap, allInProgressMutations map[metadata.TableTitle][]metadata.MutationMetadata, kafkaKeeper map[metadata.TableTitle]*metadata.KafkaKeeperMetadata, skipProjections []string, disks []clickhouse.Disk, doBackupData, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly bool, version int) (*metadata.TableMetadata, createOneTableSize, error) {
	logger := log.With().Str("table", fmt.Sprintf("%s.%s", table.Database, table.Name)).Logger()
	size := createOneTableSize{}
	var realSize, objectDiskSize map[string]int64
	var disksToPartsMap map[string][]metadata.Part
	var checksums map[string]uint64
	var hashOfAllFiles map[string]string
	var addTableToBackupErr error
	var tableBrokenParts map[string][]metadata.Part
	if doBackupData && table.BackupType == clickhouse.ShardBackupFull {
		logger.Debug().Msg("begin data backup")
		shadowBackupUUID := strings.ReplaceAll(uuid.New().String(), "-", "")
		if addFreezeErr := b.addFreeze(backupName, shadowBackupUUID, table); addFreezeErr != nil {
			return nil, size, errors.Wrap(addFreezeErr, "b.addFreeze")
		}
		disksToPartsMap, realSize, objectDiskSize, checksums, hashOfAllFiles, tableBrokenParts, addTableToBackupErr = b.AddTableToLocalBackup(ctx, backupName, tablesDiffFromRemote, shadowBackupUUID, disks, table, partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Name}], skipProjections, version)
		if addTableToBackupErr != nil {
			logger.Error().Msgf("b.AddTableToLocalBackup error: %v", addTableToBackupErr)
			return nil, size, errors.Wrap(addTableToBackupErr, "b.AddTableToLocalBackup")
		}
		if removeFreezeErr := b.removeFreeze(shadowBackupUUID, disks); removeFreezeErr != nil {
			return nil, size, errors.Wrap(removeFreezeErr, "b.removeFreeze")
		}
		// account broken and total data parts for the max_broken_part_ratio decision in createBackupLocal
		tableBrokenCount := 0
		for _, diskParts := range tableBrokenParts {
			tableBrokenCount += len(diskParts)
		}
		tableTotalParts := tableBrokenCount
		for _, parts := range disksToPartsMap {
			tableTotalParts += len(parts)
		}
		size.brokenParts = int64(tableBrokenCount)
		size.totalParts = int64(tableTotalParts)
		// more precise data size calculation
		for _, s := range realSize {
			size.dataSize += uint64(s)
		}
		for _, s := range objectDiskSize {
			size.objectDiskSize += uint64(s)
		}
	}
	// https://github.com/Altinity/clickhouse-backup/issues/529
	logger.Debug().Msg("get in progress mutations list")
	inProgressMutations := make([]metadata.MutationMetadata, 0)
	if b.cfg.ClickHouse.BackupMutations && !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly {
		// looked up from the single GetInProgressMutationsBatch query in createBackupLocal — avoids the
		// O(N^2) per-table system.mutations scan.
		inProgressMutations = allInProgressMutations[metadata.TableTitle{Database: table.Database, Table: table.Name}]
	}
	logger.Debug().Msg("create metadata")
	if !schemaOnly && !doBackupData {
		return nil, size, nil
	}
	tableMeta := metadata.TableMetadata{
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
		KafkaKeeper:    kafkaKeeper[metadata.TableTitle{Database: table.Database, Table: table.Name}],
		MetadataOnly:   schemaOnly || table.BackupType == clickhouse.ShardBackupSchema,
	}
	metadataSize, createTableMetadataErr := b.createTableMetadata(path.Join(backupPath, "metadata"), tableMeta, disks)
	if createTableMetadataErr != nil {
		logger.Error().Msgf("b.createTableMetadata error: %v", createTableMetadataErr)
		return nil, size, errors.Wrap(createTableMetadataErr, "b.createTableMetadata")
	}
	size.metadataSize = metadataSize
	return &tableMeta, size, nil
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
	if doBackupData {
		for _, table := range tables {
			// https://github.com/Altinity/clickhouse-backup/issues/1120
			if _, isKafkaKeeper := kafkaKeeperPathFromQuery(table.Engine, table.CreateTableQuery); isKafkaKeeper && !table.Skip {
				log.Warn().Msgf("`%s`.`%s`: kafka offsets in keeper are not backed up with use_embedded_backup_restore", table.Database, table.Name)
			}
		}
	}

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
			if b.EmbeddedOnClusterWorker {
				// the initiator already executed BACKUP ... ON CLUSTER, this node only describes its own shards/N/replicas/M part,
				// the empty result routes the size calculation to system.parts below, https://github.com/Altinity/clickhouse-backup/issues/928
				log.Info().Msgf("--%s: skip BACKUP SQL, describe %s only", embeddedOnClusterWorkerFlag, b.embeddedClusterPrefix)
				backupResult = append(backupResult, clickhouse.SystemBackups{})
			} else {
				if err := b.ch.SelectContext(ctx, &backupResult, backupSQL); err != nil {
					return errors.Wrap(err, "backup error")
				}
				if len(backupResult) != 1 || (backupResult[0].Status != "BACKUP_COMPLETE" && backupResult[0].Status != "BACKUP_CREATED") {
					return errors.Errorf("backup return wrong results: %+v", backupResult)
				}
			}

			if schemaOnly {
				backupDataSize = append(backupDataSize, clickhouse.BackupDataSize{Size: 0})
			} else {
				if backupResult[0].CompressedSize == 0 && backupResult[0].Id != "" {
					systemBackupResult := make([]clickhouse.SystemBackups, 0)
					// explicit column list, SELECT * breaks when new versions add columns unknown to SystemBackups (26.8 added `settings`, `engine_settings`)
					backupSizeSQL := fmt.Sprintf("SELECT id, status, compressed_size, uncompressed_size FROM system.backups WHERE id='%s'", backupResult[0].Id)
					if sizeErr := b.ch.SelectContext(ctx, &systemBackupResult, backupSizeSQL); sizeErr != nil {
						return errors.Wrap(sizeErr, "system.backups query")
					}
					if len(systemBackupResult) != 1 {
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
						// per table .json next to the .sql of this node, so every node of an ON CLUSTER backup has its own set
						metadataSize, err := b.createTableMetadata(path.Join(backupPath, b.embeddedClusterPrefix, "metadata"), metadata.TableMetadata{
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
	// a shared `embedded_backup_disk` is one directory for all nodes, the initiator wrote metadata.json before the fan-out
	// and a worker must not overwrite it, in URL mode the path is node local and the worker writes its own
	if _, statErr := os.Stat(backupMetaFile); b.EmbeddedOnClusterWorker && statErr == nil {
		log.Info().Msgf("--%s: %s already written by the initiator, skip", embeddedOnClusterWorkerFlag, backupMetaFile)
	} else if err := b.createBackupMetadata(ctx, backupMetaFile, backupName, baseBackup, backupVersion, b.embeddedBackupTags(), diskMap, diskTypes, disks, backupDataSize[0].Size, 0, backupMetadataSize, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, tablesTitle, allDatabases, allFunctions); err != nil {
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
	// ALTER TABLE FREEZE/UNFREEZE are rejected for a table whose storage policy has plain/plain_rewritable disks
	// (see checkAlterPartitionIsPossible in ClickHouse MergeTreeData.cpp), snapshot such a table from the live
	// data with merges and moves stopped instead
	var plainSnapshot *plainPolicySnapshot
	if b.isTableOnPlainDisks(table, diskList) {
		snapshot, snapshotErr := b.snapshotTableOnPlainDisks(ctx, table, diskList, shadowBackupUUID, partitionsIdsMap)
		defer b.releasePlainPolicySnapshot(snapshot)
		if snapshotErr != nil {
			return nil, nil, nil, nil, nil, nil, snapshotErr
		}
		plainSnapshot = snapshot
	} else if err := b.ch.FreezeTable(ctx, table, shadowBackupUUID); err != nil {
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
			// plain/plain_rewritable disks have no local shadow, the live parts are enumerated in the bucket
			if b.isDiskPlain(disk) {
				if plainSnapshot == nil {
					continue
				}
				plainParts, plainSize, plainErr := b.uploadPlainDiskParts(ctx, backupName, table, disk, plainSnapshot.parts)
				if plainErr != nil {
					return nil, nil, nil, nil, nil, nil, errors.Wrap(plainErr, "b.uploadPlainDiskParts")
				}
				if len(plainParts) == 0 {
					continue
				}
				disksToPartsMap[disk.Name] = plainParts
				objectDiskSize[disk.Name] = plainSize
				if version >= 19011000 {
					partNames := make([]string, 0, len(plainParts))
					for _, p := range plainParts {
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
				continue
			}
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
					parts[idx].Size = uint64(linkedSize)
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
	if plainSnapshot != nil {
		// UNFREEZE is rejected for such a table as FREEZE is and the shadow holds our own hardlinks: remove them
		// directly, best effort, a leftover shadow is removed again by cleanOwnFreezes at the end of create
		for _, disk := range diskList {
			if disk.IsBackup || b.isDiskPlain(disk) {
				continue
			}
			shadowPath := path.Join(disk.Path, "shadow", shadowBackupUUID)
			if err := os.RemoveAll(shadowPath); err != nil {
				logger.Warn().Msgf("can't remove shadow %s: %v", shadowPath, err)
			}
		}
	} else if version > 21004000 {
		// Unfreeze to unlock data on S3 disks, https://github.com/Altinity/clickhouse-backup/issues/423
		if err := b.ch.QueryContext(ctx, fmt.Sprintf("ALTER TABLE `%s`.`%s` UNFREEZE WITH NAME '%s'", table.Database, table.Name, shadowBackupUUID)); err != nil {
			if b.ch.IsIgnorableNotExistsError(err) {
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
						// WithoutCancel: server-side copy continues after client cancel, let it finish so abort cleanup sees dstKey
						if objSize, err = b.dst.CopyObject(context.WithoutCancel(ctx), storageObject.ObjectSize, srcBucket, srcKey, dstKey); err != nil {
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
						copyRetry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), copyObjectRetryClassifier{b: b})
						copyObjectErr = copyRetry.RunCtx(uploadCtx, func(ctx context.Context) error {
							var copyErr error
							// WithoutCancel: server-side copy continues after client cancel, let it finish so abort cleanup sees dstKey
							objSize, copyErr = b.dst.CopyObject(context.WithoutCancel(ctx), storageObject.ObjectSize, srcBucket, srcKey, dstKey)
							return copyErr
						})
						if copyObjectErr != nil {
							if storage.IsPermanentCopyObjectError(copyObjectErr) {
								log.Warn().Msgf("b.dst.CopyObject in %s error: %v, will try upload via streaming (possible high network traffic)", backupShadowPath, copyObjectErr)
								isCopyFailed.Store(true)
							} else {
								log.Warn().Msgf("b.dst.CopyObject in %s for srcKey=%s error: %v, will try upload via streaming for this object only", backupShadowPath, srcKey, copyObjectErr)
							}
						}
					}
					if isCopyFailed.Load() || copyObjectErr != nil {
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

// activePart is the row of system.parts clickhouse-backup needs to snapshot a part without FREEZE
type activePart struct {
	Name     string `ch:"name"`
	DiskName string `ch:"disk_name"`
	Path     string `ch:"path"`
}

// plainPolicySnapshot takes the place of FREEZE for a table whose storage policy has plain/plain_rewritable disks:
// the active parts of the local disks are hardlinked into shadow by linkLocalPartsToShadow, the parts of the plain
// disks are copied from the bucket by uploadPlainDiskParts, both with merges and moves of the table stopped until
// releasePlainPolicySnapshot
type plainPolicySnapshot struct {
	table *clickhouse.Table
	// parts are the active parts to back up, on local and plain disks, after the --partitions and
	// freeze_by_part_where filters
	parts []activePart
	// stopped lists the background operations of the table stopped for the snapshot, in SYSTEM STOP syntax
	stopped []string
}

// isTableOnPlainDisks tells whether the table must be backed up without FREEZE: ClickHouse rejects ALTER TABLE
// FREEZE for a table whose storage policy has any plain/plain_rewritable disk, wherever its parts are
// (checkAlterPartitionIsPossible checks every disk of the policy)
func (b *Backuper) isTableOnPlainDisks(table *clickhouse.Table, diskList []clickhouse.Disk) bool {
	for _, disk := range diskList {
		if b.isDiskPlain(disk) && plainDiskTableRelPath(table.DataPaths, disk) != "" {
			return true
		}
	}
	return false
}

// snapshotTableOnPlainDisks takes the snapshot of a table which can't be frozen, see plainPolicySnapshot. The replica
// is synced first when sync_replicated_tables asks for it, the way FreezeTable does, and before merges are stopped:
// SYSTEM SYNC REPLICA waits for the queued merges. A table dropped in between is skipped with an empty snapshot when
// ignore_not_exists_error_during_freeze is set, the way FreezeTable skips it. The returned snapshot is never nil, the
// caller releases it on error too, to start the background operations stopped so far
func (b *Backuper) snapshotTableOnPlainDisks(ctx context.Context, table *clickhouse.Table, diskList []clickhouse.Disk, shadowBackupUUID string, partitionsIdsMap common.EmptyMap) (*plainPolicySnapshot, error) {
	snapshot := &plainPolicySnapshot{table: table}
	b.ch.SyncReplicatedTable(ctx, table)
	// a merge would replace the parts right under the hardlinks and the bucket copy, a TTL or a manual move between
	// the volumes would remove the part from its disk the same way, whichever disk the parts are on right now
	for _, background := range []string{"MERGES", "MOVES"} {
		if err := b.ch.QueryContext(ctx, fmt.Sprintf("SYSTEM STOP %s `%s`.`%s`", background, table.Database, table.Name)); err != nil {
			if b.ch.IsIgnorableNotExistsError(err) {
				log.Warn().Msgf("can't SYSTEM STOP %s `%s`.`%s`: %v", background, table.Database, table.Name, err)
				return snapshot, nil
			}
			return snapshot, errors.Wrapf(err, "SYSTEM STOP %s", background)
		}
		snapshot.stopped = append(snapshot.stopped, background)
	}
	parts, err := b.getPlainPolicyParts(ctx, table, diskList, partitionsIdsMap)
	if err != nil {
		return snapshot, err
	}
	snapshot.parts = parts
	if err = b.linkLocalPartsToShadow(ctx, table, diskList, shadowBackupUUID, parts); err != nil {
		return snapshot, errors.Wrap(err, "b.linkLocalPartsToShadow")
	}
	return snapshot, nil
}

// releasePlainPolicySnapshot starts the background operations stopped by snapshotTableOnPlainDisks
func (b *Backuper) releasePlainPolicySnapshot(snapshot *plainPolicySnapshot) {
	for _, background := range snapshot.stopped {
		if err := b.ch.Query(fmt.Sprintf("SYSTEM START %s `%s`.`%s`", background, snapshot.table.Database, snapshot.table.Name)); err != nil {
			log.Warn().Msgf("can't SYSTEM START %s `%s`.`%s`: %v", background, snapshot.table.Database, snapshot.table.Name, err)
		}
	}
}

// getPlainPolicyParts lists the active parts of a table backed up without FREEZE in one system.parts query, the list
// is shared by linkLocalPartsToShadow and uploadPlainDiskParts. freeze_by_part_where applies to it the way it
// applies to FREEZE PARTITION, see FreezeTableByParts
func (b *Backuper) getPlainPolicyParts(ctx context.Context, table *clickhouse.Table, diskList []clickhouse.Disk, partitionsIdsMap common.EmptyMap) ([]activePart, error) {
	query := "SELECT name, disk_name, path FROM system.parts WHERE database=? AND `table`=? AND active"
	if b.cfg.ClickHouse.FreezeByPart && b.cfg.ClickHouse.FreezeByPartWhere != "" {
		query += " " + b.cfg.ClickHouse.FreezeByPartWhere
	}
	var parts []activePart
	if err := b.ch.SelectContext(ctx, &parts, query, table.Database, table.Name); err != nil {
		return nil, errors.Wrap(err, "getPlainPolicyParts: select active parts")
	}
	return b.filterPlainPolicyParts(table, parts, diskList, partitionsIdsMap)
}

// filterPlainPolicyParts keeps the parts of the requested partitions and checks that every one of them lives on a
// disk which can be snapshotted without FREEZE: a plain disk, or a disk with complete local files. A part on any
// other disk is rejected: an object disk with local metadata protects its blobs with the hardlink counter inside the
// metadata files, which only FREEZE maintains. A cache disk over such a disk is reported by system.parts under its
// own name while GetDisks lists it under the underlying disk name, so an unknown disk name is the same case
func (b *Backuper) filterPlainPolicyParts(table *clickhouse.Table, parts []activePart, diskList []clickhouse.Disk, partitionsIdsMap common.EmptyMap) ([]activePart, error) {
	filtered := make([]activePart, 0, len(parts))
	for _, part := range parts {
		if len(partitionsIdsMap) != 0 && !filesystemhelper.IsPartInPartition(part.Name, partitionsIdsMap) {
			continue
		}
		disk := b.findDiskByName(diskList, part.DiskName)
		if disk == nil {
			return nil, errors.Errorf("`%s`.`%s` have parts on plain disks and on disk %s which is not a local disk of system.disks (a cache disk over object storage is listed under the underlying disk name), only local disks are supported next to plain disks in one storage policy", table.Database, table.Name, part.DiskName)
		}
		if !b.isDiskPlain(*disk) && !b.isDiskLocal(*disk, diskList) {
			return nil, errors.Errorf("`%s`.`%s` have parts on plain disks and on disk %s of type %s, only local disks are supported next to plain disks in one storage policy", table.Database, table.Name, disk.Name, disk.Type)
		}
		filtered = append(filtered, part)
	}
	return filtered, nil
}

// partPathOnDisk returns the path of a part relative to the root of its disk. system.parts.path starts with
// system.disks.path as clickhouse-server reports it (Disk.RawPath), while Disk.Path is where clickhouse-backup sees
// the same disk once disk_mapping is applied, so the result is joined to Disk.Path for any local access
func partPathOnDisk(partPath string, disk clickhouse.Disk) (string, error) {
	diskRawPath := disk.RawPath
	if diskRawPath == "" {
		diskRawPath = disk.Path
	}
	diskRawPath = strings.TrimSuffix(diskRawPath, "/") + "/"
	if !strings.HasPrefix(partPath, diskRawPath) {
		return "", errors.Errorf("part path %s is outside of disk %s path %s", partPath, disk.Name, diskRawPath)
	}
	return strings.TrimPrefix(partPath, diskRawPath), nil
}

// prepareShadowDir creates <disk>/shadow/<uuid> for the hardlinks of linkLocalPartsToShadow. <disk>/shadow is shared
// with clickhouse-server, which stats shadow/increment.txt on every FREEZE, so when clickhouse-backup runs as root
// both directories get the owner of the server data path, as the directories FREEZE creates have it, otherwise
// FREEZE of every table on the server fails with Permission denied. An existing <disk>/shadow is chowned too, which
// repairs a directory left to root by an earlier run
func (b *Backuper) prepareShadowDir(shadowDir string, diskList []clickhouse.Disk) error {
	for _, dir := range []string{path.Dir(shadowDir), shadowDir} {
		if err := filesystemhelper.Mkdir(dir, b.ch, diskList); err != nil {
			return errors.Wrapf(err, "filesystemhelper.Mkdir %s", dir)
		}
	}
	return nil
}

// linkLocalPartsToShadow takes the place of FREEZE for the local disks of a table on a plain storage policy: it
// hardlinks every part of parts which lives on a local disk into <disk>/shadow/<shadowBackupUUID>/<part path on disk>,
// the layout FREEZE produces and MoveShadowToBackup reads. The caller stopped merges and moves of the table, so a
// part can disappear only by DROP PARTITION, such a part is skipped as FREEZE would skip it
func (b *Backuper) linkLocalPartsToShadow(ctx context.Context, table *clickhouse.Table, diskList []clickhouse.Disk, shadowBackupUUID string, parts []activePart) error {
	linked := 0
	shadowReady := map[string]struct{}{}
	for _, part := range parts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		disk := b.findDiskByName(diskList, part.DiskName)
		if disk == nil {
			return errors.Errorf("linkLocalPartsToShadow: disk %s of part %s is absent in system.disks", part.DiskName, part.Name)
		}
		if b.isDiskPlain(*disk) || b.shouldSkipByDiskNameOrType(*disk) {
			continue
		}
		relPartPath, err := partPathOnDisk(part.Path, *disk)
		if err != nil {
			return errors.Wrap(err, "linkLocalPartsToShadow")
		}
		shadowDir := path.Join(disk.Path, "shadow", shadowBackupUUID)
		if _, ready := shadowReady[disk.Name]; !ready {
			if err = b.prepareShadowDir(shadowDir, diskList); err != nil {
				return errors.Wrap(err, "linkLocalPartsToShadow")
			}
			shadowReady[disk.Name] = struct{}{}
		}
		srcPartPath := path.Join(disk.Path, relPartPath)
		shadowPartPath := path.Join(shadowDir, relPartPath)
		if err = filesystemhelper.HardlinkPart(srcPartPath, shadowPartPath); err != nil {
			// only a part which is gone as a whole is skipped, a file missing inside a live part is an error
			_, statErr := os.Stat(srcPartPath)
			if !os.IsNotExist(errors.Cause(err)) || !os.IsNotExist(statErr) {
				return errors.Wrapf(err, "linkLocalPartsToShadow: part %s", part.Name)
			}
			log.Warn().Str("database", table.Database).Str("table", table.Name).Str("part", part.Name).Msg("part removed during backup, skipped")
			if removeErr := os.RemoveAll(shadowPartPath); removeErr != nil {
				return errors.Wrapf(removeErr, "linkLocalPartsToShadow: os.RemoveAll %s", shadowPartPath)
			}
			continue
		}
		linked++
	}
	log.Debug().Str("database", table.Database).Str("table", table.Name).Int("parts", linked).Msg("local parts hardlinked to shadow")
	return nil
}

// uploadPlainDiskParts enumerates the live data parts of a plain/plain_rewritable disk on the bucket
// level (FREEZE is not supported for such disks and they have no local metadata files at all) and
// server-side copies every data object of the active parts into the backup destination under its
// logical path: <object_disk_path>/<backupName>/<diskName>/shadow/<db>/<table>/<partName>/<file...>.
// activeParts is the snapshot of the active parts of the table, listed with merges and moves already stopped by
// the caller.
func (b *Backuper) uploadPlainDiskParts(ctx context.Context, backupName string, table *clickhouse.Table, disk clickhouse.Disk, activeParts []activePart) ([]metadata.Part, int64, error) {
	// table data path relative to the disk root, checked BEFORE any bucket access:
	// most tables have no data on this disk at all
	tableRelPath := plainDiskTableRelPath(table.DataPaths, disk)
	if tableRelPath == "" {
		return nil, 0, nil
	}
	activePartsMap := make(map[string]struct{}, len(activeParts))
	for _, p := range activeParts {
		if p.DiskName == disk.Name {
			activePartsMap[p.Name] = struct{}{}
		}
	}
	if len(activePartsMap) == 0 {
		return nil, 0, nil
	}
	if err := object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, disk.Name); err != nil {
		return nil, 0, errors.Wrap(err, "object_disk.InitCredentialsAndConnections")
	}
	srcDiskConnection, exists := object_disk.DisksConnections.Load(disk.Name)
	if !exists {
		return nil, 0, errors.Errorf("uploadPlainDiskParts: %s not present in object_disk.DisksConnections", disk.Name)
	}
	layout, layoutErr := object_disk.NewPlainDiskLayout(ctx, disk.Name)
	if layoutErr != nil {
		return nil, 0, errors.Wrap(layoutErr, "object_disk.NewPlainDiskLayout")
	}
	tree, treeErr := layout.ListTree(ctx, tableRelPath)
	if treeErr != nil {
		return nil, 0, errors.Wrap(treeErr, "layout.ListTree")
	}
	if len(tree) == 0 {
		return nil, 0, nil
	}
	objectDiskPath, err := b.getObjectDiskPath()
	if err != nil {
		return nil, 0, errors.Wrap(err, "b.getObjectDiskPath")
	}
	srcBucket := srcDiskConnection.GetRemoteBucket()
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Name))
	uploadPlainDiskPartsWorkingGroup, uploadCtx := errgroup.WithContext(ctx)
	uploadPlainDiskPartsWorkingGroup.SetLimit(int(b.cfg.General.ObjectDiskServerSideCopyConcurrency))
	var partsMu sync.Mutex
	partSizes := map[string]uint64{}
	var isCopyFailed atomic.Bool
	for relPath, plainFile := range tree {
		partName := strings.SplitN(relPath, "/", 2)[0]
		// only active data parts of the requested partitions, this also filters detached/, tmp_* and table-level
		// files like format_version.txt
		if _, active := activePartsMap[partName]; !active {
			continue
		}
		srcKey := path.Join(srcDiskConnection.GetRemotePath(), plainFile.RemoteKey)
		dstKey := path.Join(objectDiskPath, backupName, disk.Name, "shadow", dbAndTableDir, relPath)
		capturedSize := plainFile.Size
		partsMu.Lock()
		partSizes[partName] += uint64(capturedSize)
		partsMu.Unlock()
		uploadPlainDiskPartsWorkingGroup.Go(func() error {
			if b.resume {
				isAlreadyProcessed, _, resumeErr := b.resumableState.IsAlreadyProcessed(path.Join(srcBucket, srcKey))
				if resumeErr != nil {
					return errors.Wrap(resumeErr, "resumableState.IsAlreadyProcessed")
				}
				if isAlreadyProcessed {
					return nil
				}
			}
			var copyObjectErr error
			if !isCopyFailed.Load() {
				copyRetry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), copyObjectRetryClassifier{b: b})
				copyObjectErr = copyRetry.RunCtx(uploadCtx, func(ctx context.Context) error {
					// WithoutCancel: server-side copy continues after client cancel, let it finish so abort cleanup sees dstKey
					_, copyErr := b.dst.CopyObject(context.WithoutCancel(ctx), capturedSize, srcBucket, srcKey, dstKey)
					return copyErr
				})
				if copyObjectErr != nil && !b.cfg.General.AllowObjectDiskStreaming {
					return errors.Wrapf(copyObjectErr, "b.dst.CopyObject for srcKey=%s error", srcKey)
				}
				if copyObjectErr != nil && storage.IsPermanentCopyObjectError(copyObjectErr) {
					isCopyFailed.Store(true)
				}
			}
			if b.cfg.General.AllowObjectDiskStreaming && (isCopyFailed.Load() || copyObjectErr != nil) {
				retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
				copyObjectErr = retry.RunCtx(uploadCtx, func(ctx context.Context) error {
					return object_disk.CopyObjectStreaming(ctx, srcDiskConnection.GetRemoteStorage(), b.dst, srcKey, dstKey, b.dst.UploadLimiter(b.cfg.General.UploadMaxBytesPerSecond))
				})
				if copyObjectErr != nil {
					return errors.Wrapf(copyObjectErr, "object_disk.CopyObjectStreaming for srcKey=%s error", srcKey)
				}
			}
			if b.resume {
				if appendErr := b.resumableState.AppendToState(path.Join(srcBucket, srcKey), capturedSize); appendErr != nil {
					return errors.Wrap(appendErr, "resumableState.AppendToState")
				}
			}
			return nil
		})
	}
	if wgWaitErr := uploadPlainDiskPartsWorkingGroup.Wait(); wgWaitErr != nil {
		return nil, 0, errors.Wrap(wgWaitErr, "one of uploadPlainDiskParts go-routine return error")
	}
	parts := make([]metadata.Part, 0, len(partSizes))
	size := int64(0)
	for partName, partSize := range partSizes {
		parts = append(parts, metadata.Part{Name: partName, Size: partSize})
		size += int64(partSize)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Name < parts[j].Name })
	return parts, size, nil
}

func (b *Backuper) createBackupMetadata(ctx context.Context, backupMetaFile, backupName, requiredBackup, version, tags string, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, backupDataSize, backupObjectDiskSize, backupMetadataSize, backupRBACSize, backupConfigSize, backupNamedCollectionsSize uint64, tableMetas []metadata.TableTitle, allDatabases []clickhouse.Database, allFunctions []clickhouse.Function) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		diskMetadataTypes := map[string]string{}
		for _, d := range disks {
			if d.IsPlain() {
				diskMetadataTypes[d.Name] = d.MetadataType
			}
		}
		backupMetadata := metadata.BackupMetadata{
			BackupName:              backupName,
			RequiredBackup:          requiredBackup,
			Disks:                   diskMap,
			DiskTypes:               diskTypes,
			DiskMetadataTypes:       diskMetadataTypes,
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
