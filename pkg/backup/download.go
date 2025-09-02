package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/custom"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/partition"
	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
	"github.com/Altinity/clickhouse-backup/v2/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/rs/zerolog"

	"github.com/rs/zerolog/log"
)

var (
	ErrBackupIsAlreadyExists = errors.New("backup is already exists")
)

func (b *Backuper) Download(backupName string, tablePattern string, partitions []string, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, resume bool, hardlinkExistsFiles bool, backupVersion string, commandId int) error {
	if pidCheckErr := pidlock.CheckAndCreatePidFile(backupName, "download"); pidCheckErr != nil {
		return pidCheckErr
	}
	defer pidlock.RemovePidFile(backupName)

	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	defer cancel()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")

	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	if b.cfg.General.RemoteStorage == "none" {
		return fmt.Errorf("general->remote_storage shall not be \"none\" for download, change you config or use REMOTE_STORAGE environment variable")
	}
	if b.cfg.General.DownloadConcurrency == 0 {
		return fmt.Errorf("`download_concurrency` shall be more than zero")
	}
	b.adjustResumeFlag(resume)

	if backupName == "" {
		remoteBackups := b.CollectRemoteBackups(ctx, "all")
		_ = b.PrintBackup(remoteBackups, "all", "text")
		return fmt.Errorf("select backup for download")
	}
	localBackups, disks, err := b.GetLocalBackups(ctx, nil)
	if err != nil {
		return err
	}
	b.DefaultDataPath, err = b.ch.GetDefaultPath(disks)
	if err != nil {
		return err
	}
	isResumeExists := false
	for i := range localBackups {
		if backupName == localBackups[i].BackupName {
			if strings.Contains(localBackups[i].Tags, "embedded") || b.cfg.General.RemoteStorage == "custom" {
				return ErrBackupIsAlreadyExists
			}
			if !b.resume {
				return ErrBackupIsAlreadyExists
			} else {
				_, checkDownloadErr := os.Stat(path.Join(b.DefaultDataPath, "backup", backupName, "download.state2"))
				if errors.Is(checkDownloadErr, os.ErrNotExist) {
					return ErrBackupIsAlreadyExists
				}
				isResumeExists = true
				log.Warn().Msgf("%s already exists will try to resume download", backupName)
			}
		}
	}
	startDownload := time.Now()
	if b.cfg.General.RemoteStorage == "custom" {
		return custom.Download(ctx, b, b.cfg, backupName, tablePattern, partitions, schemaOnly)
	}
	if err := b.initDisksPathsAndBackupDestination(ctx, disks, ""); err != nil {
		return err
	}
	defer func() {
		if err := b.dst.Close(ctx); err != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", err)
		}
	}()

	remoteBackups, err := b.dst.BackupList(ctx, true, backupName)
	if err != nil {
		return err
	}
	found := false
	var remoteBackup storage.Backup
	for _, r := range remoteBackups {
		if backupName == r.BackupName {
			remoteBackup = r
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("'%s' is not found on remote storage", backupName)
	}
	if len(remoteBackup.Tables) == 0 && !b.cfg.General.AllowEmptyBackups {
		return fmt.Errorf("'%s' is empty backup", backupName)
	}
	// if using hardlink then disable this check, if not use then check disk size
	if !hardlinkExistsFiles {
		// https://github.com/Altinity/clickhouse-backup/issues/878
		if freeSizeErr := b.CheckDisksUsage(remoteBackup, disks, isResumeExists, tablePattern); freeSizeErr != nil {
			return freeSizeErr
		}
	}
	tablesForDownload := parseTablePatternForDownload(remoteBackup.Tables, tablePattern)

	if !schemaOnly && !b.cfg.General.DownloadByPart && remoteBackup.RequiredBackup != "" {
		err := b.Download(remoteBackup.RequiredBackup, tablePattern, partitions, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, b.resume, hardlinkExistsFiles, backupVersion, commandId)
		if err != nil && !errors.Is(err, ErrBackupIsAlreadyExists) {
			return err
		}
	}

	dataSize := uint64(0)
	metadataSize := uint64(0)
	b.isEmbedded = strings.Contains(remoteBackup.Tags, "embedded")
	localBackupDir := path.Join(b.DefaultDataPath, "backup", backupName)
	if b.isEmbedded {
		// will ignore partitions cause can't manipulate .backup
		partitions = make([]string, 0)
		localBackupDir = path.Join(b.EmbeddedBackupDataPath, backupName)
	}
	err = os.MkdirAll(localBackupDir, 0750)
	if err != nil && !resume {
		return err
	}
	if b.resume {
		b.resumableState = resumable.NewState(b.GetStateDir(), backupName, "download", map[string]interface{}{
			"tablePattern": tablePattern,
			"partitions":   partitions,
			"schemaOnly":   schemaOnly,
		})
	}

	log.Debug().Str("backup", backupName).Msgf("prepare table METADATA concurrent semaphore with concurrency=%d len(tablesForDownload)=%d", b.cfg.General.DownloadConcurrency, len(tablesForDownload))
	tableMetadataAfterDownload := make(ListOfTables, len(tablesForDownload))
	doDownloadData := !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly
	if doDownloadData || schemaOnly {
		metadataGroup, metadataCtx := errgroup.WithContext(ctx)
		metadataGroup.SetLimit(int(b.cfg.General.DownloadConcurrency))
		for i, t := range tablesForDownload {
			metadataLogger := log.With().Str("table_metadata", fmt.Sprintf("%s.%s", t.Database, t.Table)).Logger()
			idx := i
			tableTitle := t
			metadataGroup.Go(func() error {
				downloadedMetadata, size, downloadMetadataErr := b.downloadTableMetadata(metadataCtx, backupName, disks, tableTitle, schemaOnly, partitions, b.resume, metadataLogger)
				if downloadMetadataErr != nil {
					return downloadMetadataErr
				}
				tableMetadataAfterDownload[idx] = downloadedMetadata
				atomic.AddUint64(&metadataSize, size)
				return nil
			})
		}
		if err := metadataGroup.Wait(); err != nil {
			return fmt.Errorf("one of Download Metadata go-routine return error: %v", err)
		}
	}
	// download, missed .inner. tables, https://github.com/Altinity/clickhouse-backup/issues/765
	var missedInnerTableErr error
	tableMetadataAfterDownload, tablesForDownload, metadataSize, missedInnerTableErr = b.downloadMissedInnerTablesMetadata(ctx, backupName, metadataSize, tablesForDownload, tableMetadataAfterDownload, disks, schemaOnly, partitions)
	if missedInnerTableErr != nil {
		return fmt.Errorf("b.downloadMissedInnerTablesMetadata error: %v", missedInnerTableErr)
	}

	if doDownloadData {
		if reBalanceErr := b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownload, disks, remoteBackup); reBalanceErr != nil {
			return reBalanceErr
		}
		b.filterPartsAndFilesByDisk(tableMetadataAfterDownload, disks)
		actualConcurrency := b.cfg.GetOptimalDownloadConcurrency()
		
		// Add compression format CPU impact analysis
		compressionFormat := b.cfg.GetCompressionFormat()
		cpuIntensiveCompression := compressionFormat == "zstd" || compressionFormat == "brotli" || compressionFormat == "xz"
		
		log.Info().Fields(map[string]interface{}{
			"backup_name": backupName,
			"operation": "download_performance_analysis",
			"configured_concurrency": b.cfg.General.DownloadConcurrency,
			"actual_concurrency": actualConcurrency,
			"storage_type": b.cfg.General.RemoteStorage,
			"cpu_count": runtime.NumCPU(),
			"tables_count": len(tableMetadataAfterDownload),
			"compression_format": compressionFormat,
			"cpu_intensive_compression": cpuIntensiveCompression,
			"performance_ratio": float64(b.cfg.General.DownloadConcurrency) / float64(runtime.NumCPU()),
			"compression_impact": func() string {
				if cpuIntensiveCompression {
					return fmt.Sprintf("High CPU compression detected (%s). Consider reducing concurrency to ~%d for CPU-bound workloads", compressionFormat, runtime.NumCPU())
				}
				return "Compression format has minimal CPU impact"
			}(),
		}).Msg("download concurrency diagnostics")
		
		log.Debug().Str("backupName", backupName).Msgf("prepare table DATA concurrent semaphore with concurrency=%d len(tableMetadataAfterDownload)=%d", b.cfg.General.DownloadConcurrency, len(tableMetadataAfterDownload))
		dataGroup, dataCtx := errgroup.WithContext(ctx)
		dataGroup.SetLimit(int(b.cfg.General.DownloadConcurrency))

		for i, tableMetadata := range tableMetadataAfterDownload {
			if tableMetadata == nil || tableMetadata.MetadataOnly {
				continue
			}
			idx := i
			dataGroup.Go(func() error {
				start := time.Now()
				var downloadDataErr error
				var downloadDataSize uint64
				downloadDataSize, downloadDataErr = b.downloadTableData(dataCtx, remoteBackup.BackupMetadata, *tableMetadataAfterDownload[idx], disks, hardlinkExistsFiles)
				if downloadDataErr != nil {
					return downloadDataErr
				}
				atomic.AddUint64(&dataSize, downloadDataSize)
				tableMetadataAfterDownload[idx].TotalBytes = downloadDataSize
				log.Info().Fields(map[string]interface{}{
					"backup_name": backupName,
					"operation":   "download_data",
					"table":       fmt.Sprintf("%s.%s", tableMetadataAfterDownload[idx].Database, tableMetadataAfterDownload[idx].Table),
					"progress":    fmt.Sprintf("%d/%d", idx+1, len(tableMetadataAfterDownload)),
					"duration":    utils.HumanizeDuration(time.Since(start)),
					"size":        utils.FormatBytes(tableMetadataAfterDownload[idx].TotalBytes),
					"version":     backupVersion,
				}).Msg("done")
				return nil
			})
		}
		if err := dataGroup.Wait(); err != nil {
			return fmt.Errorf("one of Download go-routine return error: %v", err)
		}
	}
	var rbacSize, configSize, namedCollectionsSize uint64
	if rbacOnly || rbacOnly == configsOnly == namedCollectionsOnly == false {
		rbacSize, err = b.downloadRBACData(ctx, remoteBackup)
		if err != nil {
			return fmt.Errorf("download RBAC error: %v", err)
		}
	}

	if configsOnly || rbacOnly == configsOnly == namedCollectionsOnly == false {
		configSize, err = b.downloadConfigData(ctx, remoteBackup)
		if err != nil {
			return fmt.Errorf("download CONFIGS error: %v", err)
		}
	}

	if namedCollectionsOnly || rbacOnly == configsOnly == namedCollectionsOnly == false {
		namedCollectionsSize, err = b.downloadNamedCollectionsData(ctx, remoteBackup)
		if err != nil {
			return fmt.Errorf("download NAMED COLLECTIONS error: %v", err)
		}
	}

	backupMetadata := remoteBackup.BackupMetadata
	backupMetadata.Tables = tablesForDownload

	if doDownloadData && b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" && backupMetadata.Tables != nil && len(backupMetadata.Tables) > 0 {
		localClickHouseBackupFile := path.Join(b.EmbeddedBackupDataPath, backupName, ".backup")
		remoteClickHouseBackupFile := path.Join(backupName, ".backup")
		localEmbeddedMetadataSize := int64(0)
		if localEmbeddedMetadataSize, err = b.downloadSingleBackupFile(ctx, remoteClickHouseBackupFile, localClickHouseBackupFile, disks); err != nil {
			return err
		}
		metadataSize += uint64(localEmbeddedMetadataSize)
	}

	backupMetadata.CompressedSize = 0
	backupMetadata.DataFormat = ""
	backupMetadata.DataSize = dataSize
	backupMetadata.MetadataSize = metadataSize
	backupMetadata.ConfigSize = configSize
	backupMetadata.RBACSize = rbacSize
	backupMetadata.ClickhouseBackupVersion = backupVersion
	backupMetafileLocalPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
	if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		backupMetafileLocalPath = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata.json")
	}
	if err := backupMetadata.Save(backupMetafileLocalPath); err != nil {
		return err
	}
	for _, disk := range disks {
		if disk.IsBackup {
			if err = filesystemhelper.Chown(path.Join(disk.Path, backupName), b.ch, disks, true); err != nil {
				return err
			}
		}
	}

	if b.resume {
		b.resumableState.Close()
	}

	//clean partially downloaded requiredBackup
	if remoteBackup.RequiredBackup != "" {
		if err = b.cleanPartialRequiredBackup(ctx, disks, remoteBackup.BackupName); err != nil {
			return err
		}
	}

	log.Info().Fields(map[string]interface{}{
		"backup":           backupName,
		"operation":        "download",
		"duration":         utils.HumanizeDuration(time.Since(startDownload)),
		"download_size":    utils.FormatBytes(dataSize + metadataSize + rbacSize + configSize + namedCollectionsSize),
		"object_disk_size": utils.FormatBytes(backupMetadata.ObjectDiskSize),
		"version":          backupVersion,
	}).Msg("done")
	return nil
}

func (b *Backuper) reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownload ListOfTables, disks []clickhouse.Disk, remoteBackup storage.Backup) error {
	var disksByStoragePolicyAndType map[string]map[string][]clickhouse.Disk
	filterDisksByTypeAndStoragePolicies := func(disk string, diskType string, disks []clickhouse.Disk, remoteBackup storage.Backup, t metadata.TableMetadata) (string, []clickhouse.Disk, error) {
		_, ok := remoteBackup.DiskTypes[disk]
		if !ok {
			return "", nil, fmt.Errorf("disk: %s not found in disk_types section %#v in %s/metadata.json", disk, remoteBackup.DiskTypes, remoteBackup.BackupName)
		}
		storagePolicy := b.ch.ExtractStoragePolicy(t.Query)
		if len(disksByStoragePolicyAndType) == 0 {
			disksByStoragePolicyAndType = b.splitDisksByTypeAndStoragePolicy(disks)
		}
		if _, isTypeExists := disksByStoragePolicyAndType[diskType]; !isTypeExists {
			return "", nil, fmt.Errorf("disk: %s, diskType: %s not found in system.disks", disk, diskType)
		}
		filteredDisks, isPolicyExists := disksByStoragePolicyAndType[diskType][storagePolicy]
		if !isPolicyExists || len(filteredDisks) == 0 {
			return "", nil, fmt.Errorf("storagePolicy: %s with diskType: %s not found in system.disks", storagePolicy, diskType)
		}
		return storagePolicy, filteredDisks, nil
	}

	updateDiskFreeSize := func(downloadDisk, diskType, storagePolicy string, newFreeSpace uint64) {
		for dIdx := range disksByStoragePolicyAndType[diskType][storagePolicy] {
			if disksByStoragePolicyAndType[diskType][storagePolicy][dIdx].Name == downloadDisk {
				disksByStoragePolicyAndType[diskType][storagePolicy][dIdx].FreeSpace = newFreeSpace
			}
		}
	}

	for i, t := range tableMetadataAfterDownload {
		if t == nil || t.TotalBytes == 0 {
			continue
		}
		isRebalanced := false
		totalFiles := 0
		for disk := range t.Files {
			totalFiles += len(t.Files[disk])
		}
		totalParts := 0
		for disk := range t.Parts {
			totalParts += len(t.Parts[disk])
		}
		if totalFiles == 0 && totalParts == 0 {
			continue
		}
		partSize := t.TotalBytes / uint64(totalParts)
		//re-balance parts
		for disk := range t.Parts {
			if _, diskExists := b.DiskToPathMap[disk]; !diskExists && disk != b.cfg.ClickHouse.EmbeddedBackupDisk {
				diskType := remoteBackup.DiskTypes[disk]
				storagePolicy, filteredDisks, err := filterDisksByTypeAndStoragePolicies(disk, diskType, disks, remoteBackup, *t)
				if err != nil {
					return err
				}
				rebalancedDisks := common.EmptyMap{}
				for j := range t.Parts[disk] {
					isObjectDisk, downloadDisk, newFreeSpace, reBalanceErr := b.getDownloadDiskForNonExistsDisk(diskType, filteredDisks, partSize)
					if reBalanceErr != nil {
						return reBalanceErr
					}
					rebalancedDisks[downloadDisk] = struct{}{}
					tableMetadataAfterDownload[i].Parts[disk][j].RebalancedDisk = downloadDisk
					isRebalanced = true
					if !isObjectDisk {
						updateDiskFreeSize(downloadDisk, diskType, storagePolicy, newFreeSpace)
					}
					//re-balance file depend on part
					if t.Files != nil && len(t.Files) > 0 {
						if len(t.Files[disk]) == 0 {
							return fmt.Errorf("table: `%s`.`%s` part.Name: %s, part.RebalancedDisk: %s, non empty `files` can't find disk: %s", t.Table, t.Database, t.Parts[disk][j].Name, t.Parts[disk][j].RebalancedDisk, disk)
						}
						for _, fileName := range t.Files[disk] {
							if strings.HasPrefix(fileName, disk+"_"+t.Parts[disk][j].Name+".") {
								if tableMetadataAfterDownload[i].RebalancedFiles == nil {
									tableMetadataAfterDownload[i].RebalancedFiles = map[string]string{}
								}
								tableMetadataAfterDownload[i].RebalancedFiles[fileName] = downloadDisk
							}
						}
					}
				}
				rebalancedDisksStr := strings.TrimPrefix(
					strings.Replace(fmt.Sprintf("%v", rebalancedDisks), ":{}", "", -1), "map",
				)
				log.Warn().Msgf("table '%s.%s' require disk '%s' that not found in system.disks, you can add nonexistent disks to `disk_mapping` in `clickhouse` config section, data will download to %v", t.Database, t.Table, disk, rebalancedDisksStr)
			}
		}
		if isRebalanced {
			if _, saveErr := t.Save(t.LocalFile, false); saveErr != nil {
				return saveErr
			}
		}
	}

	return nil
}

func (b *Backuper) downloadTableMetadataIfNotExists(ctx context.Context, backupName string, tableTitle metadata.TableTitle) (*metadata.TableMetadata, error) {
	metadataLocalFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	tm := &metadata.TableMetadata{}
	if _, err := tm.Load(metadataLocalFile); err == nil {
		return tm, nil
	}
	// we always download full metadata in this case without filter by partitions
	logger := log.With().Fields(map[string]interface{}{"operation": "downloadTableMetadataIfNotExists", "backupName": backupName, "table_metadata_diff": fmt.Sprintf("%s.%s", tableTitle.Database, tableTitle.Table)}).Logger()
	tm, _, err := b.downloadTableMetadata(ctx, backupName, nil, tableTitle, false, nil, false, logger)
	return tm, err
}

func (b *Backuper) downloadTableMetadata(ctx context.Context, backupName string, disks []clickhouse.Disk, tableTitle metadata.TableTitle, schemaOnly bool, partitions []string, resume bool, logger zerolog.Logger) (*metadata.TableMetadata, uint64, error) {
	start := time.Now()
	size := uint64(0)
	metadataFiles := map[string]string{}
	remoteMedataPrefix := path.Join(backupName, "metadata", common.TablePathEncode(tableTitle.Database), common.TablePathEncode(tableTitle.Table))
	metadataFiles[fmt.Sprintf("%s.json", remoteMedataPrefix)] = path.Join(b.DefaultDataPath, "backup", backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	partitionsIdMap := make(map[metadata.TableTitle]common.EmptyMap)
	if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		metadataFiles[fmt.Sprintf("%s.sql", remoteMedataPrefix)] = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.sql", common.TablePathEncode(tableTitle.Table)))
		metadataFiles[fmt.Sprintf("%s.json", remoteMedataPrefix)] = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	}
	var tableMetadata metadata.TableMetadata
	for remoteMetadataFile, localMetadataFile := range metadataFiles {
		if resume {
			isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(localMetadataFile)
			if isProcessed && strings.HasSuffix(localMetadataFile, ".json") {
				tmBody, err := os.ReadFile(localMetadataFile)
				if err != nil {
					return nil, 0, err
				}
				if err = json.Unmarshal(tmBody, &tableMetadata); err != nil {
					return nil, 0, err
				}
				partitionsIdMap, _ = partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, nil, ListOfTables{&tableMetadata}, partitions)
				filterPartsAndFilesByPartitionsFilter(tableMetadata, partitionsIdMap[metadata.TableTitle{Database: tableMetadata.Database, Table: tableMetadata.Table}])
				tableMetadata.LocalFile = localMetadataFile
			}
			if isProcessed {
				size += uint64(processedSize)
				continue
			}
		}
		var tmBody []byte
		retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
		err := retry.RunCtx(ctx, func(ctx context.Context) error {
			tmReader, err := b.dst.GetFileReader(ctx, remoteMetadataFile)
			if err != nil {
				return fmt.Errorf("can't GetFileReader(%s) error: %v", remoteMetadataFile, err)
			}
			tmBody, err = io.ReadAll(tmReader)
			if err != nil {
				return fmt.Errorf("can't io.ReadAll(%s) error: %v", remoteMetadataFile, err)
			}
			err = tmReader.Close()
			if err != nil {
				return fmt.Errorf("can't Close(%s) error: %v", remoteMetadataFile, err)
			}
			return nil
		})
		// sql file could be not present in incremental backup
		if err != nil && strings.HasSuffix(localMetadataFile, ".sql") {
			log.Warn().Str("localMetadataFile", localMetadataFile).Err(err).Send()
			continue
		}

		if err = os.MkdirAll(path.Dir(localMetadataFile), 0755); err != nil {
			return nil, 0, err
		}
		var written int64
		if strings.HasSuffix(localMetadataFile, ".sql") {
			if err = os.WriteFile(localMetadataFile, tmBody, 0640); err != nil {
				return nil, 0, err
			}
			if err = filesystemhelper.Chown(localMetadataFile, b.ch, disks, false); err != nil {
				return nil, 0, err
			}
			written = int64(len(tmBody))
			size += uint64(len(tmBody))
		} else {
			if err = json.Unmarshal(tmBody, &tableMetadata); err != nil {
				return nil, 0, fmt.Errorf("can't json.Unmarshal(%s) error: %v", remoteMetadataFile, err)
			}
			if b.shouldSkipByTableEngine(tableMetadata) || b.shouldSkipByTableName(fmt.Sprintf("%s.%s", tableMetadata.Database, tableMetadata.Table)) {
				return nil, 0, nil
			}
			partitionsIdMap, _ = partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, nil, ListOfTables{&tableMetadata}, partitions)
			filterPartsAndFilesByPartitionsFilter(tableMetadata, partitionsIdMap[metadata.TableTitle{Database: tableMetadata.Database, Table: tableMetadata.Table}])
			// save metadata
			jsonSize := uint64(0)
			jsonSize, err = tableMetadata.Save(localMetadataFile, schemaOnly)
			if err != nil {
				return nil, 0, err
			}
			written = int64(jsonSize)
			size += jsonSize
			tableMetadata.LocalFile = localMetadataFile
		}
		if resume {
			b.resumableState.AppendToState(localMetadataFile, written)
		}
	}
	logger.Info().Fields(map[string]string{
		"operation": "download_metadata",
		"backup":    backupName,
		"duration":  utils.HumanizeDuration(time.Since(start)),
		"size":      utils.FormatBytes(size),
	}).Msg("done")
	return &tableMetadata, size, nil
}

// downloadMissedInnerTablesMetadata - download, missed .inner. tables if materialized view query not contains `TO db.table` clause, https://github.com/Altinity/clickhouse-backup/issues/765
// @todo think about parallel download if sequentially will slow
func (b *Backuper) downloadMissedInnerTablesMetadata(ctx context.Context, backupName string, metadataSize uint64, tablesForDownload []metadata.TableTitle, tableMetadataAfterDownload ListOfTables, disks []clickhouse.Disk, schemaOnly bool, partitions []string) (ListOfTables, []metadata.TableTitle, uint64, error) {
	if b.isEmbedded {
		return tableMetadataAfterDownload, tablesForDownload, metadataSize, nil
	}
	for _, t := range tableMetadataAfterDownload {
		if t == nil {
			continue
		}
		if strings.HasPrefix(t.Query, "ATTACH MATERIALIZED") || strings.HasPrefix(t.Query, "CREATE MATERIALIZED") {
			if strings.Contains(t.Query, " TO ") && !strings.Contains(t.Query, " TO INNER UUID") {
				continue
			}
			var innerTableName string
			if matches := uuidRE.FindStringSubmatch(t.Query); len(matches) > 0 {
				innerTableName = fmt.Sprintf(".inner_id.%s", matches[1])
			} else {
				innerTableName = fmt.Sprintf(".inner.%s", t.Table)
			}
			innerTableExists := false
			for _, existsTable := range tablesForDownload {
				if existsTable.Table == innerTableName && existsTable.Database == t.Database {
					innerTableExists = true
					break
				}
			}
			if !innerTableExists {
				innerTableTitle := metadata.TableTitle{Database: t.Database, Table: innerTableName}
				metadataLogger := log.With().Str("missed_inner_metadata", fmt.Sprintf("%s.%s", innerTableTitle.Database, innerTableTitle.Table)).Logger()
				innerTableMetadata, size, err := b.downloadTableMetadata(ctx, backupName, disks, innerTableTitle, schemaOnly, partitions, b.resume, metadataLogger)
				if err != nil {
					return tableMetadataAfterDownload, tablesForDownload, metadataSize, err
				}
				metadataSize += size
				tablesForDownload = append(tablesForDownload, innerTableTitle)
				tableMetadataAfterDownload = append(tableMetadataAfterDownload, innerTableMetadata)
			}
		}
	}
	return tableMetadataAfterDownload, tablesForDownload, metadataSize, nil
}

func (b *Backuper) downloadRBACData(ctx context.Context, remoteBackup storage.Backup) (uint64, error) {
	return b.downloadBackupRelatedDir(ctx, remoteBackup, "access")
}

func (b *Backuper) downloadConfigData(ctx context.Context, remoteBackup storage.Backup) (uint64, error) {
	return b.downloadBackupRelatedDir(ctx, remoteBackup, "configs")
}

func (b *Backuper) downloadNamedCollectionsData(ctx context.Context, remoteBackup storage.Backup) (uint64, error) {
	return b.downloadBackupRelatedDir(ctx, remoteBackup, "named_collections")
}

func (b *Backuper) downloadBackupRelatedDir(ctx context.Context, remoteBackup storage.Backup, prefix string) (uint64, error) {
	localDir := path.Join(b.DefaultDataPath, "backup", remoteBackup.BackupName, prefix)

	if remoteBackup.DataFormat != DirectoryFormat {
		prefix = fmt.Sprintf("%s.%s", prefix, config.ArchiveExtensions[remoteBackup.DataFormat])
	}
	remoteSource := path.Join(remoteBackup.BackupName, prefix)

	if b.resume {
		if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(remoteSource); isProcessed {
			return uint64(processedSize), nil
		}
	}
	var downloadErr error
	downloadedBytes := int64(0)
	if remoteBackup.DataFormat == DirectoryFormat {
		if downloadedBytes, downloadErr = b.dst.DownloadPath(ctx, remoteSource, localDir, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter, b, b.cfg.General.DownloadMaxBytesPerSecond); downloadErr != nil {
			//SFTP can't walk on non exists paths and return error
			if !strings.Contains(downloadErr.Error(), "not exist") {
				return 0, downloadErr
			}
		}
		if _, err := os.Stat(localDir); err != nil && os.IsNotExist(err) {
			return 0, nil
		}
		if b.resume {
			b.resumableState.AppendToState(remoteSource, downloadedBytes)
		}
		log.Debug().Str("remoteSource", remoteSource).Str("operation", "downloadBackupRelatedDir").Msg("done")
		return uint64(downloadedBytes), nil
	}
	remoteFileInfo, err := b.dst.StatFile(ctx, remoteSource)
	if err != nil {
		log.Debug().Msgf("%s not exists on remote storage, skip download", remoteSource)
		return 0, nil
	}
	retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		downloadedBytes, downloadErr = b.dst.DownloadCompressedStream(ctx, remoteSource, localDir, b.cfg.General.DownloadMaxBytesPerSecond)
		return downloadErr
	})
	if err != nil {
		return 0, err
	}
	if b.resume {
		b.resumableState.AppendToState(remoteSource, remoteFileInfo.Size())
	}
	log.Debug().Str("remoteSource", remoteSource).Str("operation", "downloadBackupRelatedDir").Msg("done")
	return uint64(remoteFileInfo.Size()), nil
}

func (b *Backuper) downloadTableData(ctx context.Context, remoteBackup metadata.BackupMetadata, table metadata.TableMetadata, disks []clickhouse.Disk, hardlinkExistsFiles bool) (uint64, error) {
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	dataGroup, dataCtx := errgroup.WithContext(ctx)
	dataGroup.SetLimit(int(b.cfg.General.DownloadConcurrency))
	downloadedSize := uint64(0)
	var isRebalancedAfterHardLinks atomic.Bool

	if remoteBackup.DataFormat != DirectoryFormat {
		capacity := 0
		downloadOffset := make(map[string]int)
		for disk := range table.Files {
			capacity += len(table.Files[disk])
			downloadOffset[disk] = 0
		}
		log.Debug().Msgf("start %s.%s with concurrency=%d len(table.Files[...])=%d", table.Database, table.Table, b.cfg.General.DownloadConcurrency, capacity)
		for common.SumMapValuesInt(downloadOffset) < capacity {
			for disk := range table.Files {
				if downloadOffset[disk] >= len(table.Files[disk]) {
					continue
				}
				archiveFile := table.Files[disk][downloadOffset[disk]]
				capturedDisk := disk
				isRebalanced := false
				if capturedDisk, isRebalanced = table.RebalancedFiles[archiveFile]; !isRebalanced {
					capturedDisk = disk
				}
				capturedParts := table.Parts[capturedDisk]
				tableLocalDir := b.getLocalBackupDataPathForTable(remoteBackup.BackupName, capturedDisk, dbAndTableDir)
				downloadOffset[disk] += 1
				tableRemoteFile := path.Join(remoteBackup.BackupName, "shadow", common.TablePathEncode(table.Database), common.TablePathEncode(table.Table), archiveFile)
				dataGroup.Go(func() error {
					log.Debug().Msgf("start download %s", tableRemoteFile)
					if b.resume {
						if isProcessed, downloadedFileSize := b.resumableState.IsAlreadyProcessed(tableRemoteFile); isProcessed {
							atomic.AddUint64(&downloadedSize, uint64(downloadedFileSize))
							return nil
						}
					}
					if hardlinkExistsFiles {
						ext := "." + config.ArchiveExtensions[remoteBackup.DataFormat]
						partName := strings.TrimPrefix(strings.TrimSuffix(archiveFile, ext), capturedDisk+"_")
						var foundPart *metadata.Part
						var idx int
						for i, part := range capturedParts {
							if part.Name == partName {
								foundPart = &part
								idx = i
								break
							}
						}
						if foundPart != nil {
							found, size, err := b.hardlinkIfLocalPartExistsAndChecksumEqual(remoteBackup.BackupName, table, foundPart, disks, capturedDisk, dbAndTableDir)
							if err != nil {
								return err
							}
							if found {
								if foundPart.RebalancedDisk != "" && foundPart.RebalancedDisk != capturedDisk {
									table.Parts[capturedDisk][idx] = *foundPart
									isRebalancedAfterHardLinks.Store(true)
								}
								atomic.AddUint64(&downloadedSize, uint64(size))
								if b.resume {
									b.resumableState.AppendToState(tableRemoteFile, size)
								}
								return nil
							}

						}
						if foundPart == nil {
							// continue here can be dangerous, we just not download this part
							log.Warn().Msgf("part %s not found in metadata for archive %s on disk %s, this part will be downloaded", partName, archiveFile, disk)
						}
					}
					retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
					var downloadedBytes int64
					var downloadErr error
					err := retry.RunCtx(dataCtx, func(dataCtx context.Context) error {
						downloadedBytes, downloadErr = b.dst.DownloadCompressedStream(dataCtx, tableRemoteFile, tableLocalDir, b.cfg.General.DownloadMaxBytesPerSecond)
						if downloadErr != nil {
							return downloadErr
						}
						return nil
					})
					if err != nil {
						return err
					}
					atomic.AddUint64(&downloadedSize, uint64(downloadedBytes))
					if b.resume {
						b.resumableState.AppendToState(tableRemoteFile, downloadedBytes)
					}
					log.Debug().Msgf("finish download %s", tableRemoteFile)
					return nil
				})
			}
		}
	} else {
		capacity := 0
		for disk := range table.Parts {
			capacity += len(table.Parts[disk])
		}
		log.Debug().Msgf("start %s.%s with concurrency=%d len(table.Parts[...])=%d", table.Database, table.Table, b.cfg.General.DownloadConcurrency, capacity)

		for disk, parts := range table.Parts {
			tableRemotePath := path.Join(remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
			diskPath, diskExists := b.DiskToPathMap[disk]
			tableLocalPath := path.Join(diskPath, "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
			if b.isEmbedded {
				tableLocalPath = path.Join(diskPath, remoteBackup.BackupName, "data", dbAndTableDir)
			}
			for i, part := range parts {
				if part.Required {
					continue
				}
				if !diskExists {
					diskPath, diskExists = b.DiskToPathMap[part.RebalancedDisk]
					if !diskExists {
						return 0, fmt.Errorf("downloadTableData: table: `%s`.`%s`, disk: %s, part.Name: %s, part.RebalancedDisk: %s not rebalanced", table.Table, table.Database, disk, part.Name, part.RebalancedDisk)
					}
				}
				partRemotePath := path.Join(tableRemotePath, part.Name)
				partLocalPath := path.Join(tableLocalPath, part.Name)
				capturedPart := part
				capturedDisk := disk
				idx := i
				dataGroup.Go(func() error {
					log.Debug().Msgf("start %s -> %s", partRemotePath, partLocalPath)
					if b.resume {
						isProcesses, pathSize := b.resumableState.IsAlreadyProcessed(partRemotePath)
						atomic.AddUint64(&downloadedSize, uint64(pathSize))
						if isProcesses {
							return nil
						}
					}
					if hardlinkExistsFiles {
						found, size, err := b.hardlinkIfLocalPartExistsAndChecksumEqual(remoteBackup.BackupName, table, &capturedPart, disks, capturedDisk, dbAndTableDir)
						if err != nil {
							return err
						}
						if found {
							if capturedPart.RebalancedDisk != "" && capturedPart.RebalancedDisk != capturedDisk {
								table.Parts[capturedDisk][idx] = capturedPart
								isRebalancedAfterHardLinks.Store(true)
							}
							atomic.AddUint64(&downloadedSize, uint64(size))
							if b.resume {
								b.resumableState.AppendToState(partRemotePath, size)
							}
							return nil
						}
					}

					pathSize, downloadErr := b.dst.DownloadPath(dataCtx, partRemotePath, partLocalPath, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter, b, b.cfg.General.DownloadMaxBytesPerSecond)
					if downloadErr != nil {
						return downloadErr
					}
					atomic.AddUint64(&downloadedSize, uint64(pathSize))
					if b.resume {
						b.resumableState.AppendToState(partRemotePath, pathSize)
					}
					log.Debug().Msgf("finish %s -> %s", partRemotePath, partLocalPath)
					return nil
				})
			}
		}
	}
	if err := dataGroup.Wait(); err != nil {
		return 0, fmt.Errorf("one of downloadTableData go-routine return error: %v", err)
	}
	if isRebalancedAfterHardLinks.Load() {
		if _, saveErr := table.Save(table.LocalFile, false); saveErr != nil {
			return 0, saveErr
		}
	}
	if !b.isEmbedded && remoteBackup.RequiredBackup != "" {
		diffBytes, err := b.downloadDiffParts(ctx, remoteBackup, table, dbAndTableDir, disks, hardlinkExistsFiles)
		if err != nil {
			return 0, err
		}
		downloadedSize += uint64(diffBytes)
	}

	return downloadedSize, nil
}

func (b *Backuper) hardlinkIfLocalPartExistsAndChecksumEqual(backupName string, table metadata.TableMetadata, part *metadata.Part, disks []clickhouse.Disk, diskName, dbAndTableDir string) (bool, int64, error) {
	diskType := ""
	for _, d := range disks {
		if d.Name == diskName {
			diskType = d.Type

			break
		}
	}
	if diskType == "" {
		return false, 0, fmt.Errorf("can't find %s in disks=%v", diskName, disks)
	}
	if _, exists := table.Checksums[part.Name]; !exists {
		return false, 0, nil
	}
	for _, localDisk := range disks {
		if localDisk.Type != diskType {
			continue
		}
		if localDisk.IsBackup {
			continue
		}
		var existingPartPath string
		p1 := path.Join(localDisk.Path, "data", dbAndTableDir, part.Name)
		if _, err := os.Stat(p1); err == nil {
			existingPartPath = p1
		}
		if existingPartPath == "" && table.UUID != "" {
			p2 := path.Join(localDisk.Path, "store", table.UUID[:3], table.UUID, part.Name)
			if _, err := os.Stat(p2); err == nil {
				existingPartPath = p2
			}
		}

		if existingPartPath != "" {
			checksum, err := common.CalculateChecksum(existingPartPath, "checksums.txt")
			if err != nil {
				log.Warn().Msgf("calculating checksum for %s failed: %v", existingPartPath, err)
				return false, 0, nil
			}
			if checksum == table.Checksums[part.Name] {
				partLocalPath := path.Join(b.getLocalBackupDataPathForTable(backupName, localDisk.Name, dbAndTableDir), part.Name)
				log.Info().Msgf("Found existing part %s with matching checksum, creating hardlinks to %s", existingPartPath, partLocalPath)
				if err := b.makePartHardlinks(existingPartPath, partLocalPath); err != nil {
					return false, 0, fmt.Errorf("failed to create hardlinks for %s: %v", existingPartPath, err)
				}
				if diskName != localDisk.Name {
					part.RebalancedDisk = localDisk.Name
				}
				var partSize int64
				walkErr := filepath.Walk(existingPartPath, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if !info.IsDir() {
						partSize += info.Size()
					}
					return nil
				})
				if walkErr != nil {
					return false, 0, fmt.Errorf("failed to calculate size of %s: %v", existingPartPath, walkErr)
				}
				return true, partSize, nil
			} else {
				log.Warn().Msgf("Found existing part %s but checksums do not match. Expected %d, got %d. Will download.", existingPartPath, table.Checksums[part.Name], checksum)
			}
		}
	}
	return false, 0, nil
}

func (b *Backuper) downloadDiffParts(ctx context.Context, remoteBackup metadata.BackupMetadata, table metadata.TableMetadata, dbAndTableDir string, disks []clickhouse.Disk, hardlinkExistsFiles bool) (int64, error) {
	log.Debug().
		Str("backup", remoteBackup.BackupName).
		Str("operation", "downloadDiffParts").
		Str("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).
		Msg("start")
	start := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	downloadedDiffBytes := int64(0)
	downloadedDiffParts := uint32(0)
	downloadDiffGroup, downloadDiffCtx := errgroup.WithContext(ctx)
	downloadDiffGroup.SetLimit(int(b.cfg.General.DownloadConcurrency))
	diffRemoteFilesCache := map[string]*sync.Mutex{}
	diffRemoteFilesLock := &sync.Mutex{}
	isRebalancedAfterHardLinks := false

	for disk, parts := range table.Parts {
		diskPath, diskExists := b.DiskToPathMap[disk]
		for i, part := range parts {
			if !diskExists && part.RebalancedDisk == "" {
				return 0, fmt.Errorf("downloadDiffParts: table: `%s`.`%s`, disk: %s, part.Name: %s, part.RebalancedDisk: `%s` not rebalanced", table.Table, table.Database, disk, part.Name, part.RebalancedDisk)
			}
			if part.RebalancedDisk != "" {
				diskPath, diskExists = b.DiskToPathMap[part.RebalancedDisk]
				if !diskExists {
					return 0, fmt.Errorf("downloadDiffParts: table: `%s`.`%s`, disk: %s, part.Name: %s, part.RebalancedDisk: `%s` not rebalanced", table.Table, table.Database, disk, part.Name, part.RebalancedDisk)
				}
				disk = part.RebalancedDisk
				if b.shouldDiskNameSkipByNameOrType(disk, disks) {
					log.Warn().Str("database", table.Database).Str("table", table.Table).Str("rebalancedDisk", part.RebalancedDisk).Msg("skipped")
					continue
				}
			}
			newPath := path.Join(diskPath, "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk, part.Name)
			if checkErr := b.checkNewPath(newPath, part); checkErr != nil {
				return 0, checkErr
			}
			if !part.Required {
				continue
			}
			existsPath := path.Join(b.DiskToPathMap[disk], "backup", remoteBackup.RequiredBackup, "shadow", dbAndTableDir, disk, part.Name)
			_, statErr := os.Stat(existsPath)
			if statErr != nil && !os.IsNotExist(statErr) {
				return 0, fmt.Errorf("%s stat return error: %v", existsPath, statErr)
			}
			if statErr != nil && os.IsNotExist(statErr) {
				//if existPath already processed then expect non-empty newPath
				if b.resume {
					isProcessed, pathDiffBytes := b.resumableState.IsAlreadyProcessed(existsPath)
					if isProcessed {
						if newPathDirList, newPathDirErr := os.ReadDir(newPath); newPathDirErr != nil {
							newPathDirErr = fmt.Errorf("os.ReadDir(%s) error: %v", newPath, newPathDirErr)
							log.Error().Msg(newPathDirErr.Error())
							return 0, newPathDirErr
						} else if len(newPathDirList) == 0 {
							return 0, fmt.Errorf("os.ReadDir(%s) unexpected empty", newPath)
						}
						downloadedDiffBytes += pathDiffBytes
						continue
					}
				}
				partForDownload := part
				diskForDownload := disk
				capturedExistsPath := existsPath
				capturedNewPath := newPath
				capturedDisk := disk
				idx := i
				downloadDiffGroup.Go(func() error {
					if hardlinkExistsFiles {
						found, size, err := b.hardlinkIfLocalPartExistsAndChecksumEqual(remoteBackup.BackupName, table, &partForDownload, disks, capturedDisk, dbAndTableDir)
						if err != nil {
							return err
						}
						if found {
							if partForDownload.RebalancedDisk != "" && partForDownload.RebalancedDisk != capturedDisk {
								table.Parts[capturedDisk][idx] = partForDownload
								isRebalancedAfterHardLinks = true
							}
							atomic.AddInt64(&downloadedDiffBytes, size)
							atomic.AddUint32(&downloadedDiffParts, 1)
							if b.resume {
								b.resumableState.AppendToState(capturedExistsPath, size)
							}
							return nil
						}
					}
					tableRemoteFiles, findErr := b.findDiffBackupFilesRemote(downloadDiffCtx, remoteBackup, table, diskForDownload, partForDownload)
					if findErr != nil {
						return findErr
					}
					pathDiffBytes := int64(0)
					for tableRemoteFile, tableLocalDir := range tableRemoteFiles {
						fileDiffBytes, downloadErr := b.downloadDiffRemoteFile(downloadDiffCtx, diffRemoteFilesLock, diffRemoteFilesCache, tableRemoteFile, tableLocalDir)
						if downloadErr != nil {
							return downloadErr
						}
						downloadedPartPath := path.Join(tableLocalDir, partForDownload.Name)
						if downloadedPartPath != capturedExistsPath {
							info, err := os.Stat(downloadedPartPath)
							if err == nil {
								if !info.IsDir() {
									return fmt.Errorf("after downloadDiffRemoteFile %s exists but is not directory", downloadedPartPath)
								}
								if err = b.makePartHardlinks(downloadedPartPath, capturedExistsPath); err != nil {
									return fmt.Errorf("can't to add link to rebalanced part %s -> %s error: %v", downloadedPartPath, capturedExistsPath, err)
								}
							}
							if err != nil && !os.IsNotExist(err) {
								return fmt.Errorf("after downloadDiffRemoteFile os.Stat(%s) return error: %v", downloadedPartPath, err)
							}
						}
						pathDiffBytes += fileDiffBytes
						atomic.AddInt64(&downloadedDiffBytes, fileDiffBytes)
						atomic.AddUint32(&downloadedDiffParts, 1)
					}
					if err := b.makePartHardlinks(capturedExistsPath, capturedNewPath); err != nil {
						return fmt.Errorf("can't to add link to exists part %s -> %s error: %v", capturedNewPath, capturedExistsPath, err)
					}
					if b.resume {
						b.resumableState.AppendToState(capturedExistsPath, pathDiffBytes)
					}
					return nil
				})
			} else {
				if !b.resume || (b.resume && !b.resumableState.IsAlreadyProcessedBool(existsPath)) {
					if statErr = b.makePartHardlinks(existsPath, newPath); statErr != nil {
						return 0, fmt.Errorf("can't to add exists part: %v", statErr)
					}
				}
			}
		}
	}
	if err := downloadDiffGroup.Wait(); err != nil {
		return 0, fmt.Errorf("one of downloadDiffParts go-routine return error: %v", err)
	}
	if isRebalancedAfterHardLinks {
		if _, saveErr := table.Save(table.LocalFile, false); saveErr != nil {
			return 0, saveErr
		}
	}

	log.Info().
		Str("backup", remoteBackup.BackupName).
		Str("operation", "downloadDiffParts").
		Str("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).
		Str("duration", utils.HumanizeDuration(time.Since(start))).
		Str("diff_parts", strconv.Itoa(int(downloadedDiffParts))).
		Str("diff_bytes", utils.FormatBytes(uint64(downloadedDiffBytes))).
		Msg("done")
	return downloadedDiffBytes, nil
}

func (b *Backuper) downloadDiffRemoteFile(ctx context.Context, diffRemoteFilesLock *sync.Mutex, diffRemoteFilesCache map[string]*sync.Mutex, tableRemoteFile string, tableLocalDir string) (int64, error) {
	if b.resume {
		if isProcessed, downloadedBytes := b.resumableState.IsAlreadyProcessed(tableRemoteFile); isProcessed {
			return downloadedBytes, nil
		}
	}
	diffRemoteFilesLock.Lock()
	namedLock, isCached := diffRemoteFilesCache[tableRemoteFile]
	downloadedBytes := int64(0)
	if isCached {
		namedLock.Lock()
		log.Debug().Msgf("wait download begin %s", tableRemoteFile)
		diffRemoteFilesLock.Unlock()
		namedLock.Unlock()
		log.Debug().Msgf("wait download end %s", tableRemoteFile)
	} else {
		log.Debug().Msgf("start download from %s", tableRemoteFile)
		namedLock = &sync.Mutex{}
		diffRemoteFilesCache[tableRemoteFile] = namedLock
		namedLock.Lock()
		diffRemoteFilesLock.Unlock()
		if path.Ext(tableRemoteFile) != "" {
			retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
			err := retry.RunCtx(ctx, func(ctx context.Context) error {
				var downloadErr error
				downloadedBytes, downloadErr = b.dst.DownloadCompressedStream(ctx, tableRemoteFile, tableLocalDir, b.cfg.General.DownloadMaxBytesPerSecond)
				return downloadErr
			})
			if err != nil {
				log.Warn().Msgf("DownloadCompressedStream %s -> %s return error: %v", tableRemoteFile, tableLocalDir, err)
				return 0, err
			}
		} else {
			// remoteFile could be a directory
			if pathSize, err := b.dst.DownloadPath(ctx, tableRemoteFile, tableLocalDir, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter, b, b.cfg.General.DownloadMaxBytesPerSecond); err != nil {
				log.Warn().Msgf("DownloadPath %s -> %s return error: %v", tableRemoteFile, tableLocalDir, err)
				return 0, err
			} else {
				atomic.AddInt64(&downloadedBytes, pathSize)
			}
		}
		namedLock.Unlock()
		if b.resume {
			b.resumableState.AppendToState(tableRemoteFile, downloadedBytes)
		}
		log.Debug().Str("tableRemoteFile", tableRemoteFile).Msgf("finish download")
	}
	return downloadedBytes, nil
}

func (b *Backuper) checkNewPath(newPath string, part metadata.Part) error {
	info, err := os.Stat(newPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("%s stat return error: %v", newPath, err)
	}
	if os.IsNotExist(err) && !part.Required {
		return fmt.Errorf("%s not found after download backup", newPath)
	}
	if err == nil && !info.IsDir() {
		return fmt.Errorf("%s is not directory after download backup", newPath)
	}
	return nil
}

func (b *Backuper) findDiffBackupFilesRemote(ctx context.Context, backup metadata.BackupMetadata, table metadata.TableMetadata, disk string, part metadata.Part) (map[string]string, error) {
	var requiredTable *metadata.TableMetadata
	log.Debug().Fields(map[string]interface{}{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffBackupFilesRemote"}).Msg("start")
	requiredBackup, err := b.ReadBackupMetadataRemote(ctx, backup.RequiredBackup)
	if err != nil {
		return nil, err
	}
	requiredTable, err = b.downloadTableMetadataIfNotExists(ctx, requiredBackup.BackupName, metadata.TableTitle{Database: table.Database, Table: table.Table})
	if err != nil {
		log.Warn().Msgf("downloadTableMetadataIfNotExists %s / %s.%s return error", requiredBackup.BackupName, table.Database, table.Table)
		return nil, err
	}

	// recursive find if part in RequiredBackup also Required
	tableRemoteFiles, found, err := b.findDiffRecursive(ctx, requiredBackup, table, requiredTable, part, disk)
	if found {
		return tableRemoteFiles, nil
	}

	found = false
	// try to find part on the same disk
	tableRemoteFiles, err, found = b.findDiffOnePart(ctx, requiredBackup, table, disk, disk, part)
	if found {
		return tableRemoteFiles, nil
	}

	// try to find part on other disks
	for requiredDisk := range requiredBackup.Disks {
		if requiredDisk != disk {
			tableRemoteFiles, err, found = b.findDiffOnePart(ctx, requiredBackup, table, disk, requiredDisk, part)
			if found {
				return tableRemoteFiles, nil
			}
		}
	}
	// find one or multiple big files, disk_X.tar files by part.Name
	tableRemoteFiles = make(map[string]string)
	for requiredDisk, requiredParts := range requiredTable.Parts {
		for _, requiredPart := range requiredParts {
			if part.Name == requiredPart.Name {
				localTableDir := path.Join(b.DiskToPathMap[disk], "backup", requiredBackup.BackupName, "shadow", common.TablePathEncode(table.Database), common.TablePathEncode(table.Table), disk)
				for _, remoteFile := range requiredTable.Files[requiredDisk] {
					remoteFile = path.Join(requiredBackup.BackupName, "shadow", common.TablePathEncode(table.Database), common.TablePathEncode(table.Table), remoteFile)
					tableRemoteFiles[remoteFile] = localTableDir
				}
			}
		}
	}
	if len(tableRemoteFiles) > 0 {
		return tableRemoteFiles, nil
	}
	// not found
	return nil, fmt.Errorf("%s.%s %s not found on %s and all required backups sequence", table.Database, table.Table, part.Name, requiredBackup.BackupName)
}

func (b *Backuper) findDiffRecursive(ctx context.Context, requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, requiredTable *metadata.TableMetadata, part metadata.Part, disk string) (map[string]string, bool, error) {
	log.Debug().Fields(map[string]interface{}{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffRecursive"}).Msg("start")
	found := false
	for _, requiredParts := range requiredTable.Parts {
		for _, requiredPart := range requiredParts {
			if requiredPart.Name == part.Name {
				found = true
				if requiredPart.Required {
					tableRemoteFiles, err := b.findDiffBackupFilesRemote(ctx, *requiredBackup, table, disk, part)
					if err != nil {
						found = false
						log.Warn().Msgf("try find %s.%s %s recursive return err: %v", table.Database, table.Table, part.Name, err)
					}
					return tableRemoteFiles, found, err
				}
				break
			}
		}
		if found {
			break
		}
	}
	return nil, false, nil
}

func (b *Backuper) findDiffOnePart(ctx context.Context, requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, localDisk, remoteDisk string, part metadata.Part) (map[string]string, error, bool) {
	log.Debug().Fields(map[string]interface{}{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffOnePart"}).Msg("start")
	tableRemoteFiles := make(map[string]string)
	// find same disk and part name archive
	if requiredBackup.DataFormat != DirectoryFormat {
		if tableRemoteFile, tableLocalDir, err := b.findDiffOnePartArchive(ctx, requiredBackup, table, localDisk, remoteDisk, part); err == nil {
			tableRemoteFiles[tableRemoteFile] = tableLocalDir
			return tableRemoteFiles, nil, true
		}
	} else {
		// find same disk and part name directory
		if tableRemoteFile, tableLocalDir, err := b.findDiffOnePartDirectory(ctx, requiredBackup, table, localDisk, remoteDisk, part); err == nil {
			tableRemoteFiles[tableRemoteFile] = tableLocalDir
			return tableRemoteFiles, nil, true
		}
	}
	return nil, nil, false
}

func (b *Backuper) findDiffOnePartDirectory(ctx context.Context, requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, localDisk, remoteDisk string, part metadata.Part) (string, string, error) {
	log.Debug().Fields(map[string]interface{}{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffOnePartDirectory"}).Msg("start")
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	tableRemotePath := path.Join(requiredBackup.BackupName, "shadow", dbAndTableDir, remoteDisk, part.Name)
	tableRemoteFile := path.Join(tableRemotePath, "checksums.txt")
	return b.findDiffFileExist(ctx, requiredBackup, tableRemoteFile, tableRemotePath, localDisk, dbAndTableDir, part)
}

func (b *Backuper) findDiffOnePartArchive(ctx context.Context, requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, localDisk, remoteDisk string, part metadata.Part) (string, string, error) {
	log.Debug().Fields(map[string]interface{}{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffOnePartArchive"}).Msg("start")
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	remoteExt := config.ArchiveExtensions[requiredBackup.DataFormat]
	tableRemotePath := path.Join(requiredBackup.BackupName, "shadow", dbAndTableDir, fmt.Sprintf("%s_%s.%s", remoteDisk, common.TablePathEncode(part.Name), remoteExt))
	tableRemoteFile := tableRemotePath
	return b.findDiffFileExist(ctx, requiredBackup, tableRemoteFile, tableRemotePath, localDisk, dbAndTableDir, part)
}

func (b *Backuper) findDiffFileExist(ctx context.Context, requiredBackup *metadata.BackupMetadata, tableRemoteFile string, tableRemotePath string, localDisk string, dbAndTableDir string, part metadata.Part) (string, string, error) {
	_, err := b.dst.StatFile(ctx, tableRemoteFile)
	if err != nil {
		log.Debug().Fields(map[string]interface{}{"tableRemoteFile": tableRemoteFile, "tableRemotePath": tableRemotePath, "part": part.Name}).Msg("findDiffFileExist not found")
		return "", "", err
	}
	tableLocalDir, diskExists := b.DiskToPathMap[localDisk]
	if !diskExists {
		tableLocalDir, diskExists = b.DiskToPathMap[part.RebalancedDisk]
		if !diskExists {
			return "", "", fmt.Errorf("localDisk:%s, part.Name: %s, part.RebalancedDisk: %s is not found in system.disks", localDisk, part.Name, part.RebalancedDisk)
		}
		localDisk = part.RebalancedDisk
	}

	if path.Ext(tableRemoteFile) == ".txt" {
		tableLocalDir = path.Join(tableLocalDir, "backup", requiredBackup.BackupName, "shadow", dbAndTableDir, localDisk, part.Name)
	} else {
		tableLocalDir = path.Join(tableLocalDir, "backup", requiredBackup.BackupName, "shadow", dbAndTableDir, localDisk)
	}
	log.Debug().Fields(map[string]interface{}{"tableRemoteFile": tableRemoteFile, "tableRemotePath": tableRemotePath, "part": part.Name}).Msg("findDiffFileExist found")
	return tableRemotePath, tableLocalDir, nil
}

func (b *Backuper) ReadBackupMetadataRemote(ctx context.Context, backupName string) (*metadata.BackupMetadata, error) {
	backupList, err := b.dst.BackupList(ctx, true, backupName)
	if err != nil {
		return nil, err
	}
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			return &backup.BackupMetadata, nil
		}
	}
	return nil, fmt.Errorf("%s not found on remote storage", backupName)
}

func (b *Backuper) makePartHardlinks(exists, new string) error {
	_, err := os.Stat(exists)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(new, 0750); err != nil {
		log.Warn().Msgf("MkDirAll(%s) error: %v", new, err)
		return err
	}
	if walkErr := filepath.Walk(exists, func(fPath string, fInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		fPath = strings.TrimPrefix(fPath, exists)
		existsF := path.Join(exists, fPath)
		newF := path.Join(new, fPath)
		if fInfo.IsDir() {
			if err = os.MkdirAll(newF, fInfo.Mode()); err != nil {
				log.Warn().Msgf("MkdirAll(%s) error: %v", fPath, err)
				return err
			}
			return nil
		}

		if err = os.Link(existsF, newF); err != nil {
			existsFInfo, existsStatErr := os.Stat(existsF)
			newFInfo, newStatErr := os.Stat(newF)
			if existsStatErr != nil || newStatErr != nil || !os.SameFile(existsFInfo, newFInfo) {
				log.Warn().Msgf("Link %s -> %s error: %v, existsStatErr: %v newStatErr: %v", existsF, newF, err, existsStatErr, newStatErr)
				return err
			}
		}
		return nil
	}); walkErr != nil {
		log.Warn().Msgf("Link recursively %s -> %s return error: %v", new, exists, walkErr)
		return walkErr
	}
	return nil
}

func (b *Backuper) downloadSingleBackupFile(ctx context.Context, remoteFile string, localFile string, disks []clickhouse.Disk) (int64, error) {
	var size int64
	var isProcessed bool
	if b.resume {
		if isProcessed, size = b.resumableState.IsAlreadyProcessed(remoteFile); isProcessed {
			return size, nil
		}
	}
	retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
	err := retry.RunCtx(ctx, func(ctx context.Context) error {
		remoteReader, err := b.dst.GetFileReader(ctx, remoteFile)
		if err != nil {
			return err
		}
		defer func() {
			err = remoteReader.Close()
			if err != nil {
				log.Warn().Msgf("can't close remoteReader %s", remoteFile)
			}
		}()
		localWriter, err := os.OpenFile(localFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
		if err != nil {
			return err
		}

		defer func() {
			err = localWriter.Close()
			if err != nil {
				log.Warn().Msgf("can't close localWriter %s", localFile)
			}
		}()

		size, err = io.CopyBuffer(localWriter, remoteReader, nil)
		if err != nil {
			return err
		}

		if err = filesystemhelper.Chown(localFile, b.ch, disks, false); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if b.resume {
		b.resumableState.AppendToState(remoteFile, size)
	}
	return size, nil
}

// filterDisksByStoragePolicyAndType - https://github.com/Altinity/clickhouse-backup/issues/561
func (b *Backuper) splitDisksByTypeAndStoragePolicy(disks []clickhouse.Disk) map[string]map[string][]clickhouse.Disk {
	disksByTypeAndPolicy := map[string]map[string][]clickhouse.Disk{}
	for _, d := range disks {
		if !d.IsBackup {
			if _, typeExists := disksByTypeAndPolicy[d.Type]; !typeExists {
				disksByTypeAndPolicy[d.Type] = map[string][]clickhouse.Disk{}
			}
			for _, policy := range d.StoragePolicies {
				if _, policyExists := disksByTypeAndPolicy[d.Type][policy]; !policyExists {
					disksByTypeAndPolicy[d.Type][policy] = []clickhouse.Disk{}
				}
				disksByTypeAndPolicy[d.Type][policy] = append(disksByTypeAndPolicy[d.Type][policy], d)
			}
		}
	}
	return disksByTypeAndPolicy
}

// getDownloadDiskForNonExistsDisk - https://github.com/Altinity/clickhouse-backup/issues/561
// allow to Restore to new server with different storage policy, different disk count,
// implements `least_used` for normal disk and `random` for Object disks
func (b *Backuper) getDownloadDiskForNonExistsDisk(notExistsDiskType string, filteredDisks []clickhouse.Disk, partSize uint64) (bool, string, uint64, error) {
	// random for non-local disks
	if notExistsDiskType != "local" {
		roundRobinIdx := rand.Intn(len(filteredDisks))
		return true, filteredDisks[roundRobinIdx].Name, 0, nil
	}
	// least_used for local
	freeSpace := partSize
	leastUsedIdx := -1
	for idx, d := range filteredDisks {
		if filteredDisks[idx].FreeSpace > freeSpace {
			freeSpace = d.FreeSpace
			leastUsedIdx = idx
		}
	}
	if leastUsedIdx < 0 {
		return false, "", 0, fmt.Errorf("%s free space, not found in system.disks with `local` type", utils.FormatBytes(partSize))
	}
	return false, filteredDisks[leastUsedIdx].Name, filteredDisks[leastUsedIdx].FreeSpace - partSize, nil
}
