package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/custom"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/partition"
	"github.com/Altinity/clickhouse-backup/v2/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/eapache/go-resiliency/retrier"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

	apexLog "github.com/apex/log"
)

var (
	ErrBackupIsAlreadyExists = errors.New("backup is already exists")
)

func (b *Backuper) Download(backupName string, tablePattern string, partitions []string, schemaOnly, resume bool, backupVersion string, commandId int) error {
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

	log := b.log.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "download",
	})
	if b.cfg.General.RemoteStorage == "none" {
		return fmt.Errorf("general->remote_storage shall not be \"none\" for download, change you config or use REMOTE_STORAGE environment variable")
	}
	if b.cfg.General.DownloadConcurrency == 0 {
		return fmt.Errorf("`download_concurrency` shall be more than zero")
	}
	if !resume && b.cfg.General.UseResumableState {
		resume = true
	}
	b.resume = resume
	if backupName == "" {
		_ = b.PrintRemoteBackups(ctx, "all")
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

	for i := range localBackups {
		if backupName == localBackups[i].BackupName {
			if strings.Contains(localBackups[i].Tags, "embedded") || b.cfg.General.RemoteStorage == "custom" {
				return ErrBackupIsAlreadyExists
			}
			if !b.resume {
				return ErrBackupIsAlreadyExists
			} else {
				_, isResumeExists := os.Stat(path.Join(b.DefaultDataPath, "backup", backupName, "download.state"))
				if errors.Is(isResumeExists, os.ErrNotExist) {
					return ErrBackupIsAlreadyExists
				}
				log.Warnf("%s already exists will try to resume download", backupName)
			}
		}
	}
	startDownload := time.Now()
	if b.cfg.General.RemoteStorage == "custom" {
		return custom.Download(ctx, b.cfg, backupName, tablePattern, partitions, schemaOnly)
	}
	if err := b.initDisksPathsAndBackupDestination(ctx, disks, ""); err != nil {
		return err
	}
	defer func() {
		if err := b.dst.Close(ctx); err != nil {
			b.log.Warnf("can't close BackupDestination error: %v", err)
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
	tablesForDownload := parseTablePatternForDownload(remoteBackup.Tables, tablePattern)

	if !schemaOnly && !b.cfg.General.DownloadByPart && remoteBackup.RequiredBackup != "" {
		err := b.Download(remoteBackup.RequiredBackup, tablePattern, partitions, schemaOnly, b.resume, backupVersion, commandId)
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
		b.resumableState = resumable.NewState(b.DefaultDataPath, backupName, "download", map[string]interface{}{
			"tablePattern": tablePattern,
			"partitions":   partitions,
			"schemaOnly":   schemaOnly,
		})
	}

	log.Debugf("prepare table METADATA concurrent semaphore with concurrency=%d len(tablesForDownload)=%d", b.cfg.General.DownloadConcurrency, len(tablesForDownload))
	tableMetadataAfterDownload := make([]*metadata.TableMetadata, len(tablesForDownload))
	metadataGroup, metadataCtx := errgroup.WithContext(ctx)
	metadataGroup.SetLimit(int(b.cfg.General.DownloadConcurrency))
	for i, t := range tablesForDownload {
		metadataLogger := log.WithField("table_metadata", fmt.Sprintf("%s.%s", t.Database, t.Table))
		idx := i
		tableTitle := t
		metadataGroup.Go(func() error {
			downloadedMetadata, size, err := b.downloadTableMetadata(metadataCtx, backupName, disks, metadataLogger, tableTitle, schemaOnly, partitions, b.resume)
			if err != nil {
				return err
			}
			tableMetadataAfterDownload[idx] = downloadedMetadata
			atomic.AddUint64(&metadataSize, size)
			return nil
		})
	}
	if err := metadataGroup.Wait(); err != nil {
		return fmt.Errorf("one of Download Metadata go-routine return error: %v", err)
	}
	// download, missed .inner. tables, https://github.com/Altinity/clickhouse-backup/issues/765
	var missedInnerTableErr error
	tableMetadataAfterDownload, tablesForDownload, metadataSize, missedInnerTableErr = b.downloadMissedInnerTablesMetadata(ctx, backupName, metadataSize, tablesForDownload, tableMetadataAfterDownload, disks, schemaOnly, partitions, log)
	if missedInnerTableErr != nil {
		return fmt.Errorf("b.downloadMissedInnerTablesMetadata error: %v", missedInnerTableErr)
	}

	if !schemaOnly {
		if reBalanceErr := b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownload, disks, remoteBackup, log); reBalanceErr != nil {
			return reBalanceErr
		}
		log.Debugf("prepare table SHADOW concurrent semaphore with concurrency=%d len(tableMetadataAfterDownload)=%d", b.cfg.General.DownloadConcurrency, len(tableMetadataAfterDownload))
		dataGroup, dataCtx := errgroup.WithContext(ctx)
		dataGroup.SetLimit(int(b.cfg.General.DownloadConcurrency))

		for i, tableMetadata := range tableMetadataAfterDownload {
			if tableMetadata == nil || tableMetadata.MetadataOnly {
				continue
			}
			dataSize += tableMetadata.TotalBytes
			idx := i
			dataGroup.Go(func() error {
				start := time.Now()
				if err := b.downloadTableData(dataCtx, remoteBackup.BackupMetadata, *tableMetadataAfterDownload[idx]); err != nil {
					return err
				}
				log.WithFields(apexLog.Fields{
					"operation": "download_data",
					"table":     fmt.Sprintf("%s.%s", tableMetadataAfterDownload[idx].Database, tableMetadataAfterDownload[idx].Table),
					"progress":  fmt.Sprintf("%d/%d", idx, len(tableMetadataAfterDownload)),
					"duration":  utils.HumanizeDuration(time.Since(start)),
					"size":      utils.FormatBytes(tableMetadataAfterDownload[idx].TotalBytes),
					"version":   backupVersion,
				}).Info("done")
				return nil
			})
		}
		if err := dataGroup.Wait(); err != nil {
			return fmt.Errorf("one of Download go-routine return error: %v", err)
		}
	}
	var rbacSize, configSize uint64
	rbacSize, err = b.downloadRBACData(ctx, remoteBackup)
	if err != nil {
		return fmt.Errorf("download RBAC error: %v", err)
	}

	configSize, err = b.downloadConfigData(ctx, remoteBackup)
	if err != nil {
		return fmt.Errorf("download CONFIGS error: %v", err)
	}

	backupMetadata := remoteBackup.BackupMetadata
	backupMetadata.Tables = tablesForDownload
	backupMetadata.DataSize = dataSize
	backupMetadata.MetadataSize = metadataSize

	if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" && backupMetadata.Tables != nil && len(backupMetadata.Tables) > 0 {
		localClickHouseBackupFile := path.Join(b.EmbeddedBackupDataPath, backupName, ".backup")
		remoteClickHouseBackupFile := path.Join(backupName, ".backup")
		if err = b.downloadSingleBackupFile(ctx, remoteClickHouseBackupFile, localClickHouseBackupFile, disks); err != nil {
			return err
		}
	}

	backupMetadata.CompressedSize = 0
	backupMetadata.DataFormat = ""
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
		if localBackups, _, err = b.GetLocalBackups(ctx, disks); err == nil {
			for _, localBackup := range localBackups {
				if localBackup.BackupName != remoteBackup.BackupName && localBackup.DataSize+localBackup.CompressedSize+localBackup.MetadataSize == 0 {
					if err = b.RemoveBackupLocal(ctx, localBackup.BackupName, disks); err != nil {
						return fmt.Errorf("downloadWithDiff -> RemoveBackupLocal cleaning error: %v", err)
					} else {
						b.log.Infof("partial required backup %s deleted", localBackup.BackupName)
					}
				}
			}
		} else {
			return fmt.Errorf("downloadWithDiff -> GetLocalBackups cleaning error: %v", err)
		}
	}

	log.WithFields(apexLog.Fields{
		"duration": utils.HumanizeDuration(time.Since(startDownload)),
		"size":     utils.FormatBytes(dataSize + metadataSize + rbacSize + configSize),
		"version":  backupVersion,
	}).Info("done")
	return nil
}

func (b *Backuper) reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownload []*metadata.TableMetadata, disks []clickhouse.Disk, remoteBackup storage.Backup, log *apexLog.Entry) error {
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
				log.Warnf("table '%s.%s' require disk '%s' that not found in system.disks, you can add nonexistent disks to `disk_mapping` in `clickhouse` config section, data will download to %v", t.Database, t.Table, disk, rebalancedDisksStr)
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

func (b *Backuper) downloadTableMetadataIfNotExists(ctx context.Context, backupName string, log *apexLog.Entry, tableTitle metadata.TableTitle) (*metadata.TableMetadata, error) {
	metadataLocalFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	tm := &metadata.TableMetadata{}
	if _, err := tm.Load(metadataLocalFile); err == nil {
		return tm, nil
	}
	// we always download full metadata in this case without filter by partitions
	tm, _, err := b.downloadTableMetadata(ctx, backupName, nil, log.WithFields(apexLog.Fields{"operation": "downloadTableMetadataIfNotExists", "backupName": backupName, "table_metadata_diff": fmt.Sprintf("%s.%s", tableTitle.Database, tableTitle.Table)}), tableTitle, false, nil, false)
	return tm, err
}

func (b *Backuper) downloadTableMetadata(ctx context.Context, backupName string, disks []clickhouse.Disk, log *apexLog.Entry, tableTitle metadata.TableTitle, schemaOnly bool, partitions []string, resume bool) (*metadata.TableMetadata, uint64, error) {
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
				partitionsIdMap, _ = partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, nil, []metadata.TableMetadata{tableMetadata}, partitions)
				filterPartsAndFilesByPartitionsFilter(tableMetadata, partitionsIdMap[metadata.TableTitle{Database: tableMetadata.Database, Table: tableMetadata.Table}])
				tableMetadata.LocalFile = localMetadataFile
			}
			if isProcessed {
				size += uint64(processedSize)
				continue
			}
		}
		var tmBody []byte
		retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
		err := retry.RunCtx(ctx, func(ctx context.Context) error {
			tmReader, err := b.dst.GetFileReader(ctx, remoteMetadataFile)
			if err != nil {
				return err
			}
			tmBody, err = io.ReadAll(tmReader)
			if err != nil {
				return err
			}
			err = tmReader.Close()
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return nil, 0, err
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
				return nil, 0, err
			}
			if b.shouldSkipByTableEngine(tableMetadata) || b.shouldSkipByTableName(fmt.Sprintf("%s.%s", tableMetadata.Database, tableMetadata.Table)) {
				return nil, 0, nil
			}
			partitionsIdMap, _ = partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, nil, []metadata.TableMetadata{tableMetadata}, partitions)
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
	log.
		WithField("duration", utils.HumanizeDuration(time.Since(start))).
		WithField("size", utils.FormatBytes(size)).
		Info("done")
	return &tableMetadata, size, nil
}

// downloadMissedInnerTablesMetadata - download, missed .inner. tables if materialized view query not contains `TO db.table` clause, https://github.com/Altinity/clickhouse-backup/issues/765
// @todo think about parallel download if sequentially will slow
func (b *Backuper) downloadMissedInnerTablesMetadata(ctx context.Context, backupName string, metadataSize uint64, tablesForDownload []metadata.TableTitle, tableMetadataAfterDownload []*metadata.TableMetadata, disks []clickhouse.Disk, schemaOnly bool, partitions []string, log *apexLog.Entry) ([]*metadata.TableMetadata, []metadata.TableTitle, uint64, error) {
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
				metadataLogger := log.WithField("missed_inner_metadata", fmt.Sprintf("%s.%s", innerTableTitle.Database, innerTableTitle.Table))
				innerTableMetadata, size, err := b.downloadTableMetadata(ctx, backupName, disks, metadataLogger, innerTableTitle, schemaOnly, partitions, b.resume)
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

func (b *Backuper) downloadBackupRelatedDir(ctx context.Context, remoteBackup storage.Backup, prefix string) (uint64, error) {
	log := b.log.WithField("logger", "downloadBackupRelatedDir")

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
	if remoteBackup.DataFormat == DirectoryFormat {
		if err := b.dst.DownloadPath(ctx, remoteSource, localDir, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration, b.cfg.General.DownloadMaxBytesPerSecond); err != nil {
			//SFTP can't walk on non exists paths and return error
			if !strings.Contains(err.Error(), "not exist") {
				return 0, err
			}
		}
		downloadedBytes := int64(0)
		if _, err := os.Stat(localDir); err != nil && os.IsNotExist(err) {
			return 0, nil
		}
		if err := filepath.Walk(localDir, func(fPath string, fInfo fs.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fInfo.IsDir() {
				return nil
			}
			downloadedBytes += fInfo.Size()
			return nil
		}); err != nil {
			return 0, err
		}
		if b.resume {
			b.resumableState.AppendToState(remoteSource, downloadedBytes)
		}
		return uint64(downloadedBytes), nil
	}
	remoteFileInfo, err := b.dst.StatFile(ctx, remoteSource)
	if err != nil {
		log.Debugf("%s not exists on remote storage, skip download", remoteSource)
		return 0, nil
	}
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.DownloadCompressedStream(ctx, remoteSource, localDir, b.cfg.General.DownloadMaxBytesPerSecond)
	})
	if err != nil {
		return 0, err
	}
	if b.resume {
		b.resumableState.AppendToState(remoteSource, remoteFileInfo.Size())
	}
	return uint64(remoteFileInfo.Size()), nil
}

func (b *Backuper) downloadTableData(ctx context.Context, remoteBackup metadata.BackupMetadata, table metadata.TableMetadata) error {
	log := b.log.WithField("logger", "downloadTableData")
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dataGroup, dataCtx := errgroup.WithContext(ctx)
	dataGroup.SetLimit(int(b.cfg.General.DownloadConcurrency))

	if remoteBackup.DataFormat != DirectoryFormat {
		capacity := 0
		downloadOffset := make(map[string]int)
		for disk := range table.Files {
			capacity += len(table.Files[disk])
			downloadOffset[disk] = 0
		}
		log.Debugf("start %s.%s with concurrency=%d len(table.Files[...])=%d", table.Database, table.Table, b.cfg.General.DownloadConcurrency, capacity)
		for common.SumMapValuesInt(downloadOffset) < capacity {
			for disk := range table.Files {
				if downloadOffset[disk] >= len(table.Files[disk]) {
					continue
				}
				archiveFile := table.Files[disk][downloadOffset[disk]]
				diskName := disk
				isRebalanced := false
				if diskName, isRebalanced = table.RebalancedFiles[archiveFile]; !isRebalanced {
					diskName = disk
				}
				tableLocalDir := b.getLocalBackupDataPathForTable(remoteBackup.BackupName, diskName, dbAndTableDir)
				downloadOffset[disk] += 1
				tableRemoteFile := path.Join(remoteBackup.BackupName, "shadow", common.TablePathEncode(table.Database), common.TablePathEncode(table.Table), archiveFile)
				dataGroup.Go(func() error {
					log.Debugf("start download %s", tableRemoteFile)
					if b.resume && b.resumableState.IsAlreadyProcessedBool(tableRemoteFile) {
						return nil
					}
					retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
					err := retry.RunCtx(dataCtx, func(dataCtx context.Context) error {
						return b.dst.DownloadCompressedStream(dataCtx, tableRemoteFile, tableLocalDir, b.cfg.General.DownloadMaxBytesPerSecond)
					})
					if err != nil {
						return err
					}
					if b.resume {
						b.resumableState.AppendToState(tableRemoteFile, 0)
					}
					log.Debugf("finish download %s", tableRemoteFile)
					return nil
				})
			}
		}
	} else {
		capacity := 0
		for disk := range table.Parts {
			capacity += len(table.Parts[disk])
		}
		log.Debugf("start %s.%s with concurrency=%d len(table.Parts[...])=%d", table.Database, table.Table, b.cfg.General.DownloadConcurrency, capacity)

		for disk, parts := range table.Parts {
			tableRemotePath := path.Join(remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
			diskPath, diskExists := b.DiskToPathMap[disk]
			tableLocalPath := path.Join(diskPath, "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk)
			if b.isEmbedded {
				tableLocalPath = path.Join(diskPath, remoteBackup.BackupName, "data", dbAndTableDir)
			}
			for _, part := range parts {
				if part.Required {
					continue
				}
				if !diskExists {
					diskPath, diskExists = b.DiskToPathMap[part.RebalancedDisk]
					if !diskExists {
						return fmt.Errorf("downloadTableData: table: `%s`.`%s`, disk: %s, part.Name: %s, part.RebalancedDisk: %s not rebalanced", table.Table, table.Database, disk, part.Name, part.RebalancedDisk)
					}
				}
				partRemotePath := path.Join(tableRemotePath, part.Name)
				partLocalPath := path.Join(tableLocalPath, part.Name)
				dataGroup.Go(func() error {
					log.Debugf("start %s -> %s", partRemotePath, partLocalPath)
					if b.resume && b.resumableState.IsAlreadyProcessedBool(partRemotePath) {
						return nil
					}
					if err := b.dst.DownloadPath(dataCtx, partRemotePath, partLocalPath, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration, b.cfg.General.DownloadMaxBytesPerSecond); err != nil {
						return err
					}
					if b.resume {
						b.resumableState.AppendToState(partRemotePath, 0)
					}
					log.Debugf("finish %s -> %s", partRemotePath, partLocalPath)
					return nil
				})
			}
		}
	}
	if err := dataGroup.Wait(); err != nil {
		return fmt.Errorf("one of downloadTableData go-routine return error: %v", err)
	}

	if !b.isEmbedded && remoteBackup.RequiredBackup != "" {
		err := b.downloadDiffParts(ctx, remoteBackup, table, dbAndTableDir)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Backuper) downloadDiffParts(ctx context.Context, remoteBackup metadata.BackupMetadata, table metadata.TableMetadata, dbAndTableDir string) error {
	log := b.log.WithField("operation", "downloadDiffParts")
	log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).Debug("start")
	start := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	downloadedDiffParts := uint32(0)
	downloadDiffGroup, downloadDiffCtx := errgroup.WithContext(ctx)
	downloadDiffGroup.SetLimit(int(b.cfg.General.DownloadConcurrency))
	diffRemoteFilesCache := map[string]*sync.Mutex{}
	diffRemoteFilesLock := &sync.Mutex{}

	for disk, parts := range table.Parts {
		diskPath, diskExists := b.DiskToPathMap[disk]
		for _, part := range parts {
			if !diskExists && part.RebalancedDisk == "" {
				return fmt.Errorf("downloadDiffParts: table: `%s`.`%s`, disk: %s, part.Name: %s, part.RebalancedDisk: `%s` not rebalanced", table.Table, table.Database, disk, part.Name, part.RebalancedDisk)
			}
			if !diskExists {
				diskPath, diskExists = b.DiskToPathMap[part.RebalancedDisk]
				if !diskExists {
					return fmt.Errorf("downloadDiffParts: table: `%s`.`%s`, disk: %s, part.Name: %s, part.RebalancedDisk: `%s` not rebalanced", table.Table, table.Database, disk, part.Name, part.RebalancedDisk)
				}
				disk = part.RebalancedDisk
			}
			newPath := path.Join(diskPath, "backup", remoteBackup.BackupName, "shadow", dbAndTableDir, disk, part.Name)
			if err := b.checkNewPath(newPath, part); err != nil {
				return err
			}
			if !part.Required {
				continue
			}
			existsPath := path.Join(b.DiskToPathMap[disk], "backup", remoteBackup.RequiredBackup, "shadow", dbAndTableDir, disk, part.Name)
			_, err := os.Stat(existsPath)
			if err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("%s stat return error: %v", existsPath, err)
			}
			if err != nil && os.IsNotExist(err) {
				//if existPath already processed then expect non empty newPath
				if b.resume && b.resumableState.IsAlreadyProcessedBool(existsPath) {
					if newPathDirList, newPathDirErr := os.ReadDir(newPath); newPathDirErr != nil {
						newPathDirErr = fmt.Errorf("os.ReadDir(%s) error: %v", newPath, newPathDirErr)
						log.Error(newPathDirErr.Error())
						return newPathDirErr
					} else if len(newPathDirList) == 0 {
						return fmt.Errorf("os.ReadDir(%s) expect return non empty list", newPath)
					}
					continue
				}
				partForDownload := part
				diskForDownload := disk
				if !diskExists {
					diskForDownload = part.RebalancedDisk
				}
				downloadDiffGroup.Go(func() error {
					tableRemoteFiles, err := b.findDiffBackupFilesRemote(downloadDiffCtx, remoteBackup, table, diskForDownload, partForDownload, log)
					if err != nil {
						return err
					}

					for tableRemoteFile, tableLocalDir := range tableRemoteFiles {
						err = b.downloadDiffRemoteFile(downloadDiffCtx, diffRemoteFilesLock, diffRemoteFilesCache, tableRemoteFile, tableLocalDir)
						if err != nil {
							return err
						}
						downloadedPartPath := path.Join(tableLocalDir, partForDownload.Name)
						if downloadedPartPath != existsPath {
							info, err := os.Stat(downloadedPartPath)
							if err == nil {
								if !info.IsDir() {
									return fmt.Errorf("after downloadDiffRemoteFile %s exists but is not directory", downloadedPartPath)
								}
								if err = b.makePartHardlinks(downloadedPartPath, existsPath); err != nil {
									return fmt.Errorf("can't to add link to rebalanced part %s -> %s error: %v", downloadedPartPath, existsPath, err)
								}
							}
							if err != nil && !os.IsNotExist(err) {
								return fmt.Errorf("after downloadDiffRemoteFile os.Stat(%s) return error: %v", downloadedPartPath, err)
							}
						}
						atomic.AddUint32(&downloadedDiffParts, 1)
					}
					if err = b.makePartHardlinks(existsPath, newPath); err != nil {
						return fmt.Errorf("can't to add link to exists part %s -> %s error: %v", newPath, existsPath, err)
					}
					if b.resume {
						b.resumableState.AppendToState(existsPath, 0)
					}
					return nil
				})
			} else {
				if !b.resume || (b.resume && !b.resumableState.IsAlreadyProcessedBool(existsPath)) {
					if err = b.makePartHardlinks(existsPath, newPath); err != nil {
						return fmt.Errorf("can't to add exists part: %v", err)
					}
				}
			}
		}
	}
	if err := downloadDiffGroup.Wait(); err != nil {
		return fmt.Errorf("one of downloadDiffParts go-routine return error: %v", err)
	}
	log.WithField("duration", utils.HumanizeDuration(time.Since(start))).WithField("diff_parts", strconv.Itoa(int(downloadedDiffParts))).Info("done")
	return nil
}

func (b *Backuper) downloadDiffRemoteFile(ctx context.Context, diffRemoteFilesLock *sync.Mutex, diffRemoteFilesCache map[string]*sync.Mutex, tableRemoteFile string, tableLocalDir string) error {
	log := b.log.WithField("logger", "downloadDiffRemoteFile")
	if b.resume && b.resumableState.IsAlreadyProcessedBool(tableRemoteFile) {
		return nil
	}
	diffRemoteFilesLock.Lock()
	namedLock, isCached := diffRemoteFilesCache[tableRemoteFile]
	if isCached {
		log.Debugf("wait download begin %s", tableRemoteFile)
		namedLock.Lock()
		diffRemoteFilesLock.Unlock()
		namedLock.Unlock()
		log.Debugf("wait download end %s", tableRemoteFile)
	} else {
		log.Debugf("start download from %s", tableRemoteFile)
		namedLock = &sync.Mutex{}
		diffRemoteFilesCache[tableRemoteFile] = namedLock
		namedLock.Lock()
		diffRemoteFilesLock.Unlock()
		if path.Ext(tableRemoteFile) != "" {
			retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
			err := retry.RunCtx(ctx, func(ctx context.Context) error {
				return b.dst.DownloadCompressedStream(ctx, tableRemoteFile, tableLocalDir, b.cfg.General.DownloadMaxBytesPerSecond)
			})
			if err != nil {
				log.Warnf("DownloadCompressedStream %s -> %s return error: %v", tableRemoteFile, tableLocalDir, err)
				return err
			}
		} else {
			// remoteFile could be a directory
			if err := b.dst.DownloadPath(ctx, tableRemoteFile, tableLocalDir, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration, b.cfg.General.DownloadMaxBytesPerSecond); err != nil {
				log.Warnf("DownloadPath %s -> %s return error: %v", tableRemoteFile, tableLocalDir, err)
				return err
			}
		}
		namedLock.Unlock()
		if b.resume {
			b.resumableState.AppendToState(tableRemoteFile, 0)
		}
		log.Debugf("finish download from %s", tableRemoteFile)
	}
	return nil
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

func (b *Backuper) findDiffBackupFilesRemote(ctx context.Context, backup metadata.BackupMetadata, table metadata.TableMetadata, disk string, part metadata.Part, log *apexLog.Entry) (map[string]string, error) {
	var requiredTable *metadata.TableMetadata
	log.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffBackupFilesRemote"}).Debugf("start")
	requiredBackup, err := b.ReadBackupMetadataRemote(ctx, backup.RequiredBackup)
	if err != nil {
		return nil, err
	}
	requiredTable, err = b.downloadTableMetadataIfNotExists(ctx, requiredBackup.BackupName, log, metadata.TableTitle{Database: table.Database, Table: table.Table})
	if err != nil {
		log.Warnf("downloadTableMetadataIfNotExists %s / %s.%s return error", requiredBackup.BackupName, table.Database, table.Table)
		return nil, err
	}

	// recursive find if part in RequiredBackup also Required
	tableRemoteFiles, found, err := b.findDiffRecursive(ctx, requiredBackup, table, requiredTable, part, disk, log)
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

func (b *Backuper) findDiffRecursive(ctx context.Context, requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, requiredTable *metadata.TableMetadata, part metadata.Part, disk string, log *apexLog.Entry) (map[string]string, bool, error) {
	log.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffRecursive"}).Debugf("start")
	found := false
	for _, requiredParts := range requiredTable.Parts {
		for _, requiredPart := range requiredParts {
			if requiredPart.Name == part.Name {
				found = true
				if requiredPart.Required {
					tableRemoteFiles, err := b.findDiffBackupFilesRemote(ctx, *requiredBackup, table, disk, part, log)
					if err != nil {
						found = false
						log.Warnf("try find %s.%s %s recursive return err: %v", table.Database, table.Table, part.Name, err)
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
	log := apexLog.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffOnePart"})
	log.Debugf("start")
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
	log := apexLog.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffOnePartDirectory"})
	log.Debugf("start")
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	tableRemotePath := path.Join(requiredBackup.BackupName, "shadow", dbAndTableDir, remoteDisk, part.Name)
	tableRemoteFile := path.Join(tableRemotePath, "checksums.txt")
	return b.findDiffFileExist(ctx, requiredBackup, tableRemoteFile, tableRemotePath, localDisk, dbAndTableDir, part)
}

func (b *Backuper) findDiffOnePartArchive(ctx context.Context, requiredBackup *metadata.BackupMetadata, table metadata.TableMetadata, localDisk, remoteDisk string, part metadata.Part) (string, string, error) {
	log := apexLog.WithFields(apexLog.Fields{"database": table.Database, "table": table.Table, "part": part.Name, "logger": "findDiffOnePartArchive"})
	log.Debugf("start")
	dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	remoteExt := config.ArchiveExtensions[requiredBackup.DataFormat]
	tableRemotePath := path.Join(requiredBackup.BackupName, "shadow", dbAndTableDir, fmt.Sprintf("%s_%s.%s", remoteDisk, common.TablePathEncode(part.Name), remoteExt))
	tableRemoteFile := tableRemotePath
	return b.findDiffFileExist(ctx, requiredBackup, tableRemoteFile, tableRemotePath, localDisk, dbAndTableDir, part)
}

func (b *Backuper) findDiffFileExist(ctx context.Context, requiredBackup *metadata.BackupMetadata, tableRemoteFile string, tableRemotePath string, localDisk string, dbAndTableDir string, part metadata.Part) (string, string, error) {
	_, err := b.dst.StatFile(ctx, tableRemoteFile)
	log := b.log.WithField("logger", "findDiffFileExist")
	if err != nil {
		log.WithFields(apexLog.Fields{"tableRemoteFile": tableRemoteFile, "tableRemotePath": tableRemotePath, "part": part.Name}).Debugf("findDiffFileExist not found")
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
	log.WithFields(apexLog.Fields{"tableRemoteFile": tableRemoteFile, "tableRemotePath": tableRemotePath, "part": part.Name}).Debugf("findDiffFileExist found")
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
	log := apexLog.WithField("logger", "makePartHardlinks")
	_, err := os.Stat(exists)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(new, 0750); err != nil {
		log.Warnf("MkDirAll(%s) error: %v", new, err)
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
				log.Warnf("MkdirAll(%s) error: %v", fPath, err)
				return err
			}
			return nil
		}

		if err = os.Link(existsF, newF); err != nil {
			existsFInfo, existsStatErr := os.Stat(existsF)
			newFInfo, newStatErr := os.Stat(newF)
			if existsStatErr != nil || newStatErr != nil || !os.SameFile(existsFInfo, newFInfo) {
				log.Warnf("Link %s -> %s error: %v, existsStatErr: %v newStatErr: %v", existsF, newF, err, existsStatErr, newStatErr)
				return err
			}
		}
		return nil
	}); walkErr != nil {
		log.Warnf("Link recursively %s -> %s return error: %v", new, exists, walkErr)
		return walkErr
	}
	return nil
}

func (b *Backuper) downloadSingleBackupFile(ctx context.Context, remoteFile string, localFile string, disks []clickhouse.Disk) error {
	if b.resume && b.resumableState.IsAlreadyProcessedBool(remoteFile) {
		return nil
	}
	log := b.log.WithField("logger", "downloadSingleBackupFile")
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err := retry.RunCtx(ctx, func(ctx context.Context) error {
		remoteReader, err := b.dst.GetFileReader(ctx, remoteFile)
		if err != nil {
			return err
		}
		defer func() {
			err = remoteReader.Close()
			if err != nil {
				log.Warnf("can't close remoteReader %s", remoteFile)
			}
		}()
		localWriter, err := os.OpenFile(localFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
		if err != nil {
			return err
		}

		defer func() {
			err = localWriter.Close()
			if err != nil {
				log.Warnf("can't close localWriter %s", localFile)
			}
		}()

		_, err = io.CopyBuffer(localWriter, remoteReader, nil)
		if err != nil {
			return err
		}

		if err = filesystemhelper.Chown(localFile, b.ch, disks, false); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if b.resume {
		b.resumableState.AppendToState(remoteFile, 0)
	}
	return nil
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
