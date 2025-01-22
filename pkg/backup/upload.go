package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/custom"
	"github.com/Altinity/clickhouse-backup/v2/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/eapache/go-resiliency/retrier"

	"golang.org/x/sync/errgroup"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/rs/zerolog/log"
	"github.com/yargevad/filepathx"
)

func (b *Backuper) Upload(backupName string, deleteSource bool, diffFrom, diffFromRemote, tablePattern string, partitions []string, schemaOnly, rbacOnly, configsOnly, resume bool, backupVersion string, commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	startUpload := time.Now()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	var disks []clickhouse.Disk
	b.adjustResumeFlag(resume)
	if err = b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()
	if err = b.validateUploadParams(ctx, backupName, diffFrom, diffFromRemote); err != nil {
		return err
	}
	if b.cfg.General.RemoteStorage == "custom" {
		return custom.Upload(ctx, b.cfg, backupName, diffFrom, diffFromRemote, tablePattern, partitions, schemaOnly)
	}
	if _, disks, err = b.getLocalBackup(ctx, backupName, nil); err != nil {
		return fmt.Errorf("can't find local backup: %v", err)
	}
	if err := b.initDisksPathsAndBackupDestination(ctx, disks, backupName); err != nil {
		return err
	}
	defer func() {
		if err := b.dst.Close(ctx); err != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", err)
		}
	}()

	remoteBackups, err := b.dst.BackupList(ctx, false, "")
	if err != nil {
		return fmt.Errorf("b.dst.BackupList return error: %v", err)
	}
	for i := range remoteBackups {
		if backupName == remoteBackups[i].BackupName {
			if !b.resume {
				return fmt.Errorf("'%s' already exists on remote storage", backupName)
			} else {
				log.Warn().Msgf("'%s' already exists on remote, will try to resume upload", backupName)
			}
		}
	}
	backupMetadata, err := b.ReadBackupMetadataLocal(ctx, backupName)
	if err != nil {
		return fmt.Errorf("b.ReadBackupMetadataLocal return error: %v", err)
	}
	var tablesForUpload ListOfTables
	b.isEmbedded = strings.Contains(backupMetadata.Tags, "embedded")
	// will ignore partitions cause can't manipulate .backup
	if b.isEmbedded {
		partitions = make([]string, 0)
	}

	if len(backupMetadata.Tables) != 0 {
		tablesForUpload, err = b.prepareTableListToUpload(ctx, backupName, tablePattern, partitions)
		if err != nil {
			return fmt.Errorf("b.prepareTableListToUpload return error: %v", err)
		}
	}
	tablesForUploadFromDiff := map[metadata.TableTitle]metadata.TableMetadata{}

	if diffFrom != "" && !b.isEmbedded {
		tablesForUploadFromDiff, err = b.getTablesDiffFromLocal(ctx, diffFrom, tablePattern)
		if err != nil {
			return fmt.Errorf("b.getTablesDiffFromLocal return error: %v", err)
		}
		backupMetadata.RequiredBackup = diffFrom
	}
	if diffFromRemote != "" && !b.isEmbedded {
		tablesForUploadFromDiff, err = b.getTablesDiffFromRemote(ctx, diffFromRemote, tablePattern)
		if err != nil {
			return fmt.Errorf("b.getTablesDiffFromRemote return error: %v", err)
		}
		backupMetadata.RequiredBackup = diffFromRemote
	}
	if b.resume {
		b.resumableState = resumable.NewState(b.GetStateDir(), backupName, "upload", map[string]interface{}{
			"diffFrom":       diffFrom,
			"diffFromRemote": diffFromRemote,
			"tablePattern":   tablePattern,
			"partitions":     partitions,
			"schemaOnly":     schemaOnly,
		})
	}

	compressedDataSize := int64(0)
	metadataSize := int64(0)

	log.Debug().Msgf("prepare table concurrent semaphore with concurrency=%d len(tablesForUpload)=%d", b.cfg.General.UploadConcurrency, len(tablesForUpload))
	uploadGroup, uploadCtx := errgroup.WithContext(ctx)
	uploadGroup.SetLimit(int(b.cfg.General.UploadConcurrency))

	doUploadData := !schemaOnly && !rbacOnly && !configsOnly

	for i, table := range tablesForUpload {
		start := time.Now()
		if doUploadData {
			if diffTable, diffExists := tablesForUploadFromDiff[metadata.TableTitle{
				Database: table.Database,
				Table:    table.Table,
			}]; diffExists {
				checkLocalPart := diffFrom != "" && diffFromRemote == ""
				b.markDuplicatedParts(backupMetadata, &diffTable, &table, checkLocalPart)
			}
		}
		idx := i
		uploadGroup.Go(func() error {
			var uploadedBytes int64
			var uploadTableErr error
			//skip upload data for embedded backup with empty embedded_backup_disk
			if doUploadData && (!b.isEmbedded || b.cfg.ClickHouse.EmbeddedBackupDisk != "") {
				var files map[string][]string
				files, uploadedBytes, uploadTableErr = b.uploadTableData(uploadCtx, backupName, deleteSource, tablesForUpload[idx])
				if uploadTableErr != nil {
					return uploadTableErr
				}
				atomic.AddInt64(&compressedDataSize, uploadedBytes)
				tablesForUpload[idx].Files = files
			}
			tableMetadataSize := int64(0)
			if doUploadData || schemaOnly {
				tableMetadataSize, uploadTableErr = b.uploadTableMetadata(uploadCtx, backupName, backupMetadata.RequiredBackup, tablesForUpload[idx])
				if uploadTableErr != nil {
					return uploadTableErr
				}
				atomic.AddInt64(&metadataSize, tableMetadataSize)
			}
			log.Info().Fields(map[string]interface{}{
				"operation":     "upload_table",
				"table":         fmt.Sprintf("%s.%s", tablesForUpload[idx].Database, tablesForUpload[idx].Table),
				"progress":      fmt.Sprintf("%d/%d", idx+1, len(tablesForUpload)),
				"duration":      utils.HumanizeDuration(time.Since(start)),
				"data_size":     utils.FormatBytes(uint64(uploadedBytes)),
				"metadata_size": utils.FormatBytes(uint64(tableMetadataSize)),
				"version":       backupVersion,
			}).Msg("done")
			return nil
		})
	}
	if err := uploadGroup.Wait(); err != nil {
		return fmt.Errorf("one of upload table go-routine return error: %v", err)
	}

	// upload rbac for backup, if not configsOnly
	if rbacOnly || configsOnly == rbacOnly {
		if backupMetadata.RBACSize, err = b.uploadRBACData(ctx, backupName); err != nil {
			return fmt.Errorf("b.uploadRBACData return error: %v", err)
		}
	}
	// upload configs for backup, if not rbacOnly
	if configsOnly || configsOnly == rbacOnly {
		if backupMetadata.ConfigSize, err = b.uploadConfigData(ctx, backupName); err != nil {
			return fmt.Errorf("b.uploadConfigData return error: %v", err)
		}
	}
	//upload embedded .backup file
	if doUploadData && b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" && backupMetadata.Tables != nil && len(backupMetadata.Tables) > 0 {
		localClickHouseBackupFile := path.Join(b.EmbeddedBackupDataPath, backupName, ".backup")
		remoteClickHouseBackupFile := path.Join(backupName, ".backup")
		localEmbeddedMetadataSize := int64(0)
		if localEmbeddedMetadataSize, err = b.uploadSingleBackupFile(ctx, localClickHouseBackupFile, remoteClickHouseBackupFile); err != nil {
			return fmt.Errorf("b.uploadSingleBackupFile return error: %v", err)
		}
		metadataSize += localEmbeddedMetadataSize
	}

	// upload metadata for backup
	backupMetadata.CompressedSize = uint64(compressedDataSize)
	backupMetadata.MetadataSize = uint64(metadataSize)
	tt := make([]metadata.TableTitle, len(tablesForUpload))
	for i := range tablesForUpload {
		tt[i] = metadata.TableTitle{
			Database: tablesForUpload[i].Database,
			Table:    tablesForUpload[i].Table,
		}
	}
	backupMetadata.Tables = tt
	if b.cfg.GetCompressionFormat() != "none" {
		backupMetadata.DataFormat = b.cfg.GetCompressionFormat()
	} else {
		backupMetadata.DataFormat = DirectoryFormat
	}
	backupMetadata.ClickhouseBackupVersion = backupVersion
	newBackupMetadataBody, err := json.MarshalIndent(backupMetadata, "", "\t")
	if err != nil {
		return err
	}
	remoteBackupMetaFile := path.Join(backupName, "metadata.json")
	if !b.resume || (b.resume && !b.resumableState.IsAlreadyProcessedBool(remoteBackupMetaFile)) {
		retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
		err = retry.RunCtx(ctx, func(ctx context.Context) error {
			return b.dst.PutFile(ctx, remoteBackupMetaFile, io.NopCloser(bytes.NewReader(newBackupMetadataBody)), 0)
		})
		if err != nil {
			return fmt.Errorf("can't upload %s: %v", remoteBackupMetaFile, err)
		}
	}
	if b.resume {
		b.resumableState.Close()
	}
	log.Info().Fields(map[string]interface{}{
		"backup":           backupName,
		"operation":        "upload",
		"duration":         utils.HumanizeDuration(time.Since(startUpload)),
		"upload_size":      utils.FormatBytes(uint64(compressedDataSize) + uint64(metadataSize) + uint64(len(newBackupMetadataBody)) + backupMetadata.RBACSize + backupMetadata.ConfigSize),
		"object_disk_size": utils.FormatBytes(backupMetadata.ObjectDiskSize),
		"version":          backupVersion,
	}).Msg("done")

	// Remote old backup retention
	if err = b.RemoveOldBackupsRemote(ctx); err != nil {
		return fmt.Errorf("can't remove old backups on remote storage: %v", err)
	}
	// Local old backup retention, fix https://github.com/Altinity/clickhouse-backup/issues/834
	if err = b.RemoveOldBackupsLocal(ctx, false, nil); err != nil {
		return fmt.Errorf("can't remove old local backups: %v", err)
	}

	// explicitly delete local backup after successful upload, fix https://github.com/Altinity/clickhouse-backup/issues/777
	if b.cfg.General.BackupsToKeepLocal >= 0 && deleteSource {
		if err = b.RemoveBackupLocal(ctx, backupName, disks); err != nil {
			return fmt.Errorf("can't explicitly delete local source backup: %v", err)
		}
	}
	return nil
}

func (b *Backuper) RemoveOldBackupsRemote(ctx context.Context) error {

	if b.cfg.General.BackupsToKeepRemote < 1 {
		return nil
	}
	start := time.Now()
	backupList, err := b.dst.BackupList(ctx, true, "")
	if err != nil {
		return err
	}
	backupsToDelete := storage.GetBackupsToDeleteRemote(backupList, b.cfg.General.BackupsToKeepRemote)
	log.Info().Fields(map[string]interface{}{
		"operation": "RemoveOldBackupsRemote",
		"duration":  utils.HumanizeDuration(time.Since(start)),
	}).Msg("calculate backup list for delete remote")
	for _, backupToDelete := range backupsToDelete {
		startDelete := time.Now()
		err = b.cleanEmbeddedAndObjectDiskRemoteIfSameLocalNotPresent(ctx, backupToDelete)
		if err != nil {
			return err
		}

		if err := b.dst.RemoveBackupRemote(ctx, backupToDelete, b.cfg); err != nil {
			log.Warn().Msgf("can't deleteKey %s return error : %v", backupToDelete.BackupName, err)
		}
		log.Info().Fields(map[string]interface{}{
			"operation": "RemoveOldBackupsRemote",
			"location":  "remote",
			"backup":    backupToDelete.BackupName,
			"duration":  utils.HumanizeDuration(time.Since(startDelete)),
		}).Msg("done")
	}
	log.Info().Fields(map[string]interface{}{"operation": "RemoveOldBackupsRemote", "duration": utils.HumanizeDuration(time.Since(start))}).Msg("done")
	return nil
}

func (b *Backuper) uploadSingleBackupFile(ctx context.Context, localFile, remoteFile string) (int64, error) {
	if b.resume {
		if isProcessed, size := b.resumableState.IsAlreadyProcessed(remoteFile); isProcessed {
			return size, nil
		}
	}
	f, err := os.Open(localFile)
	if err != nil {
		return 0, fmt.Errorf("can't open %s: %v", localFile, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warn().Msgf("can't close %v: %v", f, err)
		}
	}()
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.PutFile(ctx, remoteFile, f, 0)
	})
	if err != nil {
		return 0, fmt.Errorf("can't upload %s: %v", remoteFile, err)
	}
	info, err := os.Stat(localFile)
	if err != nil {
		return 0, fmt.Errorf("can't stat %s", localFile)
	}
	if b.resume {
		b.resumableState.AppendToState(remoteFile, info.Size())
	}
	return info.Size(), nil
}

func (b *Backuper) prepareTableListToUpload(ctx context.Context, backupName string, tablePattern string, partitions []string) (tablesForUpload ListOfTables, err error) {
	metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
	if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		metadataPath = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata")
	}
	tablesForUpload, _, err = b.getTableListByPatternLocal(ctx, metadataPath, tablePattern, false, partitions)
	if err != nil {
		return nil, err
	}
	return tablesForUpload, nil
}

func (b *Backuper) validateUploadParams(ctx context.Context, backupName string, diffFrom string, diffFromRemote string) error {
	if b.cfg.General.RemoteStorage == "none" {
		return fmt.Errorf("general->remote_storage shall not be \"none\" for upload, change you config or use REMOTE_STORAGE environment variable")
	}
	if backupName == "" {
		_ = b.PrintLocalBackups(ctx, "all")
		return fmt.Errorf("select backup for upload")
	}
	if b.cfg.General.UploadConcurrency == 0 {
		return fmt.Errorf("`upload_concurrency` shall be more than zero")
	}
	if backupName == diffFrom || backupName == diffFromRemote {
		return fmt.Errorf("you cannot upload diff from the same backup")
	}
	if diffFromRemote != "" && b.cfg.General.UploadByPart == false {
		return fmt.Errorf("`--diff-from-remote` require `upload_by_part` equal true in `general` config section")
	}
	if diffFrom != "" && diffFromRemote != "" {
		return fmt.Errorf("choose setup only `--diff-from-remote` or `--diff-from`, not both")
	}
	if b.cfg.GetCompressionFormat() == "none" && !b.cfg.General.UploadByPart {
		return fmt.Errorf("%s->`compression_format`=%s incompatible with general->upload_by_part=%v", b.cfg.General.RemoteStorage, b.cfg.GetCompressionFormat(), b.cfg.General.UploadByPart)
	}

	if b.cfg.General.RemoteStorage == "custom" && b.resume {
		return fmt.Errorf("Resumable state not allowed for `remote_storage: custom`. Disable it by setting use_resumable_state=false in `general` config section")
	}
	if b.cfg.General.RemoteStorage == "s3" && len(b.cfg.S3.CustomStorageClassMap) > 0 {
		for pattern, storageClass := range b.cfg.S3.CustomStorageClassMap {
			re := regexp.MustCompile(pattern)
			if re.MatchString(backupName) {
				b.cfg.S3.StorageClass = storageClass
			}
		}
	}
	if b.cfg.General.RemoteStorage == "gcs" && len(b.cfg.GCS.CustomStorageClassMap) > 0 {
		for pattern, storageClass := range b.cfg.GCS.CustomStorageClassMap {
			re := regexp.MustCompile(pattern)
			if re.MatchString(backupName) {
				b.cfg.GCS.StorageClass = storageClass
			}
		}
	}
	return nil
}

func (b *Backuper) uploadConfigData(ctx context.Context, backupName string) (uint64, error) {
	backupPath := b.DefaultDataPath
	configBackupPath := path.Join(backupPath, "backup", backupName, "configs")
	if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		backupPath = b.EmbeddedBackupDataPath
		configBackupPath = path.Join(backupPath, backupName, "configs")
	}
	configFilesGlobPattern := path.Join(configBackupPath, "**/*.*")
	if b.cfg.GetCompressionFormat() == "none" {
		remoteConfigsDir := path.Join(backupName, "configs")
		return b.uploadBackupRelatedDir(ctx, configBackupPath, configFilesGlobPattern, remoteConfigsDir)
	}
	remoteConfigsArchive := path.Join(backupName, fmt.Sprintf("configs.%s", b.cfg.GetArchiveExtension()))
	return b.uploadBackupRelatedDir(ctx, configBackupPath, configFilesGlobPattern, remoteConfigsArchive)
}

func (b *Backuper) uploadRBACData(ctx context.Context, backupName string) (uint64, error) {
	backupPath := b.DefaultDataPath
	rbacBackupPath := path.Join(backupPath, "backup", backupName, "access")
	if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		backupPath = b.EmbeddedBackupDataPath
		rbacBackupPath = path.Join(backupPath, backupName, "access")
	}
	accessFilesGlobPattern := path.Join(rbacBackupPath, "*.*")
	if b.cfg.GetCompressionFormat() == "none" {
		remoteRBACDir := path.Join(backupName, "access")
		return b.uploadBackupRelatedDir(ctx, rbacBackupPath, accessFilesGlobPattern, remoteRBACDir)
	}
	remoteRBACArchive := path.Join(backupName, fmt.Sprintf("access.%s", b.cfg.GetArchiveExtension()))
	return b.uploadBackupRelatedDir(ctx, rbacBackupPath, accessFilesGlobPattern, remoteRBACArchive)
}

func (b *Backuper) uploadBackupRelatedDir(ctx context.Context, localBackupRelatedDir, localFilesGlobPattern, destinationRemote string) (uint64, error) {
	if _, err := os.Stat(localBackupRelatedDir); os.IsNotExist(err) {
		return 0, nil
	}
	if b.resume {
		if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(destinationRemote); isProcessed {
			return uint64(processedSize), nil
		}
	}
	var localFiles []string
	var err error
	if localFiles, err = filepathx.Glob(localFilesGlobPattern); err != nil || localFiles == nil || len(localFiles) == 0 {
		if !b.cfg.General.RBACBackupAlways {
			return 0, fmt.Errorf("list %s return list=%v with err=%v", localFilesGlobPattern, localFiles, err)
		}
		log.Warn().Msgf("list %s return list=%v with err=%v", localFilesGlobPattern, localFiles, err)
		return 0, nil
	}

	for i := 0; i < len(localFiles); i++ {
		if fileInfo, err := os.Stat(localFiles[i]); err == nil && fileInfo.IsDir() {
			localFiles = append(localFiles[:i], localFiles[i+1:]...)
			i--
		} else {
			localFiles[i] = strings.Replace(localFiles[i], localBackupRelatedDir, "", 1)
		}
	}
	if b.cfg.GetCompressionFormat() == "none" {
		remoteUploadedBytes := int64(0)
		if remoteUploadedBytes, err = b.dst.UploadPath(ctx, localBackupRelatedDir, localFiles, destinationRemote, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration, b.cfg.General.UploadMaxBytesPerSecond); err != nil {
			return 0, fmt.Errorf("can't RBAC or config upload %s: %v", destinationRemote, err)
		}
		if b.resume {
			b.resumableState.AppendToState(destinationRemote, remoteUploadedBytes)
		}
		return uint64(remoteUploadedBytes), nil
	}
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.UploadCompressedStream(ctx, localBackupRelatedDir, localFiles, destinationRemote, b.cfg.General.UploadMaxBytesPerSecond)
	})
	if err != nil {
		return 0, fmt.Errorf("can't RBAC or config upload compressed %s: %v", destinationRemote, err)
	}

	var remoteUploaded storage.RemoteFile
	retry = retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		remoteUploaded, err = b.dst.StatFile(ctx, destinationRemote)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("can't check uploaded destinationRemote: %s, error: %v", destinationRemote, err)
	}
	if b.resume {
		b.resumableState.AppendToState(destinationRemote, remoteUploaded.Size())
	}
	return uint64(remoteUploaded.Size()), nil
}

func (b *Backuper) uploadTableData(ctx context.Context, backupName string, deleteSource bool, table metadata.TableMetadata) (map[string][]string, int64, error) {
	dbAndTablePath := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	uploadedFiles := map[string][]string{}
	capacity := 0
	for disk := range table.Parts {
		capacity += len(table.Parts[disk])
	}
	log.Debug().Msgf("start %s.%s with concurrency=%d len(table.Parts[...])=%d", table.Database, table.Table, b.cfg.General.UploadConcurrency, capacity)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dataGroup, ctx := errgroup.WithContext(ctx)
	dataGroup.SetLimit(int(b.cfg.General.UploadConcurrency))
	var uploadedBytes int64

	splitParts := make(map[string][]metadata.SplitPartFiles)
	splitPartsOffset := make(map[string]int)
	splitPartsCapacity := 0
	for disk := range table.Parts {
		backupPath := b.getLocalBackupDataPathForTable(backupName, disk, dbAndTablePath)
		splitPartsList, err := b.splitPartFiles(backupPath, table.Parts[disk])
		if err != nil {
			return nil, 0, err
		}
		splitParts[disk] = splitPartsList
		splitPartsOffset[disk] = 0
		splitPartsCapacity += len(splitPartsList)
	}
	for common.SumMapValuesInt(splitPartsOffset) < splitPartsCapacity {
		for disk := range table.Parts {
			if splitPartsOffset[disk] >= len(splitParts[disk]) {
				continue
			}
			backupPath := b.getLocalBackupDataPathForTable(backupName, disk, dbAndTablePath)
			splitPart := splitParts[disk][splitPartsOffset[disk]]
			partSuffix := splitPart.Prefix
			partFiles := splitPart.Files
			splitPartsOffset[disk] += 1
			baseRemoteDataPath := path.Join(backupName, "shadow", common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
			if b.cfg.GetCompressionFormat() == "none" {
				remotePath := path.Join(baseRemoteDataPath, disk)
				remotePathFull := path.Join(remotePath, partSuffix)
				dataGroup.Go(func() error {
					if b.resume {
						if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(remotePathFull); isProcessed {
							atomic.AddInt64(&uploadedBytes, processedSize)
							return nil
						}
					}
					log.Debug().Msgf("start upload %d files to %s", len(partFiles), remotePath)
					if uploadPathBytes, err := b.dst.UploadPath(ctx, backupPath, partFiles, remotePath, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration, b.cfg.General.UploadMaxBytesPerSecond); err != nil {
						log.Error().Msgf("UploadPath return error: %v", err)
						return fmt.Errorf("can't upload: %v", err)
					} else {
						atomic.AddInt64(&uploadedBytes, uploadPathBytes)
						if b.resume {
							b.resumableState.AppendToState(remotePathFull, uploadPathBytes)
						}
					}
					// https://github.com/Altinity/clickhouse-backup/issues/777
					if deleteSource {
						for _, f := range partFiles {
							if err := os.Remove(path.Join(backupPath, f)); err != nil {
								return fmt.Errorf("can't remove %s, %v", path.Join(backupPath, f), err)
							}
						}
					}
					return nil
				})
			} else {
				fileName := fmt.Sprintf("%s_%s.%s", disk, common.TablePathEncode(partSuffix), b.cfg.GetArchiveExtension())
				uploadedFiles[disk] = append(uploadedFiles[disk], fileName)
				remoteDataFile := path.Join(baseRemoteDataPath, fileName)
				localFiles := partFiles
				dataGroup.Go(func() error {
					if b.resume {
						if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(remoteDataFile); isProcessed {
							atomic.AddInt64(&uploadedBytes, processedSize)
							return nil
						}
					}
					log.Debug().Msgf("start upload %d files to %s", len(localFiles), remoteDataFile)
					retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
					err := retry.RunCtx(ctx, func(ctx context.Context) error {
						return b.dst.UploadCompressedStream(ctx, backupPath, localFiles, remoteDataFile, b.cfg.General.UploadMaxBytesPerSecond)
					})
					if err != nil {
						log.Error().Msgf("UploadCompressedStream return error: %v", err)
						return fmt.Errorf("can't upload: %v", err)
					}

					var remoteFile storage.RemoteFile
					retry = retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
					err = retry.RunCtx(ctx, func(ctx context.Context) error {
						remoteFile, err = b.dst.StatFile(ctx, remoteDataFile)
						return err
					})
					if err != nil {
						return fmt.Errorf("can't check uploaded remoteDataFile: %s, error: %v", remoteDataFile, err)
					}
					atomic.AddInt64(&uploadedBytes, remoteFile.Size())
					if b.resume {
						b.resumableState.AppendToState(remoteDataFile, remoteFile.Size())
					}
					// https://github.com/Altinity/clickhouse-backup/issues/777
					if deleteSource {
						for _, f := range localFiles {
							if err = os.Remove(path.Join(backupPath, f)); err != nil {
								return fmt.Errorf("can't remove %s, %v", path.Join(backupPath, f), err)
							}
						}
					}
					log.Debug().Msgf("finish upload to %s", remoteDataFile)
					return nil
				})
			}
		}
	}
	if err := dataGroup.Wait(); err != nil {
		return nil, 0, fmt.Errorf("one of uploadTableData go-routine return error: %v", err)
	}
	log.Debug().Msgf("finish %s.%s with concurrency=%d len(table.Parts[...])=%d uploadedFiles=%v, uploadedBytes=%v", table.Database, table.Table, b.cfg.General.UploadConcurrency, capacity, uploadedFiles, uploadedBytes)
	return uploadedFiles, uploadedBytes, nil
}

func (b *Backuper) uploadTableMetadata(ctx context.Context, backupName string, requiredBackupName string, tableMetadata metadata.TableMetadata) (int64, error) {
	if b.isEmbedded {
		if sqlSize, err := b.uploadTableMetadataEmbedded(ctx, backupName, requiredBackupName, tableMetadata); err != nil {
			return sqlSize, err
		} else {
			jsonSize, err := b.uploadTableMetadataRegular(ctx, backupName, tableMetadata)
			return sqlSize + jsonSize, err
		}
	}
	return b.uploadTableMetadataRegular(ctx, backupName, tableMetadata)
}

func (b *Backuper) uploadTableMetadataRegular(ctx context.Context, backupName string, tableMetadata metadata.TableMetadata) (int64, error) {
	content, err := json.MarshalIndent(&tableMetadata, "", "\t")
	if err != nil {
		return 0, fmt.Errorf("can't marshal json: %v", err)
	}
	remoteTableMetaFile := path.Join(backupName, "metadata", common.TablePathEncode(tableMetadata.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableMetadata.Table)))
	if b.resume {
		if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(remoteTableMetaFile); isProcessed {
			return processedSize, nil
		}
	}
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.PutFile(ctx, remoteTableMetaFile, io.NopCloser(bytes.NewReader(content)), 0)
	})
	if err != nil {
		return 0, fmt.Errorf("can't upload: %v", err)
	}
	if b.resume {
		b.resumableState.AppendToState(remoteTableMetaFile, int64(len(content)))
	}
	return int64(len(content)), nil
}

func (b *Backuper) uploadTableMetadataEmbedded(ctx context.Context, backupName string, requiredBackupName string, tableMetadata metadata.TableMetadata) (int64, error) {
	if b.cfg.ClickHouse.EmbeddedBackupDisk == "" {
		return 0, nil
	}
	remoteTableMetaFile := path.Join(backupName, "metadata", common.TablePathEncode(tableMetadata.Database), fmt.Sprintf("%s.sql", common.TablePathEncode(tableMetadata.Table)))
	if b.resume {
		if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(remoteTableMetaFile); isProcessed {
			return processedSize, nil
		}
	}
	localTableMetaFile := path.Join(b.EmbeddedBackupDataPath, backupName, "metadata", common.TablePathEncode(tableMetadata.Database), fmt.Sprintf("%s.sql", common.TablePathEncode(tableMetadata.Table)))
	var info os.FileInfo
	var localReader *os.File
	var err error
	localReader, err = os.Open(localTableMetaFile)
	if err != nil {
		err = fmt.Errorf("can't open %s: %v", localTableMetaFile, err)
		if requiredBackupName != "" {
			log.Warn().Err(err).Send()
			return 0, nil
		} else {
			return 0, err
		}
	}
	if info, err = os.Stat(localTableMetaFile); err != nil {
		return 0, err
	}
	defer func() {
		if err := localReader.Close(); err != nil {
			log.Warn().Msgf("can't close %v: %v", localReader, err)
		}
	}()
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.PutFile(ctx, remoteTableMetaFile, localReader, 0)
	})
	if err != nil {
		return 0, fmt.Errorf("can't embeeded upload metadata: %v", err)
	}
	if b.resume {
		b.resumableState.AppendToState(remoteTableMetaFile, info.Size())
	}
	return info.Size(), nil
}

func (b *Backuper) markDuplicatedParts(backup *metadata.BackupMetadata, existsTable *metadata.TableMetadata, newTable *metadata.TableMetadata, checkLocal bool) {
	for disk, newParts := range newTable.Parts {
		if _, diskExists := existsTable.Parts[disk]; diskExists {
			if len(existsTable.Parts[disk]) == 0 {
				continue
			}
			existsPartsMap := common.EmptyMap{}
			for _, p := range existsTable.Parts[disk] {
				existsPartsMap[p.Name] = struct{}{}
			}
			for i := range newParts {
				if _, partExists := existsPartsMap[newParts[i].Name]; !partExists {
					continue
				}
				if checkLocal {
					dbAndTablePath := path.Join(common.TablePathEncode(existsTable.Database), common.TablePathEncode(existsTable.Table))
					existsPath := path.Join(b.DiskToPathMap[disk], "backup", backup.RequiredBackup, "shadow", dbAndTablePath, disk, newParts[i].Name)
					newPath := path.Join(b.DiskToPathMap[disk], "backup", backup.BackupName, "shadow", dbAndTablePath, disk, newParts[i].Name)

					if err := filesystemhelper.IsDuplicatedParts(existsPath, newPath); err != nil {
						log.Debug().Msgf("part '%s' and '%s' must be the same: %v", existsPath, newPath, err)
						continue
					}
				}
				newParts[i].Required = true
			}
		}
	}
}

func (b *Backuper) ReadBackupMetadataLocal(ctx context.Context, backupName string) (*metadata.BackupMetadata, error) {
	var backupMetadataBody []byte
	var err error
	allBackupDataPaths := []string{
		path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json"),
		path.Join(b.EmbeddedBackupDataPath, backupName, "metadata.json"),
	}

bodyRead:
	for i, backupMetadataPath := range allBackupDataPaths {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			backupMetadataBody, err = os.ReadFile(backupMetadataPath)
			if err != nil && i == len(allBackupDataPaths)-1 {
				return nil, err
			}
			if backupMetadataBody != nil && len(backupMetadataBody) > 0 {
				break bodyRead
			}
		}
	}
	if backupMetadataBody == nil || len(backupMetadataBody) == 0 {
		return nil, fmt.Errorf("%s not found in %v", path.Join(backupName, "metadata.json"), allBackupDataPaths)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		backupMetadata := metadata.BackupMetadata{}
		if err := json.Unmarshal(backupMetadataBody, &backupMetadata); err != nil {
			return nil, err
		}
		if len(backupMetadata.Tables) == 0 && !b.cfg.General.AllowEmptyBackups {
			return nil, fmt.Errorf("'%s' is empty backup", backupName)
		}
		return &backupMetadata, nil
	}
}

func (b *Backuper) splitPartFiles(basePath string, parts []metadata.Part) ([]metadata.SplitPartFiles, error) {
	if b.cfg.General.UploadByPart {
		return b.splitFilesByName(basePath, parts)
	} else {
		return b.splitFilesBySize(basePath, parts)
	}
}

func (b *Backuper) splitFilesByName(basePath string, parts []metadata.Part) ([]metadata.SplitPartFiles, error) {
	result := make([]metadata.SplitPartFiles, 0)
	for i := range parts {
		if parts[i].Required {
			continue
		}
		var files []string
		partPath := path.Join(basePath, parts[i].Name)
		err := filepath.Walk(partPath, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			relativePath := strings.TrimPrefix(filePath, basePath)
			files = append(files, relativePath)
			return nil
		})
		if err != nil {
			log.Warn().Msgf("filepath.Walk return error: %v", err)
		}
		result = append(result, metadata.SplitPartFiles{
			Prefix: parts[i].Name,
			Files:  files,
		})
	}
	return result, nil
}

func (b *Backuper) splitFilesBySize(basePath string, parts []metadata.Part) ([]metadata.SplitPartFiles, error) {
	var size int64
	var files []string
	maxSize := b.cfg.General.MaxFileSize
	result := make([]metadata.SplitPartFiles, 0)
	partSuffix := 1
	for i := range parts {
		if parts[i].Required {
			continue
		}
		partPath := path.Join(basePath, parts[i].Name)
		err := filepath.Walk(partPath, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			if (size+info.Size()) > maxSize && len(files) > 0 {
				result = append(result, metadata.SplitPartFiles{
					Prefix: strconv.Itoa(partSuffix),
					Files:  files,
				})
				files = []string{}
				size = 0
				partSuffix += 1
			}
			relativePath := strings.TrimPrefix(filePath, basePath)
			files = append(files, relativePath)
			size += info.Size()
			return nil
		})
		if err != nil {
			log.Warn().Msgf("filepath.Walk return error: %v", err)
		}
	}
	if len(files) > 0 {
		result = append(result, metadata.SplitPartFiles{
			Prefix: strconv.Itoa(partSuffix),
			Files:  files,
		})
	}
	return result, nil
}
