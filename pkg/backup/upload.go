package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/custom"
	"github.com/Altinity/clickhouse-backup/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/pkg/status"
	"github.com/eapache/go-resiliency/retrier"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/Altinity/clickhouse-backup/pkg/common"
	"github.com/Altinity/clickhouse-backup/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/pkg/utils"
	apexLog "github.com/apex/log"
	"github.com/yargevad/filepathx"
)

func (b *Backuper) Upload(backupName, diffFrom, diffFromRemote, tablePattern string, partitions []string, schemaOnly, resume bool, commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	startUpload := time.Now()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	var disks []clickhouse.Disk
	if !resume && b.cfg.General.UseResumableState {
		resume = true
	}
	b.resume = resume
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
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "upload",
	})
	if _, disks, err = b.getLocalBackup(ctx, backupName, nil); err != nil {
		return fmt.Errorf("can't find local backup: %v", err)
	}
	if err := b.init(ctx, disks, backupName); err != nil {
		return err
	}
	defer func() {
		if err := b.dst.Close(ctx); err != nil {
			b.log.Warnf("can't close BackupDestination error: %v", err)
		}
	}()

	remoteBackups, err := b.dst.BackupList(ctx, false, "")
	if err != nil {
		return err
	}
	for i := range remoteBackups {
		if backupName == remoteBackups[i].BackupName {
			if !b.resume {
				return fmt.Errorf("'%s' already exists on remote storage", backupName)
			} else {
				log.Warnf("'%s' already exists on remote, will try to resume upload", backupName)
			}
		}
	}
	backupMetadata, err := b.ReadBackupMetadataLocal(ctx, backupName)
	if err != nil {
		return err
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
			return err
		}
	}
	tablesForUploadFromDiff := map[metadata.TableTitle]metadata.TableMetadata{}

	if diffFrom != "" && !b.isEmbedded {
		tablesForUploadFromDiff, err = b.getTablesForUploadDiffLocal(ctx, diffFrom, backupMetadata, tablePattern)
		if err != nil {
			return err
		}
	}
	if diffFromRemote != "" && !b.isEmbedded {
		tablesForUploadFromDiff, err = b.getTablesForUploadDiffRemote(ctx, diffFromRemote, backupMetadata, tablePattern)
		if err != nil {
			return err
		}
	}
	if b.resume {
		b.resumableState = resumable.NewState(b.DefaultDataPath, backupName, "upload", map[string]interface{}{
			"diffFrom":       diffFrom,
			"diffFromRemote": diffFromRemote,
			"tablePattern":   tablePattern,
			"partitions":     partitions,
			"schemaOnly":     schemaOnly,
		})
	}

	compressedDataSize := int64(0)
	metadataSize := int64(0)

	log.Debugf("prepare table concurrent semaphore with concurrency=%d len(tablesForUpload)=%d", b.cfg.General.UploadConcurrency, len(tablesForUpload))
	uploadSemaphore := semaphore.NewWeighted(int64(b.cfg.General.UploadConcurrency))
	uploadGroup, uploadCtx := errgroup.WithContext(ctx)

	for i, table := range tablesForUpload {
		if err := uploadSemaphore.Acquire(uploadCtx, 1); err != nil {
			log.Errorf("can't acquire semaphore during Upload table: %v", err)
			break
		}
		start := time.Now()
		if !schemaOnly {
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
			defer uploadSemaphore.Release(1)
			var uploadedBytes int64
			if !schemaOnly {
				var files map[string][]string
				var err error
				files, uploadedBytes, err = b.uploadTableData(uploadCtx, backupName, tablesForUpload[idx])
				if err != nil {
					return err
				}
				atomic.AddInt64(&compressedDataSize, uploadedBytes)
				tablesForUpload[idx].Files = files
			}
			tableMetadataSize, err := b.uploadTableMetadata(uploadCtx, backupName, tablesForUpload[idx])
			if err != nil {
				return err
			}
			atomic.AddInt64(&metadataSize, tableMetadataSize)
			log.
				WithField("table", fmt.Sprintf("%s.%s", tablesForUpload[idx].Database, tablesForUpload[idx].Table)).
				WithField("duration", utils.HumanizeDuration(time.Since(start))).
				WithField("size", utils.FormatBytes(uint64(uploadedBytes+tableMetadataSize))).
				Info("done")
			return nil
		})
	}
	if err := uploadGroup.Wait(); err != nil {
		return fmt.Errorf("one of upload table go-routine return error: %v", err)
	}

	if !b.isEmbedded {
		// upload rbac for backup
		if backupMetadata.RBACSize, err = b.uploadRBACData(ctx, backupName); err != nil {
			return err
		}

		// upload configs for backup
		if backupMetadata.ConfigSize, err = b.uploadConfigData(ctx, backupName); err != nil {
			return err
		}
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
		backupMetadata.DataFormat = "directory"
	}
	newBackupMetadataBody, err := json.MarshalIndent(backupMetadata, "", "\t")
	if err != nil {
		return err
	}
	remoteBackupMetaFile := path.Join(backupName, "metadata.json")
	if !b.resume || (b.resume && !b.resumableState.IsAlreadyProcessedBool(remoteBackupMetaFile)) {
		retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
		err = retry.RunCtx(ctx, func(ctx context.Context) error {
			return b.dst.PutFile(ctx, remoteBackupMetaFile, io.NopCloser(bytes.NewReader(newBackupMetadataBody)))
		})
		if err != nil {
			return fmt.Errorf("can't upload %s: %v", remoteBackupMetaFile, err)
		}
	}
	if b.isEmbedded {
		localClickHouseBackupFile := path.Join(b.EmbeddedBackupDataPath, backupName, ".backup")
		remoteClickHouseBackupFile := path.Join(backupName, ".backup")
		if err = b.uploadSingleBackupFile(ctx, localClickHouseBackupFile, remoteClickHouseBackupFile); err != nil {
			return err
		}
	}
	if b.resume {
		b.resumableState.Close()
	}
	log.
		WithField("duration", utils.HumanizeDuration(time.Since(startUpload))).
		WithField("size", utils.FormatBytes(uint64(compressedDataSize)+uint64(metadataSize)+uint64(len(newBackupMetadataBody))+backupMetadata.RBACSize+backupMetadata.ConfigSize)).
		Info("done")

	// Clean
	if err = b.dst.RemoveOldBackups(ctx, b.cfg.General.BackupsToKeepRemote); err != nil {
		return fmt.Errorf("can't remove old backups on remote storage: %v", err)
	}
	return nil
}

func (b *Backuper) uploadSingleBackupFile(ctx context.Context, localFile, remoteFile string) error {
	if b.resume && b.resumableState.IsAlreadyProcessedBool(remoteFile) {
		return nil
	}
	log := b.log.WithField("logger", "uploadSingleBackupFile")
	f, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("can't open %s: %v", localFile, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warnf("can't close %v: %v", f, err)
		}
	}()
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.PutFile(ctx, remoteFile, f)
	})
	if err != nil {
		return fmt.Errorf("can't upload %s: %v", remoteFile, err)
	}
	if b.resume {
		info, err := os.Stat(localFile)
		if err != nil {
			return fmt.Errorf("can't stat %s", localFile)
		}
		b.resumableState.AppendToState(remoteFile, info.Size())
	}
	return nil
}

func (b *Backuper) prepareTableListToUpload(ctx context.Context, backupName string, tablePattern string, partitions []string) (tablesForUpload ListOfTables, err error) {
	metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
	if b.isEmbedded {
		metadataPath = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata")
	}
	tablesForUpload, _, err = getTableListByPatternLocal(ctx, b.cfg, b.ch, metadataPath, tablePattern, false, partitions)
	if err != nil {
		return nil, err
	}
	return tablesForUpload, nil
}

func (b *Backuper) getTablesForUploadDiffLocal(ctx context.Context, diffFrom string, backupMetadata *metadata.BackupMetadata, tablePattern string) (tablesForUploadFromDiff map[metadata.TableTitle]metadata.TableMetadata, err error) {
	tablesForUploadFromDiff = make(map[metadata.TableTitle]metadata.TableMetadata)
	diffFromBackup, err := b.ReadBackupMetadataLocal(ctx, diffFrom)
	if err != nil {
		return nil, err
	}
	if len(diffFromBackup.Tables) != 0 {
		backupMetadata.RequiredBackup = diffFrom
		metadataPath := path.Join(b.DefaultDataPath, "backup", diffFrom, "metadata")
		// empty partitions, because we don't want filter
		diffTablesList, _, err := getTableListByPatternLocal(ctx, b.cfg, b.ch, metadataPath, tablePattern, false, []string{})
		if err != nil {
			return nil, err
		}
		for _, t := range diffTablesList {
			tablesForUploadFromDiff[metadata.TableTitle{
				Database: t.Database,
				Table:    t.Table,
			}] = t
		}
	}
	return tablesForUploadFromDiff, nil
}

func (b *Backuper) getTablesForUploadDiffRemote(ctx context.Context, diffFromRemote string, backupMetadata *metadata.BackupMetadata, tablePattern string) (tablesForUploadFromDiff map[metadata.TableTitle]metadata.TableMetadata, err error) {
	tablesForUploadFromDiff = make(map[metadata.TableTitle]metadata.TableMetadata)
	backupList, err := b.dst.BackupList(ctx, true, diffFromRemote)
	if err != nil {
		return nil, err
	}
	var diffRemoteMetadata *metadata.BackupMetadata
	for _, backup := range backupList {
		if backup.BackupName == diffFromRemote {
			if backup.Legacy {
				return nil, fmt.Errorf("%s have legacy format and can't be used as diff-from-remote source", diffFromRemote)
			}
			diffRemoteMetadata = &backup.BackupMetadata
			break
		}
	}
	if diffRemoteMetadata == nil {
		return nil, fmt.Errorf("%s not found on remote storage", diffFromRemote)
	}

	if len(diffRemoteMetadata.Tables) != 0 {
		backupMetadata.RequiredBackup = diffFromRemote
		diffTablesList, err := getTableListByPatternRemote(ctx, b, diffRemoteMetadata, tablePattern, false)
		if err != nil {
			return nil, err
		}
		for _, t := range diffTablesList {
			tablesForUploadFromDiff[metadata.TableTitle{
				Database: t.Database,
				Table:    t.Table,
			}] = t
		}
	}
	return tablesForUploadFromDiff, nil
}

func (b *Backuper) validateUploadParams(ctx context.Context, backupName string, diffFrom string, diffFromRemote string) error {
	log := b.log.WithField("logger", "validateUploadParams")
	if b.cfg.General.RemoteStorage == "none" {
		return fmt.Errorf("general->remote_storage shall not be \"none\" for upload, change you config or use REMOTE_STORAGE environment variable")
	}
	if backupName == "" {
		_ = b.PrintLocalBackups(ctx, "all")
		return fmt.Errorf("select backup for upload")
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
	if (diffFrom != "" || diffFromRemote != "") && b.cfg.ClickHouse.UseEmbeddedBackupRestore {
		log.Warnf("--diff-from and --diff-from-remote not compatible with backups created with `use_embedded_backup_restore: true`")
	}
	if b.cfg.General.RemoteStorage == "custom" && b.resume {
		return fmt.Errorf("can't resume for `remote_storage: custom`")
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
	configBackupPath := path.Join(b.DefaultDataPath, "backup", backupName, "configs")
	configFilesGlobPattern := path.Join(configBackupPath, "**/*.*")
	remoteConfigsArchive := path.Join(backupName, fmt.Sprintf("configs.%s", b.cfg.GetArchiveExtension()))
	return b.uploadAndArchiveBackupRelatedDir(ctx, configBackupPath, configFilesGlobPattern, remoteConfigsArchive)

}

func (b *Backuper) uploadRBACData(ctx context.Context, backupName string) (uint64, error) {
	rbacBackupPath := path.Join(b.DefaultDataPath, "backup", backupName, "access")
	accessFilesGlobPattern := path.Join(rbacBackupPath, "*.*")
	remoteRBACArchive := path.Join(backupName, fmt.Sprintf("access.%s", b.cfg.GetArchiveExtension()))
	return b.uploadAndArchiveBackupRelatedDir(ctx, rbacBackupPath, accessFilesGlobPattern, remoteRBACArchive)
}

func (b *Backuper) uploadAndArchiveBackupRelatedDir(ctx context.Context, localBackupRelatedDir, localFilesGlobPattern, remoteFile string) (uint64, error) {
	if _, err := os.Stat(localBackupRelatedDir); os.IsNotExist(err) {
		return 0, nil
	}
	if b.resume {
		if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(remoteFile); isProcessed {
			return uint64(processedSize), nil
		}
	}
	var localFiles []string
	var err error
	if localFiles, err = filepathx.Glob(localFilesGlobPattern); err != nil || localFiles == nil || len(localFiles) == 0 {
		return 0, fmt.Errorf("list %s return list=%v with err=%v", localFilesGlobPattern, localFiles, err)
	}
	for i := range localFiles {
		localFiles[i] = strings.Replace(localFiles[i], localBackupRelatedDir, "", 1)
	}

	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.UploadCompressedStream(ctx, localBackupRelatedDir, localFiles, remoteFile)
	})

	if err != nil {
		return 0, fmt.Errorf("can't RBAC or config upload: %v", err)
	}
	remoteUploaded, err := b.dst.StatFile(ctx, remoteFile)
	if err != nil {
		return 0, fmt.Errorf("can't check uploaded %s file: %v", remoteFile, err)
	}
	if b.resume {
		b.resumableState.AppendToState(remoteFile, remoteUploaded.Size())
	}
	return uint64(remoteUploaded.Size()), nil
}

func (b *Backuper) uploadTableData(ctx context.Context, backupName string, table metadata.TableMetadata) (map[string][]string, int64, error) {
	dbAndTablePath := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
	uploadedFiles := map[string][]string{}
	capacity := 0
	for disk := range table.Parts {
		capacity += len(table.Parts[disk])
	}
	log := b.log.WithField("logger", "uploadTableData")
	log.Debugf("start %s.%s with concurrency=%d len(table.Parts[...])=%d", table.Database, table.Table, b.cfg.General.UploadConcurrency, capacity)
	s := semaphore.NewWeighted(int64(b.cfg.General.UploadConcurrency))
	g, ctx := errgroup.WithContext(ctx)
	var uploadedBytes int64

	splitParts := make(map[string][]metadata.SplitPartFiles, 0)
	splitPartsOffset := make(map[string]int, 0)
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
breakByError:
	for common.SumMapValuesInt(splitPartsOffset) < splitPartsCapacity {
		for disk := range table.Parts {
			if splitPartsOffset[disk] >= len(splitParts[disk]) {
				continue
			}
			if err := s.Acquire(ctx, 1); err != nil {
				log.Errorf("can't acquire semaphore during Upload data parts: %v", err)
				break breakByError
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
				g.Go(func() error {
					defer s.Release(1)
					if b.resume {
						if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(remotePathFull); isProcessed {
							atomic.AddInt64(&uploadedBytes, processedSize)
							return nil
						}
					}
					log.Debugf("start upload %d files to %s", len(partFiles), remotePath)
					if uploadPathBytes, err := b.dst.UploadPath(ctx, 0, backupPath, partFiles, remotePath, b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration); err != nil {
						log.Errorf("UploadPath return error: %v", err)
						return fmt.Errorf("can't upload: %v", err)
					} else {
						atomic.AddInt64(&uploadedBytes, uploadPathBytes)
						if b.resume {
							b.resumableState.AppendToState(remotePathFull, uploadPathBytes)
						}
					}
					log.Debugf("finish upload %d files to %s", len(partFiles), remotePath)
					return nil
				})
			} else {
				fileName := fmt.Sprintf("%s_%s.%s", disk, common.TablePathEncode(partSuffix), b.cfg.GetArchiveExtension())
				uploadedFiles[disk] = append(uploadedFiles[disk], fileName)
				remoteDataFile := path.Join(baseRemoteDataPath, fileName)
				localFiles := partFiles
				g.Go(func() error {
					defer s.Release(1)
					if b.resume {
						if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(remoteDataFile); isProcessed {
							atomic.AddInt64(&uploadedBytes, processedSize)
							return nil
						}
					}
					log.Debugf("start upload %d files to %s", len(localFiles), remoteDataFile)
					retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
					err := retry.RunCtx(ctx, func(ctx context.Context) error {
						return b.dst.UploadCompressedStream(ctx, backupPath, localFiles, remoteDataFile)
					})
					if err != nil {
						log.Errorf("UploadCompressedStream return error: %v", err)
						return fmt.Errorf("can't upload: %v", err)
					}
					remoteFile, err := b.dst.StatFile(ctx, remoteDataFile)
					if err != nil {
						return fmt.Errorf("can't check uploaded file: %v", err)
					}
					atomic.AddInt64(&uploadedBytes, remoteFile.Size())
					if b.resume {
						b.resumableState.AppendToState(remoteDataFile, remoteFile.Size())
					}
					log.Debugf("finish upload to %s", remoteDataFile)
					return nil
				})
			}
		}
	}
	if err := g.Wait(); err != nil {
		return nil, 0, fmt.Errorf("one of uploadTableData go-routine return error: %v", err)
	}
	log.Debugf("finish %s.%s with concurrency=%d len(table.Parts[...])=%d uploadedFiles=%v, uploadedBytes=%v", table.Database, table.Table, b.cfg.General.UploadConcurrency, capacity, uploadedFiles, uploadedBytes)
	return uploadedFiles, uploadedBytes, nil
}

func (b *Backuper) uploadTableMetadata(ctx context.Context, backupName string, tableMetadata metadata.TableMetadata) (int64, error) {
	if b.isEmbedded {
		if sqlSize, err := b.uploadTableMetadataEmbedded(ctx, backupName, tableMetadata); err != nil {
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
		return b.dst.PutFile(ctx, remoteTableMetaFile, io.NopCloser(bytes.NewReader(content)))
	})
	if err != nil {
		return 0, fmt.Errorf("can't upload: %v", err)
	}
	if b.resume {
		b.resumableState.AppendToState(remoteTableMetaFile, int64(len(content)))
	}
	return int64(len(content)), nil
}

func (b *Backuper) uploadTableMetadataEmbedded(ctx context.Context, backupName string, tableMetadata metadata.TableMetadata) (int64, error) {
	remoteTableMetaFile := path.Join(backupName, "metadata", common.TablePathEncode(tableMetadata.Database), fmt.Sprintf("%s.sql", common.TablePathEncode(tableMetadata.Table)))
	if b.resume {
		if isProcessed, processedSize := b.resumableState.IsAlreadyProcessed(remoteTableMetaFile); isProcessed {
			return processedSize, nil
		}
	}
	log := b.log.WithField("logger", "uploadTableMetadataEmbedded")
	localTableMetaFile := path.Join(b.EmbeddedBackupDataPath, backupName, "metadata", common.TablePathEncode(tableMetadata.Database), fmt.Sprintf("%s.sql", common.TablePathEncode(tableMetadata.Table)))
	localReader, err := os.Open(localTableMetaFile)
	if err != nil {
		return 0, fmt.Errorf("can't open %s: %v", localTableMetaFile, err)
	}
	defer func() {
		if err := localReader.Close(); err != nil {
			log.Warnf("can't close %v: %v", localReader, err)
		}
	}()
	retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
	err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.PutFile(ctx, remoteTableMetaFile, localReader)
	})
	if err != nil {
		return 0, fmt.Errorf("can't embeeded upload metadata: %v", err)
	}
	if info, err := os.Stat(localTableMetaFile); err != nil {
		return 0, fmt.Errorf("stat %s error: %v", localTableMetaFile, err)
	} else {
		if b.resume {
			b.resumableState.AppendToState(remoteTableMetaFile, info.Size())
		}
		return info.Size(), nil
	}
}

func (b *Backuper) markDuplicatedParts(backup *metadata.BackupMetadata, existsTable *metadata.TableMetadata, newTable *metadata.TableMetadata, checkLocal bool) {
	log := b.log.WithField("logger", "markDuplicatedParts")
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
						log.Debugf("part '%s' and '%s' must be the same: %v", existsPath, newPath, err)
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
	log := b.log.WithField("logger", "splitFilesByName")
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
			log.Warnf("filepath.Walk return error: %v", err)
		}
		result = append(result, metadata.SplitPartFiles{
			Prefix: parts[i].Name,
			Files:  files,
		})
	}
	return result, nil
}

func (b *Backuper) splitFilesBySize(basePath string, parts []metadata.Part) ([]metadata.SplitPartFiles, error) {
	log := b.log.WithField("logger", "splitFilesBySize")
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
			log.Warnf("filepath.Walk return error: %v", err)
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
