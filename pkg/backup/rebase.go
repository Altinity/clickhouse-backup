package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

	"github.com/eapache/go-resiliency/retrier"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// Rebase - copy required parts from the required backups chain into backupName on remote storage
// and remove the required_backup dependency, so backupName becomes a full backup
func (b *Backuper) Rebase(backupName string, commandId int) error {
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	if backupName == "" {
		return errors.New("backup name must be defined")
	}
	if pidCheckErr := pidlock.CheckAndCreatePidFile(backupName, "rebase"); pidCheckErr != nil {
		return pidCheckErr
	}
	defer pidlock.RemovePidFile(backupName)

	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return errors.Wrap(err, "status.Current.GetContextWithCancel")
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	startRebase := time.Now()
	if b.cfg.General.RemoteStorage == "none" || b.cfg.General.RemoteStorage == "custom" {
		return errors.Errorf("rebase does not support `remote_storage: %s`", b.cfg.General.RemoteStorage)
	}
	if err = b.ch.Connect(); err != nil {
		return errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()

	bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, backupName)
	if err != nil {
		return errors.Wrap(err, "storage.NewBackupDestination")
	}
	if err = bd.Connect(ctx); err != nil {
		return errors.Wrapf(err, "can't connect to %s", bd.Kind())
	}
	defer func() {
		if closeErr := bd.Close(ctx); closeErr != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", closeErr)
		}
	}()
	b.dst = bd

	if err = b.rebaseBackup(ctx, backupName); err != nil {
		return err
	}
	log.Info().Fields(map[string]interface{}{
		"backup":    backupName,
		"operation": "rebase",
		"duration":  utils.HumanizeDuration(time.Since(startRebase)),
	}).Msg("done")
	return nil
}

// rebaseRequiredLiveBackups - when `general.rebase_before_remove_old_remote: true`, rebase kept backups
// whose `required_backup` points to a backup outside the `backups_to_keep_remote` window (oldest first),
// so the whole out-of-window chain becomes deletable by GetBackupsToDeleteRemote,
// rebase failure is not fatal: the required backups chain just stays protected as before
func (b *Backuper) rebaseRequiredLiveBackups(ctx context.Context, backupList []storage.Backup) []storage.Backup {
	keep := b.cfg.General.BackupsToKeepRemote
	for {
		candidate := storage.GetOldestLiveBackupToRebase(backupList, keep)
		if candidate == nil {
			return backupList
		}
		log.Info().Fields(map[string]interface{}{
			"operation":       "RemoveOldBackupsRemote",
			"backup":          candidate.BackupName,
			"required_backup": candidate.RequiredBackup,
		}).Msg("rebase to allow delete backups outside backups_to_keep_remote")
		if rebaseErr := b.rebaseBackup(ctx, candidate.BackupName); rebaseErr != nil {
			log.Warn().Msgf("can't rebase %s, keep required backups chain on remote storage: %v", candidate.BackupName, rebaseErr)
			return backupList
		}
		for i := range backupList {
			if backupList[i].BackupName == candidate.BackupName {
				backupList[i].RequiredBackup = ""
				break
			}
		}
	}
}

// rebaseBackup - read remote backup metadata and dispatch to the embedded/regular implementation,
// requires connected b.dst (separated from Rebase so unit tests can inject a mock destination)
func (b *Backuper) rebaseBackup(ctx context.Context, backupName string) error {
	backupList, err := b.dst.BackupList(ctx, true, backupName)
	if err != nil {
		return errors.Wrap(err, "BackupList")
	}
	var remoteBackup *storage.Backup
	for i := range backupList {
		if backupList[i].BackupName == backupName {
			remoteBackup = &backupList[i]
			break
		}
	}
	if remoteBackup == nil {
		return errors.Errorf("%s not found on remote storage", backupName)
	}
	backupMetadata := &remoteBackup.BackupMetadata
	if backupMetadata.RequiredBackup == "" {
		return errors.Errorf("backup %s doesn't contain `required_backup`, nothing to rebase", backupName)
	}
	if strings.Contains(backupMetadata.Tags, "embedded") {
		if err = b.rebaseBackupEmbedded(ctx, backupMetadata); err != nil {
			return err
		}
	}
	return b.rebaseBackupRegular(ctx, backupMetadata, remoteBackup.UploadDate)
}

// rebaseBackupEmbedded - rebase is not implemented for embedded backups yet
func (b *Backuper) rebaseBackupEmbedded(_ context.Context, backupMetadata *metadata.BackupMetadata) error {
	return errors.Errorf("rebase not supported for embedded backup %s", backupMetadata.BackupName)
}

// uploadDate - the original UploadDate of the rebased backup: rebase rewrites metadata.json (fresh
// LastModified on remote storage), keeping the original date in the metadata cache preserves the
// backup position in the UploadDate-ordered retention of RemoveOldBackupsRemote
func (b *Backuper) rebaseBackupRegular(ctx context.Context, backupMetadata *metadata.BackupMetadata, uploadDate time.Time) error {
	backupName := backupMetadata.BackupName
	requiredBackups, err := b.readRequiredBackupsChain(ctx, backupMetadata)
	if err != nil {
		return err
	}
	srcBucket := ""
	switch b.cfg.General.RemoteStorage {
	case "s3":
		srcBucket = b.cfg.S3.Bucket
	case "gcs":
		srcBucket = b.cfg.GCS.Bucket
	case "azblob":
		srcBucket = b.cfg.AzureBlob.Container
	}
	remotePath, err := b.getBackupPath()
	if err != nil {
		return errors.Wrap(err, "getBackupPath")
	}

	// need to resolve `encrypted` disk types, they have object disk blobs only when the underlying disk is an object disk
	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		return errors.Wrap(err, "b.ch.GetDisks")
	}

	rebaseConcurrency := int(b.cfg.General.RebaseConcurrency)
	if rebaseConcurrency < 1 {
		rebaseConcurrency = 1
	}
	var copiedBytes, objectDiskBytes, metadataSizeDiff int64
	rebaseGroup, rebaseCtx := errgroup.WithContext(ctx)
	rebaseGroup.SetLimit(rebaseConcurrency)
	for _, t := range backupMetadata.Tables {
		tableTitle := t
		rebaseGroup.Go(func() error {
			tableCopiedBytes, tableObjectDiskBytes, tableMetadataDiff, rebaseErr := b.rebaseTable(rebaseCtx, backupMetadata, requiredBackups, tableTitle, srcBucket, remotePath, disks)
			if rebaseErr != nil {
				return rebaseErr
			}
			atomic.AddInt64(&copiedBytes, tableCopiedBytes)
			atomic.AddInt64(&objectDiskBytes, tableObjectDiskBytes)
			atomic.AddInt64(&metadataSizeDiff, tableMetadataDiff)
			return nil
		})
	}
	if err = rebaseGroup.Wait(); err != nil {
		return errors.Wrap(err, "one of rebaseTable go-routine return error")
	}

	backupMetadata.RequiredBackup = ""
	backupMetadata.CompressedSize += uint64(copiedBytes)
	backupMetadata.ObjectDiskSize += uint64(objectDiskBytes)
	if newMetadataSize := int64(backupMetadata.MetadataSize) + metadataSizeDiff; newMetadataSize > 0 {
		backupMetadata.MetadataSize = uint64(newMetadataSize)
	}
	newBackupMetadataBody, err := json.MarshalIndent(backupMetadata, "", "\t")
	if err != nil {
		return errors.Wrap(err, "json.MarshalIndent")
	}
	remoteBackupMetaFile := path.Join(backupName, "metadata.json")
	retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
	if err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.PutFile(ctx, remoteBackupMetaFile, io.NopCloser(bytes.NewReader(newBackupMetadataBody)), 0)
	}); err != nil {
		return errors.Wrapf(err, "can't upload %s", remoteBackupMetaFile)
	}
	if cacheErr := b.dst.UpdateMetadataCacheEntry(ctx, storage.Backup{BackupMetadata: *backupMetadata, UploadDate: uploadDate}); cacheErr != nil {
		log.Warn().Msgf("can't update metadata cache for %s error: %v", backupName, cacheErr)
	}
	log.Info().Fields(map[string]interface{}{
		"backup":      backupName,
		"operation":   "rebase",
		"copied_size": utils.FormatBytes(uint64(copiedBytes)),
	}).Msg("required parts copied")
	return nil
}

// readRequiredBackupsChain - read the whole `required_backup` chain, nearest ancestor first,
// each backup in the chain must have the same data_format (compression_format) and must not be embedded
func (b *Backuper) readRequiredBackupsChain(ctx context.Context, backupMetadata *metadata.BackupMetadata) ([]*metadata.BackupMetadata, error) {
	requiredBackups := make([]*metadata.BackupMetadata, 0)
	seen := map[string]struct{}{backupMetadata.BackupName: {}}
	current := backupMetadata
	for current.RequiredBackup != "" {
		if _, cycle := seen[current.RequiredBackup]; cycle {
			return nil, errors.Errorf("required backups chain contains a cycle on %s", current.RequiredBackup)
		}
		seen[current.RequiredBackup] = struct{}{}
		requiredBackup, err := b.ReadBackupMetadataRemote(ctx, current.RequiredBackup)
		if err != nil {
			return nil, errors.Wrapf(err, "ReadBackupMetadataRemote(%s)", current.RequiredBackup)
		}
		if strings.Contains(requiredBackup.Tags, "embedded") {
			return nil, errors.Errorf("required backup %s is embedded, rebase not supported", requiredBackup.BackupName)
		}
		if requiredBackup.DataFormat != backupMetadata.DataFormat {
			return nil, errors.Errorf(
				"required backup %s has data_format=%s but %s has data_format=%s, rebase requires the same compression_format and data_format for the whole backups chain",
				requiredBackup.BackupName, requiredBackup.DataFormat, backupMetadata.BackupName, backupMetadata.DataFormat,
			)
		}
		requiredBackups = append(requiredBackups, requiredBackup)
		current = requiredBackup
	}
	return requiredBackups, nil
}

// rebaseTable - copy all Required parts for one table from the required backups chain,
// remove `required` attribute, update `hash_of_all_files` and overwrite remote table metadata
func (b *Backuper) rebaseTable(ctx context.Context, backupMetadata *metadata.BackupMetadata, requiredBackups []*metadata.BackupMetadata, tableTitle metadata.TableTitle, srcBucket, remotePath string, disks []clickhouse.Disk) (int64, int64, int64, error) {
	backupName := backupMetadata.BackupName
	tm, oldMetadataSize, err := b.readRemoteTableMetadata(ctx, backupName, tableTitle)
	if err != nil {
		return 0, 0, 0, errors.Wrapf(err, "readRemoteTableMetadata(%s, `%s`.`%s`)", backupName, tableTitle.Database, tableTitle.Table)
	}
	requiredTables := make(map[string]*metadata.TableMetadata, len(requiredBackups))
	var copiedBytes, objectDiskBytes int64
	changed := false
	for dstDisk, parts := range tm.Parts {
		for i := range parts {
			if !parts[i].Required {
				continue
			}
			partName := parts[i].Name
			srcBackup, srcTable, srcDisk, findErr := b.findRequiredPartInChain(ctx, requiredBackups, requiredTables, tableTitle, partName)
			if findErr != nil {
				return 0, 0, 0, findErr
			}
			if backupMetadata.DiskTypes[dstDisk] != srcBackup.DiskTypes[srcDisk] {
				return 0, 0, 0, errors.Errorf(
					"can't rebase `%s`.`%s` part %s: disk %s type %s in %s doesn't match disk %s type %s in %s",
					tableTitle.Database, tableTitle.Table, partName,
					srcDisk, srcBackup.DiskTypes[srcDisk], srcBackup.BackupName,
					dstDisk, backupMetadata.DiskTypes[dstDisk], backupName,
				)
			}
			partCopiedBytes, copyErr := b.rebaseCopyPart(ctx, backupMetadata, srcBackup, tm, tableTitle, srcBucket, remotePath, srcDisk, dstDisk, partName)
			if copyErr != nil {
				return 0, 0, 0, copyErr
			}
			copiedBytes += partCopiedBytes
			// part on an object disk keeps only small metadata files in shadow,
			// real blobs live under `object_disk_path` and must be copied too
			hasBlobs, blobsErr := b.partOnObjectDisk(dstDisk, backupMetadata.DiskTypes, disks)
			if blobsErr != nil {
				return 0, 0, 0, blobsErr
			}
			if hasBlobs {
				partBlobBytes, blobCopyErr := b.rebaseCopyPartObjectDiskBlobs(ctx, backupMetadata, srcBackup, tableTitle, srcBucket, srcDisk, dstDisk, partName)
				if blobCopyErr != nil {
					return 0, 0, 0, blobCopyErr
				}
				objectDiskBytes += partBlobBytes
			}
			parts[i].Required = false
			if srcHash, hashExists := srcTable.HashOfAllFiles[partName]; hashExists {
				if tm.HashOfAllFiles == nil {
					tm.HashOfAllFiles = map[string]string{}
				}
				tm.HashOfAllFiles[partName] = srcHash
			}
			if srcChecksum, checksumExists := srcTable.Checksums[partName]; checksumExists {
				if tm.Checksums == nil {
					tm.Checksums = map[string]uint64{}
				}
				tm.Checksums[partName] = srcChecksum
			}
			changed = true
		}
	}
	if !changed {
		return 0, 0, 0, nil
	}
	newMetadataBody, err := json.MarshalIndent(tm, "", "\t")
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "can't marshal json")
	}
	remoteTableMetaFile := path.Join(backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
	if err = retry.RunCtx(ctx, func(ctx context.Context) error {
		return b.dst.PutFile(ctx, remoteTableMetaFile, io.NopCloser(bytes.NewReader(newMetadataBody)), 0)
	}); err != nil {
		return 0, 0, 0, errors.Wrapf(err, "can't upload %s", remoteTableMetaFile)
	}
	return copiedBytes, objectDiskBytes, int64(len(newMetadataBody)) - oldMetadataSize, nil
}

// partOnObjectDisk - report whether parts on diskName store their data as object storage blobs under `object_disk_path`,
// `encrypted` disk type resolves through live system.disks because only encrypted-over-object disks have blobs
func (b *Backuper) partOnObjectDisk(diskName string, diskTypes map[string]string, disks []clickhouse.Disk) (bool, error) {
	diskType := diskTypes[diskName]
	if b.isDiskTypeObject(diskType) {
		return true, nil
	}
	if diskType == "encrypted" {
		for _, d := range disks {
			if d.Name == diskName {
				return b.isDiskTypeEncryptedObject(d, disks), nil
			}
		}
		return false, errors.Errorf("disk %s has type `encrypted` in backup metadata but absent in system.disks, can't determine underlying disk type", diskName)
	}
	return false, nil
}

// rebaseCopyPartObjectDiskBlobs - shadow of a part on an object disk contains only small metadata files,
// real blobs live under <object_disk_path>/<backup_name>/<disk_name>, parse the shadow metadata files
// from srcBackup and copy every referenced blob into the rebased backup object disk path
func (b *Backuper) rebaseCopyPartObjectDiskBlobs(ctx context.Context, backupMetadata, srcBackup *metadata.BackupMetadata, tableTitle metadata.TableTitle, srcBucket, srcDisk, dstDisk, partName string) (int64, error) {
	remoteObjectDiskPath, err := b.getObjectDiskPath()
	if err != nil {
		return 0, errors.Wrap(err, "getObjectDiskPath")
	}
	dbAndTableDir := path.Join(common.TablePathEncode(tableTitle.Database), common.TablePathEncode(tableTitle.Table))
	retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
	var copiedBytes int64
	copyBlobs := func(ctx context.Context, fileName string, content io.Reader) error {
		// fix https://github.com/Altinity/clickhouse-backup/issues/826
		if strings.Contains(fileName, "frozen_metadata.txt") {
			return nil
		}
		objMeta, readErr := object_disk.ReadMetadataFromReader(io.NopCloser(content), fileName)
		if readErr != nil {
			return errors.Wrapf(readErr, "object_disk.ReadMetadataFromReader(%s)", fileName)
		}
		for _, storageObject := range objMeta.StorageObjects {
			if storageObject.ObjectSize == 0 {
				continue
			}
			objPath := storageObject.ObjectPath
			// 25.10+ contains full path, need to make it relative, https://github.com/Altinity/clickhouse-backup/issues/1290
			if storageObject.IsAbsolute {
				objPathParts := strings.Split(objPath, "/")
				if len(objPathParts) >= 2 {
					objPath = strings.Join(objPathParts[len(objPathParts)-2:], "/")
				}
			}
			srcObjKey := path.Join(remoteObjectDiskPath, srcBackup.BackupName, srcDisk, objPath)
			dstObjKey := path.Join(remoteObjectDiskPath, backupMetadata.BackupName, dstDisk, objPath)
			if copyErr := retry.RunCtx(ctx, func(ctx context.Context) error {
				_, err := b.dst.CopyObject(ctx, storageObject.ObjectSize, srcBucket, srcObjKey, dstObjKey)
				return err
			}); copyErr != nil {
				return errors.Wrapf(copyErr, "CopyObject %s -> %s", srcObjKey, dstObjKey)
			}
			copiedBytes += storageObject.ObjectSize
		}
		return nil
	}
	if backupMetadata.DataFormat != DirectoryFormat {
		archiveExt := config.ArchiveExtensions[backupMetadata.DataFormat]
		srcArchive := path.Join(srcBackup.BackupName, "shadow", dbAndTableDir, fmt.Sprintf("%s_%s.%s", srcDisk, common.TablePathEncode(partName), archiveExt))
		if err = b.dst.WalkCompressedStream(ctx, srcArchive, copyBlobs); err != nil {
			return 0, errors.Wrapf(err, "WalkCompressedStream(%s)", srcArchive)
		}
		return copiedBytes, nil
	}
	srcPartPrefix := path.Join(srcBackup.BackupName, "shadow", dbAndTableDir, srcDisk, partName)
	if err = b.dst.Walk(ctx, srcPartPrefix+"/", true, func(ctx context.Context, f storage.RemoteFile) error {
		// azblob hierarchical namespace emits zero-size marker entries, same filter as RemoveBackupRemote
		if f.Size() == 0 && f.LastModified().IsZero() {
			return nil
		}
		reader, readerErr := b.dst.GetFileReader(ctx, path.Join(srcPartPrefix, f.Name()))
		if readerErr != nil {
			return errors.Wrapf(readerErr, "GetFileReader(%s)", path.Join(srcPartPrefix, f.Name()))
		}
		handlerErr := copyBlobs(ctx, f.Name(), reader)
		if closeErr := reader.Close(); closeErr != nil {
			log.Warn().Msgf("can't close reader for %s error: %v", path.Join(srcPartPrefix, f.Name()), closeErr)
		}
		return handlerErr
	}); err != nil {
		return 0, errors.Wrapf(err, "Walk %s", srcPartPrefix)
	}
	return copiedBytes, nil
}

// findRequiredPartInChain - walk over the required backups chain (nearest ancestor first) and find the backup
// which really contains partName data (part exists in its table metadata without `required` attribute)
func (b *Backuper) findRequiredPartInChain(ctx context.Context, requiredBackups []*metadata.BackupMetadata, requiredTables map[string]*metadata.TableMetadata, tableTitle metadata.TableTitle, partName string) (*metadata.BackupMetadata, *metadata.TableMetadata, string, error) {
	for _, requiredBackup := range requiredBackups {
		requiredTable, exists := requiredTables[requiredBackup.BackupName]
		if !exists {
			var err error
			requiredTable, _, err = b.readRemoteTableMetadata(ctx, requiredBackup.BackupName, tableTitle)
			if err != nil {
				return nil, nil, "", errors.Wrapf(err, "readRemoteTableMetadata(%s, `%s`.`%s`)", requiredBackup.BackupName, tableTitle.Database, tableTitle.Table)
			}
			requiredTables[requiredBackup.BackupName] = requiredTable
		}
		for srcDisk, srcParts := range requiredTable.Parts {
			for j := range srcParts {
				if srcParts[j].Name == partName {
					if srcParts[j].Required {
						// data is deeper in the chain
						break
					}
					return requiredBackup, requiredTable, srcDisk, nil
				}
			}
		}
	}
	return nil, nil, "", errors.Errorf("`%s`.`%s` part %s not found in the whole required backups chain", tableTitle.Database, tableTitle.Table, partName)
}

// rebaseCopyPart - copy one part data from srcBackup into backupMetadata.BackupName via server-side CopyObject with retries
func (b *Backuper) rebaseCopyPart(ctx context.Context, backupMetadata, srcBackup *metadata.BackupMetadata, tm *metadata.TableMetadata, tableTitle metadata.TableTitle, srcBucket, remotePath, srcDisk, dstDisk, partName string) (int64, error) {
	dbAndTableDir := path.Join(common.TablePathEncode(tableTitle.Database), common.TablePathEncode(tableTitle.Table))
	retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
	if backupMetadata.DataFormat != DirectoryFormat {
		archiveExt := config.ArchiveExtensions[backupMetadata.DataFormat]
		srcArchive := fmt.Sprintf("%s_%s.%s", srcDisk, common.TablePathEncode(partName), archiveExt)
		dstArchive := fmt.Sprintf("%s_%s.%s", dstDisk, common.TablePathEncode(partName), archiveExt)
		srcRemote := path.Join(srcBackup.BackupName, "shadow", dbAndTableDir, srcArchive)
		dstRemote := path.Join(backupMetadata.BackupName, "shadow", dbAndTableDir, dstArchive)
		srcFile, err := b.dst.StatFile(ctx, srcRemote)
		if err != nil {
			return 0, errors.Wrapf(err, "`%s`.`%s` part %s expected in %s, only backups uploaded with `upload_by_part: true` can be rebased, StatFile", tableTitle.Database, tableTitle.Table, partName, srcRemote)
		}
		if err = retry.RunCtx(ctx, func(ctx context.Context) error {
			_, copyErr := b.dst.CopyObject(ctx, srcFile.Size(), srcBucket, path.Join(remotePath, srcRemote), path.Join(remotePath, dstRemote))
			return copyErr
		}); err != nil {
			return 0, errors.Wrapf(err, "CopyObject %s -> %s", srcRemote, dstRemote)
		}
		if tm.Files == nil {
			tm.Files = map[string][]string{}
		}
		tm.Files[dstDisk] = append(tm.Files[dstDisk], dstArchive)
		return srcFile.Size(), nil
	}
	// DirectoryFormat - copy every file of the part directory
	srcPartPrefix := path.Join(srcBackup.BackupName, "shadow", dbAndTableDir, srcDisk, partName)
	dstPartPrefix := path.Join(backupMetadata.BackupName, "shadow", dbAndTableDir, dstDisk, partName)
	var copiedBytes int64
	filesFound := false
	if err := b.dst.Walk(ctx, srcPartPrefix+"/", true, func(ctx context.Context, f storage.RemoteFile) error {
		// azblob hierarchical namespace emits zero-size marker entries, same filter as RemoveBackupRemote
		if f.Size() == 0 && f.LastModified().IsZero() {
			return nil
		}
		filesFound = true
		srcRemote := path.Join(srcPartPrefix, f.Name())
		dstRemote := path.Join(dstPartPrefix, f.Name())
		if copyErr := retry.RunCtx(ctx, func(ctx context.Context) error {
			_, err := b.dst.CopyObject(ctx, f.Size(), srcBucket, path.Join(remotePath, srcRemote), path.Join(remotePath, dstRemote))
			return err
		}); copyErr != nil {
			return errors.Wrapf(copyErr, "CopyObject %s -> %s", srcRemote, dstRemote)
		}
		copiedBytes += f.Size()
		return nil
	}); err != nil {
		return 0, errors.Wrapf(err, "Walk %s", srcPartPrefix)
	}
	if !filesFound {
		return 0, errors.Errorf("`%s`.`%s` part %s not found in %s", tableTitle.Database, tableTitle.Table, partName, srcPartPrefix)
	}
	return copiedBytes, nil
}

// readRemoteTableMetadata - read backupName/metadata/db/table.json from remote storage into memory
func (b *Backuper) readRemoteTableMetadata(ctx context.Context, backupName string, tableTitle metadata.TableTitle) (*metadata.TableMetadata, int64, error) {
	remoteTableMetaFile := path.Join(backupName, "metadata", common.TablePathEncode(tableTitle.Database), fmt.Sprintf("%s.json", common.TablePathEncode(tableTitle.Table)))
	reader, err := b.dst.GetFileReader(ctx, remoteTableMetaFile)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "GetFileReader(%s)", remoteTableMetaFile)
	}
	body, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil {
		return nil, 0, errors.Wrapf(err, "ReadAll(%s)", remoteTableMetaFile)
	}
	if closeErr != nil {
		log.Warn().Msgf("can't close reader for %s error: %v", remoteTableMetaFile, closeErr)
	}
	tm := &metadata.TableMetadata{}
	if err = json.Unmarshal(body, tm); err != nil {
		return nil, 0, errors.Wrapf(err, "json.Unmarshal(%s)", remoteTableMetaFile)
	}
	return tm, int64(len(body)), nil
}
