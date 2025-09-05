package backup

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/custom"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/enhanced"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// Clean - removed all data in shadow folder
func (b *Backuper) Clean(ctx context.Context) error {
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		return err
	}
	for _, disk := range disks {
		if disk.IsBackup {
			continue
		}
		shadowDir := path.Join(disk.Path, "shadow")
		if err := b.cleanDir(shadowDir); err != nil {
			return fmt.Errorf("can't clean '%s': %v", shadowDir, err)
		}
		log.Info().Msg(shadowDir)
	}
	return nil
}

func (b *Backuper) cleanDir(dirName string) error {
	items, err := os.ReadDir(dirName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, item := range items {
		if err = os.RemoveAll(path.Join(dirName, item.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Delete - remove local or remote backup
func (b *Backuper) Delete(backupType, backupName string, commandId int) error {
	if pidCheckErr := pidlock.CheckAndCreatePidFile(backupName, "delete"); pidCheckErr != nil {
		return pidCheckErr
	}
	defer pidlock.RemovePidFile(backupName)

	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	switch backupType {
	case "local":
		return b.RemoveBackupLocal(ctx, backupName, nil)
	case "remote":
		return b.RemoveBackupRemote(ctx, backupName)
	default:
		return fmt.Errorf("unknown backup type")
	}
}

func (b *Backuper) RemoveOldBackupsLocal(ctx context.Context, keepLastBackup bool, disks []clickhouse.Disk) error {
	keep := b.cfg.General.BackupsToKeepLocal
	if keep == 0 {
		return nil
	}
	// fix https://github.com/Altinity/clickhouse-backup/issues/698
	if keep < 0 {
		keep = 0
		if keepLastBackup {
			keep = 1
		}
	}

	backupList, disks, err := b.GetLocalBackups(ctx, disks)
	if err != nil {
		return err
	}
	backupsToDelete := GetBackupsToDeleteLocal(backupList, keep)
	for _, backup := range backupsToDelete {
		if deleteErr := b.RemoveBackupLocal(ctx, backup.BackupName, disks); deleteErr != nil {
			return deleteErr
		}
	}
	return nil
}

func (b *Backuper) RemoveBackupLocal(ctx context.Context, backupName string, disks []clickhouse.Disk) error {
	var err error
	start := time.Now()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	if err = b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()
	if disks == nil {
		disks, err = b.ch.GetDisks(ctx, true)
		if err != nil {
			return err
		}
	}
	backupList, disks, err := b.GetLocalBackups(ctx, disks)
	if err != nil {
		return err
	}
	hasObjectDisks := b.hasObjectDisksLocal(backupList, backupName, disks)

	for _, backup := range backupList {
		if backup.BackupName == backupName {
			b.isEmbedded = strings.Contains(backup.Tags, "embedded")
			if hasObjectDisks || (b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk == "") {
				bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, backupName)
				if err != nil {
					return err
				}
				err = bd.Connect(ctx)
				if err != nil {
					return fmt.Errorf("can't connect to remote storage: %v", err)
				}
				defer func() {
					if err := bd.Close(ctx); err != nil {
						log.Warn().Msgf("can't close BackupDestination error: %v", err)
					}
				}()
				b.dst = bd
			}
			err = b.cleanEmbeddedAndObjectDiskLocalIfSameRemoteNotPresent(ctx, backupName, disks, backup, hasObjectDisks)
			if err != nil {
				return err
			}
			for _, disk := range disks {
				backupPath := path.Join(disk.Path, "backup", backupName)
				if disk.IsBackup {
					backupPath = path.Join(disk.Path, backupName)
				}
				log.Info().Msgf("remove '%s'", backupPath)
				if err = os.RemoveAll(backupPath); err != nil {
					return err
				}
			}
			log.Info().Str("operation", "delete").
				Str("location", "local").
				Str("backup", backupName).
				Str("duration", utils.HumanizeDuration(time.Since(start))).
				Msg("done")
			return nil
		}
	}
	return fmt.Errorf("'%s' is not found on local storage", backupName)
}

func (b *Backuper) cleanEmbeddedAndObjectDiskLocalIfSameRemoteNotPresent(ctx context.Context, backupName string, disks []clickhouse.Disk, backup LocalBackup, hasObjectDisks bool) error {
	skip, err := b.skipIfTheSameRemoteBackupPresent(ctx, backup.BackupName, backup.Tags)
	log.Debug().Msgf("b.skipIfTheSameRemoteBackupPresent return skip=%v", skip)
	if err != nil {
		return err
	}
	if !skip && (hasObjectDisks || (b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk == "")) {
		startTime := time.Now()
		if deletedKeys, deleteErr := b.cleanBackupObjectDisks(ctx, backupName); deleteErr != nil {
			log.Warn().Msgf("b.cleanBackupObjectDisks return error: %v", deleteErr)
			return err
		} else {
			log.Info().Str("backup", backupName).Str("duration", utils.HumanizeDuration(time.Since(startTime))).Msgf("cleanBackupObjectDisks deleted %d keys", deletedKeys)
		}
	}
	if !skip && (b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "") {
		if err = b.cleanLocalEmbedded(ctx, backup, disks); err != nil {
			log.Warn().Msgf("b.cleanLocalEmbedded return error: %v", err)
			return err
		}
	}
	return nil
}

func (b *Backuper) hasObjectDisksLocal(backupList []LocalBackup, backupName string, disks []clickhouse.Disk) bool {
	for _, backup := range backupList {
		if backup.BackupName == backupName && !b.isEmbedded {
			for _, disk := range disks {
				if !disk.IsBackup && (b.isDiskTypeObject(disk.Type) || b.isDiskTypeEncryptedObject(disk, disks)) {
					backupExists, err := os.ReadDir(path.Join(disk.Path, "backup", backup.BackupName))
					if err == nil && len(backupExists) > 0 {
						log.Debug().Msgf("hasObjectDisksLocal: found object disk %s", disk.Name)
						return true
					}
				}
			}
		}
	}
	return false
}

func (b *Backuper) cleanLocalEmbedded(ctx context.Context, backup LocalBackup, disks []clickhouse.Disk) error {
	for _, disk := range disks {
		if disk.Name == b.cfg.ClickHouse.EmbeddedBackupDisk && disk.Type != "local" {
			if err := object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, disk.Name); err != nil {
				return err
			}
			backupPath := path.Join(disk.Path, backup.BackupName)
			if err := filepath.Walk(backupPath, func(filePath string, info fs.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() && !strings.HasSuffix(filePath, ".json") && !strings.HasPrefix(filePath, path.Join(backupPath, "access")) {
					log.Debug().Msgf("object_disk.ReadMetadataFromFile(%s)", filePath)
					meta, err := object_disk.ReadMetadataFromFile(filePath)
					if err != nil {
						return err
					}
					for _, o := range meta.StorageObjects {
						err = object_disk.DeleteFile(ctx, b.cfg.ClickHouse.EmbeddedBackupDisk, o.ObjectRelativePath)
						if err != nil {
							return err
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Backuper) skipIfTheSameRemoteBackupPresent(ctx context.Context, backupName, tags string) (bool, error) {
	if b.cfg.General.RemoteStorage != "custom" && b.cfg.General.RemoteStorage != "none" {
		if remoteList, err := b.GetRemoteBackups(ctx, true); err != nil {
			return true, err
		} else {
			for _, remoteBackup := range remoteList {
				if remoteBackup.BackupName == backupName {
					if tags == "" || (tags != "" && strings.Contains(remoteBackup.Tags, tags)) {
						return true, nil
					}
				}
			}
		}
	}
	return false, nil
}

func (b *Backuper) RemoveBackupRemote(ctx context.Context, backupName string) error {
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	start := time.Now()
	if b.cfg.General.RemoteStorage == "none" {
		err := errors.New("aborted: RemoteStorage set to \"none\"")
		log.Error().Msg(err.Error())
		return err
	}
	if b.cfg.General.RemoteStorage == "custom" {
		return custom.DeleteRemote(ctx, b.cfg, backupName)
	}
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, "")
	if err != nil {
		return err
	}
	err = bd.Connect(ctx)
	if err != nil {
		return fmt.Errorf("can't connect to remote storage: %v", err)
	}
	defer func() {
		if err := bd.Close(ctx); err != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", err)
		}
	}()

	b.dst = bd

	// Check if we should use enhanced delete operations
	if b.shouldUseEnhancedDelete(backupName) {
		return b.removeBackupRemoteEnhanced(ctx, backupName, *bd, start)
	}

	// Fall back to original implementation
	return b.removeBackupRemoteOriginal(ctx, backupName, *bd, start)
}

// removeBackupRemoteEnhanced uses enhanced storage for optimized deletion
func (b *Backuper) removeBackupRemoteEnhanced(ctx context.Context, backupName string, bd storage.BackupDestination, start time.Time) error {
	// Validate configuration
	if err := b.validateDeleteOptimizationConfig(); err != nil {
		log.Warn().Err(err).Msg("invalid delete optimization config, falling back to original implementation")
		return b.removeBackupRemoteOriginal(ctx, backupName, bd, start)
	}

	// Create enhanced storage wrapper
	wrapperOpts := &enhanced.WrapperOptions{
		EnableCache:     false, // Cache functionality removed in simplified design
		EnableMetrics:   true,
		FallbackOnError: true,
	}

	enhancedStorage, err := enhanced.NewEnhancedStorageWrapper(b.dst, b.cfg, wrapperOpts)
	if err != nil {
		log.Warn().Err(err).Msg("failed to create enhanced storage, falling back to original implementation")
		return b.removeBackupRemoteOriginal(ctx, backupName, bd, start)
	}
	defer func() {
		if err := enhancedStorage.Close(ctx); err != nil {
			log.Warn().Err(err).Msg("error closing enhanced storage")
		}
	}()

	// Get backup list
	backupList, err := b.dst.BackupList(ctx, true, backupName)
	if err != nil {
		return err
	}

	for _, backup := range backupList {
		if backup.BackupName == backupName {
			// Clean embedded and object disks first
			err = b.cleanEmbeddedAndObjectDiskRemoteIfSameLocalNotPresent(ctx, backup)
			if err != nil {
				return err
			}

			// Use enhanced deletion
			err = enhancedStorage.EnhancedDeleteBackup(ctx, backupName)
			if err != nil {
				log.Warn().Err(err).Msg("enhanced delete failed, falling back to original implementation")
				return b.removeBackupRemoteOriginal(ctx, backupName, bd, start)
			}

			// Log enhanced metrics
			metrics := enhancedStorage.GetDeleteMetrics()
			b.logEnhancedDeleteMetrics(metrics, backupName)

			log.Info().Fields(map[string]interface{}{
				"backup":          backupName,
				"location":        "remote",
				"operation":       "delete",
				"duration":        utils.HumanizeDuration(time.Since(start)),
				"enhanced":        true,
				"files_processed": metrics.FilesProcessed,
				"files_deleted":   metrics.FilesDeleted,
				"throughput_mbps": metrics.ThroughputMBps,
			}).Msg("done")
			return nil
		}
	}
	return fmt.Errorf("'%s' is not found on remote storage", backupName)
}

// removeBackupRemoteOriginal uses the original deletion implementation
func (b *Backuper) removeBackupRemoteOriginal(ctx context.Context, backupName string, bd storage.BackupDestination, start time.Time) error {
	backupList, err := b.dst.BackupList(ctx, true, backupName)
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			err = b.cleanEmbeddedAndObjectDiskRemoteIfSameLocalNotPresent(ctx, backup)
			if err != nil {
				return err
			}

			if err = b.dst.RemoveBackupRemote(ctx, backup, b.cfg, b); err != nil {
				log.Warn().Msgf("bd.RemoveBackup return error: %v", err)
				return err
			}
			log.Info().Fields(map[string]interface{}{
				"backup":    backupName,
				"location":  "remote",
				"operation": "delete",
				"duration":  utils.HumanizeDuration(time.Since(start)),
				"enhanced":  false,
			}).Msg("done")
			return nil
		}
	}
	return fmt.Errorf("'%s' is not found on remote storage", backupName)
}

func (b *Backuper) cleanEmbeddedAndObjectDiskRemoteIfSameLocalNotPresent(ctx context.Context, backup storage.Backup) error {
	var skip bool
	var err error
	if skip, err = b.skipIfSameLocalBackupPresent(ctx, backup.BackupName, backup.Tags); err != nil {
		return err
	}
	if !skip {
		if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
			if err = b.cleanRemoteEmbedded(ctx, backup); err != nil {
				log.Warn().Msgf("b.cleanRemoteEmbedded return error: %v", err)
				return err
			}
			return nil
		}
		if b.hasObjectDisksRemote(backup) || (b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk == "") {
			startTime := time.Now()
			if deletedKeys, deleteErr := b.cleanBackupObjectDisks(ctx, backup.BackupName); deleteErr != nil {
				log.Warn().Msgf("b.cleanBackupObjectDisks return error: %v", deleteErr)
			} else {
				log.Info().Str("backup", backup.BackupName).Str("duration", utils.HumanizeDuration(time.Since(startTime))).Msgf("cleanBackupObjectDisks deleted %d keys", deletedKeys)
			}
			return nil
		}
	}
	return nil
}

func (b *Backuper) hasObjectDisksRemote(backup storage.Backup) bool {
	for _, diskType := range backup.DiskTypes {
		if b.isDiskTypeObject(diskType) {
			return true
		}
	}
	return false
}

func (b *Backuper) cleanRemoteEmbedded(ctx context.Context, backup storage.Backup) error {
	if err := object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, b.cfg.ClickHouse.EmbeddedBackupDisk); err != nil {
		return err
	}
	return b.dst.Walk(ctx, backup.BackupName+"/", true, func(ctx context.Context, f storage.RemoteFile) error {
		if !strings.HasSuffix(f.Name(), ".json") {
			r, err := b.dst.GetFileReader(ctx, path.Join(backup.BackupName, f.Name()))
			if err != nil {
				return err
			}
			log.Debug().Msgf("object_disk.ReadMetadataFromReader(%s)", f.Name())
			meta, err := object_disk.ReadMetadataFromReader(r, f.Name())
			if err != nil {
				return err
			}
			for _, o := range meta.StorageObjects {
				if err = object_disk.DeleteFile(ctx, b.cfg.ClickHouse.EmbeddedBackupDisk, o.ObjectRelativePath); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// cleanBackupObjectDisks - recursive delete <object_disks_path>/<backupName>
func (b *Backuper) cleanBackupObjectDisks(ctx context.Context, backupName string) (uint, error) {
	objectDiskPath, err := b.getObjectDiskPath()
	if err != nil {
		return 0, err
	}

	// Check if we should use enhanced delete for object disks
	if b.shouldUseEnhancedDelete(backupName) {
		return b.cleanBackupObjectDisksEnhanced(ctx, backupName, objectDiskPath)
	}

	// Fall back to original implementation
	return b.cleanBackupObjectDisksOriginal(ctx, backupName, objectDiskPath)
}

// cleanBackupObjectDisksEnhanced uses enhanced batch operations for object disk cleanup
func (b *Backuper) cleanBackupObjectDisksEnhanced(ctx context.Context, backupName, objectDiskPath string) (uint, error) {
	// Create enhanced storage wrapper
	wrapperOpts := &enhanced.WrapperOptions{
		EnableCache:     false, // Cache functionality removed in simplified design
		EnableMetrics:   true,
		FallbackOnError: true,
	}

	enhancedStorage, err := enhanced.NewEnhancedStorageWrapper(b.dst, b.cfg, wrapperOpts)
	if err != nil {
		log.Debug().Err(err).Msg("failed to create enhanced storage for object disk cleanup, using original method")
		return b.cleanBackupObjectDisksOriginal(ctx, backupName, objectDiskPath)
	}
	defer func() {
		if err := enhancedStorage.Close(ctx); err != nil {
			log.Debug().Err(err).Msg("error closing enhanced storage for object disk cleanup")
		}
	}()

	// Collect all file keys to delete
	var filesToDelete []string
	walkErr := b.dst.WalkAbsolute(ctx, path.Join(objectDiskPath, backupName), true, func(ctx context.Context, f storage.RemoteFile) error {
		if b.dst.Kind() == "azblob" {
			if f.Size() > 0 || !f.LastModified().IsZero() {
				filesToDelete = append(filesToDelete, path.Join(backupName, f.Name()))
			}
		} else {
			filesToDelete = append(filesToDelete, path.Join(backupName, f.Name()))
		}
		return nil
	})

	if walkErr != nil {
		return 0, walkErr
	}

	if len(filesToDelete) == 0 {
		return 0, nil
	}

	log.Info().
		Str("backup", backupName).
		Int("files_to_delete", len(filesToDelete)).
		Msg("starting enhanced object disk cleanup")

	// Use batch delete if supported
	if enhancedStorage.SupportsBatchDelete() {
		batchSize := enhancedStorage.GetOptimalBatchSize()
		deletedCount := uint(0)

		for i := 0; i < len(filesToDelete); i += batchSize {
			end := i + batchSize
			if end > len(filesToDelete) {
				end = len(filesToDelete)
			}

			batch := filesToDelete[i:end]
			result, err := enhancedStorage.DeleteBatch(ctx, batch)
			if err != nil {
				log.Warn().Err(err).Msg("batch delete failed, falling back to original method")
				return b.cleanBackupObjectDisksOriginal(ctx, backupName, objectDiskPath)
			}

			deletedCount += uint(result.SuccessCount)

			if len(result.FailedKeys) > 0 {
				log.Warn().
					Int("failed_count", len(result.FailedKeys)).
					Msg("some files failed to delete in batch")
			}
		}

		log.Info().
			Str("backup", backupName).
			Uint("deleted_count", deletedCount).
			Msg("enhanced object disk cleanup completed")

		return deletedCount, nil
	}

	// If batch delete not supported, fall back to original method
	return b.cleanBackupObjectDisksOriginal(ctx, backupName, objectDiskPath)
}

// cleanBackupObjectDisksOriginal uses the original deletion implementation
func (b *Backuper) cleanBackupObjectDisksOriginal(ctx context.Context, backupName, objectDiskPath string) (uint, error) {
	//walk absolute path, delete relative
	deletedKeys := uint(0)
	walkErr := b.dst.WalkAbsolute(ctx, path.Join(objectDiskPath, backupName), true, func(ctx context.Context, f storage.RemoteFile) error {
		if b.dst.Kind() == "azblob" {
			if f.Size() > 0 || !f.LastModified().IsZero() {
				deletedKeys += 1
				return b.dst.DeleteFileFromObjectDiskBackup(ctx, path.Join(backupName, f.Name()))
			} else {
				return nil
			}
		}
		deletedKeys += 1
		return b.dst.DeleteFileFromObjectDiskBackup(ctx, path.Join(backupName, f.Name()))
	})
	return deletedKeys, walkErr
}

func (b *Backuper) skipIfSameLocalBackupPresent(ctx context.Context, backupName, tags string) (bool, error) {
	if localList, _, err := b.GetLocalBackups(ctx, nil); err != nil {
		return true, err
	} else {
		for _, localBackup := range localList {
			if localBackup.BackupName == backupName && strings.Contains(localBackup.Tags, tags) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (b *Backuper) CleanLocalBroken(commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	localBackups, _, err := b.GetLocalBackups(ctx, nil)
	if err != nil {
		return err
	}
	for _, backup := range localBackups {
		if backup.Broken != "" {
			if err = b.RemoveBackupLocal(ctx, backup.BackupName, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Backuper) CleanRemoteBroken(commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	remoteBackups, err := b.GetRemoteBackups(ctx, true)
	if err != nil {
		return err
	}
	for _, backup := range remoteBackups {
		if backup.Broken != "" {
			if err = b.RemoveBackupRemote(ctx, backup.BackupName); err != nil {
				return err
			}
		}
	}
	return nil
}

// isDeleteOptimizationEnabled checks if delete optimizations are enabled
func (b *Backuper) isDeleteOptimizationEnabled() bool {
	return b.cfg.General.BatchDeletion.Enabled
}

// shouldUseEnhancedDelete determines if enhanced delete should be used for the given backup
func (b *Backuper) shouldUseEnhancedDelete(backupName string) bool {
	if !b.isDeleteOptimizationEnabled() {
		log.Debug().Str("backup", backupName).Msg("delete optimizations disabled")
		return false
	}

	// Check if remote storage supports batch operations
	if !b.supportsEnhancedDelete() {
		log.Debug().Str("backup", backupName).Str("storage", b.cfg.General.RemoteStorage).
			Msg("remote storage does not support enhanced delete operations")
		return false
	}

	log.Debug().Str("backup", backupName).Msg("using enhanced delete operations")
	return true
}

// supportsEnhancedDelete checks if the current remote storage supports enhanced delete operations
func (b *Backuper) supportsEnhancedDelete() bool {
	switch b.cfg.General.RemoteStorage {
	case "s3":
		return b.cfg.S3.BatchDeletion.UseBatchAPI
	case "gcs":
		return b.cfg.GCS.BatchDeletion.UseClientPool
	case "azblob":
		return b.cfg.AzureBlob.BatchDeletion.UseBatchAPI
	case "none", "custom":
		return false
	default:
		// For other storage types (ftp, sftp, cos), enhanced delete may still provide benefits
		// through parallel workers and caching, even without native batch APIs
		return true
	}
}

// getOptimalWorkerCount determines the optimal number of workers for delete operations
func (b *Backuper) getOptimalWorkerCount() int {
	if !b.isDeleteOptimizationEnabled() {
		return 1
	}

	workers := b.cfg.General.BatchDeletion.Workers
	if workers <= 0 {
		// Auto-detect based on storage type and system resources
		switch b.cfg.General.RemoteStorage {
		case "s3":
			return b.cfg.S3.BatchDeletion.VersionConcurrency
		case "gcs":
			return b.cfg.GCS.BatchDeletion.MaxWorkers
		case "azblob":
			return b.cfg.AzureBlob.BatchDeletion.MaxWorkers
		default:
			// Default to number of CPU cores for other storage types
			return maxInt(1, int(b.cfg.General.DownloadConcurrency))
		}
	}

	return maxInt(1, workers)
}

// getOptimalBatchSize determines the optimal batch size for delete operations
func (b *Backuper) getOptimalBatchSize() int {
	if !b.isDeleteOptimizationEnabled() {
		return 1
	}

	batchSize := b.cfg.General.BatchDeletion.BatchSize
	if batchSize <= 0 {
		return 1000 // Default batch size
	}

	return batchSize
}

// createEnhancedDeleteMetrics creates metrics tracking for enhanced delete operations
func (b *Backuper) createEnhancedDeleteMetrics() *enhanced.DeleteMetrics {
	return &enhanced.DeleteMetrics{
		FilesProcessed: 0,
		FilesDeleted:   0,
		FilesFailed:    0,
		BytesDeleted:   0,
		APICallsCount:  0,
		TotalDuration:  0,
		ThroughputMBps: 0.0,
	}
}

// logEnhancedDeleteMetrics logs the metrics from enhanced delete operations
func (b *Backuper) logEnhancedDeleteMetrics(metrics *enhanced.DeleteMetrics, backupName string) {
	if metrics == nil {
		return
	}

	log.Info().
		Str("backup", backupName).
		Int64("files_processed", metrics.FilesProcessed).
		Int64("files_deleted", metrics.FilesDeleted).
		Int64("files_failed", metrics.FilesFailed).
		Int64("bytes_deleted", metrics.BytesDeleted).
		Int64("api_calls", metrics.APICallsCount).
		Str("duration", metrics.TotalDuration.String()).
		Float64("throughput_mbps", metrics.ThroughputMBps).
		Msg("enhanced delete operation completed")
}

// validateDeleteOptimizationConfig validates the delete optimization configuration
func (b *Backuper) validateDeleteOptimizationConfig() error {
	if !b.cfg.General.BatchDeletion.Enabled {
		return nil
	}

	// Validate batch size
	if b.cfg.General.BatchDeletion.BatchSize < 1 {
		return &enhanced.OptimizationConfigError{
			Field:   "batch_size",
			Value:   b.cfg.General.BatchDeletion.BatchSize,
			Message: "batch size must be greater than 0",
		}
	}

	// Validate retry attempts
	if b.cfg.General.BatchDeletion.RetryAttempts < 0 {
		return &enhanced.OptimizationConfigError{
			Field:   "retry_attempts",
			Value:   b.cfg.General.BatchDeletion.RetryAttempts,
			Message: "retry attempts cannot be negative",
		}
	}

	// Validate failure threshold
	if b.cfg.General.BatchDeletion.FailureThreshold < 0 || b.cfg.General.BatchDeletion.FailureThreshold > 1 {
		return &enhanced.OptimizationConfigError{
			Field:   "failure_threshold",
			Value:   b.cfg.General.BatchDeletion.FailureThreshold,
			Message: "failure threshold must be between 0 and 1",
		}
	}

	// Validate error strategy
	validStrategies := map[string]bool{
		"fail_fast":   true,
		"continue":    true,
		"retry_batch": true,
	}
	if !validStrategies[b.cfg.General.BatchDeletion.ErrorStrategy] {
		return &enhanced.OptimizationConfigError{
			Field:   "error_strategy",
			Value:   b.cfg.General.BatchDeletion.ErrorStrategy,
			Message: "error strategy must be one of: fail_fast, continue, retry_batch",
		}
	}

	return nil
}

// maxInt returns the maximum of two integers
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (b *Backuper) cleanPartialRequiredBackup(ctx context.Context, disks []clickhouse.Disk, currentBackupName string) error {
	if localBackups, _, err := b.GetLocalBackups(ctx, disks); err == nil {
		for _, localBackup := range localBackups {
			if localBackup.BackupName != currentBackupName && localBackup.DataSize+localBackup.CompressedSize+localBackup.MetadataSize+localBackup.RBACSize == 0 {
				if err = b.RemoveBackupLocal(ctx, localBackup.BackupName, disks); err != nil {
					return fmt.Errorf("CleanPartialRequiredBackups %s -> RemoveBackupLocal cleaning error: %v", localBackup.BackupName, err)
				} else {
					log.Info().Msgf("CleanPartialRequiredBackups %s deleted", localBackup.BackupName)
				}
			}
		}
	} else {
		return fmt.Errorf("CleanPartialRequiredBackups -> GetLocalBackups cleaning error: %v", err)
	}
	return nil
}
