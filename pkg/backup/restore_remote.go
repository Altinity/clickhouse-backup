package backup

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
	"github.com/Altinity/clickhouse-backup/v2/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	pkgerrors "github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

func (b *Backuper) RestoreFromRemote(backupName, tablePattern string, databaseMapping, tableMapping, partitions, skipProjections []string, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, replicatedCopyToDetached, skipEmptyTables, hardlinkExistsFiles, streaming bool, version string, commandId int) error {
	// don't need to create pid separately because we combine Download+Restore
	defer pidlock.RemovePidFile(backupName)
	// dry-run has no side effects, so the download report is re-labeled the same way as without --streaming
	if streaming && !b.DryRun {
		err := b.restoreFromRemoteStreaming(backupName, tablePattern, databaseMapping, tableMapping, partitions, skipProjections, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, replicatedCopyToDetached, skipEmptyTables, hardlinkExistsFiles, version, commandId)
		// https://github.com/Altinity/clickhouse-backup/issues/625
		if !errors.Is(err, ErrBackupIsAlreadyExists) {
			return err
		}
		pidlock.RemovePidFile(backupName)
		return b.Restore(backupName, tablePattern, databaseMapping, tableMapping, partitions, skipProjections, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, replicatedCopyToDetached, skipEmptyTables, version, commandId)
	}
	if err := b.Download(backupName, tablePattern, partitions, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, resume, hardlinkExistsFiles, version, commandId); err != nil {
		// https://github.com/Altinity/clickhouse-backup/issues/625
		if !errors.Is(err, ErrBackupIsAlreadyExists) {
			return pkgerrors.Wrap(err, "RestoreFromRemote Download")
		}
	}
	pidlock.RemovePidFile(backupName)
	// the restored volume equals the volume the download would bring and nothing is materialized locally,
	// so the download report is re-labeled instead of being estimated twice,
	// https://github.com/Altinity/clickhouse-backup/issues/1012
	if b.DryRun && b.DryRunResult != nil {
		b.DryRunResult.Command = "restore_remote"
		b.setDryRunResult(b.DryRunResult)
		return nil
	}
	if err := b.Restore(backupName, tablePattern, databaseMapping, tableMapping, partitions, skipProjections, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, replicatedCopyToDetached, skipEmptyTables, version, commandId); err != nil {
		return err
	}
	// the backup already existed locally, so the download produced no report and the local restore
	// dry-run above gave the exact numbers
	if b.DryRun && b.DryRunResult != nil {
		b.DryRunResult.Command = "restore_remote"
		b.setDryRunResult(b.DryRunResult)
	}
	return nil
}

// streamingTable is one downloaded table handed from the download goroutines to the restore goroutines
type streamingTable struct {
	// downloaded carries the original names and the parts layout after download (hardlink re-balance included)
	downloaded *metadata.TableMetadata
	// restore carries the mapped names and the parts filtered by --partitions and existing disks
	restore  *metadata.TableMetadata
	progress string
}

// streamingRestoredStateKey marks a table whose data was attached and whose local copy was already removed,
// so a resumed run neither re-downloads nor re-attaches it
func streamingRestoredStateKey(backupName, dbAndTableDir string) string {
	return path.Join(backupName, "streaming_restored", dbAndTableDir)
}

// restoreFromRemoteStreaming downloads and restores table by table instead of downloading the whole backup first,
// each table is attached right after its download and its local shadow copy is removed, so only a few tables
// stay on local disks at once, https://github.com/Altinity/clickhouse-backup/issues/780
//
// Schema for all tables is restored before the data pipeline starts (restorePrologue), the download side
// keeps the regular local backup layout (metadata/<db>/<table>.json + shadow/<db>/<table>/<disk>) so the
// stage-1 download/restore functions are reused as is, the local backup is deleted at the end.
// Free space checks are kept unchanged: the attached data lands on the same disks by hardlinks, so the whole
// backup data size is still required, streaming only avoids keeping the local backup copy afterwards.
func (b *Backuper) restoreFromRemoteStreaming(backupName, tablePattern string, databaseMapping, tableMapping, partitions, skipProjections []string, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, replicatedCopyToDetached, skipEmptyTables, hardlinkExistsFiles bool, backupVersion string, commandId int) error {
	if b.cfg.ClickHouse.UseEmbeddedBackupRestore {
		return pkgerrors.New("restore_remote --streaming is not supported with `use_embedded_backup_restore: true`")
	}
	if b.cfg.General.RemoteStorage == "custom" || b.cfg.General.RemoteStorage == "none" {
		return pkgerrors.Errorf("restore_remote --streaming is not supported with `remote_storage: %s`", b.cfg.General.RemoteStorage)
	}
	if b.cfg.General.DownloadConcurrency == 0 {
		return pkgerrors.New("`download_concurrency` shall be more than zero")
	}
	if pidCheckErr := pidlock.CheckAndCreatePidFile(backupName, "restore_remote"); pidCheckErr != nil {
		return pkgerrors.Wrap(pidCheckErr, "CheckAndCreatePidFile")
	}
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return pkgerrors.Wrap(err, "GetContextWithCancel")
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")

	if err := b.prepareRestoreMapping(databaseMapping, "database"); err != nil {
		return pkgerrors.Wrap(err, "prepareRestoreMapping database")
	}
	if err := b.prepareRestoreMapping(tableMapping, "table"); err != nil {
		return pkgerrors.Wrap(err, "prepareRestoreMapping table")
	}
	doDownloadData := !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly
	doRestoreData := (!schemaOnly && !rbacOnly && !configsOnly) || dataOnly

	if err := b.ch.Connect(); err != nil {
		return pkgerrors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()
	b.adjustResumeFlag(resume)

	localBackups, disks, err := b.GetLocalBackups(ctx, nil)
	if err != nil {
		return pkgerrors.Wrap(err, "GetLocalBackups")
	}
	b.DefaultDataPath, err = b.ch.GetDefaultPath(disks)
	if err != nil {
		return pkgerrors.Wrap(err, "GetDefaultPath")
	}
	isResumeExists := false
	for i := range localBackups {
		if backupName == localBackups[i].BackupName {
			if strings.Contains(localBackups[i].Tags, "embedded") || !b.resume {
				return ErrBackupIsAlreadyExists
			}
			if resumeErr := b.resumeExistingBackup(backupName, "download_restore_streaming"); resumeErr != nil {
				return resumeErr
			}
			isResumeExists = true
		}
	}
	if err := b.initDisksPathsAndBackupDestination(ctx, disks, ""); err != nil {
		return pkgerrors.Wrap(err, "initDisksPathsAndBackupDestination")
	}
	if doDownloadData {
		if err := b.checkDisksConsistency(disks); err != nil {
			return err
		}
	}
	defer func() {
		if err := b.dst.Close(ctx); err != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", err)
		}
	}()

	remoteBackup, backupManifest, tablesForDownload, err := b.downloadRemoteBackupInfo(ctx, backupName, tablePattern, disks, isResumeExists, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, hardlinkExistsFiles)
	if err != nil {
		return err
	}
	defer backupManifest.Close()
	if strings.Contains(remoteBackup.Tags, "embedded") {
		return pkgerrors.Errorf("restore_remote --streaming is not supported for embedded backup '%s'", backupName)
	}

	if !schemaOnly && !b.cfg.General.DownloadByPart && remoteBackup.RequiredBackup != "" {
		// same as Download: without download_by_part the whole required backup is downloaded first, see issue #1384
		parentDst := b.dst
		err := b.Download(remoteBackup.RequiredBackup, tablePattern, partitions, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, b.resume, hardlinkExistsFiles, backupVersion, commandId)
		b.dst = parentDst
		if err != nil && !errors.Is(err, ErrBackupIsAlreadyExists) {
			return pkgerrors.Wrap(err, "download RequiredBackup")
		}
	}

	// one `download_restore_streaming.state2` holds the download keys, the streaming_restored markers and the
	// restore side object disk keys, restorePrologue reuses it below instead of opening `restore.state2`
	downloaded, err := b.downloadTablesMetadata(ctx, backupName, "download_restore_streaming", tablePattern, partitions, remoteBackup, tablesForDownload, disks, schemaOnly, resume, hardlinkExistsFiles, isResumeExists, doDownloadData)
	if err != nil {
		return err
	}
	if b.resume && dropExists {
		// --rm drops and recreates every table in restorePrologue, tables attached by an interrupted run are gone
		// together with their removed local copy, so the per-file download state must not skip them,
		// reset it the same way restorePrologue resets the restore state with needClean
		b.resumableState.Close()
		b.resumableState = resumable.NewState(b.GetStateDir(), backupName, "download_restore_streaming", map[string]interface{}{
			"tablePattern": tablePattern,
			"partitions":   partitions,
			"schemaOnly":   schemaOnly,
			"needClean":    fmt.Sprintf("true.%d", rand.Uint64()),
		})
	}
	closeResumableState := func() {
		if b.resume && b.resumableState != nil {
			b.resumableState.Close()
			b.resumableState = nil
		}
	}
	defer closeResumableState()
	if b.localPartIndex != nil {
		defer func() {
			b.localPartIndex = nil
		}()
	}
	tablesForDownload = downloaded.tablesForDownload

	rbacSize, configSize, namedCollectionsSize, err := b.downloadBackupRelatedData(ctx, remoteBackup, rbacOnly, configsOnly, namedCollectionsOnly)
	if err != nil {
		return err
	}
	// local metadata.json is written before any data arrives with the remote sizes, so the local backup is complete
	// from the GetLocalBackups point of view while tables stream through, required parts resolution and
	// RemoveBackupLocal need it
	if _, err = b.saveLocalBackupMetadata(backupName, remoteBackup.BackupMetadata, tablesForDownload, remoteBackup.DataSize, downloaded.metadataSize, rbacSize, configSize, namedCollectionsSize, backupVersion); err != nil {
		return err
	}

	// restorePrologue may open its own BackupDestination, it is replaced by the download one already opened above,
	// the resumable state is reused as is (reuseResumableState=true)
	downloadDst := b.dst
	prologue, err := b.restorePrologue(ctx, backupName, tablePattern, partitions, skipProjections, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, skipEmptyTables, doRestoreData, true)
	if err != nil {
		b.dst = downloadDst
		return err
	}
	if prologue.closeDst {
		if closeErr := b.dst.Close(ctx); closeErr != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", closeErr)
		}
	}
	b.dst = downloadDst

	dataSize := uint64(0)
	if !prologue.done && doRestoreData && doDownloadData {
		if len(prologue.tablesForRestore) == 0 {
			if !b.cfg.General.AllowEmptyBackups {
				return pkgerrors.Errorf("not found schemas schemas by %s in %s", prologue.tablePattern, backupName)
			}
			log.Warn().Msgf("not found schemas by %s in %s", prologue.tablePattern, backupName)
		} else {
			dataSize, err = b.streamTablesData(ctx, backupName, remoteBackup.BackupMetadata, backupManifest, downloaded.tableMetadataAfterDownload, prologue, skipProjections, replicatedCopyToDetached, hardlinkExistsFiles)
			if err != nil {
				return err
			}
		}
	}
	if !prologue.done && (schemaOnly || (schemaOnly == dataOnly)) {
		if funcErr := b.restoreFunctions(ctx, prologue.backupMetadata); funcErr != nil {
			return pkgerrors.Wrap(funcErr, "restoreFunctions")
		}
	}
	// RemoveBackupLocal replaces and closes b.dst when object disks present, so the download destination is put back
	closeResumableState()
	parentDst := b.dst
	defer func() {
		b.dst = parentDst
	}()
	//clean partially downloaded requiredBackup
	if remoteBackup.RequiredBackup != "" {
		if err = b.cleanPartialRequiredBackup(ctx, disks, backupName); err != nil {
			return pkgerrors.Wrap(err, "cleanPartialRequiredBackup")
		}
	}
	// the local backup holds only metadata now, RemoveBackupLocal keeps the object disk data because the same
	// backup exists on remote storage
	if err = b.RemoveBackupLocal(ctx, backupName, disks, true); err != nil {
		return pkgerrors.Wrap(err, "RemoveBackupLocal")
	}
	log.Info().Fields(map[string]interface{}{
		"backup":        backupName,
		"operation":     "restore_remote --streaming",
		"duration":      utils.HumanizeDuration(time.Since(start)),
		"download_size": utils.FormatBytes(dataSize + downloaded.metadataSize + rbacSize + configSize + namedCollectionsSize),
		"version":       backupVersion,
	}).Msg("done")
	return nil
}

// streamTablesData runs the download -> attach -> remove local copy pipeline, downloads run with
// download_concurrency, restores with max_connections, a failure on either side cancels the other,
// returns the downloaded data size
func (b *Backuper) streamTablesData(ctx context.Context, backupName string, remoteBackup metadata.BackupMetadata, backupManifest *storage.ManifestReader, downloadedTables ListOfTables, prologue *restorePrologueResult, skipProjections []string, replicatedCopyToDetached, hardlinkExistsFiles bool) (uint64, error) {
	tablesForRestore := prologue.tablesForRestore
	disks := prologue.disks
	tablesToRewriteKeys, dstTablesMap, err := b.restoreDataRegularPrepare(ctx, prologue.tablePattern, tablesForRestore, disks, prologue.existingTablesSnapshot)
	if err != nil {
		return 0, err
	}
	diskMap, diskTypes := buildDiskMaps(disks, prologue.backupMetadata)
	// tablesForRestore carries mapped names, downloadedTables the original ones
	restoreByOrigName := make(map[metadata.TableTitle]*metadata.TableMetadata, len(tablesForRestore))
	for _, t := range tablesForRestore {
		origDatabase, origTable := b.resolveOrigTableNames(*t)
		restoreByOrigName[metadata.TableTitle{Database: origDatabase, Table: origTable}] = t
	}

	restoreConcurrency := max(b.cfg.ClickHouse.MaxConnections, 1)
	pipelineCtx, cancelPipeline := context.WithCancel(ctx)
	defer cancelPipeline()
	downloadGroup, downloadCtx := errgroup.WithContext(pipelineCtx)
	downloadGroup.SetLimit(int(b.cfg.General.DownloadConcurrency))
	restoreGroup, restoreCtx := errgroup.WithContext(pipelineCtx)
	restoreGroup.SetLimit(restoreConcurrency)
	tablesCh := make(chan streamingTable, restoreConcurrency)
	dataSize := uint64(0)
	total := len(downloadedTables)

	downloadDone := make(chan error, 1)
	go func() {
		defer close(tablesCh)
		var stateErr error
		for i, tableMetadata := range downloadedTables {
			if tableMetadata == nil || tableMetadata.MetadataOnly {
				continue
			}
			progress := fmt.Sprintf("%d/%d", i+1, total)
			tableName := fmt.Sprintf("%s.%s", tableMetadata.Database, tableMetadata.Table)
			restoreTable, selected := restoreByOrigName[metadata.TableTitle{Database: tableMetadata.Database, Table: tableMetadata.Table}]
			if !selected {
				log.Debug().Str("backup_name", backupName).Str("table", tableName).Msg("table is not selected for restore, skip download")
				continue
			}
			dbAndTableDir := path.Join(common.TablePathEncode(tableMetadata.Database), common.TablePathEncode(tableMetadata.Table))
			if b.resume {
				isRestored, _, resumeErr := b.resumableState.IsAlreadyProcessed(streamingRestoredStateKey(backupName, dbAndTableDir))
				if resumeErr != nil {
					stateErr = pkgerrors.Wrap(resumeErr, "resumableState.IsAlreadyProcessed")
					cancelPipeline()
					break
				}
				if isRestored {
					log.Info().Str("backup_name", backupName).Str("table", tableName).Str("progress", progress).Msg("table already restored by previous run, skip")
					continue
				}
			}
			downloadedTable := tableMetadata
			downloadGroup.Go(func() error {
				tableStart := time.Now()
				log.Info().Str("backup_name", backupName).Str("table", tableName).Str("progress", progress).Msg("download table start")
				downloadDataSize, downloadDataErr := b.downloadTableData(downloadCtx, remoteBackup, *downloadedTable, disks, hardlinkExistsFiles, backupManifest)
				if downloadDataErr != nil {
					return pkgerrors.Wrap(downloadDataErr, "downloadTableData")
				}
				atomic.AddUint64(&dataSize, downloadDataSize)
				log.Info().Str("backup_name", backupName).Str("table", tableName).Str("progress", progress).Str("duration", utils.HumanizeDuration(time.Since(tableStart))).Str("size", utils.FormatBytes(downloadDataSize)).Msg("download table finish")
				select {
				case tablesCh <- streamingTable{downloaded: downloadedTable, restore: restoreTable, progress: progress}:
					return nil
				case <-downloadCtx.Done():
					return downloadCtx.Err()
				}
			})
		}
		// wait for the already started downloads before closing tablesCh, they send into it
		waitErr := downloadGroup.Wait()
		if stateErr != nil {
			waitErr = stateErr
		}
		downloadDone <- waitErr
	}()

	for st := range tablesCh {
		if restoreCtx.Err() != nil {
			// drain after a restore failure, downloads are already cancelled through pipelineCtx
			continue
		}
		restoreGroup.Go(func() error {
			if hardlinkExistsFiles {
				syncRebalancedDisks(st.restore, st.downloaded)
			}
			if restoreErr := b.restoreOneTable(restoreCtx, backupName, prologue.backupMetadata, *st.restore, dstTablesMap, tablesToRewriteKeys, diskMap, diskTypes, disks, skipProjections, replicatedCopyToDetached, st.progress); restoreErr != nil {
				cancelPipeline()
				return pkgerrors.Wrap(restoreErr, "restoreOneTable")
			}
			dbAndTableDir := path.Join(common.TablePathEncode(st.downloaded.Database), common.TablePathEncode(st.downloaded.Table))
			if removeErr := b.removeTableLocal(backupName, dbAndTableDir, disks); removeErr != nil {
				cancelPipeline()
				return pkgerrors.Wrap(removeErr, "removeTableLocal")
			}
			if b.resume {
				if appendErr := b.resumableState.AppendToState(streamingRestoredStateKey(backupName, dbAndTableDir), 0); appendErr != nil {
					cancelPipeline()
					return pkgerrors.Wrap(appendErr, "resumableState.AppendToState")
				}
			}
			return nil
		})
	}
	downloadErr := <-downloadDone
	restoreErr := restoreGroup.Wait()
	if restoreErr != nil {
		return dataSize, pkgerrors.Wrap(restoreErr, "one of restore go-routine return error")
	}
	if downloadErr != nil {
		return dataSize, pkgerrors.Wrap(downloadErr, "one of download go-routine return error")
	}
	return dataSize, nil
}

// syncRebalancedDisks copies RebalancedDisk assigned by hardlinkIfLocalPartExistsAndChecksumEqual during download
// into the restore side metadata, which was read from the local json before the download rewrote it
func syncRebalancedDisks(restore *metadata.TableMetadata, downloaded *metadata.TableMetadata) {
	for disk, parts := range restore.Parts {
		downloadedByName := make(map[string]string, len(downloaded.Parts[disk]))
		for _, part := range downloaded.Parts[disk] {
			downloadedByName[part.Name] = part.RebalancedDisk
		}
		for i := range parts {
			if rebalancedDisk, found := downloadedByName[parts[i].Name]; found {
				parts[i].RebalancedDisk = rebalancedDisk
			}
		}
	}
}

func buildDiskMaps(disks []clickhouse.Disk, backupMetadata metadata.BackupMetadata) (diskMap, diskTypes map[string]string) {
	diskMap = make(map[string]string, len(disks))
	diskTypes = make(map[string]string, len(disks))
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
		diskTypes[disk.Name] = disk.Type
	}
	for diskName := range backupMetadata.DiskTypes {
		if _, exists := diskTypes[diskName]; !exists {
			diskTypes[diskName] = backupMetadata.DiskTypes[diskName]
		}
	}
	return diskMap, diskTypes
}
