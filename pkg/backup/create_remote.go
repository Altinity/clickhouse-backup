package backup

import (
	"context"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
)

func (b *Backuper) CreateToRemote(backupName string, deleteSource bool, diffFrom, diffFromRemote, tablePattern string, partitions, skipProjections []string, schemaOnly, backupRBAC, rbacOnly, backupConfigs, configsOnly, namedCollections, namedCollectionsOnly, skipCheckPartsColumns, resume, streaming bool, version string, commandId int) error {
	// don't need to create pid separately because we combine Create+Upload
	defer pidlock.RemovePidFile(backupName)
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return errors.Wrap(err, "CreateToRemote GetContextWithCancel")
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	if backupName == "" {
		backupName = NewBackupName()
	}
	if streaming {
		if err = b.validateStreamingParams(); err != nil {
			return err
		}
		// dry-run creates nothing, so there is nothing to stream, the regular create dry-run report is enough
		if !b.DryRun {
			return b.createToRemoteStreaming(ctx, backupName, diffFrom, diffFromRemote, tablePattern, partitions, skipProjections, schemaOnly, backupRBAC, rbacOnly, backupConfigs, configsOnly, namedCollections, namedCollectionsOnly, skipCheckPartsColumns, resume, version)
		}
	}
	if createErr := b.CreateBackup(backupName, diffFromRemote, tablePattern, partitions, schemaOnly, backupRBAC, rbacOnly, backupConfigs, configsOnly, namedCollections, namedCollectionsOnly, skipCheckPartsColumns, skipProjections, resume, version, commandId); createErr != nil {
		return createErr
	}
	pidlock.RemovePidFile(backupName)
	// under dry-run CreateBackup didn't create anything, so there is nothing for Upload to read
	if b.DryRun {
		if b.DryRunResult != nil {
			b.DryRunResult.Command = "create_remote"
		}
		return nil
	}
	if uploadErr := b.Upload(backupName, deleteSource, diffFrom, diffFromRemote, tablePattern, partitions, skipProjections, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, resume, version, commandId); uploadErr != nil {
		return uploadErr
	}

	return nil
}

// validateStreamingParams rejects configurations where per-table upload can't work, https://github.com/Altinity/clickhouse-backup/issues/780
func (b *Backuper) validateStreamingParams() error {
	if b.cfg.ClickHouse.UseEmbeddedBackupRestore {
		return errors.New("--streaming is not supported with `use_embedded_backup_restore: true`, BACKUP SQL produces the whole backup at once")
	}
	if b.cfg.General.RemoteStorage == "custom" || b.cfg.General.RemoteStorage == "none" {
		return errors.Errorf("--streaming is not supported with `remote_storage: %s`", b.cfg.General.RemoteStorage)
	}
	return nil
}

// createToRemoteStreaming runs the create prologue, then pipelines freeze table -> upload table -> delete local table copy,
// so only a small local footprint is required, https://github.com/Altinity/clickhouse-backup/issues/780
func (b *Backuper) createToRemoteStreaming(ctx context.Context, backupName, diffFrom, diffFromRemote, tablePattern string, partitions, skipProjections []string, schemaOnly, createRBAC, rbacOnly, createConfigs, configsOnly, createNamedCollections, namedCollectionsOnly, skipCheckPartsColumns, resume bool, backupVersion string) error {
	if pidCheckErr := pidlock.CheckAndCreatePidFile(backupName, "create_remote"); pidCheckErr != nil {
		return pidCheckErr
	}
	startBackup := time.Now()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")

	if err := b.ch.Connect(); err != nil {
		return errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()

	clickHouseVersion, versionErr := b.ch.GetVersion(ctx)
	if versionErr != nil {
		return errors.Wrap(versionErr, "b.ch.GetVersion")
	}
	if clickHouseVersion < 24003000 && len(skipProjections) > 0 {
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
	if err := b.validateUploadParams(ctx, backupName, diffFrom, diffFromRemote); err != nil {
		return errors.Wrap(err, "validateUploadParams")
	}

	p, err := b.createPrologue(ctx, tablePattern, partitions, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly)
	if err != nil {
		return err
	}
	backupRBACSize, backupConfigSize, backupNamedCollectionsSize, rbacConfigsNamedCollectionsErr := b.createConfigsNamedCollectionsAndRBACIfNecessary(ctx, backupName, createRBAC, rbacOnly, createConfigs, configsOnly, createNamedCollections, namedCollectionsOnly, p.disks, p.diskMap)
	if rbacConfigsNamedCollectionsErr != nil {
		return errors.Wrap(rbacConfigsNamedCollectionsErr, "createConfigsNamedCollectionsAndRBACIfNecessary")
	}
	err = b.createAndUploadStreaming(ctx, backupName, diffFrom, diffFromRemote, tablePattern, partitions, skipProjections, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, backupVersion, p, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, startBackup, clickHouseVersion)
	if err != nil {
		log.Error().Msgf("streaming backup failed error: %v", err)
		// the partial local backup and the tables already uploaded to remote are kept intentionally,
		// `--resume` continues both, otherwise delete the backup locally and remotely before retry
		// fix https://github.com/Altinity/clickhouse-backup/issues/1345 only clean shadow UUIDs created by this backup, don't touch other shadows
		if cleanShadowErr := b.CleanShadowUUIDs(p.disks); cleanShadowErr != nil {
			log.Error().Msgf("streaming backup failed -> b.CleanShadowUUIDs error: %v", cleanShadowErr)
		}
		return errors.Wrapf(err, "create_remote --streaming failed, partial backup '%s' is kept locally and on remote storage, use --resume to continue or delete it from both", backupName)
	}

	// fix https://github.com/Altinity/clickhouse-backup/issues/1345 clean only shadow UUIDs created by this backup
	if cleanShadowErr := b.CleanShadowUUIDs(p.disks); cleanShadowErr != nil {
		log.Warn().Msgf("b.CleanShadowUUIDs error: %v", cleanShadowErr)
	}
	if err := b.RemoveOldBackupsLocal(ctx, true, p.disks); err != nil {
		return errors.Wrap(err, "b.RemoveOldBackupsLocal")
	}
	return nil
}

// createAndUploadStreaming writes an early backup-level metadata.json, so the upload prologue can find the local backup,
// then runs createOneTable producers (limited by clickhouse.max_connections) feeding uploadOneTable consumers
// (limited by general.upload_concurrency) through a channel, each uploaded table is removed locally right away,
// finally rewrites metadata.json with real sizes and runs uploadEpilogue with deleteSource=true
func (b *Backuper) createAndUploadStreaming(ctx context.Context, backupName, diffFrom, diffFromRemote, tablePattern string, partitions, skipProjections []string, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly bool, backupVersion string, p *createPrologueResult, backupRBACSize, backupConfigSize, backupNamedCollectionsSize uint64, startBackup time.Time, version int) error {
	backupPath, err := b.createBackupDirs(backupName, p.disks)
	if err != nil {
		return err
	}
	if err = b.validateObjectDiskConfigIfNecessary(b.isObjectDiskContainsTables(p.tables, p.disks, p.doBackupData)); err != nil {
		return err
	}
	// metadata dir shall exist before uploadPrologue walks it
	if err = filesystemhelper.Mkdir(path.Join(backupPath, "metadata"), b.ch, p.disks); err != nil {
		return errors.Wrap(err, "filesystemhelper.Mkdir metadataPath")
	}
	tableMetas := make([]metadata.TableTitle, 0, len(p.tables))
	if schemaOnly || p.doBackupData {
		for _, table := range p.tables {
			if !table.Skip {
				tableMetas = append(tableMetas, metadata.TableTitle{Database: table.Database, Table: table.Name})
			}
		}
	}
	backupMetaFile := path.Join(backupPath, "metadata.json")
	if err = b.createBackupMetadata(ctx, backupMetaFile, backupName, diffFromRemote, backupVersion, "regular", p.diskMap, p.diskTypes, p.disks, 0, 0, 0, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, tableMetas, p.allDatabases, p.allFunctions); err != nil {
		return errors.Wrap(err, "createBackupMetadata return error")
	}

	// uploadPrologue connects b.dst, it is shared with object disk copy inside createOneTable
	prologue, err := b.uploadPrologue(ctx, backupName, diffFrom, diffFromRemote, tablePattern, partitions, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := b.dst.Close(ctx); closeErr != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", closeErr)
		}
	}()
	// one `create_upload_streaming.state2` is shared by object disk copy (create) and data upload, their keys don't overlap:
	// create records `<bucket>/<object key>`, upload records `<backupName>/<remote path>`
	if err = b.uploadInitResumableAndManifest(backupName, "create_upload_streaming", diffFrom, diffFromRemote, tablePattern, partitions, schemaOnly, prologue.backupExistsOnRemote); err != nil {
		return err
	}
	if b.resume {
		defer b.resumableState.Close()
	}
	defer func() {
		b.fileManifest.Close()
		b.fileManifest = nil
	}()

	var tablesDiffFromRemote map[metadata.TableTitle]metadata.TableMetadata
	if diffFromRemote != "" {
		tablesDiffFromRemote = prologue.tablesForUploadFromDiff
	}
	allInProgressMutations, err := b.checkPartsColumnsAndGetInProgressMutations(ctx, p.tables, p.doBackupData, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly)
	if err != nil {
		return err
	}
	checkLocalPart := diffFrom != "" && diffFromRemote == ""

	var backupDataSize, backupObjectDiskSize, backupMetadataSize uint64
	var brokenParts, totalParts int64
	var compressedDataSize, uploadedMetadataSize, uploadedTables int64
	var tablesForUploadMutex sync.Mutex
	tablesForUpload := ListOfTables{}
	nonSkipTables := b.CalculateNonSkipTables(p.tables)

	// upload error cancels uploadCtx and createCtx derived from it, create error cancels pipelineCtx explicitly below
	pipelineCtx, pipelineCancel := context.WithCancel(ctx)
	defer pipelineCancel()
	uploadGroup, uploadCtx := errgroup.WithContext(pipelineCtx)
	uploadGroup.SetLimit(max(int(b.cfg.General.UploadConcurrency), 1))
	createGroup, createCtx := errgroup.WithContext(uploadCtx)
	createGroup.SetLimit(max(b.cfg.ClickHouse.MaxConnections, 1))
	tablesToUpload := make(chan *metadata.TableMetadata, max(int(b.cfg.General.UploadConcurrency), 1))

	uploadDone := make(chan error, 1)
	go func() {
		for table := range tablesToUpload {
			table := table
			uploadGroup.Go(func() error {
				progress := fmt.Sprintf("%d/%d", atomic.AddInt64(&uploadedTables, 1), nonSkipTables)
				uploadedBytes, tableMetadataSize, uploadTableErr := b.uploadOneTable(uploadCtx, backupName, false, table, skipProjections, prologue.disks, prologue.backupMetadata, prologue.tablesForUploadFromDiff, checkLocalPart, p.doBackupData, schemaOnly, backupVersion, progress)
				if uploadTableErr != nil {
					return uploadTableErr
				}
				// per-table metadata/<db>/<table>.json stays local, RemoveBackupLocal in uploadEpilogue removes the rest
				dbAndTableDir := path.Join(common.TablePathEncode(table.Database), common.TablePathEncode(table.Table))
				if removeErr := b.removeTableLocal(backupName, dbAndTableDir, p.disks); removeErr != nil {
					return errors.Wrapf(removeErr, "removeTableLocal %s", dbAndTableDir)
				}
				atomic.AddInt64(&compressedDataSize, uploadedBytes)
				atomic.AddInt64(&uploadedMetadataSize, tableMetadataSize)
				tablesForUploadMutex.Lock()
				tablesForUpload = append(tablesForUpload, table)
				tablesForUploadMutex.Unlock()
				return nil
			})
		}
		uploadDone <- uploadGroup.Wait()
	}()

	for tableIdx, tableItem := range p.tables {
		//to avoid race condition
		table := tableItem
		if table.Skip {
			continue
		}
		idx := tableIdx
		createGroup.Go(func() error {
			tableMeta, tableBackupSize, createOneTableErr := b.createOneTable(createCtx, backupName, backupPath, &table, tablesDiffFromRemote, p.partitionsIdMap, allInProgressMutations, skipProjections, p.disks, p.doBackupData, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, version)
			if createOneTableErr != nil {
				return createOneTableErr
			}
			atomic.AddInt64(&brokenParts, tableBackupSize.brokenParts)
			atomic.AddInt64(&totalParts, tableBackupSize.totalParts)
			atomic.AddUint64(&backupDataSize, tableBackupSize.dataSize)
			atomic.AddUint64(&backupObjectDiskSize, tableBackupSize.objectDiskSize)
			atomic.AddUint64(&backupMetadataSize, tableBackupSize.metadataSize)
			log.Info().Str("table", fmt.Sprintf("%s.%s", table.Database, table.Name)).Str("progress", fmt.Sprintf("%d/%d", idx+1, len(p.tables))).Msg("done")
			if tableMeta == nil {
				return nil
			}
			select {
			case tablesToUpload <- tableMeta:
				return nil
			case <-createCtx.Done():
				return createCtx.Err()
			}
		})
	}
	createErr := createGroup.Wait()
	if createErr != nil {
		pipelineCancel()
	}
	close(tablesToUpload)
	uploadErr := <-uploadDone
	// the side which failed first cancels the other one, report the root cause instead of context.Canceled
	if createErr != nil && !errors.Is(createErr, context.Canceled) {
		return errors.Wrap(createErr, "one of createOneTable go-routine return error")
	}
	if uploadErr != nil {
		return errors.Wrap(uploadErr, "one of upload table go-routine return error")
	}
	if createErr != nil {
		return errors.Wrap(createErr, "one of createOneTable go-routine return error")
	}

	// tables already uploaded stay on remote storage when the ratio check fails, the returned error tells the user to delete the backup
	if err = b.checkMaxBrokenPartRatio(atomic.LoadInt64(&brokenParts), atomic.LoadInt64(&totalParts)); err != nil {
		return err
	}

	tablesForUpload.Sort(false)
	tableMetas = make([]metadata.TableTitle, len(tablesForUpload))
	for i := range tablesForUpload {
		tableMetas[i] = metadata.TableTitle{Database: tablesForUpload[i].Database, Table: tablesForUpload[i].Table}
	}
	if err = b.createBackupMetadata(ctx, backupMetaFile, backupName, diffFromRemote, backupVersion, "regular", p.diskMap, p.diskTypes, p.disks, backupDataSize, backupObjectDiskSize, backupMetadataSize, backupRBACSize, backupConfigSize, backupNamedCollectionsSize, tableMetas, p.allDatabases, p.allFunctions); err != nil {
		return errors.Wrap(err, "createBackupMetadata return error")
	}
	// prologue.backupMetadata was read from the early metadata.json with zero sizes
	prologue.backupMetadata.DataSize = backupDataSize
	prologue.backupMetadata.ObjectDiskSize = backupObjectDiskSize
	prologue.backupMetadata.MetadataSize = backupMetadataSize
	log.Info().Str("version", backupVersion).Str("operation", "createBackupLocal").Str("duration", utils.HumanizeDuration(time.Since(startBackup))).Msg("done")

	// deleteSource=true removes the local backup via RemoveBackupLocal, which skips object disk cleanup because the remote backup exists
	return b.uploadEpilogue(ctx, backupName, true, tablesForUpload, prologue.backupMetadata, prologue.disks, compressedDataSize, uploadedMetadataSize, p.doBackupData, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, backupVersion, startBackup)
}
