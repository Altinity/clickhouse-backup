package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/casstorage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/rs/zerolog/log"
)

// setupCASContext mirrors the v1 Upload context-setup pattern (status correlator
// + WithCancel). On commandId == status.NotFromAPI (-1) it returns a fresh
// background context.
func (b *Backuper) setupCASContext(commandId int) (context.Context, context.CancelFunc, error) {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return nil, nil, fmt.Errorf("cas: GetContextWithCancel: %w", err)
	}
	ctx, cancel = context.WithCancel(ctx)
	return ctx, cancel, nil
}

// ensureCAS opens a remote BackupDestination for CAS operations and returns the
// adapter wrapping it plus a closer. Caller MUST invoke closer when done.
//
// Returns an error if cas.enabled is false or the config fails validation.
func (b *Backuper) ensureCAS(ctx context.Context, backupName string) (cas.Backend, func(), error) {
	if !b.cfg.CAS.Enabled {
		return nil, func() {}, errors.New("cas: cas.enabled=false in config; cannot run cas-* commands")
	}
	if err := b.cfg.CAS.Validate(); err != nil {
		return nil, func() {}, err
	}
	if b.cfg.General.RemoteStorage == "none" || b.cfg.General.RemoteStorage == "custom" {
		return nil, func() {}, fmt.Errorf("cas: unsupported general.remote_storage=%q for cas-* commands", b.cfg.General.RemoteStorage)
	}
	// Connect to ClickHouse so we can resolve disks (needed by NewBackupDestination
	// and DefaultDataPath).
	if err := b.ch.Connect(); err != nil {
		return nil, func() {}, fmt.Errorf("cas: can't connect to clickhouse: %w", err)
	}
	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		b.ch.Close()
		return nil, func() {}, fmt.Errorf("cas: GetDisks: %w", err)
	}
	if initErr := b.initDisksPathsAndBackupDestination(ctx, disks, backupName); initErr != nil {
		b.ch.Close()
		return nil, func() {}, fmt.Errorf("cas: initDisksPathsAndBackupDestination: %w", initErr)
	}
	if b.dst == nil {
		b.ch.Close()
		return nil, func() {}, fmt.Errorf("cas: BackupDestination not initialized for remote_storage=%q", b.cfg.General.RemoteStorage)
	}
	backend := casstorage.NewStorageBackend(b.dst)
	closer := func() {
		if b.dst != nil {
			if err := b.dst.Close(ctx); err != nil {
				log.Warn().Msgf("cas: can't close BackupDestination: %v", err)
			}
		}
		b.ch.Close()
	}
	return backend, closer, nil
}

// chTablesAndDisks returns ClickHouse tables and disks suitable for the CAS
// object-disk pre-flight. Errors are logged but not fatal — a missing pre-
// flight just means cas.Upload's caller is responsible for the refusal.
func (b *Backuper) chTablesAndDisks(ctx context.Context) ([]cas.TableInfo, []cas.DiskInfo) {
	var chTables []cas.TableInfo
	var chDisks []cas.DiskInfo
	tables, err := b.ch.GetTables(ctx, "")
	if err != nil {
		log.Warn().Msgf("cas: GetTables for object-disk pre-flight failed: %v", err)
	} else {
		for _, t := range tables {
			chTables = append(chTables, cas.TableInfo{
				Database:  t.Database,
				Name:      t.Name,
				DataPaths: t.DataPaths,
			})
		}
	}
	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		log.Warn().Msgf("cas: GetDisks for object-disk pre-flight failed: %v", err)
	} else {
		for _, d := range disks {
			chDisks = append(chDisks, cas.DiskInfo{Name: d.Name, Path: d.Path, Type: d.Type})
		}
	}
	return chTables, chDisks
}

// CASUpload uploads a local backup using the CAS layout.
func (b *Backuper) CASUpload(backupName string, skipObjectDisks, dryRun bool, backupVersion string, commandId int) error {
	if backupName == "" {
		return errors.New("cas-upload: backup name is required")
	}
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	if pidErr := pidlock.CheckAndCreatePidFile(backupName, "cas-upload"); pidErr != nil {
		return pidErr
	}
	defer pidlock.RemovePidFile(backupName)

	ctx, cancel, err := b.setupCASContext(commandId)
	if err != nil {
		return err
	}
	defer cancel()

	start := time.Now()
	backend, closer, err := b.ensureCAS(ctx, backupName)
	if err != nil {
		return err
	}
	defer closer()

	// Resolve the local backup directory.
	fullLocal := path.Join(b.DefaultDataPath, "backup", backupName)
	if _, err := os.Stat(fullLocal); err != nil {
		return fmt.Errorf("cas-upload: local backup %q not found at %s; run 'clickhouse-backup create %s' first", backupName, fullLocal, backupName)
	}

	chTables, chDisks := b.chTablesAndDisks(ctx)

	res, uploadErr := cas.Upload(ctx, backend, b.cfg.CAS, backupName, cas.UploadOptions{
		LocalBackupDir:   fullLocal,
		SkipObjectDisks:  skipObjectDisks,
		DryRun:           dryRun,
		Parallelism:      int(b.cfg.General.UploadConcurrency),
		ClickHouseTables: chTables,
		Disks:            chDisks,
	})
	if uploadErr != nil {
		return uploadErr
	}
	log.Info().
		Str("backup", res.BackupName).
		Int("blobs_considered", res.BlobsConsidered).
		Int("blobs_uploaded", res.BlobsUploaded).
		Int64("bytes_uploaded", res.BytesUploaded).
		Int("archives", res.PerTableArchives).
		Bool("dry_run", res.DryRun).
		Dur("elapsed", time.Since(start)).
		Msg("cas-upload done")
	fmt.Printf("cas-upload: %s blobs=%d uploaded=%d bytes=%d archives=%d dryRun=%v elapsed=%s\n",
		res.BackupName, res.BlobsConsidered, res.BlobsUploaded, res.BytesUploaded, res.PerTableArchives, res.DryRun, time.Since(start).Round(time.Millisecond))
	return nil
}

// CASDownload materializes a CAS backup into the local backup directory.
// This does NOT load tables into ClickHouse; use cas-restore for that.
func (b *Backuper) CASDownload(backupName, tablePattern string, partitions []string, schemaOnly, dataOnly bool, backupVersion string, commandId int) error {
	if backupName == "" {
		return errors.New("cas-download: backup name is required")
	}
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	if pidErr := pidlock.CheckAndCreatePidFile(backupName, "cas-download"); pidErr != nil {
		return pidErr
	}
	defer pidlock.RemovePidFile(backupName)

	ctx, cancel, err := b.setupCASContext(commandId)
	if err != nil {
		return err
	}
	defer cancel()

	start := time.Now()
	backend, closer, err := b.ensureCAS(ctx, backupName)
	if err != nil {
		return err
	}
	defer closer()

	localBackupRoot := path.Join(b.DefaultDataPath, "backup")
	if err := os.MkdirAll(localBackupRoot, 0o755); err != nil {
		return fmt.Errorf("cas-download: mkdir %s: %w", localBackupRoot, err)
	}

	res, dlErr := cas.Download(ctx, backend, b.cfg.CAS, backupName, cas.DownloadOptions{
		LocalBackupDir: localBackupRoot,
		TableFilter:    splitTablePattern(tablePattern),
		Partitions:     partitions,
		SchemaOnly:     schemaOnly,
		DataOnly:       dataOnly,
		Parallelism:    int(b.cfg.General.DownloadConcurrency),
	})
	if dlErr != nil {
		return dlErr
	}
	log.Info().
		Str("backup", res.BackupName).
		Str("local_dir", res.LocalBackupDir).
		Int("archives", res.PerTableArchives).
		Int("blobs_fetched", res.BlobsFetched).
		Int64("bytes_fetched", res.BytesFetched).
		Dur("elapsed", time.Since(start)).
		Msg("cas-download done")
	fmt.Printf("cas-download: %s -> %s archives=%d blobs=%d bytes=%d elapsed=%s\n",
		res.BackupName, res.LocalBackupDir, res.PerTableArchives, res.BlobsFetched, res.BytesFetched, time.Since(start).Round(time.Millisecond))
	return nil
}

// CASRestore downloads a CAS backup and hands off to the v1 restore flow.
func (b *Backuper) CASRestore(
	backupName, tablePattern string,
	dbMapping, tableMapping, partitions, skipProjections []string,
	schemaOnly, dataOnly, dropExists, ignoreDependencies bool,
	restoreSchemaAsAttach, replicatedCopyToDetached, skipEmptyTables, resume bool,
	backupVersion string, commandId int,
) error {
	if backupName == "" {
		return errors.New("cas-restore: backup name is required")
	}
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	if pidErr := pidlock.CheckAndCreatePidFile(backupName, "cas-restore"); pidErr != nil {
		return pidErr
	}
	defer pidlock.RemovePidFile(backupName)

	ctx, cancel, err := b.setupCASContext(commandId)
	if err != nil {
		return err
	}
	defer cancel()

	start := time.Now()
	backend, closer, err := b.ensureCAS(ctx, backupName)
	if err != nil {
		return err
	}
	defer closer()

	localBackupRoot := path.Join(b.DefaultDataPath, "backup")
	if err := os.MkdirAll(localBackupRoot, 0o755); err != nil {
		return fmt.Errorf("cas-restore: mkdir %s: %w", localBackupRoot, err)
	}

	opts := cas.RestoreOptions{
		DownloadOptions: cas.DownloadOptions{
			LocalBackupDir: localBackupRoot,
			TableFilter:    splitTablePattern(tablePattern),
			Partitions:     partitions,
			SchemaOnly:     schemaOnly,
			DataOnly:       dataOnly,
			Parallelism:    int(b.cfg.General.DownloadConcurrency),
		},
		DropExists:               dropExists,
		DatabaseMapping:          dbMapping,
		TableMapping:             tableMapping,
		SkipProjections:          skipProjections,
		RestoreSchemaAsAttach:    restoreSchemaAsAttach,
		ReplicatedCopyToDetached: replicatedCopyToDetached,
		SkipEmptyTables:          skipEmptyTables,
		Resume:                   resume,
		BackupVersion:            backupVersion,
		CommandID:                commandId,
		IgnoreDependencies:       ignoreDependencies,
	}

	// V1 restore handoff: cas.Restore materializes the backup at
	// <LocalBackupDir>/<name> and calls this closure with that absolute path.
	// We then delegate to b.Restore using the v1 positional argument list.
	runV1 := func(ctx context.Context, _ string, ro cas.RestoreOptions) error {
		// b.Restore looks the backup up by name under b.DefaultDataPath/backup/,
		// which is exactly where cas.Download placed it.
		return b.Restore(
			backupName,
			tablePattern,
			ro.DatabaseMapping,
			ro.TableMapping,
			ro.Partitions,
			ro.SkipProjections,
			ro.SchemaOnly,
			ro.DataOnly,
			ro.DropExists,
			false, // ignoreDependencies — rejected upstream by cas.Restore
			false, // restoreRBAC: out of scope for CAS v1
			false, // rbacOnly
			false, // restoreConfigs
			false, // configsOnly
			false, // restoreNamedCollections
			false, // namedCollectionsOnly
			ro.Resume,
			ro.RestoreSchemaAsAttach,
			ro.ReplicatedCopyToDetached,
			ro.SkipEmptyTables,
			ro.BackupVersion,
			ro.CommandID,
		)
	}

	if rErr := cas.Restore(ctx, backend, b.cfg.CAS, backupName, opts, runV1); rErr != nil {
		return rErr
	}
	log.Info().Str("backup", backupName).Dur("elapsed", time.Since(start)).Msg("cas-restore done")
	fmt.Printf("cas-restore: %s elapsed=%s\n", backupName, time.Since(start).Round(time.Millisecond))
	return nil
}

// CASDelete removes a CAS backup's metadata subtree (blob reclamation is the
// next prune's responsibility).
func (b *Backuper) CASDelete(backupName string) error {
	if backupName == "" {
		return errors.New("cas-delete: backup name is required")
	}
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	ctx, cancel, err := b.setupCASContext(status.NotFromAPI)
	if err != nil {
		return err
	}
	defer cancel()
	backend, closer, err := b.ensureCAS(ctx, backupName)
	if err != nil {
		return err
	}
	defer closer()
	if err := cas.Delete(ctx, backend, b.cfg.CAS, backupName); err != nil {
		return err
	}
	fmt.Printf("cas-delete: %s removed\n", backupName)
	return nil
}

// CASVerify performs a HEAD + size check on every blob referenced by the
// backup, writing failures to stdout.
func (b *Backuper) CASVerify(backupName string, jsonOut bool) error {
	if backupName == "" {
		return errors.New("cas-verify: backup name is required")
	}
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	ctx, cancel, err := b.setupCASContext(status.NotFromAPI)
	if err != nil {
		return err
	}
	defer cancel()
	backend, closer, err := b.ensureCAS(ctx, backupName)
	if err != nil {
		return err
	}
	defer closer()
	res, vErr := cas.Verify(ctx, backend, b.cfg.CAS, backupName, cas.VerifyOptions{JSON: jsonOut}, os.Stdout)
	if vErr != nil && !errors.Is(vErr, cas.ErrVerifyFailures) {
		return vErr
	}
	if res != nil {
		log.Info().
			Str("backup", res.BackupName).
			Int("blobs_checked", res.BlobsChecked).
			Int("failures", len(res.Failures)).
			Msg("cas-verify done")
	}
	if vErr != nil {
		// Non-zero exit on verify failures — surfaced via cli action error.
		return vErr
	}
	return nil
}

// CASStatus prints a LIST-only health summary for the configured cluster.
func (b *Backuper) CASStatus() error {
	ctx, cancel, err := b.setupCASContext(status.NotFromAPI)
	if err != nil {
		return err
	}
	defer cancel()
	backend, closer, err := b.ensureCAS(ctx, "")
	if err != nil {
		return err
	}
	defer closer()
	r, sErr := cas.Status(ctx, backend, b.cfg.CAS)
	if sErr != nil {
		return sErr
	}
	return cas.PrintStatus(r, os.Stdout)
}

// splitTablePattern turns a comma-separated "db1.t1,db2.t2" string into the
// exact-match filter slice expected by cas.{Download,Upload}.TableFilter.
// Empty input returns nil (allow-all). Whitespace around each entry is trimmed.
func splitTablePattern(p string) []string {
	p = strings.TrimSpace(p)
	if p == "" {
		return nil
	}
	parts := strings.Split(p, ",")
	out := make([]string, 0, len(parts))
	for _, s := range parts {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
