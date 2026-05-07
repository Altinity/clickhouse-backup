package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
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

// snapshotObjectDiskHitsFromDisks is the pure, testable core of the snapshot
// pre-flight. It walks <localBackupDir>/shadow/<db>/<table>/<disk>/ to
// enumerate (db, table, disk) triples actually present in the backup, then
// cross-references diskTypeByName (disk name → type) to identify object-disk
// hits. Returns deduplicated hits (empty slice + nil error for empty/no-object-disk backups).
func (b *Backuper) snapshotObjectDiskHitsFromDisks(localBackupDir string, diskTypeByName map[string]string) ([]cas.ObjectDiskHit, error) {
	shadow := filepath.Join(localBackupDir, "shadow")
	var hits []cas.ObjectDiskHit
	seen := map[cas.ObjectDiskHit]struct{}{}

	dbs, err := os.ReadDir(shadow)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // empty backup or schema-only backup
		}
		return nil, fmt.Errorf("cas: read shadow dir: %w", err)
	}
	for _, dbe := range dbs {
		if !dbe.IsDir() {
			continue
		}
		db := dbe.Name()
		tables, err := os.ReadDir(filepath.Join(shadow, db))
		if err != nil {
			continue
		}
		for _, tbe := range tables {
			if !tbe.IsDir() {
				continue
			}
			table := tbe.Name()
			disks, err := os.ReadDir(filepath.Join(shadow, db, table))
			if err != nil {
				continue
			}
			for _, de := range disks {
				if !de.IsDir() {
					continue
				}
				disk := de.Name()
				diskType, ok := diskTypeByName[disk]
				if !ok {
					continue // disk not present in live system.disks; treat as local
				}
				if !cas.IsObjectDiskType(diskType) {
					continue
				}
				h := cas.ObjectDiskHit{Database: db, Table: table, Disk: disk, DiskType: diskType}
				if _, dup := seen[h]; dup {
					continue
				}
				seen[h] = struct{}{}
				hits = append(hits, h)
			}
		}
	}
	return hits, nil
}

// snapshotObjectDiskHits queries live system.disks for disk-type information,
// then delegates to snapshotObjectDiskHitsFromDisks to walk the local backup
// snapshot. If system.disks is unreachable the pre-flight is skipped (returns
// nil, nil) — matching the existing tolerance for non-fatal pre-flight errors.
func (b *Backuper) snapshotObjectDiskHits(ctx context.Context, localBackupDir string) ([]cas.ObjectDiskHit, error) {
	diskTypeByName := map[string]string{}
	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		log.Warn().Msgf("cas: GetDisks for snapshot pre-flight failed: %v; skipping pre-flight", err)
		return nil, nil
	}
	for _, d := range disks {
		diskTypeByName[d.Name] = d.Type
	}
	return b.snapshotObjectDiskHitsFromDisks(localBackupDir, diskTypeByName)
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

	// Snapshot-based pre-flight: read which disks the local backup actually
	// uses, not which disks the live ClickHouse currently has.
	if !skipObjectDisks {
		hits, err := b.snapshotObjectDiskHits(ctx, fullLocal)
		if err != nil {
			return fmt.Errorf("cas-upload: snapshot pre-flight: %w", err)
		}
		if len(hits) > 0 {
			return fmt.Errorf("%w: %s",
				cas.ErrObjectDiskRefused,
				cas.FormatObjectDiskHits(hits))
		}
	}

	res, uploadErr := cas.Upload(ctx, backend, b.cfg.CAS, backupName, cas.UploadOptions{
		LocalBackupDir:  fullLocal,
		SkipObjectDisks: skipObjectDisks,
		DryRun:          dryRun,
		Parallelism:     int(b.cfg.General.UploadConcurrency),
	})
	if uploadErr != nil {
		return uploadErr
	}
	log.Info().
		Str("backup", res.BackupName).
		Int("total_files", res.TotalFiles).
		Uint64("total_bytes", res.TotalBytes).
		Int("inline_files", res.InlineFiles).
		Uint64("inline_bytes", res.InlineBytes).
		Int("unique_blobs", res.UniqueBlobs).
		Uint64("blob_bytes_total", res.BlobBytesTotal).
		Int("blobs_uploaded", res.BlobsUploaded).
		Int64("bytes_uploaded", res.BytesUploaded).
		Int("blobs_reused", res.BlobsReused).
		Int64("bytes_reused", res.BytesReused).
		Int("archives", res.PerTableArchives).
		Int64("archive_bytes", res.ArchiveBytes).
		Bool("dry_run", res.DryRun).
		Dur("elapsed", time.Since(start)).
		Msg("cas-upload done")

	totalBytesH := utils.FormatBytes(res.TotalBytes)
	inlineBytesH := utils.FormatBytes(res.InlineBytes)
	blobBytesH := utils.FormatBytes(res.BlobBytesTotal)
	uploadedH := utils.FormatBytes(uint64(res.BytesUploaded))
	reusedH := utils.FormatBytes(uint64(res.BytesReused))
	archiveH := utils.FormatBytes(uint64(res.ArchiveBytes))
	prefix := "cas-upload"
	if res.DryRun {
		prefix = "cas-upload (dry-run)"
	}
	fmt.Printf("%s: %s\n", prefix, res.BackupName)
	fmt.Printf("  Backup content : %d files, %s total\n", res.TotalFiles, totalBytesH)
	fmt.Printf("  Inlined        : %d files, %s (packed into %d archive%s, %s compressed)\n",
		res.InlineFiles, inlineBytesH, res.PerTableArchives, plural(res.PerTableArchives), archiveH)
	fmt.Printf("  Blob store     : %d unique blobs, %s\n", res.UniqueBlobs, blobBytesH)
	fmt.Printf("    uploaded now : %d blobs, %s\n", res.BlobsUploaded, uploadedH)
	fmt.Printf("    reused       : %d blobs, %s (already in remote — saved by content-addressing)\n",
		res.BlobsReused, reusedH)
	fmt.Printf("  Wall clock     : %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
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
	// No pidlock here: the inner v1 b.Restore (invoked via runV1 below)
	// acquires its own pidlock at pkg/backup/restore.go for the actual
	// mutation phase. Acquiring here too would self-deadlock — pidlock
	// has no same-PID exemption, so the inner acquire would fail with
	// "another clickhouse-backup `cas-restore` command is already running".
	// The cas-download phase mutates only a local temp directory; concurrent
	// same-name cas-restore calls are caught when both reach b.Restore.

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
	if pidErr := pidlock.CheckAndCreatePidFile(backupName, "cas-delete"); pidErr != nil {
		return pidErr
	}
	defer pidlock.RemovePidFile(backupName)
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
