package cas

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/checksumstxt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
)

// PruneOptions tunes a single Prune run. GraceBlob / AbandonThreshold are
// applied iff their *Set flags are true; otherwise the run uses
// cfg.GraceBlobDuration() / cfg.AbandonThresholdDuration(). The *Set flags
// let an explicit zero override the configured non-zero default
// (use case: targeted cleanup, regression tests).
//
// DryRun reports candidates without deleting; Unlock is the operator escape
// hatch for a stranded prune.marker.
type PruneOptions struct {
	DryRun              bool
	GraceBlob           time.Duration
	GraceBlobSet        bool
	AbandonThreshold    time.Duration
	AbandonThresholdSet bool
	Unlock              bool
}

// PruneReport summarizes what a Prune run did. Returned even on error so
// callers can log partial progress.
type PruneReport struct {
	DryRun                bool
	LiveBackups           int
	BlobsTotal            uint64
	OrphanBlobsConsidered uint64
	OrphansHeldByGrace    uint64
	OrphansDeleted        uint64
	BytesReclaimed        int64
	AbandonedMarkersSwept int
	MetadataOrphansSwept  int
	DurationSeconds       float64
}

// Prune performs mark-and-sweep garbage collection of orphan blobs and
// metadata-orphan subtrees in the configured CAS namespace. See
// docs/cas-design.md §6.7 for the algorithm.
//
// Concurrency: a single advisory marker (cas/<cluster>/prune.marker) is
// atomically created at step 2 via PutFileIfAbsent and released via a scoped
// defer registered ONLY when this run owns the marker. A second concurrent
// prune sees created=false and returns an error without touching the marker.
func Prune(ctx context.Context, b Backend, cfg Config, opts PruneOptions) (*PruneReport, error) {
	if !cfg.Enabled {
		return nil, errors.New("cas: cas.enabled=false")
	}
	cp := cfg.ClusterPrefix()
	grace := cfg.GraceBlobDuration()
	if opts.GraceBlobSet {
		grace = opts.GraceBlob
	}
	abandon := cfg.AbandonThresholdDuration()
	if opts.AbandonThresholdSet {
		abandon = opts.AbandonThreshold
	}

	// --unlock escape hatch: delete a stranded prune.marker and exit.
	if opts.Unlock {
		_, _, exists, err := b.StatFile(ctx, PruneMarkerPath(cp))
		if err != nil {
			return nil, fmt.Errorf("cas-prune --unlock: stat marker: %w", err)
		}
		if !exists {
			return nil, errors.New("cas-prune --unlock: no prune.marker present")
		}
		if opts.DryRun {
			if m, readErr := ReadPruneMarker(ctx, b, cp); readErr == nil {
				log.Info().
					Str("host", m.Host).
					Str("run_id", m.RunID).
					Str("started_at", m.StartedAt).
					Msg("cas-prune --dry-run --unlock: would delete this marker (no action taken)")
			} else {
				log.Info().Err(readErr).Msg("cas-prune --dry-run --unlock: marker present but unparseable; would delete")
			}
			return &PruneReport{DryRun: true}, nil
		}
		if err := b.DeleteFile(ctx, PruneMarkerPath(cp)); err != nil {
			return nil, fmt.Errorf("cas-prune --unlock: delete marker: %w", err)
		}
		log.Warn().Msg("cas-prune: prune marker manually unlocked by operator")
		return &PruneReport{}, nil
	}

	rep := &PruneReport{DryRun: opts.DryRun}
	start := time.Now()
	defer func() { rep.DurationSeconds = time.Since(start).Seconds() }()

	// Step 1: refuse to run while any inprogress marker is younger than abandon.
	fresh, abandoned, err := classifyInProgress(ctx, b, cp, abandon)
	if err != nil {
		return rep, err
	}
	if len(fresh) > 0 {
		return rep, freshInProgressError(fresh)
	}

	// Step 2: atomically create prune marker; defer cleanup only if we own it.
	if !opts.DryRun {
		runID, created, err := WritePruneMarker(ctx, b, cp, hostname())
		if err != nil {
			if errors.Is(err, ErrConditionalPutNotSupported) {
				return rep, fmt.Errorf("cas-prune: backend cannot guarantee atomic markers; refusing (set cas.allow_unsafe_markers=true to override on FTP)")
			}
			return rep, fmt.Errorf("cas-prune: write marker: %w", err)
		}
		if !created {
			existing, readErr := ReadPruneMarker(ctx, b, cp)
			if readErr != nil {
				return rep, fmt.Errorf("cas-prune: another prune is in progress (could not read marker: %v)", readErr)
			}
			return rep, fmt.Errorf("cas-prune: another prune is in progress on host=%s started=%s run_id=%s",
				existing.Host, existing.StartedAt, existing.RunID)
		}
		_ = runID // we already own the marker by virtue of created=true; runID is for diagnostics only
		defer func() {
			if delErr := b.DeleteFile(ctx, PruneMarkerPath(cp)); delErr != nil {
				log.Warn().Err(delErr).Msg("cas-prune: failed to release prune.marker")
			}
		}()
	}

	// Step 3: T0 (used for grace cutoff)
	t0 := start

	// Step 4: sweep abandoned in-progress markers.
	if !opts.DryRun {
		for _, m := range abandoned {
			if err := b.DeleteFile(ctx, InProgressMarkerPath(cp, m.Backup)); err != nil {
				log.Warn().Err(err).Str("backup", m.Backup).Msg("cas-prune: delete abandoned marker")
			}
		}
	}
	rep.AbandonedMarkersSwept = len(abandoned)

	// Step 5: list live backups (subtrees with metadata.json).
	backups, err := listLiveBackups(ctx, b, cp)
	if err != nil {
		return rep, fmt.Errorf("cas-prune: list live backups: %w", err)
	}
	rep.LiveBackups = len(backups)

	// Step 6: build mark set by walking each live backup's per-table
	// archives and extracting checksums.txt entries above the inline
	// threshold (those that went to the blob store).
	marksDir, err := os.MkdirTemp("", "cas-prune-marks-*")
	if err != nil {
		return rep, fmt.Errorf("cas-prune: temp dir: %w", err)
	}
	defer os.RemoveAll(marksDir)
	marksPath := filepath.Join(marksDir, "marks")
	mw, err := NewMarkSetWriter(marksPath, 1<<20)
	if err != nil {
		return rep, fmt.Errorf("cas-prune: mark set: %w", err)
	}
	for _, bk := range backups {
		// Step 7 fail-closed: any error reading a live backup aborts the
		// run BEFORE any blob is deleted.
		if err := accumulateRefsForBackup(ctx, b, cp, bk, mw); err != nil {
			_ = mw.Close()
			return rep, fmt.Errorf("cas-prune: cannot read live backup %q: %w", bk, err)
		}
	}
	if err := mw.Close(); err != nil {
		return rep, fmt.Errorf("cas-prune: close mark set: %w", err)
	}

	// Steps 8-9: stream compare against blob store, filter by grace.
	mr, err := OpenMarkSetReader(marksPath)
	if err != nil {
		return rep, fmt.Errorf("cas-prune: open mark set: %w", err)
	}
	defer mr.Close()
	cands, sweepStats, err := SweepOrphans(ctx, b, cp, mr, grace, t0)
	if err != nil {
		return rep, fmt.Errorf("cas-prune: sweep: %w", err)
	}
	rep.BlobsTotal = sweepStats.BlobsTotal
	rep.OrphansHeldByGrace = sweepStats.OrphansHeldByGrace
	rep.OrphanBlobsConsidered = uint64(len(cands))

	// Step 10: metadata-orphan subtree sweep.
	metaOrphans, err := findMetadataOrphans(ctx, b, cp)
	if err != nil {
		return rep, fmt.Errorf("cas-prune: find metadata orphans: %w", err)
	}
	if !opts.DryRun {
		for _, p := range metaOrphans {
			if err := walkAndDeleteSubtree(ctx, b, p); err != nil {
				log.Warn().Err(err).Str("subtree", p).Msg("cas-prune: delete metadata-orphan subtree")
			}
		}
	}
	rep.MetadataOrphansSwept = len(metaOrphans)

	// Step 11: delete orphan blobs (parallel, bounded).
	if opts.DryRun {
		for _, c := range cands {
			fmt.Printf("cas-prune (dry-run): would delete %s (modTime=%s, size=%d)\n", c.Key, c.ModTime, c.Size)
		}
	} else {
		n, bytes, err := deleteBlobs(ctx, b, cands, 32)
		rep.OrphansDeleted = uint64(n)
		rep.BytesReclaimed = bytes
		if err != nil {
			return rep, fmt.Errorf("cas-prune: delete blobs: %w", err)
		}
	}
	return rep, nil
}

// inProgressMarker captures the parsed per-marker state used by classify.
type inProgressMarker struct {
	Backup  string
	Host    string
	ModTime time.Time
	Age     time.Duration
}

// classifyInProgress walks cas/<c>/inprogress/ and partitions markers into
// "fresh" (younger than abandon) and "abandoned" (older). Markers we can't
// parse are still classified by ModTime (safer than dropping them).
func classifyInProgress(ctx context.Context, b Backend, cp string, abandon time.Duration) (fresh, abandoned []inProgressMarker, err error) {
	prefix := cp + "inprogress/"
	now := time.Now()
	err = b.Walk(ctx, prefix, false, func(rf RemoteFile) error {
		if !strings.HasSuffix(rf.Key, ".marker") {
			return nil
		}
		// Backup name: strip prefix + ".marker"
		rest := strings.TrimPrefix(rf.Key, prefix)
		name := strings.TrimSuffix(rest, ".marker")
		if name == "" || strings.Contains(name, "/") {
			return nil
		}
		if rf.ModTime.IsZero() {
			log.Warn().
				Str("backup", name).
				Msg("cas-prune: in-progress marker has zero ModTime (likely FTP LIST without MLSD); classifying as fresh")
			fresh = append(fresh, inProgressMarker{Backup: name, ModTime: rf.ModTime, Age: 0})
			return nil
		}
		age := now.Sub(rf.ModTime)
		m := inProgressMarker{Backup: name, ModTime: rf.ModTime, Age: age}
		if age >= abandon {
			abandoned = append(abandoned, m)
		} else {
			fresh = append(fresh, m)
		}
		return nil
	})
	return fresh, abandoned, err
}

func freshInProgressError(fresh []inProgressMarker) error {
	parts := make([]string, len(fresh))
	for i, m := range fresh {
		parts[i] = fmt.Sprintf("%s (age=%s)", m.Backup, m.Age.Round(time.Second))
	}
	return fmt.Errorf("cas-prune: refuse to run while %d in-progress upload(s) are fresh: %s — wait for them, or run 'cas-prune --abandon-threshold=0s' if confirmed dead",
		len(fresh), strings.Join(parts, ", "))
}

// listLiveBackups walks cas/<c>/metadata/<bk>/metadata.json entries and
// returns the backup names. Mirrors cas-status's discovery logic.
func listLiveBackups(ctx context.Context, b Backend, cp string) ([]string, error) {
	prefix := cp + "metadata/"
	var backups []string
	err := b.Walk(ctx, prefix, true, func(rf RemoteFile) error {
		if !strings.HasSuffix(rf.Key, "/metadata.json") {
			return nil
		}
		rest := strings.TrimPrefix(rf.Key, prefix)
		name := strings.TrimSuffix(rest, "/metadata.json")
		if name == "" || strings.Contains(name, "/") {
			return nil
		}
		backups = append(backups, name)
		return nil
	})
	return backups, err
}

// accumulateRefsForBackup reads the per-table archives of one backup,
// parses the embedded checksums.txt files, and writes every above-threshold
// hash to the mark set. The persisted CAS params (InlineThreshold) are
// read from the backup's own metadata.json — never from current config —
// so prune is correct even if cfg.InlineThreshold has been retuned since
// the backup was written.
func accumulateRefsForBackup(ctx context.Context, b Backend, cp, name string, mw *MarkSetWriter) error {
	bm, err := readBackupMetadata(ctx, b, cp, name)
	if err != nil {
		return fmt.Errorf("read metadata.json: %w", err)
	}
	if bm.CAS == nil {
		return errors.New("backup metadata has no CAS field; cannot prune")
	}
	threshold := bm.CAS.InlineThreshold

	for _, tt := range bm.Tables {
		tm, err := readTableMetadata(ctx, b, cp, name, tt.Database, tt.Table)
		if err != nil {
			return fmt.Errorf("read table metadata for %s.%s: %w", tt.Database, tt.Table, err)
		}
		for disk := range tm.Parts {
			archKey := PartArchivePath(cp, name, disk, tt.Database, tt.Table)
			if err := accumulateRefsFromArchive(ctx, b, archKey, threshold, mw); err != nil {
				return fmt.Errorf("accumulate refs from %s: %w", archKey, err)
			}
		}
	}
	return nil
}

func readBackupMetadata(ctx context.Context, b Backend, cp, name string) (*metadata.BackupMetadata, error) {
	rc, err := b.GetFile(ctx, MetadataJSONPath(cp, name))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var bm metadata.BackupMetadata
	if err := json.Unmarshal(body, &bm); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	return &bm, nil
}

func readTableMetadata(ctx context.Context, b Backend, cp, name, db, table string) (*metadata.TableMetadata, error) {
	rc, err := b.GetFile(ctx, TableMetaPath(cp, name, db, table))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var tm metadata.TableMetadata
	if err := json.Unmarshal(body, &tm); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	return &tm, nil
}

// accumulateRefsFromArchive streams a tar.zstd per-table archive, extracts
// every checksums.txt body, parses it, and writes every above-threshold
// (filename, size, hash) entry's hash into the mark set.
func accumulateRefsFromArchive(ctx context.Context, b Backend, archKey string, threshold uint64, mw *MarkSetWriter) error {
	rc, err := b.GetFile(ctx, archKey)
	if err != nil {
		return err
	}
	defer rc.Close()
	zr, err := zstd.NewReader(rc)
	if err != nil {
		return fmt.Errorf("zstd: %w", err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("tar: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		if !strings.HasSuffix(hdr.Name, "/checksums.txt") {
			continue
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			return fmt.Errorf("read %s: %w", hdr.Name, err)
		}
		parsed, err := checksumstxt.Parse(bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("parse %s: %w", hdr.Name, err)
		}
		for _, c := range parsed.Files {
			if c.FileSize <= threshold {
				continue
			}
			h := Hash128{Low: c.FileHash.Low, High: c.FileHash.High}
			if err := mw.Write(h); err != nil {
				return err
			}
		}
	}
}

// findMetadataOrphans returns prefixes under cas/<c>/metadata/<X>/ where
// the catalog truth (metadata.json) is absent. Such subtrees represent
// half-completed deletions whose per-table JSONs / archives should be
// reclaimed.
func findMetadataOrphans(ctx context.Context, b Backend, cp string) ([]string, error) {
	metaPrefix := cp + "metadata/"
	// Discover all top-level <name> directories by walking and collecting
	// the first path component after the prefix.
	seen := map[string]bool{}
	err := b.Walk(ctx, metaPrefix, true, func(rf RemoteFile) error {
		rest := strings.TrimPrefix(rf.Key, metaPrefix)
		idx := strings.Index(rest, "/")
		if idx < 0 {
			return nil
		}
		name := rest[:idx]
		if name == "" {
			return nil
		}
		seen[name] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	var orphans []string
	for name := range seen {
		_, _, exists, err := b.StatFile(ctx, MetadataJSONPath(cp, name))
		if err != nil {
			return nil, err
		}
		if !exists {
			orphans = append(orphans, MetadataDir(cp, name))
		}
	}
	return orphans, nil
}

// deleteBlobs deletes the given orphan candidates with bounded parallelism.
// Returns the number successfully deleted, the cumulative bytes reclaimed,
// and the first error encountered (if any). Subsequent candidates after an
// error are still attempted; the error propagates after the wait.
func deleteBlobs(ctx context.Context, b Backend, cands []OrphanCandidate, parallelism int) (int, int64, error) {
	if parallelism <= 0 {
		parallelism = 32
	}
	var (
		mu       sync.Mutex
		count    int
		bytes    int64
		firstErr error
		wg       sync.WaitGroup
	)
	sem := make(chan struct{}, parallelism)
	for _, c := range cands {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := b.DeleteFile(ctx, c.Key); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			count++
			bytes += c.Size
			mu.Unlock()
		}()
	}
	wg.Wait()
	return count, bytes, firstErr
}

// PrintPruneReport renders a human-readable report to w.
func PrintPruneReport(r *PruneReport, w io.Writer) error {
	prefix := "cas-prune"
	if r.DryRun {
		prefix = "cas-prune (dry-run)"
	}
	_, err := fmt.Fprintf(w, "%s:\n  Live backups        : %d\n  Orphan candidates   : %d\n  Orphans deleted     : %d\n  Bytes reclaimed     : %d\n  Abandoned markers   : %d swept\n  Metadata orphans    : %d swept\n  Wall clock          : %.2fs\n",
		prefix,
		r.LiveBackups,
		r.OrphanBlobsConsidered,
		r.OrphansDeleted,
		r.BytesReclaimed,
		r.AbandonedMarkersSwept,
		r.MetadataOrphansSwept,
		r.DurationSeconds,
	)
	return err
}

