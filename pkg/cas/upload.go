package cas

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/checksumstxt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/rs/zerolog/log"
)

// UploadOptions configures an Upload run.
type UploadOptions struct {
	// LocalBackupDir is the absolute path of the pre-existing local backup
	// directory (produced by `clickhouse-backup create`). Upload walks
	// <LocalBackupDir>/shadow/.
	LocalBackupDir string

	// TableFilter is an optional list of "db.table" exact-match filters.
	// Empty means "all tables found under shadow/". (v1 of CAS uses exact
	// match; glob support is a future enhancement — see TODO in planUpload.)
	TableFilter []string

	// SkipObjectDisks: when true, tables on object-disks (s3/azure/etc.)
	// are silently excluded; when false (the default) Upload refuses with
	// ErrObjectDiskRefused if any are detected.
	SkipObjectDisks bool

	// DryRun: when true, classify+plan but write nothing to the backend.
	DryRun bool

	// Parallelism caps simultaneous blob uploads and the cold-list shard
	// walks. <=0 falls back to 16.
	Parallelism int

	// Disks and ClickHouseTables are caller-supplied; if both non-empty we
	// run DetectObjectDiskTables. Empty slices mean "skip the pre-flight"
	// (intended for unit tests that don't model live ClickHouse).
	Disks            []DiskInfo
	ClickHouseTables []TableInfo
}

// UploadResult summarizes what an Upload run did. The stats break down into
// three layers operators care about:
//
//  1. The backup's logical content (TotalFiles / TotalBytes — what would be
//     in a v1 backup, including duplicated content across parts).
//  2. How the content was placed: InlineFiles/InlineBytes (small files that
//     ride inside per-table tar.zstd archives) vs BlobFiles (file references
//     that go to the content-addressed blob store) and the deduplicated
//     UniqueBlobs / BlobBytesTotal.
//  3. What actually crossed the wire on this run: BlobsUploaded /
//     BytesUploaded (new blobs PUT to the remote), BlobsReused / BytesReused
//     (deduped via cold-list against existing remote blobs), and ArchiveBytes
//     (compressed bytes for the per-table archives uploaded now).
type UploadResult struct {
	BackupName string

	// Logical content (counted across every part, before blob dedup).
	TotalFiles  int
	TotalBytes  uint64
	InlineFiles int
	InlineBytes uint64
	BlobFiles   int // file references that pointed at a blob (pre-dedup)

	// Blob-store side, after content-addressed dedup within this backup.
	UniqueBlobs    int    // unique blob hashes referenced (= len(plan.blobs))
	BlobBytesTotal uint64 // sum of UniqueBlobs sizes

	// What this run sent to / dedup'd against the remote.
	BlobsUploaded int   // unique blobs newly PUT
	BytesUploaded int64 // sum of BlobsUploaded sizes
	BlobsReused   int   // unique blobs already in remote (skipped)
	BytesReused   int64 // sum of BlobsReused sizes
	ArchiveBytes  int64 // compressed bytes of per-table archives uploaded

	PerTableArchives int
	DryRun           bool

	// BlobsConsidered is an alias for UniqueBlobs kept for backwards
	// compatibility with log output written before the stats expansion.
	// New code should read UniqueBlobs.
	BlobsConsidered int
}

// uploadPlan is the in-memory description of what to upload, built by
// scanning the local backup directory and parsing every checksums.txt.
type uploadPlan struct {
	// blobs: unique hashes that exceed the inline threshold and are not
	// special-cased (checksums.txt is always inlined).
	blobs map[Hash128]blobRef

	// tables maps "disk|db|table" → tablePlan.
	tables map[string]*tablePlan
	// tableKeys preserves a sorted ordering for deterministic uploads.
	tableKeys []string

	// localRoot is the local backup directory passed to planUpload; used by
	// uploadTableJSONs to read the v1 per-table metadata that
	// 'clickhouse-backup create' wrote.
	localRoot string

	// Aggregates for stats reporting. Populated alongside the maps above.
	totalFiles  int
	totalBytes  uint64
	inlineFiles int
	inlineBytes uint64
	blobFiles   int // file references that go to the blob store (pre-dedup)
}

// blobRef points at one local file claimed to have hash h. We pick any
// file with the hash for the actual upload (callers may have multiple
// copies).
type blobRef struct {
	LocalPath string
	Size      uint64
}

// tablePlan groups everything needed to build the per-(disk, db, table)
// archive and its companion table-metadata JSON.
type tablePlan struct {
	Disk, DB, Table string
	// archiveEntries are the inline files (small files + every
	// checksums.txt) that go into the tar.zstd. NameInArchive uses the
	// "<part>/<file>" convention from §6.3.
	archiveEntries []ArchiveEntry
	// parts is the per-part list used to populate TableMetadata.Parts.
	// Sorted by part name for deterministic JSON.
	parts []metadata.Part
}

// Upload performs a CAS upload of the local backup at opts.LocalBackupDir
// to the cluster identified by cfg. Implements docs/cas-design.md §6.4.
func Upload(ctx context.Context, b Backend, cfg Config, name string, opts UploadOptions) (*UploadResult, error) {
	// 1. Validate name + config.
	if err := validateName(name); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cp := cfg.ClusterPrefix()

	// 2. Refuse if prune.marker exists.
	if _, _, exists, err := b.StatFile(ctx, PruneMarkerPath(cp)); err != nil {
		return nil, fmt.Errorf("cas: stat prune marker: %w", err)
	} else if exists {
		return nil, ErrPruneInProgress
	}

	// 3. Object-disk pre-flight.
	if !opts.SkipObjectDisks && len(opts.Disks) > 0 && len(opts.ClickHouseTables) > 0 {
		hits := DetectObjectDiskTables(opts.ClickHouseTables, opts.Disks)
		if len(hits) > 0 {
			return nil, fmt.Errorf("%w: %s", ErrObjectDiskRefused, formatObjectDiskHits(hits))
		}
	}

	// 4. Best-effort same-name check.
	if _, _, exists, err := b.StatFile(ctx, MetadataJSONPath(cp, name)); err != nil {
		return nil, fmt.Errorf("cas: stat metadata.json: %w", err)
	} else if exists {
		return nil, ErrBackupExists
	}

	// 5. Write in-progress marker (skipped on DryRun).
	if !opts.DryRun {
		if err := WriteInProgressMarker(ctx, b, cp, name, ""); err != nil {
			return nil, fmt.Errorf("cas: write inprogress marker: %w", err)
		}
	}

	// 6. Plan upload: walk shadow/, parse checksums.txt, classify.
	plan, err := planUpload(opts.LocalBackupDir, cfg.InlineThreshold, opts.TableFilter, opts.SkipObjectDisks, opts.Disks, opts.ClickHouseTables)
	if err != nil {
		// Best-effort cleanup of the marker we just wrote.
		if !opts.DryRun {
			_ = DeleteInProgressMarker(ctx, b, cp, name)
		}
		return nil, err
	}

	// Compute total bytes referenced by unique blobs (after content dedup
	// within this backup; cold-list dedup against the remote happens in
	// step 7).
	var blobBytesTotal uint64
	for _, br := range plan.blobs {
		blobBytesTotal += br.Size
	}

	res := &UploadResult{
		BackupName:      name,
		TotalFiles:      plan.totalFiles,
		TotalBytes:      plan.totalBytes,
		InlineFiles:     plan.inlineFiles,
		InlineBytes:     plan.inlineBytes,
		BlobFiles:       plan.blobFiles,
		UniqueBlobs:     len(plan.blobs),
		BlobBytesTotal:  blobBytesTotal,
		BlobsConsidered: len(plan.blobs),
		DryRun:          opts.DryRun,
	}

	if opts.DryRun {
		res.PerTableArchives = len(plan.tableKeys)
		return res, nil
	}

	// 7. Cold-list existing blobs.
	existing, err := ColdList(ctx, b, cp, opts.Parallelism)
	if err != nil {
		_ = DeleteInProgressMarker(ctx, b, cp, name)
		return nil, fmt.Errorf("cas: cold-list: %w", err)
	}

	// 8. Upload missing blobs.
	uploaded, bytesUp, err := uploadMissingBlobs(ctx, b, cp, plan, existing, opts.Parallelism)
	if err != nil {
		_ = DeleteInProgressMarker(ctx, b, cp, name)
		return nil, err
	}
	res.BlobsUploaded = uploaded
	res.BytesUploaded = bytesUp
	// Reused = unique blobs that were already in the remote (dedup'd via cold-list).
	res.BlobsReused = res.UniqueBlobs - uploaded
	if res.BlobsReused < 0 {
		res.BlobsReused = 0
	}
	res.BytesReused = int64(blobBytesTotal) - bytesUp
	if res.BytesReused < 0 {
		res.BytesReused = 0
	}

	// 9. Per-(disk,db,table) archives.
	archCount, archBytes, err := uploadPartArchives(ctx, b, cp, name, plan)
	if err != nil {
		_ = DeleteInProgressMarker(ctx, b, cp, name)
		return nil, err
	}
	res.PerTableArchives = archCount
	res.ArchiveBytes = archBytes

	// 10. Per-table JSONs.
	if err := uploadTableJSONs(ctx, b, cp, name, plan); err != nil {
		_ = DeleteInProgressMarker(ctx, b, cp, name)
		return nil, err
	}

	// 11. Pre-commit safety re-checks.
	// 11a. prune marker
	if _, _, exists, err := b.StatFile(ctx, PruneMarkerPath(cp)); err != nil {
		_ = DeleteInProgressMarker(ctx, b, cp, name)
		return nil, fmt.Errorf("cas: re-check prune marker: %w", err)
	} else if exists {
		_ = DeleteInProgressMarker(ctx, b, cp, name)
		return nil, fmt.Errorf("%w: detected concurrent prune before commit", ErrPruneInProgress)
	}
	// 11b. our own inprogress marker
	if _, _, exists, err := b.StatFile(ctx, InProgressMarkerPath(cp, name)); err != nil {
		return nil, fmt.Errorf("cas: re-check inprogress marker: %w", err)
	} else if !exists {
		return nil, fmt.Errorf("cas: in-progress marker for %q was swept (upload exceeded abandon_threshold); aborting", name)
	}

	// 12. Commit: write root metadata.json.
	bm := buildBackupMetadata(name, cfg, plan)
	bmJSON, err := json.MarshalIndent(bm, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("cas: marshal metadata.json: %w", err)
	}
	if err := putBytes(ctx, b, MetadataJSONPath(cp, name), bmJSON); err != nil {
		return nil, fmt.Errorf("cas: put metadata.json: %w", err)
	}

	// 13. Best-effort: delete inprogress marker.
	_ = DeleteInProgressMarker(ctx, b, cp, name)

	return res, nil
}

// FormatObjectDiskHits renders a compact one-line summary suitable for
// embedding in user-facing errors. Exported for callers that perform the
// pre-flight outside cas.Upload (e.g., the CLI's snapshot-based scan).
func FormatObjectDiskHits(hits []ObjectDiskHit) string { return formatObjectDiskHits(hits) }

// formatObjectDiskHits renders a compact one-line summary of detected
// object-disk hits suitable for embedding in error messages.
func formatObjectDiskHits(hits []ObjectDiskHit) string {
	parts := make([]string, len(hits))
	for i, h := range hits {
		parts[i] = fmt.Sprintf("%s.%s on %s(%s)", h.Database, h.Table, h.Disk, h.DiskType)
	}
	return strings.Join(parts, ", ")
}

// planUpload walks <root>/shadow/<db>/<table>/<disk>/<part>/, parses each
// checksums.txt, and builds a uploadPlan. opts.SkipObjectDisks plus the
// disk/table info is used here to silently exclude object-disk tables
// when requested.
func planUpload(root string, threshold uint64, filter []string, skipObjectDisks bool, disks []DiskInfo, tables []TableInfo) (*uploadPlan, error) {
	shadow := filepath.Join(root, "shadow")
	st, err := os.Stat(shadow)
	if err != nil {
		return nil, fmt.Errorf("cas: stat shadow dir: %w", err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("cas: shadow path %q is not a directory", shadow)
	}

	excluded := excludedTables(skipObjectDisks, disks, tables)

	plan := &uploadPlan{
		blobs:     make(map[Hash128]blobRef),
		tables:    make(map[string]*tablePlan),
		localRoot: root,
	}

	// Walk shadow/<db>/<table>/<disk>/<part>/
	dbs, err := readDir(shadow)
	if err != nil {
		return nil, err
	}
	for _, db := range dbs {
		dbDir := filepath.Join(shadow, db)
		tbls, err := readDir(dbDir)
		if err != nil {
			return nil, err
		}
		for _, table := range tbls {
			if !tableFilterAllows(filter, db, table) {
				continue
			}
			if excluded[db+"."+table] {
				continue
			}
			tblDir := filepath.Join(dbDir, table)
			diskNames, err := readDir(tblDir)
			if err != nil {
				return nil, err
			}
			for _, disk := range diskNames {
				diskDir := filepath.Join(tblDir, disk)
				parts, err := readDir(diskDir)
				if err != nil {
					return nil, err
				}
				key := disk + "|" + db + "|" + table
				tp, ok := plan.tables[key]
				if !ok {
					tp = &tablePlan{Disk: disk, DB: db, Table: table}
					plan.tables[key] = tp
					plan.tableKeys = append(plan.tableKeys, key)
				}
				for _, part := range parts {
					partDir := filepath.Join(diskDir, part)
					if err := planPart(partDir, part, threshold, plan, tp); err != nil {
						return nil, fmt.Errorf("cas: plan %s/%s/%s/%s: %w", db, table, disk, part, err)
					}
					tp.parts = append(tp.parts, metadata.Part{Name: part})
				}
			}
		}
	}

	// Deterministic ordering.
	sort.Strings(plan.tableKeys)
	for _, tp := range plan.tables {
		sort.Slice(tp.parts, func(i, j int) bool { return tp.parts[i].Name < tp.parts[j].Name })
		sort.Slice(tp.archiveEntries, func(i, j int) bool { return tp.archiveEntries[i].NameInArchive < tp.archiveEntries[j].NameInArchive })
	}
	return plan, nil
}

// excludedTables returns a set of "db.table" keys to skip, based on
// object-disk detection. Returns an empty set when the pre-flight is
// not requested (skipObjectDisks==false OR disks/tables empty); the
// caller-side refusal in step 3 handles that case.
func excludedTables(skipObjectDisks bool, disks []DiskInfo, tables []TableInfo) map[string]bool {
	out := make(map[string]bool)
	if !skipObjectDisks || len(disks) == 0 || len(tables) == 0 {
		return out
	}
	for _, h := range DetectObjectDiskTables(tables, disks) {
		out[h.Database+"."+h.Table] = true
	}
	return out
}

// planPart parses partDir/checksums.txt, classifies entries, and
// updates plan + tp accordingly.
//
// Classification rules (§6.3):
//   - "checksums.txt" itself: always inline (it gates the restore protocol).
//   - Files listed in checksums.txt with size <= threshold: inline.
//   - Files listed in checksums.txt with size > threshold: blob.
//   - Files on disk but NOT in checksums.txt: TODO — should be inlined per
//     §6.3, but real ClickHouse parts always list every file. Currently
//     unhandled; tests only exercise the "fully listed" case.
func planPart(partDir, partName string, threshold uint64, plan *uploadPlan, tp *tablePlan) error {
	ckPath := filepath.Join(partDir, "checksums.txt")
	f, err := os.Open(ckPath)
	if err != nil {
		return fmt.Errorf("open checksums.txt: %w", err)
	}
	parsed, perr := checksumstxt.Parse(f)
	_ = f.Close()
	if perr != nil {
		return fmt.Errorf("parse checksums.txt: %w", perr)
	}

	// checksums.txt is always inline. Stat it for byte accounting.
	tp.archiveEntries = append(tp.archiveEntries, ArchiveEntry{
		NameInArchive: partName + "/checksums.txt",
		LocalPath:     ckPath,
	})
	if st, err := os.Stat(ckPath); err == nil {
		plan.totalFiles++
		plan.inlineFiles++
		plan.totalBytes += uint64(st.Size())
		plan.inlineBytes += uint64(st.Size())
	}

	for fname, c := range parsed.Files {
		local := filepath.Join(partDir, fname)
		plan.totalFiles++
		plan.totalBytes += c.FileSize
		if c.FileSize <= threshold {
			tp.archiveEntries = append(tp.archiveEntries, ArchiveEntry{
				NameInArchive: partName + "/" + fname,
				LocalPath:     local,
			})
			plan.inlineFiles++
			plan.inlineBytes += c.FileSize
			continue
		}
		// Blob: count every reference (pre-dedup); dedup happens in plan.blobs map.
		plan.blobFiles++
		h := Hash128{Low: c.FileHash.Low, High: c.FileHash.High}
		if _, ok := plan.blobs[h]; !ok {
			plan.blobs[h] = blobRef{LocalPath: local, Size: c.FileSize}
		}
	}
	return nil
}

// tableFilterAllows returns true if the given (db, table) is permitted
// by the filter. Empty filter = allow-all. Match is exact "db.table"
// for v1 of CAS; glob support deferred (TODO).
func tableFilterAllows(filter []string, db, table string) bool {
	if len(filter) == 0 {
		return true
	}
	full := db + "." + table
	for _, f := range filter {
		if f == full {
			return true
		}
	}
	return false
}

// uploadMissingBlobs PUTs every blob in plan.blobs that is not in the
// existing set. Concurrency capped by parallelism (<=0 → 16).
func uploadMissingBlobs(ctx context.Context, b Backend, cp string, plan *uploadPlan, existing *ExistenceSet, parallelism int) (int, int64, error) {
	if parallelism <= 0 {
		parallelism = 16
	}
	type job struct {
		h    Hash128
		ref  blobRef
	}
	var jobs []job
	for h, ref := range plan.blobs {
		if existing.Has(h) {
			continue
		}
		jobs = append(jobs, job{h: h, ref: ref})
	}
	// Deterministic ordering aids debugging/tests.
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].h.High != jobs[j].h.High {
			return jobs[i].h.High < jobs[j].h.High
		}
		return jobs[i].h.Low < jobs[j].h.Low
	})

	var (
		mu        sync.Mutex
		uploaded  int
		bytesUp   int64
		firstErr  error
	)

	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	for _, j := range jobs {
		j := j
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			mu.Lock()
			already := firstErr != nil
			mu.Unlock()
			if already {
				return
			}
			f, err := os.Open(j.ref.LocalPath)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: open blob source %s: %w", j.ref.LocalPath, err)
				}
				mu.Unlock()
				return
			}
			// Production storage backends (S3, GCS, AzBlob) do NOT close the
			// io.ReadCloser passed to PutFile — they just stream Body off it
			// and return. Without an explicit defer here, every blob upload
			// would leak one fd, exhausting the process limit on backups
			// with thousands of blobs. The fakedst test backend DOES call
			// r.Close, which masks the leak in unit tests; keep both
			// behaviors compatible by closing here ourselves (double-close
			// of *os.File is a no-op error we ignore).
			defer f.Close()
			err = b.PutFile(ctx, BlobPath(cp, j.h), f, int64(j.ref.Size))
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: put blob %s: %w", BlobPath(cp, j.h), err)
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			uploaded++
			bytesUp += int64(j.ref.Size)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return uploaded, bytesUp, firstErr
}

// uploadPartArchives builds and PUTs one tar.zstd per (disk, db, table).
func uploadPartArchives(ctx context.Context, b Backend, cp, name string, plan *uploadPlan) (int, int64, error) {
	count := 0
	var totalBytes int64
	for _, key := range plan.tableKeys {
		tp := plan.tables[key]
		if len(tp.archiveEntries) == 0 {
			continue
		}
		var buf bytes.Buffer
		if err := WriteArchive(&buf, tp.archiveEntries); err != nil {
			return count, totalBytes, fmt.Errorf("cas: write archive %s/%s/%s: %w", tp.Disk, tp.DB, tp.Table, err)
		}
		key := PartArchivePath(cp, name, tp.Disk, tp.DB, tp.Table)
		size := int64(buf.Len())
		if err := putBytes(ctx, b, key, buf.Bytes()); err != nil {
			return count, totalBytes, fmt.Errorf("cas: put archive %s: %w", key, err)
		}
		count++
		totalBytes += size
	}
	return count, totalBytes, nil
}

// uploadTableJSONs writes per-(db, table) TableMetadata JSONs at
// cas/<cluster>/metadata/<backup>/metadata/<enc_db>/<enc_table>.json.
//
// One JSON per (db, table) — multiple disks are merged into a single
// file with Parts keyed by disk.
func uploadTableJSONs(ctx context.Context, b Backend, cp, name string, plan *uploadPlan) error {
	// Group plan tables by (db, table) -> []*tablePlan (one per disk).
	type dbTable struct{ DB, Table string }
	grouped := make(map[dbTable][]*tablePlan)
	var keys []dbTable
	for _, k := range plan.tableKeys {
		tp := plan.tables[k]
		dt := dbTable{DB: tp.DB, Table: tp.Table}
		if _, ok := grouped[dt]; !ok {
			keys = append(keys, dt)
		}
		grouped[dt] = append(grouped[dt], tp)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].DB != keys[j].DB {
			return keys[i].DB < keys[j].DB
		}
		return keys[i].Table < keys[j].Table
	})

	for _, dt := range keys {
		tps := grouped[dt]
		tm := metadata.TableMetadata{
			Database:     dt.DB,
			Table:        dt.Table,
			Parts:        make(map[string][]metadata.Part),
			MetadataOnly: false,
		}
		for _, tp := range tps {
			tm.Parts[tp.Disk] = append(tm.Parts[tp.Disk], tp.parts...)
		}
		// Merge schema fields from the v1 per-table metadata that
		// `clickhouse-backup create` wrote to disk. Required so cas-restore
		// on a fresh host can issue CREATE TABLE; without these fields the
		// v1 restore handoff produces an empty Query and fails.
		local, err := readLocalTableMetadata(plan.localRoot, dt.DB, dt.Table)
		if err != nil {
			return fmt.Errorf("cas: read local table metadata for %s.%s: %w", dt.DB, dt.Table, err)
		}
		tm.Query = local.Query
		tm.UUID = local.UUID
		tm.TotalBytes = local.TotalBytes
		tm.Size = local.Size
		tm.DependenciesTable = local.DependenciesTable
		tm.DependenciesDatabase = local.DependenciesDatabase
		tm.Mutations = local.Mutations
		body, err := json.MarshalIndent(&tm, "", "\t")
		if err != nil {
			return fmt.Errorf("cas: marshal table metadata %s.%s: %w", dt.DB, dt.Table, err)
		}
		key := TableMetaPath(cp, name, dt.DB, dt.Table)
		if err := putBytes(ctx, b, key, body); err != nil {
			return fmt.Errorf("cas: put table metadata %s: %w", key, err)
		}
	}
	return nil
}

// readLocalTableMetadata reads <root>/metadata/<db>/<table>.json that
// `clickhouse-backup create` wrote. Returns a zero-value TableMetadata
// + nil error if the file is missing — older create flows or test
// fixtures may omit it; the caller logs and ships an empty schema in
// that case (degrading fresh-host restore but not breaking
// table-already-exists restore).
func readLocalTableMetadata(root, db, table string) (metadata.TableMetadata, error) {
	p := filepath.Join(root, "metadata", db, table+".json")
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warn().Str("path", p).Msg("cas: local v1 per-table metadata missing; uploaded schema fields will be empty")
			return metadata.TableMetadata{}, nil
		}
		return metadata.TableMetadata{}, fmt.Errorf("cas: open %s: %w", p, err)
	}
	defer f.Close()
	var tm metadata.TableMetadata
	if err := json.NewDecoder(f).Decode(&tm); err != nil {
		return metadata.TableMetadata{}, fmt.Errorf("cas: parse %s: %w", p, err)
	}
	return tm, nil
}

// buildBackupMetadata constructs the root BackupMetadata for the commit
// step. We populate the minimum needed to round-trip via ValidateBackup
// + future cas-download. Fields that depend on live ClickHouse (UUID,
// CreationDate-from-ClickHouse, etc.) are populated by the caller in
// later tasks.
func buildBackupMetadata(name string, cfg Config, plan *uploadPlan) *metadata.BackupMetadata {
	// Build Tables list deterministically.
	type dbTable struct{ DB, Table string }
	seen := make(map[dbTable]struct{})
	var tables []metadata.TableTitle
	for _, k := range plan.tableKeys {
		tp := plan.tables[k]
		dt := dbTable{DB: tp.DB, Table: tp.Table}
		if _, ok := seen[dt]; ok {
			continue
		}
		seen[dt] = struct{}{}
		tables = append(tables, metadata.TableTitle{Database: tp.DB, Table: tp.Table})
	}
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Database != tables[j].Database {
			return tables[i].Database < tables[j].Database
		}
		return tables[i].Table < tables[j].Table
	})

	return &metadata.BackupMetadata{
		BackupName:   name,
		CreationDate: time.Now().UTC(),
		DataFormat:   "directory",
		Tables:       tables,
		CAS: &metadata.CASBackupParams{
			LayoutVersion:   LayoutVersion,
			InlineThreshold: cfg.InlineThreshold,
			ClusterID:       cfg.ClusterID,
		},
	}
}

// readDir returns the names of entries in dir. Empty slice and nil
// error if the directory exists but is empty.
func readDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

