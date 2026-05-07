package cas

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/checksumstxt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
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

	// ExcludedTables is a precomputed list of "db.table" keys to skip.
	// When non-empty, planUpload skips these tables directly without
	// invoking DetectObjectDiskTables. Used by callers (e.g. cas-upload
	// CLI) that already know which tables are object-disk-backed via a
	// snapshot walk and don't need the live-disks Path-prefix match.
	// If both ExcludedTables and Disks/ClickHouseTables are provided,
	// ExcludedTables takes priority and Disks/ClickHouseTables are
	// ignored for exclusion.
	ExcludedTables []string
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
	plan, err := planUpload(opts.LocalBackupDir, cfg.InlineThreshold, opts.TableFilter, opts.SkipObjectDisks, opts.ExcludedTables, opts.Disks, opts.ClickHouseTables)
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

// localTableMetadataEntry is one (db, table) pair discovered by walking
// <root>/metadata/. Names are post-decode (i.e. ready to use directly,
// no further TablePathDecode needed).
type localTableMetadataEntry struct {
	DB, Table string
	JSONPath  string // absolute path to the metadata JSON
}

// enumerateLocalTableMetadata walks <root>/metadata/<db_enc>/<table_enc>.json
// and returns one entry per file. The (db, table) names come from the JSON
// body's "database" / "table" fields, NOT from the on-disk path components,
// so the result is unambiguous and never depends on TablePathDecode.
func enumerateLocalTableMetadata(root string) ([]localTableMetadataEntry, error) {
	metaRoot := filepath.Join(root, "metadata")
	st, err := os.Stat(metaRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no metadata dir → no tables (caller decides what to do)
		}
		return nil, fmt.Errorf("stat metadata dir: %w", err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("metadata path %q is not a directory", metaRoot)
	}
	var out []localTableMetadataEntry
	dbs, err := readDir(metaRoot)
	if err != nil {
		return nil, err
	}
	for _, dbEnc := range dbs {
		dbDir := filepath.Join(metaRoot, dbEnc)
		dbSt, err := os.Stat(dbDir)
		if err != nil {
			return nil, fmt.Errorf("stat metadata db dir %s: %w", dbDir, err)
		}
		if !dbSt.IsDir() {
			continue // e.g., a stray file alongside the db directories
		}
		entries, err := readDir(dbDir)
		if err != nil {
			return nil, err
		}
		for _, name := range entries {
			if !strings.HasSuffix(name, ".json") {
				continue
			}
			p := filepath.Join(dbDir, name)
			body, err := os.ReadFile(p)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", p, err)
			}
			var tm metadata.TableMetadata
			if err := json.Unmarshal(body, &tm); err != nil {
				return nil, fmt.Errorf("parse %s: %w", p, err)
			}
			if tm.Database == "" || tm.Table == "" {
				return nil, fmt.Errorf("metadata JSON %s has empty database/table fields", p)
			}
			out = append(out, localTableMetadataEntry{
				DB:       tm.Database,
				Table:    tm.Table,
				JSONPath: p,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DB != out[j].DB {
			return out[i].DB < out[j].DB
		}
		return out[i].Table < out[j].Table
	})
	return out, nil
}

// planUpload enumerates tables from <root>/metadata/, then for each
// (db, table) walks <root>/shadow/<db_enc>/<table_enc>/<disk>/<part>/ to
// classify files. Tables with no shadow dir produce a tableKey entry with
// no parts — these flow through to bm.Tables without an archive.
//
// When skipObjectDisks is true, the planner consults precomputed first
// (a precomputed db.table allow-list provided by the CLI's snapshot-based
// pre-flight) and falls through to DetectObjectDiskTables(disks, tables)
// when that list is empty. Either path silently excludes object-disk-
// backed tables.
func planUpload(root string, threshold uint64, filter []string, skipObjectDisks bool, precomputed []string, disks []DiskInfo, tables []TableInfo) (*uploadPlan, error) {
	excluded := excludedTables(skipObjectDisks, precomputed, disks, tables)

	plan := &uploadPlan{
		blobs:     make(map[Hash128]blobRef),
		tables:    make(map[string]*tablePlan),
		localRoot: root,
	}

	tableEntries, err := enumerateLocalTableMetadata(root)
	if err != nil {
		return nil, err
	}

	shadow := filepath.Join(root, "shadow")
	for _, te := range tableEntries {
		db, table := te.DB, te.Table
		if !tableFilterAllows(filter, db, table) {
			continue
		}
		if excluded[db+"."+table] {
			continue
		}

		// Find part directories for this table by walking
		// shadow/<db_enc>/<table_enc>/<disk>/<part>/. Missing or empty is
		// fine (schema-only / empty-table case).
		dbEnc := common.TablePathEncode(db)
		tableEnc := common.TablePathEncode(table)
		tblDir := filepath.Join(shadow, dbEnc, tableEnc)
		st, statErr := os.Stat(tblDir)
		if statErr != nil || !st.IsDir() {
			// No shadow dir — schema-only or empty table. Register a
			// tablePlan with the default disk slot so buildBackupMetadata
			// emits a Tables entry; no parts, no archive.
			key := "default|" + db + "|" + table
			if _, ok := plan.tables[key]; !ok {
				plan.tables[key] = &tablePlan{Disk: "default", DB: db, Table: table}
				plan.tableKeys = append(plan.tableKeys, key)
			}
			continue
		}
		diskNames, err := readDir(tblDir)
		if err != nil {
			return nil, err
		}
		anyParts := false
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
				anyParts = true
			}
		}
		if !anyParts {
			// Empty shadow tree → still register a Tables entry on a
			// default disk slot so cas-restore can recreate the schema.
			key := "default|" + db + "|" + table
			if _, ok := plan.tables[key]; !ok {
				plan.tables[key] = &tablePlan{Disk: "default", DB: db, Table: table}
				plan.tableKeys = append(plan.tableKeys, key)
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

// excludedTables returns a set of "db.table" keys to skip when
// skipObjectDisks is true. Two paths:
//  1. precomputed: caller passed an explicit list (used by the CLI's
//     snapshot-driven flow that doesn't have live disk paths).
//  2. derived: caller passed DiskInfo + TableInfo, in which case we run
//     DetectObjectDiskTables (used by tests that model live ClickHouse).
//
// When both are empty, returns an empty set (effectively a no-op).
func excludedTables(skipObjectDisks bool, precomputed []string, disks []DiskInfo, tables []TableInfo) map[string]bool {
	out := make(map[string]bool)
	if !skipObjectDisks {
		return out
	}
	if len(precomputed) > 0 {
		for _, k := range precomputed {
			out[k] = true
		}
		return out
	}
	if len(disks) == 0 || len(tables) == 0 {
		return out
	}
	for _, h := range DetectObjectDiskTables(tables, disks) {
		out[h.Database+"."+h.Table] = true
	}
	return out
}

// planPart classifies a single part directory using the two-pass walker.
//
//	Pass 1: parse checksums.txt recursively (descending into .proj/ subdirs)
//	        and build extractSet = { rel_path → (hash, size) } for every
//	        above-threshold listed file.
//	Pass 2: walk the part directory recursively. For each file:
//	          - rel_path in extractSet → register a blob ref.
//	          - otherwise              → append an archive entry preserving
//	                                     <partName>/<rel_path>.
//	        Hidden / non-regular files: warn, skip.
//	        .proj directories not in any parent's extractSet: warn, skip.
func planPart(partDir, partName string, threshold uint64, plan *uploadPlan, tp *tablePlan) error {
	extractSet, knownProjDirs, err := buildExtractSet(partDir, threshold)
	if err != nil {
		return err
	}
	return walkPartFiles(partDir, partName, extractSet, knownProjDirs, plan, tp)
}

// extractEntry holds the blob target for one above-threshold file.
type extractEntry struct {
	Hash Hash128
	Size uint64
}

// buildExtractSet recursively parses checksums.txt files starting at
// partRoot. Returns:
//   - extractSet: rel_path → (hash, size) for every above-threshold
//     non-.proj checksum entry, recursively. rel_path is relative to
//     partRoot and uses forward slashes (e.g. "data.bin", "p1.proj/data.bin").
//   - knownProjDirs: rel_path → struct{} for every .proj directory referenced
//     by some checksums.txt at any level. Used in pass 2 to distinguish
//     legitimate projection subtrees from orphans.
//
// Strict failures: missing/unparseable .proj/checksums.txt; .proj entry
// whose target is missing or not a directory; non-.proj entry whose file
// is missing on disk.
func buildExtractSet(partRoot string, threshold uint64) (map[string]extractEntry, map[string]struct{}, error) {
	extractSet := map[string]extractEntry{}
	knownProj := map[string]struct{}{}
	var recurse func(dir, relPrefix string) error
	recurse = func(dir, relPrefix string) error {
		ckPath := filepath.Join(dir, "checksums.txt")
		f, err := os.Open(ckPath)
		if err != nil {
			return fmt.Errorf("open %s: %w", ckPath, err)
		}
		parsed, perr := checksumstxt.Parse(f)
		_ = f.Close()
		if perr != nil {
			return fmt.Errorf("parse %s: %w", ckPath, perr)
		}
		for fname, c := range parsed.Files {
			rel := relPrefix + fname
			// validate ALL filenames first — including .proj entries — to prevent
			// directory traversal via crafted remote checksums.txt content.
			// Upload side trusts local filesystem but applies the same validator
			// for defense in depth.
			if err := validateChecksumsTxtFilename(fname); err != nil {
				return fmt.Errorf("cas: %s: %w", ckPath, err)
			}
			if strings.HasSuffix(fname, ".proj") {
				subDir := filepath.Join(dir, fname)
				st, statErr := os.Stat(subDir)
				if statErr != nil {
					return fmt.Errorf("projection subdir %s: %w", subDir, statErr)
				}
				if !st.IsDir() {
					return fmt.Errorf("projection entry %q in %s: target on disk is not a directory", fname, ckPath)
				}
				knownProj[rel] = struct{}{}
				if err := recurse(subDir, rel+"/"); err != nil {
					return err
				}
				continue
			}
			localPath := filepath.Join(dir, fname)
			if _, err := os.Stat(localPath); err != nil {
				return fmt.Errorf("file listed in %s missing on disk: %s", ckPath, fname)
			}
			if c.FileSize > threshold {
				extractSet[rel] = extractEntry{
					Hash: Hash128{Low: c.FileHash.Low, High: c.FileHash.High},
					Size: c.FileSize,
				}
			}
		}
		return nil
	}
	if err := recurse(partRoot, ""); err != nil {
		return nil, nil, err
	}
	return extractSet, knownProj, nil
}

// walkPartFiles is pass 2: walk the on-disk part directory, route each
// regular file to either the blob store (if rel_path is in extractSet)
// or the archive (everything else, paths preserved).
//
// Hidden files (name starts with ".") and non-regular files (symlinks,
// sockets, devices) generate a Warn log and are skipped.
// .proj directories not in knownProj are also warn-and-skipped.
func walkPartFiles(partRoot, partName string, extractSet map[string]extractEntry, knownProj map[string]struct{}, plan *uploadPlan, tp *tablePlan) error {
	return filepath.WalkDir(partRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == partRoot {
			return nil
		}
		rel, err := filepath.Rel(partRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if strings.HasSuffix(rel, ".proj") {
				if _, ok := knownProj[rel]; !ok {
					log.Warn().Str("part", partName).Str("rel", rel).Msg("cas-upload: orphan .proj directory in part — skipping")
					return filepath.SkipDir
				}
			}
			return nil
		}
		base := filepath.Base(path)
		if strings.HasPrefix(base, ".") {
			log.Warn().Str("part", partName).Str("rel", rel).Msg("cas-upload: hidden file in part — skipping")
			return nil
		}
		if !d.Type().IsRegular() {
			log.Warn().Str("part", partName).Str("rel", rel).Msg("cas-upload: non-regular file in part — skipping")
			return nil
		}
		if entry, ok := extractSet[rel]; ok {
			plan.totalFiles++
			plan.totalBytes += entry.Size
			plan.blobFiles++
			if _, dup := plan.blobs[entry.Hash]; !dup {
				plan.blobs[entry.Hash] = blobRef{LocalPath: path, Size: entry.Size}
			}
			return nil
		}
		st, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("stat %s: %w", path, err)
		}
		size := uint64(st.Size())
		tp.archiveEntries = append(tp.archiveEntries, ArchiveEntry{
			NameInArchive: partName + "/" + rel,
			LocalPath:     path,
		})
		plan.totalFiles++
		plan.totalBytes += size
		plan.inlineFiles++
		plan.inlineBytes += size
		return nil
	})
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
			if len(tp.parts) == 0 {
				// Schema-only / empty table: no per-disk parts. Don't insert a
				// disk key at all — downstream (cas-download) ranges over
				// tm.Parts and would otherwise try to fetch a nonexistent
				// per-table archive for that disk.
				continue
			}
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

// readLocalTableMetadata reads <root>/metadata/<TablePathEncode(db)>/<TablePathEncode(table)>.json
// that `clickhouse-backup create` wrote. The on-disk path is always
// percent-encoded (matching create's filesystem layout); the caller
// passes db/table as DECODED identifiers, and this helper applies the
// encoding for the lookup. Returns a zero-value TableMetadata + nil
// error if the file is missing — older create flows or test fixtures
// may omit it; the caller logs and ships an empty schema in that case
// (degrading fresh-host restore but not breaking table-already-exists
// restore).
func readLocalTableMetadata(root, db, table string) (metadata.TableMetadata, error) {
	p := filepath.Join(root, "metadata", common.TablePathEncode(db), common.TablePathEncode(table)+".json")
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

