package cas

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/Altinity/clickhouse-backup/v2/pkg/checksumstxt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

// DownloadOptions configures a Download run.
type DownloadOptions struct {
	// LocalBackupDir is the root under which Download materializes
	// <LocalBackupDir>/<name>/. The directory is created if missing.
	LocalBackupDir string

	// TableFilter is an optional list of "db.table" exact-match filters.
	// Empty means all tables in the backup.
	TableFilter []string

	// Partitions is an optional part-name filter applied at the part level
	// (intersected with TableMetadata.Parts). Empty means all parts.
	Partitions []string

	// SchemaOnly: skip archive download + blob fetch; only write JSON
	// metadata files locally.
	SchemaOnly bool

	// DataOnly: in v1 of CAS this behaves like a full download (CAS only
	// stores data; schema info comes from the per-table JSON which is
	// always written). Reserved for future use.
	DataOnly bool

	// Parallelism caps simultaneous archive + blob fetches. <=0 falls
	// back to 16.
	Parallelism int
}

// DownloadResult summarizes what a Download run did.
type DownloadResult struct {
	LocalBackupDir   string
	BackupName       string
	PerTableArchives int
	BlobsFetched     int
	BytesFetched     int64
}

// projRe matches a projection-style nested filename: <name>.proj/<file>.
var projRe = regexp.MustCompile(`^[^/\x00]+\.proj/[^/\x00]+$`)

// validateRemoteFilesystemName rejects disk and part names from remote
// metadata before they are joined into local filesystem paths. A
// compromised or adversarially crafted CAS bucket could otherwise direct
// archive extraction or blob writes outside the intended local backup
// directory by setting `disk = "../../etc"` or `part_name = "../escape"`.
//
// label is only used in the error message ("disk", "part name", etc.).
func validateRemoteFilesystemName(label, name string) error {
	if name == "" || name == "." || name == ".." {
		return fmt.Errorf("cas: unsafe %s in remote metadata: %q", label, name)
	}
	if strings.ContainsAny(name, "/\\\x00") {
		return fmt.Errorf("cas: unsafe %s (path separator or NUL) in remote metadata: %q", label, name)
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("cas: unsafe %s (contains %q) in remote metadata: %q", label, "..", name)
	}
	return nil
}

// validateChecksumsTxtFilename rejects unsafe filenames listed in a
// part's checksums.txt. See docs/cas-design.md §6.5 step 5.
func validateChecksumsTxtFilename(name string) error {
	if name == "" {
		return errors.New("cas: empty filename in checksums.txt")
	}
	if strings.ContainsRune(name, 0) {
		return errors.New("cas: NUL in filename")
	}
	if strings.HasPrefix(name, "/") {
		return errors.New("cas: absolute filename")
	}
	if strings.Contains(name, "..") {
		return errors.New("cas: \"..\" in filename")
	}
	if strings.Contains(name, "/") && !projRe.MatchString(name) {
		return errors.New("cas: nested path in filename")
	}
	return nil
}

// Download materializes a v1-shaped local backup directory from a CAS
// backup. Implements docs/cas-design.md §6.5 (the cas-download portion;
// cas-restore is layered on top in Task 14).
func Download(ctx context.Context, b Backend, cfg Config, name string, opts DownloadOptions) (*DownloadResult, error) {
	if opts.LocalBackupDir == "" {
		return nil, errors.New("cas: DownloadOptions.LocalBackupDir is required")
	}

	// 1. Validate root metadata + persisted CAS params.
	bm, err := ValidateBackup(ctx, b, cfg, name)
	if err != nil {
		return nil, err
	}

	cp := cfg.ClusterPrefix()

	// 2. Set up local layout.
	localDir := filepath.Join(opts.LocalBackupDir, name)
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return nil, fmt.Errorf("cas: mkdir %s: %w", localDir, err)
	}

	res := &DownloadResult{
		LocalBackupDir: localDir,
		BackupName:     name,
	}

	// 3. Determine in-scope (db, table) by applying TableFilter to bm.Tables.
	inScope := selectTables(bm.Tables, opts.TableFilter)
	if len(inScope) == 0 && len(opts.TableFilter) > 0 {
		// Filter excluded everything; that's not necessarily an error,
		// but we still write the root metadata.json and return.
	}

	// 4. Fetch + persist per-table TableMetadata (with optional partition filter).
	type tableEntry struct {
		DB, Table string
		TM        metadata.TableMetadata
	}
	tables := make([]tableEntry, 0, len(inScope))
	partsFilter := makePartsFilter(opts.Partitions)
	for _, tt := range inScope {
		tm, err := fetchTableMetadata(ctx, b, cp, name, tt.Database, tt.Table)
		if err != nil {
			return nil, err
		}
		if partsFilter != nil {
			tm.Parts = filterParts(tm.Parts, partsFilter)
		}
		// Save to local disk under metadata/<enc_db>/<enc_table>.json.
		if err := saveLocalTableMetadata(localDir, tm); err != nil {
			return nil, err
		}
		tables = append(tables, tableEntry{DB: tt.Database, Table: tt.Table, TM: *tm})
	}

	// 5. Save root metadata.json (post per-table writes so a failure mid-
	// download leaves the catalog untouched on disk; Save order doesn't
	// matter for correctness — both are required for restore).
	//
	// We strip BackupMetadata.CAS from the local copy so that the existing
	// v1 restore flow accepts the handoff. The cross-mode guard in
	// pkg/backup/restore.go refuses to operate on backups where CAS != nil
	// — that guard is intentional for direct v1 invocation, but cas-restore
	// has already validated the backup at the CAS layer and is materializing
	// a v1-shaped local layout. Stripping the field here keeps the on-disk
	// layout indistinguishable from a v1 directory-format backup, which is
	// the contract §6.5 specifies.
	bmLocal := *bm
	bmLocal.CAS = nil
	bmPath := filepath.Join(localDir, "metadata.json")
	bmBody, err := json.MarshalIndent(&bmLocal, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("cas: marshal local metadata.json: %w", err)
	}
	if err := os.WriteFile(bmPath, bmBody, 0o640); err != nil {
		return nil, fmt.Errorf("cas: write %s: %w", bmPath, err)
	}

	if opts.SchemaOnly {
		return res, nil
	}

	// 6. Disk-space pre-flight (best-effort): estimate archive bytes via
	// StatFile; we don't pre-fetch blob sizes (would require parsing
	// checksums.txt before downloading the archives, doubling round-trips).
	// We compare archive total to filesystem free space and bail early on
	// obvious shortage; blob size is added after archive extraction.
	estimateArchiveBytes := int64(0)
	var archives []archiveJob
	for _, te := range tables {
		for disk, parts := range te.TM.Parts {
			// Reject path-traversal in remote-supplied disk and part names
			// BEFORE they participate in any path construction (incl. the
			// archive key passed to StatFile, which in turn flows into the
			// local filesystem path during extraction).
			if err := validateRemoteFilesystemName("disk", disk); err != nil {
				return nil, err
			}
			for _, p := range parts {
				if err := validateRemoteFilesystemName("part name", p.Name); err != nil {
					return nil, err
				}
			}
			key := PartArchivePath(cp, name, disk, te.DB, te.Table)
			sz, _, exists, err := b.StatFile(ctx, key)
			if err != nil {
				return nil, fmt.Errorf("cas: stat archive %s: %w", key, err)
			}
			if !exists {
				// A backup with parts on this disk should have an archive;
				// missing implies a corrupted backup.
				return nil, fmt.Errorf("cas: archive missing: %s", key)
			}
			archives = append(archives, archiveJob{
				Disk: disk, DB: te.DB, Table: te.Table, Key: key, Size: sz,
			})
			estimateArchiveBytes += sz
		}
	}
	// Best-effort free-space check on the local dir's filesystem. We
	// only have archive sizes here; blob bytes get added during extraction
	// pass below. With a 1.1x safety multiplier this catches gross-shortage
	// cases without delaying the download with a second round-trip.
	if err := checkFreeSpace(localDir, estimateArchiveBytes); err != nil {
		return nil, err
	}

	// 7. Download + extract archives (bounded parallelism).
	parallelism := opts.Parallelism
	if parallelism <= 0 {
		parallelism = 16
	}

	if err := downloadArchives(ctx, b, archives, localDir, parallelism); err != nil {
		return nil, err
	}
	res.PerTableArchives = len(archives)

	// 8. For each in-scope part: parse the on-disk checksums.txt and
	// fetch every blob whose size exceeds the persisted threshold.
	var blobs []blobJob
	estimateBlobBytes := int64(0)
	for _, te := range tables {
		for disk, parts := range te.TM.Parts {
			if err := validateRemoteFilesystemName("disk", disk); err != nil {
				return nil, err
			}
			for _, p := range parts {
				if err := validateRemoteFilesystemName("part name", p.Name); err != nil {
					return nil, err
				}
				partDir := filepath.Join(localDir, "shadow",
					common.TablePathEncode(te.DB),
					common.TablePathEncode(te.Table),
					disk, p.Name)
				ckPath := filepath.Join(partDir, "checksums.txt")
				f, err := os.Open(ckPath)
				if err != nil {
					return nil, fmt.Errorf("cas: open %s: %w", ckPath, err)
				}
				parsed, perr := checksumstxt.Parse(f)
				_ = f.Close()
				if perr != nil {
					return nil, fmt.Errorf("cas: parse %s: %w", ckPath, perr)
				}
				// Deterministic ordering for tests + debugging.
				names := make([]string, 0, len(parsed.Files))
				for n := range parsed.Files {
					names = append(names, n)
				}
				sort.Strings(names)
				for _, fname := range names {
					if err := validateChecksumsTxtFilename(fname); err != nil {
						return nil, fmt.Errorf("cas: %s: %w", ckPath, err)
					}
					c := parsed.Files[fname]
					if c.FileSize <= bm.CAS.InlineThreshold {
						continue
					}
					blobs = append(blobs, blobJob{
						PartDir:  partDir,
						FileName: fname,
						Size:     c.FileSize,
						Hash:     Hash128{Low: c.FileHash.Low, High: c.FileHash.High},
					})
					estimateBlobBytes += int64(c.FileSize)
				}
			}
		}
	}
	// Re-check free space now that we know blob bytes too.
	if err := checkFreeSpace(localDir, estimateBlobBytes); err != nil {
		return nil, err
	}

	fetched, bytesFetched, err := downloadBlobs(ctx, b, cp, blobs, parallelism)
	if err != nil {
		return nil, err
	}
	res.BlobsFetched = fetched
	res.BytesFetched = bytesFetched

	return res, nil
}

// selectTables filters bm.Tables by an exact "db.table" filter list.
// Empty filter → all tables.
func selectTables(all []metadata.TableTitle, filter []string) []metadata.TableTitle {
	if len(filter) == 0 {
		out := make([]metadata.TableTitle, len(all))
		copy(out, all)
		return out
	}
	allow := make(map[string]bool, len(filter))
	for _, f := range filter {
		allow[f] = true
	}
	var out []metadata.TableTitle
	for _, t := range all {
		if allow[t.Database+"."+t.Table] {
			out = append(out, t)
		}
	}
	return out
}

// makePartsFilter builds a name-set or returns nil for "no filter".
func makePartsFilter(names []string) map[string]bool {
	if len(names) == 0 {
		return nil
	}
	out := make(map[string]bool, len(names))
	for _, n := range names {
		out[n] = true
	}
	return out
}

// filterParts returns a copy of parts keeping only entries whose Name
// is in the allow set. Disks with no surviving parts are dropped.
func filterParts(parts map[string][]metadata.Part, allow map[string]bool) map[string][]metadata.Part {
	if allow == nil {
		return parts
	}
	out := make(map[string][]metadata.Part, len(parts))
	for disk, ps := range parts {
		var kept []metadata.Part
		for _, p := range ps {
			if allow[p.Name] {
				kept = append(kept, p)
			}
		}
		if len(kept) > 0 {
			out[disk] = kept
		}
	}
	return out
}

// fetchTableMetadata GETs the per-table JSON and parses it.
func fetchTableMetadata(ctx context.Context, b Backend, cp, name, db, table string) (*metadata.TableMetadata, error) {
	key := TableMetaPath(cp, name, db, table)
	rc, err := b.GetFile(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("cas: get %s: %w", key, err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("cas: read %s: %w", key, err)
	}
	var tm metadata.TableMetadata
	if err := json.Unmarshal(body, &tm); err != nil {
		return nil, fmt.Errorf("cas: parse %s: %w", key, err)
	}
	return &tm, nil
}

// saveLocalTableMetadata writes tm to <localDir>/metadata/<enc_db>/<enc_table>.json.
func saveLocalTableMetadata(localDir string, tm *metadata.TableMetadata) error {
	dir := filepath.Join(localDir, "metadata", common.TablePathEncode(tm.Database))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("cas: mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, common.TablePathEncode(tm.Table)+".json")
	body, err := json.MarshalIndent(tm, "", "\t")
	if err != nil {
		return fmt.Errorf("cas: marshal table metadata %s.%s: %w", tm.Database, tm.Table, err)
	}
	if err := os.WriteFile(path, body, 0o640); err != nil {
		return fmt.Errorf("cas: write %s: %w", path, err)
	}
	return nil
}

// archiveJob is one per-(disk, db, table) tar.zstd to download + extract.
type archiveJob struct {
	Disk, DB, Table string
	Key             string
	Size            int64
}

// blobJob is one large file to fetch from the CAS blob store and write
// into a part directory.
type blobJob struct {
	PartDir  string
	FileName string
	Size     uint64
	Hash     Hash128
}

// downloadArchives concurrently downloads + extracts each per-(disk, db,
// table) archive into the local shadow tree.
func downloadArchives(ctx context.Context, b Backend, jobs []archiveJob, localDir string, parallelism int) error {
	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	sem := make(chan struct{}, parallelism)
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
			if err := validateRemoteFilesystemName("disk", j.Disk); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			dst := filepath.Join(localDir, "shadow",
				common.TablePathEncode(j.DB),
				common.TablePathEncode(j.Table), j.Disk)
			if err := os.MkdirAll(dst, 0o755); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: mkdir %s: %w", dst, err)
				}
				mu.Unlock()
				return
			}
			rc, err := b.GetFile(ctx, j.Key)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: get archive %s: %w", j.Key, err)
				}
				mu.Unlock()
				return
			}
			extractErr := ExtractArchive(rc, dst)
			_ = rc.Close()
			if extractErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: extract %s: %w", j.Key, extractErr)
				}
				mu.Unlock()
				return
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// downloadBlobs concurrently fetches every blob, writing to its in-part
// destination after re-asserting path containment.
func downloadBlobs(ctx context.Context, b Backend, cp string, jobs []blobJob, parallelism int) (int, int64, error) {
	// Sort for determinism in tests.
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].PartDir != jobs[j].PartDir {
			return jobs[i].PartDir < jobs[j].PartDir
		}
		return jobs[i].FileName < jobs[j].FileName
	})
	var (
		mu       sync.Mutex
		firstErr error
		fetched  int
		bytesUp  int64
		wg       sync.WaitGroup
	)
	sem := make(chan struct{}, parallelism)
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

			// Path containment: ensure dst remains under PartDir.
			absPart, err := filepath.Abs(j.PartDir)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: abs %s: %w", j.PartDir, err)
				}
				mu.Unlock()
				return
			}
			rootPrefix := absPart + string(filepath.Separator)
			dst := filepath.Join(absPart, filepath.FromSlash(j.FileName))
			cleanDst := filepath.Clean(dst)
			if !strings.HasPrefix(cleanDst+string(filepath.Separator), rootPrefix) && cleanDst != absPart {
				mu.Lock()
				if firstErr == nil {
					firstErr = &UnsafePathError{Path: j.FileName}
				}
				mu.Unlock()
				return
			}

			if err := os.MkdirAll(filepath.Dir(cleanDst), 0o755); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: mkdir %s: %w", filepath.Dir(cleanDst), err)
				}
				mu.Unlock()
				return
			}

			rc, err := b.GetFile(ctx, BlobPath(cp, j.Hash))
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: get blob %s: %w", BlobPath(cp, j.Hash), err)
				}
				mu.Unlock()
				return
			}
			f, err := os.OpenFile(cleanDst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				_ = rc.Close()
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: open %s: %w", cleanDst, err)
				}
				mu.Unlock()
				return
			}
			n, copyErr := io.Copy(f, rc)
			_ = rc.Close()
			closeErr := f.Close()
			if copyErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: write %s: %w", cleanDst, copyErr)
				}
				mu.Unlock()
				return
			}
			if closeErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("cas: close %s: %w", cleanDst, closeErr)
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			fetched++
			bytesUp += n
			mu.Unlock()
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return 0, 0, firstErr
	}
	return fetched, bytesUp, nil
}

// checkFreeSpace returns an error if the filesystem hosting localDir has
// less than estimate*1.1 bytes free. Best-effort: failure to stat the
// filesystem is logged-and-ignored (Statfs is not available everywhere
// and a stale check shouldn't gate the download).
func checkFreeSpace(localDir string, estimate int64) error {
	if estimate <= 0 {
		return nil
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(localDir, &st); err != nil {
		// Best-effort: skip the check if the syscall is unavailable.
		return nil
	}
	// Bsize is platform-dependent type; cast to int64 via uint64.
	free := int64(st.Bavail) * int64(st.Bsize)
	required := estimate + estimate/10 // *1.1
	if free < required {
		return fmt.Errorf("cas: insufficient free space at %s: have %d bytes, need ~%d (estimate %d * 1.1)", localDir, free, required, estimate)
	}
	return nil
}
