package cas

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/Altinity/clickhouse-backup/v2/pkg/checksumstxt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/klauspost/compress/zstd"
)

// VerifyOptions configures a Verify run.
type VerifyOptions struct {
	JSON        bool
	Parallelism int // for HEADs; default 32
}

// VerifyFailure describes a single blob that failed verification.
type VerifyFailure struct {
	Kind string `json:"kind"`            // "stat_error" | "missing" | "size_mismatch"
	Path string `json:"path"`
	Want uint64 `json:"want"`
	Got  int64  `json:"got,omitempty"`  // present for size_mismatch
	Err  string `json:"err,omitempty"`  // present for stat_error
}

// VerifyResult summarises what a Verify run found.
type VerifyResult struct {
	BackupName   string
	BlobsChecked int
	Failures     []VerifyFailure
}

// expectedBlob is one (path, expected-size) pair accumulated from checksums.txt entries.
type expectedBlob struct {
	Path string
	Size uint64
}

// Verify performs a HEAD + size check on every blob referenced by the backup.
// Writes either human-readable lines (default) or line-delimited JSON
// (opts.JSON) to out as failures are discovered. Failures are written to out
// after all HEADs have completed (deterministic, sorted by path). Returns the
// structured result; if Failures is non-empty, also returns ErrVerifyFailures
// so callers (and the CLI) can detect the failure cleanly.
func Verify(ctx context.Context, b Backend, cfg Config, name string, opts VerifyOptions, out io.Writer) (*VerifyResult, error) {
	bm, err := ValidateBackup(ctx, b, cfg, name)
	if err != nil {
		return nil, err
	}
	cp := cfg.ClusterPrefix()

	blobs, err := buildVerifySet(ctx, b, cp, name, bm)
	if err != nil {
		return nil, fmt.Errorf("cas-verify: build set: %w", err)
	}

	parallelism := opts.Parallelism
	if parallelism <= 0 {
		parallelism = 32
	}
	failures := headAllInParallel(ctx, b, blobs, parallelism, opts.JSON, out)

	res := &VerifyResult{BackupName: name, BlobsChecked: len(blobs), Failures: failures}
	if len(failures) > 0 {
		return res, ErrVerifyFailures
	}
	return res, nil
}

// buildVerifySet downloads each per-table archive, extracts every
// checksums.txt, and accumulates expected blobs.
func buildVerifySet(ctx context.Context, b Backend, cp, name string, bm *metadata.BackupMetadata) ([]expectedBlob, error) {
	// De-duplicate blobs across tables — the same blob hash may be
	// referenced from multiple tables.
	seen := make(map[string]uint64)

	for _, tt := range bm.Tables {
		// Load per-table metadata to learn which disks this table lives on.
		tmRC, err := b.GetFile(ctx, TableMetaPath(cp, name, tt.Database, tt.Table))
		if err != nil {
			return nil, fmt.Errorf("cas-verify: get table metadata %s.%s: %w", tt.Database, tt.Table, err)
		}
		raw, err := io.ReadAll(tmRC)
		_ = tmRC.Close()
		if err != nil {
			return nil, fmt.Errorf("cas-verify: read table metadata %s.%s: %w", tt.Database, tt.Table, err)
		}
		var tm metadata.TableMetadata
		if err := json.Unmarshal(raw, &tm); err != nil {
			return nil, fmt.Errorf("cas-verify: parse table metadata %s.%s: %w", tt.Database, tt.Table, err)
		}

		for disk := range tm.Parts {
			if err := validateRemoteFilesystemName("disk", disk); err != nil {
				return nil, fmt.Errorf("cas-verify: %w", err)
			}
			archPath := PartArchivePath(cp, name, disk, tt.Database, tt.Table)
			archRC, err := b.GetFile(ctx, archPath)
			if err != nil {
				return nil, fmt.Errorf("cas-verify: get archive %s: %w", archPath, err)
			}
			archBytes, err := io.ReadAll(archRC)
			_ = archRC.Close()
			if err != nil {
				return nil, fmt.Errorf("cas-verify: read archive %s: %w", archPath, err)
			}

			if err := extractBlobsFromArchive(bytes.NewReader(archBytes), cp, bm.CAS.InlineThreshold, seen); err != nil {
				return nil, fmt.Errorf("cas-verify: extract blobs from %s: %w", archPath, err)
			}
		}
	}

	blobs := make([]expectedBlob, 0, len(seen))
	for path, size := range seen {
		blobs = append(blobs, expectedBlob{Path: path, Size: size})
	}
	// Sort for determinism.
	sortExpectedBlobs(blobs)
	return blobs, nil
}

// extractBlobsFromArchive streams through a tar.zstd archive, finds every
// entry whose name ends in "/checksums.txt", parses it, and accumulates
// blob (path, size) pairs in seen.
func extractBlobsFromArchive(r io.Reader, cp string, threshold uint64, seen map[string]uint64) error {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return fmt.Errorf("zstd reader: %w", err)
	}
	defer zr.Close()

	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		// Only process checksums.txt entries.
		if !strings.HasSuffix(hdr.Name, "/checksums.txt") && hdr.Name != "checksums.txt" {
			// Still must drain the entry.
			_, _ = io.Copy(io.Discard, tr)
			continue
		}

		data, err := io.ReadAll(tr)
		if err != nil {
			return fmt.Errorf("read %s: %w", hdr.Name, err)
		}

		parsed, err := checksumstxt.Parse(bytes.NewReader(data))
		if err != nil {
			// Malformed checksums.txt in archive — treat as error.
			return fmt.Errorf("parse %s: %w", hdr.Name, err)
		}

		for _, c := range parsed.Files {
			if c.FileSize <= threshold {
				// Inline — no blob to check.
				continue
			}
			h := Hash128{Low: c.FileHash.Low, High: c.FileHash.High}
			blobKey := BlobPath(cp, h)
			if existing, ok := seen[blobKey]; !ok {
				seen[blobKey] = c.FileSize
			} else if existing != c.FileSize {
				// Two checksums.txt entries claim different sizes for the
				// same blob hash. Use the first one seen; the inconsistency
				// would be caught by the upload logic.
				_ = existing
			}
		}
	}
}

// sortExpectedBlobs sorts blobs by Path for deterministic output.
func sortExpectedBlobs(blobs []expectedBlob) {
	for i := 1; i < len(blobs); i++ {
		for j := i; j > 0 && blobs[j].Path < blobs[j-1].Path; j-- {
			blobs[j], blobs[j-1] = blobs[j-1], blobs[j]
		}
	}
}

// headAllInParallel performs HEAD (StatFile) on every blob and returns failures.
// Each failure is also written to out (text or JSON per asJSON) after all
// checks complete. Output is written in sorted-path order for determinism.
func headAllInParallel(ctx context.Context, b Backend, blobs []expectedBlob, parallelism int, asJSON bool, out io.Writer) []VerifyFailure {
	type result struct {
		blob    expectedBlob
		failure *VerifyFailure
	}

	results := make([]result, len(blobs))
	for i, bl := range blobs {
		results[i].blob = bl
	}

	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	for i := range results {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			bl := results[i].blob
			size, _, exists, err := b.StatFile(ctx, bl.Path)
			if err != nil {
				results[i].failure = &VerifyFailure{
					Kind: "stat_error",
					Path: bl.Path,
					Want: bl.Size,
					Err:  err.Error(),
				}
				return
			}
			if !exists {
				results[i].failure = &VerifyFailure{
					Kind: "missing",
					Path: bl.Path,
					Want: bl.Size,
				}
				return
			}
			if uint64(size) != bl.Size {
				results[i].failure = &VerifyFailure{
					Kind: "size_mismatch",
					Path: bl.Path,
					Want: bl.Size,
					Got:  size,
				}
			}
		}()
	}
	wg.Wait()

	// Collect failures (already in sorted-path order since blobs were sorted).
	var failures []VerifyFailure
	for _, r := range results {
		if r.failure == nil {
			continue
		}
		failures = append(failures, *r.failure)
		if out != nil {
			writeVerifyFailure(out, *r.failure, asJSON)
		}
	}
	return failures
}

// writeVerifyFailure writes one failure to out in the requested format.
func writeVerifyFailure(out io.Writer, f VerifyFailure, asJSON bool) {
	if asJSON {
		data, err := json.Marshal(f)
		if err == nil {
			_, _ = fmt.Fprintf(out, "%s\n", data)
		}
		return
	}
	switch f.Kind {
	case "stat_error":
		_, _ = fmt.Fprintf(out, "STATERR  %s (want %d bytes): %s\n", f.Path, f.Want, f.Err)
	case "missing":
		_, _ = fmt.Fprintf(out, "MISSING  %s (want %d bytes)\n", f.Path, f.Want)
	case "size_mismatch":
		_, _ = fmt.Fprintf(out, "MISMATCH %s (want %d got %d bytes)\n", f.Path, f.Want, f.Got)
	default:
		_, _ = fmt.Fprintf(out, "%s %s\n", f.Kind, f.Path)
	}
}
