package cas_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/testfixtures"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/klauspost/compress/zstd"
)

// makeBlobBytes returns deterministic 1024-byte data based on seed; used
// to populate file bodies so we can byte-compare after round-trip.
func makeBlobBytes(seed byte) []byte {
	out := make([]byte, 1024)
	for i := range out {
		out[i] = seed + byte(i%17)
	}
	return out
}

// uploadAndDownload is a small helper that performs Upload + Download
// using shared config and returns the local download root.
func uploadAndDownload(t *testing.T, parts []testfixtures.PartSpec, name string, opts cas.DownloadOptions) (lb *testfixtures.LocalBackup, f *fakedst.Fake, cfg cas.Config, downloadRoot string) {
	t.Helper()
	lb = testfixtures.Build(t, parts)
	f = fakedst.New()
	cfg = testCfg(100)
	if _, err := cas.Upload(context.Background(), f, cfg, name, cas.UploadOptions{LocalBackupDir: lb.Root}); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if opts.LocalBackupDir == "" {
		opts.LocalBackupDir = t.TempDir()
	}
	downloadRoot = opts.LocalBackupDir
	if _, err := cas.Download(context.Background(), f, cfg, name, opts); err != nil {
		t.Fatalf("Download: %v", err)
	}
	return lb, f, cfg, downloadRoot
}

func TestDownload_RoundTripBytes(t *testing.T) {
	parts := []testfixtures.PartSpec{
		{Disk: "default", DB: "db1", Table: "t1", Name: "p1", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 1, HashHigh: 1},
			{Name: "primary.idx", Size: 8, HashLow: 2, HashHigh: 1},
			{Name: "data.bin", Size: 1024, HashLow: 999, HashHigh: 1, Bytes: makeBlobBytes(0x10)},
		}},
	}
	lb, _, _, root := uploadAndDownload(t, parts, "b1", cas.DownloadOptions{})
	localBackupDir := filepath.Join(root, "b1")

	// Check root metadata.json: parseable. CAS field is intentionally stripped
	// from the LOCAL copy (so v1 restore handoff works); the REMOTE
	// metadata.json keeps it. See pkg/cas/download.go for rationale.
	bmBody, err := os.ReadFile(filepath.Join(localBackupDir, "metadata.json"))
	if err != nil {
		t.Fatalf("read root metadata.json: %v", err)
	}
	var bm metadata.BackupMetadata
	if err := json.Unmarshal(bmBody, &bm); err != nil {
		t.Fatalf("parse local metadata.json: %v", err)
	}
	if bm.CAS != nil {
		t.Fatal("local metadata.json: CAS field MUST be stripped (v1 restore would refuse otherwise)")
	}
	if bm.DataFormat != "directory" {
		t.Errorf("DataFormat: got %q want directory", bm.DataFormat)
	}

	// Per-table JSON.
	tmPath := filepath.Join(localBackupDir, "metadata",
		common.TablePathEncode("db1"), common.TablePathEncode("t1")+".json")
	tmBody, err := os.ReadFile(tmPath)
	if err != nil {
		t.Fatalf("read table metadata: %v", err)
	}
	var tm metadata.TableMetadata
	if err := json.Unmarshal(tmBody, &tm); err != nil {
		t.Fatalf("parse table metadata: %v", err)
	}
	if got := len(tm.Parts["default"]); got != 1 {
		t.Errorf("Parts[default]: got %d want 1", got)
	}

	// Byte-compare every reconstructed file against the original local
	// backup's bytes.
	origPartDir := filepath.Join(lb.Root, "shadow", "db1", "t1", "default", "p1")
	dlPartDir := filepath.Join(localBackupDir, "shadow",
		common.TablePathEncode("db1"), common.TablePathEncode("t1"), "default", "p1")
	for _, f := range parts[0].Files {
		want, err := os.ReadFile(filepath.Join(origPartDir, f.Name))
		if err != nil {
			t.Fatalf("read original %s: %v", f.Name, err)
		}
		got, err := os.ReadFile(filepath.Join(dlPartDir, f.Name))
		if err != nil {
			t.Fatalf("read downloaded %s: %v", f.Name, err)
		}
		if !bytes.Equal(want, got) {
			t.Errorf("byte mismatch for %s (size want=%d got=%d)", f.Name, len(want), len(got))
		}
	}
	// checksums.txt should also exist on disk.
	if _, err := os.Stat(filepath.Join(dlPartDir, "checksums.txt")); err != nil {
		t.Errorf("checksums.txt missing: %v", err)
	}
}

func TestDownload_RefusesV1Backup(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	cp := cfg.ClusterPrefix()
	// Write a metadata.json with CAS=nil.
	bm := metadata.BackupMetadata{BackupName: "b1", DataFormat: "directory"}
	body, _ := json.Marshal(&bm)
	if err := f.PutFile(context.Background(), cas.MetadataJSONPath(cp, "b1"),
		io.NopCloser(bytes.NewReader(body)), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	_, err := cas.Download(context.Background(), f, cfg, "b1", cas.DownloadOptions{LocalBackupDir: t.TempDir()})
	if !errors.Is(err, cas.ErrV1Backup) {
		t.Fatalf("got err=%v want ErrV1Backup", err)
	}
}

func TestDownload_RefusesUnsupportedLayoutVersion(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	cp := cfg.ClusterPrefix()
	bm := metadata.BackupMetadata{
		BackupName: "b1",
		DataFormat: "directory",
		CAS: &metadata.CASBackupParams{
			LayoutVersion:   cas.LayoutVersion + 1,
			InlineThreshold: cfg.InlineThreshold,
			ClusterID:       cfg.ClusterID,
		},
	}
	body, _ := json.Marshal(&bm)
	if err := f.PutFile(context.Background(), cas.MetadataJSONPath(cp, "b1"),
		io.NopCloser(bytes.NewReader(body)), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	_, err := cas.Download(context.Background(), f, cfg, "b1", cas.DownloadOptions{LocalBackupDir: t.TempDir()})
	if !errors.Is(err, cas.ErrUnsupportedLayoutVersion) {
		t.Fatalf("got err=%v want ErrUnsupportedLayoutVersion", err)
	}
}

func TestDownload_TableFilter(t *testing.T) {
	parts := []testfixtures.PartSpec{
		{Disk: "default", DB: "db1", Table: "t1", Name: "p1", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 1, HashHigh: 1},
		}},
		{Disk: "default", DB: "db1", Table: "t2", Name: "p1", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 2, HashHigh: 1},
		}},
	}
	_, _, _, root := uploadAndDownload(t, parts, "b1", cas.DownloadOptions{
		TableFilter: []string{"db1.t1"},
	})
	localBackupDir := filepath.Join(root, "b1")

	t1Path := filepath.Join(localBackupDir, "metadata",
		common.TablePathEncode("db1"), common.TablePathEncode("t1")+".json")
	t2Path := filepath.Join(localBackupDir, "metadata",
		common.TablePathEncode("db1"), common.TablePathEncode("t2")+".json")
	if _, err := os.Stat(t1Path); err != nil {
		t.Errorf("t1 metadata missing: %v", err)
	}
	if _, err := os.Stat(t2Path); !os.IsNotExist(err) {
		t.Errorf("t2 metadata should be absent, got err=%v", err)
	}
	// Shadow check.
	t1Shadow := filepath.Join(localBackupDir, "shadow",
		common.TablePathEncode("db1"), common.TablePathEncode("t1"))
	if _, err := os.Stat(t1Shadow); err != nil {
		t.Errorf("t1 shadow missing: %v", err)
	}
	t2Shadow := filepath.Join(localBackupDir, "shadow",
		common.TablePathEncode("db1"), common.TablePathEncode("t2"))
	if _, err := os.Stat(t2Shadow); !os.IsNotExist(err) {
		t.Errorf("t2 shadow should be absent, got err=%v", err)
	}
}

func TestDownload_SchemaOnly(t *testing.T) {
	parts := []testfixtures.PartSpec{
		{Disk: "default", DB: "db1", Table: "t1", Name: "p1", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 1, HashHigh: 1},
			{Name: "data.bin", Size: 1024, HashLow: 999, HashHigh: 1, Bytes: makeBlobBytes(0x10)},
		}},
	}
	_, _, _, root := uploadAndDownload(t, parts, "b1", cas.DownloadOptions{
		SchemaOnly: true,
	})
	localBackupDir := filepath.Join(root, "b1")

	if _, err := os.Stat(filepath.Join(localBackupDir, "metadata.json")); err != nil {
		t.Errorf("metadata.json missing: %v", err)
	}
	tmPath := filepath.Join(localBackupDir, "metadata",
		common.TablePathEncode("db1"), common.TablePathEncode("t1")+".json")
	if _, err := os.Stat(tmPath); err != nil {
		t.Errorf("table metadata missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(localBackupDir, "shadow")); !os.IsNotExist(err) {
		t.Errorf("shadow/ should be absent under SchemaOnly, got err=%v", err)
	}
}

// makeArchiveBytes builds a tar.zstd archive from in-memory entries
// (name → bytes). Used by the traversal tests to bypass Upload's
// validation and put a hostile archive directly into the backend.
func makeArchiveBytes(t *testing.T, entries map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("zstd.NewWriter: %v", err)
	}
	tw := tar.NewWriter(zw)
	// Determinism for debugging.
	names := make([]string, 0, len(entries))
	for n := range entries {
		names = append(names, n)
	}
	for _, n := range names {
		body := entries[n]
		hdr := &tar.Header{
			Name:     n,
			Mode:     0o644,
			Size:     int64(len(body)),
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("tar header: %v", err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatalf("tar write: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zstd close: %v", err)
	}
	return buf.Bytes()
}

// putHostileBackup primes the fake backend with a hand-crafted CAS
// backup whose single archive has the given entries. Used by the two
// traversal tests; the resulting "backup" passes ValidateBackup.
func putHostileBackup(t *testing.T, f *fakedst.Fake, cfg cas.Config, name, db, table, disk string, archiveEntries map[string][]byte) {
	t.Helper()
	cp := cfg.ClusterPrefix()
	bm := metadata.BackupMetadata{
		BackupName: name,
		DataFormat: "directory",
		Tables:     []metadata.TableTitle{{Database: db, Table: table}},
		CAS: &metadata.CASBackupParams{
			LayoutVersion:   cas.LayoutVersion,
			InlineThreshold: cfg.InlineThreshold,
			ClusterID:       cfg.ClusterID,
		},
	}
	bmBody, _ := json.Marshal(&bm)
	if err := f.PutFile(context.Background(), cas.MetadataJSONPath(cp, name),
		io.NopCloser(bytes.NewReader(bmBody)), int64(len(bmBody))); err != nil {
		t.Fatal(err)
	}

	tm := metadata.TableMetadata{
		Database: db, Table: table,
		Parts: map[string][]metadata.Part{
			disk: {{Name: "p1"}},
		},
	}
	tmBody, _ := json.Marshal(&tm)
	if err := f.PutFile(context.Background(), cas.TableMetaPath(cp, name, db, table),
		io.NopCloser(bytes.NewReader(tmBody)), int64(len(tmBody))); err != nil {
		t.Fatal(err)
	}

	archive := makeArchiveBytes(t, archiveEntries)
	if err := f.PutFile(context.Background(), cas.PartArchivePath(cp, name, disk, db, table),
		io.NopCloser(bytes.NewReader(archive)), int64(len(archive))); err != nil {
		t.Fatal(err)
	}
}

func TestDownload_RejectsTraversalFilename(t *testing.T) {
	// checksums.txt lists "../escape.txt" as one of its files. The tar
	// itself is well-formed (no traversal in tar names), so it extracts
	// successfully; the rejection comes from validateChecksumsTxtFilename.
	ck := "checksums format version: 2\n" +
		"2 files:\n" +
		"columns.txt\n\tsize: 5\n\thash: 1 1\n\tcompressed: 0\n" +
		"../escape.txt\n\tsize: 99999\n\thash: 9 9\n\tcompressed: 0\n"
	entries := map[string][]byte{
		"p1/checksums.txt": []byte(ck),
		"p1/columns.txt":   []byte("hello"),
	}
	f := fakedst.New()
	cfg := testCfg(100)
	putHostileBackup(t, f, cfg, "b1", "db1", "t1", "default", entries)

	_, err := cas.Download(context.Background(), f, cfg, "b1", cas.DownloadOptions{
		LocalBackupDir: t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "..") {
		t.Fatalf("got err=%v want filename traversal error", err)
	}
}

func TestDownload_RejectsTarTraversal(t *testing.T) {
	// Hand-crafted tar entry name with "..". ExtractArchive must reject.
	entries := map[string][]byte{
		"../escape.txt": []byte("pwned"),
	}
	f := fakedst.New()
	cfg := testCfg(100)
	putHostileBackup(t, f, cfg, "b1", "db1", "t1", "default", entries)

	_, err := cas.Download(context.Background(), f, cfg, "b1", cas.DownloadOptions{
		LocalBackupDir: t.TempDir(),
	})
	var unsafe *cas.UnsafePathError
	if !errors.As(err, &unsafe) {
		t.Fatalf("got err=%v want *UnsafePathError", err)
	}
}

func TestDownload_PartitionFilter(t *testing.T) {
	parts := []testfixtures.PartSpec{
		{Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 1, HashHigh: 1},
		}},
		{Disk: "default", DB: "db1", Table: "t1", Name: "all_2_2_0", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 2, HashHigh: 1},
		}},
	}
	_, _, _, root := uploadAndDownload(t, parts, "b1", cas.DownloadOptions{
		Partitions: []string{"all_1_1_0"},
	})
	localBackupDir := filepath.Join(root, "b1")

	tmPath := filepath.Join(localBackupDir, "metadata",
		common.TablePathEncode("db1"), common.TablePathEncode("t1")+".json")
	tmBody, err := os.ReadFile(tmPath)
	if err != nil {
		t.Fatalf("read table metadata: %v", err)
	}
	var tm metadata.TableMetadata
	if err := json.Unmarshal(tmBody, &tm); err != nil {
		t.Fatalf("parse table metadata: %v", err)
	}
	parts1 := tm.Parts["default"]
	if len(parts1) != 1 || parts1[0].Name != "all_1_1_0" {
		t.Errorf("filtered Parts[default]: got %+v want [all_1_1_0]", parts1)
	}

	// Note: archives are downloaded whole even when partition-filtered
	// (per spec, "acceptable overhead"). So extraction may still produce
	// all_2_2_0/checksums.txt under the disk dir; we only assert the JSON
	// reflects the filter and that all_1_1_0 is present after extraction.
	dlPartDir := filepath.Join(localBackupDir, "shadow",
		common.TablePathEncode("db1"), common.TablePathEncode("t1"), "default", "all_1_1_0")
	if _, err := os.Stat(filepath.Join(dlPartDir, "checksums.txt")); err != nil {
		t.Errorf("all_1_1_0/checksums.txt missing: %v", err)
	}
}

// TestDownload_PreservesSchemaFields is a regression test that the v1
// schema fields populated in cas-upload survive the upload→download
// round-trip and land in the per-table JSON the v1 restore reads.
func TestDownload_PreservesSchemaFields(t *testing.T) {
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
		Files: []testfixtures.FileSpec{{Name: "columns.txt", Size: 8, HashLow: 1, HashHigh: 0}},
		TableMeta: metadata.TableMetadata{
			Database: "db1", Table: "t1",
			Query: "CREATE TABLE db1.t1 ENGINE=Memory",
			UUID:  "abc",
		},
	}}
	_, _, _, root := uploadAndDownload(t, parts, "b1", cas.DownloadOptions{})
	body, err := os.ReadFile(filepath.Join(root, "b1", "metadata",
		common.TablePathEncode("db1"), common.TablePathEncode("t1")+".json"))
	if err != nil {
		t.Fatalf("read downloaded table metadata: %v", err)
	}
	var got metadata.TableMetadata
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("parse table metadata: %v", err)
	}
	if got.Query == "" || got.UUID == "" {
		t.Errorf("downloaded JSON lost schema fields: %+v", got)
	}
}

// TestDownload_RejectsTraversalDiskName verifies that a remote
// TableMetadata with a malicious disk name (path traversal) is rejected
// before any local filesystem write — defense against a compromised CAS
// bucket directing extraction outside localDir.
func TestDownload_RejectsTraversalDiskName(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	ctx := context.Background()
	cp := cfg.ClusterPrefix()

	// Hand-craft a CAS-shaped metadata.json + per-table JSON whose Parts
	// map keys (disk names) contain "..".
	bm := metadata.BackupMetadata{
		BackupName: "evil",
		DataFormat: "directory",
		Tables:     []metadata.TableTitle{{Database: "db", Table: "t"}},
		CAS: &metadata.CASBackupParams{
			LayoutVersion: cas.LayoutVersion, InlineThreshold: cfg.InlineThreshold, ClusterID: cfg.ClusterID,
		},
	}
	body, _ := json.Marshal(&bm)
	if err := f.PutFile(ctx, cas.MetadataJSONPath(cp, "evil"),
		io.NopCloser(bytes.NewReader(body)), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	tm := metadata.TableMetadata{
		Database: "db", Table: "t",
		Parts: map[string][]metadata.Part{
			"../escape": {{Name: "all_1_1_0"}},
		},
	}
	tmBody, _ := json.Marshal(&tm)
	if err := f.PutFile(ctx, cas.TableMetaPath(cp, "evil", "db", "t"),
		io.NopCloser(bytes.NewReader(tmBody)), int64(len(tmBody))); err != nil {
		t.Fatal(err)
	}

	_, err := cas.Download(ctx, f, cfg, "evil", cas.DownloadOptions{LocalBackupDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected refusal for traversal disk name")
	}
	if !strings.Contains(err.Error(), "unsafe disk") {
		t.Errorf("expected 'unsafe disk' in error, got: %v", err)
	}
}

// TestDownload_RejectsTraversalPartName covers the same defense for the
// per-Part Name field.
func TestDownload_RejectsTraversalPartName(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	ctx := context.Background()
	cp := cfg.ClusterPrefix()

	bm := metadata.BackupMetadata{
		BackupName: "evil",
		DataFormat: "directory",
		Tables:     []metadata.TableTitle{{Database: "db", Table: "t"}},
		CAS: &metadata.CASBackupParams{
			LayoutVersion: cas.LayoutVersion, InlineThreshold: cfg.InlineThreshold, ClusterID: cfg.ClusterID,
		},
	}
	body, _ := json.Marshal(&bm)
	if err := f.PutFile(ctx, cas.MetadataJSONPath(cp, "evil"),
		io.NopCloser(bytes.NewReader(body)), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	tm := metadata.TableMetadata{
		Database: "db", Table: "t",
		Parts: map[string][]metadata.Part{
			"default": {{Name: "../escape"}},
		},
	}
	tmBody, _ := json.Marshal(&tm)
	if err := f.PutFile(ctx, cas.TableMetaPath(cp, "evil", "db", "t"),
		io.NopCloser(bytes.NewReader(tmBody)), int64(len(tmBody))); err != nil {
		t.Fatal(err)
	}

	_, err := cas.Download(ctx, f, cfg, "evil", cas.DownloadOptions{LocalBackupDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected refusal for traversal part name")
	}
	if !strings.Contains(err.Error(), "unsafe part name") {
		t.Errorf("expected 'unsafe part name' in error, got: %v", err)
	}
}
