package cas_test

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/testfixtures"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/klauspost/compress/zstd"
)

// testCfg returns a CAS config valid enough that Upload doesn't reject
// it on Validate(). Threshold 100 keeps small files inline and pushes
// 1024-byte files to blob.
func testCfg(threshold uint64) cas.Config {
	c := cas.Config{
		Enabled:          true,
		ClusterID:        "c1",
		RootPrefix:       "cas/",
		InlineThreshold:  threshold,
		GraceBlob:        "24h",
		AbandonThreshold: "168h",
	}
	// Populate parsed durations on the (now pointer-receiver) Validate.
	if err := c.Validate(); err != nil {
		panic(err)
	}
	return c
}

func smallPart(name string, hashLow uint64) testfixtures.PartSpec {
	return testfixtures.PartSpec{
		Disk: "default", DB: "db1", Table: "t1", Name: name,
		Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: hashLow + 1, HashHigh: 100},
			{Name: "primary.idx", Size: 8, HashLow: hashLow + 2, HashHigh: 100},
			{Name: "data.bin", Size: 1024, HashLow: hashLow + 3, HashHigh: 100},
		},
	}
}

func TestUpload_RoundTripBasic(t *testing.T) {
	lb := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 0)})
	f := fakedst.New()
	cfg := testCfg(100)
	res, err := cas.Upload(context.Background(), f, cfg, "b1", cas.UploadOptions{
		LocalBackupDir: lb.Root,
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if res.BlobsConsidered != 1 {
		t.Errorf("BlobsConsidered: got %d want 1", res.BlobsConsidered)
	}
	if res.BlobsUploaded != 1 {
		t.Errorf("BlobsUploaded: got %d want 1", res.BlobsUploaded)
	}
	if res.PerTableArchives != 1 {
		t.Errorf("PerTableArchives: got %d want 1", res.PerTableArchives)
	}
	cp := cfg.ClusterPrefix()

	// metadata.json must exist with CAS field populated.
	rc, err := f.GetFile(context.Background(), cas.MetadataJSONPath(cp, "b1"))
	if err != nil {
		t.Fatalf("get metadata.json: %v", err)
	}
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	var bm metadata.BackupMetadata
	if err := json.Unmarshal(body, &bm); err != nil {
		t.Fatalf("parse metadata.json: %v", err)
	}
	if bm.CAS == nil {
		t.Fatal("metadata.json: CAS field nil")
	}
	if bm.CAS.LayoutVersion != cas.LayoutVersion {
		t.Errorf("LayoutVersion: got %d want %d", bm.CAS.LayoutVersion, cas.LayoutVersion)
	}
	if bm.CAS.InlineThreshold != cfg.InlineThreshold {
		t.Errorf("InlineThreshold: got %d want %d", bm.CAS.InlineThreshold, cfg.InlineThreshold)
	}
	if bm.CAS.ClusterID != cfg.ClusterID {
		t.Errorf("ClusterID: got %q want %q", bm.CAS.ClusterID, cfg.ClusterID)
	}
	if bm.DataFormat != "directory" {
		t.Errorf("DataFormat: got %q want directory", bm.DataFormat)
	}

	// In-progress marker must be gone.
	if _, _, exists, err := f.StatFile(context.Background(), cas.InProgressMarkerPath(cp, "b1")); err != nil {
		t.Fatal(err)
	} else if exists {
		t.Error("in-progress marker still present after commit")
	}

	// Archive + table json present.
	if _, _, exists, _ := f.StatFile(context.Background(), cas.PartArchivePath(cp, "b1", "default", "db1", "t1")); !exists {
		t.Error("part archive missing")
	}
	if _, _, exists, _ := f.StatFile(context.Background(), cas.TableMetaPath(cp, "b1", "db1", "t1")); !exists {
		t.Error("table metadata json missing")
	}
}

func TestUpload_DedupsAcrossParts(t *testing.T) {
	// Two parts with the same blob hash for data.bin → one PutFile.
	bytes1024 := make([]byte, 1024)
	for i := range bytes1024 {
		bytes1024[i] = 0xAB
	}
	parts := []testfixtures.PartSpec{
		{Disk: "default", DB: "db1", Table: "t1", Name: "p1", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 1, HashHigh: 1},
			{Name: "data.bin", Size: 1024, HashLow: 999, HashHigh: 999, Bytes: bytes1024},
		}},
		{Disk: "default", DB: "db1", Table: "t1", Name: "p2", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 2, HashHigh: 2},
			{Name: "data.bin", Size: 1024, HashLow: 999, HashHigh: 999, Bytes: bytes1024},
		}},
	}
	lb := testfixtures.Build(t, parts)
	f := fakedst.New()
	cfg := testCfg(100)

	// Wrap to count PutFile calls on blob keys.
	wrap := newCountingBackend(f)
	res, err := cas.Upload(context.Background(), wrap, cfg, "b1", cas.UploadOptions{LocalBackupDir: lb.Root})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if res.BlobsUploaded != 1 {
		t.Errorf("BlobsUploaded: got %d want 1", res.BlobsUploaded)
	}
	cp := cfg.ClusterPrefix()
	puts := wrap.putsForPrefix(cp + "blob/")
	if puts != 1 {
		t.Errorf("blob PutFile count: got %d want 1", puts)
	}
}

func TestUpload_RefusesIfPruneMarkerPresent(t *testing.T) {
	lb := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 0)})
	f := fakedst.New()
	cfg := testCfg(100)
	if err := f.PutFile(context.Background(), cas.PruneMarkerPath(cfg.ClusterPrefix()),
		io.NopCloser(strings.NewReader("{}")), 2); err != nil {
		t.Fatal(err)
	}
	_, err := cas.Upload(context.Background(), f, cfg, "b1", cas.UploadOptions{LocalBackupDir: lb.Root})
	if !errors.Is(err, cas.ErrPruneInProgress) {
		t.Fatalf("got err=%v want ErrPruneInProgress", err)
	}
}

func TestUpload_RefusesIfBackupExists(t *testing.T) {
	lb := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 0)})
	f := fakedst.New()
	cfg := testCfg(100)
	if err := f.PutFile(context.Background(), cas.MetadataJSONPath(cfg.ClusterPrefix(), "b1"),
		io.NopCloser(strings.NewReader("{}")), 2); err != nil {
		t.Fatal(err)
	}
	_, err := cas.Upload(context.Background(), f, cfg, "b1", cas.UploadOptions{LocalBackupDir: lb.Root})
	if !errors.Is(err, cas.ErrBackupExists) {
		t.Fatalf("got err=%v want ErrBackupExists", err)
	}
}

func TestUpload_PreCommitChecksPruneMarker(t *testing.T) {
	lb := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 0)})
	f := fakedst.New()
	cfg := testCfg(100)
	cp := cfg.ClusterPrefix()

	// Wrap so that as soon as the planner has done the cold-list, we
	// inject a prune marker before the pre-commit re-check fires.
	wrap := newInjectingBackend(f)
	wrap.onStat = func(key string) {
		// Trigger when the pre-commit re-check stats the prune marker.
		// At that point all uploads + table JSONs are done; just put
		// the marker so the stat returns "exists".
		if key == cas.PruneMarkerPath(cp) && atomic.LoadInt32(&wrap.injected) == 0 {
			// Only inject AFTER step 6/7 (initial check has long passed).
			// Easy heuristic: do it the second time the prune-marker key
			// is stat'd (first = step 2, second = step 11a).
			if atomic.AddInt32(&wrap.statCount, 1) >= 2 {
				_ = f.PutFile(context.Background(), key, io.NopCloser(strings.NewReader("{}")), 2)
				atomic.StoreInt32(&wrap.injected, 1)
			}
		}
	}

	_, err := cas.Upload(context.Background(), wrap, cfg, "b1", cas.UploadOptions{LocalBackupDir: lb.Root})
	if !errors.Is(err, cas.ErrPruneInProgress) {
		t.Fatalf("got err=%v want ErrPruneInProgress", err)
	}
	// metadata.json must NOT have been written.
	if _, _, exists, _ := f.StatFile(context.Background(), cas.MetadataJSONPath(cp, "b1")); exists {
		t.Error("metadata.json was written despite prune-marker injection")
	}
	// in-progress marker must have been cleaned up.
	if _, _, exists, _ := f.StatFile(context.Background(), cas.InProgressMarkerPath(cp, "b1")); exists {
		t.Error("in-progress marker still present after abort")
	}
}

func TestUpload_PreCommitChecksOwnInProgressMarker(t *testing.T) {
	lb := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 0)})
	f := fakedst.New()
	cfg := testCfg(100)
	cp := cfg.ClusterPrefix()

	// Delete the in-progress marker right before step 11b stats it.
	wrap := newInjectingBackend(f)
	wrap.onStat = func(key string) {
		if key == cas.InProgressMarkerPath(cp, "b1") {
			_ = f.DeleteFile(context.Background(), key)
		}
	}
	_, err := cas.Upload(context.Background(), wrap, cfg, "b1", cas.UploadOptions{LocalBackupDir: lb.Root})
	if err == nil || !strings.Contains(err.Error(), "in-progress marker") {
		t.Fatalf("got err=%v want in-progress-marker abort", err)
	}
	if _, _, exists, _ := f.StatFile(context.Background(), cas.MetadataJSONPath(cp, "b1")); exists {
		t.Error("metadata.json was written despite swept marker")
	}
}

func TestUpload_DryRun(t *testing.T) {
	lb := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 0)})
	f := fakedst.New()
	cfg := testCfg(100)
	res, err := cas.Upload(context.Background(), f, cfg, "b1", cas.UploadOptions{
		LocalBackupDir: lb.Root,
		DryRun:         true,
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if !res.DryRun {
		t.Error("res.DryRun: got false want true")
	}
	if res.BlobsUploaded != 0 {
		t.Errorf("BlobsUploaded: got %d want 0", res.BlobsUploaded)
	}
	if f.Len() != 0 {
		t.Errorf("backend.Len: got %d want 0 (dry run)", f.Len())
	}
}

func TestUpload_RefusesObjectDisks(t *testing.T) {
	lb := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 0)})
	f := fakedst.New()
	cfg := testCfg(100)
	disks := []cas.DiskInfo{{Name: "s3disk", Path: "/var/lib/clickhouse/disks/s3", Type: "s3"}}
	tables := []cas.TableInfo{{Database: "db1", Name: "t1", DataPaths: []string{"/var/lib/clickhouse/disks/s3/store/abc/"}}}
	_, err := cas.Upload(context.Background(), f, cfg, "b1", cas.UploadOptions{
		LocalBackupDir:   lb.Root,
		Disks:            disks,
		ClickHouseTables: tables,
	})
	if !errors.Is(err, cas.ErrObjectDiskRefused) {
		t.Fatalf("got err=%v want ErrObjectDiskRefused", err)
	}
}

func TestUpload_SkipObjectDisks(t *testing.T) {
	// Two tables; t2 is on an object disk and must be silently excluded.
	parts := []testfixtures.PartSpec{
		{Disk: "default", DB: "db1", Table: "t1", Name: "p1", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 1, HashHigh: 1},
		}},
		{Disk: "s3disk", DB: "db1", Table: "t2", Name: "p1", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 2, HashHigh: 2},
		}},
	}
	lb := testfixtures.Build(t, parts)
	f := fakedst.New()
	cfg := testCfg(100)
	disks := []cas.DiskInfo{
		{Name: "default", Path: "/var/lib/clickhouse", Type: "local"},
		{Name: "s3disk", Path: "/var/lib/clickhouse/disks/s3", Type: "s3"},
	}
	tables := []cas.TableInfo{
		{Database: "db1", Name: "t1", DataPaths: []string{"/var/lib/clickhouse/store/abc/"}},
		{Database: "db1", Name: "t2", DataPaths: []string{"/var/lib/clickhouse/disks/s3/store/def/"}},
	}
	res, err := cas.Upload(context.Background(), f, cfg, "b1", cas.UploadOptions{
		LocalBackupDir:   lb.Root,
		SkipObjectDisks:  true,
		Disks:            disks,
		ClickHouseTables: tables,
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if res.PerTableArchives != 1 {
		t.Errorf("PerTableArchives: got %d want 1 (t2 should be skipped)", res.PerTableArchives)
	}
	cp := cfg.ClusterPrefix()
	if _, _, exists, _ := f.StatFile(context.Background(), cas.PartArchivePath(cp, "b1", "default", "db1", "t1")); !exists {
		t.Error("t1 archive missing")
	}
	if _, _, exists, _ := f.StatFile(context.Background(), cas.PartArchivePath(cp, "b1", "s3disk", "db1", "t2")); exists {
		t.Error("t2 archive should not have been uploaded")
	}
}

// TestUpload_ExcludedTablesSkipsArchive verifies the precomputed exclusion
// list flows through cas.Upload to planUpload and the excluded table's
// per-table archive is NOT written. Closes the gap between the CLI-side
// wiring test (TestSkipObjectDisks_ExclusionFiresFromSnapshot) and the
// existing live-disk-derived path (TestUpload_SkipObjectDisks).
func TestUpload_ExcludedTablesSkipsArchive(t *testing.T) {
	ctx := context.Background()
	parts := []testfixtures.PartSpec{
		{
			Disk: "default", DB: "db1", Table: "keep", Name: "all_1_1_0",
			Files: []testfixtures.FileSpec{
				{Name: "data.bin", Size: 16, HashLow: 1, HashHigh: 1},
			},
		},
		{
			Disk: "default", DB: "db1", Table: "drop", Name: "all_1_1_0",
			Files: []testfixtures.FileSpec{
				{Name: "data.bin", Size: 16, HashLow: 2, HashHigh: 2},
			},
		},
	}
	src := testfixtures.Build(t, parts)
	f := fakedst.New()
	cfg := testCfg(1024)

	if _, err := cas.Upload(ctx, f, cfg, "bk", cas.UploadOptions{
		LocalBackupDir:  src.Root,
		SkipObjectDisks: true,
		ExcludedTables:  []string{"db1.drop"},
	}); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// db1.keep's per-table archive must exist; db1.drop's must not.
	cp := cfg.ClusterPrefix()
	keepKey := cas.PartArchivePath(cp, "bk", "default", "db1", "keep")
	dropKey := cas.PartArchivePath(cp, "bk", "default", "db1", "drop")
	if _, _, exists, err := f.StatFile(ctx, keepKey); err != nil || !exists {
		t.Errorf("db1.keep archive missing: exists=%v err=%v", exists, err)
	}
	if _, _, exists, err := f.StatFile(ctx, dropKey); err != nil {
		t.Fatalf("StatFile(drop): %v", err)
	} else if exists {
		t.Errorf("db1.drop archive should NOT exist when in ExcludedTables; key=%s", dropKey)
	}
}

// TestUpload_MergesSchemaFieldsFromLocalV1Metadata verifies cas-upload
// reads the per-(db, table) JSON that `clickhouse-backup create` wrote
// and merges Query/UUID/TotalBytes/etc. into the uploaded
// TableMetadata. Without this merge, cas-restore on a fresh host can't
// recreate tables.
func TestUpload_MergesSchemaFieldsFromLocalV1Metadata(t *testing.T) {
	parts := []testfixtures.PartSpec{
		{
			Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
			Files: []testfixtures.FileSpec{
				{Name: "columns.txt", Size: 8, HashLow: 1, HashHigh: 0},
			},
			TableMeta: metadata.TableMetadata{
				Database:   "db1",
				Table:      "t1",
				Query:      "CREATE TABLE db1.t1 (id UInt64) ENGINE=MergeTree ORDER BY id",
				UUID:       "deadbeef-0000-0000-0000-000000000001",
				TotalBytes: 12345,
			},
		},
	}
	src := testfixtures.Build(t, parts)
	f := fakedst.New()
	cfg := testCfg(100)
	if _, err := cas.Upload(context.Background(), f, cfg, "bk1", cas.UploadOptions{
		LocalBackupDir: src.Root,
	}); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := f.GetFile(context.Background(), cas.TableMetaPath(cfg.ClusterPrefix(), "bk1", "db1", "t1"))
	if err != nil {
		t.Fatalf("get table metadata: %v", err)
	}
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	var got metadata.TableMetadata
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("parse table metadata: %v", err)
	}

	if got.Query == "" {
		t.Error("uploaded TableMetadata.Query is empty - fresh-host restore would fail")
	}
	if got.UUID != "deadbeef-0000-0000-0000-000000000001" {
		t.Errorf("UUID: got %q want %q", got.UUID, "deadbeef-0000-0000-0000-000000000001")
	}
	if got.TotalBytes != 12345 {
		t.Errorf("TotalBytes: got %d want 12345", got.TotalBytes)
	}
}

// TestUpload_PreservesEmptyTable verifies that a table whose metadata JSON
// exists locally but has no shadow part directory still appears in the
// uploaded BackupMetadata.Tables list. Without the fix, the table would be
// silently dropped and cas-restore could not recreate its schema.
func TestUpload_PreservesEmptyTable(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()

	// Build a synthetic backup with two tables: t1 has a part, t2 has only
	// a metadata JSON (no shadow dir).
	parts := []testfixtures.PartSpec{
		{
			Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
			Files: []testfixtures.FileSpec{
				{Name: "data.bin", Size: 64, HashLow: 1, HashHigh: 2},
			},
		},
	}
	src := testfixtures.Build(t, parts)

	// Add t2's metadata JSON manually (no shadow dir).
	t2Meta := `{"database":"db1","table":"t2","query":"CREATE TABLE db1.t2 (id UInt64) ENGINE=MergeTree ORDER BY id"}`
	t2Path := filepath.Join(src.Root, "metadata", "db1", "t2.json")
	if err := os.WriteFile(t2Path, []byte(t2Meta), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := cas.Upload(ctx, f, cfg, "bk", cas.UploadOptions{
		LocalBackupDir: src.Root,
	}); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Read the uploaded root metadata.json and assert both tables are listed.
	rc, err := f.GetFile(ctx, cas.MetadataJSONPath(cfg.ClusterPrefix(), "bk"))
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	var bm metadata.BackupMetadata
	if err := json.Unmarshal(body, &bm); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, tt := range bm.Tables {
		got[tt.Database+"."+tt.Table] = true
	}
	if !got["db1.t1"] || !got["db1.t2"] {
		t.Errorf("expected both db1.t1 and db1.t2 in bm.Tables; got %+v", bm.Tables)
	}

	// db1.t2 is schema-only; its per-table JSON must have an empty Parts map
	// (not {"default": null}), otherwise download would try to fetch a
	// nonexistent per-disk archive and fail with "cas: archive missing".
	rc2, err := f.GetFile(ctx, cas.TableMetaPath(cfg.ClusterPrefix(), "bk", "db1", "t2"))
	if err != nil {
		t.Fatal(err)
	}
	defer rc2.Close()
	body2, _ := io.ReadAll(rc2)
	var tmT2 metadata.TableMetadata
	if err := json.Unmarshal(body2, &tmT2); err != nil {
		t.Fatal(err)
	}
	if len(tmT2.Parts) != 0 {
		t.Errorf("empty-table Parts should be empty map, got %v", tmT2.Parts)
	}

	// Full download round-trip: proves the fix prevents "cas: archive missing".
	dst := t.TempDir()
	if _, err := cas.Download(ctx, f, cfg, "bk", cas.DownloadOptions{LocalBackupDir: dst}); err != nil {
		t.Fatalf("Download with empty table failed: %v", err)
	}
}

// ---------------------- test helpers ----------------------

// countingBackend wraps a Backend and counts PutFile calls per key.
type countingBackend struct {
	cas.Backend
	mu   sync.Mutex
	puts map[string]int
}

func newCountingBackend(b cas.Backend) *countingBackend {
	return &countingBackend{Backend: b, puts: map[string]int{}}
}

func (c *countingBackend) PutFile(ctx context.Context, key string, r io.ReadCloser, size int64) error {
	c.mu.Lock()
	c.puts[key]++
	c.mu.Unlock()
	return c.Backend.PutFile(ctx, key, r, size)
}

func (c *countingBackend) putsForPrefix(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k, v := range c.puts {
		if strings.HasPrefix(k, prefix) {
			n += v
		}
	}
	return n
}

// injectingBackend wraps a Backend and lets a test fire side effects
// each time StatFile is called.
type injectingBackend struct {
	cas.Backend
	onStat    func(key string)
	statCount int32
	injected  int32
}

func newInjectingBackend(b cas.Backend) *injectingBackend {
	return &injectingBackend{Backend: b}
}

func (i *injectingBackend) StatFile(ctx context.Context, key string) (int64, time.Time, bool, error) {
	if i.onStat != nil {
		i.onStat(key)
	}
	return i.Backend.StatFile(ctx, key)
}

// TestUpload_SpecialCharDbTable verifies the headline blocker fix from
// the external review: a database/table name containing characters that
// TablePathEncode percent-escapes (hyphen, dot, space, etc.) must round-
// trip without double-encoding. Before the fix, planUpload stored the
// already-encoded directory name verbatim in tablePlan.DB/Table, and
// TableMetaPath/PartArchivePath then encoded again, producing keys like
// "my%252Ddb" and breaking schema restore.
func TestUpload_SpecialCharDbTable(t *testing.T) {
	parts := []testfixtures.PartSpec{
		{
			Disk: "default", DB: "my-db", Table: "my-tbl", Name: "all_1_1_0",
			Files: []testfixtures.FileSpec{
				{Name: "columns.txt", Size: 8, HashLow: 1, HashHigh: 0},
			},
		},
	}
	lb := testfixtures.Build(t, parts)
	f := fakedst.New()
	cfg := testCfg(100)
	if _, err := cas.Upload(context.Background(), f, cfg, "bk1", cas.UploadOptions{
		LocalBackupDir: lb.Root,
	}); err != nil {
		t.Fatal(err)
	}

	// metadata.json — Tables[].Database/Table must be the DECODED original.
	rc, err := f.GetFile(context.Background(), cas.MetadataJSONPath(cfg.ClusterPrefix(), "bk1"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(rc)
	rc.Close()
	var bm metadata.BackupMetadata
	if err := json.Unmarshal(body, &bm); err != nil {
		t.Fatal(err)
	}
	if len(bm.Tables) != 1 {
		t.Fatalf("Tables: got %d want 1", len(bm.Tables))
	}
	if bm.Tables[0].Database != "my-db" {
		t.Errorf("Tables[0].Database: got %q want \"my-db\" (NOT %q)", bm.Tables[0].Database, "my%2Ddb")
	}
	if bm.Tables[0].Table != "my-tbl" {
		t.Errorf("Tables[0].Table: got %q want \"my-tbl\"", bm.Tables[0].Table)
	}

	// Per-table JSON exists at the SINGLE-encoded path.
	want := cas.TableMetaPath(cfg.ClusterPrefix(), "bk1", "my-db", "my-tbl")
	if _, _, exists, _ := f.StatFile(context.Background(), want); !exists {
		t.Errorf("per-table JSON missing at single-encoded path %s", want)
	}
	// Double-encoded path must NOT exist.
	bad := cas.TableMetaPath(cfg.ClusterPrefix(), "bk1", "my%2Ddb", "my%2Dtbl")
	if _, _, exists, _ := f.StatFile(context.Background(), bad); exists {
		t.Errorf("per-table JSON wrongly exists at DOUBLE-encoded path %s", bad)
	}
}

// TestPlanPart_WithProjection_BlobsBothLevels verifies the walker treats
// .proj entries in the parent checksums.txt as nested-part directories,
// recurses into them, and emits blobs for above-threshold files at any
// depth while preserving paths in archive entries.
func TestPlanPart_WithProjection_BlobsBothLevels(t *testing.T) {
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
		Files: []testfixtures.FileSpec{
			{Name: "data.bin", Size: 8192, HashLow: 1, HashHigh: 2},  // above threshold → blob
			{Name: "columns.txt", Size: 16, HashLow: 3, HashHigh: 4}, // below → archive
		},
		Projections: []testfixtures.ProjectionSpec{{
			Name: "p1",
			Files: []testfixtures.FileSpec{
				{Name: "data.bin", Size: 4096, HashLow: 5, HashHigh: 6}, // above → blob (different hash)
				{Name: "columns.txt", Size: 8, HashLow: 7, HashHigh: 8}, // below → archive
			},
			AggregateHashLow: 99, AggregateHashHigh: 99, AggregateSize: 4120,
		}},
	}}
	src := testfixtures.Build(t, parts)
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()

	res, err := cas.Upload(ctx, f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root})
	if err != nil {
		t.Fatal(err)
	}
	// Two unique blobs (parent data.bin + projection data.bin); the
	// p1.proj aggregate entry must NOT become a blob.
	if res.UniqueBlobs != 2 {
		t.Errorf("UniqueBlobs: got %d, want 2", res.UniqueBlobs)
	}
	cp := cfg.ClusterPrefix()
	projHash := cas.Hash128{Low: 5, High: 6}
	if _, _, exists, _ := f.StatFile(ctx, cas.BlobPath(cp, projHash)); !exists {
		t.Error("projection data.bin blob missing in remote")
	}
	bogus := cas.Hash128{Low: 99, High: 99}
	if _, _, exists, _ := f.StatFile(ctx, cas.BlobPath(cp, bogus)); exists {
		t.Error("p1.proj aggregate must not become a blob")
	}
}

// TestPlanPart_NonChecksumFilesPreserved verifies files in the part
// directory that aren't listed in checksums.txt (columns.txt, etc.) still
// land in the per-table archive. Without the new walker they were dropped.
func TestPlanPart_NonChecksumFilesPreserved(t *testing.T) {
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
		Files: []testfixtures.FileSpec{
			{Name: "data.bin", Size: 16, HashLow: 1, HashHigh: 2}, // listed
		},
	}}
	src := testfixtures.Build(t, parts)
	rogue := filepath.Join(src.Root, "shadow", "db1", "t1", "default", "all_1_1_0", "metadata_version.txt")
	if err := os.WriteFile(rogue, []byte("42\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()
	if _, err := cas.Upload(ctx, f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
		t.Fatal(err)
	}
	arch := cas.PartArchivePath(cfg.ClusterPrefix(), "bk", "default", "db1", "t1")
	rc, err := f.GetFile(ctx, arch)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	zr, err := zstd.NewReader(rc)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	found := map[string]bool{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		found[h.Name] = true
	}
	if !found["all_1_1_0/metadata_version.txt"] {
		t.Errorf("metadata_version.txt not in archive; found %v", found)
	}
	if !found["all_1_1_0/checksums.txt"] {
		t.Errorf("checksums.txt missing from archive; found %v", found)
	}
}

// TestPlanPart_NestedProjectionDedup verifies that two parts with
// identical projection content produce ONE blob ref, not two.
func TestPlanPart_NestedProjectionDedup(t *testing.T) {
	mkPart := func(name string) testfixtures.PartSpec {
		return testfixtures.PartSpec{
			Disk: "default", DB: "db1", Table: "t1", Name: name,
			Files: []testfixtures.FileSpec{
				{Name: "data.bin", Size: 2048, HashLow: 11, HashHigh: 22},
			},
			Projections: []testfixtures.ProjectionSpec{{
				Name: "p1",
				Files: []testfixtures.FileSpec{
					{Name: "data.bin", Size: 4096, HashLow: 99, HashHigh: 99},
				},
				AggregateHashLow: 1, AggregateHashHigh: 1, AggregateSize: 4096,
			}},
		}
	}
	parts := []testfixtures.PartSpec{mkPart("all_1_1_0"), mkPart("all_2_2_0")}
	src := testfixtures.Build(t, parts)
	f := fakedst.New()
	cfg := testCfg(1024)
	res, err := cas.Upload(context.Background(), f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root})
	if err != nil {
		t.Fatal(err)
	}
	if res.UniqueBlobs != 2 {
		t.Errorf("UniqueBlobs: got %d, want 2 (parent data.bin + shared projection data.bin)", res.UniqueBlobs)
	}
}

// TestPlanPart_MissingListedFile_Fails verifies the walker fails when
// checksums.txt lists a file that's absent on disk.
func TestPlanPart_MissingListedFile_Fails(t *testing.T) {
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
		Files: []testfixtures.FileSpec{
			{Name: "data.bin", Size: 8, HashLow: 1, HashHigh: 2},
		},
	}}
	src := testfixtures.Build(t, parts)
	if err := os.Remove(filepath.Join(src.Root, "shadow", "db1", "t1", "default", "all_1_1_0", "data.bin")); err != nil {
		t.Fatal(err)
	}
	f := fakedst.New()
	cfg := testCfg(1024)
	_, err := cas.Upload(context.Background(), f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root})
	if err == nil {
		t.Fatal("expected upload failure when listed file is missing on disk")
	}
	if !strings.Contains(err.Error(), "data.bin") {
		t.Errorf("error should mention data.bin; got: %v", err)
	}
}

// TestPlanPart_HiddenFile_Warns verifies a hidden file is skipped (warn).
func TestPlanPart_HiddenFile_Warns(t *testing.T) {
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
		Files: []testfixtures.FileSpec{
			{Name: "data.bin", Size: 8, HashLow: 1, HashHigh: 2},
		},
	}}
	src := testfixtures.Build(t, parts)
	hidden := filepath.Join(src.Root, "shadow", "db1", "t1", "default", "all_1_1_0", ".hidden")
	if err := os.WriteFile(hidden, []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	f := fakedst.New()
	cfg := testCfg(1024)
	if _, err := cas.Upload(context.Background(), f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
		t.Fatalf("hidden file should warn-and-skip, not fail: %v", err)
	}
	arch := cas.PartArchivePath(cfg.ClusterPrefix(), "bk", "default", "db1", "t1")
	rc, err := f.GetFile(context.Background(), arch)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	zr, err := zstd.NewReader(rc)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(h.Name, ".hidden") {
			t.Errorf("hidden file leaked into archive: %s", h.Name)
		}
	}
}

// TestPlanPart_ProjEntryNotADir_Fails verifies the walker fails loudly
// when checksums.txt has a .proj entry whose target on disk is a regular
// file rather than a directory.
func TestPlanPart_ProjEntryNotADir_Fails(t *testing.T) {
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
		Files: []testfixtures.FileSpec{
			{Name: "data.bin", Size: 8, HashLow: 1, HashHigh: 2},
		},
	}}
	src := testfixtures.Build(t, parts)
	partDir := filepath.Join(src.Root, "shadow", "db1", "t1", "default", "all_1_1_0")
	rogue := filepath.Join(partDir, "p1.proj")
	if err := os.WriteFile(rogue, []byte("not a dir"), 0o644); err != nil {
		t.Fatal(err)
	}
	rewritten := `checksums format version: 2
2 files:
data.bin
	size: 8
	hash: 1 2
	compressed: 0
p1.proj
	size: 9
	hash: 3 4
	compressed: 0
`
	if err := os.WriteFile(filepath.Join(partDir, "checksums.txt"), []byte(rewritten), 0o644); err != nil {
		t.Fatal(err)
	}
	f := fakedst.New()
	cfg := testCfg(1024)
	_, err := cas.Upload(context.Background(), f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root})
	if err == nil {
		t.Fatal("expected upload failure when .proj entry is not a directory")
	}
	if !strings.Contains(err.Error(), "p1.proj") {
		t.Errorf("error should mention p1.proj; got: %v", err)
	}
}

// TestPlanPart_OrphanProjDir_Warns verifies a .proj directory present on
// disk with no parent checksums.txt entry is skipped (warn) rather than
// fail.
func TestPlanPart_OrphanProjDir_Warns(t *testing.T) {
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
		Files: []testfixtures.FileSpec{
			{Name: "data.bin", Size: 8, HashLow: 1, HashHigh: 2},
		},
	}}
	src := testfixtures.Build(t, parts)
	orphan := filepath.Join(src.Root, "shadow", "db1", "t1", "default", "all_1_1_0", "p2.proj")
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "data.bin"), []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}
	f := fakedst.New()
	cfg := testCfg(1024)
	if _, err := cas.Upload(context.Background(), f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
		t.Fatalf("orphan .proj dir should warn-and-skip, not fail: %v", err)
	}
}

// TestUpload_RefusesIfInprogressMarkerPresent verifies that a second
// cas-upload attempt for the same backup name fails cleanly when an
// inprogress marker already exists. Without the conditional-create fix,
// the second upload would overwrite the marker and proceed.
func TestUpload_RefusesIfInprogressMarkerPresent(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()

	// Pre-write a marker simulating another host's upload in flight.
	if _, err := cas.WriteInProgressMarker(ctx, f, cfg.ClusterPrefix(), "bk", "host-other"); err != nil {
		t.Fatalf("WriteInProgressMarker setup: %v", err)
	}

	// Build a synthetic local backup; the upload should refuse before
	// touching any blob.
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "p1",
		Files: []testfixtures.FileSpec{{Name: "data.bin", Size: 16, HashLow: 1, HashHigh: 2}},
	}}
	src := testfixtures.Build(t, parts)

	_, err := cas.Upload(ctx, f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root})
	if err == nil {
		t.Fatal("expected Upload to refuse when inprogress marker is present")
	}
	if !strings.Contains(err.Error(), "another cas-upload is in progress") {
		t.Errorf("error should mention concurrent upload; got: %v", err)
	}
}

// TestUpload_TableFilter_WithSpecialChars proves that --tables filtering
// works against the decoded names operators actually type, not the
// shadow-directory encoded forms.
func TestUpload_TableFilter_WithSpecialChars(t *testing.T) {
	parts := []testfixtures.PartSpec{
		{Disk: "default", DB: "my-db", Table: "keep-me", Name: "p1",
			Files: []testfixtures.FileSpec{{Name: "columns.txt", Size: 4, HashLow: 1, HashHigh: 0}}},
		{Disk: "default", DB: "my-db", Table: "skip-me", Name: "p1",
			Files: []testfixtures.FileSpec{{Name: "columns.txt", Size: 4, HashLow: 2, HashHigh: 0}}},
	}
	lb := testfixtures.Build(t, parts)
	f := fakedst.New()
	cfg := testCfg(100)
	if _, err := cas.Upload(context.Background(), f, cfg, "bk1", cas.UploadOptions{
		LocalBackupDir: lb.Root,
		TableFilter:    []string{"my-db.keep-me"},
	}); err != nil {
		t.Fatal(err)
	}
	keep := cas.TableMetaPath(cfg.ClusterPrefix(), "bk1", "my-db", "keep-me")
	skip := cas.TableMetaPath(cfg.ClusterPrefix(), "bk1", "my-db", "skip-me")
	if _, _, exists, _ := f.StatFile(context.Background(), keep); !exists {
		t.Errorf("filter dropped the matching table; %s missing", keep)
	}
	if _, _, exists, _ := f.StatFile(context.Background(), skip); exists {
		t.Errorf("filter let a non-matching table through; %s present", skip)
	}
}

// TestUpload_LeaksNoMarkerOnRecheckError verifies that a StatFile failure
// at step 11b (the upload's own-marker re-check) cleans up the in-progress
// marker before returning the error. Without the cleanup, the marker
// persists for up to abandon_threshold (7 days) and locks out future cas-upload
// invocations of the same backup name.
func TestUpload_LeaksNoMarkerOnRecheckError(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()

	// Build a tiny synthetic backup so Upload reaches step 11b.
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "p1",
		Files: []testfixtures.FileSpec{{Name: "data.bin", Size: 16, HashLow: 1, HashHigh: 2}},
	}}
	src := testfixtures.Build(t, parts)

	// Hook fakedst to inject a StatFile error specifically on the
	// in-progress marker key, AFTER the marker has been written.
	markerKey := cas.InProgressMarkerPath(cfg.ClusterPrefix(), "bk")
	f.SetStatHook(func(key string) (size int64, modTime time.Time, exists bool, err error, override bool) {
		if key == markerKey {
			return 0, time.Time{}, false, errors.New("simulated transient backend error"), true
		}
		return 0, time.Time{}, false, nil, false
	})

	_, err := cas.Upload(ctx, f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root})
	if err == nil {
		t.Fatal("expected Upload to error when StatFile on own marker fails")
	}
	if !strings.Contains(err.Error(), "re-check inprogress marker") {
		t.Errorf("error should mention re-check; got: %v", err)
	}

	// The cleanup must have run despite the error path.
	// Clear the hook so we can check the actual backend state.
	f.SetStatHook(nil)
	_, _, exists, _ := f.StatFile(context.Background(), markerKey)
	if exists {
		t.Error("in-progress marker leaked: still present after step 11b error path")
	}
}

// TestUpload_AbortsIfColdListedBlobDisappearsBeforeCommit verifies that if a
// blob was skipped during upload (because cold-list said it already existed)
// but is gone by the time we reach step 11c, Upload returns an error and
// does NOT write metadata.json.
func TestUpload_AbortsIfColdListedBlobDisappearsBeforeCommit(t *testing.T) {
	ctx := context.Background()
	f := fakedst.New()
	// Use a threshold high enough that data.bin (1024 bytes) is uploaded as a
	// blob, not inlined.
	cfg := testCfg(100)
	cp := cfg.ClusterPrefix()

	// Build a local backup with one part containing a 1024-byte data.bin blob.
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "p1",
		Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 1, HashHigh: 100},
			{Name: "primary.idx", Size: 8, HashLow: 2, HashHigh: 100},
			{Name: "data.bin", Size: 1024, HashLow: 3, HashHigh: 100},
		},
	}}
	lb := testfixtures.Build(t, parts)

	// First, do a successful upload to populate the backend with the blob and
	// confirm the harness works. After this, metadata.json for "seed-backup"
	// exists and the blob is stored in the backend.
	_, err := cas.Upload(ctx, f, cfg, "seed-backup", cas.UploadOptions{LocalBackupDir: lb.Root})
	if err != nil {
		t.Fatalf("seed upload failed: %v", err)
	}

	// Confirm the blob exists in the backend (it was uploaded in seed phase).
	blobPrefix := cp + "blob/"
	var coldHitKey string
	if err := f.Walk(ctx, blobPrefix, true, func(rf cas.RemoteFile) error {
		coldHitKey = rf.Key
		return nil
	}); err != nil {
		t.Fatalf("Walk to find blob key: %v", err)
	}
	if coldHitKey == "" {
		t.Fatal("no blob found in backend after seed upload")
	}

	// Install a StatHook that makes the seeded blob appear to be gone (simulating
	// a concurrent prune deleting it between ColdList and step 11c).
	// ColdList uses Walk (not StatFile), so it will still see the blob as present
	// and upload will skip re-uploading it. The hook only fires during step 11c.
	f.SetStatHook(func(key string) (int64, time.Time, bool, error, bool) {
		if key == coldHitKey {
			// Blob "disappeared": return exists=false, override=true.
			return 0, time.Time{}, false, nil, true
		}
		// Pass through all other keys.
		return 0, time.Time{}, false, nil, false
	})

	// Now run a second upload for "test-backup". The cold-list will see the blob
	// (Walk is not hooked), uploadMissingBlobs will skip it, and step 11c will
	// detect that StatFile returns not-found → abort.
	_, err = cas.Upload(ctx, f, cfg, "test-backup", cas.UploadOptions{LocalBackupDir: lb.Root})
	if err == nil {
		t.Fatal("expected Upload to abort when cold-listed blob disappears before commit")
	}
	if !strings.Contains(err.Error(), "cold-listed blob") {
		t.Errorf("error should mention 'cold-listed blob'; got: %v", err)
	}
	if !strings.Contains(err.Error(), "disappeared before commit") {
		t.Errorf("error should mention 'disappeared before commit'; got: %v", err)
	}

	// metadata.json must NOT have been written.
	f.SetStatHook(nil)
	_, _, exists, _ := f.StatFile(ctx, cas.MetadataJSONPath(cp, "test-backup"))
	if exists {
		t.Error("metadata.json was written despite cold-listed blob disappearing")
	}
}

// TestUpload_LeaksNoMarkerOnCommitError verifies that a PutFile failure
// on metadata.json at step 12 cleans up the in-progress marker before
// returning the error.
func TestUpload_LeaksNoMarkerOnCommitError(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()

	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "p1",
		Files: []testfixtures.FileSpec{{Name: "data.bin", Size: 16, HashLow: 1, HashHigh: 2}},
	}}
	src := testfixtures.Build(t, parts)

	metadataKey := cas.MetadataJSONPath(cfg.ClusterPrefix(), "bk")
	f.SetPutHook(func(key string) (err error, override bool) {
		if key == metadataKey {
			return errors.New("simulated transient backend error"), true
		}
		return nil, false
	})

	_, err := cas.Upload(ctx, f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root})
	if err == nil {
		t.Fatal("expected Upload to error when metadata.json PUT fails")
	}
	if !strings.Contains(err.Error(), "put metadata.json") {
		t.Errorf("error should mention metadata.json; got: %v", err)
	}

	// Clear the hook so the post-call StatFile reads actual backend state.
	f.SetPutHook(nil)

	markerKey := cas.InProgressMarkerPath(cfg.ClusterPrefix(), "bk")
	_, _, exists, _ := f.StatFile(context.Background(), markerKey)
	if exists {
		t.Error("in-progress marker leaked: still present after metadata.json failure")
	}
}
