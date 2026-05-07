package cas_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/testfixtures"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

// testCfg returns a CAS config valid enough that Upload doesn't reject
// it on Validate(). Threshold 100 keeps small files inline and pushes
// 1024-byte files to blob.
func testCfg(threshold uint64) cas.Config {
	return cas.Config{
		Enabled:          true,
		ClusterID:        "c1",
		RootPrefix:       "cas/",
		InlineThreshold:  threshold,
		GraceBlob:        24 * time.Hour,
		AbandonThreshold: 7 * 24 * time.Hour,
	}
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

// ---------------------- test helpers ----------------------

// countingBackend wraps a Backend and counts PutFile calls per key.
type countingBackend struct {
	cas.Backend
	mu     sync.Mutex
	puts   map[string]int
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
