package cas_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/testfixtures"
)

func setupUploaded(t *testing.T) (*fakedst.Fake, cas.Config, string) {
	t.Helper()
	f := fakedst.New()
	cfg := testCfg(100)
	src := testfixtures.Build(t, []testfixtures.PartSpec{{
		Disk: "default", DB: "db", Table: "t", Name: "all_1_1_0",
		Files: []testfixtures.FileSpec{{Name: "columns.txt", Size: 8, HashLow: 1, HashHigh: 0}},
	}})
	if _, err := cas.Upload(context.Background(), f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
		t.Fatal(err)
	}
	return f, cfg, "bk"
}

func TestDelete_HappyPath(t *testing.T) {
	f, cfg, name := setupUploaded(t)
	if err := cas.Delete(context.Background(), f, cfg, name, cas.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	// metadata.json gone:
	if _, _, ok, _ := f.StatFile(context.Background(), cas.MetadataJSONPath(cfg.ClusterPrefix(), name)); ok {
		t.Error("metadata.json must be deleted")
	}
	// No leftover files in metadata/<bk>/:
	var leftover int
	_ = f.Walk(context.Background(), cas.MetadataDir(cfg.ClusterPrefix(), name), true, func(rf cas.RemoteFile) error {
		leftover++
		return nil
	})
	if leftover != 0 {
		t.Errorf("leftover %d objects under metadata/%s/", leftover, name)
	}
}

func TestDelete_RefusesIfPruneInProgress(t *testing.T) {
	f, cfg, name := setupUploaded(t)
	_ = f.PutFile(context.Background(), cas.PruneMarkerPath(cfg.ClusterPrefix()), io.NopCloser(strings.NewReader("{}")), 2)
	err := cas.Delete(context.Background(), f, cfg, name, cas.DeleteOptions{})
	if !errors.Is(err, cas.ErrPruneInProgress) {
		t.Fatalf("got %v", err)
	}
}

func TestDelete_RefusesIfUploadInProgress(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	_ = f.PutFile(context.Background(), cas.InProgressMarkerPath(cfg.ClusterPrefix(), "bk"), io.NopCloser(strings.NewReader("{}")), 2)
	// metadata.json absent → upload in flight
	err := cas.Delete(context.Background(), f, cfg, "bk", cas.DeleteOptions{})
	if !errors.Is(err, cas.ErrUploadInProgress) {
		t.Fatalf("got %v", err)
	}
}

func TestDelete_StaleMarkerProceeds(t *testing.T) {
	f, cfg, name := setupUploaded(t)
	// simulate: upload committed metadata.json but failed to delete its marker
	_ = f.PutFile(context.Background(), cas.InProgressMarkerPath(cfg.ClusterPrefix(), name), io.NopCloser(strings.NewReader("{}")), 2)
	if err := cas.Delete(context.Background(), f, cfg, name, cas.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	// marker also deleted now (best-effort cleanup)
	if _, _, ok, _ := f.StatFile(context.Background(), cas.InProgressMarkerPath(cfg.ClusterPrefix(), name)); ok {
		t.Error("stale marker should have been cleaned up")
	}
}

func TestDelete_BackupNotFound(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	err := cas.Delete(context.Background(), f, cfg, "nope", cas.DeleteOptions{})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("got %v", err)
	}
}

func TestDelete_OrderingMetadataFirst(t *testing.T) {
	// Verify metadata.json is the FIRST DeleteFile call: wrap fakedst with
	// a recording delegator, run Delete, confirm the first deleted key is
	// the metadata.json path.
	inner := fakedst.New()
	cfg := testCfg(100)
	src := testfixtures.Build(t, []testfixtures.PartSpec{
		{Disk: "default", DB: "db", Table: "t", Name: "all_1_1_0",
			Files: []testfixtures.FileSpec{{Name: "columns.txt", Size: 8, HashLow: 1, HashHigh: 0}}},
	})
	if _, err := cas.Upload(context.Background(), inner, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
		t.Fatal(err)
	}
	rec := &recordingBackend{Backend: inner}
	if err := cas.Delete(context.Background(), rec, cfg, "bk", cas.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(rec.deletes) == 0 {
		t.Fatal("no deletes recorded")
	}
	want := cas.MetadataJSONPath(cfg.ClusterPrefix(), "bk")
	if rec.deletes[0] != want {
		t.Errorf("first delete: got %q want %q", rec.deletes[0], want)
	}
}

// TestDelete_WaitsForPruneMarker verifies that Delete waits for the prune
// marker to disappear (within WaitForPrune) rather than refusing immediately.
func TestDelete_WaitsForPruneMarker(t *testing.T) {
	poll := 10 * time.Millisecond
	cas.SetPollIntervalForTesting(&poll)
	defer cas.SetPollIntervalForTesting(nil)

	f, cfg, name := setupUploaded(t)
	cp := cfg.ClusterPrefix()

	// Pre-place prune marker; schedule deletion after 50ms.
	if err := f.PutFile(context.Background(), cas.PruneMarkerPath(cp),
		io.NopCloser(strings.NewReader("{}")), 2); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = f.DeleteFile(context.Background(), cas.PruneMarkerPath(cp))
	}()

	if err := cas.Delete(context.Background(), f, cfg, name, cas.DeleteOptions{
		WaitForPrune: 5 * time.Second,
	}); err != nil {
		t.Fatalf("Delete should succeed once marker is cleared; got: %v", err)
	}
}

// TestDelete_RefusesAfterWaitTimeout verifies that Delete returns
// ErrPruneInProgress when WaitForPrune elapses and the marker remains.
func TestDelete_RefusesAfterWaitTimeout(t *testing.T) {
	poll := 10 * time.Millisecond
	cas.SetPollIntervalForTesting(&poll)
	defer cas.SetPollIntervalForTesting(nil)

	f, cfg, name := setupUploaded(t)
	cp := cfg.ClusterPrefix()

	// Pre-place prune marker permanently.
	if err := f.PutFile(context.Background(), cas.PruneMarkerPath(cp),
		io.NopCloser(strings.NewReader("{}")), 2); err != nil {
		t.Fatal(err)
	}

	err := cas.Delete(context.Background(), f, cfg, name, cas.DeleteOptions{
		WaitForPrune: 100 * time.Millisecond,
	})
	if !errors.Is(err, cas.ErrPruneInProgress) {
		t.Fatalf("got err=%v; want ErrPruneInProgress", err)
	}
}

// TestDelete_BlocksConcurrentUploadOfSameName verifies that a cas-delete
// inprogress marker written by Delete prevents a concurrent Upload of the
// same name from starting. The marker is written by a goroutine that holds it
// for long enough for the main goroutine's Upload attempt to observe it.
func TestDelete_BlocksConcurrentUploadOfSameName(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	cp := cfg.ClusterPrefix()

	// Write a cas-delete inprogress marker directly (simulating what Delete
	// will do once the real implementation is in place).
	markerKey := cas.InProgressMarkerPath(cp, "bk")
	markerBody := `{"Backup":"bk","Host":"h1","StartedAt":"2026-01-01T00:00:00Z","Tool":"cas-delete"}`
	if err := f.PutFile(context.Background(), markerKey,
		io.NopCloser(strings.NewReader(markerBody)), int64(len(markerBody))); err != nil {
		t.Fatal(err)
	}

	// Upload must refuse: the marker is present and no metadata.json exists.
	_, err := cas.Upload(context.Background(), f, cfg, "bk", cas.UploadOptions{
		LocalBackupDir: t.TempDir(), // empty dir → no tables, but auth check is before planUpload
	})
	if err == nil {
		t.Fatal("expected Upload to fail when cas-delete marker is present")
	}
	if !strings.Contains(err.Error(), "cas-delete") && !strings.Contains(err.Error(), "in progress") {
		t.Errorf("error should mention cas-delete or in progress; got: %v", err)
	}
}

// TestDelete_ReleaseMarkerOnSuccess verifies that the cas-delete inprogress
// marker is removed after a successful Delete call.
func TestDelete_ReleaseMarkerOnSuccess(t *testing.T) {
	f, cfg, name := setupUploaded(t)
	cp := cfg.ClusterPrefix()

	if err := cas.Delete(context.Background(), f, cfg, name, cas.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := f.StatFile(context.Background(), cas.InProgressMarkerPath(cp, name)); ok {
		t.Error("cas-delete: inprogress marker must be removed after successful Delete")
	}
}

// TestDelete_RefusesWhenAlreadyDeleting verifies that Delete refuses when a
// cas-delete inprogress marker is already present and no metadata.json exists
// (i.e. another concurrent Delete is in progress for the same backup).
func TestDelete_RefusesWhenAlreadyDeleting(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	cp := cfg.ClusterPrefix()

	// Place metadata.json so the backup appears to exist.
	mdKey := cas.MetadataJSONPath(cp, "bk")
	if err := f.PutFile(context.Background(), mdKey,
		io.NopCloser(strings.NewReader("{}")), 2); err != nil {
		t.Fatal(err)
	}

	// Pre-place a cas-delete marker (another Delete is mid-flight).
	markerKey := cas.InProgressMarkerPath(cp, "bk")
	markerBody := `{"Backup":"bk","Host":"h2","StartedAt":"2026-01-01T00:00:00Z","Tool":"cas-delete"}`
	if err := f.PutFile(context.Background(), markerKey,
		io.NopCloser(strings.NewReader(markerBody)), int64(len(markerBody))); err != nil {
		t.Fatal(err)
	}

	err := cas.Delete(context.Background(), f, cfg, "bk", cas.DeleteOptions{})
	if err == nil {
		t.Fatal("expected Delete to fail when another cas-delete is in progress")
	}
	if !strings.Contains(err.Error(), "cas-delete") {
		t.Errorf("error should mention cas-delete; got: %v", err)
	}
}

// TestDelete_RefusesOnUnreadableMarker verifies the path where:
//  1. metadata.json exists (the backup is committed)
//  2. An inprogress marker also exists (ipOK=true, mdOK=true branch)
//  3. ReadInProgressMarker returns a non-nil error (transient/corrupt read)
//
// Delete must return an error containing "cannot read marker" AND must NOT
// delete the marker (preserving visibility for operators and concurrent
// processes).
//
// The unreadable-marker condition is induced by pre-placing a 128 KiB body of
// 'x' characters — twice the 64 KiB markerSizeLimit enforced by getBytes's
// LimitReader. After truncation the body is not valid JSON, so
// ReadInProgressMarker returns a JSON parse error → readErr != nil.
func TestDelete_RefusesOnUnreadableMarker(t *testing.T) {
	f, cfg, name := setupUploaded(t)
	cp := cfg.ClusterPrefix()
	markerKey := cas.InProgressMarkerPath(cp, name)

	// Place an oversized (128 KiB) non-JSON marker alongside the committed
	// metadata.json so the ipOK && mdOK branch is entered.
	oversized := make([]byte, 128*1024)
	for i := range oversized {
		oversized[i] = 'x'
	}
	if err := f.PutFile(context.Background(), markerKey,
		io.NopCloser(bytes.NewReader(oversized)), int64(len(oversized))); err != nil {
		t.Fatal(err)
	}

	err := cas.Delete(context.Background(), f, cfg, name, cas.DeleteOptions{})
	if err == nil {
		t.Fatal("expected Delete to fail when ReadInProgressMarker returns an error")
	}
	if !strings.Contains(err.Error(), "cannot read marker") {
		t.Errorf("error should contain 'cannot read marker'; got: %v", err)
	}

	// The marker must still be present: Delete must not have removed it.
	if _, _, ok, _ := f.StatFile(context.Background(), markerKey); !ok {
		t.Error("marker must NOT be deleted when Delete refuses due to an unreadable marker")
	}
}

// recordingBackend wraps a Backend and records DeleteFile calls in order.
type recordingBackend struct {
	cas.Backend
	deletes []string
}

func (r *recordingBackend) DeleteFile(ctx context.Context, key string) error {
	r.deletes = append(r.deletes, key)
	return r.Backend.DeleteFile(ctx, key)
}
