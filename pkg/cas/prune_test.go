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
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/stretchr/testify/require"
)

// uploadTestBackup builds a synthetic local backup with one part containing
// one inline file + one above-threshold blob, then cas.Uploads it.
// Returns the upload result so callers can inspect blob sizes.
func uploadTestBackup(t *testing.T, f *fakedst.Fake, cfg cas.Config, name string, blobHash cas.Hash128) {
	t.Helper()
	ctx := context.Background()
	parts := []testfixtures.PartSpec{
		{
			Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
			Files: []testfixtures.FileSpec{
				{Name: "columns.txt", Size: 16, HashLow: 1, HashHigh: 0}, // inline
				{Name: "data.bin", Size: 4096, HashLow: blobHash.Low, HashHigh: blobHash.High},
			},
		},
	}
	src := testfixtures.Build(t, parts)
	if _, err := cas.Upload(ctx, f, cfg, name, cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
		t.Fatalf("Upload %s: %v", name, err)
	}
}

func ageBlob(t *testing.T, f *fakedst.Fake, cfg cas.Config, h cas.Hash128, age time.Duration) {
	t.Helper()
	f.SetModTime(cas.BlobPath(cfg.ClusterPrefix(), h), time.Now().Add(-age))
}

func TestPrune_HappyPath(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()

	// 2 backups, 4 distinct blobs.
	hShared := cas.Hash128{Low: 0x10, High: 0x10}
	h1 := cas.Hash128{Low: 0x20, High: 0x10}
	h2 := cas.Hash128{Low: 0x30, High: 0x10}
	hOrphanOld := cas.Hash128{Low: 0x40, High: 0x10}
	hOrphanFresh := cas.Hash128{Low: 0x50, High: 0x10}

	uploadTestBackup(t, f, cfg, "bk1", hShared)
	uploadTestBackup(t, f, cfg, "bk2", h1)

	// Manually drop two more blobs that aren't referenced by any backup.
	cp := cfg.ClusterPrefix()
	for _, h := range []cas.Hash128{hOrphanOld, hOrphanFresh, h2} {
		_ = f.PutFile(ctx, cas.BlobPath(cp, h), io.NopCloser(bytes.NewReader([]byte("x"))), 1)
	}
	// Age the orphan-old and h2 (also unreferenced) past grace; orphan-fresh stays inside grace.
	ageBlob(t, f, cfg, hOrphanOld, 2*time.Hour)
	ageBlob(t, f, cfg, h2, 2*time.Hour)
	ageBlob(t, f, cfg, hOrphanFresh, 30*time.Minute)
	// Also age the referenced blobs past grace (they should NOT be deleted).
	ageBlob(t, f, cfg, hShared, 2*time.Hour)
	ageBlob(t, f, cfg, h1, 2*time.Hour)

	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{GraceBlob: time.Hour, GraceBlobSet: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.OrphansDeleted != 2 {
		t.Errorf("OrphansDeleted: got %d want 2 (hOrphanOld + h2)", rep.OrphansDeleted)
	}
	// hOrphanFresh (within grace) and the referenced blobs must survive.
	if _, _, exists, _ := f.StatFile(ctx, cas.BlobPath(cp, hOrphanFresh)); !exists {
		t.Error("hOrphanFresh should be retained (within grace)")
	}
	if _, _, exists, _ := f.StatFile(ctx, cas.BlobPath(cp, hShared)); !exists {
		t.Error("hShared (referenced) must survive prune")
	}
	// Marker is gone (defer release).
	if _, _, exists, _ := f.StatFile(ctx, cas.PruneMarkerPath(cp)); exists {
		t.Error("prune.marker should be released after Prune returns")
	}
}

func TestPrune_RefusesIfFreshInProgressMarker(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()
	if _, err := cas.WriteInProgressMarker(ctx, f, cfg.ClusterPrefix(), "bk_running", "host-a"); err != nil {
		t.Fatal(err)
	}
	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{AbandonThreshold: time.Hour, AbandonThresholdSet: true})
	if err == nil || !strings.Contains(err.Error(), "in-progress upload") {
		t.Fatalf("want fresh-inprogress refusal, got rep=%+v err=%v", rep, err)
	}
	// Anti-regression: the error must point operators at --abandon-threshold,
	// not at --unlock (which removes the prune.marker, not inprogress markers).
	if !strings.Contains(err.Error(), "--abandon-threshold") {
		t.Errorf("error should point operators at --abandon-threshold; got: %v", err)
	}
	if strings.Contains(err.Error(), "--unlock") {
		t.Errorf("error should not suggest --unlock for inprogress markers; got: %v", err)
	}
}

func TestPrune_SweepsAbandonedMarker(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()
	cp := cfg.ClusterPrefix()
	if _, err := cas.WriteInProgressMarker(ctx, f, cp, "bk_dead", "host-a"); err != nil {
		t.Fatal(err)
	}
	// Age past abandon_threshold (1h here, default 7d).
	f.SetModTime(cas.InProgressMarkerPath(cp, "bk_dead"), time.Now().Add(-2*time.Hour))

	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{AbandonThreshold: time.Hour, AbandonThresholdSet: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.AbandonedMarkersFound != 1 {
		t.Errorf("AbandonedMarkersFound: got %d want 1", rep.AbandonedMarkersFound)
	}
	if _, _, exists, _ := f.StatFile(ctx, cas.InProgressMarkerPath(cp, "bk_dead")); exists {
		t.Error("abandoned marker should be deleted by prune")
	}
}

// failingBackend wraps cas.Backend and forces GetFile to fail for one key —
// used to inject a "live backup unreadable" error mid-prune.
type failingBackend struct {
	cas.Backend
	failGetKey string
}

func (f *failingBackend) GetFile(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == f.failGetKey {
		return nil, errors.New("simulated network error")
	}
	return f.Backend.GetFile(ctx, key)
}

func TestPrune_FailClosedOnUnreadableLiveBackup(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()

	uploadTestBackup(t, f, cfg, "bk1", cas.Hash128{Low: 0x10, High: 0x10})

	// Inject a failure for bk1's per-table archive.
	cp := cfg.ClusterPrefix()
	failKey := cas.PartArchivePath(cp, "bk1", "default", "db1", "t1")
	fb := &failingBackend{Backend: f, failGetKey: failKey}

	// Drop an unreferenced blob that prune SHOULD delete on a healthy run.
	hOrphan := cas.Hash128{Low: 0x99, High: 0x99}
	_ = f.PutFile(ctx, cas.BlobPath(cp, hOrphan), io.NopCloser(bytes.NewReader([]byte("x"))), 1)
	ageBlob(t, f, cfg, hOrphan, 2*time.Hour)

	rep, err := cas.Prune(ctx, fb, cfg, cas.PruneOptions{GraceBlob: time.Hour, GraceBlobSet: true})
	if err == nil {
		t.Fatal("expected fail-closed error from unreadable live backup")
	}
	if rep.OrphansDeleted != 0 {
		t.Errorf("OrphansDeleted: got %d want 0 (must NOT delete after fail-close)", rep.OrphansDeleted)
	}
	// Orphan blob must still exist.
	if _, _, exists, _ := f.StatFile(ctx, cas.BlobPath(cp, hOrphan)); !exists {
		t.Error("orphan must survive a fail-closed prune")
	}
	// Marker is gone (defer release runs even on error).
	if _, _, exists, _ := f.StatFile(ctx, cas.PruneMarkerPath(cp)); exists {
		t.Error("prune.marker should be released even on error path")
	}
}

func TestPrune_DryRun(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()
	cp := cfg.ClusterPrefix()

	hOrphan := cas.Hash128{Low: 0x77, High: 0x77}
	_ = f.PutFile(ctx, cas.BlobPath(cp, hOrphan), io.NopCloser(bytes.NewReader([]byte("x"))), 1)
	ageBlob(t, f, cfg, hOrphan, 2*time.Hour)

	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{DryRun: true, GraceBlob: time.Hour, GraceBlobSet: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.OrphanBlobsConsidered != 1 {
		t.Errorf("OrphanBlobsConsidered: got %d want 1", rep.OrphanBlobsConsidered)
	}
	if rep.OrphansDeleted != 0 {
		t.Errorf("OrphansDeleted (dry-run): got %d want 0", rep.OrphansDeleted)
	}
	// Blob still exists (not deleted in dry-run).
	if _, _, exists, _ := f.StatFile(ctx, cas.BlobPath(cp, hOrphan)); !exists {
		t.Error("dry-run must NOT delete blobs")
	}
	// No marker written in dry-run.
	if _, _, exists, _ := f.StatFile(ctx, cas.PruneMarkerPath(cp)); exists {
		t.Error("dry-run must NOT write prune.marker")
	}
}

func TestPrune_Unlock(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()
	cp := cfg.ClusterPrefix()

	if _, _, err := cas.WritePruneMarker(ctx, f, cp, "host-stuck"); err != nil {
		t.Fatal(err)
	}

	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{Unlock: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep == nil {
		t.Fatal("expected non-nil report")
	}
	if _, _, exists, _ := f.StatFile(ctx, cas.PruneMarkerPath(cp)); exists {
		t.Error("--unlock should delete the prune marker")
	}
}

func TestPrune_UnlockRefusesIfNoMarker(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	_, err := cas.Prune(context.Background(), f, cfg, cas.PruneOptions{Unlock: true})
	if err == nil || !strings.Contains(err.Error(), "no prune.marker present") {
		t.Fatalf("want no-marker error, got %v", err)
	}
}

func TestPrune_DryRunUnlockKeepsMarker(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()

	runID, created, err := cas.WritePruneMarker(ctx, f, cfg.ClusterPrefix(), "host-other")
	if err != nil || !created {
		t.Fatalf("WritePruneMarker setup: created=%v err=%v", created, err)
	}
	_ = runID

	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{Unlock: true, DryRun: true})
	if err != nil {
		t.Fatalf("Prune --dry-run --unlock returned error: %v", err)
	}
	if rep == nil || !rep.DryRun {
		t.Errorf("expected DryRun=true in report; got %+v", rep)
	}

	// The marker must still exist.
	_, _, exists, _ := f.StatFile(ctx, cas.PruneMarkerPath(cfg.ClusterPrefix()))
	if !exists {
		t.Error("prune marker was deleted by --dry-run --unlock; expected it to survive")
	}
}

func TestPrune_MetadataOrphanSubtreeSwept(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()
	cp := cfg.ClusterPrefix()

	// Hand-craft a metadata orphan: per-table JSON without metadata.json.
	body := []byte(`{"database":"db","table":"t"}`)
	if err := f.PutFile(ctx, cas.TableMetaPath(cp, "halfdeleted", "db", "t"),
		io.NopCloser(bytes.NewReader(body)), int64(len(body))); err != nil {
		t.Fatal(err)
	}

	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{GraceBlob: time.Hour, GraceBlobSet: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.MetadataOrphansFound != 1 {
		t.Errorf("MetadataOrphansFound: got %d want 1", rep.MetadataOrphansFound)
	}
	// Subtree gone.
	if _, _, exists, _ := f.StatFile(ctx, cas.TableMetaPath(cp, "halfdeleted", "db", "t")); exists {
		t.Error("metadata-orphan per-table JSON should be deleted")
	}
}

// TestPrune_ReportCountersPopulated verifies that BlobsTotal and
// OrphansHeldByGrace are correctly populated in the PruneReport.
// It constructs a fake backend with:
//   - 1 live-referenced blob (hLive)
//   - 1 stale orphan older than grace (hStaleOrphan) — will be deleted
//   - 1 fresh orphan within grace (hFreshOrphan) — held by grace
func TestPrune_ReportCountersPopulated(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()
	cp := cfg.ClusterPrefix()

	hLive := cas.Hash128{Low: 0xA1, High: 0xA1}
	hStaleOrphan := cas.Hash128{Low: 0xB2, High: 0xB2}
	hFreshOrphan := cas.Hash128{Low: 0xC3, High: 0xC3}

	// Upload a backup that references hLive.
	uploadTestBackup(t, f, cfg, "bk-live", hLive)

	// Manually place stale and fresh orphan blobs.
	for _, h := range []cas.Hash128{hStaleOrphan, hFreshOrphan} {
		if err := f.PutFile(ctx, cas.BlobPath(cp, h), io.NopCloser(bytes.NewReader([]byte("x"))), 1); err != nil {
			t.Fatal(err)
		}
	}

	// Age the live blob and stale orphan past grace; fresh orphan stays inside.
	ageBlob(t, f, cfg, hLive, 2*time.Hour)
	ageBlob(t, f, cfg, hStaleOrphan, 2*time.Hour)
	ageBlob(t, f, cfg, hFreshOrphan, 30*time.Minute)

	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{GraceBlob: time.Hour, GraceBlobSet: true})
	if err != nil {
		t.Fatal(err)
	}

	// 3 blobs total: hLive + hStaleOrphan + hFreshOrphan.
	if rep.BlobsTotal != 3 {
		t.Errorf("BlobsTotal: got %d want 3", rep.BlobsTotal)
	}
	// hFreshOrphan is an orphan but within grace → held.
	if rep.OrphansHeldByGrace != 1 {
		t.Errorf("OrphansHeldByGrace: got %d want 1", rep.OrphansHeldByGrace)
	}
	// hStaleOrphan should be deleted.
	if rep.OrphansDeleted != 1 {
		t.Errorf("OrphansDeleted: got %d want 1", rep.OrphansDeleted)
	}
}

func TestPrune_RejectsInvalidConfig(t *testing.T) {
	ctx := context.Background()
	b := fakedst.New()
	// Enabled=true but no ClusterID → Validate must reject it.
	cfg := cas.Config{Enabled: true}
	_, err := cas.Prune(ctx, b, cfg, cas.PruneOptions{})
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "cluster_id")
}

func TestPrune_RefusesWhenDisabled(t *testing.T) {
	cfg := testCfg(1024)
	cfg.Enabled = false
	_, err := cas.Prune(context.Background(), fakedst.New(), cfg, cas.PruneOptions{})
	if err == nil || !strings.Contains(err.Error(), "cas.enabled=false") {
		t.Fatalf("want cas.enabled=false error, got %v", err)
	}
}

// TestPrune_ZeroModTimeMarkerIsFresh verifies that a marker with a
// zero ModTime (e.g. FTP LIST without MLSD facts) is classified as
// fresh, not abandoned. The conservative choice avoids the data-loss
// path where prune sweeps a real in-progress upload.
func TestPrune_ZeroModTimeMarkerIsFresh(t *testing.T) {
	f := fakedst.New()
	cp := testCfg(1024).ClusterPrefix()
	ctx := context.Background()

	// Place a marker with zero ModTime via the fake's hook.
	if _, err := cas.WriteInProgressMarker(ctx, f, cp, "bk_zero", "host"); err != nil {
		t.Fatal(err)
	}
	f.SetModTime(cas.InProgressMarkerPath(cp, "bk_zero"), time.Time{})

	// Use a very small abandon threshold so a non-zero-ModTime marker
	// would otherwise classify as abandoned.
	rep, err := cas.Prune(ctx, f, testCfg(1024), cas.PruneOptions{
		AbandonThreshold:    time.Nanosecond,
		AbandonThresholdSet: true,
	})
	// The marker is fresh → Prune should refuse with the freshness error.
	if err == nil {
		t.Fatalf("expected Prune to refuse for fresh marker; rep=%+v", rep)
	}
	if !strings.Contains(err.Error(), "are fresh") {
		t.Errorf("expected 'are fresh' in error; got: %v", err)
	}
}

// TestPrune_RefusesIfAnotherPruneRunning verifies that a second cas-prune
// run refuses cleanly when another prune is in flight, AND that the
// existing marker is not deleted by the failing run's deferred cleanup.
// The latter assertion is the regression guard for the original
// "deferred-delete races second prune" bug.
func TestPrune_RefusesIfAnotherPruneRunning(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	ctx := context.Background()

	// Pre-write a prune marker simulating another prune in flight.
	runID, created, err := cas.WritePruneMarker(ctx, f, cfg.ClusterPrefix(), "host-other")
	if err != nil || !created {
		t.Fatalf("WritePruneMarker setup: created=%v err=%v", created, err)
	}
	_ = runID

	_, err = cas.Prune(ctx, f, cfg, cas.PruneOptions{})
	if err == nil {
		t.Fatal("expected Prune to refuse when marker is already held")
	}
	if !strings.Contains(err.Error(), "another prune is in progress") {
		t.Errorf("error should mention concurrent prune; got: %v", err)
	}

	// Critical: the existing marker must NOT have been deleted by the
	// failing prune's defer. Without the scoped-defer fix it would be.
	if _, _, exists, _ := f.StatFile(ctx, cas.PruneMarkerPath(cfg.ClusterPrefix())); !exists {
		t.Error("prune marker should survive a refused second prune")
	}
}

// TestPrune_ExplicitZeroOverridesConfigGrace verifies that passing
// GraceBlobSet=true with GraceBlob=0 bypasses the non-zero cfg.GraceBlob
// (24h in testCfg) and immediately prunes a freshly-created orphan blob.
func TestPrune_ExplicitZeroOverridesConfigGrace(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024) // GraceBlob is "24h" after Validate()
	ctx := context.Background()
	cp := cfg.ClusterPrefix()

	// Place a fresh orphan blob (not referenced by any backup, modtime = now).
	hFreshOrphan := cas.Hash128{Low: 0xDE, High: 0xAD}
	if err := f.PutFile(ctx, cas.BlobPath(cp, hFreshOrphan), io.NopCloser(bytes.NewReader([]byte("x"))), 1); err != nil {
		t.Fatal(err)
	}
	// modtime stays at "now" — within the 24h config grace, so a normal run
	// would hold it. With explicit --grace-blob=0s it must be swept.

	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{
		GraceBlob:    0,
		GraceBlobSet: true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), rep.OrphansHeldByGrace, "explicit zero must override 24h config grace")
	require.Equal(t, uint64(1), rep.OrphansDeleted, "fresh orphan must be deleted with grace=0")

	// Double-check the blob is actually gone.
	if _, _, exists, _ := f.StatFile(ctx, cas.BlobPath(cp, hFreshOrphan)); exists {
		t.Error("fresh orphan must be deleted when --grace-blob=0s overrides 24h config")
	}
}

// TestPrune_ExplicitZeroOverridesConfigAbandon verifies that passing
// AbandonThresholdSet=true with AbandonThreshold=0 bypasses the non-zero
// cfg.AbandonThreshold (168h in testCfg) and treats every in-progress marker
// as abandoned — allowing prune to proceed and sweep it.
func TestPrune_ExplicitZeroOverridesConfigAbandon(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024) // AbandonThreshold is "168h" after Validate()
	ctx := context.Background()
	cp := cfg.ClusterPrefix()

	// Write a fresh in-progress marker (modtime = now). Under the 168h config
	// threshold it would block prune. With explicit --abandon-threshold=0s every
	// marker has age >= 0 == threshold and is classified as abandoned.
	if _, err := cas.WriteInProgressMarker(ctx, f, cp, "bk_fresh_but_dead", "host-a"); err != nil {
		t.Fatal(err)
	}

	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{
		AbandonThreshold:    0,
		AbandonThresholdSet: true,
	})
	require.NoError(t, err, "explicit zero abandon-threshold must not block on fresh in-progress marker")
	require.Equal(t, 1, rep.AbandonedMarkersFound, "fresh marker must be swept with abandon-threshold=0")

	// The marker must be gone.
	if _, _, exists, _ := f.StatFile(ctx, cas.InProgressMarkerPath(cp, "bk_fresh_but_dead")); exists {
		t.Error("in-progress marker must be deleted when --abandon-threshold=0s overrides 168h config")
	}
}

func TestPrintPruneReport_FormatsBytes(t *testing.T) {
	var buf bytes.Buffer
	err := cas.PrintPruneReport(&cas.PruneReport{BytesReclaimed: 1572864}, &buf)
	require.NoError(t, err)
	out := buf.String()
	// 1572864 bytes = 1.5 MiB; assert FormatBytes-style rendering is present
	require.Contains(t, out, utils.FormatBytes(1572864))
	require.Contains(t, out, "(1572864)")
}
