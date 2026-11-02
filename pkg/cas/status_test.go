package cas_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/testfixtures"
	"github.com/stretchr/testify/require"
)

func TestStatus_EmptyBucket(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	r, err := cas.Status(context.Background(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.BackupCount != 0 || r.BlobCount != 0 {
		t.Errorf("expected empty report, got %+v", r)
	}
	if r.PruneMarker != nil {
		t.Error("expected no prune marker")
	}
	if len(r.InProgressFresh) != 0 || len(r.InProgressAbandoned) != 0 {
		t.Error("expected no in-progress markers")
	}
}

func TestStatus_AfterUploads(t *testing.T) {
	// Build two local backups with distinct blobs and upload them.
	// smallPart uses data.bin (1024 bytes) which exceeds threshold=100 → 1 blob per backup.
	// Both backups share no blobs (different hashLow values), so BlobCount = 2.
	ctx := context.Background()
	f := fakedst.New()
	cfg := testCfg(100)

	lb1 := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 0)})
	if _, err := cas.Upload(ctx, f, cfg, "bk_a", cas.UploadOptions{LocalBackupDir: lb1.Root}); err != nil {
		t.Fatalf("Upload bk_a: %v", err)
	}

	lb2 := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 1000)})
	if _, err := cas.Upload(ctx, f, cfg, "bk_b", cas.UploadOptions{LocalBackupDir: lb2.Root}); err != nil {
		t.Fatalf("Upload bk_b: %v", err)
	}

	r, err := cas.Status(ctx, f, cfg)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if r.BackupCount != 2 {
		t.Errorf("BackupCount: got %d want 2", r.BackupCount)
	}
	// Each upload contributes 1 blob (data.bin, 1024 bytes, distinct hashes).
	if r.BlobCount != 2 {
		t.Errorf("BlobCount: got %d want 2", r.BlobCount)
	}
	if r.BlobBytes <= 0 {
		t.Errorf("BlobBytes: got %d want >0", r.BlobBytes)
	}
	// Backups should be sorted newest-first; both present.
	names := make(map[string]bool)
	for _, bs := range r.Backups {
		names[bs.Name] = true
	}
	if !names["bk_a"] || !names["bk_b"] {
		t.Errorf("Backups: got %v want bk_a and bk_b", r.Backups)
	}
}

func TestStatus_DetectsPruneMarker(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	ctx := context.Background()
	if _, _, err := cas.WritePruneMarker(ctx, f, cfg.ClusterPrefix(), "h1"); err != nil {
		t.Fatal(err)
	}
	r, err := cas.Status(ctx, f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.PruneMarker == nil {
		t.Fatal("expected PruneMarker, got nil")
	}
	if r.PruneMarker.Path == "" {
		t.Error("PruneMarker.Path empty")
	}
}

func TestStatus_ClassifiesInProgressByAge(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	cfg.AbandonThreshold = "1h"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// fresh marker — just written, age ~ 0
	if _, err := cas.WriteInProgressMarker(ctx, f, cfg.ClusterPrefix(), "bk_recent", "h"); err != nil {
		t.Fatal(err)
	}
	// abandoned marker — write then age it to 2h ago
	if _, err := cas.WriteInProgressMarker(ctx, f, cfg.ClusterPrefix(), "bk_old", "h"); err != nil {
		t.Fatal(err)
	}
	f.SetModTime(cas.InProgressMarkerPath(cfg.ClusterPrefix(), "bk_old"), time.Now().Add(-2*time.Hour))

	r, err := cas.Status(ctx, f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.InProgressFresh) != 1 || r.InProgressFresh[0].Backup != "bk_recent" {
		t.Errorf("fresh: %+v", r.InProgressFresh)
	}
	if len(r.InProgressAbandoned) != 1 || r.InProgressAbandoned[0].Backup != "bk_old" {
		t.Errorf("abandoned: %+v", r.InProgressAbandoned)
	}
}

// TestStatusReport_JSONTags verifies that StatusReport and related structs
// marshal to snake_case keys and that Duration fields are exposed as seconds
// (not nanosecond integers) via the age_seconds field.
func TestStatusReport_JSONTags(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	cfg.AbandonThreshold = "1h"
	require.NoError(t, cfg.Validate())
	ctx := context.Background()

	// Write a prune marker.
	if _, _, err := cas.WritePruneMarker(ctx, f, cfg.ClusterPrefix(), "h1"); err != nil {
		t.Fatal(err)
	}
	// Write a fresh in-progress marker.
	if _, err := cas.WriteInProgressMarker(ctx, f, cfg.ClusterPrefix(), "bk_r", "h"); err != nil {
		t.Fatal(err)
	}

	r, err := cas.Status(ctx, f, cfg)
	require.NoError(t, err)

	raw, err := json.Marshal(r)
	require.NoError(t, err)
	s := string(raw)

	// Top-level snake_case keys must be present.
	require.True(t, strings.Contains(s, `"backup_count"`), "missing backup_count: %s", s)
	require.True(t, strings.Contains(s, `"blob_count"`), "missing blob_count: %s", s)
	require.True(t, strings.Contains(s, `"blob_bytes"`), "missing blob_bytes: %s", s)
	require.True(t, strings.Contains(s, `"in_progress_fresh"`), "missing in_progress_fresh: %s", s)
	require.True(t, strings.Contains(s, `"in_progress_abandoned"`), "missing in_progress_abandoned: %s", s)
	require.True(t, strings.Contains(s, `"backups"`), "missing backups: %s", s)

	// PruneMarker fields.
	require.True(t, strings.Contains(s, `"prune_marker"`), "missing prune_marker: %s", s)
	require.True(t, strings.Contains(s, `"age_seconds"`), "missing age_seconds in prune_marker: %s", s)

	// Age (time.Duration) must NOT appear as nanosecond integer — the field is tagged json:"-".
	require.False(t, strings.Contains(s, `"Age"`), "raw Go field name Age must not appear: %s", s)
	require.False(t, strings.Contains(s, `"age":`), "unexported age field must not appear: %s", s)
}
