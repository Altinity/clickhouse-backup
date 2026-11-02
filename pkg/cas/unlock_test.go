package cas_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
)

// TestUpload_UnlockRemovesInprogressMarker verifies that UnlockInProgress
// deletes the marker when it exists, and that no upload artifact is written.
func TestUpload_UnlockRemovesInprogressMarker(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	cp := cfg.ClusterPrefix()

	// Pre-place a marker as if a previous cas-upload was interrupted.
	created, err := cas.WriteInProgressMarker(context.Background(), f, cp, "b1", "")
	if err != nil {
		t.Fatalf("WriteInProgressMarker: %v", err)
	}
	if !created {
		t.Fatal("marker should have been created (backend was empty)")
	}
	// Confirm it was written.
	_, _, exists, statErr := f.StatFile(context.Background(), cas.InProgressMarkerPath(cp, "b1"))
	if statErr != nil || !exists {
		t.Fatalf("marker not present before unlock (exists=%v, err=%v)", exists, statErr)
	}

	// Unlock.
	if err := cas.UnlockInProgress(context.Background(), f, cfg, "b1"); err != nil {
		t.Fatalf("UnlockInProgress: %v", err)
	}

	// Marker must be gone.
	_, _, exists2, statErr2 := f.StatFile(context.Background(), cas.InProgressMarkerPath(cp, "b1"))
	if statErr2 != nil {
		t.Fatalf("StatFile after unlock: %v", statErr2)
	}
	if exists2 {
		t.Error("inprogress marker still present after UnlockInProgress")
	}

	// No metadata.json or blob should have been written (no upload happened).
	if f.Len() != 0 {
		t.Errorf("unexpected objects in backend after unlock: got %d, want 0", f.Len())
	}
}

// TestUpload_UnlockRefusesWhenNoMarker verifies that UnlockInProgress returns
// ErrNoInProgressMarker when no marker exists for the named backup.
func TestUpload_UnlockRefusesWhenNoMarker(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)

	err := cas.UnlockInProgress(context.Background(), f, cfg, "b1")
	if err == nil {
		t.Fatal("expected error when no marker present, got nil")
	}
	if !errors.Is(err, cas.ErrNoInProgressMarker) {
		t.Errorf("expected ErrNoInProgressMarker, got: %v", err)
	}
	if !strings.Contains(err.Error(), "b1") {
		t.Errorf("error should mention backup name, got: %v", err)
	}
}
