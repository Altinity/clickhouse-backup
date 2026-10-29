package cas_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
)

func TestInProgressMarker_RoundTrip(t *testing.T) {
	f := fakedst.New()
	ctx := context.Background()
	if _, err := cas.WriteInProgressMarker(ctx, f, "cas/c1/", "bk1", "host-a"); err != nil {
		t.Fatal(err)
	}
	m, err := cas.ReadInProgressMarker(ctx, f, "cas/c1/", "bk1")
	if err != nil {
		t.Fatal(err)
	}
	if m.Backup != "bk1" {
		t.Errorf("Backup: got %q", m.Backup)
	}
	if m.Host != "host-a" {
		t.Errorf("Host: got %q", m.Host)
	}
	if m.StartedAt == "" {
		t.Error("StartedAt empty")
	}
	if err := cas.DeleteInProgressMarker(ctx, f, "cas/c1/", "bk1"); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.ReadInProgressMarker(ctx, f, "cas/c1/", "bk1"); err == nil {
		t.Fatal("expected error reading deleted marker")
	}
}

func TestInProgressMarker_DefaultsHost(t *testing.T) {
	f := fakedst.New()
	ctx := context.Background()
	if _, err := cas.WriteInProgressMarker(ctx, f, "cas/c1/", "bk", ""); err != nil {
		t.Fatal(err)
	}
	m, err := cas.ReadInProgressMarker(ctx, f, "cas/c1/", "bk")
	if err != nil {
		t.Fatal(err)
	}
	if m.Host == "" {
		t.Error("Host should be filled when caller passes \"\"")
	}
}

func TestPruneMarker_RunIDReadBack(t *testing.T) {
	f := fakedst.New()
	ctx := context.Background()
	runID, created, err := cas.WritePruneMarker(ctx, f, "cas/c1/", "host-a")
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected created=true on first write")
	}
	if len(runID) != 16 {
		t.Errorf("runID len: got %d want 16", len(runID))
	}
	m, err := cas.ReadPruneMarker(ctx, f, "cas/c1/")
	if err != nil {
		t.Fatal(err)
	}
	if m.RunID != runID {
		t.Errorf("read-back: got %q want %q", m.RunID, runID)
	}
	if m.Host != "host-a" {
		t.Errorf("Host: got %q", m.Host)
	}
}

// TestPruneMarker_SecondWriteRefused verifies that WritePruneMarker returns
// created=false when a marker already exists (atomic create semantics).
func TestPruneMarker_SecondWriteRefused(t *testing.T) {
	f := fakedst.New()
	ctx := context.Background()
	a, createdA, err := cas.WritePruneMarker(ctx, f, "cas/c1/", "h")
	if err != nil || !createdA {
		t.Fatalf("first write: created=%v err=%v", createdA, err)
	}
	_, createdB, err := cas.WritePruneMarker(ctx, f, "cas/c1/", "h")
	if err != nil {
		t.Fatal(err)
	}
	if createdB {
		t.Error("second write should return created=false (marker already exists)")
	}
	// The first run's marker must still be intact.
	m, err := cas.ReadPruneMarker(ctx, f, "cas/c1/")
	if err != nil {
		t.Fatal(err)
	}
	if m.RunID != a {
		t.Errorf("marker should still hold first run-id %q; got %q", a, m.RunID)
	}
}

func TestSetMarkerTool(t *testing.T) {
	f := fakedst.New()
	ctx := context.Background()
	cas.SetMarkerTool("test-tool/1.0")
	defer cas.SetMarkerTool("clickhouse-backup")
	_, _, err := cas.WritePruneMarker(ctx, f, "cas/c1/", "h")
	if err != nil {
		t.Fatal(err)
	}
	m, err := cas.ReadPruneMarker(ctx, f, "cas/c1/")
	if err != nil {
		t.Fatal(err)
	}
	if m.Tool != "test-tool/1.0" {
		t.Errorf("Tool: got %q", m.Tool)
	}
}

// TestReadInProgressMarker_LimitsReadSize verifies that ReadInProgressMarker
// does not consume unbounded memory when the remote object is larger than the
// 64 KiB markerSizeLimit. The LimitReader truncates the body; the truncated
// bytes are not valid JSON, so the call must return an error (not a
// successfully-parsed marker, and not an OOM).
func TestReadInProgressMarker_LimitsReadSize(t *testing.T) {
	f := fakedst.New()
	ctx := context.Background()
	const cp = "cas/c1/"
	const name = "big-bk"

	// Pre-place a marker whose body is 128 KiB (2× the 64 KiB limit) of 'x'.
	// The body is not valid JSON; after truncation it remains invalid.
	oversized := make([]byte, 128*1024)
	for i := range oversized {
		oversized[i] = 'x'
	}
	markerKey := cas.InProgressMarkerPath(cp, name)
	if err := f.PutFile(ctx, markerKey,
		io.NopCloser(bytes.NewReader(oversized)), int64(len(oversized))); err != nil {
		t.Fatal(err)
	}

	m, err := cas.ReadInProgressMarker(ctx, f, cp, name)
	if err == nil {
		t.Fatalf("expected an error due to invalid JSON after LimitReader truncation; got marker=%+v", m)
	}
	if m != nil {
		t.Errorf("marker must be nil on error; got %+v", m)
	}
}

// TestReadPruneMarker_LimitsReadSize mirrors TestReadInProgressMarker_LimitsReadSize
// for ReadPruneMarker.
func TestReadPruneMarker_LimitsReadSize(t *testing.T) {
	f := fakedst.New()
	ctx := context.Background()
	const cp = "cas/c1/"

	oversized := make([]byte, 128*1024)
	for i := range oversized {
		oversized[i] = 'x'
	}
	pruneKey := cas.PruneMarkerPath(cp)
	if err := f.PutFile(ctx, pruneKey,
		io.NopCloser(bytes.NewReader(oversized)), int64(len(oversized))); err != nil {
		t.Fatal(err)
	}

	m, err := cas.ReadPruneMarker(ctx, f, cp)
	if err == nil {
		t.Fatalf("expected an error due to invalid JSON after LimitReader truncation; got marker=%+v", m)
	}
	if m != nil {
		t.Errorf("marker must be nil on error; got %+v", m)
	}
}
