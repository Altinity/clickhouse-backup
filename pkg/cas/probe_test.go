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
)

// TestProbeConditionalPut_HonoredBackend runs the probe against the in-memory
// fake, which correctly enforces the precondition. Expects nil error.
func TestProbeConditionalPut_HonoredBackend(t *testing.T) {
	f := fakedst.New()
	err := cas.ProbeConditionalPut(context.Background(), f, "cas/test-cluster/")
	if err != nil {
		t.Fatalf("expected nil on honoring backend, got: %v", err)
	}
	// Sentinel must be cleaned up after a successful probe.
	_, _, exists, _ := f.StatFile(context.Background(), "cas/test-cluster/"+cas.ProbeKey)
	if exists {
		t.Error("probe did not clean up sentinel on success")
	}
}

// TestProbeConditionalPut_SilentlyOverwritingBackend uses a stub whose
// PutFileIfAbsent always returns created=true, simulating a backend that
// ignores If-None-Match. Expects ErrConditionalPutNotHonored.
func TestProbeConditionalPut_SilentlyOverwritingBackend(t *testing.T) {
	b := &alwaysCreatesBackend{}
	err := cas.ProbeConditionalPut(context.Background(), b, "cas/test-cluster/")
	if err == nil {
		t.Fatal("expected error on silently-overwriting backend, got nil")
	}
	if !errors.Is(err, cas.ErrConditionalPutNotHonored) {
		t.Errorf("expected ErrConditionalPutNotHonored, got: %v", err)
	}
}

// TestProbeConditionalPut_ErrorOnFirstWrite verifies that an error from the
// first PutFileIfAbsent is surfaced with context "first write".
func TestProbeConditionalPut_ErrorOnFirstWrite(t *testing.T) {
	sentinel := errors.New("backend unavailable")
	b := &errOnPutBackend{err: sentinel}
	err := cas.ProbeConditionalPut(context.Background(), b, "cas/test-cluster/")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "first write") {
		t.Errorf("expected 'first write' in error, got: %v", err)
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error in chain, got: %v", err)
	}
}

// TestProbeConditionalPut_StaleSentinelCleanedAndRetried pre-places a
// sentinel via PutFile (bypassing the conditional path) and then runs the
// probe. The probe should delete the stale sentinel, re-write, and succeed.
func TestProbeConditionalPut_StaleSentinelCleanedAndRetried(t *testing.T) {
	f := fakedst.New()
	ctx := context.Background()
	// Pre-seed a stale sentinel so the first PutFileIfAbsent sees it already
	// present and returns created=false.
	_ = f.PutFile(ctx, "cas/test-cluster/"+cas.ProbeKey, io.NopCloser(bytes.NewReader([]byte("stale"))), 5)

	err := cas.ProbeConditionalPut(ctx, f, "cas/test-cluster/")
	if err != nil {
		t.Fatalf("expected nil after stale-sentinel cleanup path, got: %v", err)
	}
	// Sentinel must be cleaned up.
	_, _, exists, _ := f.StatFile(ctx, "cas/test-cluster/"+cas.ProbeKey)
	if exists {
		t.Error("probe did not clean up sentinel after stale-path success")
	}
}

// --- stubs ---

// alwaysCreatesBackend is a cas.Backend stub whose PutFileIfAbsent always
// reports created=true, simulating a backend that silently ignores If-None-Match.
type alwaysCreatesBackend struct{}

func (a *alwaysCreatesBackend) PutFileIfAbsent(_ context.Context, _ string, r io.ReadCloser, _ int64) (bool, error) {
	_ = r.Close()
	return true, nil
}
func (a *alwaysCreatesBackend) PutFile(_ context.Context, _ string, r io.ReadCloser, _ int64) error {
	_ = r.Close()
	return nil
}
func (a *alwaysCreatesBackend) GetFile(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}
func (a *alwaysCreatesBackend) StatFile(_ context.Context, _ string) (int64, time.Time, bool, error) {
	return 0, time.Time{}, false, nil
}
func (a *alwaysCreatesBackend) DeleteFile(_ context.Context, _ string) error { return nil }
func (a *alwaysCreatesBackend) Walk(_ context.Context, _ string, _ bool, _ func(cas.RemoteFile) error) error {
	return nil
}

// errOnPutBackend is a cas.Backend stub that returns an error from PutFileIfAbsent.
type errOnPutBackend struct{ err error }

func (e *errOnPutBackend) PutFileIfAbsent(_ context.Context, _ string, r io.ReadCloser, _ int64) (bool, error) {
	_ = r.Close()
	return false, e.err
}
func (e *errOnPutBackend) PutFile(_ context.Context, _ string, r io.ReadCloser, _ int64) error {
	_ = r.Close()
	return nil
}
func (e *errOnPutBackend) GetFile(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}
func (e *errOnPutBackend) StatFile(_ context.Context, _ string) (int64, time.Time, bool, error) {
	return 0, time.Time{}, false, nil
}
func (e *errOnPutBackend) DeleteFile(_ context.Context, _ string) error { return nil }
func (e *errOnPutBackend) Walk(_ context.Context, _ string, _ bool, _ func(cas.RemoteFile) error) error {
	return nil
}
