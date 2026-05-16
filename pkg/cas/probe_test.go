package cas_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
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
	// Sentinel must be cleaned up after a successful probe. Walk the prefix
	// and confirm no probe keys remain.
	ctx := context.Background()
	var found []string
	_ = f.Walk(ctx, "cas/test-cluster/"+cas.ProbeKeyPrefix, true, func(rf cas.RemoteFile) error {
		found = append(found, rf.Key)
		return nil
	})
	if len(found) != 0 {
		t.Errorf("probe did not clean up sentinel on success; leftover keys: %v", found)
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

// TestProbeConditionalPut_RejectsExistingProbeKey verifies that if the first
// PutFileIfAbsent returns created=false (as if the random key already exists),
// the probe returns an error mentioning "random-collision or backend bug".
func TestProbeConditionalPut_RejectsExistingProbeKey(t *testing.T) {
	// firstCall tracks whether this is the first PutFileIfAbsent invocation.
	b := &firstCallReturnsFalseBackend{}
	err := cas.ProbeConditionalPut(context.Background(), b, "cas/test-cluster/")
	if err == nil {
		t.Fatal("expected error when first PutFileIfAbsent returns created=false, got nil")
	}
	if !strings.Contains(err.Error(), "random-collision or backend bug") {
		t.Errorf("expected 'random-collision or backend bug' in error, got: %v", err)
	}
}

// TestProbeConditionalPut_SkipsWhenBackendReturnsNotSupported verifies that the
// probe returns nil (gracefully skipped) when the backend's PutFileIfAbsent
// returns ErrConditionalPutNotSupported on the first write. This preserves the
// original UX where the marker-write layer produces the operator-facing
// "backend cannot guarantee atomic markers" diagnostic instead of a probe error.
func TestProbeConditionalPut_SkipsWhenBackendReturnsNotSupported(t *testing.T) {
	b := &notSupportedBackend{}
	err := cas.ProbeConditionalPut(context.Background(), b, "cas/test-cluster/")
	if err != nil {
		t.Errorf("expected nil (probe gracefully skipped), got: %v", err)
	}
}

// TestProbeConditionalPut_TwoConcurrentProbesDontCollide verifies that two
// concurrent probes against the same backend don't interfere with each other.
// Because each probe picks a unique random key, both should succeed without
// either one deleting the other's sentinel.
func TestProbeConditionalPut_TwoConcurrentProbesDontCollide(t *testing.T) {
	f := fakedst.New()
	ctx := context.Background()
	const clusterPrefix = "cas/test-cluster/"

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = cas.ProbeConditionalPut(ctx, f, clusterPrefix)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("probe %d failed: %v", i, err)
		}
	}

	// After both probes complete, no probe sentinels should remain.
	var found []string
	_ = f.Walk(ctx, clusterPrefix+cas.ProbeKeyPrefix, true, func(rf cas.RemoteFile) error {
		found = append(found, rf.Key)
		return nil
	})
	if len(found) != 0 {
		t.Errorf("probes left behind sentinel keys: %v", found)
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

// notSupportedBackend is a cas.Backend stub whose PutFileIfAbsent returns
// (false, ErrConditionalPutNotSupported), simulating FTP and similar backends
// that correctly advertise they don't support conditional create.
type notSupportedBackend struct{}

func (n *notSupportedBackend) PutFileIfAbsent(_ context.Context, _ string, r io.ReadCloser, _ int64) (bool, error) {
	_ = r.Close()
	return false, cas.ErrConditionalPutNotSupported
}
func (n *notSupportedBackend) PutFile(_ context.Context, _ string, r io.ReadCloser, _ int64) error {
	_ = r.Close()
	return nil
}
func (n *notSupportedBackend) GetFile(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}
func (n *notSupportedBackend) StatFile(_ context.Context, _ string) (int64, time.Time, bool, error) {
	return 0, time.Time{}, false, nil
}
func (n *notSupportedBackend) DeleteFile(_ context.Context, _ string) error { return nil }
func (n *notSupportedBackend) Walk(_ context.Context, _ string, _ bool, _ func(cas.RemoteFile) error) error {
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

// firstCallReturnsFalseBackend is a cas.Backend stub whose first
// PutFileIfAbsent call returns (false, nil), simulating a scenario where the
// random probe key happens to already exist (random collision or backend bug).
type firstCallReturnsFalseBackend struct {
	mu    sync.Mutex
	calls int
}

func (b *firstCallReturnsFalseBackend) PutFileIfAbsent(_ context.Context, _ string, r io.ReadCloser, _ int64) (bool, error) {
	_ = r.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if b.calls == 1 {
		return false, nil
	}
	return true, nil
}
func (b *firstCallReturnsFalseBackend) PutFile(_ context.Context, _ string, r io.ReadCloser, _ int64) error {
	_ = r.Close()
	return nil
}
func (b *firstCallReturnsFalseBackend) GetFile(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}
func (b *firstCallReturnsFalseBackend) StatFile(_ context.Context, _ string) (int64, time.Time, bool, error) {
	return 0, time.Time{}, false, nil
}
func (b *firstCallReturnsFalseBackend) DeleteFile(_ context.Context, _ string) error { return nil }
func (b *firstCallReturnsFalseBackend) Walk(_ context.Context, _ string, _ bool, _ func(cas.RemoteFile) error) error {
	return nil
}
