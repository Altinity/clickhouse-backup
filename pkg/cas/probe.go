package cas

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrConditionalPutNotHonored is returned when a backend's PutFileIfAbsent
// silently overwrites instead of refusing on second write — defeating CAS
// marker locks.
var ErrConditionalPutNotHonored = errors.New("cas: backend silently ignored conditional put — marker locks unsafe")

const probeKeyPrefix = "cas-conditional-put-probe-"

// probeKeyRandom returns a 16-character hex string (64 bits of entropy) for
// use as the unique per-call suffix of the probe key. 64 bits makes random
// collision between concurrent probes astronomically unlikely.
func probeKeyRandom() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read only fails on catastrophic OS failures; fall back to
		// a time-based key rather than panicking so the probe still works.
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// ProbeConditionalPut writes <clusterPrefix>/cas-conditional-put-probe-<random>
// twice via PutFileIfAbsent. Returns nil iff the backend correctly honored the
// precondition (first created=true, second created=false). Cleans up the
// sentinel on completion.
//
// A unique random suffix is used per invocation so that two concurrent probes
// (e.g. cas-upload and cas-prune starting simultaneously) operate on different
// keys and cannot interfere with each other.
func ProbeConditionalPut(ctx context.Context, b Backend, clusterPrefix string) error {
	key := clusterPrefix + probeKeyPrefix + probeKeyRandom()
	body1 := []byte("probe-1")
	body2 := []byte("probe-2")

	// First write: try to establish the sentinel.
	created1, err := b.PutFileIfAbsent(ctx, key, io.NopCloser(bytes.NewReader(body1)), int64(len(body1)))
	if errors.Is(err, ErrConditionalPutNotSupported) {
		// Backend correctly reports it doesn't support conditional create.
		// Skip the probe — the upload/prune marker-write will refuse naturally
		// with the existing operator-facing diagnostic ("backend cannot guarantee
		// atomic markers..."), preserving the original UX. The probe is for
		// detecting backends that LIE about supporting conditional-create, not
		// for re-doing what the marker-write layer already does correctly.
		return nil
	}
	if err != nil {
		return fmt.Errorf("cas conditional-put probe: first write: %w", err)
	}
	if !created1 {
		// The key already exists. Since it is unique and random, this indicates
		// either an astronomically unlikely random collision or a backend bug
		// (e.g. the backend is not respecting the conditional-create semantics
		// at all and returned created=false for a key we just generated).
		return fmt.Errorf("cas conditional-put probe: unexpected: random key %q already exists; possible random-collision or backend bug", key)
	}

	// Second write: must report not-created if backend honors the precondition.
	created2, err := b.PutFileIfAbsent(ctx, key, io.NopCloser(bytes.NewReader(body2)), int64(len(body2)))
	// Best-effort cleanup; don't mask the probe result.
	_ = b.DeleteFile(ctx, key)
	if err != nil {
		return fmt.Errorf("cas conditional-put probe: second write: %w", err)
	}
	if created2 {
		return fmt.Errorf("%w: backend silently overwrote sentinel (update MinIO to >=2024-11 or use a backend with native conditional create; set cas.skip_conditional_put_probe=true to override at your own risk)",
			ErrConditionalPutNotHonored)
	}
	return nil
}
