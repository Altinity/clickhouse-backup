package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
)

// ErrConditionalPutNotHonored is returned when a backend's PutFileIfAbsent
// silently overwrites instead of refusing on second write — defeating CAS
// marker locks.
var ErrConditionalPutNotHonored = errors.New("cas: backend silently ignored conditional put — marker locks unsafe")

const probeKey = "cas-conditional-put-probe"

// ProbeConditionalPut writes <clusterPrefix>/cas-conditional-put-probe twice
// via PutFileIfAbsent. Returns nil iff the backend correctly honored the
// precondition (first created=true, second created=false). Cleans up the
// sentinel on completion.
//
// If a stale sentinel exists from a prior interrupted probe, it is deleted
// and the write is retried once. This handles the case where a previous
// process was killed between the first write and the cleanup.
func ProbeConditionalPut(ctx context.Context, b Backend, clusterPrefix string) error {
	key := clusterPrefix + probeKey
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
		// Stale sentinel from a prior probe; clean and retry once.
		if delErr := b.DeleteFile(ctx, key); delErr != nil {
			return fmt.Errorf("cas conditional-put probe: cleanup stale sentinel: %w", delErr)
		}
		created1, err = b.PutFileIfAbsent(ctx, key, io.NopCloser(bytes.NewReader(body1)), int64(len(body1)))
		if err != nil {
			return fmt.Errorf("cas conditional-put probe: first write (retry): %w", err)
		}
		if !created1 {
			return fmt.Errorf("cas conditional-put probe: cannot establish baseline after cleanup")
		}
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
