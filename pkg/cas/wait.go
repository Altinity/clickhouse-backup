package cas

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// pollIntervalForTesting overrides the production poll cadence in tests.
// Production: nil → defaultPollInterval (2 seconds).
var pollIntervalForTesting *time.Duration

const (
	defaultPollInterval = 2 * time.Second
	waitProgressLog     = 30 * time.Second
)

// waitForPrune polls the prune marker until it disappears, ctx is cancelled,
// or wait elapses. Returns nil to proceed; returns an ErrPruneInProgress-wrapping
// error on timeout; returns ctx.Err() on cancellation.
//
// wait == 0 means "no wait" — match the historical immediate-refusal semantics.
func waitForPrune(ctx context.Context, b Backend, clusterPrefix string, wait time.Duration) error {
	poll := defaultPollInterval
	if pollIntervalForTesting != nil {
		poll = *pollIntervalForTesting
	}
	deadline := time.Now().Add(wait)
	var firstMarker *PruneMarker
	var loggedFirst bool
	var lastLog time.Time

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, _, exists, err := b.StatFile(ctx, PruneMarkerPath(clusterPrefix))
		if err != nil {
			return fmt.Errorf("cas: stat prune marker while waiting: %w", err)
		}
		if !exists {
			return nil
		}

		// First time we see the marker, read its body for diagnostics.
		if !loggedFirst {
			firstMarker, _ = ReadPruneMarker(ctx, b, clusterPrefix) // best-effort; nil-tolerant below
			loggedFirst = true
		}

		if wait == 0 || !time.Now().Before(deadline) {
			return formatWaitTimeout(firstMarker, wait)
		}

		// Periodic INFO log.
		if time.Since(lastLog) >= waitProgressLog {
			logWaitProgress(firstMarker, time.Until(deadline), wait)
			lastLog = time.Now()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}

func formatWaitTimeout(m *PruneMarker, wait time.Duration) error {
	if m == nil {
		return fmt.Errorf("%w: prune still in progress after %s wait; refusing",
			ErrPruneInProgress, wait)
	}
	return fmt.Errorf(
		"%w: prune still in progress after %s wait (held by host=%s, run_id=%s, started=%s); refusing. "+
			"Increase cas.wait_for_prune or run cas-prune --unlock if confident the prune is dead",
		ErrPruneInProgress, wait, m.Host, m.RunID, m.StartedAt)
}

func logWaitProgress(m *PruneMarker, remaining, total time.Duration) {
	waited := total - remaining
	if m == nil {
		log.Info().Msgf("cas: waiting for prune to finish (waited=%s/%s)",
			waited.Round(time.Second), total)
		return
	}
	log.Info().Msgf("cas: waiting for prune to finish (held by host=%s since=%s, run_id=%s, waited=%s/%s)",
		m.Host, m.StartedAt, m.RunID, waited.Round(time.Second), total)
}
