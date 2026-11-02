package cas

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
)

// UnlockInProgress removes a stranded cas-upload in-progress marker for the
// named backup. It is the operator escape hatch for a backup whose upload was
// interrupted uncleanly (SIGKILL, OOM, network partition) and whose marker
// was not cleaned up by the deferred cleanup in Upload.
//
// Behavior:
//  1. Stat the marker; if absent return a clear error.
//  2. Read the marker body and log Tool / Host / StartedAt for audit trail.
//  3. Delete the marker.
//  4. Return success.
//
// UnlockInProgress does NOT perform any upload. Callers that want to resume
// the upload must run cas-upload <name> separately after unlocking.
//
// Returns ErrNoInProgressMarker if the marker does not exist.
func UnlockInProgress(ctx context.Context, b Backend, cfg Config, name string) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("cas: unlock: invalid config: %w", err)
	}
	if err := validateName(name); err != nil {
		return err
	}
	cp := cfg.ClusterPrefix()
	markerKey := InProgressMarkerPath(cp, name)

	// 1. Check existence.
	_, _, exists, err := b.StatFile(ctx, markerKey)
	if err != nil {
		return fmt.Errorf("cas: unlock: stat marker for %q: %w", name, err)
	}
	if !exists {
		return fmt.Errorf("%w: %q", ErrNoInProgressMarker, name)
	}

	// 2. Read body for audit log (best-effort; don't fail if body is unreadable).
	m, readErr := ReadInProgressMarker(ctx, b, cp, name)
	if readErr != nil {
		log.Warn().Str("backup", name).Err(readErr).Msg("cas: unlock: could not read marker body for audit; deleting anyway")
	} else {
		log.Info().
			Str("backup", name).
			Str("marker_tool", m.Tool).
			Str("marker_host", m.Host).
			Str("marker_started_at", m.StartedAt).
			Msg("cas: unlock: removing stranded inprogress marker")
	}

	// 3. Delete the marker.
	if err := b.DeleteFile(ctx, markerKey); err != nil {
		return fmt.Errorf("cas: unlock: delete marker for %q: %w", name, err)
	}

	log.Info().Str("backup", name).Msg("cas: unlock: inprogress marker removed; backup slot is now free")
	return nil
}
