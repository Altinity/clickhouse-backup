package cas

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
)

// Delete removes a CAS backup's metadata subtree. Blob reclamation is the
// next prune's responsibility. Per §6.6, metadata.json is deleted FIRST so
// the backup leaves the catalog atomically; even if the rest of the subtree
// removal is interrupted, the backup is no longer listable, and the orphan
// per-table JSONs/archives will be swept by the next prune.
func Delete(ctx context.Context, b Backend, cfg Config, name string) error {
	if err := validateName(name); err != nil {
		return err
	}
	cp := cfg.ClusterPrefix()

	// Step 1: refuse if prune in progress
	if _, _, ok, err := b.StatFile(ctx, PruneMarkerPath(cp)); err != nil {
		return fmt.Errorf("cas-delete: stat prune marker: %w", err)
	} else if ok {
		return ErrPruneInProgress
	}

	// Step 2: stale-aware inprogress check
	_, _, mdOK, mdErr := b.StatFile(ctx, MetadataJSONPath(cp, name))
	if mdErr != nil {
		return fmt.Errorf("cas-delete: stat metadata.json: %w", mdErr)
	}
	_, _, ipOK, ipErr := b.StatFile(ctx, InProgressMarkerPath(cp, name))
	if ipErr != nil {
		return fmt.Errorf("cas-delete: stat inprogress marker: %w", ipErr)
	}

	switch {
	case ipOK && !mdOK:
		return ErrUploadInProgress
	case ipOK && mdOK:
		log.Warn().Str("backup", name).Msg("cas-delete: stale inprogress marker present alongside committed metadata.json; proceeding")
	case !ipOK && !mdOK:
		return fmt.Errorf("cas: backup %q not found", name)
	}
	// (the !ipOK && mdOK case is the normal path; fall through)

	// Step 3: delete metadata.json FIRST
	if err := b.DeleteFile(ctx, MetadataJSONPath(cp, name)); err != nil {
		return fmt.Errorf("cas-delete: delete metadata.json: %w", err)
	}

	// Step 4: delete the rest of the subtree
	if err := walkAndDeleteSubtree(ctx, b, MetadataDir(cp, name)); err != nil {
		return fmt.Errorf("cas-delete: cleanup subtree: %w", err)
	}

	// Step 5: best-effort cleanup of stale inprogress marker
	if ipOK {
		if err := b.DeleteFile(ctx, InProgressMarkerPath(cp, name)); err != nil {
			log.Warn().Err(err).Str("backup", name).Msg("cas-delete: failed to delete stale inprogress marker (will be swept by next prune)")
		}
	}
	return nil
}

// walkAndDeleteSubtree lists every object under prefix and deletes each.
// Returns the first error encountered; remaining objects are NOT deleted on
// error (caller decides whether to retry; metadata-orphans are reclaimed by
// the next prune anyway).
func walkAndDeleteSubtree(ctx context.Context, b Backend, prefix string) error {
	var keys []string
	err := b.Walk(ctx, prefix, true, func(rf RemoteFile) error {
		keys = append(keys, rf.Key)
		return nil
	})
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := b.DeleteFile(ctx, k); err != nil {
			return err
		}
	}
	return nil
}
