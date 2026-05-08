package cas

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// DeleteOptions configures a Delete run.
type DeleteOptions struct {
	// WaitForPrune, when > 0, polls the prune marker for up to this duration
	// before giving up at delete step 1. 0 = refuse immediately (default).
	WaitForPrune time.Duration
}

// Delete removes a CAS backup's metadata subtree. Blob reclamation is
// reserved for Phase 2 (cas-prune); in Phase 1, deleted-backup blobs
// remain in remote storage indefinitely. Per §6.6, metadata.json is
// deleted FIRST so the backup leaves the catalog atomically; even if
// the rest of the subtree removal is interrupted, the backup is no
// longer listable, and the orphan per-table JSONs/archives will be
// swept by the future prune (or via manual cleanup, until prune ships).
func Delete(ctx context.Context, b Backend, cfg Config, name string, opts DeleteOptions) error {
	if err := validateName(name); err != nil {
		return err
	}
	cp := cfg.ClusterPrefix()

	// Step 1: refuse if prune in progress (with optional wait).
	if err := waitForPrune(ctx, b, cp, opts.WaitForPrune); err != nil {
		return err
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
		// A marker exists alongside committed metadata.json. If the marker was
		// written by another cas-delete (Tool=="cas-delete"), that delete is
		// actively removing this backup — refuse to race it. Otherwise it is a
		// stale upload marker (upload committed but failed to clean up); proceed
		// with a warning.
		existing, readErr := ReadInProgressMarker(ctx, b, cp, name)
		if readErr == nil && existing.Tool == "cas-delete" {
			return fmt.Errorf("cas-delete: another %s is in progress for %q on host=%s started=%s; wait for it to finish",
				existing.Tool, name, existing.Host, existing.StartedAt)
		}
		log.Warn().Str("backup", name).Msg("cas-delete: stale inprogress marker present alongside committed metadata.json; proceeding")
	case !ipOK && !mdOK:
		// If a v1 backup exists at the root with this name, surface the
		// proper cross-mode refusal. Operators who type a v1 backup name
		// into cas-delete get the helpful error.
		if _, _, exists, err := b.StatFile(ctx, name+"/metadata.json"); err == nil && exists {
			return ErrV1Backup
		}
		return fmt.Errorf("cas: backup %q not found", name)
	}
	// (the !ipOK && mdOK case is the normal path; fall through)

	// Step 3: Write a cas-delete inprogress marker BEFORE touching metadata.json.
	// This closes the race window where a concurrent cas-upload on another host sees
	// no metadata.json (we deleted it in step 4) and no marker, treats the name as
	// free, and starts uploading — only to have its just-written archives swept by
	// our walkAndDeleteSubtree.  cas-upload's step-5 same-name check refuses when
	// ANY inprogress marker exists (regardless of Tool), so this marker is sufficient
	// to block it until we finish.
	//
	// When ipOK is true there is already a stale upload marker present; we skip
	// writing our own (PutFileIfAbsent would return created=false anyway) and
	// instead clean it up as we did before.
	if !ipOK {
		created, werr := WriteInProgressMarkerWithTool(ctx, b, cp, name, "", "cas-delete")
		if werr != nil {
			if errors.Is(werr, ErrConditionalPutNotSupported) {
				return fmt.Errorf("cas-delete: backend cannot guarantee atomic markers; refusing")
			}
			return fmt.Errorf("cas-delete: write delete marker: %w", werr)
		}
		if !created {
			// Another operation (upload or delete) raced us and wrote the marker first.
			existing, readErr := ReadInProgressMarker(ctx, b, cp, name)
			if readErr != nil {
				return fmt.Errorf("cas-delete: another operation is in progress for %q (could not read marker: %v)", name, readErr)
			}
			return fmt.Errorf("cas-delete: another %s is in progress for %q on host=%s started=%s; wait for it to finish",
				existing.Tool, name, existing.Host, existing.StartedAt)
		}
		defer func() {
			if delErr := b.DeleteFile(ctx, InProgressMarkerPath(cp, name)); delErr != nil {
				log.Warn().Err(delErr).Str("backup", name).Msg("cas-delete: release inprogress marker")
			}
		}()
	}

	// Step 4: delete metadata.json FIRST so the backup leaves the catalog atomically.
	if err := b.DeleteFile(ctx, MetadataJSONPath(cp, name)); err != nil {
		return fmt.Errorf("cas-delete: delete metadata.json: %w", err)
	}

	// Step 5: delete the rest of the subtree
	if err := walkAndDeleteSubtree(ctx, b, MetadataDir(cp, name)); err != nil {
		return fmt.Errorf("cas-delete: cleanup subtree: %w", err)
	}

	// Step 6: best-effort cleanup of the stale upload inprogress marker (ipOK path).
	// Our own delete marker is released by the defer above.
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
