package cas

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

// nameRe permits printable ASCII identifiers with conservative punctuation.
// Excludes anything that could be misinterpreted as a path component.
var nameRe = regexp.MustCompile(`^[A-Za-z0-9._\-+:]+$`)

// validateName enforces backup-name rules: 1..128 chars, [A-Za-z0-9._\-+:].
func validateName(name string) error {
	if len(name) == 0 || len(name) > 128 {
		return ErrInvalidBackupName
	}
	if !nameRe.MatchString(name) {
		return ErrInvalidBackupName
	}
	return nil
}

// ValidateBackup loads cas/<cluster>/metadata/<name>/metadata.json, verifies
// it is a CAS backup belonging to this cluster, and that its layout
// parameters are within supported ranges. Returns the parsed metadata so
// callers can use the persisted parameters (InlineThreshold, LayoutVersion)
// for downstream operations.
//
// This is the single precondition function used by every CAS command. See
// docs/cas-design.md §6.2.1 (rationale for persisting + reading layout
// parameters from metadata, not from current config).
func ValidateBackup(ctx context.Context, b Backend, cfg Config, name string) (*metadata.BackupMetadata, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}

	cp := cfg.ClusterPrefix()
	rc, err := b.GetFile(ctx, MetadataJSONPath(cp, name))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMissingMetadata, err)
	}
	defer rc.Close()

	raw, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("cas: read metadata.json: %w", err)
	}

	var bm metadata.BackupMetadata
	if err := json.Unmarshal(raw, &bm); err != nil {
		return nil, fmt.Errorf("cas: parse metadata.json: %w", err)
	}

	if bm.CAS == nil {
		return nil, ErrV1Backup
	}

	if bm.CAS.LayoutVersion > LayoutVersion {
		return nil, fmt.Errorf("%w: backup=%d max-supported=%d", ErrUnsupportedLayoutVersion, bm.CAS.LayoutVersion, LayoutVersion)
	}

	if bm.CAS.InlineThreshold == 0 || bm.CAS.InlineThreshold > MaxInline {
		return nil, fmt.Errorf("cas: persisted inline_threshold out of range: %d", bm.CAS.InlineThreshold)
	}

	if bm.CAS.ClusterID != cfg.ClusterID {
		return nil, fmt.Errorf("%w: backup=%q config=%q", ErrClusterIDMismatch, bm.CAS.ClusterID, cfg.ClusterID)
	}

	return &bm, nil
}
