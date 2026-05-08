package cas

import (
	"context"
	"errors"
)

// V1RestoreFunc is the callback supplied by the CLI binding (Task 19) to
// invoke the existing v1 restore flow on the local directory materialized
// by cas-download. It receives the absolute local backup directory (the
// one returned in DownloadResult.LocalBackupDir) plus the original
// RestoreOptions; the binding extracts whatever subset of fields v1's
// Backuper.Restore needs.
//
// Defining the handoff as a callback keeps pkg/cas free of any dependency
// on pkg/backup (which would create an import cycle: pkg/backup already
// transitively imports pkg/cas via pkg/storage → pkg/config).
type V1RestoreFunc func(ctx context.Context, localBackupDir string, opts RestoreOptions) error

// RestoreOptions extends DownloadOptions with the v1-restore flags that
// the CAS-restore CLI surface mirrors. Only the subset of v1 flags that
// makes sense for CAS backups is exposed; the binding in Task 19 wires
// these into Backuper.Restore positional arguments.
//
// Flags omitted on purpose:
//   - IgnoreDependencies: CAS backups have no dependency chain (each is a
//     standalone snapshot); accepting it would invite confusion. Treated
//     as an error if set.
//   - RestoreRBAC, RBACOnly, RestoreConfigs, ConfigsOnly,
//     RestoreNamedCollections, NamedCollectionsOnly: out of scope for CAS
//     v1, which only handles MergeTree-family table data. Reserved for a
//     future revision.
type RestoreOptions struct {
	DownloadOptions

	// DropExists maps to v1 --rm: drop existing tables before re-creating.
	DropExists bool

	// DataOnly / SchemaOnly are inherited from DownloadOptions and are
	// passed through to v1 in the binding.

	// DatabaseMapping rewrites <src> → <dst> at restore time
	// (--restore-database-mapping).
	DatabaseMapping []string
	// TableMapping rewrites <src> → <dst> at restore time
	// (--restore-table-mapping).
	TableMapping []string
	// SkipProjections suppresses listed projections during data restore
	// (--skip-projections).
	SkipProjections []string

	// RestoreSchemaAsAttach: use ATTACH instead of CREATE for schema
	// (v1 --restore-schema-as-attach).
	RestoreSchemaAsAttach bool
	// ReplicatedCopyToDetached: for Replicated*MergeTree, copy to
	// detached/ and skip the final ATTACH (v1 --replicated-copy-to-detached).
	ReplicatedCopyToDetached bool
	// SkipEmptyTables suppresses errors for tables with no parts
	// (v1 --skip-empty-tables).
	SkipEmptyTables bool

	// Resume enables the resumable-state file (v1 --resume).
	Resume bool

	// BackupVersion is propagated to v1 for log-line consistency.
	BackupVersion string
	// CommandID is the status.Current correlator (v1 --command-id).
	CommandID int

	// IgnoreDependencies is rejected by Restore; declared here so the CLI
	// binding can set it from the cobra flag and have us produce the
	// rejection error in a single place.
	IgnoreDependencies bool
}

// Restore runs cas-download and hands off to runV1, which is expected to
// invoke the existing pkg/backup.Backuper.Restore flow against the local
// directory cas-download just materialized.
//
// Errors:
//   - ErrCASBackup / ErrV1Backup / ErrUnsupportedLayoutVersion etc. from
//     the underlying ValidateBackup + Download.
//   - A descriptive error if --ignore-dependencies is set (CAS backups
//     have no dependency chain).
//   - A descriptive error if --data-only is set (CAS restore doesn't yet
//     support data-only restoration).
//   - Whatever runV1 returns.
func Restore(ctx context.Context, b Backend, cfg Config, name string, opts RestoreOptions, runV1 V1RestoreFunc) error {
	if opts.IgnoreDependencies {
		return errors.New("cas: --ignore-dependencies is not applicable to CAS backups (no dependency chain)")
	}
	if opts.DataOnly {
		return errors.New("cas: --data-only is not yet implemented for cas-restore (use the v1 flow if you need data-only restoration)")
	}
	if runV1 == nil {
		return errors.New("cas: V1RestoreFunc not supplied; CLI binding must wire pkg/backup.Backuper.Restore")
	}
	res, err := Download(ctx, b, cfg, name, opts.DownloadOptions)
	if err != nil {
		return err
	}
	return runV1(ctx, res.LocalBackupDir, opts)
}
