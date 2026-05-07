package cas

import "errors"

var (
	// Backup classification.
	ErrV1Backup                 = errors.New("cas: refusing to operate on v1 backup")
	ErrCASBackup                = errors.New("v1: refusing to operate on CAS backup")
	ErrUnsupportedLayoutVersion = errors.New("cas: unsupported layout version")
	ErrMissingMetadata          = errors.New("cas: backup metadata.json missing")
	ErrClusterIDMismatch        = errors.New("cas: cluster_id mismatch between backup and config")
	ErrInvalidBackupName        = errors.New("cas: invalid backup name")

	// Lifecycle.
	ErrBackupExists     = errors.New("cas: backup with this name already exists")
	ErrUploadInProgress = errors.New("cas: upload in progress for this name")
	ErrPruneInProgress  = errors.New("cas: prune in progress")

	// Pre-flight.
	ErrObjectDiskRefused = errors.New("cas: object-disk tables not supported in v1 of CAS")

	// Verify.
	ErrVerifyFailures = errors.New("cas-verify: failures detected")
)
