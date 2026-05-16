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
	ErrBackupExists       = errors.New("cas: backup with this name already exists")
	ErrUploadInProgress   = errors.New("cas: upload in progress for this name")
	ErrPruneInProgress    = errors.New("cas: prune in progress")
	ErrNoInProgressMarker = errors.New("cas: no inprogress marker found for backup")

	// Pre-flight.
	ErrObjectDiskRefused = errors.New("cas: object-disk tables not supported in v1 of CAS")

	// Verify.
	ErrVerifyFailures = errors.New("cas-verify: failures detected")

	// ErrConditionalPutNotSupported is returned by PutFileIfAbsent when the
	// underlying backend cannot perform an atomic conditional write.
	// pkg/cas cannot import pkg/storage (import cycle), so this is a
	// separate sentinel; the casstorage adapter translates
	// storage.ErrConditionalPutNotSupported into this value.
	ErrConditionalPutNotSupported = errors.New("conditional PutFile not supported by this backend")
)
