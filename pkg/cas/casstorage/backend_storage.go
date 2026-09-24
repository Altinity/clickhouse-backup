// Package casstorage wires the CAS Backend interface to pkg/storage.BackupDestination.
// It lives in a sub-package so that pkg/cas itself does not import pkg/storage,
// which would create an import cycle via pkg/storage → pkg/config → pkg/cas.
package casstorage

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
)

// NewStorageBackend adapts a *storage.BackupDestination to the CAS Backend interface.
func NewStorageBackend(bd *storage.BackupDestination) cas.Backend { return &storageBackend{bd: bd} }

type storageBackend struct{ bd *storage.BackupDestination }

func (s *storageBackend) PutFile(ctx context.Context, key string, data io.ReadCloser, size int64) error {
	return s.bd.PutFile(ctx, key, data, size)
}

func (s *storageBackend) GetFile(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.bd.GetFileReader(ctx, key)
}

func (s *storageBackend) StatFile(ctx context.Context, key string) (int64, time.Time, bool, error) {
	rf, err := s.bd.StatFile(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return 0, time.Time{}, false, nil
		}
		return 0, time.Time{}, false, err
	}
	return rf.Size(), rf.LastModified(), true, nil
}

func (s *storageBackend) DeleteFile(ctx context.Context, key string) error {
	return s.bd.DeleteFile(ctx, key)
}

func (s *storageBackend) PutFileIfAbsent(ctx context.Context, key string, data io.ReadCloser, size int64) (bool, error) {
	// PutFileIfAbsent (not PutFileAbsoluteIfAbsent) so that the backend adds
	// its configured path prefix — the same prefix that PutFile, StatFile,
	// DeleteFile and GetFile all prepend. Without this, markers land at a
	// different key than StatFile/DeleteFile look for.
	created, err := s.bd.PutFileIfAbsent(ctx, key, data, size)
	if errors.Is(err, storage.ErrConditionalPutNotSupported) {
		return false, cas.ErrConditionalPutNotSupported
	}
	return created, err
}

func (s *storageBackend) Walk(ctx context.Context, prefix string, recursive bool, fn func(cas.RemoteFile) error) error {
	// pkg/storage backends (S3 in particular, see s3.go S3.Walk) strip the
	// walk-target prefix from rf.Name() — so callers see keys relative to
	// the walk root. CAS code (cas-status, cold-list, list-remote)
	// assumes ABSOLUTE keys (i.e. the same keys it constructed via
	// MetadataJSONPath / BlobPath / etc.), so we reconstruct here by
	// stripping any leading '/' (path.Join artifact in S3.Walk) and
	// re-prepending the requested prefix.
	return s.bd.Walk(ctx, strings.TrimSuffix(prefix, "/")+"/", recursive, func(_ context.Context, rf storage.RemoteFile) error {
		abs := reconstructAbsoluteKey(prefix, rf.Name())
		return fn(cas.RemoteFile{Key: abs, Size: rf.Size(), ModTime: rf.LastModified()})
	})
}

// reconstructAbsoluteKey rebuilds the absolute object key from the prefix
// passed to Walk and the (possibly relative) name returned by the underlying
// pkg/storage backend (which may strip the prefix and may prepend a leading "/").
func reconstructAbsoluteKey(prefix, relName string) string {
	return strings.TrimSuffix(prefix, "/") + "/" + strings.TrimPrefix(relName, "/")
}

// isNotFound returns true if err indicates the object doesn't exist.
// All storage backends in pkg/storage/ (s3, azblob, gcs, sftp, ftp, cos) wrap
// their provider-specific not-found errors and return storage.ErrNotFound, which
// is the canonical sentinel: errors.New("key not found") in pkg/storage/structs.go.
func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrNotFound)
}

// compile-time assertion
var _ cas.Backend = (*storageBackend)(nil)
