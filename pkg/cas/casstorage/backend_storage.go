// Package casstorage wires the CAS Backend interface to pkg/storage.BackupDestination.
// It lives in a sub-package so that pkg/cas itself does not import pkg/storage,
// which would create an import cycle via pkg/storage → pkg/config → pkg/cas.
package casstorage

import (
	"context"
	"errors"
	"io"
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

func (s *storageBackend) Walk(ctx context.Context, prefix string, recursive bool, fn func(cas.RemoteFile) error) error {
	return s.bd.Walk(ctx, prefix, recursive, func(_ context.Context, rf storage.RemoteFile) error {
		return fn(cas.RemoteFile{Key: rf.Name(), Size: rf.Size(), ModTime: rf.LastModified()})
	})
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
