package cas

import (
	"context"
	"io"
	"time"
)

// Backend is the narrow subset of remote-storage operations CAS uses.
// Defining a small interface lets tests substitute an in-memory fake and keeps
// CAS decoupled from the full storage.BackupDestination surface.
//
// All keys are full object keys (the cluster prefix is already part of them).
type Backend interface {
	PutFile(ctx context.Context, key string, data io.ReadCloser, size int64) error
	GetFile(ctx context.Context, key string) (io.ReadCloser, error)
	StatFile(ctx context.Context, key string) (size int64, modTime time.Time, exists bool, err error)
	DeleteFile(ctx context.Context, key string) error
	Walk(ctx context.Context, prefix string, recursive bool, fn func(RemoteFile) error) error

	// PutFileIfAbsent atomically writes data at key only if no object
	// exists. Returns (true, nil) on successful create; (false, nil)
	// if the key is already present; (false, ErrConditionalPutNotSupported)
	// when the underlying backend can't do atomic create.
	PutFileIfAbsent(ctx context.Context, key string, data io.ReadCloser, size int64) (created bool, err error)
}

// RemoteFile is a snapshot of an object's metadata returned by Walk callbacks.
type RemoteFile struct {
	Key     string
	Size    int64
	ModTime time.Time
}
