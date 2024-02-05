package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	// ErrNotFound is returned when file/object cannot be found
	ErrNotFound = errors.New("key not found")
)

// RemoteFile - interface describe file on remote storage
type RemoteFile interface {
	Size() int64
	Name() string
	LastModified() time.Time
}

// RemoteStorage -
type RemoteStorage interface {
	Kind() string
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
	StatFile(ctx context.Context, key string) (RemoteFile, error)
	DeleteFile(ctx context.Context, key string) error
	DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error
	Walk(ctx context.Context, prefix string, recursive bool, fn func(context.Context, RemoteFile) error) error
	WalkAbsolute(ctx context.Context, absolutePrefix string, recursive bool, fn func(context.Context, RemoteFile) error) error
	GetFileReader(ctx context.Context, key string) (io.ReadCloser, error)
	GetFileReaderWithLocalPath(ctx context.Context, key, localPath string) (io.ReadCloser, error)
	PutFile(ctx context.Context, key string, r io.ReadCloser) error
	CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error)
}
