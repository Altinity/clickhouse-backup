package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

var (
	// ErrNotFound is returned when file/object cannot be found
	ErrNotFound = errors.New("key not found")
)

// KeyError represents an error for a specific key during batch deletion
type KeyError struct {
	Key string
	Err error
}

func (e KeyError) Error() string {
	return fmt.Sprintf("key %s: %v", e.Key, e.Err)
}

// BatchDeleteError represents errors that occurred during batch deletion
type BatchDeleteError struct {
	Message  string
	Failures []KeyError
}

func (e *BatchDeleteError) Error() string {
	if len(e.Failures) == 0 {
		return e.Message
	}
	var sb strings.Builder
	sb.WriteString(e.Message)
	sb.WriteString(fmt.Sprintf(" (%d failures)", len(e.Failures)))
	// Show first few failures
	maxShow := 3
	if len(e.Failures) < maxShow {
		maxShow = len(e.Failures)
	}
	for i := 0; i < maxShow; i++ {
		sb.WriteString(fmt.Sprintf("; %s", e.Failures[i].Error()))
	}
	if len(e.Failures) > maxShow {
		sb.WriteString(fmt.Sprintf("; ... and %d more", len(e.Failures)-maxShow))
	}
	return sb.String()
}

// BatchDeleter is an optional interface that storage backends can implement
// to support batch deletion of keys for improved performance
type BatchDeleter interface {
	// DeleteKeysBatch deletes a batch of keys
	// Batching (collecting keys up to DeleteBatchSize) should be done by the caller
	// Returns nil if all keys were deleted successfully
	// Returns BatchDeleteError if some keys failed to delete
	DeleteKeysBatch(ctx context.Context, keys []string) error

	// DeleteKeysFromObjectDiskBackupBatch deletes a batch of keys from object disk backup path
	DeleteKeysFromObjectDiskBackupBatch(ctx context.Context, keys []string) error
}

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
	StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error)
	DeleteFile(ctx context.Context, key string) error
	DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error
	Walk(ctx context.Context, prefix string, recursive bool, fn func(context.Context, RemoteFile) error) error
	WalkAbsolute(ctx context.Context, absolutePrefix string, recursive bool, fn func(context.Context, RemoteFile) error) error
	GetFileReader(ctx context.Context, key string) (io.ReadCloser, error)
	GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error)
	GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error)
	PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error
	PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error
	CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error)
}
