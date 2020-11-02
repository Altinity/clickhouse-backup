package storage

import (
	"errors"
	"io"
	"time"
)

var (
	// ErrNotFound is returned when file/object cannot be found
	ErrNotFound = errors.New("file not found")
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
	GetFile(string) (RemoteFile, error)
	DeleteFile(string) error
	Connect() error
	Walk(string, func(RemoteFile)) error
	GetFileReader(key string) (io.ReadCloser, error)
	PutFile(key string, r io.ReadCloser) error
}

