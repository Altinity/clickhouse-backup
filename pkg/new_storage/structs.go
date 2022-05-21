package new_storage

import (
	"errors"
	"io"
	"time"
)

var (
	// ErrNotFound is returned when file/object cannot be found
	ErrNotFound         = errors.New("key not found")
	ErrFileDoesNotExist = errors.New("file does not exist")
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
	StatFile(key string) (RemoteFile, error)
	DeleteFile(key string) error
	Connect() error
	Walk(prefix string, recursive bool, fn func(RemoteFile) error) error
	GetFileReader(key string) (io.ReadCloser, error)
	GetFileReaderWithLocalPath(key, localPath string) (io.ReadCloser, error)
	PutFile(key string, r io.ReadCloser) error
}
