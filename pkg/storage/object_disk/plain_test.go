package object_disk

import (
	"context"
	"io"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type fakeMetaFile struct{ name string }

func (f fakeMetaFile) Size() int64             { return 1 }
func (f fakeMetaFile) Name() string            { return f.name }
func (f fakeMetaFile) LastModified() time.Time { return time.Time{} }

// fakeRemoteStorage implements only the methods newPlainDiskLayout touches, any other call panics
// through the nil embedded interface. A token mapped to an empty body emulates a prefix.path object
// deleted between the __meta/ listing and the read (a merge or DROP on the live disk), GetFileReader
// then returns an untyped 404-ish error the way S3 GetFileReaderAbsolute does, and StatFile returns
// statErr for it.
type fakeRemoteStorage struct {
	storage.RemoteStorage
	metaFiles map[string]string
	statErr   error
}

func (f *fakeRemoteStorage) Walk(_ context.Context, prefix string, _ bool, fn func(context.Context, storage.RemoteFile) error) error {
	if prefix != plainRewritableMetaDir {
		return nil
	}
	for token := range f.metaFiles {
		if err := fn(context.Background(), fakeMetaFile{name: token + "/" + plainRewritablePrefixPathName}); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeRemoteStorage) GetFileReader(_ context.Context, key string) (io.ReadCloser, error) {
	for token, body := range f.metaFiles {
		if key != path.Join(plainRewritableMetaDir, token, plainRewritablePrefixPathName) {
			continue
		}
		if body == "" {
			return nil, errors.Errorf("S3 GetFileReaderAbsolute: operation error S3: GetObject, https response error StatusCode: 404, NoSuchKey: The specified key does not exist, key=%s", key)
		}
		return io.NopCloser(strings.NewReader(body)), nil
	}
	return nil, errors.Errorf("unexpected GetFileReader key %s", key)
}

func (f *fakeRemoteStorage) StatFile(_ context.Context, key string) (storage.RemoteFile, error) {
	for token, body := range f.metaFiles {
		if key != path.Join(plainRewritableMetaDir, token, plainRewritablePrefixPathName) {
			continue
		}
		if body == "" {
			return nil, f.statErr
		}
		return fakeMetaFile{name: key}, nil
	}
	return nil, storage.NewErrNotFound(key)
}

// TestNewPlainDiskLayoutSkipsVanishedToken reproduces the race from
// https://github.com/Altinity/clickhouse-backup/actions/runs/33397256466: a prefix.path object
// listed under __meta/ is deleted before it is read, the layout must skip the token instead of
// failing the whole backup
func TestNewPlainDiskLayoutSkipsVanishedToken(t *testing.T) {
	r := require.New(t)
	remoteStorage := &fakeRemoteStorage{
		metaFiles: map[string]string{
			"vanishedtoken": "",
			"alivetoken":    "store/abc/data/\n",
		},
		statErr: storage.NewErrNotFound(path.Join(plainRewritableMetaDir, "vanishedtoken", plainRewritablePrefixPathName)),
	}
	l, err := newPlainDiskLayout(context.Background(), "disk_test", "plain_rewritable", remoteStorage)
	r.NoError(err)
	r.Equal(map[string]string{"store/abc/data": "alivetoken"}, l.dirs)
}

// TestNewPlainDiskLayoutFailsWhenReadErrorIsNotVanish keeps the fail-loudly behavior when the
// prefix.path object still exists (or StatFile errors differently), the read error is a real one
func TestNewPlainDiskLayoutFailsWhenReadErrorIsNotVanish(t *testing.T) {
	r := require.New(t)
	remoteStorage := &fakeRemoteStorage{
		metaFiles: map[string]string{
			"brokentoken": "",
		},
		statErr: errors.New("SlowDown: please reduce your request rate"),
	}
	_, err := newPlainDiskLayout(context.Background(), "disk_test", "plain_rewritable", remoteStorage)
	r.Error(err)
	r.Contains(err.Error(), "can't read")
}
