package storage

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/eapache/go-resiliency/retrier"
)

// blockingReadCloser blocks on every Read until Close is called, then reports
// EOF. It models a remote body / extract stream that has stalled (slow or
// half-open network, disk backpressure) and only unblocks when something
// closes the underlying reader.
type blockingReadCloser struct {
	closed      chan struct{}
	closeOnce   sync.Once
	readStarted chan struct{}
	startOnce   sync.Once
}

func (b *blockingReadCloser) Read(_ []byte) (int, error) {
	b.startOnce.Do(func() { close(b.readStarted) })
	<-b.closed
	return 0, io.EOF
}

func (b *blockingReadCloser) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

type fakeRemoteFile struct{ size int64 }

func (f fakeRemoteFile) Size() int64             { return f.size }
func (f fakeRemoteFile) Name() string            { return "part.tar" }
func (f fakeRemoteFile) LastModified() time.Time { return time.Time{} }

// blockingRemote is a RemoteStorage whose download reader never returns data
// until closed. Only the methods DownloadCompressedStream needs are
// implemented; the embedded nil interface satisfies the rest (never called).
type blockingRemote struct {
	RemoteStorage
	r *blockingReadCloser
}

func (m *blockingRemote) StatFile(_ context.Context, _ string) (RemoteFile, error) {
	return fakeRemoteFile{size: 1024}, nil
}

func (m *blockingRemote) GetFileReaderWithLocalPath(_ context.Context, _, _ string, _ int64) (io.ReadCloser, error) {
	return m.r, nil
}

// failClassifier never retries, so the retrier returns on the first attempt.
type failClassifier struct{}

func (failClassifier) Classify(err error) retrier.Action {
	if err == nil {
		return retrier.Succeed
	}
	return retrier.Fail
}

// downloadPathRemote enumerates a single file whose reader blocks until closed.
type downloadPathRemote struct {
	RemoteStorage
	r *blockingReadCloser
}

func (m *downloadPathRemote) Kind() string { return "S3" }

func (m *downloadPathRemote) Walk(ctx context.Context, _ string, _ bool, fn func(context.Context, RemoteFile) error) error {
	return fn(ctx, fakeRemoteFile{size: 1024})
}

func (m *downloadPathRemote) GetFileReader(_ context.Context, _ string) (io.ReadCloser, error) {
	return m.r, nil
}

// TestDownloadPathCancel is the per-file (non-archive) counterpart of
// TestDownloadCompressedStreamCancel: DownloadPath copies a remote reader into
// a local file via copyWithBuffer and must return promptly on context cancel
// even when that read is stalled.
func TestDownloadPathCancel(t *testing.T) {
	br := &blockingReadCloser{closed: make(chan struct{}), readStarted: make(chan struct{})}
	bd := &BackupDestination{RemoteStorage: &downloadPathRemote{r: br}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := bd.DownloadPath(ctx, "remote/path", t.TempDir(), 0, time.Second, 0, failClassifier{}, 0)
		done <- err
	}()

	select {
	case <-br.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("DownloadPath never started reading the remote stream")
	}
	cancel()

	select {
	case <-done:
		// Returned after cancel — correct behavior.
	case <-time.After(5 * time.Second):
		t.Fatal("DownloadPath did not return within 5s after context cancel; " +
			"a stalled read is not honoring cancellation")
	}
}

// TestDownloadCompressedStreamCancel reproduces the production hang where
// /backup/kill cancels the command context but a download stuck in a stalled
// read/extract keeps running (observed: a download completing 6.32TiB hours
// after kill). DownloadCompressedStream must return promptly once the context
// is cancelled, even when the underlying read is blocked.
func TestDownloadCompressedStreamCancel(t *testing.T) {
	br := &blockingReadCloser{closed: make(chan struct{}), readStarted: make(chan struct{})}
	bd := &BackupDestination{
		RemoteStorage:     &blockingRemote{r: br},
		compressionFormat: "tar",
		pipeBufferSize:    1024 * 1024,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := bd.DownloadCompressedStream(ctx, "shadow/db/tbl/part.tar", filepath.Join(t.TempDir(), "out"), 0)
		done <- err
	}()

	// Wait until the stream is genuinely blocked on a read, then cancel.
	select {
	case <-br.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("download never started reading the remote stream")
	}
	cancel()

	select {
	case <-done:
		// Returned after cancel — correct behavior.
	case <-time.After(5 * time.Second):
		t.Fatal("DownloadCompressedStream did not return within 5s after context cancel; " +
			"a stalled read/extract is not honoring cancellation (reproduces the kill-does-not-stop-download bug)")
	}
}
