package object_disk

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
)

// blockingReadCloser blocks on every Read until Close is called, then reports
// EOF. Models a remote body that has stalled and only unblocks when something
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
func (f fakeRemoteFile) Name() string            { return "obj" }
func (f fakeRemoteFile) LastModified() time.Time { return time.Time{} }

// srcRemote provides a source reader that blocks until closed.
type srcRemote struct {
	storage.RemoteStorage
	r *blockingReadCloser
}

func (s *srcRemote) StatFileAbsolute(_ context.Context, _ string) (storage.RemoteFile, error) {
	return fakeRemoteFile{size: 1024}, nil
}

func (s *srcRemote) GetFileReaderAbsolute(_ context.Context, _ string) (io.ReadCloser, error) {
	return s.r, nil
}

// dstRemote drains the body it is given, modeling an uploader that reads the
// source stream to completion.
type dstRemote struct {
	storage.RemoteStorage
}

func (d *dstRemote) PutFileAbsolute(_ context.Context, _ string, r io.ReadCloser, _ int64) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

// TestCopyObjectStreamingCancel reproduces the same kill-does-not-stop class of
// bug for the object_disk streaming copy used by create/restore: a stalled
// source read makes CopyObjectStreaming run forever despite context
// cancellation. It must return promptly once the context is cancelled.
func TestCopyObjectStreamingCancel(t *testing.T) {
	br := &blockingReadCloser{closed: make(chan struct{}), readStarted: make(chan struct{})}
	src := &srcRemote{r: br}
	dst := &dstRemote{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- CopyObjectStreaming(ctx, src, dst, "src/key", "dst/key", nil)
	}()

	select {
	case <-br.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("streaming copy never started reading the source")
	}
	cancel()

	select {
	case <-done:
		// Returned after cancel — correct behavior.
	case <-time.After(5 * time.Second):
		t.Fatal("CopyObjectStreaming did not return within 5s after context cancel; " +
			"a stalled read is not honoring cancellation (reproduces the kill-does-not-stop bug for create/restore object_disk)")
	}
}
