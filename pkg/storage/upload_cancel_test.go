package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// putBlockingRemote.PutFile blocks until the context is cancelled, then returns
// ctx.Err(). It models a well-behaved remote upload that aborts when its request
// context is cancelled (e.g. an S3 PutObject), and is used to verify the upload
// paths unwind promptly on /backup/kill.
type putBlockingRemote struct {
	RemoteStorage
	putStarted chan struct{}
	startOnce  sync.Once
}

func (m *putBlockingRemote) PutFile(ctx context.Context, _ string, _ io.ReadCloser, _ int64) error {
	m.startOnce.Do(func() { close(m.putStarted) })
	<-ctx.Done()
	return ctx.Err()
}

func (m *putBlockingRemote) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, size int64) error {
	return m.PutFile(ctx, key, r, size)
}

func writeTempFile(t *testing.T) (dir, name string) {
	t.Helper()
	dir = t.TempDir()
	name = "part.bin"
	if err := os.WriteFile(filepath.Join(dir, name), []byte("some payload bytes"), 0600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return dir, name
}

// TestUploadCompressedStreamCancel verifies the archive-upload path unwinds on
// context cancel. Unlike download, upload reads local files and pushes to the
// remote via PutFile(ctx); cancellation propagates through the errgroup and the
// nio pipe cross-close, so this guards that wiring stays correct.
func TestUploadCompressedStreamCancel(t *testing.T) {
	dir, name := writeTempFile(t)
	remote := &putBlockingRemote{putStarted: make(chan struct{})}
	bd := &BackupDestination{
		RemoteStorage:     remote,
		compressionFormat: "tar",
		pipeBufferSize:    1024 * 1024,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bd.UploadCompressedStream(ctx, dir, []string{name}, "remote/data.tar", 0)
	}()

	select {
	case <-remote.putStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload never started PutFile")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("UploadCompressedStream did not return within 5s after context cancel")
	}
}

// TestUploadPathCancel is the per-file (non-archive) counterpart.
func TestUploadPathCancel(t *testing.T) {
	dir, name := writeTempFile(t)
	remote := &putBlockingRemote{putStarted: make(chan struct{})}
	bd := &BackupDestination{RemoteStorage: remote}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := bd.UploadPath(ctx, dir, []string{name}, "remote", 0, time.Second, 0, failClassifier{}, 0)
		done <- err
	}()

	select {
	case <-remote.putStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload never started PutFile")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("UploadPath did not return within 5s after context cancel")
	}
}
