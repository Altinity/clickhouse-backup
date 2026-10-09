package storage

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// memFetcher serves byte ranges of an in-memory object, optionally paced per request (one request = one
// stream with a fixed ceiling, like one GET against object storage) and counting concurrent requests.
type memFetcher struct {
	data        []byte
	bytesPerSec float64
	failChunkAt int64 // offset that always fails, -1 = never
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	requests    atomic.Int64
}

func (f *memFetcher) fetch(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	f.requests.Add(1)
	if f.failChunkAt >= 0 && offset == f.failChunkAt {
		return nil, errors.New("injected range failure")
	}
	n := f.inFlight.Add(1)
	for {
		m := f.maxInFlight.Load()
		if n <= m || f.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	defer f.inFlight.Add(-1)
	if f.bytesPerSec > 0 {
		select {
		case <-time.After(time.Duration(float64(length) / f.bytesPerSec * float64(time.Second))):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return io.NopCloser(bytes.NewReader(f.data[offset : offset+length])), nil
}

func randomBytes(t *testing.T, n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

// budgetFullyFree checks that every slot of the shared budget has been returned.
func budgetFullyFree(b *semaphore.Weighted, size int64) bool {
	if !b.TryAcquire(size) {
		return false
	}
	b.Release(size)
	return true
}

func TestRangedReaderReturnsExactBytesInOrder(t *testing.T) {
	data := randomBytes(t, 10*1024*1024+12345) // not a multiple of the chunk size
	f := &memFetcher{data: data, failChunkAt: -1}
	budget := semaphore.NewWeighted(3)
	r := newRangedReader(context.Background(), f.fetch, int64(len(data)), 1024*1024, 4, 0, budget)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.True(t, bytes.Equal(data, got), "ranged reader must reproduce the object byte for byte")
	require.Equal(t, int64(11), f.requests.Load(), "one request per chunk")
	require.LessOrEqual(t, f.maxInFlight.Load(), int64(3), "the shared budget caps concurrent requests")
	require.True(t, budgetFullyFree(budget, 3), "all budget slots returned after Close")
}

func TestRangedReaderPropagatesErrorAndReleasesBudget(t *testing.T) {
	data := randomBytes(t, 8*1024*1024)
	f := &memFetcher{data: data, failChunkAt: 3 * 1024 * 1024}
	budget := semaphore.NewWeighted(4)
	r := newRangedReader(context.Background(), f.fetch, int64(len(data)), 1024*1024, 4, 1, budget)
	_, err := io.ReadAll(r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected range failure")
	require.NoError(t, r.Close())
	require.True(t, budgetFullyFree(budget, 4))
	_, err = r.Read(make([]byte, 10))
	require.Error(t, err, "reads after an error/Close keep failing")
}

func TestRangedReaderCloseMidStreamReleasesEverything(t *testing.T) {
	data := randomBytes(t, 16*1024*1024)
	f := &memFetcher{data: data, failChunkAt: -1, bytesPerSec: 64 * 1024 * 1024}
	budget := semaphore.NewWeighted(8)
	r := newRangedReader(context.Background(), f.fetch, int64(len(data)), 1024*1024, 8, 0, budget)
	buf := make([]byte, 1500*1024)
	_, err := io.ReadFull(r, buf)
	require.NoError(t, err)
	require.True(t, bytes.Equal(data[:len(buf)], buf))
	require.NoError(t, r.Close())
	require.True(t, budgetFullyFree(budget, 8), "Close mid-stream must give back prefetched chunks")
}

func TestRangedReaderCloseUnblocksPendingRead(t *testing.T) {
	data := randomBytes(t, 4*1024*1024)
	f := &memFetcher{data: data, failChunkAt: -1, bytesPerSec: 1024} // effectively stalled
	budget := semaphore.NewWeighted(2)
	r := newRangedReader(context.Background(), f.fetch, int64(len(data)), 1024*1024, 2, 0, budget)
	done := make(chan error, 1)
	go func() {
		_, err := r.Read(make([]byte, 10))
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, r.Close())
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Read stayed blocked after Close")
	}
	require.True(t, budgetFullyFree(budget, 2))
}

// Many readers competing for a small shared budget must all finish (no deadlock between readers).
func TestRangedReadersSharingSmallBudgetAllComplete(t *testing.T) {
	budget := semaphore.NewWeighted(2)
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := randomBytes(t, 3*1024*1024+7)
			f := &memFetcher{data: data, failChunkAt: -1, bytesPerSec: 256 * 1024 * 1024}
			r := newRangedReader(context.Background(), f.fetch, int64(len(data)), 256*1024, 8, 0, budget)
			got, err := io.ReadAll(r)
			_ = r.Close()
			if err == nil && !bytes.Equal(got, data) {
				err = errors.New("content mismatch")
			}
			errs <- err
		}()
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("readers sharing one budget deadlocked")
	}
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.True(t, budgetFullyFree(budget, 2))
}

// With a per-request ceiling, N ranges of one object finish ~N times faster than one stream: this is the
// single-big-part tail of a restore.
func TestRangedReaderScalesWithWorkers(t *testing.T) {
	data := randomBytes(t, 32*1024*1024)
	elapsed := func(workers int) time.Duration {
		f := &memFetcher{data: data, failChunkAt: -1, bytesPerSec: 64 * 1024 * 1024}
		budget := semaphore.NewWeighted(int64(workers))
		start := time.Now()
		r := newRangedReader(context.Background(), f.fetch, int64(len(data)), 1024*1024, workers, 0, budget)
		_, err := io.Copy(io.Discard, r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		return time.Since(start)
	}
	one := elapsed(1)
	eight := elapsed(8)
	t.Logf("32MiB at 64MiB/s per request: 1 worker %s, 8 workers %s (%.1fx)", one, eight, float64(one)/float64(eight))
	require.Greater(t, float64(one)/float64(eight), 4.0, "8 concurrent ranges must be several times faster than one stream")
}

// rangedRemote serves one tar archive through the RangedReaderProvider path only; the single-stream
// methods fail the test if DownloadCompressedStream ever falls back to them.
type rangedRemote struct {
	RemoteStorage
	t       *testing.T
	archive []byte
	used    atomic.Bool
	budget  *semaphore.Weighted
}

func (m *rangedRemote) StatFile(_ context.Context, _ string) (RemoteFile, error) {
	return fakeRemoteFile{size: int64(len(m.archive))}, nil
}

func (m *rangedRemote) GetFileRangedReader(ctx context.Context, _ string, size int64) (io.ReadCloser, bool, error) {
	m.used.Store(true)
	f := &memFetcher{data: m.archive, failChunkAt: -1}
	return newRangedReader(ctx, f.fetch, size, 64*1024, 4, 0, m.budget), true, nil
}

func (m *rangedRemote) GetFileReader(_ context.Context, _ string) (io.ReadCloser, error) {
	m.t.Fatal("single-stream GetFileReader must not be used when the ranged reader applies")
	return nil, nil
}

func (m *rangedRemote) GetFileReaderWithLocalPath(_ context.Context, _, _ string, _ int64) (io.ReadCloser, error) {
	m.t.Fatal("GetFileReaderWithLocalPath must not be used when the ranged reader applies")
	return nil, nil
}

// The ranged path must also be taken when a download rate limit is set (restore jobs always set one),
// and the extracted files must be identical.
func TestDownloadCompressedStreamUsesRangedReaderWithRateLimit(t *testing.T) {
	payload := randomBytes(t, 1024*1024+3)
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "data.bin", Mode: 0640, Size: int64(len(payload))}))
	_, err := tw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	budget := semaphore.NewWeighted(4)
	remote := &rangedRemote{t: t, archive: archive.Bytes(), budget: budget}
	bd := &BackupDestination{RemoteStorage: remote, compressionFormat: "tar", pipeBufferSize: 128 * 1024}
	out := filepath.Join(t.TempDir(), "out")
	_, err = bd.DownloadCompressedStream(context.Background(), "shadow/db/tbl/part.tar", out, 1<<40)
	require.NoError(t, err)
	require.True(t, remote.used.Load(), "ranged reader was not used")
	got, err := os.ReadFile(filepath.Join(out, "data.bin"))
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload, got))
	require.True(t, budgetFullyFree(budget, 4))
}
