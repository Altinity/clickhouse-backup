package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// fileTransferRemote - in-memory RemoteStorage which counts concurrent PutFile / GetFileReader streams
type fileTransferRemote struct {
	RemoteStorage
	mu       sync.Mutex
	files    map[string][]byte
	inFlight atomic.Int64
	peak     atomic.Int64
	failKey  string
}

func (m *fileTransferRemote) streamStarted() {
	n := m.inFlight.Add(1)
	for p := m.peak.Load(); n > p && !m.peak.CompareAndSwap(p, n); p = m.peak.Load() {
	}
	time.Sleep(5 * time.Millisecond)
}

func (m *fileTransferRemote) Kind() string { return "memory" }

func (m *fileTransferRemote) PutFile(ctx context.Context, key string, r io.ReadCloser, _ int64) error {
	m.streamStarted()
	defer m.inFlight.Add(-1)
	if key == m.failKey {
		return errors.Errorf("PutFile %s failed", key)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.files[key] = data
	m.mu.Unlock()
	return ctx.Err()
}

type countingReadCloser struct {
	io.Reader
	once    sync.Once
	onClose func()
}

func (c *countingReadCloser) Close() error {
	c.once.Do(c.onClose)
	return nil
}

func (m *fileTransferRemote) GetFileReader(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	data, ok := m.files[key]
	m.mu.Unlock()
	if !ok || key == m.failKey {
		return nil, errors.Errorf("GetFileReader %s failed", key)
	}
	m.streamStarted()
	return &countingReadCloser{Reader: bytes.NewReader(data), onClose: func() { m.inFlight.Add(-1) }}, nil
}

func (m *fileTransferRemote) Walk(ctx context.Context, prefix string, _ bool, fn func(context.Context, RemoteFile) error) error {
	m.mu.Lock()
	keys := make([]string, 0, len(m.files))
	for k := range m.files {
		if strings.HasPrefix(k, prefix+"/") {
			keys = append(keys, k)
		}
	}
	m.mu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		if err := fn(ctx, memRemoteFile{name: strings.TrimPrefix(k, prefix+"/"), size: int64(len(m.files[k]))}); err != nil {
			return err
		}
	}
	return nil
}

const fileTransferTestFiles = 24

func newFileTransferRemote() *fileTransferRemote {
	m := &fileTransferRemote{files: map[string][]byte{}}
	for i := 0; i < fileTransferTestFiles; i++ {
		m.files[path.Join("remote/part", fmt.Sprintf("col%02d.bin", i))] = []byte(strings.Repeat(fmt.Sprintf("%02d", i), i+1))
	}
	return m
}

func writeFileTransferTestFiles(t *testing.T) (string, []string, int64) {
	dir := t.TempDir()
	names := make([]string, 0, fileTransferTestFiles)
	total := int64(0)
	for i := 0; i < fileTransferTestFiles; i++ {
		name := fmt.Sprintf("col%02d.bin", i)
		data := []byte(strings.Repeat(fmt.Sprintf("%02d", i), i+1))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0600))
		names = append(names, name)
		total += int64(len(data))
	}
	return dir, names, total
}

// holdCallerSlot - the part goroutine calling UploadPath/DownloadPath already holds one slot of the budget
func holdCallerSlot(t *testing.T, slots *semaphore.Weighted) {
	require.NoError(t, slots.Acquire(context.Background(), 1))
	t.Cleanup(func() { slots.Release(1) })
}

func TestUploadPathFileTransfers(t *testing.T) {
	testCases := []struct {
		name         string
		workers      int
		budget       int64
		expectedPeak int64
	}{
		{"sequential by default", 1, 0, 1},
		{"workers without budget stay sequential", 8, 0, 1},
		{"budget bounds the streams", 8, 4, 4},
		{"workers bound the streams", 3, 16, 3},
		{"no free slot keeps the caller stream only", 8, 1, 1},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir, names, total := writeFileTransferTestFiles(t)
			remote := &fileTransferRemote{files: map[string][]byte{}}
			bd := &BackupDestination{RemoteStorage: remote}
			var slots *semaphore.Weighted
			if tc.budget > 0 {
				slots = semaphore.NewWeighted(tc.budget)
				holdCallerSlot(t, slots)
			}
			bd.SetFileTransfers(tc.workers, slots)
			uploaded, err := bd.UploadPath(context.Background(), dir, names, "remote/part", 0, 0, 0, nil, 0)
			require.NoError(t, err)
			require.Equal(t, total, uploaded)
			require.Equal(t, tc.expectedPeak, remote.peak.Load())
			require.Len(t, remote.files, fileTransferTestFiles)
			for _, name := range names {
				local, readErr := os.ReadFile(filepath.Join(dir, name))
				require.NoError(t, readErr)
				require.Equal(t, local, remote.files[path.Join("remote/part", name)])
			}
			if slots != nil {
				require.True(t, budgetFullyFree(slots, tc.budget-1), "extra streams shall release their slots")
			}
		})
	}
}

func TestDownloadPathFileTransfers(t *testing.T) {
	for _, withManifest := range []bool{false, true} {
		t.Run(fmt.Sprintf("manifest=%v", withManifest), func(t *testing.T) {
			remote := newFileTransferRemote()
			bd := &BackupDestination{RemoteStorage: remote}
			slots := semaphore.NewWeighted(4)
			holdCallerSlot(t, slots)
			bd.SetFileTransfers(8, slots)
			local := t.TempDir()
			var downloaded int64
			var err error
			if withManifest {
				names := make([]string, 0, fileTransferTestFiles)
				for i := 0; i < fileTransferTestFiles; i++ {
					names = append(names, fmt.Sprintf("col%02d.bin", i))
				}
				downloaded, err = bd.DownloadPathWithManifest(context.Background(), "remote/part", local, names, 0, 0, 0, nil, 0)
			} else {
				downloaded, err = bd.DownloadPath(context.Background(), "remote/part", local, 0, 0, 0, nil, 0)
			}
			require.NoError(t, err)
			total := int64(0)
			for key, data := range remote.files {
				total += int64(len(data))
				got, readErr := os.ReadFile(filepath.Join(local, strings.TrimPrefix(key, "remote/part/")))
				require.NoError(t, readErr)
				require.Equal(t, data, got)
			}
			require.Equal(t, total, downloaded)
			require.Equal(t, int64(4), remote.peak.Load())
			require.True(t, budgetFullyFree(slots, 3))
		})
	}
}

func TestFileTransfersErrorStopsAndReleasesSlots(t *testing.T) {
	dir, names, _ := writeFileTransferTestFiles(t)
	remote := &fileTransferRemote{files: map[string][]byte{}, failKey: "remote/part/col05.bin"}
	bd := &BackupDestination{RemoteStorage: remote}
	slots := semaphore.NewWeighted(4)
	holdCallerSlot(t, slots)
	bd.SetFileTransfers(8, slots)
	_, err := bd.UploadPath(context.Background(), dir, names, "remote/part", 0, 0, 0, nil, 0)
	require.ErrorContains(t, err, "PutFile remote/part/col05.bin failed")
	require.Less(t, len(remote.files), fileTransferTestFiles, "files after the failure shall not be uploaded")
	require.Equal(t, int64(0), remote.inFlight.Load(), "every stream shall be finished when UploadPath returns")
	require.True(t, budgetFullyFree(slots, 3))
}
