package fakedst

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
)

// Fake is an in-memory implementation of cas.Backend for use in tests.
type Fake struct {
	mu         sync.Mutex
	files      map[string]fakeFile
	statHook   func(key string) (size int64, modTime time.Time, exists bool, err error, override bool)
	putHook    func(key string) (err error, override bool)
	deleteHook func(key string) (err error, override bool)
}

type fakeFile struct {
	data    []byte
	modTime time.Time
}

// New returns an empty Fake backend.
func New() *Fake { return &Fake{files: map[string]fakeFile{}} }

// SetModTime is a test-only helper for ageing fixtures.
func (f *Fake) SetModTime(key string, t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.files[key]; ok {
		e.modTime = t
		f.files[key] = e
	}
}

// SetStatHook installs a function consulted by StatFile before its normal
// lookup. If the hook returns override=true, its other return values are
// used verbatim. Used by tests to inject errors at specific keys.
func (f *Fake) SetStatHook(h func(key string) (int64, time.Time, bool, error, bool)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statHook = h
}

// SetPutHook installs a function consulted by PutFile and PutFileIfAbsent
// before the normal store. If the hook returns override=true and a non-nil
// error, that error is returned instead of writing. Used by tests to inject
// errors at specific keys.
func (f *Fake) SetPutHook(h func(key string) (err error, override bool)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putHook = h
}

// SetDeleteHook installs a function consulted by DeleteFile before the normal
// delete. If the hook returns override=true and a non-nil error, that error is
// returned instead of deleting. Used by tests to inject delete failures.
func (f *Fake) SetDeleteHook(h func(key string) (err error, override bool)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteHook = h
}

// Len is a test helper for assertions.
func (f *Fake) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.files)
}

func (f *Fake) PutFile(ctx context.Context, key string, r io.ReadCloser, size int64) error {
	defer r.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return err
	}
	f.mu.Lock()
	hook := f.putHook
	f.mu.Unlock()
	if hook != nil {
		if err, override := hook(key); override && err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[key] = fakeFile{data: buf.Bytes(), modTime: time.Now()}
	return nil
}

// PutFileIfAbsent atomically writes data at key only if not present.
// In the in-memory fake, this is a single map operation under the lock.
func (f *Fake) PutFileIfAbsent(ctx context.Context, key string, data io.ReadCloser, size int64) (bool, error) {
	body, err := io.ReadAll(data)
	_ = data.Close()
	if err != nil {
		return false, err
	}
	f.mu.Lock()
	hook := f.putHook
	f.mu.Unlock()
	if hook != nil {
		if err, override := hook(key); override && err != nil {
			return false, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.files[key]; exists {
		return false, nil
	}
	f.files[key] = fakeFile{data: body, modTime: time.Now()}
	return true, nil
}

func (f *Fake) GetFile(ctx context.Context, key string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.files[key]
	if !ok {
		return nil, errors.New("fakedst: not found")
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), e.data...))), nil
}

func (f *Fake) StatFile(ctx context.Context, key string) (int64, time.Time, bool, error) {
	f.mu.Lock()
	hook := f.statHook
	f.mu.Unlock()
	if hook != nil {
		if size, modTime, exists, err, override := hook(key); override {
			return size, modTime, exists, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.files[key]
	if !ok {
		return 0, time.Time{}, false, nil
	}
	return int64(len(e.data)), e.modTime, true, nil
}

func (f *Fake) DeleteFile(ctx context.Context, key string) error {
	f.mu.Lock()
	hook := f.deleteHook
	f.mu.Unlock()
	if hook != nil {
		if err, override := hook(key); override && err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.files, key)
	return nil
}

func (f *Fake) Walk(ctx context.Context, prefix string, recursive bool, fn func(cas.RemoteFile) error) error {
	f.mu.Lock()
	keys := make([]string, 0, len(f.files))
	for k := range f.files {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if !recursive {
			// Only emit one-level entries: skip keys that contain '/' after the prefix.
			rest := strings.TrimPrefix(k, prefix)
			if strings.Contains(rest, "/") {
				continue
			}
		}
		keys = append(keys, k)
	}
	snapshot := make(map[string]fakeFile, len(keys))
	for _, k := range keys {
		snapshot[k] = f.files[k]
	}
	f.mu.Unlock()

	sort.Strings(keys)
	for _, k := range keys {
		e := snapshot[k]
		if err := fn(cas.RemoteFile{Key: k, Size: int64(len(e.data)), ModTime: e.modTime}); err != nil {
			return err
		}
	}
	return nil
}

// compile-time assertion
var _ cas.Backend = (*Fake)(nil)
