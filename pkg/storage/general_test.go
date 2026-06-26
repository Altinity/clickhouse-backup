package storage

// TestBackupList_SkipPrefixesFiltering verifies that BackupList correctly
// skips top-level entries whose names match a configured CAS skip-prefix,
// and that entries that merely start with the same letters (but don't match
// the trimmed prefix exactly) are NOT filtered.
//
// The test exercises the logic added in Wave 6.A around line 246 of general.go.

import (
	"context"
	"io"
	"testing"
	"time"
)

// fakeRemoteFile is a minimal RemoteFile implementation for tests.
type fakeRemoteFile struct {
	name    string
	size    int64
	modTime time.Time
}

func (f fakeRemoteFile) Name() string            { return f.name }
func (f fakeRemoteFile) Size() int64             { return f.size }
func (f fakeRemoteFile) LastModified() time.Time { return f.modTime }

// fakeRemoteStorage is a minimal RemoteStorage that only implements Walk and
// Kind; every other method panics or returns a safe error. This is sufficient
// for BackupList's non-parseMetadata path (parseMetadataOnly == some-name that
// doesn't match any entry, so we stay in the early-return branch).
type fakeRemoteStorage struct {
	entries []fakeRemoteFile
}

func (f *fakeRemoteStorage) Kind() string { return "fake" }

func (f *fakeRemoteStorage) Walk(_ context.Context, _ string, _ bool, fn func(context.Context, RemoteFile) error) error {
	for _, e := range f.entries {
		if err := fn(context.Background(), e); err != nil {
			return err
		}
	}
	return nil
}

// WalkAbsolute delegates to Walk for test simplicity.
func (f *fakeRemoteStorage) WalkAbsolute(ctx context.Context, _ string, recursive bool, fn func(context.Context, RemoteFile) error) error {
	return f.Walk(ctx, "", recursive, fn)
}

func (f *fakeRemoteStorage) Connect(_ context.Context) error { return nil }
func (f *fakeRemoteStorage) Close(_ context.Context) error   { return nil }

func (f *fakeRemoteStorage) StatFile(_ context.Context, _ string) (RemoteFile, error) {
	return nil, ErrNotFound
}
func (f *fakeRemoteStorage) StatFileAbsolute(_ context.Context, _ string) (RemoteFile, error) {
	return nil, ErrNotFound
}
func (f *fakeRemoteStorage) DeleteFile(_ context.Context, _ string) error { return nil }
func (f *fakeRemoteStorage) DeleteFileFromObjectDiskBackup(_ context.Context, _ string) error {
	return nil
}
func (f *fakeRemoteStorage) GetFileReader(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, ErrNotFound
}
func (f *fakeRemoteStorage) GetFileReaderAbsolute(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, ErrNotFound
}
func (f *fakeRemoteStorage) GetFileReaderWithLocalPath(_ context.Context, _, _ string, _ int64) (io.ReadCloser, error) {
	return nil, ErrNotFound
}
func (f *fakeRemoteStorage) PutFile(_ context.Context, _ string, r io.ReadCloser, _ int64) error {
	_ = r.Close()
	return nil
}
func (f *fakeRemoteStorage) PutFileAbsolute(_ context.Context, _ string, r io.ReadCloser, _ int64) error {
	_ = r.Close()
	return nil
}
func (f *fakeRemoteStorage) PutFileAbsoluteIfAbsent(_ context.Context, _ string, r io.ReadCloser, _ int64) (bool, error) {
	_ = r.Close()
	return true, nil
}
func (f *fakeRemoteStorage) PutFileIfAbsent(_ context.Context, _ string, r io.ReadCloser, _ int64) (bool, error) {
	_ = r.Close()
	return true, nil
}
func (f *fakeRemoteStorage) CopyObject(_ context.Context, _ int64, _, _, _ string) (int64, error) {
	return 0, nil
}

// fakeBackupDest builds a BackupDestination backed by fakeRemoteStorage with
// the given top-level entries. compressionFormat is set to "tar" so the walk
// doesn't complain about extension mismatches (irrelevant in non-parseMetadata path).
func fakeBackupDest(entries []fakeRemoteFile) *BackupDestination {
	return &BackupDestination{
		RemoteStorage:     &fakeRemoteStorage{entries: entries},
		compressionFormat: "tar",
	}
}

func TestBackupList_SkipPrefixesFiltering(t *testing.T) {
	now := time.Now()
	entries := []fakeRemoteFile{
		{name: "cas/", size: 0, modTime: now},          // should be skipped when prefix="cas/"
		{name: "v1backup-1", size: 0, modTime: now},    // must NOT be skipped
		{name: "v1backup-2", size: 0, modTime: now},    // must NOT be skipped
		{name: "casematch", size: 0, modTime: now},     // must NOT be skipped ("cas" prefix but no trailing slash)
	}
	bd := fakeBackupDest(entries)

	// Case 1: skipPrefixes=["cas/"] — only v1backup-1, v1backup-2, casematch.
	got, err := bd.BackupList(context.Background(), false, "__nonexistent__", []string{"cas/"})
	if err != nil {
		t.Fatalf("BackupList case1: %v", err)
	}
	if len(got) != 3 {
		names := make([]string, len(got))
		for i, b := range got {
			names[i] = b.BackupName
		}
		t.Errorf("case1: got %d entries %v, want 3 (v1backup-1, v1backup-2, casematch)", len(got), names)
	}
	for _, b := range got {
		if b.BackupName == "cas" || b.BackupName == "cas/" {
			t.Errorf("case1: CAS prefix entry %q should have been filtered", b.BackupName)
		}
	}

	// "casematch" must survive (it's a valid v1 backup, just happens to share a prefix).
	found := false
	for _, b := range got {
		if b.BackupName == "casematch" {
			found = true
			break
		}
	}
	if !found {
		t.Error("case1: 'casematch' was incorrectly filtered by the CAS prefix check")
	}

	// Case 2: skipPrefixes=nil — all four entries pass through.
	got2, err := bd.BackupList(context.Background(), false, "__nonexistent__", nil)
	if err != nil {
		t.Fatalf("BackupList case2: %v", err)
	}
	if len(got2) != 4 {
		t.Errorf("case2: got %d entries, want 4 (nil skipPrefixes should pass all)", len(got2))
	}

	// Case 3: skipPrefixes=[""] — empty string matches nothing defensively.
	got3, err := bd.BackupList(context.Background(), false, "__nonexistent__", []string{""})
	if err != nil {
		t.Fatalf("BackupList case3: %v", err)
	}
	if len(got3) != 4 {
		t.Errorf("case3: got %d entries, want 4 (empty-string prefix should skip nothing)", len(got3))
	}
}
