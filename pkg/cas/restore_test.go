package cas_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/testfixtures"
)

// uploadAndPrepare seeds a fake backend with a CAS backup named "b1" that
// downloads cleanly. Returned bits are everything Restore needs.
func uploadAndPrepare(t *testing.T, name string) (*fakedst.Fake, cas.Config, cas.RestoreOptions) {
	t.Helper()
	parts := []testfixtures.PartSpec{
		{Disk: "default", DB: "db1", Table: "t1", Name: "p1", Files: []testfixtures.FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 1, HashHigh: 1},
			{Name: "data.bin", Size: 1024, HashLow: 999, HashHigh: 1, Bytes: makeBlobBytes(0x42)},
		}},
	}
	lb := testfixtures.Build(t, parts)
	f := fakedst.New()
	cfg := testCfg(100)
	if _, err := cas.Upload(context.Background(), f, cfg, name, cas.UploadOptions{LocalBackupDir: lb.Root}); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	opts := cas.RestoreOptions{
		DownloadOptions: cas.DownloadOptions{LocalBackupDir: t.TempDir()},
	}
	return f, cfg, opts
}

func TestRestore_HappyPath(t *testing.T) {
	f, cfg, opts := uploadAndPrepare(t, "b1")
	var (
		gotDir  string
		gotName string
		calls   int
	)
	cb := func(ctx context.Context, localBackupDir string, ro cas.RestoreOptions) error {
		calls++
		gotDir = localBackupDir
		gotName = filepath.Base(localBackupDir)
		// Sanity: the directory should actually exist on disk after Download.
		if _, err := os.Stat(filepath.Join(localBackupDir, "metadata.json")); err != nil {
			t.Errorf("metadata.json missing under callback's localBackupDir: %v", err)
		}
		return nil
	}
	if err := cas.Restore(context.Background(), f, cfg, "b1", opts, cb); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if calls != 1 {
		t.Errorf("callback calls = %d, want 1", calls)
	}
	if gotName != "b1" {
		t.Errorf("callback localBackupDir = %q, want basename b1 (got %q)", gotDir, gotName)
	}
	wantPrefix := opts.LocalBackupDir
	if !strings.HasPrefix(gotDir, wantPrefix) {
		t.Errorf("callback localBackupDir %q is not under %q", gotDir, wantPrefix)
	}
}

func TestRestore_PropagatesCallbackError(t *testing.T) {
	f, cfg, opts := uploadAndPrepare(t, "b1")
	sentinel := errors.New("v1 restore exploded")
	cb := func(ctx context.Context, localBackupDir string, ro cas.RestoreOptions) error {
		return sentinel
	}
	err := cas.Restore(context.Background(), f, cfg, "b1", opts, cb)
	if !errors.Is(err, sentinel) {
		t.Fatalf("got err=%v want sentinel %v", err, sentinel)
	}
}

func TestRestore_RefusesIgnoreDependencies(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	opts := cas.RestoreOptions{
		DownloadOptions:    cas.DownloadOptions{LocalBackupDir: t.TempDir()},
		IgnoreDependencies: true,
	}
	called := 0
	cb := func(ctx context.Context, localBackupDir string, ro cas.RestoreOptions) error {
		called++
		return nil
	}
	err := cas.Restore(context.Background(), f, cfg, "any", opts, cb)
	if err == nil || !strings.Contains(err.Error(), "ignore-dependencies") {
		t.Fatalf("got err=%v want ignore-dependencies error", err)
	}
	if called != 0 {
		t.Errorf("callback called %d times under ignore-dependencies; want 0", called)
	}
}

func TestRestore_NilCallbackError(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	opts := cas.RestoreOptions{
		DownloadOptions: cas.DownloadOptions{LocalBackupDir: t.TempDir()},
	}
	err := cas.Restore(context.Background(), f, cfg, "b1", opts, nil)
	if err == nil || !strings.Contains(err.Error(), "V1RestoreFunc") {
		t.Fatalf("got err=%v want V1RestoreFunc-not-supplied error", err)
	}
}

func TestRestore_PropagatesDownloadError(t *testing.T) {
	// Empty backend → ValidateBackup fails on missing metadata.json.
	f := fakedst.New()
	cfg := testCfg(100)
	opts := cas.RestoreOptions{
		DownloadOptions: cas.DownloadOptions{LocalBackupDir: t.TempDir()},
	}
	called := 0
	cb := func(ctx context.Context, localBackupDir string, ro cas.RestoreOptions) error {
		called++
		return nil
	}
	err := cas.Restore(context.Background(), f, cfg, "absent", opts, cb)
	if !errors.Is(err, cas.ErrMissingMetadata) {
		t.Fatalf("got err=%v want ErrMissingMetadata", err)
	}
	if called != 0 {
		t.Errorf("callback called %d times despite Download failure; want 0", called)
	}
}
