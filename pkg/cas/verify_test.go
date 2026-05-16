package cas_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/testfixtures"
)

// uploadForVerify is a helper that builds a local backup with a blob file and
// uploads it via cas.Upload, returning the backend and the config.
func uploadForVerify(t *testing.T) (*fakedst.Fake, cas.Config) {
	t.Helper()
	parts := []testfixtures.PartSpec{
		{
			Disk: "default", DB: "db1", Table: "t1", Name: "p1",
			Files: []testfixtures.FileSpec{
				{Name: "columns.txt", Size: 23, HashLow: 1, HashHigh: 1},
				{Name: "data.bin", Size: 1024, HashLow: 999, HashHigh: 999},
			},
		},
	}
	lb := testfixtures.Build(t, parts)
	f := fakedst.New()
	cfg := testCfg(100) // threshold=100 → data.bin (1024) becomes a blob
	_, err := cas.Upload(context.Background(), f, cfg, "b1", cas.UploadOptions{
		LocalBackupDir: lb.Root,
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	return f, cfg
}

func TestVerify_AllPresent(t *testing.T) {
	f, cfg := uploadForVerify(t)
	var out bytes.Buffer
	res, err := cas.Verify(context.Background(), f, cfg, "b1", cas.VerifyOptions{}, &out)
	if err != nil {
		t.Fatalf("Verify returned err=%v; want nil", err)
	}
	if res == nil {
		t.Fatal("Verify returned nil result")
	}
	if len(res.Failures) != 0 {
		t.Errorf("Failures: got %d want 0; %v", len(res.Failures), res.Failures)
	}
	if res.BlobsChecked != 1 {
		t.Errorf("BlobsChecked: got %d want 1", res.BlobsChecked)
	}
	if res.BackupName != "b1" {
		t.Errorf("BackupName: got %q want b1", res.BackupName)
	}
	if out.Len() != 0 {
		t.Errorf("unexpected output: %q", out.String())
	}
}

func TestVerify_DetectsMissingBlob(t *testing.T) {
	f, cfg := uploadForVerify(t)
	cp := cfg.ClusterPrefix()

	// Walk the blob/ prefix to find the blob key.
	var blobKey string
	_ = f.Walk(context.Background(), cp+"blob/", true, func(rf cas.RemoteFile) error {
		blobKey = rf.Key
		return nil
	})
	if blobKey == "" {
		t.Fatal("no blob found after upload")
	}

	// Delete the blob.
	if err := f.DeleteFile(context.Background(), blobKey); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	var out bytes.Buffer
	res, err := cas.Verify(context.Background(), f, cfg, "b1", cas.VerifyOptions{}, &out)
	if !errors.Is(err, cas.ErrVerifyFailures) {
		t.Fatalf("Verify err=%v; want ErrVerifyFailures", err)
	}
	if res == nil {
		t.Fatal("Verify returned nil result alongside error")
	}
	if len(res.Failures) != 1 {
		t.Fatalf("Failures: got %d want 1; %v", len(res.Failures), res.Failures)
	}
	if res.Failures[0].Kind != "missing" {
		t.Errorf("Failure.Kind: got %q want missing", res.Failures[0].Kind)
	}
	if res.Failures[0].Path != blobKey {
		t.Errorf("Failure.Path: got %q want %q", res.Failures[0].Path, blobKey)
	}
	if !strings.Contains(out.String(), "MISSING") {
		t.Errorf("expected MISSING in output; got %q", out.String())
	}
}

func TestVerify_DetectsSizeMismatch(t *testing.T) {
	f, cfg := uploadForVerify(t)
	cp := cfg.ClusterPrefix()

	// Find the blob key.
	var blobKey string
	_ = f.Walk(context.Background(), cp+"blob/", true, func(rf cas.RemoteFile) error {
		blobKey = rf.Key
		return nil
	})
	if blobKey == "" {
		t.Fatal("no blob found after upload")
	}

	// Overwrite the blob with wrong-sized data (only 10 bytes).
	wrongData := []byte("tooshort!!")
	if err := f.PutFile(context.Background(), blobKey,
		io.NopCloser(bytes.NewReader(wrongData)), int64(len(wrongData))); err != nil {
		t.Fatalf("PutFile (overwrite): %v", err)
	}

	var out bytes.Buffer
	res, err := cas.Verify(context.Background(), f, cfg, "b1", cas.VerifyOptions{}, &out)
	if !errors.Is(err, cas.ErrVerifyFailures) {
		t.Fatalf("Verify err=%v; want ErrVerifyFailures", err)
	}
	if len(res.Failures) != 1 {
		t.Fatalf("Failures: got %d want 1; %v", len(res.Failures), res.Failures)
	}
	if res.Failures[0].Kind != "size_mismatch" {
		t.Errorf("Failure.Kind: got %q want size_mismatch", res.Failures[0].Kind)
	}
	if res.Failures[0].Want != 1024 {
		t.Errorf("Failure.Want: got %d want 1024", res.Failures[0].Want)
	}
	if res.Failures[0].Got != int64(len(wrongData)) {
		t.Errorf("Failure.Got: got %d want %d", res.Failures[0].Got, len(wrongData))
	}
	if !strings.Contains(out.String(), "MISMATCH") {
		t.Errorf("expected MISMATCH in output; got %q", out.String())
	}
}

func TestVerify_JSONOutput(t *testing.T) {
	f, cfg := uploadForVerify(t)
	cp := cfg.ClusterPrefix()

	// Find and delete the blob.
	var blobKey string
	_ = f.Walk(context.Background(), cp+"blob/", true, func(rf cas.RemoteFile) error {
		blobKey = rf.Key
		return nil
	})
	if blobKey == "" {
		t.Fatal("no blob found after upload")
	}
	if err := f.DeleteFile(context.Background(), blobKey); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	var out bytes.Buffer
	res, err := cas.Verify(context.Background(), f, cfg, "b1", cas.VerifyOptions{JSON: true}, &out)
	if !errors.Is(err, cas.ErrVerifyFailures) {
		t.Fatalf("Verify err=%v; want ErrVerifyFailures", err)
	}
	if len(res.Failures) != 1 {
		t.Fatalf("Failures: got %d want 1", len(res.Failures))
	}

	// Parse the JSON output line.
	line := strings.TrimSpace(out.String())
	var vf cas.VerifyFailure
	if err := json.Unmarshal([]byte(line), &vf); err != nil {
		t.Fatalf("json.Unmarshal output line %q: %v", line, err)
	}
	if vf.Kind != "missing" {
		t.Errorf("JSON Kind: got %q want missing", vf.Kind)
	}
	if vf.Path != blobKey {
		t.Errorf("JSON Path: got %q want %q", vf.Path, blobKey)
	}
	if vf.Want == 0 {
		t.Error("JSON Want: got 0, want non-zero")
	}
}

// stallingBackend wraps another Backend and forces StatFile to return a
// non-nil error for one specific key — simulating a transient network
// hiccup. All other methods delegate.
type stallingBackend struct {
	cas.Backend
	failKey string
}

func (s *stallingBackend) StatFile(ctx context.Context, key string) (int64, time.Time, bool, error) {
	if key == s.failKey {
		return 0, time.Time{}, false, errors.New("simulated network error")
	}
	return s.Backend.StatFile(ctx, key)
}

func TestVerify_StatErrorIsNotMissing(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	ctx := context.Background()
	parts := []testfixtures.PartSpec{{
		Disk: "default", DB: "db", Table: "t", Name: "all_1_1_0",
		Files: []testfixtures.FileSpec{
			// Above-threshold so it goes to the blob store (testCfg threshold = 100).
			{Name: "data.bin", Size: 2048, HashLow: 7, HashHigh: 7},
			{Name: "columns.txt", Size: 8, HashLow: 8, HashHigh: 8},
		},
	}}
	src := testfixtures.Build(t, parts)
	if _, err := cas.Upload(ctx, f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
		t.Fatal(err)
	}

	target := cas.BlobPath(cfg.ClusterPrefix(), cas.Hash128{Low: 7, High: 7})
	sb := &stallingBackend{Backend: f, failKey: target}

	var out bytes.Buffer
	res, err := cas.Verify(ctx, sb, cfg, "bk", cas.VerifyOptions{}, &out)
	if !errors.Is(err, cas.ErrVerifyFailures) {
		t.Fatalf("expected ErrVerifyFailures, got %v", err)
	}
	if len(res.Failures) != 1 {
		t.Fatalf("got %d failures, want 1: %+v", len(res.Failures), res.Failures)
	}
	f0 := res.Failures[0]
	if f0.Kind != "stat_error" {
		t.Errorf("Kind: got %q want \"stat_error\" (NOT \"missing\" — that would mislead operators)", f0.Kind)
	}
	if f0.Path != target {
		t.Errorf("Path: got %q want %q", f0.Path, target)
	}
	if f0.Err == "" {
		t.Error("Err: should carry the underlying StatFile error message")
	}
}

func TestVerify_RefusesV1Backup(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	cp := cfg.ClusterPrefix()

	// Write a metadata.json without a CAS field (v1 backup).
	v1meta := `{"backup_name":"b1","tables":[],"data_format":"directory"}`
	if err := f.PutFile(context.Background(), cas.MetadataJSONPath(cp, "b1"),
		io.NopCloser(strings.NewReader(v1meta)), int64(len(v1meta))); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	var out bytes.Buffer
	_, err := cas.Verify(context.Background(), f, cfg, "b1", cas.VerifyOptions{}, &out)
	if !errors.Is(err, cas.ErrV1Backup) {
		t.Fatalf("Verify err=%v; want ErrV1Backup", err)
	}
}
