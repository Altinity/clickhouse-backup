package cas_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

func cfg(t *testing.T) cas.Config {
	t.Helper()
	c := cas.DefaultConfig()
	c.Enabled = true
	c.ClusterID = "c1"
	return c
}

func putMetadata(t *testing.T, f *fakedst.Fake, cp, name string, bm metadata.BackupMetadata) {
	t.Helper()
	raw, err := json.Marshal(bm)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.PutFile(context.Background(), cas.MetadataJSONPath(cp, name),
		io.NopCloser(strings.NewReader(string(raw))), int64(len(raw))); err != nil {
		t.Fatal(err)
	}
}

func validBM() metadata.BackupMetadata {
	return metadata.BackupMetadata{
		BackupName: "bk1",
		CAS: &metadata.CASBackupParams{
			LayoutVersion:   cas.LayoutVersion,
			InlineThreshold: 524288,
			ClusterID:       "c1",
		},
	}
}

func TestValidateBackup_HappyPath(t *testing.T) {
	f := fakedst.New()
	ctx := context.Background()
	c := cfg(t)
	putMetadata(t, f, c.ClusterPrefix(), "bk1", validBM())
	bm, err := cas.ValidateBackup(ctx, f, c, "bk1")
	if err != nil {
		t.Fatal(err)
	}
	if bm.CAS == nil || bm.CAS.ClusterID != "c1" {
		t.Fatalf("wrong meta: %+v", bm)
	}
}

func TestValidateBackup_RejectsBadNames(t *testing.T) {
	f := fakedst.New()
	c := cfg(t)
	ctx := context.Background()
	for _, bad := range []string{"", strings.Repeat("a", 129), "../sneaky", "with space", "name/slash", "tab\tname", ".", "..", "..."} {
		if _, err := cas.ValidateBackup(ctx, f, c, bad); !errors.Is(err, cas.ErrInvalidBackupName) {
			t.Errorf("name=%q: want ErrInvalidBackupName, got %v", bad, err)
		}
	}
}

func TestValidateBackup_MissingMetadata(t *testing.T) {
	f := fakedst.New()
	c := cfg(t)
	ctx := context.Background()
	_, err := cas.ValidateBackup(ctx, f, c, "absent")
	if !errors.Is(err, cas.ErrMissingMetadata) {
		t.Fatalf("want ErrMissingMetadata, got %v", err)
	}
}

func TestValidateBackup_V1Backup(t *testing.T) {
	f := fakedst.New()
	c := cfg(t)
	ctx := context.Background()
	bm := validBM()
	bm.CAS = nil // v1 backup
	putMetadata(t, f, c.ClusterPrefix(), "bk1", bm)
	_, err := cas.ValidateBackup(ctx, f, c, "bk1")
	if !errors.Is(err, cas.ErrV1Backup) {
		t.Fatalf("want ErrV1Backup, got %v", err)
	}
}

func TestValidateBackup_UnsupportedLayoutVersion(t *testing.T) {
	f := fakedst.New()
	c := cfg(t)
	ctx := context.Background()
	bm := validBM()
	bm.CAS.LayoutVersion = cas.LayoutVersion + 1
	putMetadata(t, f, c.ClusterPrefix(), "bk1", bm)
	_, err := cas.ValidateBackup(ctx, f, c, "bk1")
	if !errors.Is(err, cas.ErrUnsupportedLayoutVersion) {
		t.Fatalf("got %v", err)
	}
}

func TestValidateBackup_BadInlineThreshold(t *testing.T) {
	f := fakedst.New()
	c := cfg(t)
	ctx := context.Background()
	bm := validBM()
	bm.CAS.InlineThreshold = 0
	putMetadata(t, f, c.ClusterPrefix(), "z", bm)
	if _, err := cas.ValidateBackup(ctx, f, c, "z"); err == nil {
		t.Fatal("zero must fail")
	}

	bm.CAS.InlineThreshold = cas.MaxInline + 1
	putMetadata(t, f, c.ClusterPrefix(), "z", bm)
	if _, err := cas.ValidateBackup(ctx, f, c, "z"); err == nil {
		t.Fatal("> MaxInline must fail")
	}
}

func TestValidateBackup_ClusterIDMismatch(t *testing.T) {
	f := fakedst.New()
	c := cfg(t)
	ctx := context.Background()
	bm := validBM()
	bm.CAS.ClusterID = "other-cluster"
	putMetadata(t, f, c.ClusterPrefix(), "bk1", bm)
	_, err := cas.ValidateBackup(ctx, f, c, "bk1")
	if !errors.Is(err, cas.ErrClusterIDMismatch) {
		t.Fatalf("want ErrClusterIDMismatch, got %v", err)
	}
}

func TestValidateBackup_UnparseableJSON(t *testing.T) {
	f := fakedst.New()
	c := cfg(t)
	ctx := context.Background()
	cp := c.ClusterPrefix()
	if err := f.PutFile(ctx, cas.MetadataJSONPath(cp, "bk1"),
		io.NopCloser(strings.NewReader("not json")), 8); err != nil {
		t.Fatal(err)
	}
	_, err := cas.ValidateBackup(ctx, f, c, "bk1")
	if err == nil {
		t.Fatal("must fail")
	}
	if !strings.Contains(err.Error(), "parse metadata.json") {
		t.Errorf("error should mention parse step: %v", err)
	}
}
