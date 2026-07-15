package cas_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/testfixtures"
)

func TestListRemoteCAS_FindsBackups(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	ctx := context.Background()

	src := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 0)})
	if _, err := cas.Upload(ctx, f, cfg, "bk1", cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
		t.Fatalf("upload bk1: %v", err)
	}
	if _, err := cas.Upload(ctx, f, cfg, "bk2", cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
		t.Fatalf("upload bk2: %v", err)
	}
	entries, err := cas.ListRemoteCAS(ctx, f, cfg)
	if err != nil {
		t.Fatalf("ListRemoteCAS: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(entries), entries)
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
		if e.Description != "[CAS]" {
			t.Errorf("entry %q description = %q, want %q", e.Name, e.Description, "[CAS]")
		}
		if e.UploadedAt.IsZero() {
			t.Errorf("entry %q has zero UploadedAt", e.Name)
		}
	}
	if !names["bk1"] || !names["bk2"] {
		t.Errorf("missing expected names, got %+v", names)
	}
}

func TestListRemoteCAS_DisabledReturnsNil(t *testing.T) {
	cfg := testCfg(100)
	cfg.Enabled = false
	entries, err := cas.ListRemoteCAS(context.Background(), fakedst.New(), cfg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if entries != nil {
		t.Fatalf("want nil, got %+v", entries)
	}
}

func TestListRemoteCAS_IgnoresNestedMetadataJSON(t *testing.T) {
	// table-level metadata files live deeper than <backup>/metadata.json,
	// so they must not show up as backup roots.
	f := fakedst.New()
	cfg := testCfg(100)
	ctx := context.Background()

	src := testfixtures.Build(t, []testfixtures.PartSpec{smallPart("p1", 0)})
	if _, err := cas.Upload(ctx, f, cfg, "only", cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
		t.Fatalf("upload: %v", err)
	}
	entries, err := cas.ListRemoteCAS(ctx, f, cfg)
	if err != nil {
		t.Fatalf("ListRemoteCAS: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1: %+v", len(entries), entries)
	}
	if entries[0].Name != "only" {
		t.Errorf("name: got %q want %q", entries[0].Name, "only")
	}
}

func TestListRemoteCAS_BrokenMetadataIsSurfacedNotDropped(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(100)
	ctx := context.Background()

	cp := cfg.ClusterPrefix()
	bad := cas.MetadataJSONPath(cp, "broken")
	body := []byte("{this is not json")
	if err := f.PutFile(ctx, bad, io.NopCloser(bytes.NewReader(body)), int64(len(body))); err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	entries, err := cas.ListRemoteCAS(ctx, f, cfg)
	if err != nil {
		t.Fatalf("ListRemoteCAS: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if entries[0].Name != "broken" {
		t.Errorf("name: got %q", entries[0].Name)
	}
	if entries[0].Description == "[CAS]" {
		t.Errorf("expected broken description, got %q", entries[0].Description)
	}
}
