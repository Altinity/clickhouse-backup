package testfixtures

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/checksumstxt"
)

func TestBuild_OnePart_ChecksumsRoundTrip(t *testing.T) {
	parts := []PartSpec{{
		Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
		Files: []FileSpec{
			{Name: "columns.txt", Size: 23, HashLow: 100, HashHigh: 200},
			{Name: "primary.idx", Size: 8, HashLow: 300, HashHigh: 400},
			{Name: "data.bin", Size: 1024, HashLow: 500, HashHigh: 600},
		},
	}}
	lb := Build(t, parts)

	ckPath := filepath.Join(lb.Root, "shadow", "db1", "t1", "default", "all_1_1_0", "checksums.txt")
	f, err := os.Open(ckPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	got, err := checksumstxt.Parse(f)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.Version != 2 {
		t.Errorf("version: got %d want 2", got.Version)
	}
	if len(got.Files) != 3 {
		t.Fatalf("files count: got %d want 3", len(got.Files))
	}
	for _, want := range parts[0].Files {
		gc, ok := got.Files[want.Name]
		if !ok {
			t.Errorf("file %q missing from parsed checksums", want.Name)
			continue
		}
		if gc.FileSize != want.Size {
			t.Errorf("%s size: got %d want %d", want.Name, gc.FileSize, want.Size)
		}
		if gc.FileHash.Low != want.HashLow || gc.FileHash.High != want.HashHigh {
			t.Errorf("%s hash: got (%d,%d) want (%d,%d)", want.Name,
				gc.FileHash.Low, gc.FileHash.High, want.HashLow, want.HashHigh)
		}
		// Verify file bytes were written with the claimed size.
		fp := filepath.Join(lb.Root, "shadow", "db1", "t1", "default", "all_1_1_0", want.Name)
		st, err := os.Stat(fp)
		if err != nil {
			t.Errorf("stat %s: %v", fp, err)
			continue
		}
		if uint64(st.Size()) != want.Size {
			t.Errorf("%s on-disk size: got %d want %d", want.Name, st.Size(), want.Size)
		}
	}
}

func TestBuild_PartsIndexed(t *testing.T) {
	parts := []PartSpec{
		{Disk: "default", DB: "db1", Table: "t1", Name: "p1", Files: []FileSpec{{Name: "columns.txt", Size: 5, HashLow: 1, HashHigh: 2}}},
		{Disk: "default", DB: "db1", Table: "t1", Name: "p2", Files: []FileSpec{{Name: "columns.txt", Size: 5, HashLow: 3, HashHigh: 4}}},
		{Disk: "fast", DB: "db1", Table: "t2", Name: "p1", Files: []FileSpec{{Name: "columns.txt", Size: 5, HashLow: 5, HashHigh: 6}}},
	}
	lb := Build(t, parts)
	if got, want := len(lb.Parts["default:db1.t1"]), 2; got != want {
		t.Errorf("default:db1.t1 parts: got %d want %d", got, want)
	}
	if got, want := len(lb.Parts["fast:db1.t2"]), 1; got != want {
		t.Errorf("fast:db1.t2 parts: got %d want %d", got, want)
	}
}
