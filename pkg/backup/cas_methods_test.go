package backup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotObjectDiskHits_EmptyBackup(t *testing.T) {
	tmp := t.TempDir()
	// No shadow/ dir at all.
	b := &Backuper{}
	hits, err := b.snapshotObjectDiskHitsFromDisks(tmp, map[string]string{
		"default": "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Errorf("got %d hits, want 0", len(hits))
	}
}

func TestSnapshotObjectDiskHits_FindsObjectDisk(t *testing.T) {
	tmp := t.TempDir()
	// Construct shadow/db1/t1/{default,s3main}/all_1_1_0/
	for _, disk := range []string{"default", "s3main"} {
		p := filepath.Join(tmp, "shadow", "db1", "t1", disk, "all_1_1_0")
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	b := &Backuper{}
	hits, err := b.snapshotObjectDiskHitsFromDisks(tmp, map[string]string{
		"default": "local",
		"s3main":  "s3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("got %d hits, want 1: %+v", len(hits), hits)
	}
	if hits[0].Disk != "s3main" || hits[0].DiskType != "s3" {
		t.Errorf("hit: got %+v want s3main/s3", hits[0])
	}
}

func TestSnapshotObjectDiskHits_DedupesSameTriple(t *testing.T) {
	tmp := t.TempDir()
	// Same disk under two parts.
	for _, part := range []string{"all_1_1_0", "all_2_2_0"} {
		p := filepath.Join(tmp, "shadow", "db", "t", "s3", part)
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	b := &Backuper{}
	hits, _ := b.snapshotObjectDiskHitsFromDisks(tmp, map[string]string{"s3": "s3"})
	if len(hits) != 1 {
		t.Fatalf("got %d hits, want 1 (deduped): %+v", len(hits), hits)
	}
}

func TestSnapshotObjectDiskHits_UnknownDiskSkipped(t *testing.T) {
	tmp := t.TempDir()
	// Disk "mystery" not in diskTypeByName — should be treated as local (skipped).
	p := filepath.Join(tmp, "shadow", "db", "t", "mystery", "all_1_1_0")
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatal(err)
	}
	b := &Backuper{}
	hits, err := b.snapshotObjectDiskHitsFromDisks(tmp, map[string]string{
		"default": "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Errorf("got %d hits for unknown disk, want 0", len(hits))
	}
}

func TestSnapshotObjectDiskHits_MultipleTablesMultipleDisks(t *testing.T) {
	tmp := t.TempDir()
	// db1.t1 on s3a; db1.t2 on local; db2.t3 on azure
	dirs := []string{
		filepath.Join(tmp, "shadow", "db1", "t1", "s3a", "all_1_1_0"),
		filepath.Join(tmp, "shadow", "db1", "t2", "default", "all_1_1_0"),
		filepath.Join(tmp, "shadow", "db2", "t3", "azuredisk", "all_1_1_0"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	b := &Backuper{}
	hits, err := b.snapshotObjectDiskHitsFromDisks(tmp, map[string]string{
		"default":   "local",
		"s3a":       "s3",
		"azuredisk": "azure_blob_storage",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 {
		t.Fatalf("got %d hits, want 2: %+v", len(hits), hits)
	}
}
