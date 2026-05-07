package backup

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
)

// TestCASRestore_PidlockRegression encodes the contract that the cas-restore
// path must not double-acquire the per-backup pidlock. Before the fix,
// CASRestore took the lock and then b.Restore re-acquired it, deadlocking on
// Linux because pidlock has no same-PID exemption (verified by Test below).
//
// We can't easily exercise the full CASRestore stack in a unit test (needs
// ClickHouse + storage), so this test pins the invariant directly: the
// CheckAndCreatePidFile semantics that would catch a regression.
func TestCASRestore_PidlockHasNoSamePIDExemption(t *testing.T) {
	// Use a unique name so we don't collide with any leftover pidfile.
	name := "cas_test_pidlock_regression"
	if err := pidlock.CheckAndCreatePidFile(name, "outer-test"); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	defer pidlock.RemovePidFile(name)

	// Second acquire in the same process MUST fail. If pidlock ever grew a
	// same-PID exemption, this test breaks and the comment in cas_methods.go
	// (about why we removed the outer pidlock from CASRestore) becomes
	// outdated — re-evaluate at that point.
	err := pidlock.CheckAndCreatePidFile(name, "inner-test")
	if err == nil {
		// Roll back the second acquire so we don't leave state behind.
		pidlock.RemovePidFile(name)
		t.Fatal("expected second pidlock acquire in same process to fail; pidlock semantics changed — re-evaluate cas-restore double-lock comment")
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Errorf("expected 'already running' in error, got: %v", err)
	}
}

func TestSplitTablePattern(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"db.t", []string{"db.t"}},
		{"db1.t1,db2.t2", []string{"db1.t1", "db2.t2"}},
		{"db1.t1, db2.t2", []string{"db1.t1", "db2.t2"}},
		{"  db.t  ", []string{"db.t"}},
		{",,", nil},
	}
	for _, c := range cases {
		got := splitTablePattern(c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("splitTablePattern(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestEnsureCAS_RefusesWhenDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.CAS.Enabled = false
	b := &Backuper{cfg: cfg}
	_, _, err := b.ensureCAS(context.Background(), "anyname")
	if err == nil {
		t.Fatal("expected refusal when cas.enabled=false")
	}
	if !strings.Contains(err.Error(), "cas.enabled=false") {
		t.Errorf("error should mention cas.enabled=false, got: %v", err)
	}
}

func TestEnsureCAS_RefusesUnsupportedRemoteStorage(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.CAS.Enabled = true
	cfg.CAS.ClusterID = "c1"
	cfg.General.RemoteStorage = "none"
	b := &Backuper{cfg: cfg}
	_, _, err := b.ensureCAS(context.Background(), "anyname")
	if err == nil || !strings.Contains(err.Error(), "remote_storage") {
		t.Errorf("expected remote_storage error, got: %v", err)
	}
}

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
