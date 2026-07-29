package cas

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCollectBlobJobsRecursive_RejectsTraversalProjEntry verifies the download
// recursion rejects a maliciously-crafted .proj entry whose name contains "..".
// Without the validator hoist, the directory traversal would succeed during
// the recursive blob discovery. This is a security regression test for T6.
func TestCollectBlobJobsRecursive_RejectsTraversalProjEntry(t *testing.T) {
	// Synthesize a minimal in-memory checksums.txt with a malicious .proj entry
	// containing "..". The collectBlobJobsRecursive function should reject it
	// at the validateChecksumsTxtFilename stage before attempting filepath.Join.
	tmp := t.TempDir()
	ckPath := filepath.Join(tmp, "checksums.txt")
	// v2 checksums format with one bad .proj entry that contains ".."
	body := `checksums format version: 2
1 files:
../escape.proj	size: 100	hash: 1 2	compressed: 0
`
	if err := os.WriteFile(ckPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	var blobs []blobJob
	var est int64
	err := collectBlobJobsRecursive(tmp, 1024, &blobs, &est)
	if err == nil {
		t.Fatal("expected error for ..-containing .proj entry")
	}
	// validateChecksumsTxtFilename's error message should fire.
	t.Logf("got expected error: %v", err)
}

// TestCollectBlobJobsRecursive_RejectsTraversalFilename verifies the download
// recursion also rejects malicious non-.proj filenames containing "..".
func TestCollectBlobJobsRecursive_RejectsTraversalFilename(t *testing.T) {
	tmp := t.TempDir()
	ckPath := filepath.Join(tmp, "checksums.txt")
	// v2 checksums format with one bad non-.proj entry containing ".."
	body := `checksums format version: 2
1 files:
../escape.bin	size: 100	hash: 1 2	compressed: 0
`
	if err := os.WriteFile(ckPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	var blobs []blobJob
	var est int64
	err := collectBlobJobsRecursive(tmp, 1024, &blobs, &est)
	if err == nil {
		t.Fatal("expected error for ..-containing non-.proj entry")
	}
	t.Logf("got expected error: %v", err)
}

// TestCollectBlobJobsRecursive_RejectsAbsolutePath verifies that absolute
// paths in checksums.txt are rejected.
func TestCollectBlobJobsRecursive_RejectsAbsolutePath(t *testing.T) {
	tmp := t.TempDir()
	ckPath := filepath.Join(tmp, "checksums.txt")
	body := `checksums format version: 2
1 files:
/etc/passwd	size: 100	hash: 1 2	compressed: 0
`
	if err := os.WriteFile(ckPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	var blobs []blobJob
	var est int64
	err := collectBlobJobsRecursive(tmp, 1024, &blobs, &est)
	if err == nil {
		t.Fatal("expected error for absolute path entry")
	}
	t.Logf("got expected error: %v", err)
}
