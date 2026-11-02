package cas_test

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/klauspost/compress/zstd"
)

// makeTestPart creates a temp source dir with two small files.
func makeTestPart(t *testing.T) (root string, columns []byte, checksums []byte) {
	t.Helper()
	root = t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "all_1_1_0"), 0o755); err != nil {
		t.Fatal(err)
	}
	columns = []byte("id UInt64\nx String\n")
	checksums = []byte("checksums format version: 4\n...some-blob...")
	if err := os.WriteFile(filepath.Join(root, "all_1_1_0", "columns.txt"), columns, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "all_1_1_0", "checksums.txt"), checksums, 0o644); err != nil {
		t.Fatal(err)
	}
	return root, columns, checksums
}

func TestArchive_RoundTrip(t *testing.T) {
	src, wantCols, wantChk := makeTestPart(t)
	var buf bytes.Buffer
	err := cas.WriteArchive(&buf, []cas.ArchiveEntry{
		{NameInArchive: "all_1_1_0/columns.txt", LocalPath: filepath.Join(src, "all_1_1_0", "columns.txt")},
		{NameInArchive: "all_1_1_0/checksums.txt", LocalPath: filepath.Join(src, "all_1_1_0", "checksums.txt")},
	})
	if err != nil {
		t.Fatal(err)
	}

	out := t.TempDir()
	if err := cas.ExtractArchive(&buf, out); err != nil {
		t.Fatal(err)
	}

	gotCols, _ := os.ReadFile(filepath.Join(out, "all_1_1_0", "columns.txt"))
	gotChk, _ := os.ReadFile(filepath.Join(out, "all_1_1_0", "checksums.txt"))
	if !bytes.Equal(gotCols, wantCols) {
		t.Errorf("columns.txt mismatch")
	}
	if !bytes.Equal(gotChk, wantChk) {
		t.Errorf("checksums.txt mismatch")
	}
}

// craftHostileTar emits a single-entry tar.zst whose tar entry has the given
// name. Bypasses WriteArchive's name validation so we can test ExtractArchive
// in isolation.
func craftHostileTar(t *testing.T, name string, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, _ := zstd.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	if err := tw.WriteHeader(&tar.Header{Name: name, Size: int64(len(data)), Mode: 0o644, Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestArchive_ExtractRejectsTraversal(t *testing.T) {
	blob := craftHostileTar(t, "../escape.txt", []byte("x"))
	err := cas.ExtractArchive(bytes.NewReader(blob), t.TempDir())
	var ue *cas.UnsafePathError
	if !errors.As(err, &ue) {
		t.Fatalf("want UnsafePathError, got %T %v", err, err)
	}
}

func TestArchive_ExtractRejectsAbsolute(t *testing.T) {
	blob := craftHostileTar(t, "/etc/passwd", []byte("x"))
	err := cas.ExtractArchive(bytes.NewReader(blob), t.TempDir())
	var ue *cas.UnsafePathError
	if !errors.As(err, &ue) {
		t.Fatal("absolute path must be rejected")
	}
}

func TestArchive_ExtractRejectsEmbeddedNUL(t *testing.T) {
	// Go's tar.Reader parses ustar name fields as C strings (NUL-terminated),
	// so a NUL injected into the raw ustar header bytes is silently truncated
	// before the name ever reaches validateArchiveName. The NUL attack vector
	// via ustar-format tar does not exist on Go's reader.
	//
	// We test that WriteArchive itself (the entry point we control) rejects a
	// NUL-containing NameInArchive before writing anything.
	err := cas.WriteArchive(io.Discard, []cas.ArchiveEntry{
		{NameInArchive: "ok\x00bad", LocalPath: "/etc/hostname"},
	})
	var ue *cas.UnsafePathError
	if !errors.As(err, &ue) {
		t.Fatalf("NUL in NameInArchive must be rejected by WriteArchive, got %T %v", err, err)
	}
}

func TestArchive_ExtractRejectsNonRegular(t *testing.T) {
	var buf bytes.Buffer
	zw, _ := zstd.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	_ = tw.WriteHeader(&tar.Header{Name: "link", Linkname: "/etc/passwd", Typeflag: tar.TypeSymlink})
	_ = tw.Close()
	_ = zw.Close()
	err := cas.ExtractArchive(&buf, t.TempDir())
	if err == nil {
		t.Fatal("symlink entry must be rejected")
	}
}

func TestArchive_WriteRejectsBadName(t *testing.T) {
	err := cas.WriteArchive(io.Discard, []cas.ArchiveEntry{{NameInArchive: "../escape", LocalPath: "/etc/hostname"}})
	var ue *cas.UnsafePathError
	if !errors.As(err, &ue) {
		t.Fatal("WriteArchive must reject bad NameInArchive")
	}
}

func TestArchive_WriteRejectsDuplicateNames(t *testing.T) {
	src, _, _ := makeTestPart(t)
	err := cas.WriteArchive(io.Discard, []cas.ArchiveEntry{
		{NameInArchive: "x", LocalPath: filepath.Join(src, "all_1_1_0", "columns.txt")},
		{NameInArchive: "x", LocalPath: filepath.Join(src, "all_1_1_0", "checksums.txt")},
	})
	if err == nil {
		t.Fatal("duplicate names must be rejected")
	}
}
