package part_checksum

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestSipHash128_Deterministic guards against accidental refactors that would
// change the SipHash128 output. Values produced by this implementation; the
// integration test cross-checks against live system.parts.hash_of_all_files.
func TestSipHash128_Deterministic(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", "f711edcba8b6b5e5e983a656dbc1b532"},
	}
	for _, c := range cases {
		h := NewSipHash128()
		h.Write([]byte(c.in))
		lo, hi := h.Sum128()
		got := fmt.Sprintf("%016x%016x", hi, lo)
		if got != c.want {
			t.Errorf("SipHash128(%q): got %s want %s", c.in, got, c.want)
		}
	}
}

// TestComputeHashOfAllFiles_v2 round-trips a small v2 checksums.txt and
// compares to a value we compute manually using the same algorithm.
func TestComputeHashOfAllFiles_v2(t *testing.T) {
	files := []FileChecksum{
		{Name: "a.bin", FileSize: 4, FileHash: Hash128{Low64: 0x1122334455667788, High64: 0x99aabbccddeeff00}},
		{Name: "z.bin", FileSize: 9, FileHash: Hash128{Low64: 1, High64: 2}},
		{Name: "m.bin", FileSize: 1, FileHash: Hash128{Low64: 0xdeadbeef, High64: 0xcafef00d}},
	}
	got := computeHashOfAllFiles(append([]FileChecksum(nil), files...))

	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	state := NewSipHash128()
	var buf [8]byte
	for _, f := range files {
		binary.LittleEndian.PutUint64(buf[:], uint64(len(f.Name)))
		state.Write(buf[:])
		state.Write([]byte(f.Name))
		binary.LittleEndian.PutUint64(buf[:], f.FileHash.Low64)
		state.Write(buf[:])
		binary.LittleEndian.PutUint64(buf[:], f.FileHash.High64)
		state.Write(buf[:])
	}
	lo, hi := state.Sum128()
	want := fmt.Sprintf("%016x%016x", hi, lo)
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

// TestParseV5 builds a v5 minimalistic checksums.txt by hand and checks we
// extract the embedded hash_of_all_files correctly.
func TestParseV5(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	buf.WriteString("checksums format version: 5\n")
	tmp := make([]byte, binary.MaxVarintLen64)
	buf.Write(tmp[:binary.PutUvarint(tmp, 7)]) // num_compressed_files
	buf.Write(tmp[:binary.PutUvarint(tmp, 5)]) // num_uncompressed_files
	hashBytes := make([]byte, 16)
	binary.LittleEndian.PutUint64(hashBytes[0:8], 0x0123456789abcdef)
	binary.LittleEndian.PutUint64(hashBytes[8:16], 0xfedcba9876543210)
	buf.Write(hashBytes)        // hash_of_all_files
	buf.Write(make([]byte, 16)) // hash_of_uncompressed_files (ignored)
	buf.Write(make([]byte, 16)) // uncompressed_hash_of_compressed_files (ignored)

	if err := os.WriteFile(filepath.Join(dir, "checksums.txt"), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := HashOfAllFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := "fedcba98765432100123456789abcdef"
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

// TestParseV3RoundTrip builds a tiny v3 checksums.txt and verifies parsing
// produces the same file entries as we encoded.
func TestParseV3RoundTrip(t *testing.T) {
	dir := t.TempDir()
	files := []FileChecksum{
		{Name: "data.bin", FileSize: 100, FileHash: Hash128{Low64: 0xaaaa, High64: 0xbbbb},
			IsCompressed: true, UncompressedSize: 250, UncompressedHash: Hash128{Low64: 0xcccc, High64: 0xdddd}},
		{Name: "primary.idx", FileSize: 16, FileHash: Hash128{Low64: 0x1111, High64: 0x2222}, IsCompressed: false},
	}
	var buf bytes.Buffer
	buf.WriteString("checksums format version: 3\n")
	varbuf := make([]byte, binary.MaxVarintLen64)
	buf.Write(varbuf[:binary.PutUvarint(varbuf, uint64(len(files)))])
	for _, f := range files {
		buf.Write(varbuf[:binary.PutUvarint(varbuf, uint64(len(f.Name)))])
		buf.WriteString(f.Name)
		buf.Write(varbuf[:binary.PutUvarint(varbuf, f.FileSize)])
		hb := make([]byte, 16)
		binary.LittleEndian.PutUint64(hb[0:8], f.FileHash.Low64)
		binary.LittleEndian.PutUint64(hb[8:16], f.FileHash.High64)
		buf.Write(hb)
		if f.IsCompressed {
			buf.WriteByte(1)
			buf.Write(varbuf[:binary.PutUvarint(varbuf, f.UncompressedSize)])
			binary.LittleEndian.PutUint64(hb[0:8], f.UncompressedHash.Low64)
			binary.LittleEndian.PutUint64(hb[8:16], f.UncompressedHash.High64)
			buf.Write(hb)
		} else {
			buf.WriteByte(0)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "checksums.txt"), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := HashOfAllFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := computeHashOfAllFiles(append([]FileChecksum(nil), files...))
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}
