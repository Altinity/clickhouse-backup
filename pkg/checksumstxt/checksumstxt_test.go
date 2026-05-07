package checksumstxt

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/compress"
)

// uvar appends a LEB128-encoded uint64.
func uvar(dst []byte, x uint64) []byte {
	for x >= 0x80 {
		dst = append(dst, byte(x)|0x80)
		x >>= 7
	}
	return append(dst, byte(x))
}

// strBin appends a length-prefixed string in ClickHouse binary format.
func strBin(dst []byte, s string) []byte {
	dst = uvar(dst, uint64(len(s)))
	return append(dst, s...)
}

func u128LE(low, high uint64) []byte {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[:8], low)
	binary.LittleEndian.PutUint64(b[8:], high)
	return b[:]
}

// buildV3Body returns the inner body (after the version line) for a v3/v4 file.
func buildV3Body(records []struct {
	name string
	c    Checksum
}) []byte {
	var b []byte
	b = uvar(b, uint64(len(records)))
	for _, r := range records {
		b = strBin(b, r.name)
		b = uvar(b, r.c.FileSize)
		b = append(b, u128LE(r.c.FileHash.Low, r.c.FileHash.High)...)
		if r.c.IsCompressed {
			b = append(b, 1)
			b = uvar(b, r.c.UncompressedSize)
			b = append(b, u128LE(r.c.UncompressedHash.Low, r.c.UncompressedHash.High)...)
		} else {
			b = append(b, 0)
		}
	}
	return b
}

func TestParseV2(t *testing.T) {
	const input = "checksums format version: 2\n" +
		"2 files:\n" +
		"columns.txt\n" +
		"\tsize: 123\n" +
		"\thash: 1 2\n" +
		"\tcompressed: 0\n" +
		"id.bin\n" +
		"\tsize: 4096\n" +
		"\thash: 100 200\n" +
		"\tcompressed: 1\n" +
		"\tuncompressed size: 8192\n" +
		"\tuncompressed hash: 300 400\n"

	f, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if f.Version != 2 || len(f.Files) != 2 {
		t.Fatalf("got version=%d files=%d", f.Version, len(f.Files))
	}
	c := f.Files["columns.txt"]
	if c.FileSize != 123 || c.FileHash != (Hash128{1, 2}) || c.IsCompressed {
		t.Errorf("columns.txt: %+v", c)
	}
	c = f.Files["id.bin"]
	want := Checksum{
		FileSize:         4096,
		FileHash:         Hash128{100, 200},
		IsCompressed:     true,
		UncompressedSize: 8192,
		UncompressedHash: Hash128{300, 400},
	}
	if c != want {
		t.Errorf("id.bin: got %+v want %+v", c, want)
	}
}

func TestParseV3(t *testing.T) {
	records := []struct {
		name string
		c    Checksum
	}{
		{"columns.txt", Checksum{FileSize: 123, FileHash: Hash128{0xAABB, 0xCCDD}}},
		{"id.bin", Checksum{
			FileSize: 4096, FileHash: Hash128{1, 2},
			IsCompressed: true, UncompressedSize: 8192, UncompressedHash: Hash128{3, 4},
		}},
	}
	body := buildV3Body(records)

	var buf bytes.Buffer
	buf.WriteString("checksums format version: 3\n")
	buf.Write(body)

	f, err := Parse(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if f.Version != 3 || len(f.Files) != 2 {
		t.Fatalf("got version=%d files=%d", f.Version, len(f.Files))
	}
	if f.Files["columns.txt"] != records[0].c {
		t.Errorf("columns.txt: %+v", f.Files["columns.txt"])
	}
	if f.Files["id.bin"] != records[1].c {
		t.Errorf("id.bin: %+v", f.Files["id.bin"])
	}
}

func TestParseV4_LZ4(t *testing.T) {
	testParseV4(t, compress.LZ4)
}

func TestParseV4_None(t *testing.T) {
	testParseV4(t, compress.None)
}

func TestParseV4_ZSTD(t *testing.T) {
	testParseV4(t, compress.ZSTD)
}

func testParseV4(t *testing.T, m compress.Method) {
	t.Helper()
	records := []struct {
		name string
		c    Checksum
	}{
		{"primary.idx", Checksum{FileSize: 64, FileHash: Hash128{0xDEADBEEF, 0xCAFEBABE}}},
		{"id.bin", Checksum{
			FileSize: 4096, FileHash: Hash128{1, 2},
			IsCompressed: true, UncompressedSize: 8192, UncompressedHash: Hash128{3, 4},
		}},
	}
	body := buildV3Body(records)

	w := compress.NewWriter(compress.LevelZero, m)
	if err := w.Compress(body); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	buf.WriteString("checksums format version: 4\n")
	buf.Write(w.Data)

	f, err := Parse(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if f.Version != 4 || len(f.Files) != 2 {
		t.Fatalf("got version=%d files=%d", f.Version, len(f.Files))
	}
	if f.Files["primary.idx"] != records[0].c || f.Files["id.bin"] != records[1].c {
		t.Errorf("mismatch: %+v", f.Files)
	}
}

func TestParseV4_MultiBlock(t *testing.T) {
	// Concatenated blocks should decompress to a single v3 body.
	body := buildV3Body([]struct {
		name string
		c    Checksum
	}{
		{"a.bin", Checksum{FileSize: 1, FileHash: Hash128{1, 1}}},
		{"b.bin", Checksum{FileSize: 2, FileHash: Hash128{2, 2}}},
	})
	// Update count to 2 (already 2 in builder). Split bytes into two halves
	// and emit each as its own block.
	half := len(body) / 2
	w1 := compress.NewWriter(compress.LevelZero, compress.LZ4)
	if err := w1.Compress(body[:half]); err != nil {
		t.Fatal(err)
	}
	w2 := compress.NewWriter(compress.LevelZero, compress.LZ4)
	if err := w2.Compress(body[half:]); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	buf.WriteString("checksums format version: 4\n")
	buf.Write(w1.Data)
	buf.Write(w2.Data)

	f, err := Parse(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Files) != 2 {
		t.Fatalf("got %d files", len(f.Files))
	}
}

func TestParseRejectsTrailingBytes(t *testing.T) {
	body := buildV3Body([]struct {
		name string
		c    Checksum
	}{{"x", Checksum{FileSize: 1, FileHash: Hash128{1, 2}}}})
	var buf bytes.Buffer
	buf.WriteString("checksums format version: 3\n")
	buf.Write(body)
	buf.WriteByte(0xFF) // junk after body

	if _, err := Parse(&buf); err == nil {
		t.Fatal("expected error for trailing bytes")
	}
}

func TestParseRejectsV1AndUnknown(t *testing.T) {
	for _, version := range []string{"1", "999"} {
		input := "checksums format version: " + version + "\n"
		if _, err := Parse(strings.NewReader(input)); err == nil {
			t.Errorf("version %s: expected error", version)
		}
	}
}

func TestParseRejectsV5(t *testing.T) {
	input := "checksums format version: 5\n"
	if _, err := Parse(strings.NewReader(input)); err == nil {
		t.Fatal("expected error: v5 must go through ParseMinimalistic")
	}
}

func TestParseMinimalistic(t *testing.T) {
	var body []byte
	body = uvar(body, 7)  // num_compressed_files
	body = uvar(body, 11) // num_uncompressed_files
	body = append(body, u128LE(0x11, 0x22)...)
	body = append(body, u128LE(0x33, 0x44)...)
	body = append(body, u128LE(0x55, 0x66)...)

	var buf bytes.Buffer
	buf.WriteString("checksums format version: 5\n")
	buf.Write(body)

	m, err := ParseMinimalistic(&buf)
	if err != nil {
		t.Fatal(err)
	}
	want := &Minimalistic{
		NumCompressedFiles:                7,
		NumUncompressedFiles:              11,
		HashOfAllFiles:                    Hash128{0x11, 0x22},
		HashOfUncompressedFiles:           Hash128{0x33, 0x44},
		UncompressedHashOfCompressedFiles: Hash128{0x55, 0x66},
	}
	if *m != *want {
		t.Errorf("got %+v want %+v", m, want)
	}
}

func TestParseMinimalisticRejectsNon5(t *testing.T) {
	if _, err := ParseMinimalistic(strings.NewReader("checksums format version: 4\n")); err == nil {
		t.Fatal("expected error")
	}
}
