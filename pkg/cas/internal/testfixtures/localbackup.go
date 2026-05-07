// Package testfixtures provides helpers for synthesizing a "fake local
// backup directory" tree that mirrors what `clickhouse-backup create`
// produces, so tests can drive the CAS upload path without a live
// ClickHouse instance.
package testfixtures

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

// LocalBackup describes the synthesized backup-on-disk layout returned
// by Build.
type LocalBackup struct {
	// Root is the absolute path of the synthesized backup directory.
	Root string
	// Parts indexes the original PartSpec slices used to build the layout,
	// keyed by "disk:db.table" for easy lookup in tests.
	Parts map[string][]PartSpec
}

// PartSpec describes one MergeTree-style part to materialize on disk.
type PartSpec struct {
	Disk, DB, Table, Name string
	Files                 []FileSpec // every file the part contains, including any "checksums.txt"-listed files
	// TableMeta is optional. When zero-value, Build still writes a minimal
	// v1 metadata/<db>/<table>.json so cas-upload's merge logic has
	// something to read.
	TableMeta metadata.TableMetadata
}

// FileSpec describes one file inside a part.
//
// Bytes is optional: if non-nil the bytes are written verbatim; otherwise
// Build synthesizes Size deterministic pseudo-bytes based on Name. The
// CAS upload path trusts checksums.txt — the actual file bytes do not
// need to hash to (HashLow, HashHigh).
type FileSpec struct {
	Name     string
	Size     uint64
	HashLow  uint64
	HashHigh uint64
	Bytes    []byte
}

// Build creates a temp directory tree for the given parts and returns
// the resulting LocalBackup. checksums.txt is always written last for
// each part with the v2 text format listing every other file.
//
// The layout is shadow-only:
//
//	<root>/shadow/<db>/<table>/<disk>/<part>/<file>
//
// (db and table are written verbatim, NOT TablePathEncode'd; Upload
// re-encodes when computing remote keys. Tests should pick names that
// don't collide with separator characters.)
func Build(t *testing.T, parts []PartSpec) *LocalBackup {
	t.Helper()
	root := t.TempDir()
	lb := &LocalBackup{
		Root:  root,
		Parts: make(map[string][]PartSpec),
	}
	for _, p := range parts {
		key := p.Disk + ":" + p.DB + "." + p.Table
		lb.Parts[key] = append(lb.Parts[key], p)
		partDir := filepath.Join(root, "shadow", p.DB, p.Table, p.Disk, p.Name)
		if err := os.MkdirAll(partDir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", partDir, err)
		}

		// Write every "real" file first.
		var listed []FileSpec
		for _, f := range p.Files {
			if f.Name == "checksums.txt" {
				// If caller provides a checksums.txt entry we ignore its
				// bytes and synthesize the v2 file ourselves; we still
				// include it in the listed set so it appears in the body
				// (callers can include it intentionally).
				continue
			}
			listed = append(listed, f)
			data := f.Bytes
			if data == nil {
				data = synthBytes(f.Name, f.Size)
			}
			if uint64(len(data)) != f.Size {
				t.Fatalf("file %q: bytes length %d != size %d", f.Name, len(data), f.Size)
			}
			fp := filepath.Join(partDir, f.Name)
			if err := os.MkdirAll(filepath.Dir(fp), 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", filepath.Dir(fp), err)
			}
			if err := os.WriteFile(fp, data, 0o644); err != nil {
				t.Fatalf("write %s: %v", fp, err)
			}
		}

		// Synthesize checksums.txt last.
		ck := buildChecksumsV2(listed)
		ckPath := filepath.Join(partDir, "checksums.txt")
		if err := os.WriteFile(ckPath, []byte(ck), 0o644); err != nil {
			t.Fatalf("write %s: %v", ckPath, err)
		}
	}

	// Write one v1-style metadata/<db>/<table>.json per (db, table). Mimics
	// what `clickhouse-backup create` writes; cas-upload merges the schema
	// fields from these files into the uploaded TableMetadata.
	seen := map[string]bool{}
	for _, p := range parts {
		key := p.DB + "." + p.Table
		if seen[key] {
			continue
		}
		seen[key] = true

		tm := p.TableMeta
		if tm.Database == "" {
			tm.Database = p.DB
		}
		if tm.Table == "" {
			tm.Table = p.Table
		}
		if tm.Query == "" {
			tm.Query = "CREATE TABLE " + p.DB + "." + p.Table + " (id UInt64) ENGINE=MergeTree ORDER BY id"
		}
		if tm.UUID == "" {
			tm.UUID = "00000000-0000-0000-0000-000000000000"
		}

		metaDir := filepath.Join(root, "metadata", p.DB)
		if err := os.MkdirAll(metaDir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", metaDir, err)
		}
		body, err := json.MarshalIndent(&tm, "", "\t")
		if err != nil {
			t.Fatalf("marshal table metadata %s.%s: %v", p.DB, p.Table, err)
		}
		metaPath := filepath.Join(metaDir, p.Table+".json")
		if err := os.WriteFile(metaPath, body, 0o644); err != nil {
			t.Fatalf("write %s: %v", metaPath, err)
		}
	}
	return lb
}

// buildChecksumsV2 emits a v2 text-format checksums.txt body for the
// given files. None of the files are marked compressed.
func buildChecksumsV2(files []FileSpec) string {
	var b strings.Builder
	b.WriteString("checksums format version: 2\n")
	fmt.Fprintf(&b, "%d files:\n", len(files))
	for _, f := range files {
		b.WriteString(f.Name)
		b.WriteByte('\n')
		fmt.Fprintf(&b, "\tsize: %d\n", f.Size)
		fmt.Fprintf(&b, "\thash: %d %d\n", f.HashLow, f.HashHigh)
		b.WriteString("\tcompressed: 0\n")
	}
	return b.String()
}

// synthBytes returns a deterministic pseudo-random byte slice of the
// requested size, seeded by name. We don't need cryptographic quality —
// just stable bytes that tests can predict if they need to.
func synthBytes(name string, size uint64) []byte {
	out := make([]byte, size)
	// Cheap LCG seeded from the name's bytes.
	var seed uint64 = 1469598103934665603 // FNV offset basis-ish
	for i := 0; i < len(name); i++ {
		seed = seed*1099511628211 ^ uint64(name[i])
	}
	for i := uint64(0); i < size; i++ {
		seed = seed*6364136223846793005 + 1442695040888963407
		out[i] = byte(seed >> 56)
	}
	return out
}
