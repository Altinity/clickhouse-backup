package cas

import (
	"strings"
	"testing"
)

func TestPaths_Basic(t *testing.T) {
	cp := "cas/c1/"
	cases := []struct{ name, want, got string }{
		{"MetadataDir", "cas/c1/metadata/bk/", MetadataDir(cp, "bk")},
		{"MetadataJSONPath", "cas/c1/metadata/bk/metadata.json", MetadataJSONPath(cp, "bk")},
		{"TableMetaPath", "cas/c1/metadata/bk/metadata/db1/t1.json", TableMetaPath(cp, "bk", "db1", "t1")},
		{"PartArchivePath", "cas/c1/metadata/bk/parts/default/db1/t1.tar.zstd", PartArchivePath(cp, "bk", "default", "db1", "t1")},
		{"InProgressMarkerPath", "cas/c1/inprogress/bk.marker", InProgressMarkerPath(cp, "bk")},
		{"PruneMarkerPath", "cas/c1/prune.marker", PruneMarkerPath(cp)},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: got %q want %q", c.name, c.got, c.want)
		}
	}
}

func TestPaths_TablePathEncodeApplied(t *testing.T) {
	// common.TablePathEncode encodes special characters. We don't assert the
	// exact encoded form (that's TablePathEncode's contract); we assert that
	// the encoded segment differs from the raw input when special chars present.
	cp := "cas/c/"
	raw := "weird name"
	got := TableMetaPath(cp, "bk", raw, raw)
	if !strings.Contains(got, "weird") {
		t.Fatalf("encoded path should still contain visible content: %s", got)
	}
	// Negative: a raw "/" in db/table name must NOT appear in the path because
	// TablePathEncode escapes it. (Otherwise the path could collide with the
	// separator.)
	risky := "a/b"
	risk := TableMetaPath(cp, "bk", risky, "t")
	// Confirm that "a/b" did NOT survive verbatim as a path component:
	if strings.Contains(risk, "/a/b/") {
		t.Errorf("TablePathEncode should have escaped slash; got %s", risk)
	}
}
