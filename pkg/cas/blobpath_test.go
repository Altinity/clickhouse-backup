package cas

import (
	"strings"
	"testing"
)

func TestHashHex_KnownValue(t *testing.T) {
	h := Hash128{Low: 0x1122334455667788, High: 0x99aabbccddeeff00}
	// Low LE = 88 77 66 55 44 33 22 11
	// High LE = 00 ff ee dd cc bb aa 99
	want := "8877665544332211" + "00ffeeddccbbaa99"
	if got := hashHex(h); got != want {
		t.Fatalf("hashHex: got %q want %q", got, want)
	}
}

func TestShardPrefix(t *testing.T) {
	h := Hash128{Low: 0x1122334455667788, High: 0}
	if got := ShardPrefix(h); got != "88" {
		t.Fatalf("ShardPrefix: got %q want \"88\"", got)
	}
}

func TestBlobPath_Format(t *testing.T) {
	h := Hash128{Low: 0x1122334455667788, High: 0x99aabbccddeeff00}
	want := "cas/c1/blob/88/77665544332211" + "00ffeeddccbbaa99"
	got := BlobPath("cas/c1/", h)
	if got != want {
		t.Fatalf("BlobPath: got %q want %q", got, want)
	}
	// Sanity: hex portion is exactly 30 chars after the shard.
	rest := strings.TrimPrefix(got, "cas/c1/blob/88/")
	if len(rest) != 30 {
		t.Fatalf("rest len: got %d want 30", len(rest))
	}
}

func TestBlobPath_DistinctHashesProduceDistinctPaths(t *testing.T) {
	a := Hash128{Low: 1, High: 0}
	b := Hash128{Low: 2, High: 0}
	if BlobPath("cas/c/", a) == BlobPath("cas/c/", b) {
		t.Fatal("distinct hashes produced same path")
	}
}
