package cas_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
)

// putBlob is a test helper to populate the fake with a key in the format
// cas/<cluster>/blob/<shard>/<rest>.
func putBlob(t *testing.T, f *fakedst.Fake, clusterPrefix string, h cas.Hash128) {
	t.Helper()
	if err := f.PutFile(context.Background(), cas.BlobPath(clusterPrefix, h),
		io.NopCloser(strings.NewReader("x")), 1); err != nil {
		t.Fatal(err)
	}
}

func TestColdList_FindsAllBlobs(t *testing.T) {
	f := fakedst.New()
	cp := "cas/c1/"
	hs := []cas.Hash128{
		{Low: 0x1122334455667788, High: 0x99aabbccddeeff00},
		{Low: 0xaaaaaaaaaaaaaaaa, High: 0xbbbbbbbbbbbbbbbb},
		{Low: 0, High: 1},
	}
	for _, h := range hs {
		putBlob(t, f, cp, h)
	}
	set, err := cas.ColdList(context.Background(), f, cp, 16)
	if err != nil {
		t.Fatal(err)
	}
	if set.Len() != len(hs) {
		t.Errorf("Len: got %d want %d", set.Len(), len(hs))
	}
	for _, h := range hs {
		if !set.Has(h) {
			t.Errorf("missing %+v", h)
		}
	}
}

func TestColdList_IgnoresUnrelatedKeys(t *testing.T) {
	f := fakedst.New()
	cp := "cas/c1/"
	ctx := context.Background()
	h := cas.Hash128{Low: 1, High: 2}
	putBlob(t, f, cp, h)
	// unrelated debris in the same shard prefix:
	_ = f.PutFile(ctx, cp+"blob/00/short", io.NopCloser(strings.NewReader("x")), 1)                              // wrong length
	_ = f.PutFile(ctx, cp+"blob/00/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", io.NopCloser(strings.NewReader("x")), 1)     // not hex
	_ = f.PutFile(ctx, cp+"blob/00/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", io.NopCloser(strings.NewReader("x")), 1)     // not hex, 30 chars
	// unrelated outside blob/:
	_ = f.PutFile(ctx, cp+"metadata/x", io.NopCloser(strings.NewReader("x")), 1)
	set, err := cas.ColdList(ctx, f, cp, 16)
	if err != nil {
		t.Fatal(err)
	}
	if !set.Has(h) {
		t.Error("missed real blob")
	}
	if set.Len() != 1 {
		t.Errorf("expected 1 valid blob, got %d", set.Len())
	}
}

func TestColdList_RoundTripWithBlobPath(t *testing.T) {
	// Property: ColdList recovers exactly the hashes that BlobPath was used
	// to write. This is the load-bearing invariant — if hashHex/decodeBlobHash
	// ever drift, dedup silently breaks.
	f := fakedst.New()
	cp := "cas/c1/"
	ctx := context.Background()
	var want []cas.Hash128
	for i := 0; i < 32; i++ {
		h := cas.Hash128{Low: uint64(i) * 0x0101010101010101, High: uint64(i)<<32 | uint64(i)}
		putBlob(t, f, cp, h)
		want = append(want, h)
	}
	set, _ := cas.ColdList(ctx, f, cp, 16)
	if set.Len() != len(want) {
		t.Fatalf("Len: got %d want %d", set.Len(), len(want))
	}
	for _, h := range want {
		if !set.Has(h) {
			t.Errorf("missing %v", h)
		}
	}
}

func TestColdList_EmptyBucket(t *testing.T) {
	f := fakedst.New()
	set, err := cas.ColdList(context.Background(), f, "cas/c1/", 16)
	if err != nil {
		t.Fatal(err)
	}
	if set.Len() != 0 {
		t.Error("empty bucket should produce empty set")
	}
}
