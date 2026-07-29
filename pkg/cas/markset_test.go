package cas

import (
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMarkSet_WriteSortRead(t *testing.T) {
	tmp := t.TempDir()
	w, err := NewMarkSetWriter(filepath.Join(tmp, "marks"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	refs := []Hash128{
		{High: 0xff, Low: 1},
		{High: 0x00, Low: 5},
		{High: 0x80, Low: 3},
		{High: 0x00, Low: 5}, // duplicate
		{High: 0x00, Low: 1},
	}
	for _, h := range refs {
		if err := w.Write(h); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenMarkSetReader(filepath.Join(tmp, "marks"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var got []Hash128
	for {
		h, ok, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, h)
	}
	want := []Hash128{
		{High: 0x00, Low: 1},
		{High: 0x00, Low: 5},
		{High: 0x80, Low: 3},
		{High: 0xff, Low: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestMarkSet_LargeExternalSort(t *testing.T) {
	// Force multi-run mergesort: chunk = 256, write 5000 random refs.
	// Output must be sorted, deduplicated, and contain exactly the unique
	// inputs.
	tmp := t.TempDir()
	w, err := NewMarkSetWriter(filepath.Join(tmp, "marks"), 256)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(42))
	uniq := map[Hash128]struct{}{}
	for i := 0; i < 5000; i++ {
		h := Hash128{Low: rng.Uint64() & 0xffff, High: rng.Uint64() & 0xff} // many collisions
		uniq[h] = struct{}{}
		if err := w.Write(h); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenMarkSetReader(filepath.Join(tmp, "marks"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var got []Hash128
	for {
		h, ok, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, h)
	}
	if len(got) != len(uniq) {
		t.Errorf("unique count: got %d want %d", len(got), len(uniq))
	}
	// Verify sorted.
	for i := 1; i < len(got); i++ {
		if !hashLess(got[i-1], got[i]) {
			t.Fatalf("not sorted at %d: %+v vs %+v", i, got[i-1], got[i])
		}
	}
}

func TestMarkSet_EmptySetIsValid(t *testing.T) {
	tmp := t.TempDir()
	w, err := NewMarkSetWriter(filepath.Join(tmp, "marks"), 256)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenMarkSetReader(filepath.Join(tmp, "marks"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, ok, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected empty MarkSet but Next returned a hash")
	}
}

func TestMarkSet_CloseTwiceIsNoop(t *testing.T) {
	tmp := t.TempDir()
	w, err := NewMarkSetWriter(filepath.Join(tmp, "marks"), 256)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Write(Hash128{Low: 1})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
