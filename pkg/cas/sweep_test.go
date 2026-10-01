package cas_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
)

// buildMarkSet writes the given hashes into a temporary MarkSet file and
// returns an OPEN MarkSetReader positioned at the start. Caller must Close.
func buildMarkSet(t *testing.T, hashes []cas.Hash128) *cas.MarkSetReader {
	t.Helper()
	tmp := t.TempDir()
	p := filepath.Join(tmp, "marks")
	w, err := cas.NewMarkSetWriter(p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hashes {
		if err := w.Write(h); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := cas.OpenMarkSetReader(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func putBlobAt(t *testing.T, f *fakedst.Fake, cp string, h cas.Hash128, modTime time.Time) {
	t.Helper()
	key := cas.BlobPath(cp, h)
	if err := f.PutFile(context.Background(), key, io.NopCloser(bytes.NewReader([]byte("x"))), 1); err != nil {
		t.Fatal(err)
	}
	f.SetModTime(key, modTime)
}

func TestSweep_ReturnsOnlyUnreferencedAndOldEnough(t *testing.T) {
	f := fakedst.New()
	cp := "cas/c1/"
	now := time.Now()
	old := now.Add(-2 * time.Hour)   // beyond grace
	fresh := now.Add(-30 * time.Minute) // within grace

	// b1, b2 referenced; b3 unreferenced+old; b4 unreferenced+fresh; b5 referenced
	h1 := cas.Hash128{Low: 0x01, High: 0x10}
	h2 := cas.Hash128{Low: 0x02, High: 0x20}
	h3 := cas.Hash128{Low: 0x03, High: 0x30}
	h4 := cas.Hash128{Low: 0x04, High: 0x40}
	h5 := cas.Hash128{Low: 0x05, High: 0x50}
	for _, h := range []cas.Hash128{h1, h2, h5} {
		putBlobAt(t, f, cp, h, old)
	}
	putBlobAt(t, f, cp, h3, old)
	putBlobAt(t, f, cp, h4, fresh)

	marks := buildMarkSet(t, []cas.Hash128{h1, h2, h5})
	defer marks.Close()

	cands, _, err := cas.SweepOrphans(context.Background(), f, cp, marks, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 1 || cands[0].Hash != h3 {
		t.Errorf("got %+v want only h3", cands)
	}
}

func TestSweep_RespectsGracePeriodPrecisely(t *testing.T) {
	f := fakedst.New()
	cp := "cas/c1/"
	now := time.Now()

	h := cas.Hash128{Low: 0x99, High: 0xff}
	// Blob ModTime exactly grace ago — must NOT be deleted (cutoff is strict <).
	putBlobAt(t, f, cp, h, now.Add(-time.Hour))

	marks := buildMarkSet(t, nil) // empty marks → all blobs are orphans
	defer marks.Close()

	cands, _, err := cas.SweepOrphans(context.Background(), f, cp, marks, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 0 {
		t.Errorf("expected 0 candidates (exactly-grace-aged should be retained); got %+v", cands)
	}

	// One nanosecond older than grace → must be a candidate.
	putBlobAt(t, f, cp, h, now.Add(-time.Hour-time.Nanosecond))
	marks2 := buildMarkSet(t, nil)
	defer marks2.Close()
	cands, _, err = cas.SweepOrphans(context.Background(), f, cp, marks2, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 1 {
		t.Errorf("expected 1 candidate; got %+v", cands)
	}
}

func TestSweep_AllReferenced_NoCandidates(t *testing.T) {
	f := fakedst.New()
	cp := "cas/c1/"
	now := time.Now()
	old := now.Add(-2 * time.Hour)

	hs := []cas.Hash128{
		{Low: 1, High: 10}, {Low: 2, High: 20}, {Low: 3, High: 30},
	}
	for _, h := range hs {
		putBlobAt(t, f, cp, h, old)
	}

	marks := buildMarkSet(t, hs)
	defer marks.Close()

	cands, _, err := cas.SweepOrphans(context.Background(), f, cp, marks, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 0 {
		t.Errorf("expected 0 candidates (all referenced); got %+v", cands)
	}
}

func TestSweep_EmptyBucket(t *testing.T) {
	f := fakedst.New()
	marks := buildMarkSet(t, nil)
	defer marks.Close()

	cands, _, err := cas.SweepOrphans(context.Background(), f, "cas/c1/", marks, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 0 {
		t.Errorf("expected 0 candidates; got %+v", cands)
	}
}

// TestSweep_ZeroModTimeBlobIsSkipped verifies that a blob with a zero
// ModTime is NOT classified as orphan-eligible — same conservative
// choice as the marker side: false-positive cleanup would delete live
// blobs on FTP backends that return zero ModTime.
func TestSweep_ZeroModTimeBlobIsSkipped(t *testing.T) {
	f := fakedst.New()
	cfg := testCfg(1024)
	cp := cfg.ClusterPrefix()
	ctx := context.Background()

	// Create an orphan blob with zero ModTime.
	hOrphan := cas.Hash128{Low: 0xab, High: 0x10}
	_ = f.PutFile(ctx, cas.BlobPath(cp, hOrphan), io.NopCloser(bytes.NewReader([]byte("x"))), 1)
	f.SetModTime(cas.BlobPath(cp, hOrphan), time.Time{})

	// Empty mark set → the only path SweepOrphans uses is the orphan-vs-cutoff
	// branch. Without the zero-ModTime guard, the blob would be classified
	// as orphan past grace.
	rep, err := cas.Prune(ctx, f, cfg, cas.PruneOptions{
		GraceBlob:    time.Nanosecond,
		GraceBlobSet: true,
	})
	if err != nil {
		t.Fatalf("Prune unexpectedly errored: %v", err)
	}
	if rep.OrphansDeleted != 0 {
		t.Errorf("zero-ModTime blob was reaped (OrphansDeleted=%d); expected 0", rep.OrphansDeleted)
	}
	if _, _, exists, _ := f.StatFile(ctx, cas.BlobPath(cp, hOrphan)); !exists {
		t.Error("zero-ModTime blob was deleted; expected to survive")
	}
}

func TestSweep_ManyShardsParallel(t *testing.T) {
	f := fakedst.New()
	cp := "cas/c1/"
	old := time.Now().Add(-2 * time.Hour)
	// Sprinkle blobs across many shard prefixes.
	var hs []cas.Hash128
	for i := uint64(0); i < 50; i++ {
		h := cas.Hash128{Low: i*0x1010101, High: i}
		putBlobAt(t, f, cp, h, old)
		hs = append(hs, h)
	}
	marks := buildMarkSet(t, nil) // empty: every blob is an orphan
	defer marks.Close()

	cands, _, err := cas.SweepOrphans(context.Background(), f, cp, marks, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != len(hs) {
		t.Errorf("got %d candidates, want %d", len(cands), len(hs))
	}
	// Verify hash-set equality.
	gotHashes := make([]cas.Hash128, 0, len(cands))
	for _, c := range cands {
		gotHashes = append(gotHashes, c.Hash)
	}
	sort.Slice(gotHashes, func(i, j int) bool {
		if gotHashes[i].High != gotHashes[j].High {
			return gotHashes[i].High < gotHashes[j].High
		}
		return gotHashes[i].Low < gotHashes[j].Low
	})
	wantHashes := append([]cas.Hash128(nil), hs...)
	sort.Slice(wantHashes, func(i, j int) bool {
		if wantHashes[i].High != wantHashes[j].High {
			return wantHashes[i].High < wantHashes[j].High
		}
		return wantHashes[i].Low < wantHashes[j].Low
	})
	if !reflect.DeepEqual(gotHashes, wantHashes) {
		t.Errorf("hash set mismatch")
	}
}

// BenchmarkSweepOrphans_LargeN measures the heap-merge path with N blobs
// spread evenly across all 256 shards.  The benchmark is intentionally
// free of absolute assertions so it never becomes flaky; its purpose is
// to make future O(N k) regressions visible in benchmark history.
//
// Run with:
//
//	go test ./pkg/cas/ -bench BenchmarkSweepOrphans -benchtime=1x -count=1
func BenchmarkSweepOrphans_LargeN(b *testing.B) {
	const totalBlobs = 10_000 // scaled down so the benchmark runs quickly
	const numShards = 256
	perShard := totalBlobs / numShards

	now := time.Now()
	old := now.Add(-2 * time.Hour)

	f := fakedst.New()
	cp := "cas/bench/"
	ctx := context.Background()

	// Pre-populate: spread blobs across all 256 shards.
	// Hash128.High encodes the shard (top byte) so blobs land deterministically.
	for shard := 0; shard < numShards; shard++ {
		for j := 0; j < perShard; j++ {
			h := cas.Hash128{
				High: uint64(shard) << 56,
				Low:  uint64(j),
			}
			key := cas.BlobPath(cp, h)
			_ = f.PutFile(ctx, key, io.NopCloser(bytes.NewReader([]byte("x"))), 1)
			f.SetModTime(key, old)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tmp := b.TempDir()
		p := fmt.Sprintf("%s/marks-%d", tmp, i)
		w, err := cas.NewMarkSetWriter(p, 1024)
		if err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil { // empty mark set: all blobs are orphans
			b.Fatal(err)
		}
		r, err := cas.OpenMarkSetReader(p)
		if err != nil {
			b.Fatal(err)
		}
		cands, _, err := cas.SweepOrphans(ctx, f, cp, r, time.Hour, now)
		_ = r.Close()
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(cands)), "orphans")
	}
}
