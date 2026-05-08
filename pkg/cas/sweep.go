package cas

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// OrphanCandidate identifies a blob that the sweep phase considers eligible
// for deletion: not present in the live mark set AND older than the grace
// cutoff. The Key is the absolute object key (i.e. what BlobPath would
// produce), suitable for direct DeleteFile.
type OrphanCandidate struct {
	Hash    Hash128
	Key     string
	Size    int64
	ModTime time.Time
}

// SweepStats holds aggregate counters produced by a single SweepOrphans call.
type SweepStats struct {
	// BlobsTotal is the total number of blobs enumerated during the sweep,
	// regardless of whether they are live-referenced or orphaned.
	BlobsTotal uint64
	// OrphansHeldByGrace counts orphan blobs (not referenced by any live
	// backup) that were skipped because they fell inside the grace window.
	OrphansHeldByGrace uint64
}

// SweepOrphans walks every cas/<cluster>/blob/<aa>/ prefix in parallel,
// collects candidate blobs (those not in marks), and filters to those
// strictly older than t0-grace. The mark set MUST be sorted (i.e. produced
// by MarkSetWriter); SweepOrphans consumes it in a single forward pass.
//
// parallelism caps simultaneous shard walks; <=0 falls back to 32. The
// returned slice has no specified order.
func SweepOrphans(ctx context.Context, b Backend, clusterPrefix string, marks *MarkSetReader, grace time.Duration, t0 time.Time) ([]OrphanCandidate, SweepStats, error) {
	cutoff := t0.Add(-grace)
	const parallelism = 32

	shards := make([]shardOutForCompare, 256)

	var wg sync.WaitGroup
	sem := make(chan struct{}, parallelism)
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			prefix := fmt.Sprintf("%sblob/%02x/", clusterPrefix, i)
			var blobs []remoteBlob
			err := b.Walk(ctx, prefix, true, func(rf RemoteFile) error {
				h, ok := parseHashFromKey(rf.Key, prefix)
				if !ok {
					// Skip debris that doesn't match the blob key shape
					// (e.g. operator-injected files); not a fatal error.
					return nil
				}
				blobs = append(blobs, remoteBlob{hash: h, key: rf.Key, modTime: rf.ModTime, size: rf.Size})
				return nil
			})
			sort.Slice(blobs, func(a, c int) bool { return hashLess(blobs[a].hash, blobs[c].hash) })
			shards[i] = shardOutForCompare{blobs: blobs, err: err}
		}(i)
	}
	wg.Wait()

	for i, s := range shards {
		if s.err != nil {
			return nil, SweepStats{}, fmt.Errorf("cas-sweep: shard %02x: %w", i, s.err)
		}
	}

	// Stream-merge the 256 sorted shards into a single sorted iterator,
	// then walk it side-by-side with the mark set.
	candidates, stats, err := streamCompareWithMarks(shards, marks, cutoff)
	if err != nil {
		return nil, SweepStats{}, err
	}
	return candidates, stats, nil
}

type remoteBlob struct {
	hash    Hash128
	key     string
	modTime time.Time
	size    int64
}

// parseHashFromKey extracts a Hash128 from an absolute blob key of the form
// "<clusterPrefix>blob/<aa>/<rest>" where the prefix arg is the leading
// "<clusterPrefix>blob/<aa>/". Returns (zero, false) if the key doesn't
// match the expected shape (length, hex chars).
func parseHashFromKey(key, prefix string) (Hash128, bool) {
	if !strings.HasPrefix(key, prefix) {
		return Hash128{}, false
	}
	rest := key[len(prefix):]
	if len(rest) != 30 {
		return Hash128{}, false
	}
	// The shard byte (2 hex chars) lives in the prefix itself, in the
	// segment between "blob/" and the trailing "/". Extract it.
	// prefix = "<clusterPrefix>blob/<aa>/" — find the <aa>.
	const blobMarker = "blob/"
	bm := strings.Index(prefix, blobMarker)
	if bm < 0 {
		return Hash128{}, false
	}
	shardStart := bm + len(blobMarker)
	if shardStart+3 > len(prefix) {
		return Hash128{}, false
	}
	shardHex := prefix[shardStart : shardStart+2]
	full := shardHex + rest
	if len(full) != 32 {
		return Hash128{}, false
	}
	var b [16]byte
	if _, err := hex.Decode(b[:], []byte(full)); err != nil {
		return Hash128{}, false
	}
	return Hash128{
		Low:  binary.LittleEndian.Uint64(b[0:8]),
		High: binary.LittleEndian.Uint64(b[8:16]),
	}, true
}

// streamCompareWithMarks merges the sorted shard outputs with the sorted
// mark stream and emits OrphanCandidate for any blob not in marks AND older
// than cutoff. It also returns SweepStats with aggregate counters.
func streamCompareWithMarks(shards []shardOutForCompare, marks *MarkSetReader, cutoff time.Time) ([]OrphanCandidate, SweepStats, error) {
	// Flatten shards in sorted order. Shards are already individually
	// sorted; flatten via heap merge.
	it := newShardIter(shards)
	var (
		mark     Hash128
		haveMark bool
	)
	advanceMark := func() error {
		h, ok, err := marks.Next()
		if err != nil {
			return err
		}
		mark = h
		haveMark = ok
		return nil
	}
	if err := advanceMark(); err != nil {
		return nil, SweepStats{}, err
	}

	var out []OrphanCandidate
	var stats SweepStats
	for it.valid {
		blob := it.current
		stats.BlobsTotal++
		// Advance mark stream past anything strictly less than blob.hash.
		for haveMark && hashLess(mark, blob.hash) {
			if err := advanceMark(); err != nil {
				return nil, SweepStats{}, err
			}
		}
		if !(haveMark && mark == blob.hash) {
			// Blob is not referenced by any live backup → orphan candidate.
			if blob.modTime.IsZero() {
				log.Warn().
					Str("key", blob.key).
					Msg("cas-sweep: blob has zero ModTime (likely FTP LIST without MLSD); skipping (treating as inside grace window)")
				stats.OrphansHeldByGrace++
			} else if blob.modTime.Before(cutoff) {
				out = append(out, OrphanCandidate{
					Hash: blob.hash, Key: blob.key, ModTime: blob.modTime, Size: blob.size,
				})
			} else {
				// Orphan but within the grace window — held for now.
				stats.OrphansHeldByGrace++
			}
		}
		if err := it.advance(); err != nil {
			return nil, SweepStats{}, err
		}
	}
	return out, stats, nil
}

// shardOutForCompare is an alias used by streamCompareWithMarks. We keep
// the type local so the caller doesn't have to expose internal `remoteBlob`.
type shardOutForCompare = struct {
	blobs []remoteBlob
	err   error
}

// shardIter is a min-heap iterator across the 256 shard slices.
type shardIter struct {
	heads   []shardHead
	current remoteBlob
	valid   bool
}

type shardHead struct {
	blobs []remoteBlob
	idx   int
}

func newShardIter(shards []shardOutForCompare) *shardIter {
	it := &shardIter{}
	for _, s := range shards {
		if len(s.blobs) > 0 {
			it.heads = append(it.heads, shardHead{blobs: s.blobs, idx: 0})
		}
	}
	_ = it.advance()
	return it
}

func (it *shardIter) advance() error {
	if len(it.heads) == 0 {
		it.valid = false
		return nil
	}
	// Find the smallest current element.
	min := 0
	for i := 1; i < len(it.heads); i++ {
		if hashLess(it.heads[i].blobs[it.heads[i].idx].hash, it.heads[min].blobs[it.heads[min].idx].hash) {
			min = i
		}
	}
	it.current = it.heads[min].blobs[it.heads[min].idx]
	it.valid = true
	it.heads[min].idx++
	if it.heads[min].idx >= len(it.heads[min].blobs) {
		it.heads = append(it.heads[:min], it.heads[min+1:]...)
	}
	return nil
}
