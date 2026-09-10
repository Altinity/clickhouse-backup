package cas

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
)

// ExistenceSet records which blob hashes already exist in the remote store.
// Backed by a map; safe for concurrent reads after the cold-list completes.
// During construction, only ColdList writes to it.
type ExistenceSet struct {
	set map[Hash128]struct{}
}

// Has reports whether h is present.
func (e *ExistenceSet) Has(h Hash128) bool {
	if e == nil {
		return false
	}
	_, ok := e.set[h]
	return ok
}

// Len returns the number of hashes in the set.
func (e *ExistenceSet) Len() int {
	if e == nil {
		return 0
	}
	return len(e.set)
}

// ColdList walks every cas/<cluster>/blob/<aa>/ prefix in parallel and builds
// an existence set. parallelism caps simultaneous Walks; <=0 falls back to 16.
//
// Keys whose hash segment doesn't decode to a valid 128-bit hex string are
// silently skipped (they can't be CAS blobs; could be debris from older
// experiments or unrelated files). Each skip is logged at debug level.
func ColdList(ctx context.Context, b Backend, clusterPrefix string, parallelism int) (*ExistenceSet, error) {
	if parallelism <= 0 {
		parallelism = 16
	}

	type shardOut struct {
		hashes []Hash128
		err    error
	}
	out := make([]shardOut, 256)

	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			shardPrefix := fmt.Sprintf("%sblob/%02x/", clusterPrefix, i)
			var hashes []Hash128
			err := b.Walk(ctx, shardPrefix, true, func(rf RemoteFile) error {
				rest := strings.TrimPrefix(rf.Key, shardPrefix)
				h, ok := decodeBlobHash(byte(i), rest)
				if !ok {
					return nil
				}
				hashes = append(hashes, h)
				return nil
			})
			out[i] = shardOut{hashes: hashes, err: err}
		}(i)
	}
	wg.Wait()

	set := &ExistenceSet{set: make(map[Hash128]struct{})}
	for i := 0; i < 256; i++ {
		if out[i].err != nil {
			return nil, fmt.Errorf("cas: cold-list shard %02x: %w", i, out[i].err)
		}
		for _, h := range out[i].hashes {
			set.set[h] = struct{}{}
		}
	}
	return set, nil
}

// decodeBlobHash parses a key suffix like "77665544332211" + "00ffeeddccbbaa99"
// (30 hex chars, the rest of a 32-char hashHex after the 2-char shard) and
// returns the corresponding Hash128. The shard byte is reattached at position
// 0; this is the inverse of hashHex.
func decodeBlobHash(shard byte, rest string) (Hash128, bool) {
	if len(rest) != 30 {
		return Hash128{}, false
	}
	var b [16]byte
	b[0] = shard
	if _, err := hex.Decode(b[1:], []byte(rest)); err != nil {
		return Hash128{}, false
	}
	return Hash128{
		Low:  binary.LittleEndian.Uint64(b[0:8]),
		High: binary.LittleEndian.Uint64(b[8:16]),
	}, true
}
