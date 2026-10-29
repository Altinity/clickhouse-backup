package cas

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// MarkSetWriter accumulates Hash128 references and produces a sorted, deduped
// on-disk file at finalPath on Close. Implementation: an in-memory buffer of
// `chunk` entries; when full, the buffer is sorted, deduplicated, and spilled
// to a "run" file. On Close, all run files are k-way-merged into the final
// output, deduplicating across runs.
//
// The on-disk format is a simple binary stream of 16-byte hashes
// (Low LE, then High LE, matching the byte order used by hashHex). The set
// is intended for the cas-prune mark phase where the live-blob reference
// count can reach ~10^8 across the catalog and won't fit in RAM.
type MarkSetWriter struct {
	finalPath string
	runDir    string
	chunk     int
	buf       []Hash128
	runs      []string
	closed    bool
	written   uint64
}

// NewMarkSetWriter opens a new writer that will produce a sorted, deduped
// file at finalPath. chunk is the in-memory buffer size before spilling
// (each entry is 16 bytes; 1<<20 ≈ 16 MiB of RAM).
func NewMarkSetWriter(finalPath string, chunk int) (*MarkSetWriter, error) {
	if chunk <= 0 {
		chunk = 1 << 20
	}
	parent := filepath.Dir(finalPath)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("markset: mkdir %s: %w", parent, err)
	}
	runDir, err := os.MkdirTemp(parent, "markset-runs-*")
	if err != nil {
		return nil, fmt.Errorf("markset: temp dir: %w", err)
	}
	return &MarkSetWriter{
		finalPath: finalPath,
		runDir:    runDir,
		chunk:     chunk,
		buf:       make([]Hash128, 0, chunk),
	}, nil
}

// Write appends one hash to the in-memory buffer; spills to disk when full.
func (w *MarkSetWriter) Write(h Hash128) error {
	if w.closed {
		return fmt.Errorf("markset: writer is closed")
	}
	w.buf = append(w.buf, h)
	w.written++
	if len(w.buf) >= w.chunk {
		return w.spill()
	}
	return nil
}

// Count returns the total number of hashes written (including duplicates before
// deduplication). Available after the first Write call.
func (w *MarkSetWriter) Count() uint64 { return w.written }

// Close flushes the final in-memory chunk and merges all runs into finalPath.
// The temporary run directory is removed on success. Calling Close more than
// once is a no-op.
func (w *MarkSetWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.spill(); err != nil {
		return err
	}
	if err := mergeRuns(w.runs, w.finalPath); err != nil {
		return err
	}
	// Best-effort cleanup of the run directory.
	_ = os.RemoveAll(w.runDir)
	return nil
}

func (w *MarkSetWriter) spill() error {
	if len(w.buf) == 0 {
		return nil
	}
	sort.Slice(w.buf, func(i, j int) bool { return hashLess(w.buf[i], w.buf[j]) })
	p := filepath.Join(w.runDir, fmt.Sprintf("run-%05d", len(w.runs)))
	f, err := os.Create(p)
	if err != nil {
		return fmt.Errorf("markset: create run file: %w", err)
	}
	bw := bufio.NewWriter(f)
	var prev Hash128
	first := true
	for _, h := range w.buf {
		if !first && h == prev {
			continue
		}
		if err := writeHashBinary(bw, h); err != nil {
			_ = f.Close()
			return fmt.Errorf("markset: write run: %w", err)
		}
		prev = h
		first = false
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	w.buf = w.buf[:0]
	w.runs = append(w.runs, p)
	return nil
}

// MarkSetReader streams sorted, deduplicated hashes from a file produced by
// MarkSetWriter.
type MarkSetReader struct {
	f  *os.File
	br *bufio.Reader
}

// OpenMarkSetReader opens the file produced by MarkSetWriter.Close.
func OpenMarkSetReader(p string) (*MarkSetReader, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("markset: open: %w", err)
	}
	return &MarkSetReader{f: f, br: bufio.NewReader(f)}, nil
}

// Next returns the next hash, or (Hash128{}, false, nil) at EOF.
func (r *MarkSetReader) Next() (Hash128, bool, error) {
	var b [16]byte
	n, err := io.ReadFull(r.br, b[:])
	if err == io.EOF {
		return Hash128{}, false, nil
	}
	if err == io.ErrUnexpectedEOF {
		return Hash128{}, false, fmt.Errorf("markset: short read at offset (got %d bytes)", n)
	}
	if err != nil {
		return Hash128{}, false, err
	}
	return Hash128{
		Low:  binary.LittleEndian.Uint64(b[0:8]),
		High: binary.LittleEndian.Uint64(b[8:16]),
	}, true, nil
}

// Close releases the underlying file handle.
func (r *MarkSetReader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

// hashLess defines the canonical ordering: High first, then Low.
// (Same convention used everywhere we need to sort Hash128.)
func hashLess(a, b Hash128) bool {
	if a.High != b.High {
		return a.High < b.High
	}
	return a.Low < b.Low
}

func writeHashBinary(w io.Writer, h Hash128) error {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], h.Low)
	binary.LittleEndian.PutUint64(b[8:16], h.High)
	_, err := w.Write(b[:])
	return err
}

// runIter is a single-run iterator used by mergeRuns.
type runIter struct {
	f       *os.File
	br      *bufio.Reader
	current Hash128
	valid   bool
}

func openRunIter(p string) (*runIter, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	it := &runIter{f: f, br: bufio.NewReader(f)}
	if err := it.advance(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return it, nil
}

func (it *runIter) advance() error {
	var b [16]byte
	n, err := io.ReadFull(it.br, b[:])
	if err == io.EOF {
		it.valid = false
		return nil
	}
	if err == io.ErrUnexpectedEOF {
		return fmt.Errorf("markset: short read in run (got %d bytes)", n)
	}
	if err != nil {
		return err
	}
	it.current = Hash128{
		Low:  binary.LittleEndian.Uint64(b[0:8]),
		High: binary.LittleEndian.Uint64(b[8:16]),
	}
	it.valid = true
	return nil
}

func (it *runIter) close() error {
	if it.f == nil {
		return nil
	}
	err := it.f.Close()
	it.f = nil
	return err
}

// runHeap is a min-heap of runIter pointers ordered by current hash.
type runHeap []*runIter

func (h runHeap) Len() int            { return len(h) }
func (h runHeap) Less(i, j int) bool  { return hashLess(h[i].current, h[j].current) }
func (h runHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x interface{}) { *h = append(*h, x.(*runIter)) }
func (h *runHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// mergeRuns performs k-way merge over runs and writes a sorted, deduplicated
// stream to dst. Each run is itself sorted+deduped (per spill contract).
func mergeRuns(runs []string, dst string) error {
	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("markset: create dst: %w", err)
	}
	bw := bufio.NewWriter(out)

	if len(runs) == 0 {
		// Empty mark set is a valid output (zero-byte file).
		if err := bw.Flush(); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	}

	h := &runHeap{}
	heap.Init(h)
	for _, p := range runs {
		it, err := openRunIter(p)
		if err != nil {
			closeAll(*h)
			_ = out.Close()
			return err
		}
		if it.valid {
			heap.Push(h, it)
		} else {
			_ = it.close()
		}
	}

	var prev Hash128
	first := true
	for h.Len() > 0 {
		top := (*h)[0]
		cur := top.current
		if first || cur != prev {
			if err := writeHashBinary(bw, cur); err != nil {
				closeAll(*h)
				_ = out.Close()
				return err
			}
			prev = cur
			first = false
		}
		if err := top.advance(); err != nil {
			closeAll(*h)
			_ = out.Close()
			return err
		}
		if top.valid {
			heap.Fix(h, 0)
		} else {
			heap.Pop(h)
			_ = top.close()
		}
	}

	if err := bw.Flush(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func closeAll(its []*runIter) {
	for _, it := range its {
		_ = it.close()
	}
}
