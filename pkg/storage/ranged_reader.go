package storage

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

// rangeFetchFunc opens a reader for bytes [offset, offset+length) of one remote object.
type rangeFetchFunc func(ctx context.Context, offset, length int64) (io.ReadCloser, error)

type rangedChunk struct {
	data []byte
	err  error
}

// rangedReader streams one remote object as concurrent byte-range requests and hands the bytes to the
// consumer strictly in order, so it can feed the existing decompress/untar pipeline unchanged.
//
// Why: one HTTP stream from object storage has a fixed ceiling, so when a restore is down to its last big
// part the link sits mostly idle. Ranges of the same object scale with their number.
//
// Memory is bounded twice: each reader keeps at most `workers` chunks in flight (the window), and every
// chunk also holds one slot of `budget`, which is shared by all ranged readers of the process. Chunk
// indexes are claimed in order by goroutines that already hold both slots, so the lowest unconsumed chunk
// of every reader is always being fetched or already delivered: readers can starve each other of
// prefetch but can never deadlock.
type rangedReader struct {
	ctx     context.Context
	cancel  context.CancelFunc
	fetch   rangeFetchFunc
	size    int64
	chunk   int64
	nChunks int64
	retries int
	budget  *semaphore.Weighted
	window  chan struct{}
	results []chan rangedChunk
	wg      sync.WaitGroup

	claimMu sync.Mutex
	next    int64 // next chunk index to claim, guarded by claimMu

	// consumer state, guarded by readMu (Read and Close may run concurrently)
	readMu    sync.Mutex
	cur       []byte
	curPos    int
	curIdx    int64 // next chunk to hand to the consumer
	holding   bool  // the consumer still holds the slots of the last delivered chunk
	stickyErr error
	closeOnce sync.Once
}

func newRangedReader(ctx context.Context, fetch rangeFetchFunc, size, chunk int64, workers, retries int, budget *semaphore.Weighted) *rangedReader {
	ctx, cancel := context.WithCancel(ctx)
	nChunks := (size + chunk - 1) / chunk
	if int64(workers) > nChunks {
		workers = int(nChunks)
	}
	if workers < 1 {
		workers = 1
	}
	r := &rangedReader{
		ctx:     ctx,
		cancel:  cancel,
		fetch:   fetch,
		size:    size,
		chunk:   chunk,
		nChunks: nChunks,
		retries: retries,
		budget:  budget,
		window:  make(chan struct{}, workers),
		results: make([]chan rangedChunk, nChunks),
	}
	for i := range r.results {
		r.results[i] = make(chan rangedChunk, 1)
	}
	for w := 0; w < workers; w++ {
		r.wg.Add(1)
		go r.worker()
	}
	return r
}

func (r *rangedReader) release() {
	r.budget.Release(1)
	<-r.window
}

func (r *rangedReader) worker() {
	defer r.wg.Done()
	for {
		select {
		case r.window <- struct{}{}:
		case <-r.ctx.Done():
			return
		}
		if err := r.budget.Acquire(r.ctx, 1); err != nil {
			<-r.window
			return
		}
		r.claimMu.Lock()
		i := r.next
		if i >= r.nChunks {
			r.claimMu.Unlock()
			r.release()
			return
		}
		r.next++
		r.claimMu.Unlock()
		data, err := r.fetchChunk(i)
		// every claimed chunk delivers exactly one result (buffered, never blocks), Close relies on it
		r.results[i] <- rangedChunk{data: data, err: err}
	}
}

func (r *rangedReader) fetchChunk(i int64) ([]byte, error) {
	off := i * r.chunk
	length := r.chunk
	if off+length > r.size {
		length = r.size - off
	}
	var lastErr error
	for attempt := 0; attempt <= r.retries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
			case <-r.ctx.Done():
				return nil, r.ctx.Err()
			}
		}
		rc, err := r.fetch(r.ctx, off, length)
		if err != nil {
			lastErr = err
			if IsNotFoundErr(err) || r.ctx.Err() != nil {
				break
			}
			continue
		}
		buf := make([]byte, length)
		_, err = io.ReadFull(rc, buf)
		closeErr := rc.Close()
		if err == nil && closeErr == nil {
			return buf, nil
		}
		if err == nil {
			err = closeErr
		}
		lastErr = err
		if r.ctx.Err() != nil {
			break
		}
	}
	return nil, errors.Wrapf(lastErr, "ranged read of bytes [%d, %d)", off, off+length)
}

func (r *rangedReader) Read(p []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if r.stickyErr != nil {
		return 0, r.stickyErr
	}
	for r.curPos >= len(r.cur) {
		if r.holding {
			r.release()
			r.holding = false
			r.cur = nil
			r.curPos = 0
		}
		if r.curIdx >= r.nChunks {
			return 0, io.EOF
		}
		select {
		case c := <-r.results[r.curIdx]:
			r.curIdx++
			r.holding = true
			if c.err != nil {
				r.stickyErr = c.err
				return 0, c.err
			}
			r.cur = c.data
			r.curPos = 0
		case <-r.ctx.Done():
			r.stickyErr = r.ctx.Err()
			return 0, r.stickyErr
		}
	}
	n := copy(p, r.cur[r.curPos:])
	r.curPos += n
	return n, nil
}

// Close stops the workers and gives back every slot this reader still holds. It is safe to call
// concurrently with a blocked Read (the download watchdog does that on cancellation).
func (r *rangedReader) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		r.wg.Wait()
		r.readMu.Lock()
		defer r.readMu.Unlock()
		if r.holding {
			r.release()
			r.holding = false
		}
		r.claimMu.Lock()
		claimed := r.next
		r.claimMu.Unlock()
		for i := r.curIdx; i < claimed; i++ {
			<-r.results[i]
			r.release()
		}
		r.curIdx = claimed
		r.cur = nil
		if r.stickyErr == nil {
			r.stickyErr = errors.New("rangedReader: read after Close")
		}
	})
	return nil
}
