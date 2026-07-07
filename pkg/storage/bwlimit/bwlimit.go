package bwlimit

import (
	"context"
	"io"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type Limiter struct {
	maxBytesPerSecond int64
	maxTokens         int64
	tokens            int64
	lastRefill        time.Time
	mu                sync.Mutex

	// debug accounting, sampled at most once per second to ease e2e verification
	grantedBytes int64
	sleptNanos   int64
	lastLog      time.Time
}

func New(maxBytesPerSecond uint64) *Limiter {
	if maxBytesPerSecond == 0 {
		return nil
	}
	bytesPerSecond := clampRate(maxBytesPerSecond)
	maxTokens := bytesPerSecond * 2
	if bytesPerSecond > math.MaxInt64/2 {
		maxTokens = math.MaxInt64
	}
	log.Debug().Msgf("bwlimit.New rate=%d bytes/s maxTokens=%d", bytesPerSecond, maxTokens)
	now := time.Now()
	return &Limiter{
		maxBytesPerSecond: bytesPerSecond,
		maxTokens:         maxTokens,
		tokens:            bytesPerSecond,
		lastRefill:        now,
		lastLog:           now,
	}
}

func clampRate(maxBytesPerSecond uint64) int64 {
	if maxBytesPerSecond > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(maxBytesPerSecond)
}

// SameRate reports whether the limiter already enforces maxBytesPerSecond.
// It is nil-safe: a nil limiter matches only a zero (disabled) rate.
func (l *Limiter) SameRate(maxBytesPerSecond uint64) bool {
	if l == nil {
		return maxBytesPerSecond == 0
	}
	return l.maxBytesPerSecond == clampRate(maxBytesPerSecond)
}

func (l *Limiter) Wait(ctx context.Context, n int) error {
	if l == nil || n <= 0 {
		return nil
	}
	for n > 0 {
		chunk := n
		if int64(chunk) > l.maxTokens {
			chunk = int(l.maxTokens)
		}
		if err := l.waitChunk(ctx, int64(chunk)); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

func (l *Limiter) waitChunk(ctx context.Context, n int64) error {
	for {
		l.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(l.lastRefill)
		if elapsed > 0 {
			refill := int64(float64(l.maxBytesPerSecond) * elapsed.Seconds())
			if refill > 0 {
				l.tokens += refill
				if l.tokens > l.maxTokens {
					l.tokens = l.maxTokens
				}
				l.lastRefill = now
			}
		}
		if l.tokens >= n {
			l.tokens -= n
			l.grantedBytes += n
			l.maybeLogLocked(now)
			l.mu.Unlock()
			return nil
		}
		deficit := n - l.tokens
		waitTime := time.Duration(float64(deficit) / float64(l.maxBytesPerSecond) * float64(time.Second))
		l.sleptNanos += waitTime.Nanoseconds()
		l.mu.Unlock()

		timer := time.NewTimer(waitTime)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// maybeLogLocked emits an effective-throughput debug line at most once per second.
// Caller must hold l.mu; now must be the current time used by the caller.
func (l *Limiter) maybeLogLocked(now time.Time) {
	elapsed := now.Sub(l.lastLog)
	if elapsed < time.Second {
		return
	}
	log.Debug().Msgf("bwlimit throughput=%.0f bytes/s (limit=%d) granted=%d slept=%s over %s",
		float64(l.grantedBytes)/elapsed.Seconds(), l.maxBytesPerSecond, l.grantedBytes, time.Duration(l.sleptNanos), elapsed)
	l.grantedBytes = 0
	l.sleptNanos = 0
	l.lastLog = now
}

type rateLimitedReader struct {
	ctx     context.Context
	reader  io.Reader
	limiter *Limiter
}

func Reader(ctx context.Context, reader io.Reader, limiter *Limiter) io.Reader {
	if limiter == nil {
		return reader
	}
	return &rateLimitedReader{ctx: ctx, reader: reader, limiter: limiter}
}

func (r *rateLimitedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return r.reader.Read(p)
	}
	if int64(len(p)) > r.limiter.maxBytesPerSecond {
		p = p[:r.limiter.maxBytesPerSecond]
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		// keep the bytes we already read (io.Reader contract) and surface the
		// wait error (e.g. context cancellation) so the caller can stop cleanly
		if waitErr := r.limiter.Wait(r.ctx, n); waitErr != nil {
			return n, waitErr
		}
	}
	return n, err
}

type rateLimitedReadCloser struct {
	io.Reader
	closer io.Closer
}

func ReadCloser(ctx context.Context, reader io.ReadCloser, limiter *Limiter) io.ReadCloser {
	if limiter == nil {
		return reader
	}
	return &rateLimitedReadCloser{
		Reader: Reader(ctx, reader, limiter),
		closer: reader,
	}
}

func (r *rateLimitedReadCloser) Close() error {
	return r.closer.Close()
}
