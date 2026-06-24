package bwlimit

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

type closeRecorder struct {
	*bytes.Reader
	closed bool
}

func (r *closeRecorder) Close() error {
	r.closed = true
	return nil
}

func TestReaderReturnsOriginalWhenUnlimited(t *testing.T) {
	reader := bytes.NewReader([]byte("abc"))

	got := Reader(context.Background(), reader, New(0))

	if got != reader {
		t.Fatalf("Reader() should return original reader when limiter is disabled")
	}
}

func TestReadCloserPreservesClose(t *testing.T) {
	reader := &closeRecorder{Reader: bytes.NewReader([]byte("abc"))}
	wrapped := ReadCloser(context.Background(), reader, New(1024))

	if _, err := io.ReadAll(wrapped); err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !reader.closed {
		t.Fatal("Close() did not close the wrapped reader")
	}
}

func TestReadPreservesBytesOnContextCancel(t *testing.T) {
	// rate of 1 byte/s: the initial 1-byte burst is consumed by the first read
	// without blocking; the second read must then block in Wait, where the
	// cancelled context surfaces as an error without losing the byte read
	ctx, cancel := context.WithCancel(context.Background())

	r := Reader(ctx, bytes.NewReader([]byte("ab")), New(1))
	p := make([]byte, 1)
	if n, err := r.Read(p); err != nil || n != 1 {
		t.Fatalf("first Read() = (%d, %v), want (1, nil)", n, err)
	}

	cancel()
	n, err := r.Read(p)
	if err == nil {
		t.Fatal("Read() should return the context error")
	}
	if n != 1 {
		t.Fatalf("Read() should keep the byte already read, got n = %d", n)
	}
}

func TestSameRate(t *testing.T) {
	if !New(0).SameRate(0) {
		t.Fatal("nil limiter should match a disabled rate")
	}
	if New(0).SameRate(1024) {
		t.Fatal("nil limiter should not match a non-zero rate")
	}
	limiter := New(1024)
	if !limiter.SameRate(1024) {
		t.Fatal("SameRate(1024) should match the configured rate")
	}
	if limiter.SameRate(2048) {
		t.Fatal("SameRate(2048) should not match a different rate")
	}
}

func TestLimiterIsSharedAcrossReaders(t *testing.T) {
	limiter := New(100)
	reader1 := Reader(context.Background(), bytes.NewReader(bytes.Repeat([]byte("a"), 100)), limiter)
	reader2 := Reader(context.Background(), bytes.NewReader(bytes.Repeat([]byte("b"), 100)), limiter)

	start := time.Now()
	if _, err := io.ReadAll(reader1); err != nil {
		t.Fatalf("ReadAll(reader1) error = %v", err)
	}
	if _, err := io.ReadAll(reader2); err != nil {
		t.Fatalf("ReadAll(reader2) error = %v", err)
	}

	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("shared limiter finished too quickly: %s", elapsed)
	}
}
