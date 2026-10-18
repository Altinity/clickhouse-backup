package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/bwlimit"
	libSFTP "github.com/pkg/sftp"
	"github.com/stretchr/testify/require"
)

// delayLine delivers everything read from src after delay without serializing it,
// so it adds latency like a network round trip but keeps the throughput
func delayLine(src io.Reader, delay time.Duration) io.Reader {
	type chunk struct {
		data []byte
		at   time.Time
	}
	pr, pw := io.Pipe()
	chunks := make(chan chunk, 4096)
	go func() {
		defer close(chunks)
		for {
			buf := make([]byte, 64*1024)
			n, err := src.Read(buf)
			if n > 0 {
				chunks <- chunk{data: buf[:n], at: time.Now().Add(delay)}
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		for c := range chunks {
			time.Sleep(time.Until(c.at))
			if _, err := pw.Write(c.data); err != nil {
				return
			}
		}
		_ = pw.Close()
	}()
	return pr
}

func newTestSFTP(t *testing.T, concurrency int, rtt time.Duration) *SFTP {
	clientToServerR, clientToServerW := io.Pipe()
	serverToClientR, serverToClientW := io.Pipe()
	server, err := libSFTP.NewServer(struct {
		io.Reader
		io.WriteCloser
	}{delayLine(clientToServerR, rtt), serverToClientW})
	require.NoError(t, err)
	go func() { _ = server.Serve() }()
	opts := []libSFTP.ClientOption{libSFTP.UseConcurrentReads(true), libSFTP.UseConcurrentWrites(true), libSFTP.MaxConcurrentRequestsPerFile(16)}
	client, err := libSFTP.NewClientPipe(serverToClientR, clientToServerW, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		// the client waits for its receive loop, which ends when the server side is closed
		_ = server.Close()
		_ = client.Close()
	})
	return &SFTP{sftpClient: client, Config: &config.SFTPConfig{Concurrency: concurrency}}
}

// TestSFTPPutFileAbsolutePipeIsConcurrent compressed uploads pass an io.Pipe without a size to PutFileAbsolute,
// sftp.File.ReadFrom then sends one write per round trip, see https://github.com/Altinity/clickhouse-backup/issues/1607
func TestSFTPPutFileAbsolutePipeIsConcurrent(t *testing.T) {
	const rtt = 5 * time.Millisecond
	data := bytes.Repeat([]byte("0123456789abcdef"), 2*1024*1024/16)
	upload := func(concurrency int) time.Duration {
		sftp := newTestSFTP(t, concurrency, rtt)
		key := filepath.Join(t.TempDir(), "sub", "archive.tar")
		pr, pw := io.Pipe()
		go func() {
			_, err := pw.Write(data)
			_ = pw.CloseWithError(err)
		}()
		start := time.Now()
		require.NoError(t, sftp.PutFileAbsolute(context.Background(), key, pr, 0))
		elapsed := time.Since(start)
		uploaded, err := os.ReadFile(key)
		require.NoError(t, err)
		require.Equal(t, data, uploaded)
		return elapsed
	}
	sequential := upload(0)
	concurrent := upload(16)
	t.Logf("sequential=%s concurrent=%s", sequential, concurrent)
	// 64 writes of 32KiB, at least 320ms one by one, about 4 round trips with 16 in flight
	require.Less(t, concurrent, sequential/3, "upload from a pipe should keep several writes in flight")
}

// TestSFTPRateLimitedDownloadIsConcurrent download_max_bytes_per_second wraps the sftp.File,
// io.Copy must still reach its concurrent WriteTo instead of one 32KiB Read per round trip,
// see https://github.com/Altinity/clickhouse-backup/issues/1607
func TestSFTPRateLimitedDownloadIsConcurrent(t *testing.T) {
	const rtt = 5 * time.Millisecond
	data := bytes.Repeat([]byte("0123456789abcdef"), 2*1024*1024/16)
	key := filepath.Join(t.TempDir(), "archive.tar")
	require.NoError(t, os.WriteFile(key, data, 0644))
	// far above the test throughput, so only the read pattern matters
	limiter := bwlimit.New(1024 * 1024 * 1024)
	download := func(hideWriterTo bool) time.Duration {
		sftp := newTestSFTP(t, 16, rtt)
		r, err := sftp.GetFileReaderAbsolute(context.Background(), key)
		require.NoError(t, err)
		if hideWriterTo {
			r = struct{ io.ReadCloser }{r}
		}
		r = bwlimit.ReadCloser(context.Background(), r, limiter)
		var dst bytes.Buffer
		start := time.Now()
		// hide bytes.Buffer.ReadFrom, it reads with large buffers which sftp.File.Read splits into concurrent requests,
		// the nio pipe in DownloadCompressedStream and the os.File in DownloadPath read with 32KiB
		_, err = io.Copy(struct{ io.Writer }{&dst}, r)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, data, dst.Bytes())
		return elapsed
	}
	sequential := download(true)
	concurrent := download(false)
	t.Logf("sequential=%s concurrent=%s", sequential, concurrent)
	require.Less(t, concurrent, sequential/3, "rate limited download should keep several reads in flight")
}
