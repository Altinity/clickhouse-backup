package storage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// fakeS3Delete emulates an S3-compatible gateway where DeleteObjects fails for batches bigger than
// failBatchesAbove objects (0 = every batch fails) while single DeleteObject calls succeed,
// except for keys listed in failSingleKeys, see https://github.com/Altinity/clickhouse-backup/issues/1532
type fakeS3Delete struct {
	mu               sync.Mutex
	failBatchesAbove int
	failSingleKeys   map[string]bool
	batchSizes       []int
	singleDeleted    []string
}

func (f *fakeS3Delete) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
		body := make([]byte, r.ContentLength)
		_, _ = io.ReadFull(r.Body, body)
		size := strings.Count(string(body), "<Key>")
		f.batchSizes = append(f.batchSizes, size)
		if size > f.failBatchesAbove {
			// emulate a reset stream after 200 OK: the SDK fails with "deserialization failed"
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("<DeleteResult><Deleted>"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`))
	case r.Method == http.MethodDelete:
		key := strings.TrimPrefix(r.URL.Path, "/bucket/")
		if f.failSingleKeys[key] {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>boom</Message></Error>`))
			return
		}
		f.singleDeleted = append(f.singleDeleted, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func newFakeS3(t *testing.T, fake *fakeS3Delete, cfg *config.S3Config) *S3 {
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	client := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		HTTPClient:  srv.Client(),
		Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})
	cfg.Bucket = "bucket"
	if cfg.DeleteConcurrency == 0 {
		cfg.DeleteConcurrency = 4
	}
	return &S3{client: client, Config: cfg}
}

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "k" + string(rune('a'+i))
	}
	return out
}

func TestS3DeleteBatchFallbackToSingle(t *testing.T) {
	fake := &fakeS3Delete{}
	s := newFakeS3(t, fake, &config.S3Config{DeleteBatchFallbackToSingle: true})
	require.NoError(t, s.deleteKeys(context.Background(), keys(5)))
	require.Equal(t, []int{5}, fake.batchSizes)
	require.ElementsMatch(t, keys(5), fake.singleDeleted)
}

func TestS3DeleteBatchNoFallbackReturnsError(t *testing.T) {
	fake := &fakeS3Delete{}
	s := newFakeS3(t, fake, &config.S3Config{DeleteBatchFallbackToSingle: false})
	err := s.deleteKeys(context.Background(), keys(5))
	require.Error(t, err)
	require.Contains(t, err.Error(), "S3 batch delete failed for batch starting at index 0")
	require.Equal(t, []int{5}, fake.batchSizes)
	require.Empty(t, fake.singleDeleted)
}

func TestS3DeleteBatchMinSizeSplitsThenSingle(t *testing.T) {
	fake := &fakeS3Delete{}
	s := newFakeS3(t, fake, &config.S3Config{DeleteBatchFallbackToSingle: true, DeleteBatchMinSize: 2})
	require.NoError(t, s.deleteKeys(context.Background(), keys(8)))
	// 8 -> 4,4 -> 2,2,2,2; batches of 2 are not split further and fall back to singles
	require.Equal(t, []int{8, 4, 2, 2, 4, 2, 2}, fake.batchSizes)
	require.ElementsMatch(t, keys(8), fake.singleDeleted)
}

func TestS3DeleteBatchMinSizeSplitSucceeds(t *testing.T) {
	fake := &fakeS3Delete{failBatchesAbove: 4}
	s := newFakeS3(t, fake, &config.S3Config{DeleteBatchFallbackToSingle: true, DeleteBatchMinSize: 4})
	require.NoError(t, s.deleteKeys(context.Background(), keys(8)))
	require.Equal(t, []int{8, 4, 4}, fake.batchSizes)
	require.Empty(t, fake.singleDeleted)
}

func TestS3DeleteBatchMinSizeWithoutFallbackReturnsError(t *testing.T) {
	fake := &fakeS3Delete{}
	s := newFakeS3(t, fake, &config.S3Config{DeleteBatchFallbackToSingle: false, DeleteBatchMinSize: 4})
	err := s.deleteKeys(context.Background(), keys(8))
	require.Error(t, err)
	require.Equal(t, []int{8, 4}, fake.batchSizes)
	require.Empty(t, fake.singleDeleted)
}

func TestS3DeleteBatchFallbackCollectsSingleFailures(t *testing.T) {
	fake := &fakeS3Delete{failSingleKeys: map[string]bool{"kc": true}}
	s := newFakeS3(t, fake, &config.S3Config{DeleteBatchFallbackToSingle: true})
	err := s.deleteKeys(context.Background(), keys(5))
	var batchErr *BatchDeleteError
	require.True(t, errors.As(err, &batchErr), "expected BatchDeleteError, got %v", err)
	require.Len(t, batchErr.Failures, 1)
	require.Equal(t, "kc", batchErr.Failures[0].Key)
	require.ElementsMatch(t, []string{"ka", "kb", "kd", "ke"}, fake.singleDeleted)
}
