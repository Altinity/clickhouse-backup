package object_disk

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDeleteFilesUsesNativePrefixAndPreservesPartialErrors(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	present := map[string]bool{"native/a": true, "native/b": true}
	denyB := true
	var calls, singleCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("versioning") {
			_, _ = w.Write([]byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`))
			return
		}
		if r.Method == http.MethodDelete {
			mu.Lock()
			singleCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost || !r.URL.Query().Has("delete") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var request struct {
			Objects []struct{ Key string } `xml:"Object"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode batch: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		calls++
		_, _ = fmt.Fprint(w, `<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, object := range request.Objects {
			if !strings.HasPrefix(object.Key, "native/") {
				t.Errorf("wrong native prefix: %s", object.Key)
			}
			if object.Key == "native/b" && denyB {
				_, _ = fmt.Fprint(w, `<Error><Key>native/b</Key><Code>AccessDenied</Code><Message>denied</Message></Error>`)
				continue
			}
			delete(present, object.Key)
			_, _ = fmt.Fprintf(w, `<Deleted><Key>%s</Key></Deleted>`, object.Key)
		}
		_, _ = fmt.Fprint(w, `</DeleteResult>`)
	}))
	defer server.Close()
	s3 := &storage.S3{Config: &config.S3Config{
		Endpoint: server.URL, Region: "us-east-1", Bucket: "test-bucket",
		AccessKey: "test-access-key", SecretKey: "test-secret-key", ForcePathStyle: true,
		Path: "native", ObjectDiskPath: "wrong-backup-prefix", DeleteConcurrency: 2,
	}}
	require.NoError(t, s3.Connect(ctx))
	defer func() { require.NoError(t, s3.Close(ctx)) }()
	disk := t.Name()
	DisksConnections.Store(disk, &ObjectStorageConnection{Type: "s3", S3: s3})
	defer DisksConnections.Delete(disk)
	err := DeleteFiles(ctx, disk, []string{"a", "b"})
	var partial *storage.BatchDeleteError
	require.ErrorAs(t, err, &partial)
	require.Len(t, partial.Failures, 1)
	require.Equal(t, "native/b", partial.Failures[0].Key)
	mu.Lock()
	require.Equal(t, map[string]bool{"native/b": true}, present)
	denyB = false
	mu.Unlock()
	// A retry includes the already deleted key. S3 considers that successful.
	require.NoError(t, DeleteFiles(ctx, disk, []string{"a", "b"}))
	mu.Lock()
	require.Empty(t, present)
	require.Equal(t, 2, calls)
	mu.Unlock()
	// ClickHouse's support_batch_delete=false maps to this credential type,
	// while the connection still uses the S3 client for single deletes.
	DisksCredentials.Store(disk, ObjectStorageCredentials{Type: "gcs"})
	defer DisksCredentials.Delete(disk)
	require.NoError(t, DeleteFiles(ctx, disk, []string{"a", "b"}))
	mu.Lock()
	require.Equal(t, 2, calls, "do not send batches when the disk disables them")
	require.Equal(t, 2, singleCalls)
	mu.Unlock()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, DeleteFiles(canceled, disk, []string{"a"}), context.Canceled)
}

func TestNativeConnectionCopiesBatchSettings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.Query().Has("versioning") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
		_, _ = w.Write([]byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`))
	}))
	defer server.Close()
	disk := t.Name()
	SystemDisks.Store(disk, clickhouse.Disk{Name: disk, Type: "s3"})
	DisksCredentials.Store(disk, ObjectStorageCredentials{
		Type: "s3", EndPoint: server.URL + "/test-bucket/native/",
		S3AccessKey: "test-access-key", S3SecretKey: "test-secret-key",
	})
	defer SystemDisks.Delete(disk)
	defer DisksCredentials.Delete(disk)
	cfg := config.DefaultConfig()
	cfg.S3.DeleteConcurrency = 3
	cfg.S3.DeleteBatchMinSize = 7
	cfg.S3.DeleteBatchFallbackToSingle = true
	connection, err := makeObjectDiskConnection(context.Background(), &clickhouse.ClickHouse{}, cfg, disk)
	require.NoError(t, err)
	defer func() { require.NoError(t, connection.S3.Close(context.Background())) }()
	require.Equal(t, 3, connection.S3.Config.DeleteConcurrency)
	require.Equal(t, 7, connection.S3.Config.DeleteBatchMinSize)
	require.True(t, connection.S3.Config.DeleteBatchFallbackToSingle)
	require.Equal(t, "native", connection.S3.Config.Path)
}

func TestDeleteFilesRequiresConnection(t *testing.T) {
	err := DeleteFiles(context.Background(), t.Name(), []string{"object"})
	require.ErrorContains(t, err, "not present")
	require.NoError(t, DeleteFiles(context.Background(), t.Name(), nil))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, errors.Is(DeleteFiles(canceled, t.Name(), nil), context.Canceled))
}
