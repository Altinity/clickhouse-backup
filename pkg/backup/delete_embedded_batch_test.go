package backup

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/stretchr/testify/require"
)

func TestCleanRemoteEmbeddedRetainsCatalogOnFailure(t *testing.T) {
	for _, failure := range []string{"partial_delete", "pointer_parse", "pointer_read"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.Query().Has("versioning") {
					_, _ = w.Write([]byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`))
					return
				}
				if r.Method == http.MethodPost && r.URL.Query().Has("delete") && failure == "partial_delete" {
					_, _ = w.Write([]byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Deleted><Key>native/a</Key></Deleted><Error><Key>native/b</Key><Code>AccessDenied</Code><Message>denied</Message></Error></DeleteResult>`))
					return
				}
				t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				w.WriteHeader(http.StatusBadRequest)
			}))
			defer server.Close()
			s3 := &storage.S3{Config: &config.S3Config{
				Endpoint: server.URL, Region: "us-east-1", Bucket: "test-bucket",
				AccessKey: "test-access-key", SecretKey: "test-secret-key", ForcePathStyle: true,
				DeleteConcurrency: 2,
			}}
			require.NoError(t, s3.Connect(ctx))
			defer func() { require.NoError(t, s3.Close(ctx)) }()
			disk := t.Name()
			object_disk.DisksCredentials.Store(disk, object_disk.ObjectStorageCredentials{Type: "s3"})
			object_disk.DisksConnections.Store(disk, &object_disk.ObjectStorageConnection{Type: "s3", S3: s3})
			defer object_disk.DisksCredentials.Delete(disk)
			defer object_disk.DisksConnections.Delete(disk)
			files := map[string][]byte{
				"test/metadata.json":    []byte(`{"backup_name":"test"}`),
				"test/shadow/db/t/file": []byte("5\n2\t2\n1\tnative/a\n1\tnative/b\n0\n0\n\n"),
			}
			if failure == "pointer_parse" {
				files["test/shadow/db/t/file"] = []byte("malformed pointer")
			}
			catalog := &rebaseRemoteMock{files: files, errs: make(map[string]error)}
			if failure == "pointer_read" {
				catalog.errs["GetFileReader test/shadow/db/t/file"] = errors.New("pointer read denied")
			}
			before := make(map[string][]byte)
			for name, body := range files {
				before[name] = slices.Clone(body)
			}
			b := &Backuper{cfg: &config.Config{
				ClickHouse: config.ClickHouseConfig{EmbeddedBackupDisk: disk},
				General:    config.GeneralConfig{DeleteBatchSize: 2},
				S3:         config.S3Config{DeleteConcurrency: 2},
			}, dst: &storage.BackupDestination{RemoteStorage: catalog}}
			err := b.cleanRemoteEmbedded(ctx, storage.Backup{BackupMetadata: metadata.BackupMetadata{
				BackupName: "test", DataFormat: DirectoryFormat, DiskTypes: map[string]string{disk: "s3"},
			}})
			// RemoveBackupRemote returns this error before calling the catalog's
			// RemoveBackupRemote. Pointers remain available for the next attempt.
			require.Error(t, err)
			if failure == "partial_delete" {
				var partial *storage.BatchDeleteError
				require.ErrorAs(t, err, &partial)
				require.Len(t, partial.Failures, 1)
			}
			require.Equal(t, before, catalog.files)
		})
	}
}

func TestDeleteEmbeddedFilesBoundsAndFinalBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const workers, batchSize, files = 3, 8, 37
	var active, peak atomic.Int32
	started := make(chan struct{}, workers)
	release := make(chan struct{})
	var batches [][]string
	done := make(chan error, 1)
	go func() {
		done <- deleteEmbeddedFiles(ctx, workers, batchSize,
			func(ctx context.Context, emit func(string) error) error {
				for i := 0; i < files; i++ {
					if err := emit(fmt.Sprint(i)); err != nil {
						return err
					}
				}
				return nil
			},
			func(ctx context.Context, name string, emit func(string) error) error {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); old < n && !peak.CompareAndSwap(old, n); old = peak.Load() {
				}
				select {
				case started <- struct{}{}:
				default:
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-release:
				}
				for _, suffix := range []string{"/first", "/second"} {
					if err := emit(name + suffix); err != nil {
						return err
					}
				}
				return nil
			},
			func(ctx context.Context, keys []string) error {
				batches = append(batches, slices.Clone(keys))
				return nil
			})
	}()
	for i := 0; i < workers; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("pointer reads did not run concurrently")
		}
	}
	close(release)
	require.NoError(t, <-done)
	require.EqualValues(t, workers, peak.Load())
	require.Zero(t, active.Load())
	var actual, expected []string
	for i, batch := range batches {
		require.LessOrEqual(t, len(batch), batchSize)
		if i < len(batches)-1 {
			require.Len(t, batch, batchSize)
		}
		actual = append(actual, batch...)
	}
	for i := 0; i < files; i++ {
		expected = append(expected, fmt.Sprint(i)+"/first", fmt.Sprint(i)+"/second")
	}
	require.ElementsMatch(t, expected, actual)
	require.Len(t, batches[len(batches)-1], files*2%batchSize)
}

func TestDeleteEmbeddedFilesPropagatesErrors(t *testing.T) {
	for _, stage := range []string{"walk", "read", "delete", "partial_delete"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var want error = errors.New(stage + " failed")
			if stage == "partial_delete" {
				want = &storage.BatchDeleteError{Failures: []storage.KeyError{{Key: "native/1", Err: errors.New("access denied")}}}
			}
			err := deleteEmbeddedFiles(ctx, 3, 5,
				func(ctx context.Context, emit func(string) error) error {
					if stage == "walk" {
						return want
					}
					for i := 0; i < 10000; i++ {
						if err := emit(fmt.Sprint(i)); err != nil {
							return err
						}
					}
					return nil
				},
				func(ctx context.Context, name string, emit func(string) error) error {
					if stage == "read" {
						return want
					}
					return emit("native/" + name)
				},
				func(ctx context.Context, keys []string) error { return want })
			require.ErrorIs(t, err, want)
		})
	}
}

func TestDeleteEmbeddedFilesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- deleteEmbeddedFiles(ctx, 2, 5,
			func(ctx context.Context, emit func(string) error) error {
				for i := 0; ; i++ {
					if err := emit(fmt.Sprint(i)); err != nil {
						return err
					}
				}
			},
			func(ctx context.Context, name string, emit func(string) error) error {
				select {
				case started <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return ctx.Err()
			},
			func(ctx context.Context, keys []string) error { return errors.New("unexpected delete") })
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled deletion did not release all workers")
	}
}
