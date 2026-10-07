package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/stretchr/testify/require"
)

// Embedded backup data files contain object-disk pointers even when the native
// file is named serialization.json. The tool's own JSON files contain JSON and
// must not be parsed as pointers.
func TestCleanEmbeddedIncludesNativeJSON(t *testing.T) {
	for _, location := range []string{"local", "remote", "remote_tar", "remote_gzip", "remote_local_disk"} {
		t.Run(location, func(t *testing.T) {
			ctx := context.Background()
			var mu sync.Mutex
			var deleted []string
			var walked bool
			remoteFiles := make(map[string][]byte)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Query().Has("versioning"):
					_, _ = w.Write([]byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`))
				case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
					listing := struct {
						XMLName     xml.Name `xml:"ListBucketResult"`
						IsTruncated bool
						Contents    []embeddedCleanupS3Object
					}{}
					mu.Lock()
					walked = true
					var keys []string
					for key := range remoteFiles {
						if strings.HasPrefix(key, r.URL.Query().Get("prefix")) {
							keys = append(keys, key)
						}
					}
					sort.Strings(keys)
					for _, key := range keys {
						listing.Contents = append(listing.Contents, embeddedCleanupS3Object{
							Key: key, Size: len(remoteFiles[key]), LastModified: "2025-01-01T00:00:00Z",
						})
					}
					mu.Unlock()
					if err := xml.NewEncoder(w).Encode(listing); err != nil {
						t.Errorf("encode S3 listing: %v", err)
					}
				case r.Method == http.MethodGet:
					mu.Lock()
					body, exists := remoteFiles[strings.TrimPrefix(r.URL.Path, "/test-bucket/")]
					mu.Unlock()
					if !exists {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					_, _ = w.Write(body)
				case r.Method == http.MethodDelete:
					mu.Lock()
					deleted = append(deleted, strings.TrimPrefix(r.URL.Path, "/test-bucket/"))
					mu.Unlock()
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			t.Cleanup(server.Close)
			s3 := &storage.S3{Config: &config.S3Config{
				Endpoint: server.URL, Region: "us-east-1", Bucket: "test-bucket",
				AccessKey: "test-access-key", SecretKey: "test-secret-key", ForcePathStyle: true,
			}}
			require.NoError(t, s3.Connect(ctx))
			t.Cleanup(func() { require.NoError(t, s3.Close(ctx)) })
			diskName := t.Name()
			object_disk.DisksCredentials.Store(diskName, object_disk.ObjectStorageCredentials{Type: "s3"})
			object_disk.DisksConnections.Store(diskName, &object_disk.ObjectStorageConnection{Type: "s3", S3: s3})
			t.Cleanup(func() {
				object_disk.DisksConnections.Delete(diskName)
				object_disk.DisksCredentials.Delete(diskName)
			})

			files := map[string][]byte{
				storage.ManifestFileName:                         []byte("compressed manifest, not an object pointer"),
				"metadata.json":                                  []byte(`{"backup_name":"test-backup"}`),
				"metadata/db/table.json":                         []byte(`{"database":"db","table":"table"}`),
				"metadata/data/table.json":                       []byte(`{"database":"data","table":"table"}`),
				"access/users.json":                              []byte(`{"users":[]}`),
				"access/user.sql":                                []byte("CREATE USER test_user"),
				"access/users.jsonl":                             []byte(`{"users":[]}`),
				"configs/server.json":                            []byte(`{"config":{}}`),
				"configs/config.d/server.xml":                    []byte("<clickhouse/>"),
				"named_collections/settings.json":                []byte(`{"settings":{}}`),
				"named_collections/collection.xml":               []byte("<clickhouse/>"),
				"shards/1/replicas/2/metadata/db/table.json":     []byte(`{"database":"db","table":"table"}`),
				"shards/1/replicas/2/metadata/shadow/table.json": []byte(`{"database":"shadow","table":"table"}`),
			}
			for _, directory := range []string{"access", "configs", "named_collections"} {
				for _, extension := range config.ArchiveExtensions {
					files[directory+"."+extension] = []byte("archive, not an object pointer")
				}
			}
			pointerFiles := []string{
				".backup",
				"metadata/db/table.sql",
				"data/db/table/all_1_1_0/checksums.txt",
				"data/db/table/all_1_1_0/serialization.json",
				"data/db/table/all_1_1_0/custom.json",
				"data/db/table/all_1_1_0/access.tar",
				"data/db/table/all_1_1_0/manifest.bolt.gz",
				"shadow/db/table/disk/all_1_1_0/configs.tar.gz",
				"shadow/db/table/disk/all_1_1_0/serialization.json",
				"shards/1/replicas/2/data/db/table/all_1_1_0/serialization.json",
				"shards/1/replicas/2/shadow/db/table/disk/all_1_1_0/serialization.json",
			}
			var expected []string
			for i, name := range pointerFiles {
				objectKey := fmt.Sprintf("native/object-%d", i)
				files[name] = []byte(fmt.Sprintf("5\n1\t265\n265\t%s\n0\n0\n\n", objectKey))
				expected = append(expected, objectKey)
			}
			b := &Backuper{cfg: &config.Config{ClickHouse: config.ClickHouseConfig{EmbeddedBackupDisk: diskName}}}
			backupMetadata := metadata.BackupMetadata{
				BackupName: "test-backup", DataFormat: DirectoryFormat,
				DiskTypes: map[string]string{diskName: "s3"},
			}
			if location == "remote_local_disk" {
				backupMetadata.DiskTypes[diskName] = "local"
				// A native backup on a local disk contains ordinary files, not
				// object pointers, even when the backup is uploaded to S3.
				for _, name := range pointerFiles {
					files[name] = []byte("ordinary backup data, not an object pointer")
				}
				expected = nil
			}
			if location == "local" {
				root := t.TempDir()
				for name, body := range files {
					file := filepath.Join(root, backupMetadata.BackupName, filepath.FromSlash(name))
					require.NoError(t, os.MkdirAll(filepath.Dir(file), 0750))
					require.NoError(t, os.WriteFile(file, body, 0640))
				}
				require.NoError(t, b.cleanLocalEmbedded(ctx, LocalBackup{BackupMetadata: backupMetadata}, []clickhouse.Disk{{
					Name: diskName, Type: "s3", Path: root,
				}}))
			} else {
				compressionFormat := "none"
				if format, compressed := strings.CutPrefix(location, "remote_"); compressed && location != "remote_local_disk" {
					compressionFormat = format
					backupMetadata.DataFormat = format
					var archive bytes.Buffer
					var target io.Writer = &archive
					var gz *gzip.Writer
					if format == "gzip" {
						gz = gzip.NewWriter(&archive)
						target = gz
					}
					tw := tar.NewWriter(target)
					for i, name := range pointerFiles {
						if name == ".backup" || strings.HasPrefix(name, "metadata/") {
							continue
						}
						body := files[name]
						entryName := fmt.Sprintf("all_%d_%d_0/%s", i, i, path.Base(name))
						require.NoError(t, tw.WriteHeader(&tar.Header{Name: entryName, Mode: 0640, Size: int64(len(body))}))
						_, err := tw.Write(body)
						require.NoError(t, err)
						delete(files, name)
					}
					require.NoError(t, tw.Close())
					if gz != nil {
						require.NoError(t, gz.Close())
					}
					files["shadow/db/table/disk_all_1_1_0."+config.ArchiveExtensions[format]] = archive.Bytes()
				}
				mu.Lock()
				for name, body := range files {
					remoteFiles[path.Join(backupMetadata.BackupName, name)] = body
				}
				mu.Unlock()
				b.cfg.General.RemoteStorage = "s3"
				b.cfg.S3.CompressionFormat = compressionFormat
				var err error
				b.dst, err = storage.NewBackupDestination(ctx, b.cfg, &clickhouse.ClickHouse{}, "")
				require.NoError(t, err)
				// Exercise real S3 Walk, including the leading slash in returned names.
				b.dst.RemoteStorage = s3
				require.NoError(t, b.cleanRemoteEmbedded(ctx, storage.Backup{BackupMetadata: backupMetadata}))
			}
			mu.Lock()
			defer mu.Unlock()
			require.ElementsMatch(t, expected, deleted)
			if location == "remote_local_disk" {
				require.False(t, walked, "local-disk backups must not enter object-pointer cleanup")
			}
		})
	}
}

type embeddedCleanupS3Object struct {
	Key          string
	Size         int
	LastModified string
}
