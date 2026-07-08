package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// The TestRebase* unit tests cover the negative paths of Rebase/rebaseTable which are
// too expensive to reproduce in test/integration (broken chains, unreadable metadata,
// storage failures): rebaseRemoteMock is an in-memory RemoteStorage injected via the
// embedded interface of storage.BackupDestination, `errs` breaks exactly one
// "<Method> <key>" call. The positive scenarios stay in test/integration/rebase_test.go.

type rebaseMockFile struct {
	name string
	size int64
}

func (f rebaseMockFile) Size() int64             { return f.size }
func (f rebaseMockFile) Name() string            { return f.name }
func (f rebaseMockFile) LastModified() time.Time { return time.Unix(1, 0) }

type rebaseRemoteMock struct {
	storage.RemoteStorage
	kind   string
	mu     sync.Mutex
	files  map[string][]byte
	errs   map[string]error
	copied map[string]string // dstKey -> srcKey
}

func (m *rebaseRemoteMock) Kind() string { return m.kind }

func (m *rebaseRemoteMock) StatFile(_ context.Context, key string) (storage.RemoteFile, error) {
	if err := m.errs["StatFile "+key]; err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	body, exists := m.files[key]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return rebaseMockFile{name: path.Base(key), size: int64(len(body))}, nil
}

func (m *rebaseRemoteMock) GetFileReader(_ context.Context, key string) (io.ReadCloser, error) {
	if err := m.errs["GetFileReader "+key]; err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	body, exists := m.files[key]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (m *rebaseRemoteMock) Walk(ctx context.Context, prefix string, _ bool, fn func(context.Context, storage.RemoteFile) error) error {
	if err := m.errs["Walk "+prefix]; err != nil {
		return err
	}
	m.mu.Lock()
	names := make([]string, 0)
	for key := range m.files {
		if name, found := strings.CutPrefix(key, prefix); found {
			names = append(names, name)
		}
	}
	m.mu.Unlock()
	sort.Strings(names)
	for _, name := range names {
		m.mu.Lock()
		size := int64(len(m.files[prefix+name]))
		m.mu.Unlock()
		if err := fn(ctx, rebaseMockFile{name: name, size: size}); err != nil {
			return err
		}
	}
	return nil
}

func (m *rebaseRemoteMock) PutFile(_ context.Context, key string, r io.ReadCloser, _ int64) error {
	if err := m.errs["PutFile "+key]; err != nil {
		return err
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.files[key] = body
	m.mu.Unlock()
	return nil
}

func (m *rebaseRemoteMock) CopyObject(_ context.Context, srcSize int64, _, srcKey, dstKey string) (int64, error) {
	if err := m.errs["CopyObject "+srcKey]; err != nil {
		return 0, err
	}
	m.mu.Lock()
	m.copied[dstKey] = srcKey
	m.mu.Unlock()
	return srcSize, nil
}

const (
	rebaseTestDisk  = "default"
	rebaseTestDB    = "rebase_db"
	rebaseTestTable = "t1"
)

func rebaseTableMetaKey(backupName string) string {
	return path.Join(backupName, "metadata", common.TablePathEncode(rebaseTestDB), common.TablePathEncode(rebaseTestTable)+".json")
}

func rebasePartKey(backupName, partName, fileName string) string {
	return path.Join(backupName, "shadow", common.TablePathEncode(rebaseTestDB), common.TablePathEncode(rebaseTestTable), rebaseTestDisk, partName, fileName)
}

func rebaseArchiveName(partName string) string {
	return fmt.Sprintf("%s_%s.tar", rebaseTestDisk, common.TablePathEncode(partName))
}

func rebaseArchiveKey(backupName, partName string) string {
	return path.Join(backupName, "shadow", common.TablePathEncode(rebaseTestDB), common.TablePathEncode(rebaseTestTable), rebaseArchiveName(partName))
}

// rebaseFixture - `full` <- `inc1` <- `inc2` chain: part_full data lives in `full` (marked
// required in both increments — the deep-chain case), part_inc1 in `inc1`, part_inc2 in `inc2`
type rebaseFixture struct {
	b     *Backuper
	mock  *rebaseRemoteMock
	full  *metadata.BackupMetadata
	inc1  *metadata.BackupMetadata
	inc2  *metadata.BackupMetadata
	chain []*metadata.BackupMetadata
	table metadata.TableTitle
}

func (f *rebaseFixture) putBackupMeta(t *testing.T, bm *metadata.BackupMetadata) {
	body, err := json.Marshal(bm)
	require.NoError(t, err)
	f.mock.files[path.Join(bm.BackupName, "metadata.json")] = body
}

func (f *rebaseFixture) putTableMeta(t *testing.T, backupName string, tm *metadata.TableMetadata) {
	body, err := json.Marshal(tm)
	require.NoError(t, err)
	f.mock.files[rebaseTableMetaKey(backupName)] = body
}

func (f *rebaseFixture) setDiskType(diskType string) {
	for _, bm := range []*metadata.BackupMetadata{f.full, f.inc1, f.inc2} {
		bm.DiskTypes[rebaseTestDisk] = diskType
	}
}

func (f *rebaseFixture) rebaseTable(t *testing.T) (int64, int64, int64, error) {
	t.Helper()
	return f.b.rebaseTable(context.Background(), f.inc2, f.chain, f.table, "mock-bucket", "", nil)
}

func newRebaseFixture(t *testing.T, dataFormat string) *rebaseFixture {
	cfg := &config.Config{}
	cfg.General.RemoteStorage = "s3"
	cfg.General.RetriesOnFailure = 1
	cfg.General.RetriesDuration = time.Millisecond
	cfg.S3.Bucket = "mock-bucket"
	cfg.S3.ObjectDiskPath = "disks-cold"
	b := NewBackuper(cfg)

	// unique Kind per test: BackupList caches metadata in os.TempDir() keyed by Kind()
	kind := "rebase-unit-" + strings.ReplaceAll(t.Name(), "/", "_")
	mock := &rebaseRemoteMock{
		kind:   kind,
		files:  map[string][]byte{},
		errs:   map[string]error{},
		copied: map[string]string{},
	}
	t.Cleanup(func() {
		_ = os.Remove(path.Join(os.TempDir(), fmt.Sprintf(".clickhouse-backup-metadata.cache.%s", kind)))
	})
	b.dst = &storage.BackupDestination{RemoteStorage: mock}

	table := metadata.TableTitle{Database: rebaseTestDB, Table: rebaseTestTable}
	newBackupMeta := func(name, requiredBackup string) *metadata.BackupMetadata {
		return &metadata.BackupMetadata{
			BackupName:     name,
			RequiredBackup: requiredBackup,
			DataFormat:     dataFormat,
			DiskTypes:      map[string]string{rebaseTestDisk: "local"},
			Tables:         []metadata.TableTitle{table},
		}
	}
	f := &rebaseFixture{
		b:     b,
		mock:  mock,
		full:  newBackupMeta("full", ""),
		inc1:  newBackupMeta("inc1", "full"),
		inc2:  newBackupMeta("inc2", "inc1"),
		table: table,
	}
	f.chain = []*metadata.BackupMetadata{f.inc1, f.full}

	f.putTableMeta(t, "full", &metadata.TableMetadata{
		Database: rebaseTestDB, Table: rebaseTestTable,
		Parts:          map[string][]metadata.Part{rebaseTestDisk: {{Name: "part_full"}}},
		HashOfAllFiles: map[string]string{"part_full": "hash-full"},
		Checksums:      map[string]uint64{"part_full": 42},
	})
	f.putTableMeta(t, "inc1", &metadata.TableMetadata{
		Database: rebaseTestDB, Table: rebaseTestTable,
		Parts:          map[string][]metadata.Part{rebaseTestDisk: {{Name: "part_full", Required: true}, {Name: "part_inc1"}}},
		HashOfAllFiles: map[string]string{"part_inc1": "hash-inc1"},
		Checksums:      map[string]uint64{"part_inc1": 43},
	})
	f.putTableMeta(t, "inc2", &metadata.TableMetadata{
		Database: rebaseTestDB, Table: rebaseTestTable,
		Parts: map[string][]metadata.Part{rebaseTestDisk: {{Name: "part_full", Required: true}, {Name: "part_inc1", Required: true}, {Name: "part_inc2"}}},
	})

	if dataFormat == DirectoryFormat {
		f.mock.files[rebasePartKey("full", "part_full", "data.bin")] = []byte("full-part-data")
		f.mock.files[rebasePartKey("inc1", "part_inc1", "data.bin")] = []byte("inc1-part-data")
	} else {
		f.mock.files[rebaseArchiveKey("full", "part_full")] = []byte("full-part-archive-body")
		f.mock.files[rebaseArchiveKey("inc1", "part_inc1")] = []byte("inc1-part-archive-body")
	}
	return f
}

// TestRebaseEarlyErrors - Rebase argument/config validation reachable without a ClickHouse connection
func TestRebaseEarlyErrors(t *testing.T) {
	cfg := &config.Config{}
	cfg.General.RemoteStorage = "s3"
	require.EqualError(t, NewBackuper(cfg).Rebase("", status.NotFromAPI), "backup name must be defined")

	for _, remoteStorage := range []string{"none", "custom"} {
		cfg = &config.Config{}
		cfg.General.RemoteStorage = remoteStorage
		err := NewBackuper(cfg).Rebase("rebase_unit_early_"+remoteStorage, status.NotFromAPI)
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("rebase does not support `remote_storage: %s`", remoteStorage))
	}
}

// TestRebaseBackupErrors - errors between b.dst initialization and the per-table rebase
func TestRebaseBackupErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("backup missing on remote", func(t *testing.T) {
		f := newRebaseFixture(t, DirectoryFormat)
		err := f.b.rebaseBackup(ctx, "unknown_backup")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown_backup not found on remote storage")
	})

	t.Run("nothing to rebase", func(t *testing.T) {
		f := newRebaseFixture(t, DirectoryFormat)
		f.putBackupMeta(t, f.full)
		err := f.b.rebaseBackup(ctx, "full")
		require.Error(t, err)
		require.Contains(t, err.Error(), "backup full doesn't contain `required_backup`, nothing to rebase")
	})

	t.Run("embedded backup not supported", func(t *testing.T) {
		f := newRebaseFixture(t, DirectoryFormat)
		f.inc2.Tags = "embedded"
		f.putBackupMeta(t, f.inc2)
		err := f.b.rebaseBackup(ctx, "inc2")
		require.EqualError(t, err, "rebase not supported for embedded backup inc2")
	})
}

func TestRebaseReadRequiredBackupsChain(t *testing.T) {
	ctx := context.Background()

	t.Run("nearest ancestor first", func(t *testing.T) {
		f := newRebaseFixture(t, DirectoryFormat)
		f.putBackupMeta(t, f.inc1)
		f.putBackupMeta(t, f.full)
		chain, err := f.b.readRequiredBackupsChain(ctx, f.inc2)
		require.NoError(t, err)
		require.Len(t, chain, 2)
		require.Equal(t, "inc1", chain[0].BackupName)
		require.Equal(t, "full", chain[1].BackupName)
	})

	t.Run("cycle", func(t *testing.T) {
		f := newRebaseFixture(t, DirectoryFormat)
		f.inc1.RequiredBackup = "inc2"
		f.putBackupMeta(t, f.inc1)
		_, err := f.b.readRequiredBackupsChain(ctx, f.inc2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "required backups chain contains a cycle on inc2")
	})

	t.Run("embedded ancestor", func(t *testing.T) {
		f := newRebaseFixture(t, DirectoryFormat)
		f.inc1.Tags = "embedded"
		f.putBackupMeta(t, f.inc1)
		_, err := f.b.readRequiredBackupsChain(ctx, f.inc2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "required backup inc1 is embedded, rebase not supported")
	})

	t.Run("data_format mismatch", func(t *testing.T) {
		f := newRebaseFixture(t, DirectoryFormat)
		f.inc1.DataFormat = "tar"
		f.putBackupMeta(t, f.inc1)
		_, err := f.b.readRequiredBackupsChain(ctx, f.inc2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "rebase requires the same compression_format and data_format")
	})

	t.Run("ancestor missing on remote", func(t *testing.T) {
		f := newRebaseFixture(t, DirectoryFormat)
		for key := range f.mock.files {
			if strings.HasPrefix(key, "inc1/") {
				delete(f.mock.files, key)
			}
		}
		_, err := f.b.readRequiredBackupsChain(ctx, f.inc2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "inc1 not found on remote storage")
	})
}

// TestRebaseTableDirectory - the deep-chain case from findRequiredPartInChain: part_full is
// `required` in both inc2 and inc1, so its data must be copied directly from `full` into `inc2`
func TestRebaseTableDirectory(t *testing.T) {
	f := newRebaseFixture(t, DirectoryFormat)
	oldMetaSize := int64(len(f.mock.files[rebaseTableMetaKey("inc2")]))

	copiedBytes, objectDiskBytes, metadataDiff, err := f.rebaseTable(t)
	require.NoError(t, err)
	require.Equal(t, int64(len("full-part-data")+len("inc1-part-data")), copiedBytes)
	require.Equal(t, int64(0), objectDiskBytes)

	require.Equal(t, map[string]string{
		rebasePartKey("inc2", "part_full", "data.bin"): rebasePartKey("full", "part_full", "data.bin"),
		rebasePartKey("inc2", "part_inc1", "data.bin"): rebasePartKey("inc1", "part_inc1", "data.bin"),
	}, f.mock.copied)

	newMetaBody := f.mock.files[rebaseTableMetaKey("inc2")]
	require.Equal(t, int64(len(newMetaBody))-oldMetaSize, metadataDiff)
	tm := &metadata.TableMetadata{}
	require.NoError(t, json.Unmarshal(newMetaBody, tm))
	require.Len(t, tm.Parts[rebaseTestDisk], 3)
	for _, p := range tm.Parts[rebaseTestDisk] {
		require.Falsef(t, p.Required, "part %s still marked required after rebase", p.Name)
	}
	require.Equal(t, map[string]string{"part_full": "hash-full", "part_inc1": "hash-inc1"}, tm.HashOfAllFiles)
	require.Equal(t, map[string]uint64{"part_full": 42, "part_inc1": 43}, tm.Checksums)
	require.Empty(t, tm.Files, "directory format must not append archives to Files")
}

func TestRebaseTableArchive(t *testing.T) {
	f := newRebaseFixture(t, "tar")

	copiedBytes, objectDiskBytes, _, err := f.rebaseTable(t)
	require.NoError(t, err)
	require.Equal(t, int64(len("full-part-archive-body")+len("inc1-part-archive-body")), copiedBytes)
	require.Equal(t, int64(0), objectDiskBytes)

	require.Equal(t, map[string]string{
		rebaseArchiveKey("inc2", "part_full"): rebaseArchiveKey("full", "part_full"),
		rebaseArchiveKey("inc2", "part_inc1"): rebaseArchiveKey("inc1", "part_inc1"),
	}, f.mock.copied)

	tm := &metadata.TableMetadata{}
	require.NoError(t, json.Unmarshal(f.mock.files[rebaseTableMetaKey("inc2")], tm))
	require.ElementsMatch(t, []string{rebaseArchiveName("part_full"), rebaseArchiveName("part_inc1")}, tm.Files[rebaseTestDisk])
}

func TestRebaseTableNothingRequired(t *testing.T) {
	f := newRebaseFixture(t, DirectoryFormat)
	f.putTableMeta(t, "inc2", &metadata.TableMetadata{
		Database: rebaseTestDB, Table: rebaseTestTable,
		Parts: map[string][]metadata.Part{rebaseTestDisk: {{Name: "part_inc2"}}},
	})
	oldMetaBody := f.mock.files[rebaseTableMetaKey("inc2")]

	copiedBytes, objectDiskBytes, metadataDiff, err := f.rebaseTable(t)
	require.NoError(t, err)
	require.Equal(t, int64(0), copiedBytes)
	require.Equal(t, int64(0), objectDiskBytes)
	require.Equal(t, int64(0), metadataDiff)
	require.Empty(t, f.mock.copied)
	require.Equal(t, oldMetaBody, f.mock.files[rebaseTableMetaKey("inc2")], "table metadata must not be re-uploaded when nothing changed")
}

// TestRebaseTableObjectDiskBlobs - parts on an object disk keep only small metadata files in
// shadow, the referenced blobs under `object_disk_path` must be copied too (directory data_format,
// the archive branch needs a real compressed stream and stays covered by test/integration)
func TestRebaseTableObjectDiskBlobs(t *testing.T) {
	f := newRebaseFixture(t, DirectoryFormat)
	f.setDiskType("s3")
	// object disk metadata VersionRelativePath: version, count+total, size+path per object, refcount;
	// zero-size objects must be skipped, frozen_metadata.txt must not be parsed as blob metadata
	f.mock.files[rebasePartKey("full", "part_full", "data.bin")] = []byte("2\n2\t10\n10\txyz/blob1\n0\txyz/blob0\n0\n")
	f.mock.files[rebasePartKey("full", "part_full", "frozen_metadata.txt")] = []byte("not blob metadata")
	f.mock.files[rebasePartKey("inc1", "part_inc1", "data.bin")] = []byte("2\n1\t7\n7\tabc/blob2\n0\n")

	_, objectDiskBytes, _, err := f.rebaseTable(t)
	require.NoError(t, err)
	require.Equal(t, int64(10+7), objectDiskBytes)

	require.Equal(t, rebasePartKey("full", "part_full", "data.bin"), f.mock.copied[rebasePartKey("inc2", "part_full", "data.bin")])
	require.Equal(t, "disks-cold/full/default/xyz/blob1", f.mock.copied["disks-cold/inc2/default/xyz/blob1"])
	require.Equal(t, "disks-cold/inc1/default/abc/blob2", f.mock.copied["disks-cold/inc2/default/abc/blob2"])
	_, blob0Copied := f.mock.copied["disks-cold/inc2/default/xyz/blob0"]
	require.False(t, blob0Copied, "zero-size storage objects must be skipped")
}

func TestRebaseTableErrors(t *testing.T) {
	mockErr := errors.New("mock storage failure")
	testCases := []struct {
		name       string
		dataFormat string
		mutate     func(t *testing.T, f *rebaseFixture)
		wantErr    string
	}{
		{
			name:       "target table metadata unreadable",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.mock.errs["GetFileReader "+rebaseTableMetaKey("inc2")] = mockErr
			},
			wantErr: "readRemoteTableMetadata(inc2",
		},
		{
			name:       "target table metadata corrupt",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.mock.files[rebaseTableMetaKey("inc2")] = []byte("{invalid json")
			},
			wantErr: "json.Unmarshal",
		},
		{
			name:       "ancestor table metadata unreadable",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.mock.errs["GetFileReader "+rebaseTableMetaKey("inc1")] = mockErr
			},
			wantErr: "readRemoteTableMetadata(inc1",
		},
		{
			name:       "part absent in the whole chain",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.putTableMeta(t, "full", &metadata.TableMetadata{
					Database: rebaseTestDB, Table: rebaseTestTable,
					Parts: map[string][]metadata.Part{rebaseTestDisk: {{Name: "part_other"}}},
				})
			},
			wantErr: "part part_full not found in the whole required backups chain",
		},
		{
			name:       "part required through the whole chain",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.putTableMeta(t, "full", &metadata.TableMetadata{
					Database: rebaseTestDB, Table: rebaseTestTable,
					Parts: map[string][]metadata.Part{rebaseTestDisk: {{Name: "part_full", Required: true}}},
				})
			},
			wantErr: "part part_full not found in the whole required backups chain",
		},
		{
			name:       "disk type mismatch",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.full.DiskTypes[rebaseTestDisk] = "s3"
			},
			wantErr: "doesn't match disk",
		},
		{
			name:       "directory part files absent",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				delete(f.mock.files, rebasePartKey("full", "part_full", "data.bin"))
			},
			wantErr: "part part_full not found in full/shadow",
		},
		{
			name:       "directory Walk fails",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.mock.errs["Walk "+rebasePartKey("full", "part_full", "")+"/"] = mockErr
			},
			wantErr: "Walk full/shadow",
		},
		{
			name:       "directory CopyObject fails",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.mock.errs["CopyObject "+rebasePartKey("full", "part_full", "data.bin")] = mockErr
			},
			wantErr: "CopyObject",
		},
		{
			name:       "archive absent means upload_by_part false",
			dataFormat: "tar",
			mutate: func(t *testing.T, f *rebaseFixture) {
				delete(f.mock.files, rebaseArchiveKey("full", "part_full"))
			},
			wantErr: "only backups uploaded with `upload_by_part: true` can be rebased",
		},
		{
			name:       "archive CopyObject fails",
			dataFormat: "tar",
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.mock.errs["CopyObject "+rebaseArchiveKey("full", "part_full")] = mockErr
			},
			wantErr: "CopyObject",
		},
		{
			name:       "encrypted disk absent in system.disks",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.setDiskType("encrypted")
			},
			wantErr: "can't determine underlying disk type",
		},
		{
			name:       "object disk blob metadata corrupt",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.setDiskType("s3")
			},
			wantErr: "object_disk.ReadMetadataFromReader",
		},
		{
			name:       "object disk blob CopyObject fails",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.setDiskType("s3")
				f.mock.files[rebasePartKey("full", "part_full", "data.bin")] = []byte("2\n1\t10\n10\txyz/blob1\n0\n")
				f.mock.errs["CopyObject disks-cold/full/default/xyz/blob1"] = mockErr
			},
			wantErr: "CopyObject disks-cold/full/default/xyz/blob1",
		},
		{
			name:       "table metadata upload fails",
			dataFormat: DirectoryFormat,
			mutate: func(t *testing.T, f *rebaseFixture) {
				f.mock.errs["PutFile "+rebaseTableMetaKey("inc2")] = mockErr
			},
			wantErr: "can't upload inc2/metadata",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRebaseFixture(t, tc.dataFormat)
			tc.mutate(t, f)
			_, _, _, err := f.rebaseTable(t)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
