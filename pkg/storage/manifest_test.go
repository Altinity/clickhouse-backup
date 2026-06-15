package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBackupManifest(t *testing.T) {
	t.Parallel()
	m := NewBackupManifest("test_backup")
	assert.Equal(t, ManifestVersion, m.Version)
	assert.Equal(t, "test_backup", m.BackupName)
	assert.NotZero(t, m.CreatedAt)
	assert.Empty(t, m.Files)
	assert.Equal(t, 0, m.TotalFiles)
	assert.Equal(t, int64(0), m.TotalSize)
}

func TestBackupManifest_AddFile(t *testing.T) {
	t.Parallel()
	m := NewBackupManifest("test_backup")
	now := time.Now().UTC()

	m.AddFile("shadow/default/table1/default/part1/data.bin", 12345, now)
	m.AddFile("shadow/default/table1/default/part1/primary.idx", 456, now)
	m.AddFile("metadata/default/table1.json", 789, now)

	assert.Equal(t, 3, m.TotalFiles)
	assert.Equal(t, int64(12345+456+789), m.TotalSize)
	assert.Len(t, m.Files, 3)

	assert.Equal(t, "shadow/default/table1/default/part1/data.bin", m.Files[0].Path)
	assert.Equal(t, int64(12345), m.Files[0].Size)
	assert.Equal(t, now, m.Files[0].LastModified)
}

func TestBackupManifest_FilesUnderPrefix(t *testing.T) {
	t.Parallel()
	m := NewBackupManifest("test_backup")
	now := time.Now().UTC()

	// Add files from different tables and parts
	m.AddFile("shadow/default/table1/default/part1/data.bin", 100, now)
	m.AddFile("shadow/default/table1/default/part1/primary.idx", 50, now)
	m.AddFile("shadow/default/table1/default/part2/data.bin", 200, now)
	m.AddFile("shadow/default/table2/default/part1/data.bin", 300, now)
	m.AddFile("metadata/default/table1.json", 10, now)

	// Files under part1 of table1
	files := m.FilesUnderPrefix("shadow/default/table1/default/part1")
	assert.Len(t, files, 2)
	for _, f := range files {
		assert.Contains(t, f.Path, "shadow/default/table1/default/part1/")
	}

	// Files under table1 (all parts)
	files = m.FilesUnderPrefix("shadow/default/table1")
	assert.Len(t, files, 3) // part1/data.bin, part1/primary.idx, part2/data.bin

	// Files under table2
	files = m.FilesUnderPrefix("shadow/default/table2")
	assert.Len(t, files, 1)

	// Files under metadata
	files = m.FilesUnderPrefix("metadata")
	assert.Len(t, files, 1)

	// Non-existent prefix
	files = m.FilesUnderPrefix("shadow/default/table3")
	assert.Len(t, files, 0)
}

func TestBackupManifest_FilesUnderPrefix_TrailingSlash(t *testing.T) {
	t.Parallel()
	m := NewBackupManifest("test")
	now := time.Now().UTC()
	m.AddFile("shadow/db/tbl/disk/part1/file.bin", 100, now)

	// Should work with or without trailing slash
	files1 := m.FilesUnderPrefix("shadow/db/tbl/disk/part1")
	files2 := m.FilesUnderPrefix("shadow/db/tbl/disk/part1/")
	assert.Equal(t, len(files1), len(files2))
}

func TestBackupManifest_MarshalUnmarshal(t *testing.T) {
	t.Parallel()
	m := NewBackupManifest("my_backup_2025")
	now := time.Now().UTC().Truncate(time.Millisecond) // truncate for JSON roundtrip

	m.AddFile("shadow/default/orders/default/20250101_1_1_0/data.bin", 1048576, now)
	m.AddFile("shadow/default/orders/default/20250101_1_1_0/primary.idx", 256, now)
	m.AddFile("metadata/default/orders.json", 1024, now)

	data, err := m.Marshal()
	require.NoError(t, err)
	assert.Contains(t, string(data), `"version": 1`)
	assert.Contains(t, string(data), `"backup_name": "my_backup_2025"`)
	assert.Contains(t, string(data), `"total_files": 3`)

	// Unmarshal
	m2, err := UnmarshalManifest(data)
	require.NoError(t, err)
	assert.Equal(t, m.Version, m2.Version)
	assert.Equal(t, m.BackupName, m2.BackupName)
	assert.Equal(t, m.TotalFiles, m2.TotalFiles)
	assert.Equal(t, m.TotalSize, m2.TotalSize)
	assert.Len(t, m2.Files, 3)

	// Verify file entries round-trip
	assert.Equal(t, m.Files[0].Path, m2.Files[0].Path)
	assert.Equal(t, m.Files[0].Size, m2.Files[0].Size)
	assert.Equal(t, m.Files[1].Path, m2.Files[1].Path)
	assert.Equal(t, m.Files[2].Path, m2.Files[2].Path)
}

func TestUnmarshalManifest_InvalidJSON(t *testing.T) {
	t.Parallel()
	_, err := UnmarshalManifest([]byte(`not json`))
	assert.Error(t, err)
}

func TestUnmarshalManifest_EmptyFiles(t *testing.T) {
	t.Parallel()
	data := []byte(`{"version":1,"backup_name":"empty","created_at":"2025-05-16T00:00:00Z","total_size":0,"total_files":0,"files":[]}`)
	m, err := UnmarshalManifest(data)
	require.NoError(t, err)
	assert.Equal(t, "empty", m.BackupName)
	assert.Empty(t, m.Files)
}

func TestManifestEntryToRemoteFile(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	entry := ManifestEntry{
		Path:         "shadow/default/table1/default/part1/data.bin",
		Size:         12345,
		LastModified: now,
	}

	// No prefix stripping
	rf := ManifestEntryToRemoteFile(entry, "")
	assert.Equal(t, "shadow/default/table1/default/part1/data.bin", rf.Name())
	assert.Equal(t, int64(12345), rf.Size())
	assert.Equal(t, now, rf.LastModified())

	// With prefix stripping
	rf2 := ManifestEntryToRemoteFile(entry, "shadow/default/table1/default/part1")
	assert.Equal(t, "data.bin", rf2.Name())
	assert.Equal(t, int64(12345), rf2.Size())
}

func TestManifestEntryToRemoteFile_PrefixWithSlash(t *testing.T) {
	t.Parallel()
	entry := ManifestEntry{
		Path: "shadow/db/tbl/disk/part/subdir/file.bin",
		Size: 100,
	}
	rf := ManifestEntryToRemoteFile(entry, "shadow/db/tbl/disk/part/")
	assert.Equal(t, "subdir/file.bin", rf.Name())
}

func TestBackupManifest_LargeFileCount(t *testing.T) {
	t.Parallel()
	m := NewBackupManifest("large_backup")
	now := time.Now().UTC()

	// Simulate a real backup with 10k files
	for i := 0; i < 10000; i++ {
		m.AddFile(
			"shadow/default/trades/default/20250101_1_1_0/column"+string(rune('A'+i%26))+".bin",
			int64(1024+i),
			now,
		)
	}

	assert.Equal(t, 10000, m.TotalFiles)
	assert.Greater(t, m.TotalSize, int64(0))

	// Marshal and unmarshal should work
	data, err := m.Marshal()
	require.NoError(t, err)

	m2, err := UnmarshalManifest(data)
	require.NoError(t, err)
	assert.Equal(t, 10000, m2.TotalFiles)
	assert.Equal(t, m.TotalSize, m2.TotalSize)
}

func TestManifestFileName_IsCorrect(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "manifest.json", ManifestFileName)
}

func TestManifestVersion_IsOne(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 1, ManifestVersion)
}

func TestBackupManifest_HasFile(t *testing.T) {
	t.Parallel()
	m := NewBackupManifest("test")
	now := time.Now().UTC()

	m.AddFile("metadata/default/orders.json", 100, now)
	m.AddFile("shadow/default/orders/default/part1/data.bin", 200, now)

	assert.True(t, m.HasFile("metadata/default/orders.json"))
	assert.True(t, m.HasFile("shadow/default/orders/default/part1/data.bin"))
	assert.False(t, m.HasFile("metadata/default/nonexistent.json"))
	assert.False(t, m.HasFile("metadata/default/orders.json/"))
	assert.False(t, m.HasFile(""))
}

func TestNewBackupManifestWithCapacity(t *testing.T) {
	t.Parallel()
	m := NewBackupManifestWithCapacity("test_backup", 5000)
	assert.Equal(t, ManifestVersion, m.Version)
	assert.Equal(t, "test_backup", m.BackupName)
	assert.NotZero(t, m.CreatedAt)
	assert.Empty(t, m.Files)
	assert.Equal(t, 5000, cap(m.Files))
	assert.Equal(t, 0, m.TotalFiles)
	assert.Equal(t, int64(0), m.TotalSize)
}

func TestNewBackupManifestWithCapacity_MinimumFloor(t *testing.T) {
	t.Parallel()
	m := NewBackupManifestWithCapacity("test", 10)
	assert.Equal(t, 256, cap(m.Files), "capacity should be at least 256")
}

func TestNewBackupManifestWithCapacity_ZeroCapacity(t *testing.T) {
	t.Parallel()
	m := NewBackupManifestWithCapacity("test", 0)
	assert.Equal(t, 256, cap(m.Files), "zero capacity should use floor of 256")
}

func TestNewBackupManifestWithCapacity_NegativeCapacity(t *testing.T) {
	t.Parallel()
	m := NewBackupManifestWithCapacity("test", -100)
	assert.Equal(t, 256, cap(m.Files), "negative capacity should use floor of 256")
}

func TestNewBackupManifestWithCapacity_CorrectBehavior(t *testing.T) {
	t.Parallel()
	// Pre-sized manifest should produce identical results to default manifest
	now := time.Now().UTC()
	m1 := NewBackupManifest("test")
	m2 := NewBackupManifestWithCapacity("test", 10000)

	for i := 0; i < 1000; i++ {
		path := "shadow/default/table/default/part/col" + string(rune('A'+i%26)) + ".bin"
		m1.AddFile(path, int64(i*100), now)
		m2.AddFile(path, int64(i*100), now)
	}

	assert.Equal(t, m1.TotalFiles, m2.TotalFiles)
	assert.Equal(t, m1.TotalSize, m2.TotalSize)
	assert.Equal(t, len(m1.Files), len(m2.Files))

	// Verify marshal/unmarshal produces identical output
	data1, err1 := m1.Marshal()
	data2, err2 := m2.Marshal()
	require.NoError(t, err1)
	require.NoError(t, err2)

	// Unmarshal and compare (can't compare JSON directly due to timestamp)
	um1, _ := UnmarshalManifest(data1)
	um2, _ := UnmarshalManifest(data2)
	assert.Equal(t, um1.TotalFiles, um2.TotalFiles)
	assert.Equal(t, um1.TotalSize, um2.TotalSize)
}

func TestNewBackupManifestWithCapacity_LargeCapacityNoAlloc(t *testing.T) {
	t.Parallel()
	// Verify that pre-sizing to exact capacity produces correct results
	m := NewBackupManifestWithCapacity("test", 50000)
	now := time.Now().UTC()

	for i := 0; i < 50000; i++ {
		m.AddFile("shadow/default/t/d/p/f.bin", int64(i), now)
	}
	assert.Equal(t, 50000, m.TotalFiles)
	// Pre-sized capacity should prevent any reallocation
	assert.GreaterOrEqual(t, cap(m.Files), 50000)
}

func TestBackupManifest_FilesUnderPrefix_NoPartialMatch(t *testing.T) {
	t.Parallel()
	m := NewBackupManifest("test")
	now := time.Now().UTC()

	// "part1" should not match "part10" or "part1_suffix"
	m.AddFile("shadow/db/tbl/disk/part1/file.bin", 100, now)
	m.AddFile("shadow/db/tbl/disk/part10/file.bin", 200, now)
	m.AddFile("shadow/db/tbl/disk/part1_suffix/file.bin", 300, now)

	files := m.FilesUnderPrefix("shadow/db/tbl/disk/part1")
	assert.Len(t, files, 1, "should only match exact prefix with trailing slash")
	assert.Equal(t, "shadow/db/tbl/disk/part1/file.bin", files[0].Path)
}
