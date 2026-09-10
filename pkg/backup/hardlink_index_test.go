package backup

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/stretchr/testify/require"
)

// createLocalBackupFixture writes table metadata and the shadow part directory of a local backup
// exactly the way `clickhouse-backup create` lays them out on a local disk
func createLocalBackupFixture(t *testing.T, diskPath, diskName, backupName, database, table, partName, checksumsContent string, writeChecksums bool) uint64 {
	t.Helper()
	dbAndTableDir := path.Join(common.TablePathEncode(database), common.TablePathEncode(table))
	shadowPartPath := path.Join(diskPath, "backup", backupName, "shadow", dbAndTableDir, diskName, partName)
	require.NoError(t, os.MkdirAll(shadowPartPath, 0o750))
	require.NoError(t, os.WriteFile(path.Join(shadowPartPath, "checksums.txt"), []byte(checksumsContent), 0o640))
	checksum, err := common.CalculateChecksum(shadowPartPath, "checksums.txt")
	require.NoError(t, err)

	tm := metadata.TableMetadata{
		Database: database,
		Table:    table,
		Parts:    map[string][]metadata.Part{diskName: {{Name: partName}}},
	}
	if writeChecksums {
		tm.Checksums = map[string]uint64{partName: checksum}
	}
	metadataDir := path.Join(diskPath, "backup", backupName, "metadata", common.TablePathEncode(database))
	require.NoError(t, os.MkdirAll(metadataDir, 0o750))
	body, err := json.Marshal(tm)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path.Join(metadataDir, common.TablePathEncode(table)+".json"), body, 0o640))
	return checksum
}

func TestBuildLocalPartIndexFindsPartWithSameChecksum(t *testing.T) {
	diskPath := t.TempDir()
	checksum := createLocalBackupFixture(t, diskPath, "default", "backup_1", "default", "test", "all_1_1_0", "checksums", true)

	b := &Backuper{cfg: &config.Config{}, DefaultDataPath: diskPath}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local"}}
	b.localPartIndex = b.buildLocalPartIndex([]LocalBackup{{BackupMetadata: metadata.BackupMetadata{BackupName: "backup_1"}}}, disks)
	require.True(t, b.localPartIndex.complete)
	require.Len(t, b.localPartIndex.paths, 1)

	table := metadata.TableMetadata{Database: "default", Table: "test", Checksums: map[string]uint64{"all_1_1_0": checksum}}
	part := metadata.Part{Name: "all_1_1_0"}
	existingPartPath, localDisk, err := b.findLocalPartWithSameChecksum(table, &part, disks, "local", path.Join("default", "test"))
	require.NoError(t, err)
	require.NotNil(t, localDisk)
	require.Equal(t, path.Join(diskPath, "backup", "backup_1", "shadow", "default", "test", "default", "all_1_1_0"), existingPartPath)
}

func TestBuildLocalPartIndexRejectsStaleEntry(t *testing.T) {
	diskPath := t.TempDir()
	checksum := createLocalBackupFixture(t, diskPath, "default", "backup_1", "default", "test", "all_1_1_0", "checksums", true)

	// the part content changed after the metadata was written, the index entry is stale
	shadowPartPath := path.Join(diskPath, "backup", "backup_1", "shadow", "default", "test", "default", "all_1_1_0")
	require.NoError(t, os.WriteFile(path.Join(shadowPartPath, "checksums.txt"), []byte("changed"), 0o640))

	b := &Backuper{cfg: &config.Config{}, DefaultDataPath: diskPath}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local"}}
	b.localPartIndex = b.buildLocalPartIndex([]LocalBackup{{BackupMetadata: metadata.BackupMetadata{BackupName: "backup_1"}}}, disks)
	require.True(t, b.localPartIndex.complete)

	table := metadata.TableMetadata{Database: "default", Table: "test", Checksums: map[string]uint64{"all_1_1_0": checksum}}
	part := metadata.Part{Name: "all_1_1_0"}
	existingPartPath, localDisk, err := b.findLocalPartWithSameChecksum(table, &part, disks, "local", path.Join("default", "test"))
	require.NoError(t, err)
	require.Empty(t, existingPartPath)
	require.Nil(t, localDisk)
}

// a shadow part which no local backup metadata mentions is exactly what the glob would find and the
// complete index would not, so it proves the glob is really skipped
func TestBuildLocalPartIndexCompleteSkipsGlob(t *testing.T) {
	diskPath := t.TempDir()
	createLocalBackupFixture(t, diskPath, "default", "backup_1", "default", "test", "all_1_1_0", "checksums", true)

	// second backup has the part on disk but the metadata of another table only
	unIndexedPartPath := path.Join(diskPath, "backup", "backup_2", "shadow", "default", "test", "default", "all_2_2_0")
	require.NoError(t, os.MkdirAll(unIndexedPartPath, 0o750))
	require.NoError(t, os.WriteFile(path.Join(unIndexedPartPath, "checksums.txt"), []byte("orphan"), 0o640))
	unIndexedChecksum, err := common.CalculateChecksum(unIndexedPartPath, "checksums.txt")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(path.Join(diskPath, "backup", "backup_2", "metadata"), 0o750))

	b := &Backuper{cfg: &config.Config{}, DefaultDataPath: diskPath}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local"}}
	table := metadata.TableMetadata{Database: "default", Table: "test", Checksums: map[string]uint64{"all_2_2_0": unIndexedChecksum}}
	part := metadata.Part{Name: "all_2_2_0"}

	// nil index keeps the old glob behavior and finds the orphan part
	existingPartPath, localDisk, err := b.findLocalPartWithSameChecksum(table, &part, disks, "local", path.Join("default", "test"))
	require.NoError(t, err)
	require.NotNil(t, localDisk)
	require.Equal(t, unIndexedPartPath, existingPartPath)

	// complete index has no entry for it, so nothing is found and no glob is executed
	b.localPartIndex = b.buildLocalPartIndex([]LocalBackup{
		{BackupMetadata: metadata.BackupMetadata{BackupName: "backup_1"}},
		{BackupMetadata: metadata.BackupMetadata{BackupName: "backup_2"}},
	}, disks)
	require.True(t, b.localPartIndex.complete)
	existingPartPath, localDisk, err = b.findLocalPartWithSameChecksum(table, &part, disks, "local", path.Join("default", "test"))
	require.NoError(t, err)
	require.Empty(t, existingPartPath)
	require.Nil(t, localDisk)
}

func TestBuildLocalPartIndexIncompleteFallsBackToGlob(t *testing.T) {
	diskPath := t.TempDir()
	checksum := createLocalBackupFixture(t, diskPath, "default", "backup_1", "default", "test", "all_1_1_0", "checksums", true)

	b := &Backuper{cfg: &config.Config{}, DefaultDataPath: diskPath}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local"}}
	// a broken local backup can't be indexed, the index is a subset and the glob has to run
	b.localPartIndex = b.buildLocalPartIndex([]LocalBackup{
		{BackupMetadata: metadata.BackupMetadata{BackupName: "backup_broken"}, Broken: "broken metadata.json not found"},
	}, disks)
	require.False(t, b.localPartIndex.complete)
	require.Empty(t, b.localPartIndex.paths)

	table := metadata.TableMetadata{Database: "default", Table: "test", Checksums: map[string]uint64{"all_1_1_0": checksum}}
	part := metadata.Part{Name: "all_1_1_0"}
	existingPartPath, localDisk, err := b.findLocalPartWithSameChecksum(table, &part, disks, "local", path.Join("default", "test"))
	require.NoError(t, err)
	require.NotNil(t, localDisk)
	require.Equal(t, path.Join(diskPath, "backup", "backup_1", "shadow", "default", "test", "default", "all_1_1_0"), existingPartPath)
}

// old clickhouse-backup versions wrote table metadata without `checksums`, such a backup is still
// indexed because the index only tells where a part lives, the CRC64 verification happens on lookup
func TestBuildLocalPartIndexWithoutChecksumsInMetadata(t *testing.T) {
	diskPath := t.TempDir()
	checksum := createLocalBackupFixture(t, diskPath, "default", "backup_1", "default", "test", "all_1_1_0", "checksums", false)

	b := &Backuper{cfg: &config.Config{}, DefaultDataPath: diskPath}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local"}}
	b.localPartIndex = b.buildLocalPartIndex([]LocalBackup{{BackupMetadata: metadata.BackupMetadata{BackupName: "backup_1"}}}, disks)
	require.True(t, b.localPartIndex.complete)

	table := metadata.TableMetadata{Database: "default", Table: "test", Checksums: map[string]uint64{"all_1_1_0": checksum}}
	part := metadata.Part{Name: "all_1_1_0"}
	existingPartPath, localDisk, err := b.findLocalPartWithSameChecksum(table, &part, disks, "local", path.Join("default", "test"))
	require.NoError(t, err)
	require.NotNil(t, localDisk)
	require.Equal(t, path.Join(diskPath, "backup", "backup_1", "shadow", "default", "test", "default", "all_1_1_0"), existingPartPath)
}

// embedded backups live on an IsBackup disk with another layout, they neither get indexed nor
// make the index incomplete, IsBackup disks are skipped by findLocalPartWithSameChecksum as well
func TestBuildLocalPartIndexSkipsEmbeddedBackups(t *testing.T) {
	diskPath := t.TempDir()
	b := &Backuper{cfg: &config.Config{}, DefaultDataPath: diskPath}
	disks := []clickhouse.Disk{
		{Name: "default", Path: diskPath, Type: "local"},
		{Name: "backups_s3", Path: path.Join(diskPath, "embedded"), Type: "s3", IsBackup: true},
	}
	idx := b.buildLocalPartIndex([]LocalBackup{
		{BackupMetadata: metadata.BackupMetadata{BackupName: "backup_embedded", Tags: "regular,embedded"}},
	}, disks)
	require.True(t, idx.complete)
	require.Empty(t, idx.paths)
}

// newBackuperWithClosedClickHouse returns a Backuper whose ClickHouse connection fails fast, so a
// test which must NOT reach system.parts fails loudly instead of hanging, see issue #857
func newBackuperWithClosedClickHouse(t *testing.T) *Backuper {
	t.Helper()
	cfg := config.DefaultConfig()
	// port 1 is closed, any query attempt fails immediately instead of retrying forever
	cfg.ClickHouse.Host = "127.0.0.1"
	cfg.ClickHouse.Port = 1
	b := NewBackuper(cfg)
	b.ch.BreakConnectOnError = true
	return b
}

// createLivePartFixture writes a live part directory the way ClickHouse lays it out under the disk,
// marker content makes the hardlinked file identify which candidate was chosen
func createLivePartFixture(t *testing.T, diskPath, database, table, partName, marker string) string {
	t.Helper()
	livePartPath := path.Join(diskPath, "data", database, table, partName)
	require.NoError(t, os.MkdirAll(livePartPath, 0o750))
	require.NoError(t, os.WriteFile(path.Join(livePartPath, "checksums.txt"), []byte(marker), 0o640))
	return livePartPath
}

// a populated snapshot answers the lookup without touching system.parts, the closed ClickHouse
// connection proves no query is issued, https://github.com/Altinity/clickhouse-backup/issues/1457
func TestHardlinkByHashOfAllFilesUsesSnapshot(t *testing.T) {
	diskPath := t.TempDir()
	livePartPath := createLivePartFixture(t, diskPath, "default", "test", "all_9_9_0", "live")

	b := newBackuperWithClosedClickHouse(t)
	b.DefaultDataPath = diskPath
	b.DiskToPathMap = map[string]string{"default": diskPath}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local"}}
	table := metadata.TableMetadata{
		Database:       "default",
		Table:          "test",
		HashOfAllFiles: map[string]string{"all_1_1_0": "ABCDEF0123456789abcdef0123456789"},
	}
	livePartsByHash := map[string][]livePartRow{
		"abcdef0123456789abcdef0123456789": {{
			Name: "all_9_9_0", Path: livePartPath, Disk: "default",
			Database: "default", Table: "test", Hash: "abcdef0123456789abcdef0123456789",
		}},
	}
	part := metadata.Part{Name: "all_1_1_0"}
	found, size, err := b.hardlinkByHashOfAllFiles(context.Background(), "backup_1", table, &part, disks, "default", path.Join("default", "test"), livePartsByHash)
	require.NoError(t, err)
	require.True(t, found)
	require.Greater(t, size, int64(0))
	// the part was hardlinked into the backup shadow path
	require.FileExists(t, path.Join(diskPath, "backup", "backup_1", "shadow", "default", "test", "default", "all_1_1_0", "checksums.txt"))
}

// a snapshot which has no row for the expected hash is a real miss, no per-part query is issued
func TestHardlinkByHashOfAllFilesSnapshotMissSkipsQuery(t *testing.T) {
	diskPath := t.TempDir()
	b := newBackuperWithClosedClickHouse(t)
	b.DefaultDataPath = diskPath
	b.DiskToPathMap = map[string]string{"default": diskPath}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local"}}
	table := metadata.TableMetadata{
		Database:       "default",
		Table:          "test",
		HashOfAllFiles: map[string]string{"all_1_1_0": "abcdef0123456789abcdef0123456789"},
	}
	part := metadata.Part{Name: "all_1_1_0"}
	found, size, err := b.hardlinkByHashOfAllFiles(context.Background(), "backup_1", table, &part, disks, "default", path.Join("default", "test"), map[string][]livePartRow{})
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, size)
}

// a candidate which vanished from disk between the snapshot and the hardlink must be reported as
// not found so the caller can re-read system.parts, instead of failing the whole download
func TestHardlinkFromLivePartsStaleCandidate(t *testing.T) {
	diskPath := t.TempDir()
	b := newBackuperWithClosedClickHouse(t)
	b.DefaultDataPath = diskPath
	b.DiskToPathMap = map[string]string{"default": diskPath}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local"}}
	table := metadata.TableMetadata{Database: "default", Table: "test"}
	rows := []livePartRow{{
		Name: "all_9_9_0", Path: path.Join(diskPath, "data", "default", "test", "already_merged_away"),
		Disk: "default", Database: "default", Table: "test", Hash: "abcdef0123456789abcdef0123456789",
	}}
	part := metadata.Part{Name: "all_1_1_0"}

	// tolerateStale: the caller gets a clean miss and can fall back to a live query
	found, size, err := b.hardlinkFromLiveParts(rows, true, "backup_1", table, &part, disks, "default", path.Join("default", "test"))
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, size)

	// the live query path keeps the old behavior and reports the missing directory as an error
	found, _, err = b.hardlinkFromLiveParts(rows, false, "backup_1", table, &part, disks, "default", path.Join("default", "test"))
	require.Error(t, err)
	require.False(t, found)
}

// candidate selection must stay identical to the per-part query path: same table wins first, then
// the closest part name by levenshtein distance, then the lexicographically smallest name
func TestHardlinkFromLivePartsCandidateSelection(t *testing.T) {
	diskPath := t.TempDir()
	otherTablePath := createLivePartFixture(t, diskPath, "default", "other", "all_1_1_0", "other-table")
	farNamePath := createLivePartFixture(t, diskPath, "default", "test", "all_777_777_7", "far-name")
	closeNamePath := createLivePartFixture(t, diskPath, "default", "test", "all_1_1_1", "close-name")

	b := newBackuperWithClosedClickHouse(t)
	b.DefaultDataPath = diskPath
	b.DiskToPathMap = map[string]string{"default": diskPath}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local"}}
	table := metadata.TableMetadata{Database: "default", Table: "test"}
	// deliberately ordered worst-first so only the sort can pick the right one
	rows := []livePartRow{
		{Name: "all_1_1_0", Path: otherTablePath, Disk: "default", Database: "default", Table: "other"},
		{Name: "all_777_777_7", Path: farNamePath, Disk: "default", Database: "default", Table: "test"},
		{Name: "all_1_1_1", Path: closeNamePath, Disk: "default", Database: "default", Table: "test"},
	}
	rowsBefore := make([]livePartRow, len(rows))
	copy(rowsBefore, rows)

	part := metadata.Part{Name: "all_1_1_0"}
	found, _, err := b.hardlinkFromLiveParts(rows, true, "backup_1", table, &part, disks, "default", path.Join("default", "test"))
	require.NoError(t, err)
	require.True(t, found)
	// same table beats the identical part name of another table, closest name wins inside the table,
	// the marker content identifies which source directory was actually hardlinked
	linked, readErr := os.ReadFile(path.Join(diskPath, "backup", "backup_1", "shadow", "default", "test", "default", "all_1_1_0", "checksums.txt"))
	require.NoError(t, readErr)
	require.Equal(t, "close-name", string(linked))
	// the shared snapshot slice must not be reordered, concurrent parts read it at the same time
	require.Equal(t, rowsBefore, rows)
}

// a table without hash_of_all_files needs no system.parts read at all
func TestFetchLivePartsByHashNoHashes(t *testing.T) {
	b := newBackuperWithClosedClickHouse(t)
	result, err := b.fetchLivePartsByHash(context.Background(), metadata.TableMetadata{Database: "default", Table: "test"})
	require.NoError(t, err)
	require.Empty(t, result)
}

// `--rbac-only` backups have no metadata directory and no shadow parts, they must not make the
// index incomplete, otherwise a single such backup lying around disables the optimization
func TestBuildLocalPartIndexWithoutMetadataDir(t *testing.T) {
	diskPath := t.TempDir()
	require.NoError(t, os.MkdirAll(path.Join(diskPath, "backup", "backup_rbac", "access"), 0o750))

	b := &Backuper{cfg: &config.Config{}, DefaultDataPath: diskPath}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local"}}
	idx := b.buildLocalPartIndex([]LocalBackup{{BackupMetadata: metadata.BackupMetadata{BackupName: "backup_rbac"}}}, disks)
	require.True(t, idx.complete)
	require.Empty(t, idx.paths)
}

// a part moved to another disk by `rebalance` lives under the rebalanced disk, not the original one
func TestBuildLocalPartIndexRebalancedDisk(t *testing.T) {
	defaultPath := t.TempDir()
	coldPath := t.TempDir()
	dbAndTableDir := path.Join("default", "test")
	shadowPartPath := path.Join(coldPath, "backup", "backup_1", "shadow", dbAndTableDir, "cold", "all_1_1_0")
	require.NoError(t, os.MkdirAll(shadowPartPath, 0o750))
	require.NoError(t, os.WriteFile(path.Join(shadowPartPath, "checksums.txt"), []byte("checksums"), 0o640))
	checksum, err := common.CalculateChecksum(shadowPartPath, "checksums.txt")
	require.NoError(t, err)

	tm := metadata.TableMetadata{
		Database:  "default",
		Table:     "test",
		Parts:     map[string][]metadata.Part{"default": {{Name: "all_1_1_0", RebalancedDisk: "cold"}}},
		Checksums: map[string]uint64{"all_1_1_0": checksum},
	}
	metadataDir := path.Join(defaultPath, "backup", "backup_1", "metadata", "default")
	require.NoError(t, os.MkdirAll(metadataDir, 0o750))
	body, err := json.Marshal(tm)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path.Join(metadataDir, "test.json"), body, 0o640))

	b := &Backuper{cfg: &config.Config{}, DefaultDataPath: defaultPath}
	disks := []clickhouse.Disk{
		{Name: "default", Path: defaultPath, Type: "local"},
		{Name: "cold", Path: coldPath, Type: "local"},
	}
	b.localPartIndex = b.buildLocalPartIndex([]LocalBackup{{BackupMetadata: metadata.BackupMetadata{BackupName: "backup_1"}}}, disks)
	require.True(t, b.localPartIndex.complete)

	part := metadata.Part{Name: "all_1_1_0"}
	existingPartPath, localDisk, findErr := b.findLocalPartWithSameChecksum(tm, &part, disks, "local", dbAndTableDir)
	require.NoError(t, findErr)
	require.NotNil(t, localDisk)
	require.Equal(t, "cold", localDisk.Name)
	require.Equal(t, shadowPartPath, existingPartPath)
}
