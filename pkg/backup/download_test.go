package backup

import (
	"context"
	"errors"
	"os"
	"path"
	"regexp"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var b = Backuper{
	DefaultDataPath: "/var/lib/clickhouse",
	DiskToPathMap: map[string]string{
		"default": "/var/lib/clickhouse",
		"s3":      "/var/lib/clickhouse/disks/s3",
	},
	cfg:        &config.Config{ClickHouse: config.ClickHouseConfig{EmbeddedBackupDisk: "backup"}},
	isEmbedded: false,
}

var baseDisks = []clickhouse.Disk{
	{
		Name:            "default",
		Path:            "/var/lib/clickhouse",
		Type:            "local",
		FreeSpace:       1024,
		StoragePolicies: []string{"default", "jbod"},
		IsBackup:        false,
	},
	{
		Name:            "s3",
		Path:            "/var/lib/clickhouse/disks/s3",
		Type:            "s3",
		FreeSpace:       0,
		StoragePolicies: []string{"s3_only", "jbod"},
		IsBackup:        false,
	},
	{
		Name:            "backup_s3",
		Path:            "/var/lib/clickhouse/disks/backup_s3",
		Type:            "s3",
		FreeSpace:       0,
		StoragePolicies: nil,
		IsBackup:        true,
	},
}
var jbodDisks = []clickhouse.Disk{
	{
		Name:            "hdd2",
		Path:            "/hdd2",
		Type:            "local",
		FreeSpace:       1024,
		StoragePolicies: []string{"default", "jbod"},
		IsBackup:        false,
	},
	{
		Name:            "s3_disk2",
		Path:            "/var/lib/clickhouse/disks/s3_disk2",
		Type:            "s3",
		FreeSpace:       0,
		StoragePolicies: []string{"s3_only", "jbod"},
		IsBackup:        false,
	},
}

var remoteBackup = storage.Backup{
	BackupMetadata: metadata.BackupMetadata{
		BackupName: "Test",
		// doesn't matter which  disk path in backup, should use system.disks?
		Disks:                   map[string]string{"default": "/test", "hdd2": "/disk2", "s3": "/s3", "s3_disk2": "/s3_new"},
		DiskTypes:               map[string]string{"default": "local", "hdd2": "local", "s3": "s3", "s3_disk2": "s3"},
		ClickhouseBackupVersion: "unknown",
		CreationDate:            time.Now(),
		Tags:                    "",
		ClickHouseVersion:       "unknown",
		Databases:               nil,
		Tables: []metadata.TableTitle{
			{
				Database: "default",
				Table:    "test",
			},
		},
		Functions:      nil,
		DataFormat:     "tar",
		RequiredBackup: "",
	},
	UploadDate: time.Now(),
}

func TestIsRemoteMetadataNotFound(t *testing.T) {
	notFoundMessages := []string{
		"object doesn't exist",
		"key not found: metadata/default/test.json",
		"NoSuchKey: The specified key does not exist",
		"operation error S3: GetObject, https response error StatusCode: 404",
		"StatusCode 404",
		// real backend phrasings observed in test/integration TestMetadataNotFound*
		"550 /backup/metadata/default/test.json: No such file or directory", // FTP
		"file does not exist", // SFTP
		"AzureBlob GetFileReaderAbsolute Download: RESPONSE ERROR (ServiceCode=BlobNotFound) RESPONSE Status: 404 The specified blob does not exist.", // Azure
	}
	for _, msg := range notFoundMessages {
		assert.True(t, isRemoteMetadataNotFound(errors.New(msg)), msg)
	}

	assert.False(t, isRemoteMetadataNotFound(nil))
	assert.False(t, isRemoteMetadataNotFound(errors.New("temporary network timeout")))
}

func TestResumeExistingBackupMissingStateFileReturnsError(t *testing.T) {
	backupName := "test_resume_crash"
	defaultDataPath := t.TempDir()
	backupDir := path.Join(defaultDataPath, "backup", backupName)
	assert.NoError(t, os.MkdirAll(backupDir, 0o750))

	backuper := &Backuper{DefaultDataPath: defaultDataPath}

	err := backuper.resumeExistingBackup(backupName)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download.state2")
	assert.Contains(t, err.Error(), "delete local "+backupName)
	// must keep wrapping ErrBackupIsAlreadyExists so RestoreFromRemote reuses the local backup (issue #625)
	assert.ErrorIs(t, err, ErrBackupIsAlreadyExists)

	assert.NoError(t, os.WriteFile(path.Join(backupDir, "download.state2"), []byte("state"), 0o640))
	assert.NoError(t, backuper.resumeExistingBackup(backupName))
}

func TestReBalanceTablesMetadataIfDiskNotExists_Files_NoErrors(t *testing.T) {
	remoteBackup.DataFormat = "tar"
	baseTable := metadata.TableMetadata{
		Files: map[string][]string{
			"default": {"default_part_1_1_0.tar", "default_part_2_2_0.tar"},
			"s3":      {"s3_part_5_5_0.tar", "s3_part_6_6_0.tar"},
		},
		Parts: map[string][]metadata.Part{
			"default": {{Name: "part_1_1_0"}, {Name: "part_2_2_0"}},
			"s3":      {{Name: "part_5_5_0"}, {Name: "part_6_6_0"}},
		},
		RebalancedFiles: nil,
		Database:        "default",
		Table:           "test",
		Query:           "CREATE TABLE default.test(id UInt64) ENGINE=MergeTree() ORDER BY id",
		Size:            nil,
		TotalBytes:      1000,
		Mutations:       nil,
		MetadataOnly:    false,
		LocalFile:       "/dev/null",
	}
	var tableMetadataAfterDownload ListOfTables
	addTable := baseTable
	tableMetadataAfterDownload = append(tableMetadataAfterDownload, &addTable)
	baseTable.Table = "test2"
	baseTable.Query = "CREATE TABLE default.test(id UInt64) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='jbod'"
	baseTable.Files = map[string][]string{
		"default":  {"default_part_1_1_0.tar", "default_part_2_2_0.tar"},
		"hdd2":     {"hdd2_part_3_3_0.tar", "hdd2_part_4_4_0.tar"},
		"s3":       {"s3_part_5_5_0.tar", "s3_part_6_6_0.tar"},
		"s3_disk2": {"s3_disk2_part_7_7_0.tar", "s3_disk2_part_8_8_0.tar"},
	}
	baseTable.Parts = map[string][]metadata.Part{
		"default":  {{Name: "part_1_1_0"}, {Name: "part_2_2_0"}},
		"hdd2":     {{Name: "part_3_3_0"}, {Name: "part_4_4_0"}},
		"s3":       {{Name: "part_5_5_0"}, {Name: "part_6_6_0"}},
		"s3_disk2": {{Name: "part_7_7_0"}, {Name: "part_8_8_0"}},
	}

	addTable2 := baseTable
	tableMetadataAfterDownload = append(tableMetadataAfterDownload, &addTable2)
	assert.NoError(t, b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownload, baseDisks, remoteBackup))
	//rebalanced table
	meta := tableMetadataAfterDownload[1]
	assert.Equal(t, 4, len(meta.RebalancedFiles), "expect 4 rebalanced files in %s.%s", meta.Database, meta.Table)
	for i, d := range jbodDisks {
		for _, p := range meta.Parts[d.Name] {
			assert.Equal(t, baseDisks[i].Name, p.RebalancedDisk, "expect rebalanced part:%s", p.Name)
		}
	}

	//non-rebalanced table
	meta = tableMetadataAfterDownload[0]
	assert.Equal(t, 0, len(meta.RebalancedFiles), "0 files shall rebalanced")
	for _, d := range baseDisks {
		for _, p := range meta.Parts[d.Name] {
			assert.Equal(t, "", p.RebalancedDisk, "expect no rebalanced part: %s", p.Name)
		}
	}

}

func TestReBalanceTablesMetadataIfDiskNotExists_Parts_NoErrors(t *testing.T) {
	remoteBackup.DataFormat = "directory"
	baseTable := metadata.TableMetadata{
		Parts: map[string][]metadata.Part{
			"default": {{Name: "part_1_1_0"}, {Name: "part_2_2_0"}},
			"s3":      {{Name: "part_5_5_0"}, {Name: "part_6_6_0"}},
		},
		RebalancedFiles: nil,
		Database:        "default",
		Table:           "test",
		Query:           "CREATE TABLE default.test(id UInt64) ENGINE=MergeTree() ORDER BY id",
		Size:            nil,
		TotalBytes:      1000,
		Mutations:       nil,
		MetadataOnly:    false,
		LocalFile:       "/dev/null",
	}
	var tableMetadataAfterDownload ListOfTables
	tableMetadataAfterDownload = append(tableMetadataAfterDownload, &baseTable)
	baseTable.Table = "test2"
	baseTable.Query = "CREATE TABLE default.test(id UInt64) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='jbod'"
	baseTable.Parts = map[string][]metadata.Part{
		"default":  {{Name: "part_1_1_0"}, {Name: "part_2_2_0"}},
		"hdd2":     {{Name: "part_3_3_0"}, {Name: "part_4_4_0"}},
		"s3":       {{Name: "part_5_5_0"}, {Name: "part_6_6_0"}},
		"s3_disk2": {{Name: "part_7_7_0"}, {Name: "part_8_8_0"}},
	}

	tableMetadataAfterDownload = append(tableMetadataAfterDownload, &baseTable)

	tableMetadataAfterDownloadRepacked := make(ListOfTables, len(tableMetadataAfterDownload))
	for i := range tableMetadataAfterDownload {
		tableMetadataAfterDownloadRepacked[i] = tableMetadataAfterDownload[i]
	}
	assert.NoError(t, b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, baseDisks, remoteBackup))
	// no files re-balance
	for _, meta := range tableMetadataAfterDownload {
		assert.Equal(t, 0, len(meta.RebalancedFiles))
	}
	// re-balance parts
	meta := tableMetadataAfterDownload[1]
	for i, d := range jbodDisks {
		for _, p := range meta.Parts[d.Name] {
			assert.Equal(t, baseDisks[i].Name, p.RebalancedDisk, "expect rebalanced part")
		}
	}
	// non re-balance parts
	meta = tableMetadataAfterDownload[0]
	for _, d := range baseDisks {
		for _, p := range meta.Parts[d.Name] {
			assert.Equal(t, "", p.RebalancedDisk, "expect no rebalanced part")
		}
	}
}

func TestReBalanceTablesMetadataIfDiskNotExists_CheckErrors(t *testing.T) {
	invalidRemoteBackup := remoteBackup
	invalidRemoteBackup.DataFormat = DirectoryFormat
	invalidTable := metadata.TableMetadata{
		Parts: map[string][]metadata.Part{
			"default": {{Name: "part_1_1_0"}, {Name: "part_2_2_0"}},
			"hdd2":    {{Name: "part_3_3_0"}, {Name: "part_4_4_0"}},
		},
		RebalancedFiles: nil,
		Database:        "default",
		Table:           "test",
		Query:           "CREATE TABLE default.test(id UInt64) ENGINE=MergeTree() ORDER BY id",
		Size:            nil,
		TotalBytes:      1000,
		Mutations:       nil,
		MetadataOnly:    false,
		LocalFile:       "/dev/null",
	}
	tableMetadataAfterDownload := ListOfTables{&invalidTable}
	// hdd2 not exists diskType
	delete(invalidRemoteBackup.DiskTypes, "hdd2")
	tableMetadataAfterDownloadRepacked := make(ListOfTables, len(tableMetadataAfterDownload))
	for i := range tableMetadataAfterDownload {
		tableMetadataAfterDownloadRepacked[i] = tableMetadataAfterDownload[i]
	}
	err := b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, baseDisks, invalidRemoteBackup)
	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"disk: hdd2 not found in disk_types section map[string]string{\"default\":\"local\", \"s3\":\"s3\", \"s3_disk2\":\"s3\"} in Test/metadata.json",
	)

	// invalid disk_type
	invalidRemoteBackup.DiskTypes["hdd2"] = "unknown"
	tableMetadataAfterDownloadRepacked = make(ListOfTables, len(tableMetadataAfterDownload))
	for i := range tableMetadataAfterDownload {
		tableMetadataAfterDownloadRepacked[i] = tableMetadataAfterDownload[i]
	}
	err = b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, baseDisks, invalidRemoteBackup)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk: hdd2, diskType: unknown not found in system.disks")

	// invalid storage_policy
	invalidRemoteBackup.DiskTypes["hdd2"] = "local"
	invalidTable.Parts = map[string][]metadata.Part{
		"default":  {{Name: "part_1_1_0"}, {Name: "part_2_2_0"}},
		"hdd2":     {{Name: "part_3_3_0"}, {Name: "part_4_4_0"}},
		"s3":       {{Name: "part_5_5_0"}, {Name: "part_6_6_0"}},
		"s3_disk2": {{Name: "part_7_7_0"}, {Name: "part_8_8_0"}},
	}

	invalidTable.Table = "test3"
	invalidTable.Query = "CREATE TABLE default.test3(id UInt64) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='invalid'"
	tableMetadataAfterDownloadRepacked = ListOfTables{&invalidTable}
	err = b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, baseDisks, invalidRemoteBackup)
	require.Error(t, err)
	matched, matchErr := regexp.MatchString(`storagePolicy: invalid with diskType: \w+ not found in system.disks`, err.Error())
	assert.NoError(t, matchErr)
	assert.True(t, matched)

	// no free space
	invalidDisks := baseDisks
	invalidDisks[0].FreeSpace = 0
	invalidTable.Table = "test4"
	invalidTable.Query = "CREATE TABLE default.test(id UInt64) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='default'"
	invalidTable.Parts = map[string][]metadata.Part{
		"default": {{Name: "part_1_1_0"}, {Name: "part_2_2_0"}},
		"hdd2":    {{Name: "part_3_3_0"}, {Name: "part_4_4_0"}},
	}
	tableMetadataAfterDownloadRepacked = ListOfTables{&invalidTable}
	err = b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, invalidDisks, invalidRemoteBackup)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "250B free space, not found in system.disks with `local` type")

}

// https://github.com/Altinity/clickhouse-backup/issues/1268
func TestCheckFreeSpaceForDownload(t *testing.T) {
	ctx := context.Background()
	backuper := &Backuper{cfg: &config.Config{}}
	freeSpaceRemoteBackup := storage.Backup{BackupMetadata: metadata.BackupMetadata{BackupName: "test_free_space"}}
	disks := []clickhouse.Disk{
		{Name: "default", Path: "/var/lib/clickhouse", Type: "local", FreeSpace: 1000},
	}
	tables := ListOfTables{
		{
			Database: "default",
			Table:    "test",
			Parts:    map[string][]metadata.Part{"default": {{Name: "all_1_1_0", Size: 600}, {Name: "all_2_2_0", Size: 500}}},
		},
	}
	// not enough free space
	err := backuper.checkFreeSpaceForDownload(ctx, freeSpaceRemoteBackup, tables, disks, false, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires 1.07KiB free space to download")
	// resume downgrades error to warning
	require.NoError(t, backuper.checkFreeSpaceForDownload(ctx, freeSpaceRemoteBackup, tables, disks, false, true))
	// enough free space
	disks[0].FreeSpace = 2048
	require.NoError(t, backuper.checkFreeSpaceForDownload(ctx, freeSpaceRemoteBackup, tables, disks, false, false))
	// required part with unresolvable size (empty RequiredBackup) produces warning only
	disks[0].FreeSpace = 1000
	tables[0].Parts["default"][0].Required = true
	require.NoError(t, backuper.checkFreeSpaceForDownload(ctx, freeSpaceRemoteBackup, tables, disks, false, false))
	// required part which already exists in the local required backup will be hardlinked and not counted
	requiredDiskPath := t.TempDir()
	require.NoError(t, os.MkdirAll(path.Join(requiredDiskPath, "backup", "test_free_space_full", "shadow", "default", "test", "default", "all_1_1_0"), 0o750))
	backuper.DiskToPathMap = map[string]string{"default": requiredDiskPath}
	freeSpaceRemoteBackup.RequiredBackup = "test_free_space_full"
	require.NoError(t, backuper.checkFreeSpaceForDownload(ctx, freeSpaceRemoteBackup, tables, disks, false, false))
	freeSpaceRemoteBackup.RequiredBackup = ""
	backuper.DiskToPathMap = nil
	tables[0].Parts["default"][0].Required = false
	// parts without size (backup created by older version) produce warning only
	tables[0].Parts["default"][0].Size = 0
	tables[0].Parts["default"][1].Size = 0
	require.NoError(t, backuper.checkFreeSpaceForDownload(ctx, freeSpaceRemoteBackup, tables, disks, false, false))
}

// https://github.com/Altinity/clickhouse-backup/issues/1268
func TestCheckFreeSpaceForDownloadHardlinkExistsFiles(t *testing.T) {
	ctx := context.Background()
	diskPath := t.TempDir()
	partPath := path.Join(diskPath, "data", "default", "test", "all_1_1_0")
	require.NoError(t, os.MkdirAll(partPath, 0o750))
	require.NoError(t, os.WriteFile(path.Join(partPath, "checksums.txt"), []byte("checksums"), 0o640))
	checksum, err := common.CalculateChecksum(partPath, "checksums.txt")
	require.NoError(t, err)

	backuper := &Backuper{cfg: &config.Config{}}
	freeSpaceRemoteBackup := storage.Backup{BackupMetadata: metadata.BackupMetadata{BackupName: "test_free_space_hardlink"}}
	disks := []clickhouse.Disk{{Name: "default", Path: diskPath, Type: "local", FreeSpace: 1000}}
	tables := ListOfTables{
		{
			Database:  "default",
			Table:     "test",
			Parts:     map[string][]metadata.Part{"default": {{Name: "all_1_1_0", Size: 4096}}},
			Checksums: map[string]uint64{"all_1_1_0": checksum},
		},
	}
	// without hardlinks part size exceeds free space
	require.Error(t, backuper.checkFreeSpaceForDownload(ctx, freeSpaceRemoteBackup, tables, disks, false, false))
	// with hardlinks the part matches local one by checksum and requires no space
	require.NoError(t, backuper.checkFreeSpaceForDownload(ctx, freeSpaceRemoteBackup, tables, disks, true, false))
	// checksum mismatch means the part will be downloaded and counted again
	tables[0].Checksums["all_1_1_0"] = checksum + 1
	require.Error(t, backuper.checkFreeSpaceForDownload(ctx, freeSpaceRemoteBackup, tables, disks, true, false))
}
