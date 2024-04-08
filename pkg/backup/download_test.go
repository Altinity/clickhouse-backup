package backup

import (
	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	apexLog "github.com/apex/log"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
	"time"
)

var b = Backuper{
	log:             &apexLog.Entry{},
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
var log = apexLog.WithField("logger", "test")

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
	var tableMetadataAfterDownload []metadata.TableMetadata
	tableMetadataAfterDownload = append(tableMetadataAfterDownload, baseTable)
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

	tableMetadataAfterDownload = append(tableMetadataAfterDownload, baseTable)
	tableMetadataAfterDownloadRepacked := make([]*metadata.TableMetadata, len(tableMetadataAfterDownload))
	for i := range tableMetadataAfterDownload {
		tableMetadataAfterDownloadRepacked[i] = &tableMetadataAfterDownload[i]
	}
	assert.NoError(t, b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, baseDisks, remoteBackup, log))
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
	var tableMetadataAfterDownload []metadata.TableMetadata
	tableMetadataAfterDownload = append(tableMetadataAfterDownload, baseTable)
	baseTable.Table = "test2"
	baseTable.Query = "CREATE TABLE default.test(id UInt64) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='jbod'"
	baseTable.Parts = map[string][]metadata.Part{
		"default":  {{Name: "part_1_1_0"}, {Name: "part_2_2_0"}},
		"hdd2":     {{Name: "part_3_3_0"}, {Name: "part_4_4_0"}},
		"s3":       {{Name: "part_5_5_0"}, {Name: "part_6_6_0"}},
		"s3_disk2": {{Name: "part_7_7_0"}, {Name: "part_8_8_0"}},
	}

	tableMetadataAfterDownload = append(tableMetadataAfterDownload, baseTable)

	tableMetadataAfterDownloadRepacked := make([]*metadata.TableMetadata, len(tableMetadataAfterDownload))
	for i := range tableMetadataAfterDownload {
		tableMetadataAfterDownloadRepacked[i] = &tableMetadataAfterDownload[i]
	}
	assert.NoError(t, b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, baseDisks, remoteBackup, log))
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
	tableMetadataAfterDownload := []metadata.TableMetadata{invalidTable}
	// hdd2 not exists diskType
	delete(invalidRemoteBackup.DiskTypes, "hdd2")
	tableMetadataAfterDownloadRepacked := make([]*metadata.TableMetadata, len(tableMetadataAfterDownload))
	for i := range tableMetadataAfterDownload {
		tableMetadataAfterDownloadRepacked[i] = &tableMetadataAfterDownload[i]
	}
	err := b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, baseDisks, invalidRemoteBackup, log)
	assert.Error(t, err)
	assert.Equal(t,
		"disk: hdd2 not found in disk_types section map[string]string{\"default\":\"local\", \"s3\":\"s3\", \"s3_disk2\":\"s3\"} in Test/metadata.json",
		err.Error(),
	)

	// invalid disk_type
	invalidRemoteBackup.DiskTypes["hdd2"] = "unknown"
	tableMetadataAfterDownloadRepacked = make([]*metadata.TableMetadata, len(tableMetadataAfterDownload))
	for i := range tableMetadataAfterDownload {
		tableMetadataAfterDownloadRepacked[i] = &tableMetadataAfterDownload[i]
	}
	err = b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, baseDisks, invalidRemoteBackup, log)
	assert.Error(t, err)
	assert.Equal(t, "disk: hdd2, diskType: unknown not found in system.disks", err.Error())

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
	tableMetadataAfterDownloadRepacked = []*metadata.TableMetadata{&invalidTable}
	err = b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, baseDisks, invalidRemoteBackup, log)
	assert.Error(t, err)
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
	tableMetadataAfterDownloadRepacked = []*metadata.TableMetadata{&invalidTable}
	err = b.reBalanceTablesMetadataIfDiskNotExists(tableMetadataAfterDownloadRepacked, invalidDisks, invalidRemoteBackup, log)
	assert.Error(t, err)
	assert.Equal(t, "250B free space, not found in system.disks with `local` type", err.Error())

}
