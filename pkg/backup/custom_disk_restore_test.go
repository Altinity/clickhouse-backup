package backup

import (
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/stretchr/testify/assert"
)

// TestTableDiskNames - disk names of a backed up table are collected from both parts and files, deduplicated
// and sorted, so the alias order doesn't depend on map iteration
func TestTableDiskNames(t *testing.T) {
	tm := &metadata.TableMetadata{
		Parts: map[string][]metadata.Part{
			"__tmp_internal_b": {{Name: "all_1_1_0"}},
			"default":          {{Name: "all_2_2_0"}},
		},
		Files: map[string][]string{
			"__tmp_internal_b": {"__tmp_internal_b_all_1_1_0.tar"},
			"__tmp_internal_a": {"__tmp_internal_a_all_3_3_0.tar"},
		},
	}
	assert.Equal(t, []string{"__tmp_internal_a", "__tmp_internal_b", "default"}, tableDiskNames(tm))
}

// TestCustomDiskAliases - a `SETTINGS disk = disk(...)` disk is named after a hash of its declaration, that
// hash changes between clickhouse-server versions, so the backup disk name has to be aliased to the disk the
// restored table actually lives on, https://github.com/Altinity/clickhouse-backup/issues/943
func TestCustomDiskAliases(t *testing.T) {
	target := clickhouse.Disk{
		Name:            "__tmp_internal_new",
		Path:            "/var/lib/clickhouse/disks/custom_s3/",
		Type:            "s3",
		MetadataType:    "local",
		FreeSpace:       100,
		TotalSpace:      200,
		StoragePolicies: []string{"____tmp_internal_new"},
		RawPath:         "/var/lib/clickhouse/disks/custom_s3/",
	}
	liveDisks := []clickhouse.Disk{
		{Name: "default", Path: "/var/lib/clickhouse/", Type: "local"},
		target,
		// disk of a dropped table stays registered until clickhouse-server restart
		{Name: "__tmp_internal_stale", Path: "/var/lib/clickhouse/disks/__tmp_internal_stale/", Type: "s3", MetadataType: "local"},
	}

	testcases := []struct {
		name            string
		backupDiskNames []string
		backupDiskPaths map[string]string
		want            []customDiskAlias
	}{
		{
			name:            "backup disk name differs from the live one",
			backupDiskNames: []string{"__tmp_internal_old", "default"},
			backupDiskPaths: map[string]string{
				"__tmp_internal_old": "/var/lib/clickhouse/disks/custom_s3/",
				"default":            "/var/lib/clickhouse/",
			},
			want: []customDiskAlias{{Disk: clickhouse.Disk{
				Name:            "__tmp_internal_old",
				Path:            target.Path,
				Type:            target.Type,
				MetadataType:    target.MetadataType,
				FreeSpace:       target.FreeSpace,
				TotalSpace:      target.TotalSpace,
				StoragePolicies: target.StoragePolicies,
				RawPath:         target.RawPath,
			}, LocalPath: "/var/lib/clickhouse/disks/custom_s3/"}},
		},
		{
			name:            "stale generated disk of a dropped table is redirected to the target",
			backupDiskNames: []string{"__tmp_internal_stale"},
			backupDiskPaths: map[string]string{"__tmp_internal_stale": "/var/lib/clickhouse/disks/__tmp_internal_stale/"},
			want: []customDiskAlias{{Disk: clickhouse.Disk{
				Name:            "__tmp_internal_stale",
				Path:            target.Path,
				Type:            target.Type,
				MetadataType:    target.MetadataType,
				FreeSpace:       target.FreeSpace,
				TotalSpace:      target.TotalSpace,
				StoragePolicies: target.StoragePolicies,
				RawPath:         target.RawPath,
			}, LocalPath: "/var/lib/clickhouse/disks/__tmp_internal_stale/"}},
		},
		{
			name:            "same clickhouse version, same disk name, nothing to alias",
			backupDiskNames: []string{"__tmp_internal_new"},
			backupDiskPaths: map[string]string{"__tmp_internal_new": target.Path},
			want:            []customDiskAlias{},
		},
		{
			name:            "disk path of the backup is kept for the local shadow directory",
			backupDiskNames: []string{"__tmp_internal_old"},
			backupDiskPaths: map[string]string{"__tmp_internal_old": "/var/lib/clickhouse/disks/__tmp_internal_old/"},
			want: []customDiskAlias{{Disk: clickhouse.Disk{
				Name:            "__tmp_internal_old",
				Path:            target.Path,
				Type:            target.Type,
				MetadataType:    target.MetadataType,
				FreeSpace:       target.FreeSpace,
				TotalSpace:      target.TotalSpace,
				StoragePolicies: target.StoragePolicies,
				RawPath:         target.RawPath,
			}, LocalPath: "/var/lib/clickhouse/disks/__tmp_internal_old/"}},
		},
		{
			name:            "no path in backup metadata falls back to the target path",
			backupDiskNames: []string{"__tmp_internal_old"},
			backupDiskPaths: map[string]string{},
			want: []customDiskAlias{{Disk: clickhouse.Disk{
				Name:            "__tmp_internal_old",
				Path:            target.Path,
				Type:            target.Type,
				MetadataType:    target.MetadataType,
				FreeSpace:       target.FreeSpace,
				TotalSpace:      target.TotalSpace,
				StoragePolicies: target.StoragePolicies,
				RawPath:         target.RawPath,
			}, LocalPath: target.Path}},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, customDiskAliases(tc.backupDiskNames, tc.backupDiskPaths, liveDisks, target))
		})
	}
}

// TestResolveCustomDiskAliasesNoCustomTables - restores without an inline disk declaration must not touch
// system.disks, b.ch has no connection here so any query would hang or panic
func TestResolveCustomDiskAliasesNoCustomTables(t *testing.T) {
	b := &Backuper{ch: &clickhouse.ClickHouse{BreakConnectOnError: true}}
	disks := []clickhouse.Disk{{Name: "default", Path: "/var/lib/clickhouse/", Type: "local"}}
	tables := ListOfTables{
		nil,
		&metadata.TableMetadata{Database: "db", Table: "t", Query: "CREATE TABLE db.t (id UInt64) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='s3'"},
	}
	result, err := b.resolveCustomDiskAliases(t.Context(), tables, map[string]string{}, disks, map[string]string{}, map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, disks, result)
}
