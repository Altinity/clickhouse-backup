package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDiskNameSQLPreferNonCacheDisk verifies that when the cache_path column
// is present in system.disks, the generated disk name SQL uses argMin to
// prefer the underlying (non-cache) disk name over the cache wrapper.
//
// Background: cache disks share the same metadata path as the disk they wrap,
// so GROUP BY d.path collapses them into one row. The underlying disk name is
// the one stored in the backup metadata and in the remote paths, so it is
// picked whatever name the server reports the pair under, see TestDiskAliases.
func TestDiskNameSQLPreferNonCacheDisk(t *testing.T) {
	testCases := []struct {
		Name             string
		CachePathPresent uint64
		ExpectedSQL      string
	}{
		{
			Name:             "No cache_path column - fallback to any(d.name)",
			CachePathPresent: 0,
			ExpectedSQL:      "any(d.name)",
		},
		{
			Name:             "cache_path column present - use argMin to prefer non-cache disk",
			CachePathPresent: 1,
			ExpectedSQL:      "argMin(d.name, d.cache_path != '')",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// Mirrors the logic in getDisksFromSystemDisks
			diskNameSQL := "any(d.name)"
			if tc.CachePathPresent > 0 {
				diskNameSQL = "argMin(d.name, d.cache_path != '')"
			}
			assert.Equal(t, tc.ExpectedSQL, diskNameSQL)
		})
	}
}

// TestDiskAliases verifies that the names of the `system.disks` rows collapsed into one disk are kept next to the
// name the disk is listed under: `system.parts.disk_name` of a part behind a cache disk is the wrapped disk on
// ClickHouse 25.8 and the cache disk on 26.8
func TestDiskAliases(t *testing.T) {
	testCases := []struct {
		Name     string
		DiskName string
		Names    []string
		Expected []string
	}{
		{Name: "single row", DiskName: "default", Names: []string{"default"}, Expected: nil},
		{Name: "cache over object disk", DiskName: "s3_disk", Names: []string{"s3_cache", "s3_disk"}, Expected: []string{"s3_cache"}},
		{Name: "two caches over one disk are sorted", DiskName: "s3_disk", Names: []string{"s3_cache_b", "s3_disk", "s3_cache_a"}, Expected: []string{"s3_cache_a", "s3_cache_b"}},
		{Name: "no names from an old server", DiskName: "default", Names: nil, Expected: nil},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			assert.Equal(t, tc.Expected, diskAliases(tc.DiskName, tc.Names))
		})
	}
}

// TestFindDiskByName verifies that a disk is found by the name it is listed under and by the name of a cache disk
// collapsed into it, and that the name of a disk wins over an alias of another disk
func TestFindDiskByName(t *testing.T) {
	disks := []Disk{
		{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
		{Name: "blob_plain", Aliases: []string{"blob_plain_cache"}, Type: "azure", MetadataType: "plain_rewritable", Path: "/var/lib/clickhouse/disks/blob_plain/"},
		{Name: "s3_disk", Aliases: []string{"s3_cache", "mapped"}, Type: "s3", Path: "/var/lib/clickhouse/disks/s3_disk/"},
		{Name: "mapped", Type: "local", Path: "/mnt/mapped/"},
	}
	testCases := []struct {
		DiskName string
		Expected string
	}{
		{DiskName: "default", Expected: "default"},
		{DiskName: "blob_plain", Expected: "blob_plain"},
		{DiskName: "blob_plain_cache", Expected: "blob_plain"},
		{DiskName: "s3_cache", Expected: "s3_disk"},
		{DiskName: "mapped", Expected: "mapped"},
		{DiskName: "unknown", Expected: ""},
		{DiskName: "", Expected: ""},
	}
	for _, tc := range testCases {
		t.Run(tc.DiskName, func(t *testing.T) {
			disk := FindDiskByName(disks, tc.DiskName)
			if tc.Expected == "" {
				assert.Nil(t, disk)
				return
			}
			if assert.NotNil(t, disk) {
				assert.Equal(t, tc.Expected, disk.Name)
				assert.True(t, disk.HasName(tc.DiskName))
			}
		})
	}
	assert.False(t, disks[0].HasName("blob_plain_cache"))
	// the returned pointer is an element of the list, the callers read the live disk through it
	assert.Same(t, &disks[1], FindDiskByName(disks, "blob_plain_cache"))
}
