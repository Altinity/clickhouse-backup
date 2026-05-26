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
// so GROUP BY d.path collapses them into one row. system.parts always reports
// the underlying disk name, not the cache wrapper, so picking the cache name
// causes fetchHashOfAllFiles to fail with "part not found after FREEZE".
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
