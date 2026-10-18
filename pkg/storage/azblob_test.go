package storage

import (
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/stretchr/testify/assert"
)

// TestAzureBlobCalculateBlockSize https://github.com/Altinity/clickhouse-backup/issues/1142
func TestAzureBlobCalculateBlockSize(t *testing.T) {
	const mib = int64(1024 * 1024)
	const gib = 1024 * mib
	testCases := []struct {
		name          string
		fileSize      int64
		chunkSize     int64
		maxPartsCount int64
		expected      int64
		// withinMaxParts - blocks count <= max_parts_count is achievable, false for the automatic 10MiB cap and the 4000MiB Azure block limit
		withinMaxParts bool
	}{
		{"zero chunk_size keeps automatic size clamped to 10MiB", 10 * gib, 0, 256, 10 * mib, false},
		{"zero chunk_size keeps automatic size clamped to 2MiB", 10 * gib, 0, 50000, 2 * mib, true},
		{"512MiB chunk_size", 10 * gib, 512 * mib, 256, 512 * mib, true},
		{"8GiB chunk_size clamped to 4000MiB", 10 * gib, 8 * gib, 256, 4000 * mib, true},
		{"8GiB chunk_size clamped to 4000MiB exceeds max_parts_count for huge blob", 1024 * gib, 8 * gib, 256, 4000 * mib, false},
		{"parts count equal to max_parts_count uses chunk_size", 10 * gib, 500 * mib, 21, 500 * mib, true},
		{"parts count above max_parts_count falls back to size/max_parts_count", 10 * gib, 1 * mib, 256, 10 * gib / 256, true},
		{"fallback rounds up the remainder", 5*mib + 1, 1 * mib, 2, 5*mib/2 + 1, true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := &AzureBlob{Config: &config.AzureBlobConfig{ChunkSize: tc.chunkSize, MaxPartsCount: tc.maxPartsCount}}
			blockSize := a.calculateBlockSize(tc.fileSize)
			assert.Equal(t, tc.expected, blockSize)
			if tc.withinMaxParts {
				assert.LessOrEqual(t, (tc.fileSize+blockSize-1)/blockSize, tc.maxPartsCount)
			} else {
				assert.Greater(t, (tc.fileSize+blockSize-1)/blockSize, tc.maxPartsCount)
			}
		})
	}
	assert.Equal(t, int64(0), config.DefaultConfig().AzureBlob.ChunkSize)
}
