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
	const fileSize = 10 * gib
	testCases := []struct {
		name          string
		chunkSize     int64
		maxPartsCount int64
		expected      int64
	}{
		{"zero chunk_size keeps automatic size clamped to 10MiB", 0, 256, 10 * mib},
		{"zero chunk_size keeps automatic size clamped to 2MiB", 0, 50000, 2 * mib},
		{"512MiB chunk_size", 512 * mib, 256, 512 * mib},
		{"8GiB chunk_size clamped to 4000MiB", 8 * gib, 256, 4000 * mib},
		{"parts count equal to max_parts_count uses chunk_size", 500 * mib, 21, 500 * mib},
		{"parts count above max_parts_count falls back to size/max_parts_count", 1 * mib, 256, fileSize / 256},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := &AzureBlob{Config: &config.AzureBlobConfig{ChunkSize: tc.chunkSize, MaxPartsCount: tc.maxPartsCount}}
			assert.Equal(t, tc.expected, a.calculateBlockSize(fileSize))
		})
	}
	assert.Equal(t, int64(0), config.DefaultConfig().AzureBlob.ChunkSize)
}
