package storage

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSafeExtractPath(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "download")

	traversalCases := []struct {
		name string
	}{
		{"../evil"},
		{"../../evil"},
		{"sub/../../evil"},
		{"sub/../../../evil"},
		{""},
		{"."},
		{".."},
	}
	for _, tc := range traversalCases {
		t.Run("reject_"+tc.name, func(t *testing.T) {
			resolved, err := safeExtractPath(baseDir, tc.name)
			assert.Error(t, err, "entry %q must be rejected", tc.name)
			assert.Empty(t, resolved)
		})
	}

	allowedCases := []struct {
		name string
	}{
		{"metadata.bin"},
		{"shadow/data.bin"},
		{"store/some db/0/all_1_1_0/bin.bin"},
		{"a/../b.bin"},
	}
	for _, tc := range allowedCases {
		t.Run("allow_"+tc.name, func(t *testing.T) {
			resolved, err := safeExtractPath(baseDir, tc.name)
			require.NoError(t, err, "entry %q must be allowed", tc.name)
			assert.Equal(t, filepath.Join(baseDir, filepath.Clean(tc.name)), resolved)
		})
	}
}
