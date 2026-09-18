package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// https://github.com/Altinity/clickhouse-backup/issues/1568
func TestSplitRestoreObjectKeys(t *testing.T) {
	suffix := objectKeySuffix("regression", "completed", "canary")
	assert.Regexp(t, `^_regression_[0-9a-f]{8}$`, suffix)
	assert.NotEqual(t, suffix, objectKeySuffix("regression", "completed2", "canary"))

	testCases := []struct {
		name       string
		objectPath string
		isAbsolute bool
		keySuffix  string
		expected   restoreObjectKeys
	}{
		{
			name: "relative key without mapping", objectPath: "abc/xyz",
			expected: restoreObjectKeys{srcKey: "abc/xyz", dstKey: "abc/xyz", metaPath: "abc/xyz"},
		},
		{
			name: "relative key with mapping", objectPath: "abc/xyz", keySuffix: suffix,
			expected: restoreObjectKeys{srcKey: "abc/xyz", dstKey: "abc" + suffix + "/xyz", metaPath: "abc" + suffix + "/xyz"},
		},
		{
			name: "absolute key with mapping, disk prefix contains the backup name", objectPath: "regression-test/live/abc/xyz", isAbsolute: true, keySuffix: suffix,
			expected: restoreObjectKeys{srcKey: "abc/xyz", dstKey: "abc" + suffix + "/xyz", metaPath: "regression-test/live/abc" + suffix + "/xyz"},
		},
		{
			name: "already rewritten by a previous restore into another table", objectPath: "live/abc_regression_deadbeef/xyz", isAbsolute: true, keySuffix: suffix,
			expected: restoreObjectKeys{srcKey: "abc/xyz", dstKey: "abc" + suffix + "/xyz", metaPath: "live/abc" + suffix + "/xyz"},
		},
		{
			name: "already rewritten by the old format, restore without mapping", objectPath: "abc_regression/xyz",
			expected: restoreObjectKeys{srcKey: "abc/xyz", dstKey: "abc/xyz", metaPath: "abc/xyz"},
		},
		{
			name: "single component (ClickHouse before 22.x) with mapping", objectPath: "xyz", keySuffix: suffix,
			expected: restoreObjectKeys{srcKey: "xyz", dstKey: "xyz" + suffix, metaPath: "xyz" + suffix},
		},
		{
			name: "single component already rewritten, restore without mapping", objectPath: "xyz_regression_deadbeef",
			expected: restoreObjectKeys{srcKey: "xyz", dstKey: "xyz", metaPath: "xyz"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, splitRestoreObjectKeys(tc.objectPath, tc.isAbsolute, "regression", tc.keySuffix))
		})
	}
}
