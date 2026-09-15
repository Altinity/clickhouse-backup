package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResolveDiskPaths - `system.disks.path` is relative both for plain/plain_rewritable disks (bucket key
// prefix) and for every local disk of a clickhouse-server started with a relative `<path>`, see
// https://github.com/Altinity/clickhouse-backup/issues/1121
func TestResolveDiskPaths(t *testing.T) {
	testcases := []struct {
		name        string
		disks       []Disk
		diskMapping map[string]string
		enrich      bool
		expected    map[string]string // disk name -> expected Path
		expectedRaw map[string]string // disk name -> expected RawPath
	}{
		{
			name: "absolute paths without disk_mapping are unchanged",
			disks: []Disk{
				{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
				{Name: "s3disk", Type: "s3", Path: "/var/lib/clickhouse/disks/s3/"},
			},
			expected:    map[string]string{"default": "/var/lib/clickhouse/", "s3disk": "/var/lib/clickhouse/disks/s3/"},
			expectedRaw: map[string]string{"default": "/var/lib/clickhouse/", "s3disk": "/var/lib/clickhouse/disks/s3/"},
		},
		{
			name: "relative server path resolved via disk_mapping[default]",
			disks: []Disk{
				{Name: "default", Type: "local", Path: "./"},
				{Name: "miniocached", Type: "s3", Path: "./disks/miniocached/"},
			},
			diskMapping: map[string]string{"default": "/var/lib/clickhouse/"},
			expected:    map[string]string{"default": "/var/lib/clickhouse/", "miniocached": "/var/lib/clickhouse/disks/miniocached/"},
			expectedRaw: map[string]string{"default": "./", "miniocached": "./disks/miniocached/"},
		},
		{
			name: "relative server path resolved independently of system.disks row order",
			disks: []Disk{
				{Name: "miniocached", Type: "s3", Path: "./disks/miniocached/"},
				{Name: "default", Type: "local", Path: "./"},
			},
			diskMapping: map[string]string{"default": "/var/lib/clickhouse/"},
			expected:    map[string]string{"default": "/var/lib/clickhouse/", "miniocached": "/var/lib/clickhouse/disks/miniocached/"},
			expectedRaw: map[string]string{"default": "./", "miniocached": "./disks/miniocached/"},
		},
		{
			name: "relative server path without disk_mapping stays relative",
			disks: []Disk{
				{Name: "default", Type: "local", Path: "./"},
				{Name: "miniocached", Type: "s3", Path: "./disks/miniocached/"},
			},
			expected:    map[string]string{"default": "./", "miniocached": "./disks/miniocached/"},
			expectedRaw: map[string]string{"default": "./", "miniocached": "./disks/miniocached/"},
		},
		{
			name: "explicit per disk mapping wins over the default root",
			disks: []Disk{
				{Name: "default", Type: "local", Path: "./"},
				{Name: "miniocached", Type: "s3", Path: "./disks/miniocached/"},
			},
			diskMapping: map[string]string{"default": "/var/lib/clickhouse/", "miniocached": "/mnt/cache/"},
			expected:    map[string]string{"default": "/var/lib/clickhouse/", "miniocached": "/mnt/cache/"},
		},
		{
			name: "plain disk bucket key prefix becomes a pseudo local path",
			disks: []Disk{
				{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
				{Name: "disk_s3_plain_rewritable", Type: "s3_plain_rewritable", MetadataType: "plain_rewritable", Path: "disk_s3_plain_rewritable/cluster/shard/"},
			},
			expected: map[string]string{
				"default":                  "/var/lib/clickhouse/",
				"disk_s3_plain_rewritable": "/var/lib/clickhouse/disks/disk_s3_plain_rewritable/",
			},
			expectedRaw: map[string]string{"disk_s3_plain_rewritable": "disk_s3_plain_rewritable/cluster/shard/"},
		},
		{
			name: "plain disk in the bucket root reports an empty path",
			disks: []Disk{
				{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
				{Name: "azure_plain", Type: "object_storage", MetadataType: "plain", Path: ""},
			},
			expected: map[string]string{
				"default":     "/var/lib/clickhouse/",
				"azure_plain": "/var/lib/clickhouse/disks/azure_plain/",
			},
			expectedRaw: map[string]string{"azure_plain": ""},
		},
		{
			name: "plain disk pseudo path is built from the mapped default",
			disks: []Disk{
				{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
				{Name: "azure_plain", Type: "object_storage", MetadataType: "plain", Path: ""},
			},
			diskMapping: map[string]string{"default": "/mnt/ch/"},
			expected: map[string]string{
				"default":     "/mnt/ch/",
				"azure_plain": "/mnt/ch/disks/azure_plain/",
			},
		},
		{
			name: "non plain disk with an empty path is not rewritten",
			disks: []Disk{
				{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
				{Name: "web", Type: "web", Path: ""},
			},
			expected: map[string]string{"default": "/var/lib/clickhouse/", "web": ""},
		},
		{
			name: "mapped default without a system.disks row is still the root",
			disks: []Disk{
				{Name: "relative_s3", Type: "s3", Path: "./disks/relative_s3/"},
			},
			diskMapping: map[string]string{"default": "/mnt/ch"},
			enrich:      true,
			expected:    map[string]string{"default": "/mnt/ch", "relative_s3": "/mnt/ch/disks/relative_s3/"},
		},
		{
			name: "enrich appends a mapped disk which is absent in system.disks",
			disks: []Disk{
				{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
			},
			diskMapping: map[string]string{"default-gp3": "/var/lib/clickhouse/"},
			enrich:      true,
			expected:    map[string]string{"default": "/var/lib/clickhouse/", "default-gp3": "/var/lib/clickhouse/"},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			disks := resolveDiskPaths(tc.disks, tc.diskMapping, tc.enrich)
			actual := map[string]string{}
			actualRaw := map[string]string{}
			for _, d := range disks {
				actual[d.Name] = d.Path
				actualRaw[d.Name] = d.RawPath
			}
			assert.Equal(t, tc.expected, actual)
			for name, rawPath := range tc.expectedRaw {
				assert.Equal(t, rawPath, actualRaw[name], "RawPath of disk %s", name)
			}
		})
	}
}

// TestNormalizeDataPathsByDisks - relative `system.tables.data_paths` must get exactly the same
// RawPath -> Path substitution as the disks themselves, otherwise no disk path is a prefix of any data path
func TestNormalizeDataPathsByDisks(t *testing.T) {
	testcases := []struct {
		name      string
		disks     []Disk
		dataPaths []string
		expected  []string
	}{
		{
			name: "relative server path with resolved disks",
			disks: []Disk{
				{Name: "default", Path: "/var/lib/clickhouse/", RawPath: "./"},
				{Name: "miniocached", Path: "/var/lib/clickhouse/disks/miniocached/", RawPath: "./disks/miniocached/"},
			},
			dataPaths: []string{"./store/abc/abcdef/", "./disks/miniocached/store/abc/abcdef/"},
			expected:  []string{"/var/lib/clickhouse/store/abc/abcdef/", "/var/lib/clickhouse/disks/miniocached/store/abc/abcdef/"},
		},
		{
			name: "unresolved relative disks leave data paths untouched",
			disks: []Disk{
				{Name: "default", Path: "./", RawPath: "./"},
				{Name: "miniocached", Path: "./disks/miniocached/", RawPath: "./disks/miniocached/"},
			},
			dataPaths: []string{"./store/abc/abcdef/", "./disks/miniocached/store/abc/abcdef/"},
			expected:  []string{"./store/abc/abcdef/", "./disks/miniocached/store/abc/abcdef/"},
		},
		{
			name: "plain disk bucket key prefix becomes the pseudo local path",
			disks: []Disk{
				{Name: "default", Path: "/var/lib/clickhouse/", RawPath: "/var/lib/clickhouse/"},
				{Name: "disk_s3_plain_rewritable", Path: "/var/lib/clickhouse/disks/disk_s3_plain_rewritable/", RawPath: "disk_s3_plain_rewritable/cluster/shard/"},
			},
			dataPaths: []string{"disk_s3_plain_rewritable/cluster/shard/store/abc/abcdef/"},
			expected:  []string{"/var/lib/clickhouse/disks/disk_s3_plain_rewritable/store/abc/abcdef/"},
		},
		{
			name: "plain disk in the bucket root keeps disk root relative data paths",
			disks: []Disk{
				{Name: "default", Path: "/var/lib/clickhouse/", RawPath: "/var/lib/clickhouse/"},
				{Name: "azure_plain", Path: "/var/lib/clickhouse/disks/azure_plain/", RawPath: ""},
			},
			dataPaths: []string{"store/abc/abcdef/"},
			expected:  []string{"store/abc/abcdef/"},
		},
		{
			name: "absolute data paths are never rewritten",
			disks: []Disk{
				{Name: "default", Path: "/var/lib/clickhouse/", RawPath: "./"},
			},
			dataPaths: []string{"/var/lib/clickhouse/store/abc/abcdef/"},
			expected:  []string{"/var/lib/clickhouse/store/abc/abcdef/"},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tables := []Table{{Database: "db", Name: "t", DataPaths: tc.dataPaths}}
			normalizeDataPathsByDisks(tables, tc.disks)
			assert.Equal(t, tc.expected, tables[0].DataPaths)
		})
	}
}

// TestGetDisksByPaths - the `default` fallback must never overwrite a data path which really matched a disk,
// see https://github.com/Altinity/clickhouse-backup/issues/1121
func TestGetDisksByPaths(t *testing.T) {
	tieredDisks := []Disk{
		{Name: "default", Path: "/var/lib/clickhouse/"},
		{Name: "miniocached", Path: "/var/lib/clickhouse/disks/miniocached/"},
	}
	testcases := []struct {
		name      string
		disks     []Disk
		dataPaths []string
		expected  map[string]string
		expectErr string
	}{
		{
			name:      "tiered table with absolute data paths",
			disks:     tieredDisks,
			dataPaths: []string{"/var/lib/clickhouse/store/abc/abcdef/", "/var/lib/clickhouse/disks/miniocached/store/abc/abcdef/"},
			expected: map[string]string{
				"default":     "/var/lib/clickhouse/store/abc/abcdef/",
				"miniocached": "/var/lib/clickhouse/disks/miniocached/store/abc/abcdef/",
			},
		},
		{
			name:      "tiered table with reversed data path order",
			disks:     tieredDisks,
			dataPaths: []string{"/var/lib/clickhouse/disks/miniocached/store/abc/abcdef/", "/var/lib/clickhouse/store/abc/abcdef/"},
			expected: map[string]string{
				"default":     "/var/lib/clickhouse/store/abc/abcdef/",
				"miniocached": "/var/lib/clickhouse/disks/miniocached/store/abc/abcdef/",
			},
		},
		{
			name:      "unmatched data path must not overwrite a real default match",
			disks:     tieredDisks,
			dataPaths: []string{"/var/lib/clickhouse/store/abc/abcdef/", "/somewhere/else/store/abc/abcdef/"},
			expected:  map[string]string{"default": "/var/lib/clickhouse/store/abc/abcdef/"},
		},
		{
			name:      "unmatched data path first, real default match afterwards",
			disks:     tieredDisks,
			dataPaths: []string{"/somewhere/else/store/abc/abcdef/", "/var/lib/clickhouse/store/abc/abcdef/"},
			expected:  map[string]string{"default": "/var/lib/clickhouse/store/abc/abcdef/"},
		},
		{
			name:      "only unmatched data paths keep the default fallback",
			disks:     tieredDisks,
			dataPaths: []string{"/somewhere/else/store/abc/abcdef/"},
			expected:  map[string]string{"default": "/somewhere/else/store/abc/abcdef/"},
		},
		{
			name:      "two different data paths really matching one disk are a misconfiguration",
			disks:     []Disk{{Name: "default", Path: "/var/lib/clickhouse/"}},
			dataPaths: []string{"/var/lib/clickhouse/store/abc/abcdef/", "/var/lib/clickhouse/store/abc/fedcba/"},
			expectErr: `both resolve to disk "default"`,
		},
		{
			name: "two disk names sharing one path (issue #676 enrich)",
			disks: []Disk{
				{Name: "default", Path: "/var/lib/clickhouse/"},
				{Name: "default-gp3", Path: "/var/lib/clickhouse/"},
			},
			dataPaths: []string{"/var/lib/clickhouse/store/abc/abcdef/"},
			expected: map[string]string{
				"default":     "/var/lib/clickhouse/store/abc/abcdef/",
				"default-gp3": "/var/lib/clickhouse/store/abc/abcdef/",
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := GetDisksByPaths(tc.disks, tc.dataPaths)
			if tc.expectErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
