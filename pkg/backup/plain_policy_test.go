package backup

import (
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
)

// TestPartPathOnDisk verifies that system.parts.path is matched against the disk path clickhouse-server reports
// (Disk.RawPath) and not against Disk.Path, which clickhouse.disk_mapping may have rewritten
func TestPartPathOnDisk(t *testing.T) {
	testcases := []struct {
		name     string
		partPath string
		disk     clickhouse.Disk
		want     string
		wantErr  string
	}{
		{
			name:     "no disk_mapping",
			partPath: "/var/lib/clickhouse/store/abc/abcdef/all_1_1_0/",
			disk:     clickhouse.Disk{Name: "default", Path: "/var/lib/clickhouse/", RawPath: "/var/lib/clickhouse/"},
			want:     "store/abc/abcdef/all_1_1_0/",
		},
		{
			name:     "disk_mapping rewrote the disk path",
			partPath: "/var/lib/clickhouse/store/abc/abcdef/all_1_1_0/",
			disk:     clickhouse.Disk{Name: "default", Path: "/mnt/chdata/", RawPath: "/var/lib/clickhouse/"},
			want:     "store/abc/abcdef/all_1_1_0/",
		},
		{
			name:     "relative server path resolved through disk_mapping",
			partPath: "data/store/abc/abcdef/all_1_1_0/",
			disk:     clickhouse.Disk{Name: "default", Path: "/mnt/chdata/", RawPath: "data/"},
			want:     "store/abc/abcdef/all_1_1_0/",
		},
		{
			name:     "disk path without trailing slash",
			partPath: "/var/lib/clickhouse/store/abc/abcdef/all_1_1_0/",
			disk:     clickhouse.Disk{Name: "default", Path: "/var/lib/clickhouse", RawPath: "/var/lib/clickhouse"},
			want:     "store/abc/abcdef/all_1_1_0/",
		},
		{
			name:     "no RawPath falls back to Path",
			partPath: "/mnt/hot/store/abc/abcdef/all_1_1_0/",
			disk:     clickhouse.Disk{Name: "hot", Path: "/mnt/hot/"},
			want:     "store/abc/abcdef/all_1_1_0/",
		},
		{
			name:     "part outside of the disk",
			partPath: "/var/lib/clickhouse/store/abc/abcdef/all_1_1_0/",
			disk:     clickhouse.Disk{Name: "hot", Path: "/mnt/hot/", RawPath: "/mnt/hot/"},
			wantErr:  "outside of disk hot",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := partPathOnDisk(tc.partPath, tc.disk)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("want %q, got %q", tc.want, got)
			}
		})
	}
}

// TestFilterPlainPolicyParts verifies which disks may hold parts next to plain disks in one storage policy: only
// disks with complete local files, every object disk with local metadata (and its cache/encrypted wrappers) is
// rejected, a disk name unknown to GetDisks is rejected the same way, --partitions narrows the list before the check.
// A cache disk is one GetDisks entry with the disk it wraps, system.parts reports the parts behind it under the
// wrapped disk name (ClickHouse 25.8) or under the cache disk name (26.8): both resolve to the same disk and the
// returned parts carry the name of the GetDisks entry, the one used in the backup
func TestFilterPlainPolicyParts(t *testing.T) {
	disks := []clickhouse.Disk{
		{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
		{Name: "enc_local", Type: "encrypted", Path: "/var/lib/clickhouse/enc/"},
		{Name: "s3_disk", Aliases: []string{"s3_disk_cache"}, Type: "s3", Path: "/var/lib/clickhouse/disks/s3_disk/"},
		{Name: "enc_s3", Type: "encrypted", Path: "/var/lib/clickhouse/disks/s3_disk/enc/"},
		{Name: "s3_cache", Type: "cache", Path: "/var/lib/clickhouse/disks/s3_cache/"},
		{Name: "web", Type: "web", Path: "/var/lib/clickhouse/disks/web/"},
		{Name: "plain", Aliases: []string{"plain_cache"}, Type: "s3", MetadataType: "plain_rewritable", Path: "/var/lib/clickhouse/disks/plain/"},
	}
	part := func(name, disk string) activePart {
		return activePart{Name: name, DiskName: disk, Path: "/var/lib/clickhouse/store/abc/abcdef/" + name + "/"}
	}
	b := &Backuper{}
	table := &clickhouse.Table{Database: "db", Name: "t"}
	testcases := []struct {
		name       string
		parts      []activePart
		partitions common.EmptyMap
		wantNames  []string
		// wantDisks are the expected DiskName of the returned parts, checked when set
		wantDisks []string
		wantErr   string
	}{
		{
			name:      "local and plain",
			parts:     []activePart{part("202501_1_1_0", "default"), part("202412_1_1_0", "plain")},
			wantNames: []string{"202501_1_1_0", "202412_1_1_0"},
		},
		{
			name:      "encrypted over local and plain",
			parts:     []activePart{part("202501_1_1_0", "enc_local"), part("202412_1_1_0", "plain")},
			wantNames: []string{"202501_1_1_0", "202412_1_1_0"},
		},
		{
			name:      "plain only",
			parts:     []activePart{part("202412_1_1_0", "plain")},
			wantNames: []string{"202412_1_1_0"},
		},
		{
			name:    "object disk",
			parts:   []activePart{part("202501_1_1_0", "s3_disk"), part("202412_1_1_0", "plain")},
			wantErr: "disk s3_disk of type s3",
		},
		{
			name:    "encrypted over object disk",
			parts:   []activePart{part("202501_1_1_0", "enc_s3"), part("202412_1_1_0", "plain")},
			wantErr: "disk enc_s3 of type encrypted",
		},
		{
			name:    "cache disk of an old server",
			parts:   []activePart{part("202501_1_1_0", "s3_cache"), part("202412_1_1_0", "plain")},
			wantErr: "disk s3_cache of type cache",
		},
		{
			name:    "web disk",
			parts:   []activePart{part("202501_1_1_0", "web")},
			wantErr: "disk web of type web",
		},
		{
			name:    "cache over object disk reported under the cache disk name",
			parts:   []activePart{part("202501_1_1_0", "s3_disk_cache"), part("202412_1_1_0", "plain")},
			wantErr: "disk s3_disk (system.parts reports it as s3_disk_cache) of type s3",
		},
		{
			name:    "disk absent in system.disks",
			parts:   []activePart{part("202501_1_1_0", "unknown_disk"), part("202412_1_1_0", "plain")},
			wantErr: "disk unknown_disk which is absent in system.disks",
		},
		{
			name:      "cache over plain disk reported under the wrapped disk name, ClickHouse 25.8",
			parts:     []activePart{part("202501_1_1_0", "default"), part("202412_1_1_0", "plain")},
			wantNames: []string{"202501_1_1_0", "202412_1_1_0"},
			wantDisks: []string{"default", "plain"},
		},
		{
			name:      "cache over plain disk reported under the cache disk name, ClickHouse 26.8",
			parts:     []activePart{part("202501_1_1_0", "default"), part("202412_1_1_0", "plain_cache")},
			wantNames: []string{"202501_1_1_0", "202412_1_1_0"},
			wantDisks: []string{"default", "plain"},
		},
		{
			name:      "all parts behind the cache disk, ClickHouse 26.8",
			parts:     []activePart{part("202412_1_1_0", "plain_cache"), part("202411_1_1_0", "plain_cache")},
			wantNames: []string{"202412_1_1_0", "202411_1_1_0"},
			wantDisks: []string{"plain", "plain"},
		},
		{
			name:       "partitions filter drops the rejected part outside of the requested partitions",
			parts:      []activePart{part("202501_1_1_0", "default"), part("202412_1_1_0", "s3_disk")},
			partitions: common.EmptyMap{"202501": {}},
			wantNames:  []string{"202501_1_1_0"},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := b.filterPlainPolicyParts(table, tc.parts, disks, tc.partitions)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			gotNames := make([]string, 0, len(got))
			gotDisks := make([]string, 0, len(got))
			for _, p := range got {
				gotNames = append(gotNames, p.Name)
				gotDisks = append(gotDisks, p.DiskName)
			}
			if strings.Join(gotNames, ",") != strings.Join(tc.wantNames, ",") {
				t.Fatalf("want parts %v, got %v", tc.wantNames, gotNames)
			}
			if tc.wantDisks != nil && strings.Join(gotDisks, ",") != strings.Join(tc.wantDisks, ",") {
				t.Fatalf("want part disks %v, got %v", tc.wantDisks, gotDisks)
			}
		})
	}
}

// TestSkipDisksMatchCacheAndWrappedDiskNames verifies that clickhouse.skip_disks skips a cache disk and the disk it
// wraps whichever of the two names is configured: they are one GetDisks entry, listed under the wrapped disk name,
// and the names the backup metadata or system.parts can carry (wrapped on 25.8, cache on 26.8) resolve to it
func TestSkipDisksMatchCacheAndWrappedDiskNames(t *testing.T) {
	disks := []clickhouse.Disk{
		{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
		{Name: "plain", Aliases: []string{"plain_cache"}, Type: "azure", MetadataType: "plain_rewritable", Path: "/var/lib/clickhouse/disks/plain/"},
		{Name: "s3_disk", Aliases: []string{"s3_disk_cache"}, Type: "s3", Path: "/var/lib/clickhouse/disks/s3_disk/"},
	}
	testcases := []struct {
		name      string
		skipDisks []string
		skipTypes []string
		// wantSkipped lists every disk name which must be skipped, all the other names of disks must not be
		wantSkipped []string
	}{
		{name: "nothing configured"},
		{name: "wrapped disk name", skipDisks: []string{"plain"}, wantSkipped: []string{"plain", "plain_cache"}},
		{name: "cache disk name", skipDisks: []string{"plain_cache"}, wantSkipped: []string{"plain", "plain_cache"}},
		{name: "both names", skipDisks: []string{"plain", "plain_cache"}, wantSkipped: []string{"plain", "plain_cache"}},
		{name: "another disk", skipDisks: []string{"s3_disk_cache"}, wantSkipped: []string{"s3_disk", "s3_disk_cache"}},
		{name: "disk type", skipTypes: []string{"S3"}, wantSkipped: []string{"s3_disk", "s3_disk_cache"}},
		{name: "unknown name", skipDisks: []string{"plain_cache_2"}},
	}
	allNames := []string{"default", "plain", "plain_cache", "s3_disk", "s3_disk_cache", "unknown_disk"}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			b := &Backuper{cfg: &config.Config{}}
			b.cfg.ClickHouse.SkipDisks = tc.skipDisks
			b.cfg.ClickHouse.SkipDiskTypes = tc.skipTypes
			wantSkipped := map[string]bool{}
			for _, name := range tc.wantSkipped {
				wantSkipped[name] = true
			}
			for _, name := range allNames {
				if got := b.shouldDiskNameSkipByNameOrType(name, disks); got != wantSkipped[name] {
					t.Errorf("shouldDiskNameSkipByNameOrType(%q): want %v, got %v", name, wantSkipped[name], got)
				}
			}
			for _, disk := range disks {
				if got := b.shouldSkipByDiskNameOrType(disk); got != wantSkipped[disk.Name] {
					t.Errorf("shouldSkipByDiskNameOrType(%q): want %v, got %v", disk.Name, wantSkipped[disk.Name], got)
				}
			}
		})
	}
}
