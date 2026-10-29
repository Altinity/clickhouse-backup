package backup

import (
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
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
// rejected, a disk name unknown to GetDisks is rejected the same way, --partitions narrows the list before the check
func TestFilterPlainPolicyParts(t *testing.T) {
	disks := []clickhouse.Disk{
		{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
		{Name: "enc_local", Type: "encrypted", Path: "/var/lib/clickhouse/enc/"},
		{Name: "s3_disk", Type: "s3", Path: "/var/lib/clickhouse/disks/s3_disk/"},
		{Name: "enc_s3", Type: "encrypted", Path: "/var/lib/clickhouse/disks/s3_disk/enc/"},
		{Name: "s3_cache", Type: "cache", Path: "/var/lib/clickhouse/disks/s3_cache/"},
		{Name: "web", Type: "web", Path: "/var/lib/clickhouse/disks/web/"},
		{Name: "plain", Type: "s3", MetadataType: "plain_rewritable", Path: "/var/lib/clickhouse/disks/plain/"},
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
		wantErr    string
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
			name:    "cache wrapper listed by GetDisks under the underlying disk name",
			parts:   []activePart{part("202501_1_1_0", "s3_disk_cache"), part("202412_1_1_0", "plain")},
			wantErr: "disk s3_disk_cache which is not a local disk of system.disks",
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
			for _, p := range got {
				gotNames = append(gotNames, p.Name)
			}
			if strings.Join(gotNames, ",") != strings.Join(tc.wantNames, ",") {
				t.Fatalf("want parts %v, got %v", tc.wantNames, gotNames)
			}
		})
	}
}
