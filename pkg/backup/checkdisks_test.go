package backup

import (
	"os"
	"path"
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
)

// TestCheckDisksConsistency verifies the issue #1037 pre-flight: clickhouse-backup must fail fast
// when it can't see the same local disks as clickhouse-server, instead of silently producing a
// schema-only backup / partial restore.
func TestCheckDisksConsistency(t *testing.T) {
	mkLocalDisk := func(t *testing.T, marker string) string {
		t.Helper()
		root := t.TempDir()
		if marker != "" {
			if err := os.Mkdir(path.Join(root, marker), 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", marker, err)
			}
		}
		return root
	}

	testcases := []struct {
		name      string
		skipDisks []string
		disks     func(t *testing.T) []clickhouse.Disk
		wantErr   string // substring; "" means no error
	}{
		{
			name: "local disk with store marker",
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{{Name: "default", Type: "local", Path: mkLocalDisk(t, "store")}}
			},
		},
		{
			name: "local disk with data marker",
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{{Name: "default", Type: "local", Path: mkLocalDisk(t, "data")}}
			},
		},
		{
			name: "local disk with metadata marker (old ClickHouse)",
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{{Name: "default", Type: "local", Path: mkLocalDisk(t, "metadata")}}
			},
		},
		{
			name: "default disk exists but empty",
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{{Name: "default", Type: "local", Path: mkLocalDisk(t, "")}}
			},
			wantErr: "store/data/metadata",
		},
		{
			name: "default disk path does not exist",
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{{Name: "default", Type: "local", Path: "/non/existent/clickhouse/path"}}
			},
			wantErr: "is not exists",
		},
		{
			name: "secondary local disk empty is fine (no marker required)",
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{
					{Name: "default", Type: "local", Path: mkLocalDisk(t, "store")},
					{Name: "hdd3", Type: "local", Path: mkLocalDisk(t, "")},
				}
			},
		},
		{
			name: "secondary local disk path must still exist",
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{
					{Name: "default", Type: "local", Path: mkLocalDisk(t, "store")},
					{Name: "hdd3", Type: "local", Path: "/non/existent/hdd3_data"},
				}
			},
			wantErr: "is not exists",
		},
		{
			name: "object storage disk path does not exist still fails",
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{{Name: "s3", Type: "s3", Path: "/non/existent/s3/meta"}}
			},
			wantErr: "is not exists",
		},
		{
			name: "object storage disk exists empty is fine",
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{{Name: "s3", Type: "s3", Path: mkLocalDisk(t, "")}}
			},
		},
		{
			name:      "skipped disk is ignored even when missing",
			skipDisks: []string{"default"},
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{{Name: "default", Type: "local", Path: "/non/existent/clickhouse/path"}}
			},
		},
		{
			name: "backup disk is ignored",
			disks: func(t *testing.T) []clickhouse.Disk {
				return []clickhouse.Disk{{Name: "backup", Type: "local", Path: "/non/existent/backup", IsBackup: true}}
			},
		},
		{
			name: "disk path is a file not a directory",
			disks: func(t *testing.T) []clickhouse.Disk {
				root := t.TempDir()
				f := path.Join(root, "not-a-dir")
				if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
					t.Fatalf("write file: %v", err)
				}
				return []clickhouse.Disk{{Name: "default", Type: "local", Path: f}}
			},
			wantErr: "is not a directory",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				ClickHouse: config.ClickHouseConfig{
					SkipDisks: tc.skipDisks,
				},
			}
			b := NewBackuper(cfg)
			err := b.checkDisksConsistency(tc.disks(t))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
