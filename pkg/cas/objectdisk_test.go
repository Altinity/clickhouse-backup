package cas_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
)

func TestIsObjectDiskType(t *testing.T) {
	yes := []string{"s3", "s3_plain", "azure_blob_storage", "hdfs", "web"}
	no := []string{"local", "encrypted", "memory", ""}
	for _, s := range yes {
		if !cas.IsObjectDiskType(s) {
			t.Errorf("yes: %q wrongly false", s)
		}
	}
	for _, s := range no {
		if cas.IsObjectDiskType(s) {
			t.Errorf("no: %q wrongly true", s)
		}
	}
}

func sortHits(h []cas.ObjectDiskHit) {
	sort.Slice(h, func(i, j int) bool {
		if h[i].Database != h[j].Database {
			return h[i].Database < h[j].Database
		}
		if h[i].Table != h[j].Table {
			return h[i].Table < h[j].Table
		}
		return h[i].Disk < h[j].Disk
	})
}

func TestDetectObjectDiskTables_HappyPath(t *testing.T) {
	disks := []cas.DiskInfo{
		{Name: "default", Path: "/var/lib/clickhouse/", Type: "local"},
		{Name: "s3main", Path: "/var/lib/clickhouse/disks/s3/", Type: "s3"},
		{Name: "azhot", Path: "/var/lib/clickhouse/disks/azure/", Type: "azure_blob_storage"},
	}
	tables := []cas.TableInfo{
		{Database: "db1", Name: "t_local", DataPaths: []string{"/var/lib/clickhouse/data/db1/t_local/"}},
		{Database: "db1", Name: "t_s3", DataPaths: []string{"/var/lib/clickhouse/disks/s3/data/db1/t_s3/"}},
		{Database: "db1", Name: "t_az", DataPaths: []string{"/var/lib/clickhouse/disks/azure/data/db1/t_az/"}},
		{Database: "db1", Name: "t_multi", DataPaths: []string{
			"/var/lib/clickhouse/data/db1/t_multi/",          // local
			"/var/lib/clickhouse/disks/s3/data/db1/t_multi/", // object
		}},
	}
	got := cas.DetectObjectDiskTables(tables, disks)
	want := []cas.ObjectDiskHit{
		{Database: "db1", Table: "t_az", Disk: "azhot", DiskType: "azure_blob_storage"},
		{Database: "db1", Table: "t_multi", Disk: "s3main", DiskType: "s3"},
		{Database: "db1", Table: "t_s3", Disk: "s3main", DiskType: "s3"},
	}
	sortHits(got)
	sortHits(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestDetectObjectDiskTables_LongestPrefixWins(t *testing.T) {
	// /var/lib/clickhouse/ is local; /var/lib/clickhouse/disks/s3/ is s3.
	// A path under disks/s3/ must NOT be classified as local even though the
	// local prefix also matches.
	disks := []cas.DiskInfo{
		{Name: "default", Path: "/var/lib/clickhouse/", Type: "local"},
		{Name: "s3", Path: "/var/lib/clickhouse/disks/s3/", Type: "s3"},
	}
	tables := []cas.TableInfo{
		{Database: "db", Name: "t", DataPaths: []string{"/var/lib/clickhouse/disks/s3/data/db/t/"}},
	}
	got := cas.DetectObjectDiskTables(tables, disks)
	if len(got) != 1 || got[0].Disk != "s3" {
		t.Fatalf("got %+v", got)
	}
}

func TestDetectObjectDiskTables_NoFalsePositiveOnSiblingPrefix(t *testing.T) {
	// A disk at /foo/ should NOT match a path /foobar/...
	disks := []cas.DiskInfo{
		{Name: "d1", Path: "/foo/", Type: "s3"},
	}
	tables := []cas.TableInfo{
		{Database: "db", Name: "t", DataPaths: []string{"/foobar/data/"}},
	}
	if got := cas.DetectObjectDiskTables(tables, disks); len(got) != 0 {
		t.Fatalf("expected no hits, got %+v", got)
	}
}

func TestDetectObjectDiskTables_DedupesSameTriple(t *testing.T) {
	disks := []cas.DiskInfo{{Name: "s3", Path: "/s3/", Type: "s3"}}
	tables := []cas.TableInfo{
		{Database: "db", Name: "t", DataPaths: []string{
			"/s3/a/", "/s3/b/", // two paths under the same disk
		}},
	}
	if got := cas.DetectObjectDiskTables(tables, disks); len(got) != 1 {
		t.Fatalf("expected 1 deduped hit, got %+v", got)
	}
}

func TestDetectObjectDiskTables_EmptyInputs(t *testing.T) {
	if got := cas.DetectObjectDiskTables(nil, nil); len(got) != 0 {
		t.Fatal("nil/nil")
	}
	if got := cas.DetectObjectDiskTables([]cas.TableInfo{}, []cas.DiskInfo{}); len(got) != 0 {
		t.Fatal("empty")
	}
}
