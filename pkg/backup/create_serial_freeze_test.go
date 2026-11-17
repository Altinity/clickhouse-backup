package backup

import (
	"os"
	"path"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"

	"github.com/stretchr/testify/require"
)

// https://github.com/Altinity/clickhouse-backup/issues/1651
func TestSerialFreezeTableIndex(t *testing.T) {
	disks := []clickhouse.Disk{
		{Name: "default", Type: "local", Path: "/var/lib/clickhouse/"},
		{Name: "plain", Type: "s3", MetadataType: "plain_rewritable", Path: "/var/lib/clickhouse/disks/plain/", RawPath: "plain/"},
	}
	full := func(name, engine string, totalBytes uint64) clickhouse.Table {
		return clickhouse.Table{Database: "db", Name: name, Engine: engine, TotalBytes: totalBytes, BackupType: clickhouse.ShardBackupFull, DataPaths: []string{"/var/lib/clickhouse/store/abc/" + name + "/"}}
	}
	plainTable := full("on_plain", "MergeTree", 1)
	plainTable.DataPaths = []string{"/var/lib/clickhouse/disks/plain/store/abc/on_plain/"}
	tables := []clickhouse.Table{
		// GetTables sorts by total_bytes DESC, the biggest table goes first
		full("big", "ReplicatedMergeTree", 1000),
		{Database: "db", Name: "skipped", Engine: "MergeTree", Skip: true, BackupType: clickhouse.ShardBackupNone},
		{Database: "db", Name: "schema_only", Engine: "MergeTree", BackupType: clickhouse.ShardBackupSchema},
		full("view", "View", 0),
		full("mv", "MaterializedView", 0),
		full("distributed", "Distributed", 0),
		full("kafka", "Kafka", 0),
		plainTable,
		full("small", "MergeTree", 10),
		full("empty", "MergeTree", 0),
		full("mysql", "MaterializedMySQL", 5),
	}
	r := require.New(t)
	newBackuper := func(dataPath string, freezeByPart bool) *Backuper {
		cfg := config.DefaultConfig()
		cfg.ClickHouse.FreezeByPart = freezeByPart
		return &Backuper{cfg: cfg, DefaultDataPath: dataPath}
	}
	freshDir := t.TempDir()
	b := newBackuper(freshDir, false)
	// ALTER TABLE ... FREEZE increments the counter even for an empty table, the smallest table which really runs FREEZE goes first
	r.Equal(9, b.serialFreezeTableIndex(tables, disks, true))
	// FREEZE PARTITION of freeze_by_part runs only for existing parts, an empty table creates no counter
	r.Equal(10, newBackuper(freshDir, true).serialFreezeTableIndex(tables, disks, true))
	r.Equal(-1, newBackuper(freshDir, true).serialFreezeTableIndex(tables[9:10], disks, true))
	// schema only engines and tables on plain disks run no FREEZE
	r.Equal(-1, b.serialFreezeTableIndex(tables[1:8], disks, true))
	// data path is not reachable from this host, the same fallback
	r.Equal(0, newBackuper(path.Join(freshDir, "not-exists"), false).serialFreezeTableIndex(tables[:1], disks, true))
	// no FREEZE at all
	r.Equal(-1, b.serialFreezeTableIndex(tables, disks, false))
	r.Equal(-1, b.serialFreezeTableIndex(nil, disks, true))

	// increment.txt already has a value, everything stays parallel
	warmDir := t.TempDir()
	incrementFile := path.Join(warmDir, "shadow", shadowIncrementFile)
	r.NoError(os.MkdirAll(path.Dir(incrementFile), 0750))
	r.NoError(os.WriteFile(incrementFile, []byte("1\n"), 0640))
	r.Equal(-1, newBackuper(warmDir, false).serialFreezeTableIndex(tables, disks, true))
	r.FileExists(incrementFile)

	// an empty increment.txt fails every FREEZE with code 32, it is removed and the first FREEZE runs alone to recreate it
	r.NoError(os.WriteFile(incrementFile, nil, 0640))
	// nothing to FREEZE, the empty file stays as is
	r.Equal(-1, newBackuper(warmDir, false).serialFreezeTableIndex(tables[1:8], disks, true))
	r.FileExists(incrementFile)
	r.Equal(9, newBackuper(warmDir, false).serialFreezeTableIndex(tables, disks, true))
	r.NoFileExists(incrementFile)
}
