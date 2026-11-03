package backup

import (
	"os"
	"path"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"

	"github.com/stretchr/testify/require"
)

// https://github.com/Altinity/clickhouse-backup/issues/1651
func TestSerialFreezeTableIndex(t *testing.T) {
	tables := []clickhouse.Table{
		{Name: "skipped", Skip: true, BackupType: clickhouse.ShardBackupFull},
		{Name: "schema_only", BackupType: clickhouse.ShardBackupSchema},
		{Name: "first_full", BackupType: clickhouse.ShardBackupFull},
		{Name: "second_full", BackupType: clickhouse.ShardBackupFull},
	}
	freshDir := t.TempDir()
	warmDir := t.TempDir()
	require.NoError(t, os.MkdirAll(path.Join(warmDir, "shadow"), 0750))
	require.NoError(t, os.WriteFile(path.Join(warmDir, "shadow", shadowIncrementFile), []byte("1"), 0640))

	r := require.New(t)
	// fresh server: first table which really runs FREEZE goes serially
	r.Equal(2, serialFreezeTableIndex(tables, true, freshDir))
	// data path is not reachable from this host: same fallback
	r.Equal(2, serialFreezeTableIndex(tables, true, path.Join(freshDir, "not-exists")))
	// increment.txt already exists: everything stays parallel
	r.Equal(-1, serialFreezeTableIndex(tables, true, warmDir))
	// no FREEZE at all
	r.Equal(-1, serialFreezeTableIndex(tables, false, freshDir))
	r.Equal(-1, serialFreezeTableIndex(tables[:2], true, freshDir))
	r.Equal(-1, serialFreezeTableIndex(nil, true, freshDir))
}
