package backup

import (
	"os"
	"path"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// https://github.com/Altinity/clickhouse-backup/issues/1563
func TestFreezesStateAddListRemove(t *testing.T) {
	r := require.New(t)
	file := freezesFilePath(t.TempDir(), "backup1")
	f, err := openFreezes(file)
	r.NoError(err)
	r.NoError(f.add("uuid1", freezeRecord{Database: "db", Table: "t1"}))
	r.NoError(f.add("uuid2", freezeRecord{Database: "db", Table: "t2"}))

	records, err := f.list()
	r.NoError(err)
	r.Len(records, 2)
	r.Equal("t1", records["uuid1"].Table)
	r.Equal("db", records["uuid2"].Database)

	r.NoError(f.remove("uuid1"))
	// the file is kept while records remain
	r.NoError(f.closeAndRemove())
	r.FileExists(file)

	// records survive reopen, as after SIGKILL
	f, err = openFreezes(file)
	r.NoError(err)
	records, err = f.list()
	r.NoError(err)
	r.Len(records, 1)
	r.Equal("t2", records["uuid2"].Table)
	r.NoError(f.remove("uuid2"))
	r.NoError(f.closeAndRemove())
	r.NoFileExists(file)
}

// a second process working on the same backup holds the bbolt lock, openFreezes must fail fast with ErrTimeout
func TestFreezesStateLockedByAnotherOpen(t *testing.T) {
	r := require.New(t)
	file := freezesFilePath(t.TempDir(), "backup1")
	f, err := openFreezes(file)
	r.NoError(err)
	defer func() { r.NoError(f.close()) }()

	_, err = openFreezes(file)
	r.Error(err)
	r.True(errors.Is(err, bolt.ErrTimeout), "expected bolt.ErrTimeout, got %v", err)
}

// removeFreeze keeps the record while shadow/<uuid> still exists on any disk
func TestRemoveFreezeKeepsRecordWhileShadowExists(t *testing.T) {
	r := require.New(t)
	dataPath := t.TempDir()
	disks := []clickhouse.Disk{
		{Name: "default", Path: dataPath},
		{Name: "backups", Path: path.Join(dataPath, "backups"), IsBackup: true},
	}
	b := &Backuper{DefaultDataPath: dataPath}
	r.NoError(b.addFreeze("backup1", "uuid1", &clickhouse.Table{Database: "db", Name: "t1"}))
	shadowDir := path.Join(dataPath, "shadow", "uuid1")
	r.NoError(os.MkdirAll(shadowDir, 0755))

	r.NoError(b.removeFreeze("uuid1", disks))
	records, err := b.freezes.list()
	r.NoError(err)
	r.Len(records, 1, "record must stay while shadow/uuid1 exists")

	r.NoError(os.RemoveAll(shadowDir))
	r.NoError(b.removeFreeze("uuid1", disks))
	records, err = b.freezes.list()
	r.NoError(err)
	r.Empty(records)
	r.NoError(b.freezes.closeAndRemove())
	r.NoFileExists(freezesFilePath(dataPath, "backup1"))
}
