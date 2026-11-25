package backup

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

// https://github.com/Altinity/clickhouse-backup/issues/1651
func TestRemoveEmptyShadowIncrement(t *testing.T) {
	r := require.New(t)
	dataPath := t.TempDir()
	b := &Backuper{DefaultDataPath: dataPath}
	incrementFile := path.Join(dataPath, "shadow", shadowIncrementFile)

	// missing file and missing shadow dir, nothing to do
	b.removeEmptyShadowIncrement()
	r.NoFileExists(incrementFile)

	// the counter has a value, it stays as is
	r.NoError(os.MkdirAll(path.Dir(incrementFile), 0750))
	r.NoError(os.WriteFile(incrementFile, []byte("1\n"), 0640))
	b.removeEmptyShadowIncrement()
	r.FileExists(incrementFile)

	// an empty counter fails every FREEZE with code 32, it is removed so clickhouse-server recreates it
	r.NoError(os.WriteFile(incrementFile, nil, 0640))
	b.removeEmptyShadowIncrement()
	r.NoFileExists(incrementFile)
}
