package backup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"

	"github.com/stretchr/testify/require"
)

// a killed download leaves storage.ExtractTempDirPrefix directories, they must be removed for the downloaded
// backup only, part directories and their content must stay untouched, see issue #1633
func TestRemoveExtractLeftovers(t *testing.T) {
	defaultPath := t.TempDir()
	hddPath := t.TempDir()
	b := &Backuper{DiskToPathMap: map[string]string{"default": defaultPath, "hdd": hddPath}}
	tmp := storage.ExtractTempDirPrefix + "123"
	removed := []string{
		filepath.Join(defaultPath, "backup", "b1", "shadow", "db", "t", "default", tmp),
		filepath.Join(hddPath, "backup", "b1", "shadow", "db", "t", "hdd", tmp),
		filepath.Join(defaultPath, "backup", "b1", "access", tmp),
		filepath.Join(defaultPath, "backup", "b1", "configs", tmp),
	}
	kept := []string{
		filepath.Join(defaultPath, "backup", "b1", "shadow", "db", "t", "default", "all_1_1_0", "data.bin"),
		// in-flight extraction of another backup
		filepath.Join(defaultPath, "backup", "b2", "shadow", "db", "t", "default", tmp, "all_1_1_0", "data.bin"),
		// below the depth limit, only part content lives there
		filepath.Join(defaultPath, "backup", "b1", "shadow", "db", "t", "default", "all_1_1_0", tmp, "x.bin"),
	}
	for _, dir := range removed {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "all_1_1_0"), 0750))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "all_1_1_0", "data.bin"), []byte("x"), 0640))
	}
	for _, file := range kept {
		require.NoError(t, os.MkdirAll(filepath.Dir(file), 0750))
		require.NoError(t, os.WriteFile(file, []byte("x"), 0640))
	}

	require.NoError(t, b.removeExtractLeftovers("b1"))
	for _, dir := range removed {
		require.NoDirExists(t, dir)
	}
	for _, file := range kept {
		require.FileExists(t, file)
	}

	t.Run("embedded", func(t *testing.T) {
		b.isEmbedded = true
		b.embeddedClusterPrefix = "shards/1/replicas/1"
		dir := filepath.Join(hddPath, "b3", "shards", "1", "replicas", "1", "data", "db", "t", tmp)
		require.NoError(t, os.MkdirAll(dir, 0750))
		require.NoError(t, b.removeExtractLeftovers("b3"))
		require.NoDirExists(t, dir)
	})

	t.Run("missing backup", func(t *testing.T) {
		require.NoError(t, b.removeExtractLeftovers("absent"))
	})
}
