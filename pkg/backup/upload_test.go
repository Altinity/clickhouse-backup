package backup

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/stretchr/testify/require"
)

// makePartDir creates <basePath>/<partName>/ with the given file names, projection files go to <proj>.proj/ sub-directory
func makePartDir(t *testing.T, basePath, partName string, files ...string) {
	for _, f := range files {
		fullPath := path.Join(basePath, partName, f)
		require.NoError(t, os.MkdirAll(path.Dir(fullPath), 0o755))
		require.NoError(t, os.WriteFile(fullPath, []byte("x"), 0o644))
	}
}

// https://github.com/Altinity/clickhouse-backup/issues/1550
func TestSplitFilesByNameReturnsOnlyPrefixes(t *testing.T) {
	b := &Backuper{cfg: &config.Config{General: config.GeneralConfig{UploadByPart: true}}}
	parts := []metadata.Part{
		{Name: "20260101_1_1_0"},
		{Name: "20260102_2_2_0", Required: true},
		{Name: "20260103_3_3_0"},
	}
	split, err := b.splitPartFiles(t.TempDir(), parts, "db", "table", nil)
	require.NoError(t, err)
	require.Equal(t, []metadata.SplitPartFiles{{Prefix: "20260101_1_1_0"}, {Prefix: "20260103_3_3_0"}}, split)
}

func TestWalkPartFiles(t *testing.T) {
	basePath := t.TempDir()
	makePartDir(t, basePath, "20260101_1_1_0", "checksums.txt", "col1.bin", "col1.cmrk2", "p1.proj/col1.bin", "p1.proj/checksums.txt")
	makePartDir(t, basePath, "20260102_2_2_0", "checksums.txt", "col1.bin")
	b := &Backuper{}

	files := b.walkPartFiles(basePath, "20260101_1_1_0", "db", "table", nil)
	require.ElementsMatch(t, []string{
		"/20260101_1_1_0/checksums.txt", "/20260101_1_1_0/col1.bin", "/20260101_1_1_0/col1.cmrk2",
		"/20260101_1_1_0/p1.proj/col1.bin", "/20260101_1_1_0/p1.proj/checksums.txt",
	}, files, "only files of the requested part, relative to basePath")

	files = b.walkPartFiles(basePath, "20260101_1_1_0", "db", "table", []string{"db.table:p1"})
	require.ElementsMatch(t, []string{"/20260101_1_1_0/checksums.txt", "/20260101_1_1_0/col1.bin", "/20260101_1_1_0/col1.cmrk2"}, files, "skipProjections still applied")

	require.Empty(t, b.walkPartFiles(basePath, "not_exists", "db", "table", nil))
}

// whole-table list (old behavior) vs one part at a time (new behavior), reported via -v
func TestWalkPartFilesMemory(t *testing.T) {
	basePath := path.Join(t.TempDir(), "backup", "shadow", "db", "table", "default")
	const partsCount, filesPerPart = 50, 400
	parts := make([]metadata.Part, partsCount)
	for i := range parts {
		parts[i].Name = fmt.Sprintf("20260101_%d_%d_0", i, i)
		names := make([]string, filesPerPart)
		for j := range names {
			names[j] = fmt.Sprintf("column_name_%d.bin", j)
		}
		makePartDir(t, basePath, parts[i].Name, names...)
	}
	b := &Backuper{}
	heapInuse := func() uint64 {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapInuse
	}
	base := heapInuse()
	whole := make([][]string, 0, partsCount)
	for _, p := range parts {
		whole = append(whole, b.walkPartFiles(basePath, p.Name, "db", "table", nil))
	}
	wholeTable := heapInuse() - base
	require.Len(t, whole, partsCount)
	whole = nil
	base = heapInuse()
	var maxOnePart uint64
	for _, p := range parts {
		files := b.walkPartFiles(basePath, p.Name, "db", "table", nil)
		if inuse := heapInuse() - base; inuse > maxOnePart {
			maxOnePart = inuse
		}
		require.Len(t, files, filesPerPart)
	}
	t.Logf("%d parts x %d files: whole table list = %d KiB, one part at a time = %d KiB", partsCount, filesPerPart, wholeTable/1024, maxOnePart/1024)
	require.Less(t, maxOnePart*10, wholeTable)
}
