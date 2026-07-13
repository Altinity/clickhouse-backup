package storage

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gunzipToTemp decompresses a .gz file into a temporary file and returns its path.
func gunzipToTemp(t *testing.T, gzPath string) string {
	t.Helper()
	src, err := os.Open(gzPath)
	require.NoError(t, err)
	defer src.Close()
	gz, err := gzip.NewReader(src)
	require.NoError(t, err)
	dst, err := os.CreateTemp(t.TempDir(), "manifest-*.bolt")
	require.NoError(t, err)
	_, err = io.Copy(dst, gz)
	require.NoError(t, err)
	require.NoError(t, dst.Close())
	return dst.Name()
}

func finalizeAndOpen(t *testing.T, w *ManifestWriter) *ManifestReader {
	t.Helper()
	gzPath, err := w.Finalize()
	require.NoError(t, err)
	reader, err := openManifestReader(gunzipToTemp(t, gzPath))
	require.NoError(t, err)
	return reader
}

func TestManifestWriterReaderRoundtrip(t *testing.T) {
	t.Parallel()
	w, err := NewManifestWriter("test_backup")
	require.NoError(t, err)
	defer w.Close()

	w.AddFile("shadow/default/table1/default/part1/data.bin")
	w.AddFile("shadow/default/table1/default/part1/primary.idx")
	w.AddFile("shadow/default/table1/default/part2/data.bin")
	w.AddFile("shadow/default/table2/default/part1/data.bin")
	w.AddFile("metadata/default/table1.json")

	assert.Equal(t, 5, w.TotalFiles)

	reader := finalizeAndOpen(t, w)
	defer reader.Close()

	// Files under part1 of table1, names relative to the prefix
	files, err := reader.FilesUnderPrefix("shadow/default/table1/default/part1")
	require.NoError(t, err)
	assert.Equal(t, []string{"data.bin", "primary.idx"}, files)

	// Files under table1 (all parts)
	files, err = reader.FilesUnderPrefix("shadow/default/table1")
	require.NoError(t, err)
	assert.Equal(t, []string{"default/part1/data.bin", "default/part1/primary.idx", "default/part2/data.bin"}, files)

	// Trailing slash is equivalent
	files2, err := reader.FilesUnderPrefix("shadow/default/table1/")
	require.NoError(t, err)
	assert.Equal(t, files, files2)

	// Non-existent prefix
	files, err = reader.FilesUnderPrefix("shadow/default/table3")
	require.NoError(t, err)
	assert.Empty(t, files)
}

func TestManifestReader_FilesUnderPrefix_NoPartialMatch(t *testing.T) {
	t.Parallel()
	w, err := NewManifestWriter("test")
	require.NoError(t, err)
	defer w.Close()

	// "part1" should not match "part10" or "part1_suffix"
	w.AddFile("shadow/db/tbl/disk/part1/file.bin")
	w.AddFile("shadow/db/tbl/disk/part10/file.bin")
	w.AddFile("shadow/db/tbl/disk/part1_suffix/file.bin")

	reader := finalizeAndOpen(t, w)
	defer reader.Close()

	files, err := reader.FilesUnderPrefix("shadow/db/tbl/disk/part1")
	require.NoError(t, err)
	assert.Equal(t, []string{"file.bin"}, files, "should only match exact prefix with trailing slash")
}

func TestManifestWriter_BatchFlush(t *testing.T) {
	t.Parallel()
	w, err := NewManifestWriter("large_backup")
	require.NoError(t, err)
	defer w.Close()

	// Cross manifestBatchSize to force an intermediate flush plus the Finalize flush
	total := manifestBatchSize + 100
	for i := 0; i < total; i++ {
		w.AddFile(fmt.Sprintf("shadow/default/trades/default/part%05d/data.bin", i))
	}
	assert.Equal(t, total, w.TotalFiles)

	reader := finalizeAndOpen(t, w)
	defer reader.Close()

	files, err := reader.FilesUnderPrefix("shadow/default/trades")
	require.NoError(t, err)
	assert.Len(t, files, total)
}

func TestManifestWriter_Concurrent(t *testing.T) {
	t.Parallel()
	w, err := NewManifestWriter("concurrent")
	require.NoError(t, err)
	defer w.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				w.AddFile(fmt.Sprintf("shadow/db/tbl/disk/part%d/file%d.bin", g, i))
			}
		}(g)
	}
	wg.Wait()
	assert.Equal(t, 8*500, w.TotalFiles)

	reader := finalizeAndOpen(t, w)
	defer reader.Close()
	files, err := reader.FilesUnderPrefix("shadow/db/tbl/disk/part3")
	require.NoError(t, err)
	assert.Len(t, files, 500)
}

func TestManifestWriter_EmptyManifest(t *testing.T) {
	t.Parallel()
	w, err := NewManifestWriter("empty")
	require.NoError(t, err)
	defer w.Close()

	reader := finalizeAndOpen(t, w)
	defer reader.Close()

	files, err := reader.FilesUnderPrefix("shadow/db/tbl")
	require.NoError(t, err)
	assert.Empty(t, files)
}

func TestManifestWriter_CloseRemovesTempFiles(t *testing.T) {
	t.Parallel()
	w, err := NewManifestWriter("cleanup")
	require.NoError(t, err)
	w.AddFile("shadow/db/tbl/disk/part/file.bin")

	gzPath, err := w.Finalize()
	require.NoError(t, err)
	boltPath := w.localPath
	_, err = os.Stat(gzPath)
	require.NoError(t, err)

	w.Close()
	_, err = os.Stat(gzPath)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(boltPath)
	assert.True(t, os.IsNotExist(err))
	// Close is idempotent and nil-safe
	w.Close()
	(*ManifestWriter)(nil).Close()
}

func TestManifestWriter_FinalizeTwiceFails(t *testing.T) {
	t.Parallel()
	w, err := NewManifestWriter("twice")
	require.NoError(t, err)
	defer w.Close()

	_, err = w.Finalize()
	require.NoError(t, err)
	_, err = w.Finalize()
	assert.Error(t, err)
}

func TestOpenManifestReader_InvalidFile(t *testing.T) {
	t.Parallel()
	tmp, err := os.CreateTemp(t.TempDir(), "garbage-*.bolt")
	require.NoError(t, err)
	_, err = tmp.WriteString("not a bolt database")
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	_, err = openManifestReader(tmp.Name())
	assert.Error(t, err)
}

func TestManifestFileName_IsCorrect(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "manifest.bolt.gz", ManifestFileName)
}

func TestManifestVersion_IsOne(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 1, ManifestVersion)
}
