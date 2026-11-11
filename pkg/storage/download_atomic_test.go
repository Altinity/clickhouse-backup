package storage

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// tarRemote serves a fixed byte stream as the remote archive, then ends it with readErr
// (io.EOF means a complete download).
type tarRemote struct {
	RemoteStorage
	data    []byte
	readErr error
}

func (m *tarRemote) StatFile(_ context.Context, _ string) (RemoteFile, error) {
	return fakeRemoteFile{size: int64(len(m.data))}, nil
}

func (m *tarRemote) GetFileReaderWithLocalPath(_ context.Context, _, _ string, _ int64) (io.ReadCloser, error) {
	return io.NopCloser(io.MultiReader(bytes.NewReader(m.data), &errReader{err: m.readErr})), nil
}

type errReader struct{ err error }

func (e *errReader) Read(_ []byte) (int, error) { return 0, e.err }

// partTar builds an archive with a part directory holding checksums.txt and a data.bin.
func partTar(t *testing.T, partName string, dataSize int) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	files := []struct {
		name string
		body []byte
	}{
		{partName + "/checksums.txt", []byte("checksums")},
		{partName + "/data.bin", bytes.Repeat([]byte("x"), dataSize)},
	}
	for _, f := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0640, Size: int64(len(f.body)), Typeflag: tar.TypeReg}))
		_, err := tw.Write(f.body)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

func newTarDestination(data []byte, readErr error) *BackupDestination {
	return &BackupDestination{
		RemoteStorage:     &tarRemote{data: data, readErr: readErr},
		compressionFormat: "tar",
		pipeBufferSize:    1024 * 1024,
	}
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// An interrupted extraction must not leave a part directory: downloadDiffParts treats an existing
// required-part directory as complete and hard-links it, see issue #1633.
func TestDownloadCompressedStreamInterruptedLeavesNoPartDir(t *testing.T) {
	full := partTar(t, "20261001_1_1_0", 64*1024)
	// cut inside data.bin, after checksums.txt was already fully written
	bd := newTarDestination(full[:len(full)/2], io.ErrUnexpectedEOF)
	localPath := filepath.Join(t.TempDir(), "table")

	_, err := bd.DownloadCompressedStream(context.Background(), "shadow/db/tbl/20261001_1_1_0.tar", localPath, 0)
	require.Error(t, err)
	require.Empty(t, dirNames(t, localPath), "no part directory and no temporary directory must remain")
}

func TestDownloadCompressedStreamMergesCompletePartIntoPlace(t *testing.T) {
	bd := newTarDestination(partTar(t, "20261001_1_1_0", 1024), io.EOF)
	localPath := filepath.Join(t.TempDir(), "table")
	// existing part directory, e.g. files of the same part extracted from another archive (upload_by_part=false)
	oldPart := filepath.Join(localPath, "20261001_1_1_0")
	require.NoError(t, os.MkdirAll(oldPart, 0750))
	require.NoError(t, os.WriteFile(filepath.Join(oldPart, "other.bin"), []byte("other"), 0640))
	require.NoError(t, os.WriteFile(filepath.Join(oldPart, "checksums.txt"), []byte("old"), 0640))

	n, err := bd.DownloadCompressedStream(context.Background(), "shadow/db/tbl/20261001_1_1_0.tar", localPath, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1024+len("checksums")), n)
	require.Equal(t, []string{"20261001_1_1_0"}, dirNames(t, localPath))
	require.ElementsMatch(t, []string{"checksums.txt", "data.bin", "other.bin"}, dirNames(t, oldPart))
	checksums, err := os.ReadFile(filepath.Join(oldPart, "checksums.txt"))
	require.NoError(t, err)
	require.Equal(t, "checksums", string(checksums), "a file from the archive replaces the existing one")
}

// filesTar builds an archive holding the given name->body entries in order.
func filesTar(t *testing.T, files [][2]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: f[0], Mode: 0640, Size: int64(len(f[1])), Typeflag: tar.TypeReg}))
		_, err := tw.Write([]byte(f[1]))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

// With upload_by_part=false splitFilesBySize can put the files of one part into several archives,
// all of them are extracted into the same table directory, the result must be the union.
func TestDownloadCompressedStreamMergesPartSplitAcrossArchives(t *testing.T) {
	first := filesTar(t, [][2]string{{"all_1_1_0/checksums.txt", "c1"}, {"all_1_1_0/a.bin", "a"}})
	second := filesTar(t, [][2]string{{"all_1_1_0/b.bin", "b"}, {"all_2_2_0/checksums.txt", "c2"}})
	expected := map[string][]string{
		"all_1_1_0": {"checksums.txt", "a.bin", "b.bin"},
		"all_2_2_0": {"checksums.txt"},
	}
	assertMerged := func(t *testing.T, localPath string) {
		require.ElementsMatch(t, []string{"all_1_1_0", "all_2_2_0"}, dirNames(t, localPath))
		for part, files := range expected {
			require.ElementsMatch(t, files, dirNames(t, filepath.Join(localPath, part)), part)
		}
	}

	t.Run("sequential", func(t *testing.T) {
		localPath := filepath.Join(t.TempDir(), "table")
		for _, archive := range [][]byte{first, second} {
			_, err := newTarDestination(archive, io.EOF).DownloadCompressedStream(context.Background(), "shadow/db/tbl/default_1.tar", localPath, 0)
			require.NoError(t, err)
		}
		assertMerged(t, localPath)
	})

	t.Run("concurrent", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			localPath := filepath.Join(t.TempDir(), "table")
			var wg sync.WaitGroup
			errs := make([]error, 2)
			for j, archive := range [][]byte{first, second} {
				wg.Add(1)
				go func(j int, archive []byte) {
					defer wg.Done()
					_, errs[j] = newTarDestination(archive, io.EOF).DownloadCompressedStream(context.Background(), "shadow/db/tbl/default_1.tar", localPath, 0)
				}(j, archive)
			}
			wg.Wait()
			require.NoError(t, errs[0])
			require.NoError(t, errs[1])
			assertMerged(t, localPath)
		}
	})
}
