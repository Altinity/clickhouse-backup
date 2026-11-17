package storage

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
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

func TestDownloadCompressedStreamMovesCompletePartIntoPlace(t *testing.T) {
	bd := newTarDestination(partTar(t, "20261001_1_1_0", 1024), io.EOF)
	localPath := filepath.Join(t.TempDir(), "table")
	// leftover of an earlier failed attempt, written by an older version
	oldPart := filepath.Join(localPath, "20261001_1_1_0")
	require.NoError(t, os.MkdirAll(oldPart, 0750))
	require.NoError(t, os.WriteFile(filepath.Join(oldPart, "stale.bin"), []byte("stale"), 0640))

	n, err := bd.DownloadCompressedStream(context.Background(), "shadow/db/tbl/20261001_1_1_0.tar", localPath, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1024+len("checksums")), n)
	require.Equal(t, []string{"20261001_1_1_0"}, dirNames(t, localPath))
	require.ElementsMatch(t, []string{"checksums.txt", "data.bin"}, dirNames(t, oldPart))
}
