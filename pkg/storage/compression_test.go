package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/mholt/archives"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func timeParse(s string) time.Time {
	t, err := time.Parse("2006-01-02T15-04-05", s)
	if err != nil {
		log.Fatal().Stack().Err(err).Send()
	}
	return t
}

func TestGetBackupsToDelete(t *testing.T) {
	testData := []Backup{
		{metadata.BackupMetadata{BackupName: "three"}, "", timeParse("2019-03-28T19-50-13")},
		{metadata.BackupMetadata{BackupName: "one"}, "", timeParse("2019-03-28T19-50-11")},
		{metadata.BackupMetadata{BackupName: "five"}, "", timeParse("2019-03-28T19-50-15")},
		{metadata.BackupMetadata{BackupName: "two"}, "", timeParse("2019-03-28T19-50-12")},
		{metadata.BackupMetadata{BackupName: "four"}, "", timeParse("2019-03-28T19-50-14")},
	}
	expectedData := []Backup{
		{metadata.BackupMetadata{BackupName: "two"}, "", timeParse("2019-03-28T19-50-12")},
		{metadata.BackupMetadata{BackupName: "one"}, "", timeParse("2019-03-28T19-50-11")},
	}
	assert.Equal(t, expectedData, GetBackupsToDeleteRemote(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDeleteRemote([]Backup{testData[0]}, 3))
}

func TestGetBackupsToDeleteWithRequiredBackup(t *testing.T) {
	// fix https://github.com/Altinity/clickhouse-backup/issues/111
	testData := []Backup{
		{metadata.BackupMetadata{BackupName: "3"}, "", timeParse("2019-03-28T19-50-13")},
		{metadata.BackupMetadata{BackupName: "1"}, "", timeParse("2019-03-28T19-50-11")},
		{metadata.BackupMetadata{BackupName: "5", RequiredBackup: "2"}, "", timeParse("2019-03-28T19-50-15")},
		{metadata.BackupMetadata{BackupName: "2"}, "", timeParse("2019-03-28T19-50-12")},
		{metadata.BackupMetadata{BackupName: "4", RequiredBackup: "3"}, "", timeParse("2019-03-28T19-50-14")},
	}
	expectedData := []Backup{
		{metadata.BackupMetadata{BackupName: "1"}, "", timeParse("2019-03-28T19-50-11")},
	}
	assert.Equal(t, expectedData, GetBackupsToDeleteRemote(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDeleteRemote([]Backup{testData[0]}, 3))

	// fix https://github.com/Altinity/clickhouse-backup/issues/385
	testData = []Backup{
		{metadata.BackupMetadata{BackupName: "3", RequiredBackup: "2"}, "", timeParse("2019-03-28T19-50-13")},
		{metadata.BackupMetadata{BackupName: "1"}, "", timeParse("2019-03-28T19-50-11")},
		{metadata.BackupMetadata{BackupName: "5", RequiredBackup: "4"}, "", timeParse("2019-03-28T19-50-15")},
		{metadata.BackupMetadata{BackupName: "2", RequiredBackup: "1"}, "", timeParse("2019-03-28T19-50-12")},
		{metadata.BackupMetadata{BackupName: "4", RequiredBackup: "3"}, "", timeParse("2019-03-28T19-50-14")},
	}
	expectedData = []Backup{}
	assert.Equal(t, expectedData, GetBackupsToDeleteRemote(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDeleteRemote([]Backup{testData[0]}, 3))

}

func TestGetBackupsToDeleteWithInvalidUploadDate(t *testing.T) {
	// fix https://github.com/Altinity/clickhouse-backup/issues/409
	testData := []Backup{
		{metadata.BackupMetadata{BackupName: "1"}, "", timeParse("2022-03-03T18-08-01")},
		{metadata.BackupMetadata{BackupName: "2"}, "", timeParse("2022-03-03T18-08-02")},
		{BackupMetadata: metadata.BackupMetadata{BackupName: "3"}, Broken: ""}, // UploadDate initialized with default value
		{metadata.BackupMetadata{BackupName: "4"}, "", timeParse("2022-03-03T18-08-04")},
	}
	expectedData := []Backup{
		{metadata.BackupMetadata{BackupName: "1"}, "", timeParse("2022-03-03T18-08-01")},
	}
	assert.Equal(t, expectedData, GetBackupsToDeleteRemote(testData, 2))

}

func TestGetBackupsToDeleteWithRecursiveRequiredBackups(t *testing.T) {
	// fix https://github.com/Altinity/clickhouse-backup/issues/525
	testData := []Backup{
		{metadata.BackupMetadata{BackupName: "2022-09-01T05-00-01"}, "", timeParse("2022-09-01T05-00-01")},
		{metadata.BackupMetadata{BackupName: "2022-09-01T21-00-03", RequiredBackup: "2022-09-01T05-00-01"}, "", timeParse("2022-09-01T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-02T21-00-02", RequiredBackup: "2022-09-01T21-00-03"}, "", timeParse("2022-09-02T21-00-02")},
		{metadata.BackupMetadata{BackupName: "2022-09-03T21-00-03", RequiredBackup: "2022-09-02T21-00-02"}, "", timeParse("2022-09-03T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-04T21-00-03", RequiredBackup: "2022-09-04T21-00-03"}, "", timeParse("2022-09-04T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-05T21-00-03", RequiredBackup: "2022-09-04T21-00-03"}, "", timeParse("2022-09-05T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-06T21-00-03", RequiredBackup: "2022-09-05T21-00-03"}, "", timeParse("2022-09-06T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-07T21-00-03", RequiredBackup: "2022-09-06T21-00-03"}, "", timeParse("2022-09-07T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-08T21-00-03", RequiredBackup: "2022-09-07T21-00-03"}, "", timeParse("2022-09-08T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-09T21-00-03", RequiredBackup: "2022-09-08T21-00-03"}, "", timeParse("2022-09-09T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-10T21-00-03", RequiredBackup: "2022-09-09T21-00-03"}, "", timeParse("2022-09-10T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-11T21-00-03", RequiredBackup: "2022-09-10T21-00-03"}, "", timeParse("2022-09-11T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-12T21-00-02", RequiredBackup: "2022-09-11T21-00-03"}, "", timeParse("2022-09-12T21-00-02")},
		{metadata.BackupMetadata{BackupName: "2022-09-13T21-00-03", RequiredBackup: "2022-09-12T21-00-02"}, "", timeParse("2022-09-13T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-14T21-00-03", RequiredBackup: "2022-09-13T21-00-03"}, "", timeParse("2022-09-14T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T09-30-20"}, "", timeParse("2022-10-03T09-30-20")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T09-39-37", RequiredBackup: "2022-10-03T09-30-20"}, "", timeParse("2022-10-03T09-39-37")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T09-40-03", RequiredBackup: "2022-10-03T09-39-37"}, "", timeParse("2022-10-03T09-40-03")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T09-41-31", RequiredBackup: "2022-10-03T09-40-03"}, "", timeParse("2022-10-03T09-41-31")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T09-52-12", RequiredBackup: "2022-10-03T09-41-31"}, "", timeParse("2022-10-03T09-52-12")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T10-11-15", RequiredBackup: "2022-10-03T09-52-12"}, "", timeParse("2022-10-03T10-11-15")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T10-12-38", RequiredBackup: "2022-10-03T10-11-15"}, "", timeParse("2022-10-03T10-12-38")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T10-12-57", RequiredBackup: "2022-10-03T10-12-38"}, "", timeParse("2022-10-03T10-12-57")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T10-13-16", RequiredBackup: "2022-10-03T10-12-57"}, "", timeParse("2022-10-03T10-13-16")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T10-15-32", RequiredBackup: "2022-10-03T10-13-16"}, "", timeParse("2022-10-03T10-15-32")},
	}
	expectedData := []Backup{
		{metadata.BackupMetadata{BackupName: "2022-09-14T21-00-03", RequiredBackup: "2022-09-13T21-00-03"}, "", timeParse("2022-09-14T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-13T21-00-03", RequiredBackup: "2022-09-12T21-00-02"}, "", timeParse("2022-09-13T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-12T21-00-02", RequiredBackup: "2022-09-11T21-00-03"}, "", timeParse("2022-09-12T21-00-02")},
		{metadata.BackupMetadata{BackupName: "2022-09-11T21-00-03", RequiredBackup: "2022-09-10T21-00-03"}, "", timeParse("2022-09-11T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-10T21-00-03", RequiredBackup: "2022-09-09T21-00-03"}, "", timeParse("2022-09-10T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-09T21-00-03", RequiredBackup: "2022-09-08T21-00-03"}, "", timeParse("2022-09-09T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-08T21-00-03", RequiredBackup: "2022-09-07T21-00-03"}, "", timeParse("2022-09-08T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-07T21-00-03", RequiredBackup: "2022-09-06T21-00-03"}, "", timeParse("2022-09-07T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-06T21-00-03", RequiredBackup: "2022-09-05T21-00-03"}, "", timeParse("2022-09-06T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-05T21-00-03", RequiredBackup: "2022-09-04T21-00-03"}, "", timeParse("2022-09-05T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-04T21-00-03", RequiredBackup: "2022-09-04T21-00-03"}, "", timeParse("2022-09-04T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-03T21-00-03", RequiredBackup: "2022-09-02T21-00-02"}, "", timeParse("2022-09-03T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-02T21-00-02", RequiredBackup: "2022-09-01T21-00-03"}, "", timeParse("2022-09-02T21-00-02")},
		{metadata.BackupMetadata{BackupName: "2022-09-01T21-00-03", RequiredBackup: "2022-09-01T05-00-01"}, "", timeParse("2022-09-01T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-01T05-00-01"}, "", timeParse("2022-09-01T05-00-01")},
	}
	assert.Equal(t, expectedData, GetBackupsToDeleteRemote(testData, 6))
}

func TestGetArchiveWriterReaderRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	payload := bytes.Repeat([]byte("clickhouse-backup compression payload 0123456789\n"), 4096)
	srcPath := filepath.Join(dir, "payload.bin")
	require.NoError(t, os.WriteFile(srcPath, payload, 0644))

	cases := []struct {
		format         string
		useMultiThread bool
		threads        int
		bufferSize     int
	}{
		{"gzip", false, 1, 0},
		{"gzip", false, 1, 32768},  // single-threaded DEFLATE window (max 32KB)
		{"gzip", true, 4, 0},       // pgzip default block size, 4 workers
		{"gzip", true, 4, 1 << 20}, // pgzip block size 1MB (>16384), 4 workers
		{"zstd", false, 1, 0},
		{"zstd", true, 4, 0},
		{"zstd", true, 4, 1 << 22}, // 4MB window, power of two
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s_mt=%v_threads=%d_buf=%d", tc.format, tc.useMultiThread, tc.threads, tc.bufferSize), func(t *testing.T) {
			w, err := getArchiveWriter(tc.format, 1, tc.useMultiThread, tc.threads, tc.bufferSize)
			require.NoError(t, err)
			var buf bytes.Buffer
			files := []archives.FileInfo{{
				FileInfo: func(t *testing.T, path string) os.FileInfo {
					info, err := os.Stat(path)
					require.NoError(t, err)
					return info
				}(t, srcPath),
				NameInArchive: "payload.bin",
				Open:          func() (fs.File, error) { return os.Open(srcPath) },
			}}
			require.NoError(t, w.Archive(ctx, &buf, files))
			require.NotZero(t, buf.Len())

			r, err := getArchiveReader(tc.format, tc.useMultiThread, tc.threads, tc.bufferSize)
			require.NoError(t, err)
			var got []byte
			extractErr := r.Extract(ctx, bytes.NewReader(buf.Bytes()), func(_ context.Context, f archives.FileInfo) error {
				rc, openErr := f.Open()
				if openErr != nil {
					return openErr
				}
				defer func() {
					if closeErr := rc.Close(); closeErr != nil {
						t.Errorf("can't close extractor: %+v", closeErr)
					}
				}()
				got, openErr = io.ReadAll(rc)
				return openErr
			})
			require.NoError(t, extractErr)
			assert.Equal(t, payload, got)
		})
	}
}
