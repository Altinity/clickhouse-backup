package storage

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FirstFileName generator: metadata files are stored as is, the second hits.bin is deduplicated
// into the first one via <data_file>, the use_base file is stored in the base backup (only the appended bytes here)
const testCloudBackupFirstFileNameXML = `<config><version>1</version><deduplicate_files>1</deduplicate_files>` +
	`<timestamp>2026-09-20 12:34:56</timestamp><uuid>8b6c9b4e-0000-0000-0000-000000000000</uuid>` +
	`<base_backup>S3('https://s3.us-east-1.amazonaws.com/bucket/base')</base_backup><base_backup_uuid>8b6c9b4e-0000-0000-0000-000000000001</base_backup_uuid>` +
	`<contents>` +
	`<file><name>metadata/default.sql</name><size>77</size><checksum>abcdef0123456789abcdef0123456789</checksum></file>` +
	`<file><name>metadata/default/hits.sql</name><size>256</size><checksum>0123456789abcdef0123456789abcdef</checksum></file>` +
	`<file><name>data/default/hits/all_1_1_0/hits.bin</name><size>1000</size><checksum>11111111111111111111111111111111</checksum></file>` +
	`<file><name>data/default/hits/all_2_2_0/hits.bin</name><size>1000</size><checksum>11111111111111111111111111111111</checksum><data_file>data/default/hits/all_1_1_0/hits.bin</data_file></file>` +
	`<file><name>data/default/hits/all_0_0_0/hits.bin</name><size>5000</size><checksum>22222222222222222222222222222222</checksum><use_base>true</use_base></file>` +
	`<file><name>data/default/hits/all_0_0_0/count.txt</name><size>300</size><checksum>33333333333333333333333333333333</checksum><use_base>true</use_base><base_size>100</base_size><base_checksum>44444444444444444444444444444444</base_checksum></file>` +
	`<file><name>metadata/default/empty.sql</name><size>0</size></file>` +
	`</contents></config>`

// Checksum generator (ClickHouse Cloud): every stored file has <data_file> = the checksum-named blob
const testCloudBackupChecksumXML = `<config><version>1</version><deduplicate_files>1</deduplicate_files>` +
	`<timestamp>2026-09-21 01:02:03</timestamp><uuid>8b6c9b4e-0000-0000-0000-000000000002</uuid>` +
	`<data_file_name_generator>checksum</data_file_name_generator>` +
	`<contents>` +
	`<file><name>metadata/default/hits.sql</name><size>256</size><checksum>0123456789abcdef0123456789abcdef</checksum><data_file>012/3456789abcdef0123456789abcdef</data_file></file>` +
	`<file><name>data/default/hits/all_0_0_0/data.packed</name><size>4096</size><checksum>fedcba9876543210fedcba9876543210</checksum><data_file>fed/cba9876543210fedcba9876543210</data_file></file>` +
	`</contents></config>`

func TestParseCloudBackupSummary(t *testing.T) {
	summary, err := parseCloudBackupSummary(strings.NewReader(testCloudBackupFirstFileNameXML))
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 9, 20, 12, 34, 56, 0, time.UTC), summary.Timestamp)
	// 77 + 256 + 1000 (deduplicated copy skipped) + 0 (whole file in base) + 200 (appended to base)
	assert.Equal(t, uint64(77+256+1000+200), summary.DataSize)

	summary, err = parseCloudBackupSummary(strings.NewReader(testCloudBackupChecksumXML))
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 9, 21, 1, 2, 3, 0, time.UTC), summary.Timestamp)
	assert.Equal(t, uint64(256+4096), summary.DataSize)

	// deduplicate_files=0 stores every file under its own name
	noDedup := strings.Replace(testCloudBackupFirstFileNameXML, "<deduplicate_files>1</deduplicate_files>", "<deduplicate_files>0</deduplicate_files>", 1)
	summary, err = parseCloudBackupSummary(strings.NewReader(noDedup))
	require.NoError(t, err)
	assert.Equal(t, uint64(77+256+1000+1000+200), summary.DataSize)

	_, err = parseCloudBackupSummary(strings.NewReader(`<config><contents><file><size>abc</size></file></contents></config>`))
	assert.Error(t, err)
	_, err = parseCloudBackupSummary(strings.NewReader(`<config><contents><file>`))
	assert.Error(t, err)
}

type memRemoteFile struct {
	name         string
	size         int64
	lastModified time.Time
}

func (f memRemoteFile) Size() int64             { return f.size }
func (f memRemoteFile) Name() string            { return f.name }
func (f memRemoteFile) LastModified() time.Time { return f.lastModified }

// memRemoteStorage - in-memory RemoteStorage, only the methods used by BackupList are implemented
type memRemoteStorage struct {
	RemoteStorage
	files        map[string]string
	lastModified time.Time
}

func (m *memRemoteStorage) Kind() string { return "mem-cloud-backup-test" }

func (m *memRemoteStorage) StatFile(_ context.Context, key string) (RemoteFile, error) {
	content, exists := m.files[key]
	if !exists {
		return nil, ErrNotFound
	}
	return memRemoteFile{name: key, size: int64(len(content)), lastModified: m.lastModified}, nil
}

func (m *memRemoteStorage) GetFileReader(_ context.Context, key string) (io.ReadCloser, error) {
	content, exists := m.files[key]
	if !exists {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewBufferString(content)), nil
}

// Walk - non-recursive listing of the top-level "directories" under prefix
func (m *memRemoteStorage) Walk(ctx context.Context, prefix string, _ bool, fn func(context.Context, RemoteFile) error) error {
	prefix = strings.TrimPrefix(prefix, "/")
	dirs := map[string]struct{}{}
	for key := range m.files {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		dirs[strings.SplitN(strings.TrimPrefix(key, prefix), "/", 2)[0]+"/"] = struct{}{}
	}
	names := make([]string, 0, len(dirs))
	for name := range dirs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := fn(ctx, memRemoteFile{name: name, lastModified: m.lastModified}); err != nil {
			return err
		}
	}
	return nil
}

func TestBackupListDetectsCloudBackup(t *testing.T) {
	// the metadata cache lives in os.TempDir
	t.Setenv("TMPDIR", t.TempDir())
	lastModified := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	bd := &BackupDestination{RemoteStorage: &memRemoteStorage{
		lastModified: lastModified,
		files: map[string]string{
			// regular clickhouse-backup backup
			"regular/metadata.json": `{"backup_name":"regular","data_format":"tar","data_size":10}`,
			// embedded clickhouse-backup backup carries its own .backup, metadata.json wins
			"embedded/metadata.json": `{"backup_name":"embedded","data_format":"embedded","data_size":20}`,
			"embedded/.backup":       testCloudBackupChecksumXML,
			// native BACKUP ... TO S3 (ClickHouse Cloud)
			"cloud/.backup": testCloudBackupChecksumXML,
			"cloud/012/3456789abcdef0123456789abcdef": "CREATE TABLE ...",
			// neither metadata.json nor .backup
			"broken/shadow/default/t/part.tar": "x",
		},
	}}
	ctx := context.Background()

	backups, err := bd.BackupList(ctx, true, "")
	require.NoError(t, err)
	byName := map[string]Backup{}
	for _, b := range backups {
		byName[b.BackupName] = b
	}
	require.Len(t, byName, 4)
	assert.Equal(t, "tar", byName["regular"].DataFormat)
	assert.Equal(t, "embedded", byName["embedded"].DataFormat)
	assert.Empty(t, byName["embedded"].Broken)

	cloud := byName["cloud"]
	assert.Empty(t, cloud.Broken)
	assert.Equal(t, CloudBackupDataFormat, cloud.DataFormat)
	assert.Equal(t, uint64(256+4096), cloud.DataSize)
	assert.Equal(t, uint64(len(testCloudBackupChecksumXML)), cloud.MetadataSize)
	assert.Equal(t, time.Date(2026, 9, 21, 1, 2, 3, 0, time.UTC), cloud.CreationDate)
	assert.Equal(t, lastModified, cloud.UploadDate)

	assert.Equal(t, "broken (can't stat metadata.json)", byName["broken"].Broken)

	// fast path used by restore_remote / download, cache is removed to exercise the direct read
	require.NoError(t, bd.writeMetadataCacheFile(ctx, map[string]Backup{}))
	backups, err = bd.BackupList(ctx, true, "cloud")
	require.NoError(t, err)
	require.Len(t, backups, 1)
	assert.Equal(t, CloudBackupDataFormat, backups[0].DataFormat)
	assert.Equal(t, uint64(256+4096), backups[0].DataSize)

	// metadata.json uploaded after a list cached the backup as cloud (embedded create_remote in progress)
	bd.RemoteStorage.(*memRemoteStorage).files["cloud/metadata.json"] = `{"backup_name":"cloud","data_format":"embedded","data_size":30}`
	backups, err = bd.BackupList(ctx, true, "cloud")
	require.NoError(t, err)
	require.Len(t, backups, 1)
	assert.Equal(t, "embedded", backups[0].DataFormat)
	backups, err = bd.BackupList(ctx, true, "")
	require.NoError(t, err)
	for _, b := range backups {
		assert.NotEqual(t, CloudBackupDataFormat, b.DataFormat, b.BackupName)
	}

	backups, err = bd.BackupList(ctx, true, "broken")
	require.NoError(t, err)
	require.Len(t, backups, 1)
	assert.Equal(t, "broken (can't stat metadata.json)", backups[0].Broken)
}

func TestParseCloudBackupLocation(t *testing.T) {
	testCases := []struct {
		name, baseBackup, bucket, expectedKey, expectedString string
		expectedFound                                         bool
		expectedSecrets                                       []string
	}{
		{"s3 path style", `S3('https://s3.us-east-1.amazonaws.com/bucket/path/base', 'AKIAKEY', 'se\'cret')`, "bucket", "path/base", "https://s3.us-east-1.amazonaws.com/bucket/path/base", true, []string{"AKIAKEY", "se'cret"}},
		{"s3 virtual hosted", `S3('https://bucket.s3.us-east-1.amazonaws.com/path/base/', 'KEYX', 'SECRETX')`, "bucket", "path/base", "https://bucket.s3.us-east-1.amazonaws.com/path/base/", true, []string{"KEYX", "SECRETX"}},
		{"gcs", `S3('https://storage.googleapis.com/bucket/base','KEYX','SECRETX')`, "bucket", "base", "https://storage.googleapis.com/bucket/base", true, []string{"KEYX", "SECRETX"}},
		{"s3 scheme", `S3('s3://bucket/path/base')`, "bucket", "path/base", "s3://bucket/path/base", true, nil},
		{"iam role", `S3('https://s3.us-west-2.amazonaws.com/bucket/base', extra_credentials(role_arn = 'arn:aws:iam::1:role/r'))`, "bucket", "base", "https://s3.us-west-2.amazonaws.com/bucket/base", true, nil},
		{"key value url", `S3(url = 'http://minio:9000/bucket/base', access_key_id = 'KEYX', secret_access_key = 'SECRETX')`, "bucket", "base", "http://minio:9000/bucket/base", true, []string{"KEYX", "SECRETX"}},
		{"another bucket", `S3('https://s3.us-east-1.amazonaws.com/other/base','KEYX','SECRETX')`, "bucket", "", "https://s3.us-east-1.amazonaws.com/other/base", false, []string{"KEYX", "SECRETX"}},
		{"bucket name prefix only", `S3('https://s3.us-east-1.amazonaws.com/bucket2/base')`, "bucket", "", "https://s3.us-east-1.amazonaws.com/bucket2/base", false, nil},
		{"azblob", `AzureBlobStorage('DefaultEndpointsProtocol=https;AccountName=acc;AccountKey=azkey==;BlobEndpoint=https://acc.blob.core.windows.net;', 'container', 'path/base/')`, "container", "path/base", "azblob://container/path/base", true, []string{"azkey=="}},
		{"azblob account args", `AzureBlobStorage('https://acc.blob.core.windows.net', 'container', 'base', 'acc', 'azkey')`, "other", "", "azblob://container/base", false, []string{"acc", "azkey"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			loc, err := ParseCloudBackupLocation(tc.baseBackup)
			require.NoError(t, err)
			key, found := loc.KeyIn(tc.bucket)
			assert.Equal(t, tc.expectedFound, found)
			assert.Equal(t, tc.expectedKey, key)
			assert.Equal(t, tc.expectedString, loc.String())
			assert.ElementsMatch(t, tc.expectedSecrets, loc.Secrets)
			for _, secret := range loc.Secrets {
				assert.NotContains(t, loc.String(), secret)
			}
		})
	}
	_, err := ParseCloudBackupLocation("S3")
	assert.Error(t, err)
	_, err = ParseCloudBackupLocation("S3('unterminated)")
	assert.Error(t, err)
}

func TestCloudBaseBackupName(t *testing.T) {
	base := func(u string) string { return "S3('" + u + "', 'KEY', 'SECRET')" }
	name, tags := cloudBaseBackupName(base("https://s3.us-east-1.amazonaws.com/bucket/backups/full"), "bucket", "/backups/")
	assert.Equal(t, "full", name)
	assert.Empty(t, tags)
	name, _ = cloudBaseBackupName(base("https://bucket.s3.us-east-1.amazonaws.com/full"), "bucket", "")
	assert.Equal(t, "full", name)
	// nested under another path of the same bucket
	name, tags = cloudBaseBackupName(base("https://s3.us-east-1.amazonaws.com/bucket/other/full"), "bucket", "backups")
	assert.Empty(t, name)
	assert.Equal(t, "base=https://s3.us-east-1.amazonaws.com/bucket/other/full", tags)
	name, tags = cloudBaseBackupName(base("https://s3.us-east-1.amazonaws.com/bucket/backups/deep/full"), "bucket", "backups")
	assert.Empty(t, name)
	assert.NotContains(t, tags, "SECRET")
	name, tags = cloudBaseBackupName(base("https://s3.us-east-1.amazonaws.com/other/full"), "bucket", "")
	assert.Empty(t, name)
	assert.Equal(t, "base=https://s3.us-east-1.amazonaws.com/other/full", tags)
	name, tags = cloudBaseBackupName("garbage", "bucket", "")
	assert.Empty(t, name)
	assert.Equal(t, "base=unknown", tags)
}

func TestBackupListCloudBaseBackup(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	// the XML escaping of <base_backup> is decoded, memRemoteStorage has no bucket, so the base is described in tags
	bd := &BackupDestination{RemoteStorage: &memRemoteStorage{files: map[string]string{
		"incremental/.backup": `<config><version>1</version><timestamp>2026-09-23 00:00:00</timestamp><uuid>u2</uuid>` +
			`<base_backup>S3(&apos;https://s3.us-east-1.amazonaws.com/bucket/full&apos;, &apos;KEY&apos;, &apos;SECRET&apos;)</base_backup><base_backup_uuid>u1</base_backup_uuid>` +
			`<contents><file><name>metadata/default/t.sql</name><size>10</size><use_base>true</use_base></file></contents></config>`,
	}}}
	backups, err := bd.BackupList(context.Background(), true, "")
	require.NoError(t, err)
	require.Len(t, backups, 1)
	assert.Equal(t, CloudBackupDataFormat, backups[0].DataFormat)
	assert.Equal(t, "base=https://s3.us-east-1.amazonaws.com/bucket/full", backups[0].Tags)
}

func TestReadCloudBackupHeader(t *testing.T) {
	// the header reader stops at <contents>, a truncated file list is not an error
	header, err := ReadCloudBackupHeader(strings.NewReader(`<config><uuid>u2</uuid><base_backup>S3('u')</base_backup><base_backup_uuid>u1</base_backup_uuid><contents><file><name>x`))
	require.NoError(t, err)
	assert.Equal(t, "u2", header.UUID)
	assert.Equal(t, "S3('u')", header.BaseBackup)
	assert.Equal(t, "u1", header.BaseBackupUUID)
}
