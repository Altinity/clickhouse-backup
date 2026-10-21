package backup

import (
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"

	"github.com/stretchr/testify/require"
)

var resumableCommands = []string{"create", "upload", "download", "restore", "create_upload_streaming", "download_restore_streaming"}

// requireResumableConfigHash checks for every resumable command whether change(cfg) changes the state config hash,
// mustChange lists the commands which must reset the state, all others must keep it
func requireResumableConfigHash(t *testing.T, newCfg func() *config.Config, name string, change func(cfg *config.Config), mustChange ...string) {
	t.Helper()
	changed := newCfg()
	change(changed)
	for _, command := range resumableCommands {
		before, after := resumableConfigHash(newCfg(), command), resumableConfigHash(changed, command)
		shouldChange := false
		for _, c := range mustChange {
			shouldChange = shouldChange || c == command
		}
		if shouldChange {
			require.NotEqual(t, before, after, "%s must reset %s state", name, command)
		} else {
			require.Equal(t, before, after, "%s must keep %s state", name, command)
		}
	}
}

// https://github.com/Altinity/clickhouse-backup/issues/1010
func TestResumableConfigHashS3(t *testing.T) {
	newCfg := func() *config.Config {
		cfg := config.DefaultConfig()
		cfg.General.RemoteStorage = "s3"
		cfg.S3.Bucket = "bucket"
		cfg.S3.Path = "backup/shard1"
		cfg.S3.ObjectDiskPath = "object_disks/shard1"
		return cfg
	}
	for _, command := range resumableCommands {
		require.Equal(t, resumableConfigHash(newCfg(), command), resumableConfigHash(newCfg(), command), command)
	}

	// changes which don't move the state keys to another target must keep resume working
	requireResumableConfigHash(t, newCfg, "tuning", func(cfg *config.Config) {
		cfg.General.UploadConcurrency += 7
		cfg.General.DownloadConcurrency += 7
		cfg.General.RetriesOnFailure += 7
		cfg.General.LogLevel = "debug"
		cfg.S3.Concurrency += 7
		cfg.S3.AccessKey = "rotated"
		cfg.S3.SecretKey = "rotated"
		cfg.S3.SSE = "aws:kms"
		cfg.S3.SSEKMSKeyId = "rotated"
		cfg.GCS.Bucket = "inactive-storage-section"
		cfg.GCS.EncryptionKey = "inactive-storage-section"
	})

	all := resumableCommands
	notCreate := []string{"upload", "download", "restore", "create_upload_streaming", "download_restore_streaming"}
	requireResumableConfigHash(t, newCfg, "s3.path", func(cfg *config.Config) { cfg.S3.Path = "backup/shard2" }, notCreate...)
	requireResumableConfigHash(t, newCfg, "s3.bucket", func(cfg *config.Config) { cfg.S3.Bucket = "other" }, all...)
	requireResumableConfigHash(t, newCfg, "s3.endpoint", func(cfg *config.Config) { cfg.S3.Endpoint = "https://other:9000" }, all...)
	requireResumableConfigHash(t, newCfg, "s3.object_disk_path", func(cfg *config.Config) { cfg.S3.ObjectDiskPath = "object_disks/shard2" }, all...)
	requireResumableConfigHash(t, newCfg, "s3.sse_customer_key set", func(cfg *config.Config) { cfg.S3.SSECustomerKey = "key1" }, all...)
	requireResumableConfigHash(t, newCfg, "s3.compression_format", func(cfg *config.Config) { cfg.S3.CompressionFormat = "zstd" }, "upload", "create_upload_streaming")
	requireResumableConfigHash(t, newCfg, "remote_storage", func(cfg *config.Config) {
		cfg.General.RemoteStorage = "gcs"
		cfg.GCS = config.GCSConfig{Bucket: cfg.S3.Bucket, Path: cfg.S3.Path, ObjectDiskPath: cfg.S3.ObjectDiskPath, CompressionFormat: cfg.S3.CompressionFormat}
	}, all...)
	requireResumableConfigHash(t, newCfg, "restore_database_mapping", func(cfg *config.Config) {
		cfg.General.RestoreDatabaseMapping = map[string]string{"db": "db_copy"}
	}, "restore", "download_restore_streaming")
	requireResumableConfigHash(t, newCfg, "restore_table_mapping", func(cfg *config.Config) {
		cfg.General.RestoreTableMapping = map[string]string{"t": "t_copy"}
	}, "restore", "download_restore_streaming")

	// the customer key is detected in both directions: rotated and removed
	withKey := func() *config.Config {
		cfg := newCfg()
		cfg.S3.SSECustomerKey = "key1"
		return cfg
	}
	requireResumableConfigHash(t, withKey, "s3.sse_customer_key rotated", func(cfg *config.Config) { cfg.S3.SSECustomerKey = "key2" }, all...)
	requireResumableConfigHash(t, withKey, "s3.sse_customer_key removed", func(cfg *config.Config) { cfg.S3.SSECustomerKey = "" }, all...)
}

// https://github.com/Altinity/clickhouse-backup/issues/1010
func TestResumableConfigHashGCS(t *testing.T) {
	newCfg := func() *config.Config {
		cfg := config.DefaultConfig()
		cfg.General.RemoteStorage = "gcs"
		cfg.GCS.Bucket = "bucket"
		cfg.GCS.Path = "backup/shard1"
		cfg.GCS.ObjectDiskPath = "object_disks/shard1"
		cfg.GCS.EncryptionKey = "key1"
		return cfg
	}
	all := resumableCommands
	notCreate := []string{"upload", "download", "restore", "create_upload_streaming", "download_restore_streaming"}
	requireResumableConfigHash(t, newCfg, "tuning", func(cfg *config.Config) {
		cfg.GCS.ClientPoolSize += 7
		cfg.GCS.CredentialsJSON = "rotated"
		cfg.S3.Bucket = "inactive-storage-section"
	})
	requireResumableConfigHash(t, newCfg, "gcs.path", func(cfg *config.Config) { cfg.GCS.Path = "backup/shard2" }, notCreate...)
	requireResumableConfigHash(t, newCfg, "gcs.bucket", func(cfg *config.Config) { cfg.GCS.Bucket = "other" }, all...)
	requireResumableConfigHash(t, newCfg, "gcs.endpoint", func(cfg *config.Config) { cfg.GCS.Endpoint = "https://other" }, all...)
	requireResumableConfigHash(t, newCfg, "gcs.object_disk_path", func(cfg *config.Config) { cfg.GCS.ObjectDiskPath = "object_disks/shard2" }, all...)
	requireResumableConfigHash(t, newCfg, "gcs.encryption_key rotated", func(cfg *config.Config) { cfg.GCS.EncryptionKey = "key2" }, all...)
	requireResumableConfigHash(t, newCfg, "gcs.encryption_key removed", func(cfg *config.Config) { cfg.GCS.EncryptionKey = "" }, all...)
	requireResumableConfigHash(t, newCfg, "gcs.compression_format", func(cfg *config.Config) { cfg.GCS.CompressionFormat = "zstd" }, "upload", "create_upload_streaming")
}

// https://github.com/Altinity/clickhouse-backup/issues/1010
func TestResumableConfigHashAzureBlob(t *testing.T) {
	newCfg := func() *config.Config {
		cfg := config.DefaultConfig()
		cfg.General.RemoteStorage = "azblob"
		cfg.AzureBlob.AccountName = "account"
		cfg.AzureBlob.Container = "container"
		cfg.AzureBlob.Path = "backup/shard1"
		cfg.AzureBlob.ObjectDiskPath = "object_disks/shard1"
		cfg.AzureBlob.SSEKey = "key1"
		return cfg
	}
	all := resumableCommands
	notCreate := []string{"upload", "download", "restore", "create_upload_streaming", "download_restore_streaming"}
	requireResumableConfigHash(t, newCfg, "tuning", func(cfg *config.Config) {
		cfg.AzureBlob.AccountKey = "rotated"
		cfg.AzureBlob.EndpointSchema = "http"
		cfg.S3.Bucket = "inactive-storage-section"
	})
	requireResumableConfigHash(t, newCfg, "azblob.path", func(cfg *config.Config) { cfg.AzureBlob.Path = "backup/shard2" }, notCreate...)
	requireResumableConfigHash(t, newCfg, "azblob.account_name", func(cfg *config.Config) { cfg.AzureBlob.AccountName = "other" }, all...)
	requireResumableConfigHash(t, newCfg, "azblob.container", func(cfg *config.Config) { cfg.AzureBlob.Container = "other" }, all...)
	requireResumableConfigHash(t, newCfg, "azblob.endpoint_suffix", func(cfg *config.Config) { cfg.AzureBlob.EndpointSuffix = "other.net" }, all...)
	requireResumableConfigHash(t, newCfg, "azblob.object_disk_path", func(cfg *config.Config) { cfg.AzureBlob.ObjectDiskPath = "object_disks/shard2" }, all...)
	requireResumableConfigHash(t, newCfg, "azblob.sse_key rotated", func(cfg *config.Config) { cfg.AzureBlob.SSEKey = "key2" }, all...)
	requireResumableConfigHash(t, newCfg, "azblob.sse_key removed", func(cfg *config.Config) { cfg.AzureBlob.SSEKey = "" }, all...)
	requireResumableConfigHash(t, newCfg, "azblob.compression_format", func(cfg *config.Config) { cfg.AzureBlob.CompressionFormat = "zstd" }, "upload", "create_upload_streaming")
}

// https://github.com/Altinity/clickhouse-backup/issues/1010
func TestRestoreMappingParam(t *testing.T) {
	require.Equal(t, []string{"a:a2", "b:b2"}, restoreMappingParam(map[string]string{"b": "b2", "a": "a2"}))
	require.Equal(t, []string{}, restoreMappingParam(nil))
}
