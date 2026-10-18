package backup

import (
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"

	"github.com/stretchr/testify/require"
)

// https://github.com/Altinity/clickhouse-backup/issues/1010
func TestResumableConfigHash(t *testing.T) {
	newCfg := func() *config.Config {
		cfg := config.DefaultConfig()
		cfg.General.RemoteStorage = "s3"
		cfg.S3.Bucket = "bucket"
		cfg.S3.Path = "backup/shard1"
		cfg.S3.ObjectDiskPath = "object_disks/shard1"
		return cfg
	}
	base := newCfg()
	for _, command := range []string{"create", "upload", "download", "restore", "create_upload_streaming", "download_restore_streaming"} {
		require.Equal(t, resumableConfigHash(base, command), resumableConfigHash(newCfg(), command), command)
	}

	// changes which don't move the state keys to another location must keep resume working
	tuned := newCfg()
	tuned.General.UploadConcurrency = base.General.UploadConcurrency + 7
	tuned.General.DownloadConcurrency = base.General.DownloadConcurrency + 7
	tuned.General.RetriesOnFailure = base.General.RetriesOnFailure + 7
	tuned.General.LogLevel = "debug"
	tuned.General.MaxFileSize = base.General.MaxFileSize + 1
	tuned.S3.Concurrency = base.S3.Concurrency + 7
	tuned.S3.SecretKey = "rotated"
	tuned.GCS.Bucket = "inactive-storage-section"
	for _, command := range []string{"upload", "download", "restore"} {
		require.Equal(t, resumableConfigHash(base, command), resumableConfigHash(tuned, command), command)
	}

	changes := map[string]func(cfg *config.Config){
		"s3.path":             func(cfg *config.Config) { cfg.S3.Path = "backup/shard2" },
		"s3.bucket":           func(cfg *config.Config) { cfg.S3.Bucket = "other" },
		"s3.endpoint":         func(cfg *config.Config) { cfg.S3.Endpoint = "https://other:9000" },
		"s3.object_disk_path": func(cfg *config.Config) { cfg.S3.ObjectDiskPath = "object_disks/shard2" },
		"compression_format":  func(cfg *config.Config) { cfg.S3.CompressionFormat = "zstd" },
		"remote_storage": func(cfg *config.Config) {
			cfg.General.RemoteStorage = "gcs"
			cfg.GCS = config.GCSConfig{Bucket: cfg.S3.Bucket, Path: cfg.S3.Path, ObjectDiskPath: cfg.S3.ObjectDiskPath, CompressionFormat: cfg.S3.CompressionFormat}
		},
	}
	for name, change := range changes {
		changed := newCfg()
		change(changed)
		for _, command := range []string{"create", "upload", "download", "restore"} {
			require.NotEqual(t, resumableConfigHash(base, command), resumableConfigHash(changed, command), "%s %s", name, command)
		}
	}

	// mappings change the restore targets only
	mapped := newCfg()
	mapped.General.RestoreDatabaseMapping = map[string]string{"db": "db_copy"}
	require.Equal(t, resumableConfigHash(base, "upload"), resumableConfigHash(mapped, "upload"))
	require.Equal(t, resumableConfigHash(base, "download"), resumableConfigHash(mapped, "download"))
	require.NotEqual(t, resumableConfigHash(base, "restore"), resumableConfigHash(mapped, "restore"))
	require.NotEqual(t, resumableConfigHash(base, "download_restore_streaming"), resumableConfigHash(mapped, "download_restore_streaming"))
	tableMapped := newCfg()
	tableMapped.General.RestoreTableMapping = map[string]string{"t": "t_copy"}
	require.NotEqual(t, resumableConfigHash(base, "restore"), resumableConfigHash(tableMapped, "restore"))
}
