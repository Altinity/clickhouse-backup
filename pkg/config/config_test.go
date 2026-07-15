package config

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"
)

func TestMaskSensitiveEnvValue(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  string
	}{
		{"S3_SECRET_KEY", "secret", maskedEnvValue},
		{"CLICKHOUSE_PASSWORD", "password", maskedEnvValue},
		{"AZBLOB_SAS_TOKEN", "sas-token", maskedEnvValue},
		{"AZBLOB_ACCOUNT_KEY", "account-key", maskedEnvValue},
		{"AZBLOB_SSE_KEY", "sse-key", maskedEnvValue},
		{"S3_SSE_CUSTOMER_KEY", "customer-key", maskedEnvValue},
		{"GCS_ENCRYPTION_KEY", "encryption-key", maskedEnvValue},
		{"GCS_CREDENTIALS_JSON", "{}", maskedEnvValue},
		{"s3_secret_key", "secret", maskedEnvValue},
		{"S3_BUCKET", "backup-bucket", "backup-bucket"},
		{"REMOTE_STORAGE", "s3", "s3"},
		{"LOG_LEVEL", "debug", "debug"},
		{"UPLOAD_CONCURRENCY", "4", "4"},
		{"FOO", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := maskSensitiveEnvValue(tc.name, tc.value); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

func TestMaskEnvOverrideCommand(t *testing.T) {
	cases := []struct {
		name    string
		command string
		want    string
	}{
		{"no env flag", "create my_backup", "create my_backup"},
		{
			"sensitive --env=",
			"create --env=S3_SECRET_KEY=super-secret my_backup",
			"create --env=S3_SECRET_KEY=[MASKED] my_backup",
		},
		{
			"sensitive --env space",
			"create --env S3_SECRET_KEY=super-secret my_backup",
			"create --env S3_SECRET_KEY=[MASKED] my_backup",
		},
		{
			"sensitive --environment-override=",
			"upload --environment-override=CLICKHOUSE_PASSWORD=p@ss backup",
			"upload --environment-override=CLICKHOUSE_PASSWORD=[MASKED] backup",
		},
		{
			"non-sensitive left intact",
			"create --env=REMOTE_STORAGE=s3 my_backup",
			"create --env=REMOTE_STORAGE=s3 my_backup",
		},
		{
			"mixed sensitive and non-sensitive",
			"create --env=REMOTE_STORAGE=s3 --env=S3_SECRET_KEY=abc123 my_backup",
			"create --env=REMOTE_STORAGE=s3 --env=S3_SECRET_KEY=[MASKED] my_backup",
		},
		{
			"quoted sensitive value",
			`create --env="AZBLOB_ACCOUNT_KEY=a b c" my_backup`,
			`create --env="AZBLOB_ACCOUNT_KEY=[MASKED]" my_backup`,
		},
		{
			`double-quoted --env="VAR=value"`,
			`create --env="S3_SECRET_KEY=super-secret" my_backup`,
			`create --env="S3_SECRET_KEY=[MASKED]" my_backup`,
		},
		{
			`single-quoted --env='VAR=value'`,
			`create --env='S3_SECRET_KEY=super-secret' my_backup`,
			`create --env='S3_SECRET_KEY=[MASKED]' my_backup`,
		},
		{
			`quoted non-sensitive left intact`,
			`create --env="REMOTE_STORAGE=s3" my_backup`,
			`create --env="REMOTE_STORAGE=s3" my_backup`,
		},
		{
			`double-quoted space form --env "VAR=value"`,
			`create --env "S3_SECRET_KEY=super-secret" my_backup`,
			`create --env "S3_SECRET_KEY=[MASKED]" my_backup`,
		},
		{
			"empty value not masked",
			"create --env=S3_SECRET_KEY= my_backup",
			"create --env=S3_SECRET_KEY= my_backup",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MaskEnvOverrideCommand(tc.command)
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
			if strings.Contains(got, "super-secret") || strings.Contains(got, "p@ss") || strings.Contains(got, "abc123") || strings.Contains(got, "a b c") {
				t.Fatalf("sensitive value leaked in masked command: %q", got)
			}
		})
	}
}

func TestOverrideEnvVarsMasksSensitiveValuesOnlyInLogs(t *testing.T) {
	const (
		sensitiveName     = "CLICKHOUSE_BACKUP_TEST_SECRET_KEY"
		sensitiveValue    = "plain-secret"
		nonSensitiveName  = "CLICKHOUSE_BACKUP_TEST_REMOTE_STORAGE"
		nonSensitiveValue = "s3"
		nameOnlyFlag      = "CLICKHOUSE_BACKUP_TEST_FLAG"
	)

	t.Setenv(sensitiveName, "original-secret")
	t.Setenv(nonSensitiveName, "original-storage")
	t.Setenv(nameOnlyFlag, "false")
	// Pin LOG_LEVEL so OverrideEnvVars, which resets the global zerolog level
	// from this variable, does not drop below info and suppress the override
	// logs this test asserts on when the outer process sets LOG_LEVEL=warn/error.
	t.Setenv("LOG_LEVEL", "info")

	var buf bytes.Buffer
	originalLogger := log.Logger
	originalGlobalLevel := zerolog.GlobalLevel()
	t.Cleanup(func() {
		log.Logger = originalLogger
		zerolog.SetGlobalLevel(originalGlobalLevel)
	})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = zerolog.New(&buf)

	oldValues := OverrideEnvVars(newEnvContext(t,
		sensitiveName+"="+sensitiveValue,
		nonSensitiveName+"="+nonSensitiveValue,
		nameOnlyFlag,
	))
	defer RestoreEnvVars(oldValues)

	if got := os.Getenv(sensitiveName); got != sensitiveValue {
		t.Fatalf("expected sensitive env override value %q, got %q", sensitiveValue, got)
	}
	if got := os.Getenv(nonSensitiveName); got != nonSensitiveValue {
		t.Fatalf("expected non-sensitive env override value %q, got %q", nonSensitiveValue, got)
	}
	if got := os.Getenv(nameOnlyFlag); got != "true" {
		t.Fatalf("expected name-only env override value %q, got %q", "true", got)
	}

	output := buf.String()
	if strings.Contains(output, sensitiveValue) {
		t.Fatalf("sensitive value leaked in log output: %s", output)
	}
	if !strings.Contains(output, "override "+sensitiveName+"="+maskedEnvValue) {
		t.Fatalf("expected masked sensitive override in log output, got: %s", output)
	}
	if !strings.Contains(output, "override "+nonSensitiveName+"="+nonSensitiveValue) {
		t.Fatalf("expected non-sensitive override value in log output, got: %s", output)
	}
	if !strings.Contains(output, "override "+nameOnlyFlag+"=true") {
		t.Fatalf("expected name-only override value in log output, got: %s", output)
	}
}

func TestDisableEnvironmentOverride(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yml")
	writeConfig := func(disable bool) {
		content := fmt.Sprintf("general:\n  remote_storage: \"s3\"\n  disable_environment_override: %v\ns3:\n  bucket: \"from-file\"\n", disable)
		if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
			t.Fatalf("can't write config file: %v", err)
		}
	}
	newCliContext := func(envValues ...string) *cli.Context {
		env := cli.StringSlice{}
		flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
		flagSet.Var(&env, "env", "")
		flagSet.String("config", configPath, "")
		for _, value := range envValues {
			if err := flagSet.Set("env", value); err != nil {
				t.Fatalf("failed to set env flag %q: %v", value, err)
			}
		}
		return cli.NewContext(cli.NewApp(), flagSet, nil)
	}

	// envconfig overrides config file values by default
	t.Setenv("S3_BUCKET", "from-env")
	writeConfig(false)
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.S3.Bucket != "from-env" {
		t.Fatalf("expected S3_BUCKET env override %q, got %q", "from-env", cfg.S3.Bucket)
	}

	// disable_environment_override: true skips envconfig
	writeConfig(true)
	cfg, err = LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.S3.Bucket != "from-file" {
		t.Fatalf("expected config file value %q, got %q", "from-file", cfg.S3.Bucket)
	}

	// the option itself can't be enabled from the environment
	if err = os.Unsetenv("S3_BUCKET"); err != nil {
		t.Fatalf("can't unset S3_BUCKET: %v", err)
	}
	t.Setenv("DISABLE_ENVIRONMENT_OVERRIDE", "true")
	t.Setenv("DISABLEENVIRONMENTOVERRIDE", "true")
	writeConfig(false)
	cfg, err = LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.General.DisableEnvironmentOverride {
		t.Fatal("disable_environment_override must be settable only from the config file, not from the environment")
	}

	// --env overrides config file values by default
	writeConfig(false)
	cfg = GetConfigFromCli(newCliContext("S3_BUCKET=from-cli"))
	if cfg.S3.Bucket != "from-cli" {
		t.Fatalf("expected --env override %q, got %q", "from-cli", cfg.S3.Bucket)
	}

	// disable_environment_override: true ignores --env and doesn't touch the process environment
	writeConfig(true)
	cfg = GetConfigFromCli(newCliContext("S3_BUCKET=from-cli"))
	if cfg.S3.Bucket != "from-file" {
		t.Fatalf("expected config file value %q, got %q", "from-file", cfg.S3.Bucket)
	}
	if _, present := os.LookupEnv("S3_BUCKET"); present {
		t.Fatal("--env leaked into the process environment while disable_environment_override is enabled")
	}
}

func newEnvContext(t *testing.T, values ...string) *cli.Context {
	t.Helper()

	env := cli.StringSlice{}
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.Var(&env, "env", "")
	for _, value := range values {
		if err := flagSet.Set("env", value); err != nil {
			t.Fatalf("failed to set env flag %q: %v", value, err)
		}
	}
	return cli.NewContext(cli.NewApp(), flagSet, nil)
}

func TestValidateConfigCompressionTuning(t *testing.T) {
	// validCompressionConfig returns a DefaultConfig wired to s3 with the given compression_format,
	// so ValidateConfig only fails on the compression tuning options under test.
	validCompressionConfig := func(format string) *Config {
		cfg := DefaultConfig()
		cfg.General.RemoteStorage = "s3"
		cfg.S3.CompressionFormat = format
		return cfg
	}
	cases := []struct {
		name           string
		format         string
		useMultiThread bool
		threads        int
		bufferSize     int
		wantErr        bool
	}{
		{"zstd defaults", "zstd", false, 0, 0, false},
		{"zstd multi-thread 4 threads 4MB window", "zstd", true, 4, 4 << 20, false},
		{"zstd window not power of two", "zstd", true, 0, 4<<20 - 1, true},
		{"zstd window too small", "zstd", false, 0, 512, true},
		{"zstd window too large", "zstd", false, 0, 1 << 30, true},
		{"gzip multi-thread 1MB block", "gzip", true, 0, 1 << 20, false},
		{"gzip multi-thread block too small", "gzip", true, 0, 16384, true},
		{"gzip single-thread 32KB window", "gzip", false, 0, 32768, false},
		{"gzip single-thread window too large", "gzip", false, 0, 65536, true},
		// unsupported formats relax instead of failing: the default compression_use_multi_thread=true
		// must not break them, and the knobs are no-ops there, see https://github.com/Altinity/clickhouse-backup/issues/1378
		{"multi_thread on unsupported format", "brotli", true, 0, 0, false},
		{"buffer_size on unsupported format", "brotli", false, 0, 1024, false},
		{"negative threads", "zstd", true, -1, 0, true},
		{"threads set without multi_thread", "zstd", false, 4, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validCompressionConfig(tc.format)
			cfg.General.CompressionUseMultiThread = tc.useMultiThread
			cfg.General.CompressionThreads = tc.threads
			cfg.General.CompressionBufferSize = tc.bufferSize
			err := ValidateConfig(cfg)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for %s: %v", tc.name, err)
			}
			// on formats that don't support multi-thread the knob must be silently disabled, not honored
			multiThreadSupported := tc.format == "zstd" || tc.format == "gzip" || tc.format == "gz"
			if !multiThreadSupported && cfg.General.CompressionUseMultiThread {
				t.Fatalf("expected compression_use_multi_thread to be disabled for unsupported format %s", tc.format)
			}
		})
	}
}

func TestLoadConfigErrors(t *testing.T) {
	writeConfig := func(t *testing.T, content string) string {
		configPath := filepath.Join(t.TempDir(), "config.yml")
		if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
			t.Fatalf("can't write config file: %v", err)
		}
		return configPath
	}
	requireErrContains := func(t *testing.T, err error, want string) {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", want)
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error containing %q, got: %v", want, err)
		}
	}

	t.Run("missing file is tolerated", func(t *testing.T) {
		cfg, err := LoadConfig(filepath.Join(t.TempDir(), "nonexistent.yml"))
		if err != nil {
			t.Fatalf("missing config file must fall back to defaults, got: %v", err)
		}
		if cfg.General.RemoteStorage != "none" {
			t.Fatalf("expected default remote_storage none, got %q", cfg.General.RemoteStorage)
		}
	})
	t.Run("unreadable file", func(t *testing.T) {
		// a directory exists but can't be read as a file
		_, err := LoadConfig(t.TempDir())
		requireErrContains(t, err, "can't open config file")
	})
	t.Run("invalid yaml", func(t *testing.T) {
		_, err := LoadConfig(writeConfig(t, "general: [unclosed\n\tbroken"))
		requireErrContains(t, err, "can't parse config file")
	})
	t.Run("envconfig type mismatch", func(t *testing.T) {
		t.Setenv("UPLOAD_CONCURRENCY", "not-a-number")
		_, err := LoadConfig(writeConfig(t, "general:\n  remote_storage: \"none\"\n"))
		requireErrContains(t, err, "envconfig.Process")
	})
	t.Run("validation failure propagated", func(t *testing.T) {
		_, err := LoadConfig(writeConfig(t, "general:\n  remote_storage: \"banana\"\n"))
		requireErrContains(t, err, "LoadConfig ValidateConfig")
		requireErrContains(t, err, "unknown remote storage")
	})
}

func TestValidateConfigErrors(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(cfg *Config)
		wantErr string
	}{
		{"valid default config", func(cfg *Config) {}, ""},
		{"bad s3 retry_mode", func(cfg *Config) {
			cfg.General.RemoteStorage = "s3"
			cfg.S3.RetryMode = "aggressive"
		}, "ValidateConfig ParseRetryMode"},
		{"bad s3 http_idle_conn_timeout", func(cfg *Config) {
			cfg.General.RemoteStorage = "s3"
			cfg.S3.HTTPIdleConnTimeout = "1parsec"
		}, "invalid s3 http_idle_conn_timeout"},
		{"gcs allow_multipart_upload with default config", func(cfg *Config) {
			cfg.General.RemoteStorage = "gcs"
			cfg.GCS.AllowMultipartUpload = true
		}, ""},
		{"gcs allow_multipart_upload incompatible with endpoint", func(cfg *Config) {
			cfg.General.RemoteStorage = "gcs"
			cfg.GCS.AllowMultipartUpload = true
			cfg.GCS.Endpoint = "http://gcs:8080/storage/v1/"
		}, "gcs `allow_multipart_upload` requires gRPC client and is not compatible with `endpoint`"},
		{"gcs allow_multipart_upload incompatible with force_http", func(cfg *Config) {
			cfg.General.RemoteStorage = "gcs"
			cfg.GCS.AllowMultipartUpload = true
			cfg.GCS.ForceHttp = true
		}, "gcs `allow_multipart_upload` requires gRPC client and is not compatible with `force_http`"},
		{"gcs allow_multipart_upload incompatible with encryption_key", func(cfg *Config) {
			cfg.General.RemoteStorage = "gcs"
			cfg.GCS.AllowMultipartUpload = true
			cfg.GCS.EncryptionKey = "dGVzdC1rZXktdGVzdC1rZXktdGVzdC1rZXktdGVzdA=="
		}, "gcs `allow_multipart_upload` is not compatible with `encryption_key`"},
		{"gcs allow_multipart_download with default config", func(cfg *Config) {
			cfg.General.RemoteStorage = "gcs"
			cfg.GCS.AllowMultipartDownload = true
		}, ""},
		{"gcs allow_multipart_download requires download_concurrency > 1", func(cfg *Config) {
			cfg.General.RemoteStorage = "gcs"
			cfg.GCS.AllowMultipartDownload = true
			cfg.GCS.DownloadConcurrency = 1
		}, "`allow_multipart_download` require `download_concurrency` in `gcs` section more than 1"},
		{"unknown remote storage", func(cfg *Config) {
			cfg.General.RemoteStorage = "banana"
		}, "'banana' is unknown remote storage"},
		{"ftp concurrency below download concurrency", func(cfg *Config) {
			cfg.General.RemoteStorage = "ftp"
			cfg.General.DownloadConcurrency = 4
			cfg.FTP.Concurrency = 1
		}, "FTP_CONCURRENCY=1 should be great or equal"},
		{"lz4 compression rejected", func(cfg *Config) {
			cfg.General.RemoteStorage = "s3"
			cfg.S3.CompressionFormat = "lz4"
		}, "clickhouse already compressed data by lz4"},
		{"unsupported compression format", func(cfg *Config) {
			cfg.General.RemoteStorage = "s3"
			cfg.S3.CompressionFormat = "rar"
		}, "'rar' is unsupported compression format"},
		{"bad clickhouse timeout", func(cfg *Config) {
			cfg.ClickHouse.Timeout = "1parsec"
		}, "invalid clickhouse timeout"},
		{"embedded backup restore requires timeout >= 4h", func(cfg *Config) {
			cfg.ClickHouse.UseEmbeddedBackupRestore = true
			cfg.ClickHouse.Timeout = "1h"
		}, "not enough for `use_embedded_backup_restore: true`"},
		{"freeze_by_part incompatible with embedded backup restore", func(cfg *Config) {
			cfg.ClickHouse.UseEmbeddedBackupRestore = true
			cfg.ClickHouse.Timeout = "5h"
			cfg.ClickHouse.FreezeByPart = true
		}, "`freeze_by_part: true` is not compatible with `use_embedded_backup_restore: true`"},
		{"bad cos timeout", func(cfg *Config) {
			cfg.COS.Timeout = "1parsec"
		}, "invalid cos timeout"},
		{"bad ftp timeout", func(cfg *Config) {
			cfg.FTP.Timeout = "1parsec"
		}, "invalid ftp timeout"},
		{"bad azblob timeout", func(cfg *Config) {
			cfg.AzureBlob.Timeout = "1parsec"
		}, "invalid azblob timeout"},
		{"bad s3 storage class", func(cfg *Config) {
			cfg.S3.StorageClass = "SUPER_FAST"
		}, "'SUPER_FAST' is bad S3_STORAGE_CLASS"},
		{"custom s3 storage class allowed with use_custom_storage_class", func(cfg *Config) {
			cfg.S3.UseCustomStorageClass = true
			cfg.S3.StorageClass = "SUPER_FAST"
		}, ""},
		{"multipart download requires s3 concurrency > 1", func(cfg *Config) {
			cfg.S3.AllowMultipartDownload = true
			cfg.S3.Concurrency = 1
		}, "`allow_multipart_download` require `concurrency`"},
		{"api secure without certificate", func(cfg *Config) {
			cfg.API.Secure = true
		}, "api.certificate_file must be defined"},
		{"api secure without private key", func(cfg *Config) {
			cfg.API.Secure = true
			cfg.API.CertificateFile = "/nonexistent/cert.pem"
		}, "api.private_key_file must be defined"},
		{"api secure with unreadable key pair", func(cfg *Config) {
			cfg.API.Secure = true
			cfg.API.CertificateFile = "/nonexistent/cert.pem"
			cfg.API.PrivateKeyFile = "/nonexistent/key.pem"
		}, "ValidateConfig LoadX509KeyPair"},
		{"bad custom command timeout", func(cfg *Config) {
			cfg.Custom.CommandTimeout = "1parsec"
		}, "invalid custom command timeout"},
		{"empty custom command timeout", func(cfg *Config) {
			cfg.Custom.CommandTimeout = ""
		}, "empty custom command timeout"},
		{"bad retries pause", func(cfg *Config) {
			cfg.General.RetriesPause = "1parsec"
		}, "invalid retries pause"},
		{"empty retries pause", func(cfg *Config) {
			cfg.General.RetriesPause = ""
		}, "empty retries pause"},
		{"bad watch interval", func(cfg *Config) {
			cfg.General.WatchInterval = "1parsec"
		}, "invalid watch interval"},
		{"bad full interval", func(cfg *Config) {
			cfg.General.FullInterval = "1parsec"
		}, "invalid full interval for watch"},
		{"watch schedule without name", func(cfg *Config) {
			cfg.General.WatchSchedules = WatchSchedules{{Full: "@daily"}}
		}, "watch schedule requires non-empty `name`"},
		{"watch schedule invalid name", func(cfg *Config) {
			cfg.General.WatchSchedules = WatchSchedules{{Name: "bad name!", Full: "@daily"}}
		}, "only [a-zA-Z0-9_-] allowed"},
		{"watch schedule without full cron", func(cfg *Config) {
			cfg.General.WatchSchedules = WatchSchedules{{Name: "nightly"}}
		}, "requires `full` cron expression"},
		{"watch schedule bad full cron", func(cfg *Config) {
			cfg.General.WatchSchedules = WatchSchedules{{Name: "nightly", Full: "not-a-cron"}}
		}, "invalid `full` cron expression"},
		{"watch schedule bad increment cron", func(cfg *Config) {
			cfg.General.WatchSchedules = WatchSchedules{{Name: "nightly", Full: "@daily", Increment: "not-a-cron"}}
		}, "invalid `increment` cron expression"},
		{"watch schedule bad full_type", func(cfg *Config) {
			cfg.General.WatchSchedules = WatchSchedules{{Name: "nightly", Full: "@daily", FullType: "merge"}}
		}, "invalid `full_type` `merge`"},
		{"duplicate watch schedule name", func(cfg *Config) {
			cfg.General.WatchSchedules = WatchSchedules{
				{Name: "nightly", Full: "@daily"},
				{Name: "nightly", Full: "@weekly"},
			}
		}, "duplicate watch schedule name `nightly`"},
		{"watch schedule name prefix overlap", func(cfg *Config) {
			cfg.General.WatchSchedules = WatchSchedules{
				{Name: "prod", Full: "@daily"},
				{Name: "prod-eu", Full: "@daily"},
			}
		}, "backup chains will overlap"},
		{"bad api cancel_operation_timeout", func(cfg *Config) {
			cfg.API.CancelOperationTimeout = "1parsec"
		}, "invalid api.cancel_operation_timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mutate(cfg)
			err := ValidateConfig(cfg)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestValidateObjectDiskConfig(t *testing.T) {
	storages := []struct {
		name string
		set  func(cfg *Config, path, objectDiskPath string)
	}{
		{"s3", func(cfg *Config, p, o string) { cfg.S3.Path, cfg.S3.ObjectDiskPath = p, o }},
		{"gcs", func(cfg *Config, p, o string) { cfg.GCS.Path, cfg.GCS.ObjectDiskPath = p, o }},
		{"azblob", func(cfg *Config, p, o string) { cfg.AzureBlob.Path, cfg.AzureBlob.ObjectDiskPath = p, o }},
		{"cos", func(cfg *Config, p, o string) { cfg.COS.Path, cfg.COS.ObjectDiskPath = p, o }},
		{"ftp", func(cfg *Config, p, o string) { cfg.FTP.Path, cfg.FTP.ObjectDiskPath = p, o }},
		{"sftp", func(cfg *Config, p, o string) { cfg.SFTP.Path, cfg.SFTP.ObjectDiskPath = p, o }},
	}
	cases := []struct {
		name           string
		path           string
		objectDiskPath string
		wantErr        bool
	}{
		{"both empty", "", "", true},
		{"object_disk_path without path", "", "object_disk", true},
		{"path without object_disk_path", "backup", "", true},
		{"object_disk_path is prefix of path", "object_disk/backup", "object_disk", true},
		{"equal paths", "backup", "backup", true},
		{"disjoint paths", "backup", "object_disk", false},
	}
	for _, storage := range storages {
		for _, tc := range cases {
			t.Run(storage.name+" "+tc.name, func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.General.RemoteStorage = storage.name
				storage.set(cfg, tc.path, tc.objectDiskPath)
				err := ValidateObjectDiskConfig(cfg)
				if !tc.wantErr {
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					return
				}
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), "invalid "+storage.name+"->object_disk_path") {
					t.Fatalf("expected error to mention %s->object_disk_path, got: %v", storage.name, err)
				}
			})
		}
	}
	t.Run("use_embedded_backup_restore skips validation", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.General.RemoteStorage = "s3"
		cfg.ClickHouse.UseEmbeddedBackupRestore = true
		cfg.S3.Path, cfg.S3.ObjectDiskPath = "", ""
		if err := ValidateObjectDiskConfig(cfg); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("non-object-disk remote storage skips validation", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.General.RemoteStorage = "none"
		if err := ValidateObjectDiskConfig(cfg); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestDefaultMaxBrokenPartRatio(t *testing.T) {
	if got := DefaultConfig().General.MaxBrokenPartRatio; got != 0 {
		t.Fatalf("expected default max_broken_part_ratio to be 0, got %v", got)
	}
}

func TestValidateMaxBrokenPartRatio(t *testing.T) {
	cases := []struct {
		name    string
		ratio   float64
		wantErr bool
	}{
		{"default zero", 0, false},
		{"mid", 0.5, false},
		{"one", 1, false},
		{"negative", -0.1, true},
		{"above one", 1.5, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.General.RemoteStorage = "s3"
			cfg.General.MaxBrokenPartRatio = tc.ratio
			err := ValidateConfig(cfg)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for ratio %v, got nil", tc.ratio)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for ratio %v: %v", tc.ratio, err)
			}
		})
	}
}

func TestAllowPartialBackup(t *testing.T) {
	cases := []struct {
		name        string
		ratio       float64
		broken      int
		total       int
		wantAllowed bool
	}{
		{"no broken parts always allowed", 0, 0, 10, true},
		{"default ratio rejects any broken part", 0, 1, 10, false},
		{"broken under threshold allowed", 0.5, 4, 10, true},
		{"broken exactly at threshold allowed", 0.5, 5, 10, true},
		{"broken over threshold rejected", 0.5, 6, 10, false},
		{"zero total parts rejected when broken", 0.5, 1, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := GeneralConfig{MaxBrokenPartRatio: tc.ratio}
			if got := cfg.AllowPartialBackup(tc.broken, tc.total); got != tc.wantAllowed {
				t.Fatalf("AllowPartialBackup(%d,%d) with ratio %v = %v, want %v", tc.broken, tc.total, tc.ratio, got, tc.wantAllowed)
			}
		})
	}
}

func TestDefaultCompleteResumableAfterRestartCommands(t *testing.T) {
	cfg := DefaultConfig()

	for _, command := range []string{"upload", "download"} {
		if !cfg.API.IsCompleteResumableAfterRestartCommand(command) {
			t.Fatalf("expected %q to be allowed for automatic resume after restart", command)
		}
	}

	for _, command := range []string{"create", "restore"} {
		if cfg.API.IsCompleteResumableAfterRestartCommand(command) {
			t.Fatalf("expected %q to require explicit opt-in for automatic resume after restart", command)
		}
	}
}
