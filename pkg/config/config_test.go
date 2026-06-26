package config

import (
	"bytes"
	"flag"
	"os"
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
