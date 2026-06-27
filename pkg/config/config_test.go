package config

import "testing"

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
