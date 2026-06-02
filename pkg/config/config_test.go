package config

import "testing"

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
