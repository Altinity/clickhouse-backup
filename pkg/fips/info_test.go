package fips

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPrintInfo_Format pins the line-oriented output so downstream graders
// (audit scripts, e2e) can grep without a parser. Stubs the crypto/fips140
// seam so the assertions are deterministic regardless of how `go test` ran.
func TestPrintInfo_Format(t *testing.T) {
	origEnabled, origEnforced, origVersion, origGODEBUG := Enabled, Enforced, Version, GODEBUGRaw
	t.Cleanup(func() { Enabled, Enforced, Version, GODEBUGRaw = origEnabled, origEnforced, origVersion, origGODEBUG })
	Enabled = func() bool { return false }
	Enforced = func() bool { return false }
	Version = func() string { return "v1.0.0" }
	GODEBUGRaw = func() string { return "fips140=off" }

	var buf bytes.Buffer
	PrintInfo(&buf, "clickhouse-backup", "2.6.0-fips", "2e411f5", "2026-05-29T09:19:23")
	out := buf.String()

	for _, want := range []string{
		"binary:          clickhouse-backup\n",
		"version:         2.6.0-fips\n",
		"git_sha:         2e411f5\n",
		"built_at:        2026-05-29T09:19:23\n",
		"go_version:      " + runtime.Version() + "\n",
		"goos:            " + runtime.GOOS + "\n",
		"goarch:          " + runtime.GOARCH + "\n",
		"fips_module:\n",
		"  enabled:       false\n",
		"  enforced:      false\n",
		"  version:       v1.0.0\n",
		"godebug:\n",
		"  runtime_env:   \"fips140=off\"\n",
	} {
		require.Contains(t, out, want)
	}
}
