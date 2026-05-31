package fips

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRuntime_Stubbable verifies the indirection — production callers see
// crypto/fips140.{Enabled,Enforced}; tests swap the function-pointer pair to
// drive output without depending on the actual Go toolchain FIPS posture
// (which a unit-test runner cannot control).
func TestRuntime_Stubbable(t *testing.T) {
	origEnabled, origEnforced := Enabled, Enforced
	t.Cleanup(func() { Enabled, Enforced = origEnabled, origEnforced })

	for _, want := range []bool{true, false} {
		Enabled = func() bool { return want }
		Enforced = func() bool { return !want }
		require.Equal(t, want, Enabled(), "Enabled stub")
		require.Equal(t, !want, Enforced(), "Enforced stub")
	}
}

// TestGODEBUGRaw_ReadsEnv confirms the helper returns the GODEBUG env var
// verbatim, including multi-key payloads and empty after unset.
func TestGODEBUGRaw_ReadsEnv(t *testing.T) {
	const payload = "fips140=on,other=foo"
	t.Setenv("GODEBUG", payload)
	require.Equal(t, payload, GODEBUGRaw(), "GODEBUGRaw should mirror env verbatim")

	t.Setenv("GODEBUG", "")
	require.Equal(t, "", GODEBUGRaw(), "GODEBUGRaw should be empty after unset")
}

// TestBuildSetting_ReturnsExistingKey uses GOOS because every Go binary
// (including the test binary) has a GOOS build setting populated by the
// toolchain via runtime/debug.ReadBuildInfo().
func TestBuildSetting_ReturnsExistingKey(t *testing.T) {
	require.NotEmpty(t, BuildSetting("GOOS"), "GOOS build setting must be populated for any Go binary")
}

// TestBuildSetting_EmptyForUnknownKey confirms the absent-key contract.
func TestBuildSetting_EmptyForUnknownKey(t *testing.T) {
	require.Equal(t, "", BuildSetting("NotARealSetting"))
}
