package fips

import (
	"crypto/fips140"
	"os"
	"runtime/debug"
)

// Indirection vars so tests can drive every branch without controlling the Go
// toolchain. Production paths read crypto/fips140.Enabled / Enforced / Version;
// tests swap the function pointer.
var (
	Enabled  = fips140.Enabled
	Enforced = fips140.Enforced
	Version  = fips140.Version
)

// GODEBUGRaw returns the raw GODEBUG env var. Indirection seam so the FIPS info
// dump can distinguish GODEBUG unset from fips140=on.
var GODEBUGRaw = func() string { return os.Getenv("GODEBUG") }

// BuildSetting walks runtime/debug.ReadBuildInfo().Settings and returns the
// value for the given key (or "" when ReadBuildInfo fails or key is absent).
// Used to surface DefaultGODEBUG and GOFIPS140.
var BuildSetting = func(key string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == key {
			return s.Value
		}
	}
	return ""
}
