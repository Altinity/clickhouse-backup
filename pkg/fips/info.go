package fips

import (
	"fmt"
	"io"
	"runtime"
)

// PrintInfo writes a human-readable FIPS build + runtime posture dump to w.
// Surfaces every field a customer or auditor would otherwise retrieve via
// `go version -m <binary>` (which requires a local Go toolchain) plus the live
// crypto/fips140 introspection.
//
// Output is line-oriented `key: value` so tests can grep without a parser.
func PrintInfo(w io.Writer, binaryName, version, gitCommit, buildDate string) {
	_, _ = fmt.Fprintf(w, "binary:          %s\n", binaryName)
	_, _ = fmt.Fprintf(w, "version:         %s\n", version)
	_, _ = fmt.Fprintf(w, "git_sha:         %s\n", gitCommit)
	_, _ = fmt.Fprintf(w, "built_at:        %s\n", buildDate)
	_, _ = fmt.Fprintf(w, "go_version:      %s\n", runtime.Version())
	_, _ = fmt.Fprintf(w, "goos:            %s\n", runtime.GOOS)
	_, _ = fmt.Fprintf(w, "goarch:          %s\n", runtime.GOARCH)
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "fips_module:")
	_, _ = fmt.Fprintf(w, "  build_setting: GOFIPS140=%q\n", BuildSetting("GOFIPS140"))
	_, _ = fmt.Fprintf(w, "  enabled:       %t\n", Enabled())
	_, _ = fmt.Fprintf(w, "  enforced:      %t\n", Enforced())
	_, _ = fmt.Fprintf(w, "  version:       %s\n", Version())
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "godebug:")
	_, _ = fmt.Fprintf(w, "  runtime_env:   %q\n", GODEBUGRaw())
	_, _ = fmt.Fprintf(w, "  default:       %q\n", BuildSetting("DefaultGODEBUG"))
}
