// Package acvpwrapper implements ACVP module-wrapper protocol handlers that
// are embedded into the clickhouse-backup binary.
//
// Files are split by concern:
//   - wrapper.go: base modulewrapper command set and protocol loop.
//   - official_compat.go: shared command handlers modeled after Go fips140test
//     behavior where public APIs allow.
package acvpwrapper
