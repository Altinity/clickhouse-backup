# QA-STP ClickHouse Backup FIPS Compatibility Release
# Software Test Plan

(c) 2026 Altinity Inc. All Rights Reserved.

**Author:** vsviderskyi

**Date:** May 19, 2026

## Table of Contents

* 1 [Introduction](#introduction)
* 2 [Timeline](#timeline)
* 3 [Configuration Requirements](#configuration-requirements)
* 4 [Build Verification](#build-verification)
* 5 [Human Resources And Assignments](#human-resources-and-assignments)
* 6 [Release Notes](#release-notes)
* 7 [FIPS-Compatible `clickhouse-backup-fips` Configuration](#fips-compatible-clickhouse-backup-fips-configuration)
* 8 [Test Environment](#test-environment)
* 9 [Inputs and Outputs of `clickhouse-backup-fips`](#inputs-and-outputs-of-clickhouse-backup-fips)
* 10 [Connectivity Against ClickHouse FIPS and Non-FIPS Servers](#connectivity-against-clickhouse-fips-and-non-fips-servers)
* 11 [`clickhouse-backup-fips --version` Output](#clickhouse-backup-fips---version-output)
* 12 [GODEBUG `fips140` Modes](#godebug-fips140-modes)
* 13 [FIPS Integrity Self-test Failure on Tampered Binary](#fips-integrity-self-test-failure-on-tampered-binary)
* 14 [Forced CAST Failures](#forced-cast-failures)
* 15 [Inbound TLS — REST API With `openssl s_client`](#inbound-tls-rest-api-with-openssl-s_client)
* 16 [Outbound TLS to ClickHouse Server With `openssl s_server`](#outbound-tls-to-clickhouse-server-with-openssl-s_server)
* 17 [Outbound TLS to S3 Endpoint With `openssl s_server`](#outbound-tls-to-s3-endpoint-with-openssl-s_server)
* 18 [ACVP Tests](#acvp-tests)
* 19 [Server Listening-Port Assertion](#server-listening-port-assertion)
* 20 [Connection to FIPS ClickHouse with Non-FIPS Configuration](#connection-to-fips-clickhouse-with-non-fips-configuration)
* 21 [Outbound TLS to Non-FIPS ClickHouse with Cipher Profile](#outbound-tls-to-non-fips-clickhouse-with-cipher-profile)

## Introduction

This test plan covers FIPS 140-3 compatibility testing of the `clickhouse-backup` Altinity FIPS-compatible build (`clickhouse-backup-fips`).

The main goal of this test plan is to validate that `clickhouse-backup` is FIPS 140-3 compatible.
Test results ensure that `clickhouse-backup`:
- Operates correctly under FIPS constraints
- Properly enforces cryptographic restrictions
- Uses FIPS-compliant TLS for all inbound and outbound connections.

To validate this, the following items SHALL be checked:

* The FIPS-built `clickhouse-backup` binary starts with the Go FIPS 140-3 cryptographic module enabled and reports it in `--version` output under all three Go FIPS runtime modes (`GODEBUG` unset, `GODEBUG=fips140=on`, `GODEBUG=fips140=only`).
* The FIPS cipher policy is enforced for inbound and outbound TLS when running in strict mode (`GODEBUG=fips140=only`).
* The binary aborts on startup if the FIPS integrity check or any startup cryptographic self-test fails.
* The binary stays operational against both FIPS-compatible and non-FIPS-compatible ClickHouse server versions.
* The ACVP (Automated Cryptographic Validation Protocol) test wrapper bundled with `clickhouse-backup` runs successfully against the FIPS-built binary (when this optional check is enabled by tests).
* The FIPS-built `clickhouse-backup` binary includes the Go build setting `GOFIPS140=v1.0.0` in its embedded build metadata.
* The regular (non-FIPS) `clickhouse-backup` binary reports `FIPS 140-3: false` in `--version` output.
* The `clickhouse-backup-fips server` process listens on the FIPS REST API TLS port (`7172`) when started in strict mode (`GODEBUG=fips140=only`).
* A non-FIPS-correct backup configuration (`secure: false`) is rejected when connecting to the FIPS ClickHouse TLS endpoint (`:9440`).
* The FIPS-built binary remains interoperable with a non-FIPS ClickHouse server over secure native TLS when the TLS connection is restricted to FIPS-approved cipher suites.

## Timeline

The testing of `clickhouse-backup` FIPS builds SHALL be started on May 7, 2026 and be completed by May 27, 2026.

## Configuration Requirements

Plain HTTP/TCP on any external connection is a configuration error for FIPS compliance.
TLS must be enabled for all connections to:

- ClickHouse Server
- `clickhouse-backup` REST API
- S3-compatible remote storage endpoint

For TLS policy validation, the test suite also uses OpenSSL probe tools:

- `openssl s_client` (acts as a TLS client to test inbound API listener policy)
- `openssl s_server` (acts as a TLS server to test outbound client policy)

## Build Verification

**Objective:** Verify binaries are FIPS builds and linked to Go Cryptographic Module v1.0.0.

**Certificates:**
- [CMVP #5247](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5247)
- [CAVP A6650](https://csrc.nist.gov/projects/cryptographic-algorithm-validation-program/details?product=19371)


| Test Assertion | Description | Expected Result |
| --- | --- | --- |
| FIPS indicator in binary version output | Run `clickhouse-backup-fips --version` (`clickhouse_backup_fips_version_output`) and run control check on non-FIPS binary (`clickhouse_backup_fips_version_output_negative_check`) | FIPS binary reports `FIPS 140-3: true`; non-FIPS binary does not report `true` |
| Build flag | Run `go version -m clickhouse-backup-fips` (`gofips140_build_flags_present`) | Output contains `build	GOFIPS140=v1.0.0` |
| FIPS runtime behavior across Go modes | Run `godebug_fips140_modes` with `GODEBUG` unset, `fips140=on`, and `fips140=only` | For each mode, `--version` reports `FIPS 140-3: true`, and `tables` against the FIPS ClickHouse TLS endpoint succeeds (`exit 0`) |

Direct checks of `crypto/fips140.Version()` and `crypto/fips140.Enabled()` are not called as standalone assertions in the current `clickhouse-backup` TestFlows scenarios; their behavior is validated through `--version` output and runtime connectivity checks above.


## Human Resources And Assignments

The following team members SHALL be dedicated to this release:

* Vitalii Sviderskyi (regression tests)
* Vitaliy Zakaznikov (manager, regression tests)
* Eugene Klimov (clickhouse-backup, FIPS build and Docker image)


## Release Notes

* https://github.com/Altinity/clickhouse-backup/blob/master/ChangeLog.md
* https://docs.altinity.com/altinitystablebuilds/fips-compatible-altinity-builds/


## FIPS-Compatible `clickhouse-backup-fips` Configuration

A `clickhouse-backup-fips` configuration is FIPS-compatible only when:

* `clickhouse.secure: true`.
* `clickhouse.port: 9440` (secure native TLS). Plain native TCP `9000` and plain HTTP `8123` MUST NOT be used against a FIPS-configured ClickHouse server.
* `api.secure: true` plus a valid certificate / private key when running `clickhouse-backup-fips server`.
* For S3-compatible remote storage, `s3.endpoint` is left empty and `s3.region` is set, so the SDK targets the AWS FIPS hostname `s3-fips.<region>.amazonaws.com`.

The canonical clickhouse-backup configurations used by the regression suite are:

* `test/testflows/clickhouse_backup/configs/backup/fips/config-fips-connectivity-fips-server.yml` — secure native TCP `9440` against the FIPS ClickHouse server.
* `test/testflows/clickhouse_backup/configs/backup/fips/config-fips-connectivity-nonfips-server.yml` — negative check: FIPS-compatible client config (`secure: true`, `port: 9440`) against a default-config non-FIPS ClickHouse server (no `tcp_port_secure` listener); the connection must fail.
* `test/testflows/clickhouse_backup/configs/backup/fips/config-fips-api-tls.yml` — REST API listener on `7172` with `api.secure: true`.
* `test/testflows/clickhouse_backup/configs/backup/fips/config-fips-outbound-clickhouse-tls.yml` — outbound TLS policy checks against a ClickHouse-style endpoint (`openssl s_server`).
* `test/testflows/clickhouse_backup/configs/backup/fips/config-fips-outbound-s3-tls.yml` — outbound TLS policy checks against an S3-style HTTPS endpoint (`openssl s_server`).
* `test/testflows/clickhouse_backup/configs/backup/fips/config-fips-connectivity-fips-server-misconfig.yml` — negative check with `secure: false` against FIPS ClickHouse TLS port `9440`.
* `test/testflows/clickhouse_backup/configs/backup/fips/config-fips-nonfips-ch-tls-fipscipher.yml` — end-to-end positive TLS check against non-FIPS ClickHouse with FIPS-approved ciphers.


## Test Environment

The following artifacts and tools will be used:

* FIPS `clickhouse-backup` binary built with `GOFIPS140=v1.0.0` (`clickhouse-backup-fips`).
* FIPS-compatible Altinity ClickHouse server image: `altinity/clickhouse-server:25.3.8.30001.altinityfips`.
* Non-FIPS Altinity ClickHouse server image: `altinity/clickhouse-server:25.8.16.10002.altinitystable`.
* `openssl` CLI tool on the test host for TLS client and server probes.

> [!NOTE]
> The regression sets `GODEBUG` per command rather than at the FIPS container level. The suite covers all three modes documented in [GODEBUG fips140 Modes](#godebug-fips140-modes) (`unset`, `fips140=on`, `fips140=only`), and the forced-CAST scenario also injects `GODEBUG=failfipscast=<NAME>,fips140=on`; a single container-level value would prevent the matrix and the negative-self-test path from running. The Altinity FIPS Docker image still ships with `GODEBUG=fips140=only` as documented in [FIPS Configuration](#fips-configuration); that default is honored when the image is run as-is.

## Inputs and Outputs of `clickhouse-backup-fips`

`clickhouse-backup-fips` exposes the following network surface in the regression environment:

* Inbound: REST API listener on TCP port `7172`. With `api.secure: true` (the FIPS regression default) the listener accepts only TLS handshakes; no plain HTTP listener is opened. No other ports are bound by the binary itself.
* Outbound to ClickHouse: secure native TCP port `9440` (`clickhouse.secure: true`, `clickhouse.port: 9440`). Plain native TCP `9000` and plain HTTP `8123` MUST NOT be used by `clickhouse-backup-fips`.
* Outbound to S3-compatible storage: HTTPS to the AWS FIPS hostname `s3-fips.<region>.amazonaws.com:443` when `s3.endpoint` is empty and `s3.region` is set.

The [Server Listening-Port Assertion](#server-listening-port-assertion) subsection below describes how the inbound surface is verified.

## Connectivity Against ClickHouse FIPS and Non-FIPS Servers

Check that `clickhouse-backup-fips` connects to a FIPS-compatible ClickHouse server and cannot connect to a default-config non-FIPS one.

Run the FIPS `clickhouse-backup` against:

* Positive: FIPS-compatible Altinity ClickHouse server `altinity/clickhouse-server:25.3.8.30001.altinityfips` over secure native TCP port `9440`.
* Negative: non-FIPS Altinity ClickHouse server `altinity/clickhouse-server:25.8.16.10002.altinitystable` running with image defaults (no `tcp_port_secure` listener), with the FIPS-compatible client config (`secure: true`, `port: 9440`).

Expected result:

* Positive case: `clickhouse-backup-fips tables` exits `0` and returns the list of tables.
* Negative case: `clickhouse-backup-fips tables` exits with a non-zero code because the non-FIPS server does not expose the secure native TCP port required by the FIPS-compatible client config; no table-listing success marker appears.

## `clickhouse-backup-fips --version` Output

Check that the FIPS-built binary reports the FIPS 140-3 module as active in its version output, and that the binary itself is built with `GOFIPS140=v1.0.0`.

Run:

```
clickhouse-backup-fips --version
go version -m $(which clickhouse-backup-fips)
```

Expected result:

* `--version` output contains the line `FIPS 140-3: true` (this corresponds to Go's `crypto/fips140.Enabled()` returning true at runtime).
* `go version -m` output contains `build	GOFIPS140=v1.0.0`.
* Control check: the regular (non-FIPS) `clickhouse-backup --version` output reports `FIPS 140-3: false`.

## GODEBUG `fips140` Modes


Check that `clickhouse-backup-fips` behaves correctly under each of the three Go FIPS runtime modes listed below. 

For every mode run both `--version` and a basic `tables` command against the FIPS-compatible Altinity ClickHouse server `altinity/clickhouse-server:25.3.8.30001.altinityfips`.

* `GODEBUG` not set — FIPS mode is enabled by build-time default (`GOFIPS140=v1.0.0`).

    Expected result:
    * `--version` reports `FIPS 140-3: true`.
    * `tables` returns the list of tables.

* `GODEBUG=fips140=on` — FIPS mode is enabled explicitly without strict enforcement. This is the mode used for the forced CAST test below.

    Expected result:
    * `--version` reports `FIPS 140-3: true`.
    * `tables` returns the list of tables.

* `GODEBUG=fips140=only` — FIPS mode is enabled with strict enforcement; any non-approved cryptographic operation triggers an error or panic. This is the mode used for the TLS policy tests below and the default of the FIPS Docker image.

    Expected result:
    * `--version` reports `FIPS 140-3: true`.
    * `tables` against an approved TLS configuration returns the list of tables.
    * Non-approved cryptographic operations cause the binary to fail.
    * The full `clickhouse-backup` TestFlows regression suite runs in this mode without panics or strict-FIPS-only regressions.

> [!NOTE]
> No negative test exists for "the binary panics when `GODEBUG` is unset". `clickhouse-backup-fips` is built with `GOFIPS140=v1.0.0`, so the FIPS module is enabled by the build flag, not by `GODEBUG`. The "GODEBUG not set" mode above IS the production-default operation; the binary is expected to operate normally there.

## FIPS Integrity Self-test Failure on Tampered Binary


Check that the FIPS startup integrity self-test stops the binary if the FIPS module bytes have been modified.

Take a copy of `clickhouse-backup-fips`, corrupt its `.go.fipsinfo` checksum section, and try to run the tampered copy.

Expected result:

* The tampered binary panics on startup with `panic: fips140: verification mismatch` and exits with a non-zero exit code.
* The unmodified original binary continues to work normally.

## Forced CAST Failures


Check that the FIPS module refuses to start if any startup self-test fails.

Run the FIPS binary with the `GODEBUG=failfipscast` hook, substituting one self-test name at a time, for example:

```
GODEBUG=failfipscast=SHA2-256,fips140=on clickhouse-backup-fips --version
```

`SHA2-256` in the command above can be replaced with any effective CAST name from the list below:

```
AES-CBC
CTR_DRBG
CounterKDF
DetECDSA P-256 SHA2-512 sign
ECDH PCT
ECDSA P-256 SHA2-512 sign and verify
ECDSA PCT
Ed25519 sign and verify
Ed25519 sign and verify PCT
HKDF-SHA2-256
HMAC-SHA2-256
KAS-ECC-SSC P-256
ML-DSA sign and verify PCT
ML-DSA-44
ML-KEM PCT
ML-KEM PCT
ML-KEM-768
PBKDF2
RSA sign and verify PCT
RSASSA-PKCS-v1.5 2048-bit sign and verify
SHA2-256
SHA2-512
TLSv1.2-SHA2-256
TLSv1.3-SHA2-256
cSHAKE128
```

The list is taken directly from the Go FIPS test suite (file `crypto/internal/fips140test/cast_test.go` of the Go release in use).

Expected result for every name in the list:

* Baseline run with `GODEBUG=fips140=on clickhouse-backup-fips --version` succeeds.
* The process exits with a non-zero code.
* The output contains `fatal error: FIPS 140-3 self-test failed: <NAME>: simulated CAST failure`.

How to obtain and refresh this list: 

* Open `$(go env GOROOT)/src/crypto/internal/fips140test/cast_test.go` and copy the entries from the `allCASTs` slice. 

* The list should be refreshed when the Go version used to build `clickhouse-backup-fips` is upgraded, because new algorithms may add/rename/remove entries.

## Inbound TLS — REST API With `openssl s_client`

Check that the REST API listener of `clickhouse-backup-fips server` accepts only FIPS-approved TLS handshakes from clients.

Start `clickhouse-backup-fips server` with `GODEBUG=fips140=only` and the FIPS TLS API config (`config-fips-api-tls.yml`, using regression TLS certificate fixtures), then connect with `openssl s_client`.

Cipher names below are taken from the official Altinity FIPS configuration documentation (<https://docs.altinity.com/altinitystablebuilds/fips-compatible-altinity-builds/#configuration-of-altinity-stable-builds-for-fips-compatible-operation>).

FIPS-approved profiles (handshake MUST succeed):

* `openssl s_client -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256` — expected result: handshake succeeds (`CONNECTION ESTABLISHED`).
* `openssl s_client -tls1_3 -ciphersuites TLS_AES_256_GCM_SHA384` — expected result: handshake succeeds.
* `openssl s_client -tls1_2 -cipher ECDHE-RSA-AES128-GCM-SHA256` — expected result: handshake succeeds.
* `openssl s_client -tls1_2 -cipher ECDHE-RSA-AES256-GCM-SHA384` — expected result: handshake succeeds.

Non-FIPS profiles (handshake MUST be rejected):

* `openssl s_client -tls1_3 -ciphersuites TLS_CHACHA20_POLY1305_SHA256` — expected result: handshake is rejected (`alert handshake failure` or equivalent).
* `openssl s_client -tls1_2 -cipher ECDHE-RSA-CHACHA20-POLY1305` — expected result: handshake is rejected.
* `openssl s_client -tls1_2 -cipher RC4-SHA` — expected result: handshake is rejected (legacy/weak cipher).
* `openssl s_client -tls1_2 -cipher DES-CBC3-SHA` — expected result: handshake is rejected (legacy/weak cipher).
* `openssl s_client -tls1` — expected result: handshake is rejected (TLSv1.0 is below the FIPS minimum protocol version).
* `openssl s_client -tls1_1` — expected result: handshake is rejected (TLSv1.1 is below the FIPS minimum protocol version).

## Outbound TLS to ClickHouse Server With `openssl s_server`


Check that `clickhouse-backup-fips` enforces outbound TLS policy when connecting to a ClickHouse-style TLS endpoint with both approved and non-approved cipher profiles.

Start a local `openssl s_server` on the secure native ClickHouse port `9440` with a chosen cipher profile, then run `clickhouse-backup-fips` with `GODEBUG=fips140=only` and a config that points at this endpoint. Cipher names below come from the official Altinity FIPS configuration documentation.

FIPS-approved profiles (handshake MUST be accepted by `clickhouse-backup-fips` policy):

* `openssl s_server -tls1_2 -cipher ECDHE-RSA-AES128-GCM-SHA256` — expected result: TLS handshake is not rejected by the FIPS policy.
* `openssl s_server -tls1_2 -cipher ECDHE-RSA-AES256-GCM-SHA384` — expected result: TLS handshake is not rejected by the FIPS policy.
* `openssl s_server -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256` — expected result: TLS handshake is not rejected by the FIPS policy.
* `openssl s_server -tls1_3 -ciphersuites TLS_AES_256_GCM_SHA384` — expected result: TLS handshake is not rejected by the FIPS policy.

Non-FIPS profiles (handshake MUST be rejected):

* `openssl s_server -tls1_2 -cipher ECDHE-RSA-CHACHA20-POLY1305` — expected result: `clickhouse-backup-fips` fails with `remote error: tls: handshake failure` and `openssl s_server` reports `no shared cipher`.
* `openssl s_server -tls1_2 -cipher DHE-RSA-AES256-GCM-SHA384` — expected result: handshake is rejected as above.
* `openssl s_server -tls1_2 -cipher DHE-RSA-AES128-GCM-SHA256` — expected result: handshake is rejected as above.
* `openssl s_server -tls1_2 -cipher AES256-GCM-SHA384` — expected result: handshake is rejected as above.
* `openssl s_server -tls1_2 -cipher AES128-GCM-SHA256` — expected result: handshake is rejected as above.
* `openssl s_server -tls1_3 -ciphersuites TLS_CHACHA20_POLY1305_SHA256` — expected result: handshake is rejected as above.

## Outbound TLS to S3 Endpoint With `openssl s_server`


Check that `clickhouse-backup-fips` remains compatible with FIPS-approved outbound HTTPS profiles when connecting to an S3-style remote storage endpoint.

Two FIPS-specific items shape this setup:

* Leave `s3.endpoint` blank and set `s3.region` (e.g. `us-east-1`); the SDK targets the AWS FIPS hostname `s3-fips.us-east-1.amazonaws.com`.
* Name the `openssl s_server` container after that hostname and have it listen on port `443`.

Run `clickhouse-backup-fips list remote` with `GODEBUG=fips140=only`. Cipher names below come from the official Altinity FIPS configuration documentation.

FIPS-approved profiles (handshake MUST be accepted by `clickhouse-backup-fips` policy):

* `openssl s_server -tls1_2 -cipher ECDHE-RSA-AES128-GCM-SHA256` — expected result: TLS handshake is not rejected by the FIPS policy (downstream HTTP / S3-protocol errors from `openssl s_server -www` are acceptable, since this server is not a real S3 API).
* `openssl s_server -tls1_2 -cipher ECDHE-RSA-AES256-GCM-SHA384` — expected result: same as above.
* `openssl s_server -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256` — expected result: same as above.
* `openssl s_server -tls1_3 -ciphersuites TLS_AES_256_GCM_SHA384` — expected result: same as above.

## ACVP Tests

Run the ACVP (Automated Cryptographic Validation Protocol) wrapper bundled with `clickhouse-backup`. This part is required for FIPS compatibity, but tests can be executed optionally.

Invoke `pkg/acvpwrapper/run.sh` against the FIPS-built binary.

Expected result:

* The wrapper runs the algorithm test vectors and exits successfully with no failures across the run.
* This check is optional in automation and runs only when `RUN_ACVP_TESTS=1` is set.

## Server Listening-Port Assertion

Check that `clickhouse-backup-fips server`, when started with the FIPS-compatible REST API config (`api.secure: true`, no plain ports configured), listens on the FIPS-approved TLS port.

Steps:

1. Start `clickhouse-backup-fips server` with `config-fips-api-tls.yml` and `GODEBUG=fips140=only` inside the FIPS backup container.
2. Read `/proc/net/tcp` and `/proc/net/tcp6` inside the container and extract every row whose state column equals `0A` (LISTEN). 
If the preferred host-style command `sudo ss -ltnp | grep "pid=<binary-pid>"` cannot be used in the FIPS regression container because `sudo` access is not available there (and process-owner socket inspection via `ss -p` requires elevated privileges, use `/proc/net/tcp{,6}` parsing, which is readable without `sudo` and still gives the container-network LISTEN set.

Expected result:

* The set of LISTEN ports is exactly `{7172}`.
* No other ports (in particular plain HTTP `8080`, the non-FIPS default `7171`, or any other) are bound in the FIPS container's network namespace.
* `openssl s_client -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256 -connect localhost:7172` completes the TLS handshake, confirming `:7172` carries TLS (not plain HTTP).

## Connection to FIPS ClickHouse with Non-FIPS Configuration

Check that a `clickhouse-backup-fips` configuration that violates [FIPS-Compatible `clickhouse-backup-fips` Configuration](#fips-compatible-clickhouse-backup-fips-configuration) cannot reach a FIPS-deployed ClickHouse server's secure port.

Steps:

1. Bring up the FIPS-compatible Altinity ClickHouse server (e.g. `altinity/clickhouse-server:25.3.8.30001.altinityfips`) with `tcp_port_secure: 9440` enabled.
2. Run `env GODEBUG=fips140=only clickhouse-backup-fips -c <misconfig> tables` from the FIPS backup container, where `<misconfig>` points at `clickhouse_fips_server:9440` with `secure: false` (i.e. plain native protocol against a TLS-only listener).

Expected result:

* The command exits with a non-zero exit code.
* The output does NOT contain a list-of-tables success marker (no `Atomic` / `Ordinary` row).

## Outbound TLS to Non-FIPS ClickHouse with Cipher Profile

Check that the FIPS outbound TLS policy applies end-to-end against a real non-FIPS ClickHouse server (not just `openssl s_server`), including the ClickHouse native protocol layer that runs after the handshake.

Bring up a non-FIPS Altinity ClickHouse server (`altinity/clickhouse-server:25.8.16.10002.altinitystable`) with `tcp_port_secure: 9440` enabled and the OpenSSL `<cipherList>` / `<cipherSuites>` restricted to a single profile per case. Run `env GODEBUG=fips140=only clickhouse-backup-fips -c <config> tables` from the FIPS backup container.

Expected result for each case:

* FIPS-approved profile (`ECDHE-RSA-AES128-GCM-SHA256` / `TLS_AES_128_GCM_SHA256`): `tables` exits `0` and output includes a ClickHouse connection success marker (positive case).

* Non-FIPS profile (`ECDHE-RSA-CHACHA20-POLY1305` / `TLS_CHACHA20_POLY1305_SHA256`): `clickhouse-backup-fips` fails with a 
TLS handshake-failure marker (`remote error: tls: handshake failure` / `no shared cipher` / `tls: protocol version not 
supported`).
