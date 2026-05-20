# QA-STP ClickHouse Backup FIPS Compatibility Release
# Software Test Plan

(c) 2026 Altinity Inc. All Rights Reserved.

**Author:** vsviderskyi

**Date:** May 19, 2026

## Execution Summary

**Completed:**

**Test Results:**

*

**Build Report:**

*

**Summary:**

Started to execute test plan on TBD and ended on TBD.

## Table of Contents

* 1 [Introduction](#introduction)
* 2 [Timeline](#timeline)
* 3 [Human Resources And Assignments](#human-resources-and-assignments)
* 4 [End User Recommendations](#end-user-recommendations)
    * 4.1 [Release Notes](#release-notes)
    * 4.2 [FIPS Configuration](#fips-configuration)
* 5 [Known Issues](#known-issues)
    * 5.1 [Open Issues](#open-issues)
    * 5.2 [Summary](#summary)
* 6 [Scope](#scope)
    * 6.1 [Test Environment](#test-environment)
    * 6.2 [Automated FIPS Regression Tests](#automated-fips-regression-tests)
        * 6.2.1 [Connectivity Against ClickHouse FIPS and Non-FIPS Server Versions](#connectivity-against-clickhouse-fips-and-non-fips-server-versions)
        * 6.2.2 [`clickhouse-backup-fips --version` Output](#clickhouse-backup-fips---version-output)
        * 6.2.3 [GODEBUG `fips140` Modes](#godebug-fips140-modes)
        * 6.2.4 [FIPS Integrity Self-test Failure on Tampered Binary](#fips-integrity-self-test-failure-on-tampered-binary)
        * 6.2.5 [Forced CAST Failures](#forced-cast-failures)
        * 6.2.6 [Inbound TLS — REST API With `openssl s_client`](#inbound-tls--rest-api-with-openssl-s_client)
        * 6.2.7 [Outbound TLS to ClickHouse Server With `openssl s_server`](#outbound-tls-to-clickhouse-server-with-openssl-s_server)
        * 6.2.8 [Outbound TLS to S3 Endpoint With `openssl s_server`](#outbound-tls-to-s3-endpoint-with-openssl-s_server)
        * 6.2.9 [ACVP Tests](#acvp-tests)

## Introduction

This test plan covers FIPS 140-3 compatibility testing of the `clickhouse-backup` Altinity FIPS-compatible build (`clickhouse-backup-fips`).

The main goal of this test plan is to validate that `clickhouse-backup` is FIPS 140-3 compatible. To validate this, the following items SHALL be checked:

* The FIPS-built `clickhouse-backup` binary starts with the Go FIPS 140-3 cryptographic module enabled and reports it in `--version` output under all three Go FIPS runtime modes (`GODEBUG` unset, `GODEBUG=fips140=on`, `GODEBUG=fips140=only`).
* The FIPS cipher policy is enforced for inbound and outbound TLS when running in strict mode (`GODEBUG=fips140=only`).
* The binary aborts on startup if the FIPS integrity check or any startup cryptographic self-test fails.
* The binary stays operational against both FIPS-compatible and non-FIPS-compatible ClickHouse server versions.
* The ACVP (Automated Cryptographic Validation Protocol) test wrapper bundled with `clickhouse-backup` runs successfully against the FIPS-built binary.

## Timeline

The testing of `clickhouse-backup` FIPS builds SHALL be started on May 7, 2026 and be completed by May 25, 2026.

## Human Resources And Assignments

The following team members SHALL be dedicated to this release:

* Vitalii Sviderskyi (regression tests)
* Vitaliy Zakaznikov (manager, regression tests)
* Eugene Klimov (clickhouse-backup, FIPS build and Docker image)

## End User Recommendations

### Release Notes

* https://github.com/Altinity/clickhouse-backup/blob/master/ChangeLog.md
* https://docs.altinity.com/altinitystablebuilds/fips-compatible-altinity-builds/

### FIPS Configuration

For FIPS-compatible operation, users should:

* Use the FIPS-built `clickhouse-backup-fips` binary.
* Configure TLS for FIPS-compatible ClickHouse server using TLSv1.2 and TLSv1.3 only, with the FIPS-approved cipher suites published in the Altinity FIPS documentation (<https://docs.altinity.com/altinitystablebuilds/fips-compatible-altinity-builds/#configuration-of-altinity-stable-builds-for-fips-compatible-operation>):
    * TLSv1.2 `cipherList`: `ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:AES128-GCM-SHA256:AES256-GCM-SHA384`
    * TLSv1.3 `cipherSuites`: `TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384`

`clickhouse-backup-fips` can also be used against non-FIPS ClickHouse servers.

## Known Issues

* None known at the time of writing.

### Open Issues

* TBD

### Summary

> [!NOTE]
> **Pass\*** — tests passed with known fails.

| Test Suite | Result | Comments |
| --- | --- | --- |
| Connectivity vs FIPS and non-FIPS ClickHouse server | [_TBD_](#connectivity-against-clickhouse-fips-and-non-fips-server-versions) | FIPS `clickhouse-backup` vs FIPS ClickHouse server, and FIPS `clickhouse-backup` vs non-FIPS ClickHouse server |
| `clickhouse-backup-fips --version` output | [_TBD_](#clickhouse-backup-fips---version-output) | |
| Test with `GODEBUG=fips140=on`, `GODEBUG=fips140=only`, and when `GODEBUG` is not set at all | [_TBD_](#godebug-fips140-modes) | |
| FIPS Integrity Self-test Failure on Tampered Binary | [_TBD_](#fips-integrity-self-test-failure-on-tampered-binary) | Tampered `.go.fipsinfo` checksum must abort startup |
| Forced CAST Failures | [_TBD_](#forced-cast-failures) | |
| Inbound TLS — REST API (`openssl s_client`) | [_TBD_](#inbound-tls--rest-api-with-openssl-s_client) | |
| Outbound TLS to ClickHouse server (`openssl s_server`) | [_TBD_](#outbound-tls-to-clickhouse-server-with-openssl-s_server) | |
| Outbound TLS to S3 (`openssl s_server`) | [_TBD_](#outbound-tls-to-s3-endpoint-with-openssl-s_server) | |
| ACVP Tests | [_TBD_](#acvp-tests) | |

## Scope

The scope of testing the `clickhouse-backup` FIPS-compatible release SHALL be defined as follows.

### Test Environment

The following artifacts and tools will be used:

* FIPS `clickhouse-backup` binary built with `GOFIPS140=v1.0.0` (`clickhouse-backup-fips`).
* FIPS-compatible Altinity ClickHouse server image: `altinity/clickhouse-server:25.3.8.30001.altinityfips`.
* Non-FIPS Altinity ClickHouse server image: `altinity/clickhouse-server:25.8.16.10002.altinitystable`.
* `openssl` CLI tool on the test host for TLS client and server probes.

### Automated FIPS Regression Tests

All FIPS tests below SHALL be executed as automated TestFlows scenarios. The following test suites SHALL be executed.

#### Connectivity Against ClickHouse FIPS and Non-FIPS Servers

Results:

* TBD

Check that `clickhouse-backup-fips` connects to both versions of the ClickHouse server.

Run the FIPS `clickhouse-backup` against:

* FIPS-compatible Altinity ClickHouse server `altinity/clickhouse-server:25.3.8.30001.altinityfips` over the secure native TCP port `9440`.
* Non-FIPS Altinity ClickHouse server `altinity/clickhouse-server:25.8.16.10002.altinitystable` over the standard native TCP port `9000`.

Expected result:

* In both cases `clickhouse-backup-fips tables` returns the list of tables without TLS or authentication errors and exits with code `0`.

#### `clickhouse-backup-fips --version` Output

Results:

* TBD

Check that the FIPS-built binary reports the FIPS 140-3 module as active in its version output, and that the binary itself is built with `GOFIPS140=v1.0.0`.

Run:

```
clickhouse-backup-fips --version
go version -m $(which clickhouse-backup-fips)
```

Expected result:

* `--version` output contains the line `FIPS 140-3: true` (this corresponds to Go's `crypto/fips140.Enabled()` returning true at runtime).
* `go version -m` output contains `build	GOFIPS140=v1.0.0`.

#### GODEBUG `fips140` Modes

Results:

* TBD

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

#### FIPS Integrity Self-test Failure on Tampered Binary

Results:

* TBD

Check that the FIPS startup integrity self-test stops the binary if the FIPS module bytes have been modified.

Take a copy of `clickhouse-backup-fips`, corrupt its `.go.fipsinfo` checksum section, and try to run the tampered copy.

Expected result:

* The tampered binary panics on startup with `panic: fips140: verification mismatch` and exits with a non-zero exit code.
* The unmodified original binary continues to work normally.

#### Forced CAST Failures

Results:

* TBD

Check that the FIPS module refuses to start if any startup self-test fails.

Run the FIPS binary with the `GODEBUG=failfipscast` hook, substituting one self-test name at a time, for example:

```
GODEBUG=failfipscast=SHA2-256,fips140=on clickhouse-backup-fips --version
```

`SHA2-256` in the command above can be replaced with any name from the list below:

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

* The process exits with a non-zero code.
* The output contains `fatal error: FIPS 140-3 self-test failed: <NAME>: simulated CAST failure`.

How to obtain and refresh this list: 

* Open `$(go env GOROOT)/src/crypto/internal/fips140test/cast_test.go` and copy the entries from the `allCASTs` slice. 

* The list should be refreshed when the Go version used to build `clickhouse-backup-fips` is upgraded, because new algorithms may add/rename/remove entries.

#### Inbound TLS — REST API With `openssl s_client`

Results:

* TBD

Check that the REST API listener of `clickhouse-backup-fips server` accepts only FIPS-approved TLS handshakes from clients.

Start `clickhouse-backup-fips server` with `GODEBUG=fips140=only`, generate a local CA and server certificate, and connect with `openssl s_client`. 

Cipher names below are taken from the official Altinity FIPS configuration documentation (<https://docs.altinity.com/altinitystablebuilds/fips-compatible-altinity-builds/#configuration-of-altinity-stable-builds-for-fips-compatible-operation>).

FIPS-approved profiles (handshake MUST succeed):

* `openssl s_client -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256` — expected result: handshake succeeds (`CONNECTION ESTABLISHED`).
* `openssl s_client -tls1_3 -ciphersuites TLS_AES_256_GCM_SHA384` — expected result: handshake succeeds.
* `openssl s_client -tls1_2 -cipher ECDHE-RSA-AES128-GCM-SHA256` — expected result: handshake succeeds.

Non-FIPS profiles (handshake MUST be rejected):

* `openssl s_client -tls1_3 -ciphersuites TLS_CHACHA20_POLY1305_SHA256` — expected result: handshake is rejected (`alert handshake failure` or equivalent).
* `openssl s_client -tls1_2 -cipher ECDHE-RSA-CHACHA20-POLY1305` — expected result: handshake is rejected.
* `openssl s_client -tls1_2 -cipher RC4-SHA` — expected result: handshake is rejected (legacy/weak cipher).
* `openssl s_client -tls1_2 -cipher DES-CBC3-SHA` — expected result: handshake is rejected (legacy/weak cipher).
* `openssl s_client -tls1` — expected result: handshake is rejected (TLSv1.0 is below the FIPS minimum protocol version).
* `openssl s_client -tls1_1` — expected result: handshake is rejected (TLSv1.1 is below the FIPS minimum protocol version).

#### Outbound TLS to ClickHouse Server With `openssl s_server`

Results:

* TBD

Check that `clickhouse-backup-fips` refuses to connect to a ClickHouse-style TLS endpoint that offers only non-FIPS ciphers.

Start a local `openssl s_server` on the secure native ClickHouse port `9440` with a chosen cipher profile, then run `clickhouse-backup-fips` with `GODEBUG=fips140=only` and a config that points at this endpoint. Cipher names below come from the official Altinity FIPS configuration documentation.

FIPS-approved profiles (handshake MUST be accepted by `clickhouse-backup-fips` policy):

* `openssl s_server -tls1_2 -cipher ECDHE-RSA-AES128-GCM-SHA256` — expected result: TLS handshake is not rejected by the FIPS policy.
* `openssl s_server -tls1_2 -cipher ECDHE-RSA-AES256-GCM-SHA384` — expected result: TLS handshake is not rejected by the FIPS policy.
* `openssl s_server -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256` — expected result: TLS handshake is not rejected by the FIPS policy.
* `openssl s_server -tls1_3 -ciphersuites TLS_AES_256_GCM_SHA384` — expected result: TLS handshake is not rejected by the FIPS policy.

Non-FIPS profiles (handshake MUST be rejected):

* `openssl s_server -tls1_2 -cipher ECDHE-RSA-CHACHA20-POLY1305` — expected result: `clickhouse-backup-fips` fails with `remote error: tls: handshake failure` and `openssl s_server` reports `no shared cipher`.
* `openssl s_server -tls1_2 -cipher DHE-RSA-AES256-GCM-SHA384` — expected result: handshake is rejected as above.
* `openssl s_server -tls1_3 -ciphersuites TLS_CHACHA20_POLY1305_SHA256` — expected result: handshake is rejected as above.

#### Outbound TLS to S3 Endpoint With `openssl s_server`

Results:

* TBD

Check that `clickhouse-backup-fips` enforces the same TLS policy on outbound HTTPS to an S3-style remote storage endpoint.

Point `s3.endpoint` in the `clickhouse-backup` config to a local `openssl s_server` on `127.0.0.1:9443` with a chosen cipher profile, then run `clickhouse-backup-fips list remote` with `GODEBUG=fips140=only`. Cipher names below come from the official Altinity FIPS configuration documentation.

FIPS-approved profiles (handshake MUST be accepted by `clickhouse-backup-fips` policy):

* `openssl s_server -tls1_2 -cipher ECDHE-RSA-AES128-GCM-SHA256` — expected result: TLS handshake is not rejected by the FIPS policy (downstream HTTP/auth errors from `openssl s_server -www` are acceptable, since this server is not a real S3 API).
* `openssl s_server -tls1_2 -cipher ECDHE-RSA-AES256-GCM-SHA384` — expected result: same as above.
* `openssl s_server -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256` — expected result: same as above.
* `openssl s_server -tls1_3 -ciphersuites TLS_AES_256_GCM_SHA384` — expected result: same as above.

Non-FIPS profiles (handshake MUST be rejected):

* `openssl s_server -tls1_2 -cipher ECDHE-RSA-CHACHA20-POLY1305` — expected result: `clickhouse-backup-fips` fails with `remote error: tls: handshake failure` / `no shared cipher`.
* `openssl s_server -tls1_2 -cipher DHE-RSA-AES256-GCM-SHA384` — expected result: handshake is rejected as above.
* `openssl s_server -tls1_3 -ciphersuites TLS_CHACHA20_POLY1305_SHA256` — expected result: handshake is rejected as above.

#### ACVP Tests

Results:

* TBD

Run the ACVP (Automated Cryptographic Validation Protocol) wrapper bundled with `clickhouse-backup`. This is a required part of every FIPS test run.

Invoke `pkg/acvpwrapper/run.sh` against the FIPS-built binary.

Expected result:

* The wrapper runs the algorithm test vectors and exits successfully with no failures across the run.
