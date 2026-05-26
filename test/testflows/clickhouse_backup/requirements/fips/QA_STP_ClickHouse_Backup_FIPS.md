# QA-STP ClickHouse Backup FIPS Compatibility Release
# Software Test Plan

(c) 2026 Altinity Inc. All Rights Reserved.

**Author:** vsviderskyi

**Date:** May 19, 2026

## Execution Summary

**Completed:**

**Build Report:**

*

**Summary:**

Started to execute test plan on TBD and ended on TBD.

## Table of Contents

* 1 [Introduction](#introduction)
* 2 [Timeline](#timeline)
* 3 [Configuration Requirements](#configuration-requirements)
* 4 [Build Verification](#build-verification)
* 5 [Human Resources And Assignments](#human-resources-and-assignments)
* 6 [Release Notes](#release-notes)
* 7 [FIPS Configuration](#fips-configuration)
* 8 [FIPS-Compatible `clickhouse-backup-fips` Configuration](#fips-compatible-clickhouse-backup-fips-configuration)
* 9 [Summary](#summary)
* 10 [Test Environment](#test-environment)
* 11 [Inputs and Outputs of `clickhouse-backup-fips`](#inputs-and-outputs-of-clickhouse-backup-fips)
* 12 [Connectivity Against ClickHouse FIPS and Non-FIPS Servers](#connectivity-against-clickhouse-fips-and-non-fips-servers)
* 13 [`clickhouse-backup-fips --version` Output](#clickhouse-backup-fips---version-output)
* 14 [GODEBUG `fips140` Modes](#godebug-fips140-modes)
* 15 [FIPS Integrity Self-test Failure on Tampered Binary](#fips-integrity-self-test-failure-on-tampered-binary)
* 16 [Forced CAST Failures](#forced-cast-failures)
* 17 [Inbound TLS — REST API With `openssl s_client`](#inbound-tls-rest-api-with-openssl-s_client)
* 18 [Outbound TLS to ClickHouse Server With `openssl s_server`](#outbound-tls-to-clickhouse-server-with-openssl-s_server)
* 19 [Outbound TLS to S3 Endpoint With `openssl s_server`](#outbound-tls-to-s3-endpoint-with-openssl-s_server)
* 20 [ACVP Tests](#acvp-tests)
* 21 [Server Listening-Port Assertion](#server-listening-port-assertion)
* 22 [Connection to FIPS ClickHouse with Non-FIPS Configuration](#connection-to-fips-clickhouse-with-non-fips-configuration)
* 23 [Outbound TLS to Non-FIPS ClickHouse with Cipher Profile](#outbound-tls-to-non-fips-clickhouse-with-cipher-profile)

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

## Configuration Requirements

## Build Verification

**Objective:** Verify binaries are FIPS builds and linked to Go Cryptographic Module v1.0.0.

**Certificates:**
- [CMVP #5247](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5247)
- [CAVP A6650](https://csrc.nist.gov/projects/cryptographic-algorithm-validation-program/details?product=19371)


## Human Resources And Assignments

The following team members SHALL be dedicated to this release:

* Vitalii Sviderskyi (regression tests)
* Vitaliy Zakaznikov (manager, regression tests)
* Eugene Klimov (clickhouse-backup, FIPS build and Docker image)


## Release Notes

* https://github.com/Altinity/clickhouse-backup/blob/master/ChangeLog.md
* https://docs.altinity.com/altinitystablebuilds/fips-compatible-altinity-builds/

## FIPS Configuration

For FIPS-compatible operation, users should:

* Use the FIPS-built `clickhouse-backup-fips` binary.
* Configure TLS for FIPS-compatible ClickHouse server using TLSv1.2 and TLSv1.3 only, with the FIPS-approved cipher suites published in the Altinity FIPS documentation (<https://docs.altinity.com/altinitystablebuilds/fips-compatible-altinity-builds/#configuration-of-altinity-stable-builds-for-fips-compatible-operation>):
    * TLSv1.2 `cipherList`: `ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:AES128-GCM-SHA256:AES256-GCM-SHA384`
    * TLSv1.3 `cipherSuites`: `TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384`

`clickhouse-backup-fips` can also be used against non-FIPS ClickHouse servers.

## FIPS-Compatible `clickhouse-backup-fips` Configuration

A `clickhouse-backup-fips` configuration is FIPS-compatible only when:

* `clickhouse.secure: true`.
* `clickhouse.port: 9440` (secure native TLS). Plain native TCP `9000` and plain HTTP `8123` MUST NOT be used against a FIPS-configured ClickHouse server.
* `api.secure: true` plus a valid certificate / private key when running `clickhouse-backup-fips server`.
* For S3-compatible remote storage, `s3.endpoint` is left empty and `s3.region` is set, so the SDK targets the AWS FIPS hostname `s3-fips.<region>.amazonaws.com`.

The canonical FIPS client-side configurations used by the regression suite are:

* `test/testflows/clickhouse_backup/configs/backup/fips/config-fips-connectivity-fips-server.yml` — secure native TCP `9440` against the FIPS ClickHouse server.
* `test/testflows/clickhouse_backup/configs/backup/fips/config-fips-api-tls.yml` — REST API listener on `7172` with `api.secure: true`.


## Summary

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
* Outbound to ClickHouse: secure native TCP port `9440` (`clickhouse.secure: true`, `clickhouse.port: 9440`). Plain native TCP `9000` is only used by the explicit non-FIPS interoperability scenario.
* Outbound to S3-compatible storage: HTTPS to the AWS FIPS hostname `s3-fips.<region>.amazonaws.com:443` when `s3.endpoint` is empty and `s3.region` is set.

The [Server Listening-Port Assertion](#server-listening-port-assertion) subsection below describes how the inbound surface is verified.

## Connectivity Against ClickHouse FIPS and Non-FIPS Servers

Check that `clickhouse-backup-fips` connects to both versions of the ClickHouse server.

Run the FIPS `clickhouse-backup` against:

* FIPS-compatible Altinity ClickHouse server `altinity/clickhouse-server:25.3.8.30001.altinityfips` over the secure native TCP port `9440`.
* Non-FIPS Altinity ClickHouse server `altinity/clickhouse-server:25.8.16.10002.altinitystable` over the standard native TCP port `9000`.

Expected result:

* In both cases `clickhouse-backup-fips tables` returns the list of tables without TLS or authentication errors and exits with code `0`.

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

## Inbound TLS — REST API With `openssl s_client`

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

## Outbound TLS to ClickHouse Server With `openssl s_server`


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

## Outbound TLS to S3 Endpoint With `openssl s_server`


Check that `clickhouse-backup-fips` enforces the same TLS policy on outbound HTTPS to an S3-style remote storage endpoint.

Three FIPS-specific items shape this setup:

* Leave `s3.endpoint` blank and set `s3.region` (e.g. `us-east-1`); the SDK targets the AWS FIPS hostname `s3-fips.us-east-1.amazonaws.com`.
* Name the `openssl s_server` container after that hostname and have it listen on port `443`.
* Set `s3.secret_key` to at least 10 characters and `s3.disable_cert_verification: true`.

Run `clickhouse-backup-fips list remote` with `GODEBUG=fips140=only`. Cipher names below come from the official Altinity FIPS configuration documentation.

FIPS-approved profiles (handshake MUST be accepted by `clickhouse-backup-fips` policy):

* `openssl s_server -tls1_2 -cipher ECDHE-RSA-AES128-GCM-SHA256` — expected result: TLS handshake is not rejected by the FIPS policy (downstream HTTP / S3-protocol errors from `openssl s_server -www` are acceptable, since this server is not a real S3 API).
* `openssl s_server -tls1_2 -cipher ECDHE-RSA-AES256-GCM-SHA384` — expected result: same as above.
* `openssl s_server -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256` — expected result: same as above.
* `openssl s_server -tls1_3 -ciphersuites TLS_AES_256_GCM_SHA384` — expected result: same as above.

Non-FIPS profiles (handshake MUST be rejected):

* `openssl s_server -tls1_2 -cipher ECDHE-RSA-CHACHA20-POLY1305` — expected result: `clickhouse-backup-fips` fails with `remote error: tls: handshake failure` / `no shared cipher`.
* `openssl s_server -tls1_2 -cipher DHE-RSA-AES256-GCM-SHA384` — expected result: handshake is rejected as above.
* `openssl s_server -tls1_3 -ciphersuites TLS_CHACHA20_POLY1305_SHA256` — expected result: handshake is rejected as above.

## ACVP Tests

Run the ACVP (Automated Cryptographic Validation Protocol) wrapper bundled with `clickhouse-backup`. This is a required part of every FIPS test run.

Invoke `pkg/acvpwrapper/run.sh` against the FIPS-built binary.

Expected result:

* The wrapper runs the algorithm test vectors and exits successfully with no failures across the run.

## Server Listening-Port Assertion

Check that `clickhouse-backup-fips server`, when started with the FIPS-compatible REST API config (`api.secure: true`, no plain ports configured), opens only the FIPS-approved TLS listener.

Steps:

1. Start `clickhouse-backup-fips server` with `config-fips-api-tls.yml` and `GODEBUG=fips140=only` inside the FIPS backup container.
2. Capture the server PID from the PID file written by the test harness.
3. Read `/proc/<PID>/net/tcp` and `/proc/<PID>/net/tcp6` inside the container and extract every row whose state column equals `0A` (LISTEN). The FIPS regression container does not install `iproute2`, so `ss -ltnp` / `netstat -ltnp` are not available; `/proc/<PID>/net/tcp` is the network namespace view, but the container only runs `sleep infinity` plus the test-launched FIPS server, so any LISTEN socket in the namespace is necessarily one the FIPS server opened.

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

* FIPS-approved profile (`ECDHE-RSA-AES128-GCM-SHA256` / `TLS_AES_128_GCM_SHA256`): `tables` exits 0 and lists the server's tables (positive case).
* Non-FIPS profile (`ECDHE-RSA-CHACHA20-POLY1305` / `TLS_CHACHA20_POLY1305_SHA256`): `clickhouse-backup-fips` fails with a TLS handshake-failure marker (`remote error: tls: handshake failure` / `no shared cipher` / `tls: protocol version not supported`).
