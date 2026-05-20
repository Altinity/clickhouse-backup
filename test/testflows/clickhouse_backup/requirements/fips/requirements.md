# QA-SRS013 ClickHouse Backup Utility FIPS Compatibility
# Software Requirements Specification

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Terminology](#terminology)
    * 3.1 [SRS](#srs)
    * 3.2 [FIPS 140-3](#fips-140-3)
    * 3.3 [CAST](#cast)
    * 3.4 [ACVP definition](#acvp-definition)
    * 3.5 [GODEBUG](#godebug)
    * 3.6 [FIPS-compatible TLS connection](#fips-compatible-tls-connection)
    * 3.7 [clickhouse-backup-fips](#clickhouse-backup-fips)
* 4 [Requirements](#requirements)
    * 4.1 [General](#general)
        * 4.1.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GoCryptographicModule](#rqsrs-013clickhousebackuputilityfipsgocryptographicmodule)
        * 4.1.2 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Build.GOFIPS140](#rqsrs-013clickhousebackuputilityfipsbuildgofips140)
        * 4.1.3 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Binary](#rqsrs-013clickhousebackuputilityfipsbinary)
        * 4.1.4 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.DockerImage](#rqsrs-013clickhousebackuputilityfipsdockerimage)
        * 4.1.5 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.TLSProtocolVersions](#rqsrs-013clickhousebackuputilityfipsapprovedtlsprotocolversions)
        * 4.1.6 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv12.Approved](#rqsrs-013clickhousebackuputilityfipsapprovedciphersuitestlsv12approved)
        * 4.1.7 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv13.Approved](#rqsrs-013clickhousebackuputilityfipsapprovedciphersuitestlsv13approved)
    * 4.2 [Connectivity](#connectivity)
        * 4.2.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Connectivity.FIPSEndpoint](#rqsrs-013clickhousebackuputilityfipsconnectivityfipsendpoint)
        * 4.2.2 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Connectivity.NonFIPSEndpoint](#rqsrs-013clickhousebackuputilityfipsconnectivitynonfipsendpoint)
    * 4.3 [Version Output](#version-output)
        * 4.3.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Version.Status](#rqsrs-013clickhousebackuputilityfipsversionstatus)
        * 4.3.2 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Version.BuildSetting](#rqsrs-013clickhousebackuputilityfipsversionbuildsetting)
    * 4.4 [GODEBUG fips140 Modes](#godebug-fips140-modes)
        * 4.4.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.Unset](#rqsrs-013clickhousebackuputilityfipsgodebugunset)
        * 4.4.2 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.On](#rqsrs-013clickhousebackuputilityfipsgodebugon)
        * 4.4.3 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.Only](#rqsrs-013clickhousebackuputilityfipsgodebugonly)
    * 4.5 [Startup Integrity Self-Tests](#startup-integrity-self-tests)
        * 4.5.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.Integrity](#rqsrs-013clickhousebackuputilityfipsselftestintegrity)
        * 4.5.2 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.TamperedBinary](#rqsrs-013clickhousebackuputilityfipsselftesttamperedbinary)
        * 4.5.3 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.CAST.ForcedFailure](#rqsrs-013clickhousebackuputilityfipsselftestcastforcedfailure)
        * 4.5.4 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.CAST.Coverage](#rqsrs-013clickhousebackuputilityfipsselftestcastcoverage)
    * 4.6 [Inbound TLS — REST API](#inbound-tls-rest-api)
        * 4.6.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.ApprovedCiphers](#rqsrs-013clickhousebackuputilityfipstlsinboundrestapiapprovedciphers)
        * 4.6.2 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.NonApprovedCiphers.Reject](#rqsrs-013clickhousebackuputilityfipstlsinboundrestapinonapprovedciphersreject)
        * 4.6.3 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.LegacyProtocols.Reject](#rqsrs-013clickhousebackuputilityfipstlsinboundrestapilegacyprotocolsreject)
    * 4.7 [Outbound TLS — ClickHouse Endpoint](#outbound-tls-clickhouse-endpoint)
        * 4.7.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.ClickHouseEndpoint.Ciphers.Approved](#rqsrs-013clickhousebackuputilityfipstlsoutboundclickhouseendpointciphersapproved)
        * 4.7.2 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.ClickHouseEndpoint.NonApprovedCiphers.Reject](#rqsrs-013clickhousebackuputilityfipstlsoutboundclickhouseendpointnonapprovedciphersreject)
    * 4.8 [Outbound TLS — S3 Endpoint](#outbound-tls-s3-endpoint)
        * 4.8.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.S3.Ciphers.Approved](#rqsrs-013clickhousebackuputilityfipstlsoutbounds3ciphersapproved)
        * 4.8.2 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.S3.NonApprovedCiphers.Reject](#rqsrs-013clickhousebackuputilityfipstlsoutbounds3nonapprovedciphersreject)
    * 4.9 [ACVP](#acvp)
        * 4.9.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.ACVP.Wrapper](#rqsrs-013clickhousebackuputilityfipsacvpwrapper)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software hosted in the [GitHub Repository].
All the updates are tracked using the [Revision History].

## Introduction

This [SRS] covers the requirements for the FIPS-compatible build of the [clickhouse-backup] utility,
distributed as `clickhouse-backup-fips`, that supports easy [ClickHouse] backup and restore
with cloud storage support while operating under the [Go FIPS 140-3 Cryptographic Module].

The FIPS-compatible build SHALL provide the same functional capabilities as the standard
[clickhouse-backup] utility, while additionally enforcing approved cryptographic algorithms.

## Terminology

### SRS

Software Requirements Specification.

### FIPS 140-3

Federal Information Processing Standard Publication 140-3, the U.S. government computer security standard for validating cryptographic modules.

### CAST

Conditional Algorithm Self-Test, a per-algorithm power-on self-test required by FIPS 140-3.

### ACVP definition

Automated Cryptographic Validation Protocol, the NIST mechanism for automated cryptographic
algorithm conformance testing.

### GODEBUG

The Go runtime debug variable that controls runtime behavior of the Go FIPS 140-3 module
via the `fips140` key (`off`, `on`, `only`).

### FIPS-compatible TLS connection

A FIPS-compatible TLS connection for [clickhouse-backup-fips] SHALL be defined as:

* TLS protocol version TLSv1.2 or TLSv1.3.
* A cipher suite from the approved set defined by this SRS.
* No protocol or cipher downgrade to a non-approved profile.

### clickhouse-backup-fips

The FIPS-compatible build of the [clickhouse-backup] utility produced with `GOFIPS140=v1.0.0`.

## Requirements

### General

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GoCryptographicModule
version: 1.0

The [clickhouse-backup-fips] binary SHALL use the [Go FIPS 140-3 Cryptographic Module] for all cryptographic operations.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Build.GOFIPS140
version: 1.0

The [clickhouse-backup-fips] binary SHALL be built with the Go build setting `GOFIPS140=v1.0.0`, which SHALL be observable in the output of `go version -m $(which clickhouse-backup-fips)` as the line `build	GOFIPS140=v1.0.0`.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Binary
version: 1.0

The FIPS-compatible build of the [clickhouse-backup] utility SHALL be distributed as a separate
binary named `clickhouse-backup-fips`, distinct from the standard `clickhouse-backup` binary.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.DockerImage
version: 1.0

The [clickhouse-backup-fips] binary SHALL be distributed as a dedicated Docker image that SHALL run with strict FIPS enforcement (`GODEBUG=fips140=only`) by default.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.TLSProtocolVersions
version: 1.0

The [clickhouse-backup-fips] binary SHALL accept and initiate [TLS] handshakes only over
[TLS] protocol versions TLSv1.2 and TLSv1.3; handshakes attempting TLSv1.0 or TLSv1.1 SHALL
be rejected.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv12.Approved
version: 1.0

The [clickhouse-backup-fips] binary SHALL accept and initiate TLSv1.2 handshakes using only
the following FIPS-approved cipher suites:

* `ECDHE-RSA-AES128-GCM-SHA256`
* `ECDHE-RSA-AES256-GCM-SHA384`
* `ECDHE-ECDSA-AES128-GCM-SHA256`
* `ECDHE-ECDSA-AES256-GCM-SHA384`
* `AES128-GCM-SHA256`
* `AES256-GCM-SHA384`

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv13.Approved
version: 1.0

The [clickhouse-backup-fips] binary SHALL accept and initiate TLSv1.3 handshakes using only
the following FIPS-approved cipher suites:

* `TLS_AES_128_GCM_SHA256`
* `TLS_AES_256_GCM_SHA384`

### Connectivity

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Connectivity.FIPSEndpoint
version: 1.0

The [clickhouse-backup-fips] binary SHALL successfully connect to a FIPS-configured
ClickHouse endpoint over secure native [TLS] TCP port `9440` and SHALL return the list of tables
from `clickhouse-backup-fips tables` without [TLS] or authentication errors, exiting with code `0`.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Connectivity.NonFIPSEndpoint
version: 1.0

The [clickhouse-backup-fips] binary SHALL successfully connect to a non-FIPS ClickHouse endpoint
over standard native TCP port `9000` and SHALL return the list of tables from
`clickhouse-backup-fips tables`, exiting with code `0`.

### Version Output

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Version.Status
version: 1.0

The `clickhouse-backup-fips --version` output SHALL contain the line `FIPS 140-3: true`,
corresponding to [`crypto/fips140.Enabled()`][`crypto/fips140` package] returning `true` at runtime.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Version.BuildSetting
version: 1.0

The output of `go version -m $(which clickhouse-backup-fips)` SHALL contain the line
`build	GOFIPS140=v1.0.0`, confirming the binary was built against the Go FIPS 140-3 module.

### GODEBUG fips140 Modes

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.Unset
version: 1.0

When `GODEBUG` is not set, the [clickhouse-backup-fips] binary SHALL operate with FIPS 140-3 mode
enabled by build-time default, `--version` SHALL report `FIPS 140-3: true`, and the basic
`clickhouse-backup-fips tables` command SHALL return the list of tables from a FIPS-configured
ClickHouse endpoint.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.On
version: 1.0

When started with `GODEBUG=fips140=on`, the [clickhouse-backup-fips] binary SHALL operate with
FIPS 140-3 mode enabled without strict enforcement, `--version` SHALL report `FIPS 140-3: true`,
and the basic `clickhouse-backup-fips tables` command SHALL return the list of tables from a
FIPS-configured ClickHouse endpoint.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.Only
version: 1.0

When started with `GODEBUG=fips140=only`, the [clickhouse-backup-fips] binary SHALL operate with
strict FIPS 140-3 enforcement so that any non-approved cryptographic operation triggers an error
or panic, `--version` SHALL report `FIPS 140-3: true`, and `clickhouse-backup-fips tables` against
an approved [TLS] configuration SHALL return the list of tables.

### Startup Integrity Self-Tests

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.Integrity
version: 1.0

A copy of the [clickhouse-backup-fips] binary SHALL execute the FIPS 140-3 integrity self-test
over the [Go FIPS 140-3 Cryptographic Module] bytes, and SHALL refuse to start if the self-test fails.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.TamperedBinary
version: 1.0

A copy of the [clickhouse-backup-fips] binary whose `.go.fipsinfo` checksum section has been modified SHALL panic on startup with the message `panic: fips140: verification mismatch` and exit with a non-zero exit code.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.CAST.ForcedFailure
version: 1.0

When started with `GODEBUG=failfipscast=<NAME>,fips140=on`, where `<NAME>` is any
[CAST] algorithm name from the `allCASTs` slice in `crypto/internal/fips140test/cast_test.go`
of the Go release used to build the binary, the [clickhouse-backup-fips] process SHALL exit
with a non-zero code and SHALL show `fatal error: FIPS 140-3 self-test failed: <NAME>: simulated CAST failure`.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.CAST.Coverage
version: 1.0

The forced-[CAST] test described above SHALL be exercised for every algorithm name listed in
the `allCASTs` slice of the Go FIPS test suite (`crypto/internal/fips140test/cast_test.go`), including (but not limited to): 
* `AES-CBC`, 
* `CTR_DRBG`, 
* `CounterKDF`,
* `DetECDSA P-256 SHA2-512 sign`, 
* `ECDH PCT`, 
* `ECDSA P-256 SHA2-512 sign and verify`, 
* `ECDSA PCT`,
* `Ed25519 sign and verify`, 
* `Ed25519 sign and verify PCT`, 
* `HKDF-SHA2-256`, 
* `HMAC-SHA2-256`,
* `KAS-ECC-SSC P-256`, 
* `ML-DSA sign and verify PCT`, 
* `ML-DSA-44`, 
* `ML-KEM PCT`, 
* `ML-KEM-768`,
* `PBKDF2`, 
* `RSA sign and verify PCT`, 
* `RSASSA-PKCS-v1.5 2048-bit sign and verify`, 
* `SHA2-256`,
* `SHA2-512`, 
* `TLSv1.2-SHA2-256`, 
* `TLSv1.3-SHA2-256`, 
* `cSHAKE128`. 
The list should be refreshed when the Go version used to build [clickhouse-backup-fips] is upgraded.

### Inbound TLS — REST API

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.ApprovedCiphers
version: 1.0

The REST API listener of `clickhouse-backup-fips server`, when started with `GODEBUG=fips140=only`, SHALL accept [TLS] handshakes from clients using the following FIPS-approved profiles:

* TLSv1.3 with `TLS_AES_128_GCM_SHA256`
* TLSv1.3 with `TLS_AES_256_GCM_SHA384`
* TLSv1.2 with `ECDHE-RSA-AES128-GCM-SHA256`

For each of the above, an `openssl s_client` connection SHALL complete with `CONNECTION ESTABLISHED`.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.NonApprovedCiphers.Reject
version: 1.0

The REST API listener of `clickhouse-backup-fips server`, when started with `GODEBUG=fips140=only`, SHALL reject [TLS] handshakes from clients using non-FIPS-approved cipher suites, including:

* TLSv1.3 with `TLS_CHACHA20_POLY1305_SHA256`
* TLSv1.2 with `ECDHE-RSA-CHACHA20-POLY1305`
* TLSv1.2 with `RC4-SHA`
* TLSv1.2 with `DES-CBC3-SHA`

For each of the above, an `openssl s_client` connection SHALL fail with `alert handshake failure`
or an equivalent rejection.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.LegacyProtocols.Reject
version: 1.0

The REST API listener of `clickhouse-backup-fips server`, when started with `GODEBUG=fips140=only`,
SHALL reject [TLS] handshakes that negotiate TLSv1.0 (`openssl s_client -tls1`) or
TLSv1.1 (`openssl s_client -tls1_1`), since these protocol versions do not follow the FIPS requirements.

### Outbound TLS — ClickHouse Endpoint

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.ClickHouseEndpoint.Ciphers.Approved
version: 1.0

The [clickhouse-backup-fips] binary, when started with `GODEBUG=fips140=only` and pointed at a ClickHouse [TLS] endpoint on secure native port `9440`, SHALL successfully complete the
[TLS] handshake when the remote endpoint offers any of the following FIPS-approved profiles:

* TLSv1.2 with `ECDHE-RSA-AES128-GCM-SHA256`
* TLSv1.2 with `ECDHE-RSA-AES256-GCM-SHA384`
* TLSv1.3 with `TLS_AES_128_GCM_SHA256`
* TLSv1.3 with `TLS_AES_256_GCM_SHA384`

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.ClickHouseEndpoint.NonApprovedCiphers.Reject
version: 1.0

The [clickhouse-backup-fips] binary, when started with `GODEBUG=fips140=only` and pointed at a ClickHouse [TLS] endpoint on secure native port `9440`, SHALL refuse to complete the [TLS] handshake when the remote endpoint offers only non-FIPS-approved cipher suites, including:

* TLSv1.2 with `ECDHE-RSA-CHACHA20-POLY1305`
* TLSv1.2 with `DHE-RSA-AES256-GCM-SHA384`
* TLSv1.3 with `TLS_CHACHA20_POLY1305_SHA256`

For each of the above, [clickhouse-backup-fips] SHALL fail with `remote error: tls: handshake failure`
and the peer (e.g. `openssl s_server`) SHALL report `no shared cipher`.

### Outbound TLS — S3 Endpoint

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.S3.Ciphers.Approved
version: 1.0

The [clickhouse-backup-fips] binary, when started with `GODEBUG=fips140=only` and configured with an S3 remote storage endpoint (`s3.endpoint`) pointed at a [TLS] server on `127.0.0.1:9443`, SHALL successfully complete the [TLS] handshake when the remote endpoint offers any of the following FIPS-approved profiles:

* TLSv1.2 with `ECDHE-RSA-AES128-GCM-SHA256`
* TLSv1.2 with `ECDHE-RSA-AES256-GCM-SHA384`
* TLSv1.3 with `TLS_AES_128_GCM_SHA256`
* TLSv1.3 with `TLS_AES_256_GCM_SHA384`.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.S3.NonApprovedCiphers.Reject
version: 1.0

The [clickhouse-backup-fips] binary, when started with `GODEBUG=fips140=only` and configured with an S3-style remote storage endpoint (`s3.endpoint`) pointed at a [TLS] server on `127.0.0.1:9443`, SHALL refuse to complete the [TLS] handshake when the remote endpoint offers only non-FIPS-approved cipher suites, including:

* TLSv1.2 with `ECDHE-RSA-CHACHA20-POLY1305`
* TLSv1.2 with `DHE-RSA-AES256-GCM-SHA384`
* TLSv1.3 with `TLS_CHACHA20_POLY1305_SHA256`

For each of the above, `clickhouse-backup-fips list remote` SHALL fail with
`remote error: tls: handshake failure` or `no shared cipher`.

### ACVP

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.ACVP.Wrapper
version: 1.0

The [ACVP] wrapper bundled with [clickhouse-backup] at `pkg/acvpwrapper/run.sh` SHALL execute the algorithm test vectors against the [clickhouse-backup-fips] binary and SHALL execute successfully with zero failures across the entire run.

## References

* **clickhouse-backup:** https://github.com/Altinity/clickhouse-backup
* **ClickHouse:** https://clickhouse.tech
* **GitHub Repository:** https://github.com/Altinity/clickhouse-backup/blob/master/test/testflows/clickhouse_backup/requirements/fips/requirements.md
* **Revision History:** https://github.com/Altinity/clickhouse-backup/commits/master/test/testflows/clickhouse_backup/requirements/fips/requirements.md
* **Git:** https://git-scm.com/
* **Go `crypto/fips140` Package Reference (`Enabled`, `Enforced`, `Version`):** https://pkg.go.dev/crypto/fips140#Enabled
* **wolfCrypt FIPS 140-3 — wolfSSL FIPS Licensing Overview:** https://www.wolfssl.com/license/fips/
[SRS]: #srs
[clickhouse-backup]: https://github.com/Altinity/clickhouse-backup
[clickhouse-backup-fips]: #clickhouse-backup-fips
[ClickHouse]: https://clickhouse.tech
[TLS]: https://datatracker.ietf.org/doc/html/rfc8446
[CAST]: #cast
[ACVP]: #acvp
[GitHub Repository]: https://github.com/Altinity/clickhouse-backup/blob/master/test/testflows/clickhouse_backup/requirements/fips/requirements.md
[Revision History]: https://github.com/Altinity/clickhouse-backup/commits/master/test/testflows/clickhouse_backup/requirements/fips/requirements.md
[Git]: https://git-scm.com/
[GitHub]: https://github.com/
[`crypto/fips140` package]: https://pkg.go.dev/crypto/fips140#Enabled
