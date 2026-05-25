# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v2.1.240306.1133530.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Specification
from testflows.core import Requirement

Heading = Specification.Heading

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GoCryptographicModule = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GoCryptographicModule',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary SHALL use the [Go FIPS 140-3 Cryptographic Module] for all cryptographic operations.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.1.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Build_GOFIPS140 = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Build.GOFIPS140',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary SHALL be built with the Go build setting `GOFIPS140=v1.0.0`, which SHALL be observable in the output of `go version -m $(which clickhouse-backup-fips)` as the line `build\tGOFIPS140=v1.0.0`.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.1.2'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Binary = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Binary',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The FIPS-compatible build of the [clickhouse-backup] utility SHALL be distributed as a separate\n'
        'binary named `clickhouse-backup-fips`, distinct from the standard `clickhouse-backup` binary.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.1.3'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_DockerImage = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.DockerImage',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary SHALL be distributed as a dedicated Docker image that SHALL run with strict FIPS enforcement (`GODEBUG=fips140=only`) by default.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.1.4'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Approved_TLSProtocolVersions = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.TLSProtocolVersions',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary SHALL accept and initiate [TLS] handshakes only over\n'
        '[TLS] protocol versions TLSv1.2 and TLSv1.3; handshakes attempting TLSv1.0 or TLSv1.1 SHALL\n'
        'be rejected.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.1.5'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Approved_CipherSuites_TLSv12_Approved = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv12.Approved',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary SHALL accept and initiate TLSv1.2 handshakes using only\n'
        'the following FIPS-approved cipher suites:\n'
        '\n'
        '* `ECDHE-RSA-AES128-GCM-SHA256`\n'
        '* `ECDHE-RSA-AES256-GCM-SHA384`\n'
        '* `ECDHE-ECDSA-AES128-GCM-SHA256`\n'
        '* `ECDHE-ECDSA-AES256-GCM-SHA384`\n'
        '* `AES128-GCM-SHA256`\n'
        '* `AES256-GCM-SHA384`\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.1.6'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Approved_CipherSuites_TLSv13_Approved = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv13.Approved',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary SHALL accept and initiate TLSv1.3 handshakes using only\n'
        'the following FIPS-approved cipher suites:\n'
        '\n'
        '* `TLS_AES_128_GCM_SHA256`\n'
        '* `TLS_AES_256_GCM_SHA384`\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.1.7'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Connectivity_FIPSEndpoint = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Connectivity.FIPSEndpoint',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary SHALL successfully connect to a FIPS-configured\n'
        'ClickHouse endpoint over secure native [TLS] TCP port `9440` and SHALL return the list of tables\n'
        'from `clickhouse-backup-fips tables` without [TLS] or authentication errors, exiting with code `0`.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.2.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Connectivity_NonFIPSEndpoint = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Connectivity.NonFIPSEndpoint',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary SHALL successfully connect to a non-FIPS ClickHouse endpoint\n'
        'over standard native TCP port `9000` and SHALL return the list of tables from\n'
        '`clickhouse-backup-fips tables`, exiting with code `0`.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.2.2'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_Status = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Version.Status',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The `clickhouse-backup-fips --version` output SHALL contain the line `FIPS 140-3: true`,\n'
        'corresponding to [`crypto/fips140.Enabled()`][`crypto/fips140` package] returning `true` at runtime.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.3.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_BuildSetting = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Version.BuildSetting',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The output of `go version -m $(which clickhouse-backup-fips)` SHALL contain the line\n'
        '`build\tGOFIPS140=v1.0.0`, confirming the binary was built against the Go FIPS 140-3 module.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.3.2'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GODEBUG_Unset = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.Unset',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'When `GODEBUG` is not set, the [clickhouse-backup-fips] binary SHALL operate with FIPS 140-3 mode\n'
        'enabled by build-time default, `--version` SHALL report `FIPS 140-3: true`, and the basic\n'
        '`clickhouse-backup-fips tables` command SHALL return the list of tables from a FIPS-configured\n'
        'ClickHouse endpoint.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.4.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GODEBUG_On = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.On',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'When started with `GODEBUG=fips140=on`, the [clickhouse-backup-fips] binary SHALL operate with\n'
        'FIPS 140-3 mode enabled without strict enforcement, `--version` SHALL report `FIPS 140-3: true`,\n'
        'and the basic `clickhouse-backup-fips tables` command SHALL return the list of tables from a\n'
        'FIPS-configured ClickHouse endpoint.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.4.2'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GODEBUG_Only = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.Only',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'When started with `GODEBUG=fips140=only`, the [clickhouse-backup-fips] binary SHALL operate with\n'
        'strict FIPS 140-3 enforcement so that any non-approved cryptographic operation triggers an error\n'
        'or panic, `--version` SHALL report `FIPS 140-3: true`, and `clickhouse-backup-fips tables` against\n'
        'an approved [TLS] configuration SHALL return the list of tables.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.4.3'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_Integrity = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.Integrity',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'A copy of the [clickhouse-backup-fips] binary SHALL execute the FIPS 140-3 integrity self-test\n'
        'over the [Go FIPS 140-3 Cryptographic Module] bytes, and SHALL refuse to start if the self-test fails.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.5.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_TamperedBinary = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.TamperedBinary',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'A copy of the [clickhouse-backup-fips] binary whose `.go.fipsinfo` checksum section has been modified SHALL panic on startup with the message `panic: fips140: verification mismatch` and exit with a non-zero exit code.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.5.2'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_CAST_ForcedFailure = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.CAST.ForcedFailure',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'When started with `GODEBUG=failfipscast=<NAME>,fips140=on`, where `<NAME>` is any\n'
        '[CAST] algorithm name from the `allCASTs` slice in `crypto/internal/fips140test/cast_test.go`\n'
        'of the Go release used to build the binary, the [clickhouse-backup-fips] process SHALL exit\n'
        'with a non-zero code and SHALL show `fatal error: FIPS 140-3 self-test failed: <NAME>: simulated CAST failure`.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.5.3'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_CAST_Coverage = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.CAST.Coverage',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The forced-[CAST] test described above SHALL be exercised for every algorithm name listed in\n'
        'the `allCASTs` slice of the Go FIPS test suite (`crypto/internal/fips140test/cast_test.go`), including (but not limited to): \n'
        '* `AES-CBC`, \n'
        '* `CTR_DRBG`, \n'
        '* `CounterKDF`,\n'
        '* `DetECDSA P-256 SHA2-512 sign`, \n'
        '* `ECDH PCT`, \n'
        '* `ECDSA P-256 SHA2-512 sign and verify`, \n'
        '* `ECDSA PCT`,\n'
        '* `Ed25519 sign and verify`, \n'
        '* `Ed25519 sign and verify PCT`, \n'
        '* `HKDF-SHA2-256`, \n'
        '* `HMAC-SHA2-256`,\n'
        '* `KAS-ECC-SSC P-256`, \n'
        '* `ML-DSA sign and verify PCT`, \n'
        '* `ML-DSA-44`, \n'
        '* `ML-KEM PCT`, \n'
        '* `ML-KEM-768`,\n'
        '* `PBKDF2`, \n'
        '* `RSA sign and verify PCT`, \n'
        '* `RSASSA-PKCS-v1.5 2048-bit sign and verify`, \n'
        '* `SHA2-256`,\n'
        '* `SHA2-512`, \n'
        '* `TLSv1.2-SHA2-256`, \n'
        '* `TLSv1.3-SHA2-256`, \n'
        '* `cSHAKE128`. \n'
        'The list should be refreshed when the Go version used to build [clickhouse-backup-fips] is upgraded.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.5.4'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_ApprovedCiphers = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.ApprovedCiphers',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The REST API listener of `clickhouse-backup-fips server`, when started with `GODEBUG=fips140=only`, SHALL accept [TLS] handshakes from clients using the following FIPS-approved profiles:\n'
        '\n'
        '* TLSv1.3 with `TLS_AES_128_GCM_SHA256`\n'
        '* TLSv1.3 with `TLS_AES_256_GCM_SHA384`\n'
        '* TLSv1.2 with `ECDHE-RSA-AES128-GCM-SHA256`\n'
        '\n'
        'For each of the above, an `openssl s_client` connection SHALL complete with `CONNECTION ESTABLISHED`.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.6.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_NonApprovedCiphers_Reject = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.NonApprovedCiphers.Reject',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The REST API listener of `clickhouse-backup-fips server`, when started with `GODEBUG=fips140=only`, SHALL reject [TLS] handshakes from clients using non-FIPS-approved cipher suites, including:\n'
        '\n'
        '* TLSv1.3 with `TLS_CHACHA20_POLY1305_SHA256`\n'
        '* TLSv1.2 with `ECDHE-RSA-CHACHA20-POLY1305`\n'
        '* TLSv1.2 with `RC4-SHA`\n'
        '* TLSv1.2 with `DES-CBC3-SHA`\n'
        '\n'
        'For each of the above, an `openssl s_client` connection SHALL fail with `alert handshake failure`\n'
        'or an equivalent rejection.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.6.2'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_LegacyProtocols_Reject = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.LegacyProtocols.Reject',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The REST API listener of `clickhouse-backup-fips server`, when started with `GODEBUG=fips140=only`,\n'
        'SHALL reject [TLS] handshakes that negotiate TLSv1.0 (`openssl s_client -tls1`) or\n'
        'TLSv1.1 (`openssl s_client -tls1_1`), since these protocol versions do not follow the FIPS requirements.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.6.3'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_ClickHouseEndpoint_Ciphers_Approved = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.ClickHouseEndpoint.Ciphers.Approved',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary, when started with `GODEBUG=fips140=only` and pointed at a ClickHouse [TLS] endpoint on secure native port `9440`, SHALL successfully complete the\n'
        '[TLS] handshake when the remote endpoint offers any of the following FIPS-approved profiles:\n'
        '\n'
        '* TLSv1.2 with `ECDHE-RSA-AES128-GCM-SHA256`\n'
        '* TLSv1.2 with `ECDHE-RSA-AES256-GCM-SHA384`\n'
        '* TLSv1.3 with `TLS_AES_128_GCM_SHA256`\n'
        '* TLSv1.3 with `TLS_AES_256_GCM_SHA384`\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.7.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_ClickHouseEndpoint_NonApprovedCiphers_Reject = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.ClickHouseEndpoint.NonApprovedCiphers.Reject',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary, when started with `GODEBUG=fips140=only` and pointed at a ClickHouse [TLS] endpoint on secure native port `9440`, SHALL refuse to complete the [TLS] handshake when the remote endpoint offers only non-FIPS-approved cipher suites, including:\n'
        '\n'
        '* TLSv1.2 with `ECDHE-RSA-CHACHA20-POLY1305`\n'
        '* TLSv1.2 with `DHE-RSA-AES256-GCM-SHA384`\n'
        '* TLSv1.3 with `TLS_CHACHA20_POLY1305_SHA256`\n'
        '\n'
        'For each of the above, [clickhouse-backup-fips] SHALL fail with `remote error: tls: handshake failure`\n'
        'and the peer (e.g. `openssl s_server`) SHALL report `no shared cipher`.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.7.2'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_S3_Ciphers_Approved = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.S3.Ciphers.Approved',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary, when started with `GODEBUG=fips140=only` and configured against an S3-compatible HTTPS endpoint as described above, SHALL NOT have its [TLS] handshake rejected by the FIPS policy when the remote endpoint offers any of the following FIPS-approved profiles:\n'
        '\n'
        '* TLSv1.2 with `ECDHE-RSA-AES128-GCM-SHA256`\n'
        '* TLSv1.2 with `ECDHE-RSA-AES256-GCM-SHA384`\n'
        '* TLSv1.3 with `TLS_AES_128_GCM_SHA256`\n'
        '* TLSv1.3 with `TLS_AES_256_GCM_SHA384`.\n'
        '\n'
        'Downstream HTTP / S3-protocol errors are acceptable, since `openssl s_server -www` is not a real S3 API.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.8.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_S3_NonApprovedCiphers_Reject = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.S3.NonApprovedCiphers.Reject',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary, when started with `GODEBUG=fips140=only` and configured against an S3-compatible HTTPS endpoint as described above, SHALL refuse to complete the [TLS] handshake when the remote endpoint offers only non-FIPS-approved cipher suites, including:\n'
        '\n'
        '* TLSv1.2 with `ECDHE-RSA-CHACHA20-POLY1305`\n'
        '* TLSv1.2 with `DHE-RSA-AES256-GCM-SHA384`\n'
        '* TLSv1.3 with `TLS_CHACHA20_POLY1305_SHA256`\n'
        '\n'
        'For each of the above, `clickhouse-backup-fips list remote` SHALL fail with\n'
        '`remote error: tls: handshake failure` or `no shared cipher`.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.8.2'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_ACVP_Wrapper = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.ACVP.Wrapper',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [ACVP] wrapper bundled with [clickhouse-backup] at `pkg/acvpwrapper/run.sh` SHALL execute the algorithm test vectors against the [clickhouse-backup-fips] binary and SHALL execute successfully with zero failures across the entire run.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.9.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_NetworkSurface = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.NetworkSurface',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup-fips] binary SHALL expose the following network surface during FIPS-compatible operation:\n'
        '\n'
        '* Inbound: a REST API [TLS] listener on TCP port `7172` when started in `server` mode with `api.secure: true`; no plain HTTP listener.\n'
        '* Outbound to ClickHouse: secure native [TLS] TCP port `9440`.\n'
        '* Outbound to S3-compatible storage: HTTPS to the AWS FIPS hostname `s3-fips.<region>.amazonaws.com:443`.\n'
        '\n'
        'No other ports SHALL be opened or used by the [clickhouse-backup-fips] binary in FIPS-compatible operation.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.10.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Configuration_SecureClickHouse = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Configuration.SecureClickHouse',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'A FIPS-compatible [clickhouse-backup-fips] configuration SHALL satisfy all of the following:\n'
        '\n'
        '* `clickhouse.secure: true`.\n'
        '* `clickhouse.port: 9440` (secure native [TLS]). Plain native TCP `9000` and plain HTTP `8123` SHALL NOT be used against a FIPS-configured ClickHouse server.\n'
        '* `api.secure: true` plus a valid certificate and private key when running `clickhouse-backup-fips server`.\n'
        '* `s3.endpoint` left empty and `s3.region` set to a valid AWS region so the SDK targets the AWS FIPS hostname `s3-fips.<region>.amazonaws.com`.\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.11.1'
)

RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Server_Listener = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Server.Listener',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '`clickhouse-backup-fips server`, when started with a FIPS-compatible REST API configuration (`api.secure: true`, no plain ports configured), SHALL open exactly one [TLS] listener on TCP port `7172` and SHALL NOT bind any other port. The single listener SHALL accept only the FIPS-approved [TLS] cipher suites defined by [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv12.Approved](#rqsrs-013clickhousebackuputilityfipsapprovedciphersuitestlsv12approved) and [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv13.Approved](#rqsrs-013clickhousebackuputilityfipsapprovedciphersuitestlsv13approved).\n'
        '\n'
    ),
    link=None,
    level=3,
    num='4.12.1'
)

QA_SRS013_ClickHouse_Backup_Utility_FIPS_Compatibility = Specification(
    name='QA-SRS013 ClickHouse Backup Utility FIPS Compatibility',
    description=None,
    author=None,
    date=None,
    status=None,
    approved_by=None,
    approved_date=None,
    approved_version=None,
    version=None,
    group=None,
    type=None,
    link=None,
    uid=None,
    parent=None,
    children=None,
    headings=(
        Heading(name='Revision History', level=1, num='1'),
        Heading(name='Introduction', level=1, num='2'),
        Heading(name='Terminology', level=1, num='3'),
        Heading(name='SRS', level=2, num='3.1'),
        Heading(name='FIPS 140-3', level=2, num='3.2'),
        Heading(name='CAST', level=2, num='3.3'),
        Heading(name='ACVP definition', level=2, num='3.4'),
        Heading(name='GODEBUG', level=2, num='3.5'),
        Heading(name='FIPS-compatible TLS connection', level=2, num='3.6'),
        Heading(name='clickhouse-backup-fips', level=2, num='3.7'),
        Heading(name='Requirements', level=1, num='4'),
        Heading(name='General', level=2, num='4.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GoCryptographicModule', level=3, num='4.1.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Build.GOFIPS140', level=3, num='4.1.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Binary', level=3, num='4.1.3'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.DockerImage', level=3, num='4.1.4'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.TLSProtocolVersions', level=3, num='4.1.5'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv12.Approved', level=3, num='4.1.6'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv13.Approved', level=3, num='4.1.7'),
        Heading(name='Connectivity', level=2, num='4.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Connectivity.FIPSEndpoint', level=3, num='4.2.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Connectivity.NonFIPSEndpoint', level=3, num='4.2.2'),
        Heading(name='Version Output', level=2, num='4.3'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Version.Status', level=3, num='4.3.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Version.BuildSetting', level=3, num='4.3.2'),
        Heading(name='GODEBUG fips140 Modes', level=2, num='4.4'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.Unset', level=3, num='4.4.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.On', level=3, num='4.4.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.GODEBUG.Only', level=3, num='4.4.3'),
        Heading(name='Startup Integrity Self-Tests', level=2, num='4.5'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.Integrity', level=3, num='4.5.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.TamperedBinary', level=3, num='4.5.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.CAST.ForcedFailure', level=3, num='4.5.3'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.SelfTest.CAST.Coverage', level=3, num='4.5.4'),
        Heading(name='Inbound TLS — REST API', level=2, num='4.6'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.ApprovedCiphers', level=3, num='4.6.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.NonApprovedCiphers.Reject', level=3, num='4.6.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.LegacyProtocols.Reject', level=3, num='4.6.3'),
        Heading(name='Outbound TLS — ClickHouse Endpoint', level=2, num='4.7'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.ClickHouseEndpoint.Ciphers.Approved', level=3, num='4.7.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.ClickHouseEndpoint.NonApprovedCiphers.Reject', level=3, num='4.7.2'),
        Heading(name='Outbound TLS — S3 Endpoint', level=2, num='4.8'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.S3.Ciphers.Approved', level=3, num='4.8.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.S3.NonApprovedCiphers.Reject', level=3, num='4.8.2'),
        Heading(name='ACVP', level=2, num='4.9'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.ACVP.Wrapper', level=3, num='4.9.1'),
        Heading(name='Network Surface', level=2, num='4.10'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.NetworkSurface', level=3, num='4.10.1'),
        Heading(name='Client Configuration', level=2, num='4.11'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Configuration.SecureClickHouse', level=3, num='4.11.1'),
        Heading(name='Server Listener', level=2, num='4.12'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Server.Listener', level=3, num='4.12.1'),
        Heading(name='References', level=1, num='5'),
        ),
    requirements=(
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GoCryptographicModule,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Build_GOFIPS140,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Binary,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_DockerImage,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Approved_TLSProtocolVersions,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Approved_CipherSuites_TLSv12_Approved,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Approved_CipherSuites_TLSv13_Approved,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Connectivity_FIPSEndpoint,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Connectivity_NonFIPSEndpoint,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_Status,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_BuildSetting,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GODEBUG_Unset,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GODEBUG_On,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GODEBUG_Only,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_Integrity,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_TamperedBinary,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_CAST_ForcedFailure,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_CAST_Coverage,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_ApprovedCiphers,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_NonApprovedCiphers_Reject,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_LegacyProtocols_Reject,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_ClickHouseEndpoint_Ciphers_Approved,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_ClickHouseEndpoint_NonApprovedCiphers_Reject,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_S3_Ciphers_Approved,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_S3_NonApprovedCiphers_Reject,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_ACVP_Wrapper,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_NetworkSurface,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Configuration_SecureClickHouse,
        RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Server_Listener,
        ),
    content='''
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
    * 4.10 [Network Surface](#network-surface)
        * 4.10.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.NetworkSurface](#rqsrs-013clickhousebackuputilityfipsnetworksurface)
    * 4.11 [Client Configuration](#client-configuration)
        * 4.11.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Configuration.SecureClickHouse](#rqsrs-013clickhousebackuputilityfipsconfigurationsecureclickhouse)
    * 4.12 [Server Listener](#server-listener)
        * 4.12.1 [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Server.Listener](#rqsrs-013clickhousebackuputilityfipsserverlistener)
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

The [clickhouse-backup-fips] binary, when started with `GODEBUG=fips140=only` and configured against an S3-compatible HTTPS endpoint as described above, SHALL NOT have its [TLS] handshake rejected by the FIPS policy when the remote endpoint offers any of the following FIPS-approved profiles:

* TLSv1.2 with `ECDHE-RSA-AES128-GCM-SHA256`
* TLSv1.2 with `ECDHE-RSA-AES256-GCM-SHA384`
* TLSv1.3 with `TLS_AES_128_GCM_SHA256`
* TLSv1.3 with `TLS_AES_256_GCM_SHA384`.

Downstream HTTP / S3-protocol errors are acceptable, since `openssl s_server -www` is not a real S3 API.

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Outbound.S3.NonApprovedCiphers.Reject
version: 1.0

The [clickhouse-backup-fips] binary, when started with `GODEBUG=fips140=only` and configured against an S3-compatible HTTPS endpoint as described above, SHALL refuse to complete the [TLS] handshake when the remote endpoint offers only non-FIPS-approved cipher suites, including:

* TLSv1.2 with `ECDHE-RSA-CHACHA20-POLY1305`
* TLSv1.2 with `DHE-RSA-AES256-GCM-SHA384`
* TLSv1.3 with `TLS_CHACHA20_POLY1305_SHA256`

For each of the above, `clickhouse-backup-fips list remote` SHALL fail with
`remote error: tls: handshake failure` or `no shared cipher`.

### ACVP

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.ACVP.Wrapper
version: 1.0

The [ACVP] wrapper bundled with [clickhouse-backup] at `pkg/acvpwrapper/run.sh` SHALL execute the algorithm test vectors against the [clickhouse-backup-fips] binary and SHALL execute successfully with zero failures across the entire run.

### Network Surface

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.NetworkSurface
version: 1.0

The [clickhouse-backup-fips] binary SHALL expose the following network surface during FIPS-compatible operation:

* Inbound: a REST API [TLS] listener on TCP port `7172` when started in `server` mode with `api.secure: true`; no plain HTTP listener.
* Outbound to ClickHouse: secure native [TLS] TCP port `9440`.
* Outbound to S3-compatible storage: HTTPS to the AWS FIPS hostname `s3-fips.<region>.amazonaws.com:443`.

No other ports SHALL be opened or used by the [clickhouse-backup-fips] binary in FIPS-compatible operation.

### Client Configuration

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Configuration.SecureClickHouse
version: 1.0

A FIPS-compatible [clickhouse-backup-fips] configuration SHALL satisfy all of the following:

* `clickhouse.secure: true`.
* `clickhouse.port: 9440` (secure native [TLS]). Plain native TCP `9000` and plain HTTP `8123` SHALL NOT be used against a FIPS-configured ClickHouse server.
* `api.secure: true` plus a valid certificate and private key when running `clickhouse-backup-fips server`.
* `s3.endpoint` left empty and `s3.region` set to a valid AWS region so the SDK targets the AWS FIPS hostname `s3-fips.<region>.amazonaws.com`.

### Server Listener

#### RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Server.Listener
version: 1.0

`clickhouse-backup-fips server`, when started with a FIPS-compatible REST API configuration (`api.secure: true`, no plain ports configured), SHALL open exactly one [TLS] listener on TCP port `7172` and SHALL NOT bind any other port. The single listener SHALL accept only the FIPS-approved [TLS] cipher suites defined by [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv12.Approved](#rqsrs-013clickhousebackuputilityfipsapprovedciphersuitestlsv12approved) and [RQ.SRS-013.ClickHouse.BackupUtility.FIPS.Approved.CipherSuites.TLSv13.Approved](#rqsrs-013clickhousebackuputilityfipsapprovedciphersuitestlsv13approved).

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
'''
)
