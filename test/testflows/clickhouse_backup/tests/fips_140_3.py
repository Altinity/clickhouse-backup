import sys

from testflows.core import *
from testflows.asserts import error
append_path(sys.path, "../..")
from clickhouse_backup.requirements.fips.requirements import *


FIPS_BINARY_IN_CONTAINER = "/bin/clickhouse-backup-fips"
FIPS_VERSION_LABEL = "FIPS 140-3:"
FIPS_VERSION_TRUE = "true"


# Bind-mount path inside the container: ``/etc/clickhouse-backup/`` (per-PID
# copy made by ``regression.py``'s ``shutil.copytree`` of ``configs/backup/``).
# TLS is referenced by that config (``/etc/clickhouse-server/ssl/server.{crt,key}``)
# is inherited via ``volumes_from_name="clickhouse1"`` on the FIPS container -
# tests never generate certificates of their own.
FIPS_TLS_CONFIG_PATH = "/etc/clickhouse-backup/config-fips-api-tls.yml"
FIPS_TLS_LISTEN_PORT = 7172

# FIPS 140-3 cipher policy as documented in the SRS
# (RQ.SRS-013.ClickHouse.BackupUtility.FIPS.TLS.Inbound.RESTAPI.*).
# The OpenSSL CLI uses two different flags:
#   ``-ciphersuites <name>``  : TLSv1.3 (IANA names, e.g. ``TLS_AES_128_GCM_SHA256``)
#   ``-cipher <name>``        : TLSv1.2 (OpenSSL names, e.g. ``ECDHE-RSA-AES128-GCM-SHA256``)
# Test certificates use RSA so only ``ECDHE-RSA-*`` 1.2 suites can succeed.
FIPS_TLS13_APPROVED = ("TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384")
FIPS_TLS12_APPROVED = ("ECDHE-RSA-AES128-GCM-SHA256",)
NON_FIPS_TLS13 = ("TLS_CHACHA20_POLY1305_SHA256",)
# ``RC4-SHA`` / ``DES-CBC3-SHA`` are listed in the SRS but aren't compiled into
# OpenSSL 3.x by default. When the openssl CLI refuses to offer the cipher,
# ``_check_tls_handshake`` accepts it as a valid form of "rejected".
NON_FIPS_TLS12 = ("ECDHE-RSA-CHACHA20-POLY1305", "RC4-SHA", "DES-CBC3-SHA")


def _require_fips_container(test):
    """Skip the calling scenario if no FIPS backup container is available.

    The ``clickhouse_backup_fips`` container is provisioned by
    ``helpers.cluster.Cluster._do_up`` only when the host-side FIPS binary
    (``clickhouse-backup/clickhouse-backup-race-fips``) exists.
    """
    backup_fips = getattr(test.context, "backup_fips", None)
    if backup_fips is None:
        skip(
            "clickhouse-backup-race-fips binary not available on host; "
            "build it with `make build-race-fips-docker` to enable FIPS scenarios."
        )
    return backup_fips


def _read_fips_status(node, binary):
    """Run ``<binary> --version`` on ``node`` and return ``(status, output)``.

    ``status`` is the lower-cased value after ``FIPS 140-3:`` in the
    ``--version`` output, or ``None`` if the line is absent. ``output`` is
    the full command output for use in error messages.
    """
    r = node.cmd(f"{binary} --version")
    for line in r.output.splitlines():
        normalized = line.strip()
        if normalized.lower().startswith(FIPS_VERSION_LABEL.lower()):
            value = normalized.split(":", 1)[1].strip().lower() if ":" in normalized else ""
            return value, r.output
    return None, r.output


def _check_tls_handshake(node, target, tls_flag, cipher=None, ciphersuites=None,
                         expected_success=True):
    """Try to open a TLS connection with ``openssl s_client`` and check the result.

    The function runs the OpenSSL command-line client inside ``node``, points
    it at ``target`` (``host:port``), forces a specific TLS protocol version
    (``tls_flag``) and optionally restricts the cipher / cipher suite that
    the client is allowed to offer. It then asserts that the TLS handshake
    either completes (``expected_success=True``) or is rejected
    (``expected_success=False``).

    :param node: cluster Node from which ``openssl`` is invoked (the
        ``clickhouse_backup_fips`` container, which has ``openssl``
        installed and shares the docker network with the API listener).
    :param target: ``host:port`` of the TLS server, e.g. ``localhost:7172``.
    :param tls_flag: ``openssl s_client`` protocol switch:
        ``-tls1``, ``-tls1_1``, ``-tls1_2`` or ``-tls1_3``.
    :param cipher: TLSv1.2 OpenSSL cipher name (use with ``-tls1_2``),
        e.g. ``ECDHE-RSA-AES128-GCM-SHA256``.
    :param ciphersuites: TLSv1.3 IANA suite name (use with ``-tls1_3``),
        e.g. ``TLS_AES_128_GCM_SHA256``.
    :param expected_success: ``True`` if the handshake should complete,
        ``False`` if the server is expected to reject it.

    A connection-level error (port closed, host unreachable, etc.) is
    surfaced as a clear assertion failure rather than being silently
    misclassified as a TLS-policy rejection.
    """
    if cipher and ciphersuites:
        raise ValueError("specify either `cipher` or `ciphersuites`, not both")

    cipher_arg = ""
    if cipher:
        cipher_arg = f"-cipher {cipher}"
    elif ciphersuites:
        cipher_arg = f"-ciphersuites {ciphersuites}"

    cmd = (
        f"timeout 10 openssl s_client -connect {target} -brief "
        f"{tls_flag} {cipher_arg} </dev/null 2>&1"
    )
    r = node.cmd(cmd, no_checks=True)
    output = (r.output or "")
    output_lower = output.lower()

    cipher_unavailable = (
        "no cipher match" in output_lower
        or "error setting cipher list" in output_lower
        or "no ciphers available" in output_lower
        or "cipher_list" in output_lower and "no cipher" in output_lower
    )

    handshake_failed = r.exitcode != 0 and (
        "handshake failure" in output_lower
        or "no shared cipher" in output_lower
        or "ssl alert" in output_lower
        or "alert" in output_lower
        or "no protocols available" in output_lower
        or "unsupported protocol" in output_lower
        or "wrong version number" in output_lower
        or cipher_unavailable
    )

    if expected_success:
        assert not cipher_unavailable, error(
            f"openssl client refused to offer the cipher; cannot test the positive case.\n{output}"
        )
        assert r.exitcode == 0, error(
            f"Expected handshake to succeed, exit={r.exitcode}\n{output}"
        )
    else:
        assert handshake_failed, error(
            f"Expected handshake rejection, exit={r.exitcode}\n{output}"
        )


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_Status("1.0")
)
def clickhouse_backup_fips_version_output(self):
    """Validate that `clickhouse-backup-fips --version` reports `FIPS 140-3: true`.

    The binary is built with ``GOFIPS140=v1.0.0`` so FIPS 140-3 mode is the
    build-time default and ``crypto/fips140.Enabled()`` returns ``true``.
    """
    backup_fips = _require_fips_container(self)

    with When("I run `clickhouse-backup-fips --version`"):
        status, output = _read_fips_status(backup_fips, FIPS_BINARY_IN_CONTAINER)

    with Then(f"`{FIPS_VERSION_LABEL}` line is present"):
        assert status is not None, error(
            f"`{FIPS_VERSION_LABEL}` line missing from `--version`:\n{output}"
        )

    with And(f"`{FIPS_VERSION_LABEL}` reports `{FIPS_VERSION_TRUE}`"):
        assert status == FIPS_VERSION_TRUE, error(
            f"Expected `{FIPS_VERSION_LABEL} {FIPS_VERSION_TRUE}`, got `{status}`.\n{output}"
        )


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_Status("1.0")
)
def clickhouse_backup_fips_version_output_negative_check(self):
    """Self-check for `clickhouse_backup_fips_version_output`.

    Run the same ``--version`` parser against the regular (non-FIPS)
    ``clickhouse-backup`` binary - which is exactly what ``make build-race``
    produces, i.e. equivalent to a ``Makefile`` where ``GOFIPS140=v1.0.0``
    has been stripped out of the ``build-race-fips`` target. The status
    line MUST report ``false``; if it ever reported ``true`` the positive
    scenario above would silently accept any binary and would no longer be
    enforcing FIPS at all.

    Reuses the shared ``self.context.backup`` container - no extra binary
    is mounted, no Makefile is touched, no files are written - so the check
    runs identically locally and on CI/CD.
    """
    backup = self.context.backup

    with When("I run `clickhouse-backup --version` on the non-FIPS binary"):
        status, output = _read_fips_status(backup, "/bin/clickhouse-backup")

    with Then(f"`{FIPS_VERSION_LABEL}` line is present"):
        assert status is not None, error(
            f"`{FIPS_VERSION_LABEL}` line missing from non-FIPS `--version`:\n{output}"
        )

    with And(f"`{FIPS_VERSION_LABEL}` does NOT report `{FIPS_VERSION_TRUE}`"):
        assert status != FIPS_VERSION_TRUE, error(
            f"non-FIPS binary unexpectedly reports `{FIPS_VERSION_LABEL} {FIPS_VERSION_TRUE}`. "
            f"`clickhouse_backup_fips_version_output` would not catch a Makefile with FIPS removed.\n{output}"
        )


@TestScenario
def gofips140_build_flags_present(self):
    """Validate `go version -m` includes `build\tGOFIPS140=v1.0.0`."""
    xfail("not implemented yet")


@TestScenario
def godebug_fips140_modes(self):
    """Validate `GODEBUG` runtime modes (`unset`/`fips140=on`/`fips140=only`)."""
    xfail("not implemented yet")


@TestScenario
def connectivity_against_non_fips_clickhouse_server(self):
    """Validate `clickhouse-backup-fips` connectivity against non-FIPS ClickHouse server when `GODEBUG=fips140=on`"""
    xfail("not implemented yet")


@TestScenario
def connectivity_against_fips_clickhouse_server(self):
    """Validate `clickhouse-backup-fips` connectivity against FIPS-compatible ClickHouse server when `GODEBUG=fips140=only`."""
    xfail("not implemented yet")


@TestScenario
def fips_integrity_self_test_failure_on_tampered_binary(self):
    """Validate startup abort for a tampered `.go.fipsinfo` checksum."""
    xfail("not implemented yet")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_ApprovedCiphers("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_NonApprovedCiphers_Reject("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_LegacyProtocols_Reject("1.0"),
)
def inbound_tls_cipher_negotiation(self):
    """Validate inbound TLS policy of the `clickhouse-backup-fips` REST API.

    Starts ``clickhouse-backup-fips server`` inside the dedicated
    ``clickhouse_backup_fips`` container with ``GODEBUG=fips140=only`` and
    the static TLS-API config at ``/etc/clickhouse-backup/config-fips-api-tls.yml``,
    then runs ``openssl s_client`` from inside the same container to try a
    TLS connection against the listener on ``localhost:7172`` for each
    cipher / cipher suite / protocol the SRS calls out, and asserts:

    * FIPS-approved TLSv1.3 / TLSv1.2 cipher suites complete the handshake.
    * Non-approved suites (ChaCha20-Poly1305, RC4, 3DES, etc.) are rejected.
    * Legacy protocols (TLSv1.0, TLSv1.1) are rejected.

    The TLS API config is a
    static file committed under ``configs/backup/`` and bind-mounted by the
    per-PID copy of that directory; the certificate / key referenced by it
    are the same static fixtures used by the ClickHouse nodes
    (``/etc/clickhouse-server/ssl/server.{crt,key}``), inherited via
    ``volumes_from_name="clickhouse1"`` on the FIPS container.
    """
    backup_fips = _require_fips_container(self)

    target = f"localhost:{FIPS_TLS_LISTEN_PORT}"

    try:
        with Given(
            "I start clickhouse-backup-fips server with GODEBUG=fips140=only and the TLS API config",
            description=f"config={FIPS_TLS_CONFIG_PATH} listen=:{FIPS_TLS_LISTEN_PORT}",
        ):
            backup_fips.start_server(
                binary=FIPS_BINARY_IN_CONTAINER,
                config=FIPS_TLS_CONFIG_PATH,
                extra_env={"GODEBUG": "fips140=only"},
                listen_port=FIPS_TLS_LISTEN_PORT,
                timeout=60,
            )

        with When("I try to connect using each FIPS-approved TLSv1.3 cipher suite"):
            for ciphersuite in FIPS_TLS13_APPROVED:
                with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be accepted"):
                    _check_tls_handshake(
                        backup_fips, target=target, tls_flag="-tls1_3",
                        ciphersuites=ciphersuite, expected_success=True,
                    )

        with And("I try to connect using each FIPS-approved TLSv1.2 cipher"):
            for cipher in FIPS_TLS12_APPROVED:
                with Check(f"TLSv1.2 cipher {cipher} should be accepted"):
                    _check_tls_handshake(
                        backup_fips, target=target, tls_flag="-tls1_2",
                        cipher=cipher, expected_success=True,
                    )

        with And("I try to connect using each non-FIPS TLSv1.3 cipher suite"):
            for ciphersuite in NON_FIPS_TLS13:
                with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be rejected"):
                    _check_tls_handshake(
                        backup_fips, target=target, tls_flag="-tls1_3",
                        ciphersuites=ciphersuite, expected_success=False,
                    )

        with And("I try to connect using each non-FIPS TLSv1.2 cipher"):
            for cipher in NON_FIPS_TLS12:
                with Check(f"TLSv1.2 cipher {cipher} should be rejected"):
                    _check_tls_handshake(
                        backup_fips, target=target, tls_flag="-tls1_2",
                        cipher=cipher, expected_success=False,
                    )

        with And("I try to connect using legacy TLSv1.0 / TLSv1.1 protocols"):
            with Check("TLSv1.0 handshake should be rejected"):
                _check_tls_handshake(
                    backup_fips, target=target, tls_flag="-tls1",
                    expected_success=False,
                )
            with Check("TLSv1.1 handshake should be rejected"):
                _check_tls_handshake(
                    backup_fips, target=target, tls_flag="-tls1_1",
                    expected_success=False,
                )

    finally:
        with Finally("I stop the FIPS server"):
            backup_fips.stop_server()


@TestScenario
def outbound_tls_cipher_negotiation(self):
    """Validate outbound TLS policy from `clickhouse-backup-fips` to ClickHouse server with `openssl s_server`."""
    xfail("not implemented yet")


@TestScenario
def outbound_tls_to_s3_endpoint_with_openssl_s_server(self):
    """Validate outbound TLS policy to S3 endpoint with `openssl s_server`."""
    xfail("not implemented yet")


@TestScenario
def forced_cast_failures(self):
    """Validate forced CAST failures with `GODEBUG=failfipscast=...,fips140=on`."""
    xfail("not implemented yet")


@TestScenario
def acvp_tests(self):
    """Validate ACVP tests."""
    xfail("not implemented yet")

@TestFeature
@Name("FIPS 140-3 Compatibility")
def fips_140_3(self):
    """FIPS 140-3 automation entrypoint for clickhouse-backup.
    """
    Scenario(run=clickhouse_backup_fips_version_output, flags=TE)
    Scenario(run=clickhouse_backup_fips_version_output_negative_check, flags=TE)
    Scenario(run=gofips140_build_flags_present, flags=TE)
    Scenario(run=godebug_fips140_modes, flags=TE)
    Scenario(run=connectivity_against_non_fips_clickhouse_server, flags=TE)
    Scenario(run=connectivity_against_fips_clickhouse_server, flags=TE)
    Scenario(run=fips_integrity_self_test_failure_on_tampered_binary, flags=TE)
    Scenario(run=inbound_tls_cipher_negotiation, flags=TE)
    Scenario(run=outbound_tls_cipher_negotiation, flags=TE)
    Scenario(run=outbound_tls_to_s3_endpoint_with_openssl_s_server, flags=TE)
    Scenario(run=forced_cast_failures, flags=TE)
    Scenario(run=acvp_tests, flags=TE)


if main():
    fips_140_3()

