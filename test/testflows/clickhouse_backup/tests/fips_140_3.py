import os
import sys

from testflows.core import *
from testflows.asserts import error
append_path(sys.path, "../..")
from clickhouse_backup.requirements.fips.requirements import *


FIPS_BINARY_IN_CONTAINER = "/bin/clickhouse-backup-fips"
FIPS_VERSION_LABEL = "FIPS 140-3:"
FIPS_VERSION_TRUE = "true"


FIPS_GO_BUILD_SETTING = "build\tGOFIPS140=v1.0.0"


# All clickhouse-backup-fips per-scenario configs live in the dedicated
# ``configs/backup/fips/`` subfolder; ``regression.py`` copies the whole
# ``configs/backup/`` tree per-PID and bind-mounts it at
# ``/etc/clickhouse-backup``, so the in-container path keeps the subdir.
FIPS_TLS_CONFIG_PATH                = "/etc/clickhouse-backup/fips/config-fips-api-tls.yml"
FIPS_OUTBOUND_CH_CONFIG_PATH        = "/etc/clickhouse-backup/fips/config-fips-outbound-clickhouse-tls.yml"
FIPS_OUTBOUND_S3_CONFIG_PATH        = "/etc/clickhouse-backup/fips/config-fips-outbound-s3-tls.yml"
FIPS_TLS_LISTEN_PORT                = 7172

FIPS_CONNECTIVITY_FIPS_CONFIG_PATH      = "/etc/clickhouse-backup/fips/config-fips-connectivity-fips-server.yml"
FIPS_CONNECTIVITY_NONFIPS_CONFIG_PATH   = "/etc/clickhouse-backup/fips/config-fips-connectivity-nonfips-server.yml"
FIPS_CH_SERVER_IMAGE                    = "altinity/clickhouse-server:25.3.8.30001.altinityfips"
NON_FIPS_CH_SERVER_IMAGE                = "altinity/clickhouse-server:25.8.16.10002.altinitystable"
FIPS_CH_SERVER_NAME                     = "clickhouse_fips_server"
NON_FIPS_CH_SERVER_NAME                 = "clickhouse_nonfips_server"

# Standard ClickHouse secure native-TCP port
FIPS_OUTBOUND_CH_TLS_PORT           = 9440

# Default HTTPS port. The S3 outbound scenario must use 443 because the AWS
# SDK Go v2 generates the FIPS endpoint URL as bare
# `https://s3-fips.<region>.amazonaws.com` (no port suffix), so the
# container has to be reachable on the default HTTPS port for the SDK's
# request to land on it. See `config-fips-outbound-s3-tls.yml` for the
# full reasoning behind the AWS-style hostname approach.
FIPS_OUTBOUND_S3_TLS_PORT           = 443

# Hostname of the auxiliary ``openssl s_server``
OPENSSL_AUX_NAME                    = "openssl_server"

# For the S3 outbound scenario the container is named after the AWS S3 FIPS
# endpoint hostname so Docker's embedded DNS resolves the SDK-generated
# URL (``https://s3-fips.us-east-1.amazonaws.com``) to it. ``us-east-1``
# matches ``s3.region`` in ``config-fips-outbound-s3-tls.yml``.
OPENSSL_S3_FIPS_AUX_NAME            = "s3-fips.us-east-1.amazonaws.com"

# OpenSSL CLI uses two different flags:
#   ``-ciphersuites <name>``  : TLSv1.3 (IANA names, e.g. ``TLS_AES_128_GCM_SHA256``)
#   ``-cipher <name>``        : TLSv1.2 (OpenSSL names, e.g. ``ECDHE-RSA-AES128-GCM-SHA256``)

FIPS_TLS12_APPROVED = (
    "ECDHE-RSA-AES128-GCM-SHA256",
    "ECDHE-RSA-AES256-GCM-SHA384",
)
FIPS_TLS13_APPROVED = (
    "TLS_AES_128_GCM_SHA256",
    "TLS_AES_256_GCM_SHA384",
)

# Single non-approved TLSv1.3 suite
NON_FIPS_TLS13 = ("TLS_CHACHA20_POLY1305_SHA256",)

# Non-FIPS TLS1.2 suites for the inbound REST API
NON_FIPS_TLS12_INBOUND_REST = (
    "ECDHE-RSA-CHACHA20-POLY1305",
    "RC4-SHA",
    "DES-CBC3-SHA",
)

# Non-FIPS TLS1.2 suites for the outbound verification.
#
# The set covers three different reasons a TLS1.2 cipher must be rejected
# by the Go FIPS 140-3 outbound policy:
#
# 1. Non-approved bulk cipher (CHACHA20):
#       ECDHE-RSA-CHACHA20-POLY1305
# 2. Non-approved key exchange (DHE):
#       DHE-RSA-AES256-GCM-SHA384
#       DHE-RSA-AES128-GCM-SHA256
# 3. Plain RSA static key exchange (no forward secrecy):
#       AES256-GCM-SHA384
#       AES128-GCM-SHA256
NON_FIPS_TLS12_OUTBOUND = (
    "ECDHE-RSA-CHACHA20-POLY1305",
    "DHE-RSA-AES256-GCM-SHA384",
    "DHE-RSA-AES128-GCM-SHA256",
    "AES256-GCM-SHA384",
    "AES128-GCM-SHA256",
)


# Cryptographic Algorithm Self-Test (CAST) names are from Go's FIPS 140-3
# module. Shows `allCASTs` in `crypto/internal/fips140test/cast_test.go`
# of the Go release used to build `clickhouse-backup-race-fips`. Each name
# is a valid value for `GODEBUG=failfipscast=<NAME>`: setting it forces
# that one CAST to fail at startup, which Go's FIPS module turns into a
# fatal abort.
FIPS_FAILFIPSCAST_NAMES = (
    "AES-CBC",
    "CTR_DRBG",
    "CounterKDF",
    "DetECDSA P-256 SHA2-512 sign",
    "ECDH PCT",
    "ECDSA P-256 SHA2-512 sign and verify",
    "ECDSA PCT",
    "Ed25519 sign and verify",
    "Ed25519 sign and verify PCT",
    "HKDF-SHA2-256",
    "HMAC-SHA2-256",
    "KAS-ECC-SSC P-256",
    "ML-DSA sign and verify PCT",
    "ML-DSA-44",
    "ML-KEM PCT",
    "ML-KEM-768",
    "PBKDF2",
    "RSA sign and verify PCT",
    "RSASSA-PKCS-v1.5 2048-bit sign and verify",
    "SHA2-256",
    "SHA2-512",
    "TLSv1.2-SHA2-256",
    "TLSv1.3-SHA2-256",
    "cSHAKE128",
)

# Marker that Go's FIPS module writes to stderr on a failfipscast-forced
# CAST. The full line is
# `fatal error: FIPS 140-3 self-test failed: <NAME>: simulated CAST failure`.
FIPS_FAILFIPSCAST_MARKER = "simulated CAST failure"

# Required CAST names that MUST be registered in any Go FIPS module (these
# are foundational names present since v1.0.0). If forcing one of
# these does NOT abort `clickhouse-backup-fips`, the failfipscast hook
# itself is broken or the binary is not running in FIPS mode.
FIPS_FAILFIPSCAST_REQUIRED_NAMES = (
    "SHA2-256",
    "HMAC-SHA2-256",
    "AES-CBC",
)

# ACVP wrapper scenario opt-in.
# Set `RUN_ACVP_TESTS=1` locally or in the CI workflow to enable it.
FIPS_ACVP_ENV_FLAG          = "RUN_ACVP_TESTS"
FIPS_ACVP_ENV_FLAG_VALUES   = ("1", "true", "yes", "on")
# Path to the wrapper script, relative to `cluster.tests_dir`
# (`test/testflows/clickhouse_backup`). The script is part of the merged
# ACVP wrapper PR and lives at the repository root.
FIPS_ACVP_SCRIPT_RELPATH    = "../../../pkg/acvpwrapper/run.sh"
# Oracle line printed by `check_expected.go` when the run passes. Tracked
# in `pkg/acvpwrapper/README.md` ("Reproduce The Current Result").
FIPS_ACVP_EXPECTED_OUTPUT   = "38 ACVP tests matched expectations"
# Timeout for the host shell - first run does Docker image pulls,
# a boringssl clone, an acvptool build, and then the ACVP run itself.
FIPS_ACVP_TIMEOUT_SEC       = 30 * 60


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


OUTBOUND_TLS_CMD_TIMEOUT_SEC = 15


def _check_outbound_tls_handshake(node, command, expected_success):
    """Run ``command`` inside ``node`` and assert on the *outbound* TLS outcome.

    ``command`` invokes ``clickhouse-backup-fips`` with
    ``GODEBUG=fips140=only`` against an ``openssl s_server`` whose offered
    cipher we want the FIPS policy to accept or reject. The function wraps
    ``command`` in ``timeout <N>`` so the retry loop is bounded; the check
    is **TLS-policy-only**:

    * ``expected_success=True``  - the FIPS policy must NOT reject the
      handshake.

    * ``expected_success=False`` - the FIPS policy MUST reject the
      handshake.
    """
    bounded = f"timeout {OUTBOUND_TLS_CMD_TIMEOUT_SEC} {command}"
    r = node.cmd(bounded, no_checks=True)
    output = (r.output or "")
    output_lower = output.lower()

    handshake_rejected = (
        "remote error: tls: handshake failure" in output_lower
        or "tls: handshake failure" in output_lower
        or "no shared cipher" in output_lower
        or "tls: no cipher suite supported" in output_lower
        or "tls: protocol version not supported" in output_lower
    )

    if expected_success:
        assert not handshake_rejected, error(
            f"Expected the FIPS policy to ACCEPT the handshake, but the output "
            f"contains a TLS handshake-failure marker.\n{output}"
        )
    else:
        assert handshake_rejected, error(
            f"Expected the FIPS policy to REJECT the handshake, but no "
            f"TLS handshake-failure marker was found.\n{output}"
        )


def _check_outbound_tls_with_cipher(cluster, backup_fips, *, listen, command,
                                    expected_success, tls_version,
                                    cipher=None, ciphersuites=None,
                                    aux_name=OPENSSL_AUX_NAME):
    """One outbound TLS handshake check for one cipher profile.

    Brings up an ``openssl s_server`` container, runs ``command`` inside
    the FIPS backup container, asserts the FIPS-policy outcome via
    `check_outbound_tls_handshake`, and stops the aux container in
    `Finally`. The aux container reuses the cluster's static SSL fixtures
    (`configs/clickhouse/ssl/{server.crt,server.key,dhparam.pem}`).

    :param cluster: cluster used to start/stop the aux container.
    :param backup_fips: FIPS backup container that runs `command`.
    :param listen: port the aux `openssl s_server` accepts on.
    :param command: full `clickhouse-backup-fips` invocation string
        (already wrapped with `env GODEBUG=fips140=only ...`).
    :param expected_success: `True` if the FIPS policy MUST accept the
        handshake, `False` if it MUST reject it.
    :param tls_version: `-tls1_2` or `-tls1_3` (passed to `s_server`).
    :param cipher: TLSv1.2 OpenSSL cipher name (use with `-tls1_2`);
        mutually exclusive with `ciphersuites`.
    :param ciphersuites: TLSv1.3 IANA cipher-suite name (use with
        `-tls1_3`); mutually exclusive with `cipher`.
    :param aux_name: docker hostname / network alias of the aux container;
        callers can name it after a target hostname.
    """
    ssl_dir = cluster.ssl_certs_dir
    try:
        cluster.start_openssl_container(
            name=aux_name, role="s_server", listen=listen,
            cert_path=os.path.join(ssl_dir, "server.crt"),
            key_path=os.path.join(ssl_dir, "server.key"),
            dhparam_path=os.path.join(ssl_dir, "dhparam.pem"),
            cipher=cipher, ciphersuites=ciphersuites,
            tls_version=tls_version,
        )
        _check_outbound_tls_handshake(backup_fips, command, expected_success)
    finally:
        cluster.stop_auxiliary_container(aux_name)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_Status("1.0")
)
def clickhouse_backup_fips_version_output(self):
    """Validate that `clickhouse-backup-fips --version` reports `FIPS 140-3: true`.

    The binary is built with `GOFIPS140=v1.0.0` so FIPS 140-3 mode is the
    build-time default and `crypto/fips140.Enabled()` returns `true`.
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

def clickhouse_backup_fips_version_output_negative_check(self): # Add requirement
    """Self-check for `clickhouse_backup_fips_version_output`.

    Run the same `--version` parser as in clickhouse_backup_fips_version_output 
    against the regular (non-FIPS) `clickhouse-backup` binary,
     which is exactly what `make build-race`
    produces. The status line MUST report `false`; if it ever reported `true` the positive
    scenario above would silently accept any binary and would no longer be
    enforcing FIPS at all.
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
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_BuildSetting("1.0")
)
def gofips140_build_flags_present(self):
    """Validate `go version -m clickhouse-backup-fips` output contains `build\tGOFIPS140=v1.0.0`.

    Confirms the FIPS binary was built against the Go FIPS 140-3 module.
    """
    backup_fips = _require_fips_container(self)

    with When("I run `go version -m` against the FIPS binary"):
        r = backup_fips.cmd(f"go version -m {FIPS_BINARY_IN_CONTAINER}")
        debug(f"STDOUT:\n{r.output}")
        debug(f"EXIT CODE: {r.exitcode}")
        debug(f"FIPS_GO_BUILD_SETTING: {FIPS_GO_BUILD_SETTING!r}")

    with Then(f"the output contains the build setting `{FIPS_GO_BUILD_SETTING}`"):
        assert FIPS_GO_BUILD_SETTING in r.output, error(
            f"`go version -m {FIPS_BINARY_IN_CONTAINER}` output is missing "
            f"`{FIPS_GO_BUILD_SETTING}`:\n{r.output}"
        )


@TestScenario
def godebug_fips140_modes(self):
    """Validate `GODEBUG` runtime modes (`unset`/`fips140=on`/`fips140=only`).
    Set GODEBUG=fips140=on and check if FIPS tests pass.
    Set GODEBUG=fips140=only and check if FIPS tests pass.
    Leave GODEBUG unset and check if FIPS tests pass."""
    xfail("not implemented yet")


def _assert_tables_succeeds(backup_fips, *, config, godebug):
    """Run ``clickhouse-backup-fips -c <config> tables`` and assert it exits 0.

    Wrapped in ``timeout`` so a hung handshake or DNS failure does not
    block the regression for minutes; on success the command is fast.
    """
    cmd = (
        f"timeout {OUTBOUND_TLS_CMD_TIMEOUT_SEC} env GODEBUG={godebug} "
        f"{FIPS_BINARY_IN_CONTAINER} -c {config} tables"
    )
    r = backup_fips.cmd(cmd, no_checks=True)
    assert r.exitcode == 0, error(
        f"`clickhouse-backup-fips tables` against `{config}` failed "
        f"(exit={r.exitcode}, GODEBUG={godebug}):\n{r.output}"
    )


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Connectivity_FIPSEndpoint("1.0")
)
def connectivity_against_fips_clickhouse_server(self):
    """Validate `clickhouse-backup-fips tables` against a FIPS-compatible ClickHouse server.

    Brings up a dedicated `altinity/clickhouse-server:25.3.8.30001.altinityfips`
    container with `tcp_port_secure: 9440` enabled by
    `configs/clickhouse_fips_server/config.d/listeners.xml`, then runs
    `clickhouse-backup-fips -c <config> tables` from the FIPS backup
    container with `GODEBUG=fips140=only`. Exit code MUST be 0.
    """
    backup_fips = _require_fips_container(self)
    cluster = self.context.cluster
    listeners_xml = os.path.join(
        cluster.tests_dir,
        "configs/clickhouse_fips_server/config.d/listeners.xml",
    )

    try:
        with Given("a dedicated FIPS-compatible Altinity ClickHouse server"):
            cluster.start_clickhouse_server_container(
                name=FIPS_CH_SERVER_NAME,
                image_tag=FIPS_CH_SERVER_IMAGE,
                extra_volumes=[
                    (cluster.ssl_certs_dir, "/etc/clickhouse-server/ssl"),
                    (listeners_xml, "/etc/clickhouse-server/config.d/listeners.xml"),
                ],
            )
        with When("I run `clickhouse-backup-fips tables` over secure native TCP 9440"):
            _assert_tables_succeeds(
                backup_fips,
                config=FIPS_CONNECTIVITY_FIPS_CONFIG_PATH,
                godebug="fips140=only",
            )
    finally:
        cluster.stop_auxiliary_container(FIPS_CH_SERVER_NAME)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Connectivity_NonFIPSEndpoint("1.0")
)
def connectivity_against_non_fips_clickhouse_server(self):
    """Validate `clickhouse-backup-fips tables` against a non-FIPS ClickHouse server.

    Brings up a dedicated `altinity/clickhouse-server:25.8.16.10002.altinitystable`
    container (image defaults: plain native TCP `9000`, no TLS), then runs
    `clickhouse-backup-fips -c <config> tables` from the FIPS backup
    container with `GODEBUG=fips140=on`. Exit code MUST be 0 - the FIPS
    binary stays interoperable with a non-FIPS server in `on` mode (the
    strict `only` mode is reserved for FIPS-vs-FIPS).
    """
    backup_fips = _require_fips_container(self)
    cluster = self.context.cluster

    try:
        with Given("a dedicated non-FIPS Altinity ClickHouse server"):
            cluster.start_clickhouse_server_container(
                name=NON_FIPS_CH_SERVER_NAME,
                image_tag=NON_FIPS_CH_SERVER_IMAGE,
            )
        with When("I run `clickhouse-backup-fips tables` over plain native TCP 9000"):
            _assert_tables_succeeds(
                backup_fips,
                config=FIPS_CONNECTIVITY_NONFIPS_CONFIG_PATH,
                godebug="fips140=on",
            )
    finally:
        cluster.stop_auxiliary_container(NON_FIPS_CH_SERVER_NAME)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_Integrity("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_TamperedBinary("1.0"),
)
def fips_integrity_self_test_failure_on_tampered_binary(self):
    """Validate the FIPS startup integrity self-test rejects a tampered binary.

    Run ``scripts/tamper_go_fips_checksum.sh`` (bind-mounted into the FIPS
    container at ``/scripts/``) against ``clickhouse-backup-fips``. The
    script makes a temporary copy of the ELF, flips one byte inside the
    Go FIPS module checksum (``.go.fipsinfo`` section, HMAC offset), runs
    the tampered copy with ``GODEBUG=fips140=on``, and exits ``0`` only
    when the binary aborts with ``panic: fips140: verification mismatch``.

    The host-side FIPS binary is bind-mounted read-only, so the original
    is never modified and other FIPS scenarios are unaffected.
    """
    backup_fips = _require_fips_container(self)

    with When("I run the FIPS checksum tamper script against the FIPS binary"):
        r = backup_fips.cmd(
            f"/scripts/tamper_go_fips_checksum.sh {FIPS_BINARY_IN_CONTAINER}",
            no_checks=True,
        )

    with Then("the tampered binary panics with `fips140: verification mismatch`"):
        assert "fips140: verification mismatch" in (r.output or ""), error(
            f"tamper script did not see `fips140: verification mismatch` in the "
            f"tampered binary's output (script exit={r.exitcode}).\n{r.output}"
        )

    with Then("the tamper script exits 0 (tampered binary exited non-zero as required)"):
        assert r.exitcode == 0, error(
            f"tamper script exit={r.exitcode} (expected 0). See script docstring "
            f"for the meaning of non-zero exit codes.\n{r.output}"
        )


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
    the static TLS-API config at ``/etc/clickhouse-backup/fips/config-fips-api-tls.yml``,
    then runs ``openssl s_client`` from inside the same container to try a
    TLS connection against the listener on ``localhost:7172`` for each
    cipher / cipher suite / protocol the SRS calls out, and asserts:

    * FIPS-approved TLSv1.3 / TLSv1.2 cipher suites complete the handshake.
    * Non-approved suites (ChaCha20-Poly1305, RC4, 3DES, etc.) are rejected.
    * Legacy protocols (TLSv1.0, TLSv1.1) are rejected.

    The TLS API config is a
    static file committed under ``configs/backup/`` and bind-mounted by the 
    copy of that directory; the certificate / key referenced by it
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
            for cipher in NON_FIPS_TLS12_INBOUND_REST:
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
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_ClickHouseEndpoint_Ciphers_Approved("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_ClickHouseEndpoint_NonApprovedCiphers_Reject("1.0"),
)
def outbound_tls_cipher_negotiation(self):
    """Validate outbound TLS policy when `clickhouse-backup-fips` connects to ClickHouse over TLS.

    For each cipher / cipher suite from the shared FIPS-approved /
    non-approved lists, start `openssl s_server` on the secure native
    ClickHouse port `9440` with that profile and run:

        `GODEBUG=fips140=only clickhouse-backup-fips -c <config> tables`

    inside the FIPS container, asserting that:

    * approved profiles - the TLS handshake is NOT rejected by the FIPS
      policy.

    * non-approved profiles - the FIPS client refuses the handshake with
      `remote error: tls: handshake failure` / `no shared cipher`.

    The ``s_server`` container reuses the cluster's static SSL configuration and
    the `clickhouse-backup-fips` config is the static
    `configs/backup/fips/config-fips-outbound-clickhouse-tls.yml` with
    `skip_verify: true` so the assertion stays focused on cipher policy.
    """
    backup_fips = _require_fips_container(self)
    cluster = self.context.cluster

    cmd = (
        f"env GODEBUG=fips140=only {FIPS_BINARY_IN_CONTAINER} "
        f"-c {FIPS_OUTBOUND_CH_CONFIG_PATH} tables 2>&1"
    )

    with When("I try each FIPS-approved TLSv1.3 cipher suite on the CH endpoint"):
        for ciphersuite in FIPS_TLS13_APPROVED:
            with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be accepted"):
                _check_outbound_tls_with_cipher(
                    cluster, backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=True,
                )

    with And("I try each FIPS-approved TLSv1.2 cipher on the CH endpoint"):
        for cipher in FIPS_TLS12_APPROVED:
            with Check(f"TLSv1.2 cipher {cipher} should be accepted"):
                _check_outbound_tls_with_cipher(
                    cluster, backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_2",
                    cipher=cipher, command=cmd, expected_success=True,
                )

    with And("I try each non-FIPS TLSv1.3 cipher suite on the CH endpoint"):
        for ciphersuite in NON_FIPS_TLS13:
            with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be rejected"):
                _check_outbound_tls_with_cipher(
                    cluster, backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=False,
                )

    with And("I try each non-FIPS TLSv1.2 cipher on the CH endpoint"):
        for cipher in NON_FIPS_TLS12_OUTBOUND:
            with Check(f"TLSv1.2 cipher {cipher} should be rejected"):
                _check_outbound_tls_with_cipher(
                    cluster, backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_2",
                    cipher=cipher, command=cmd, expected_success=False,
                )


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_S3_Ciphers_Approved("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_S3_NonApprovedCiphers_Reject("1.0"),
)
def outbound_tls_to_s3_endpoint_with_openssl_s_server(self):
    """Validate outbound TLS policy when `clickhouse-backup-fips` connects to an S3 endpoint over HTTPS.

    For each cipher / cipher suite from the shared FIPS-approved /
    non-approved lists, start ``openssl s_server`` with that profile and run:

        GODEBUG=fips140=only clickhouse-backup-fips -c <config> list remote

    inside the FIPS container, asserting that:

    * approved profiles - the TLS handshake is NOT rejected by the FIPS
      policy (downstream HTTP / S3 protocol errors from ``s_server -www``
      are acceptable - it is not a real S3 API).
    * non-approved profiles - the FIPS client refuses the handshake with
      ``remote error: tls: handshake failure`` / ``no shared cipher``.

    The ``s_server`` container reuses the cluster's static SSL fixtures
    and the ``clickhouse-backup-fips`` config is the static
    ``configs/backup/fips/config-fips-outbound-s3-tls.yml`` with
    ``s3.disable_cert_verification: true`` so the assertion stays
    focused on cipher policy.
    """
    backup_fips = _require_fips_container(self)
    cluster = self.context.cluster

    # `env GODEBUG=...` so the `timeout` prefix added inside
    # `_check_outbound_tls_handshake` can exec a real program.
    # cmd - command to run inside the FIPS container
    cmd = (
        f"env GODEBUG=fips140=only {FIPS_BINARY_IN_CONTAINER} "
        f"-c {FIPS_OUTBOUND_S3_CONFIG_PATH} list remote 2>&1"
    )

    with When("I try each FIPS-approved TLSv1.3 cipher suite on the S3 endpoint"):
        for ciphersuite in FIPS_TLS13_APPROVED:
            with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be accepted"):
                _check_outbound_tls_with_cipher(
                    cluster, backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=True,
                )

    with And("I try each FIPS-approved TLSv1.2 cipher on the S3 endpoint"):
        for cipher in FIPS_TLS12_APPROVED:
            with Check(f"TLSv1.2 cipher {cipher} should be accepted"):
                _check_outbound_tls_with_cipher(
                    cluster, backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_2",
                    cipher=cipher, command=cmd, expected_success=True,
                )

    with And("I try each non-FIPS TLSv1.3 cipher suite on the S3 endpoint"):
        for ciphersuite in NON_FIPS_TLS13:
            with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be rejected"):
                _check_outbound_tls_with_cipher(
                    cluster, backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=False,
                )

    with And("I try each non-FIPS TLSv1.2 cipher on the S3 endpoint"):
        for cipher in NON_FIPS_TLS12_OUTBOUND:
            with Check(f"TLSv1.2 cipher {cipher} should be rejected"):
                _check_outbound_tls_with_cipher(
                    cluster, backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_2",
                    cipher=cipher, command=cmd, expected_success=False,
                )


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_CAST_ForcedFailure("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_CAST_Coverage("1.0"),
)
def forced_cast_failures(self):
    """Validate forced CAST failures with `GODEBUG=failfipscast=<NAME>,fips140=on`.

    For each CAST name in `FIPS_FAILFIPSCAST_NAMES` (mirrors Go's upstream
    `allCASTs` slice), run

        env 'GODEBUG=failfipscast=<NAME>,fips140=on' clickhouse-backup-fips --version

    inside the FIPS container and classify the outcome:

    * Abort with `fatal error: FIPS 140-3 self-test failed: <NAME>:
      simulated CAST failure` - the hook forced this CAST (PASS).
    * Normal startup with exit 0 - Go silently ignores the failfipscast
      value because `<NAME>` is not registered in the active Go FIPS
      module (`GOFIPS140=v1.0.0` ships a subset of the upstream list).
      Names listed in `FIPS_FAILFIPSCAST_REQUIRED_NAMES` MUST always
      abort; any other unrecognised name marks the Check as Skip.
    * Anything else - FAIL.

    After the loop the scenario asserts at least one CAST was forced, so
    a binary with the failfipscast hook fully disabled cannot pass on
    skips alone.
    """
    backup_fips = _require_fips_container(self)

    forced_count = 0

    with When(
        "for each CAST name I run `clickhouse-backup-fips --version` "
        "with `GODEBUG=failfipscast=<NAME>,fips140=on`"
    ):
        for cast in FIPS_FAILFIPSCAST_NAMES:
            with Check(f"forced CAST failure: {cast}"):
                # Single-quote the GODEBUG value so CAST names containing
                # spaces (e.g. `DetECDSA P-256 SHA2-512 sign`) are passed
                # through as one argument to `env`. `2>&1` because Go writes
                # `fatal error:` lines to stderr.
                cmd = (
                    f"env 'GODEBUG=failfipscast={cast},fips140=on' "
                    f"{FIPS_BINARY_IN_CONTAINER} --version 2>&1"
                )
                r = backup_fips.cmd(cmd, no_checks=True)
                output = r.output or ""

                marker_present = FIPS_FAILFIPSCAST_MARKER in output
                cast_in_output = cast in output

                if r.exitcode != 0 and marker_present and cast_in_output:
                    forced_count += 1
                    continue

                if r.exitcode == 0 and not marker_present:
                    # Go's failfipscast silently ignores unknown CAST names.
                    # Required CASTs must always be registered; everything
                    # else is treated as "not in this module version" and
                    # the Check is skipped.
                    if cast in FIPS_FAILFIPSCAST_REQUIRED_NAMES:
                        assert False, error(
                            f"required CAST `{cast}` was not forced "
                            f"(exit=0, no `{FIPS_FAILFIPSCAST_MARKER}` "
                            f"marker). This name has been registered in "
                            f"every Go FIPS module since v1.0.0; if it "
                            f"does not abort the binary, the failfipscast "
                            f"hook is broken or the binary is not running "
                            f"in FIPS mode.\n{output}"
                        )
                    skip(
                        f"CAST `{cast}` is not registered in the Go FIPS "
                        f"module the binary was built against "
                        f"(`GOFIPS140=v1.0.0`); refresh "
                        f"`FIPS_FAILFIPSCAST_NAMES` after a Go upgrade."
                    )

                # Any other combination is an unexpected failure mode
                # (non-zero exit without the marker, or marker without
                # the cast name).
                assert False, error(
                    f"unexpected outcome for `failfipscast={cast}` "
                    f"(exit={r.exitcode}, marker={marker_present}, "
                    f"cast_in_output={cast_in_output}). Expected either "
                    f"a forced CAST abort or a normal startup with exit=0."
                    f"\n{output}"
                )

    with Then("at least one CAST was actually forced"):
        assert forced_count > 0, error(
            f"no CAST in `FIPS_FAILFIPSCAST_NAMES` aborted the binary "
            f"(forced_count=0). The `failfipscast` enforcement appears "
            f"to be disabled."
        )


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_ACVP_Wrapper("1.0"),
)
def acvp_tests(self):
    """Validate the bundled ACVP wrapper (`pkg/acvpwrapper/run.sh`).

    Runs the wrapper on the host (the script itself orchestrates Docker)
    and asserts it exits 0 and prints the expected line tracked
    in `FIPS_ACVP_EXPECTED_OUTPUT`.

    Opt-in: skipped unless `RUN_ACVP_TESTS=1` is set.
    """
    flag = os.environ.get(FIPS_ACVP_ENV_FLAG, "").strip().lower()
    if flag not in FIPS_ACVP_ENV_FLAG_VALUES:
        skip(
            f"set {FIPS_ACVP_ENV_FLAG}=1 to enable; the wrapper pulls "
            f"Docker images and clones upstream repos."
        )

    cluster = self.context.cluster
    script_path = os.path.normpath(os.path.join(cluster.tests_dir, FIPS_ACVP_SCRIPT_RELPATH))

    if not os.path.isfile(script_path):
        skip(f"ACVP wrapper script not found at {script_path}")

    # The wrapper drives `docker run` itself, so it must execute on the
    # host, not inside a cluster container. The first invocation pulls
    # images and clones boringssl + acvp-testdata, hence the extended
    # timeout.
    host = cluster.bash(None)
    prev_timeout = host.timeout
    host.timeout = FIPS_ACVP_TIMEOUT_SEC
    try:
        with When(f"I run `bash {script_path}` on the host"):
            r = host(f"bash {script_path} 2>&1")
    finally:
        host.timeout = prev_timeout

    output = r.output or ""

    with Then("the script exits 0"):
        assert r.exitcode == 0, error(
            f"`bash {script_path}` exit={r.exitcode}.\n{output}"
        )

    with And(f"output contains `{FIPS_ACVP_EXPECTED_OUTPUT}`"):
        assert FIPS_ACVP_EXPECTED_OUTPUT in output, error(
            f"ACVP run did not print `{FIPS_ACVP_EXPECTED_OUTPUT}`. "
            f"See `pkg/acvpwrapper/README.md` for the expected oracle.\n{output}"
        )

@TestFeature
@Name("FIPS 140-3 Compatibility")
def fips_140_3(self):
    """FIPS 140-3 automation entrypoint for clickhouse-backup.
    """
    Scenario(run=clickhouse_backup_fips_version_output, flags=TE) # done
    Scenario(run=clickhouse_backup_fips_version_output_negative_check, flags=TE) # done
    Scenario(run=gofips140_build_flags_present, flags=TE) # done
    Scenario(run=godebug_fips140_modes, flags=TE)
    Scenario(run=connectivity_against_non_fips_clickhouse_server, flags=TE) # done
    Scenario(run=connectivity_against_fips_clickhouse_server, flags=TE) # done
    Scenario(run=fips_integrity_self_test_failure_on_tampered_binary, flags=TE) # done
    Scenario(run=inbound_tls_cipher_negotiation, flags=TE) # done
    Scenario(run=outbound_tls_cipher_negotiation, flags=TE) # done
    Scenario(run=outbound_tls_to_s3_endpoint_with_openssl_s_server, flags=TE) # done
    Scenario(run=forced_cast_failures, flags=TE) # done
    Scenario(run=acvp_tests, flags=TE) # done


if main():
    fips_140_3()
