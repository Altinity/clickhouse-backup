import os
import sys

from testflows.core import *
from testflows.asserts import error
append_path(sys.path, "../..")
from clickhouse_backup.requirements.fips.requirements import *

 # the following are common fips variables:
FIPS_BINARY_IN_CONTAINER = "/bin/clickhouse-backup-fips"
FIPS_VERSION_LABEL = "FIPS 140-3:"
FIPS_VERSION_TRUE = "true"
FIPS_CH_SERVER_NAME = "clickhouse_fips_server"
NON_FIPS_CH_SERVER_NAME  = "clickhouse_nonfips_server"

# clickhouse-backup-fips configs:
# ``configs/backup/fips/`` subfolder; ``regression.py`` copies the whole
# ``configs/backup/`` tree per-PID and bind-mounts it at
# ``/etc/clickhouse-backup``, so the in-container path keeps the subdir.
FIPS_TLS_CONFIG_PATH = "/etc/clickhouse-backup/fips/config-fips-api-tls.yml"
FIPS_OUTBOUND_CH_CONFIG_PATH = "/etc/clickhouse-backup/fips/config-fips-outbound-clickhouse-tls.yml"
FIPS_OUTBOUND_S3_CONFIG_PATH = "/etc/clickhouse-backup/fips/config-fips-outbound-s3-tls.yml"
FIPS_CONNECTIVITY_FIPS_CONFIG_PATH = "/etc/clickhouse-backup/fips/config-fips-connectivity-fips-server.yml"
FIPS_CONNECTIVITY_NONFIPS_CONFIG_PATH = "/etc/clickhouse-backup/fips/config-fips-connectivity-nonfips-server.yml"
FIPS_TLS_LISTEN_PORT = 7172

# Backup configs for `outbound_tls_to_nonfips_clickhouse_with_cipher_profile`.
# Each one points at the non-FIPS CH server on its secure native port `9440`,
# with `secure: true` and `skip_verify: true` so the assertion stays focused
# on cipher-policy outcomes; the server's `<openSSL><cipherList>` /
# `<cipherSuites>` are switched per case via `listeners-*.xml` overrides.
FIPS_NONFIPS_CH_TLS_FIPSCIPHER_CONFIG_PATH      = "/etc/clickhouse-backup/fips/config-fips-nonfips-ch-tls-fipscipher.yml"
FIPS_CH_SERVER_IMAGE                        = "altinity/clickhouse-server:25.3.8.30001.altinityfips"
NON_FIPS_CH_SERVER_IMAGE                    = "altinity/clickhouse-server:25.8.16.10002.altinitystable"

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

# Non-FIPS TLS1.2 suites for the outbound verification.
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

CLI_CMD_TIMEOUT_SEC = 15 # Timeout for clickhouse-backup-fips command runs.

def _require_fips_container(test):
    """Skip the calling scenario if no FIPS backup container is available.

    The `clickhouse_backup_fips` container is provisioned by
    `helpers.cluster.Cluster._do_up` only when the host-side FIPS binary
    `clickhouse-backup/clickhouse-backup-race-fips` exists.
    """
    backup_fips = getattr(test.context, "backup_fips", None)
    if backup_fips is None:
        skip(
            "clickhouse-backup-race-fips binary not available on host; "
            "build it with `make build-race-fips-docker` to enable FIPS scenarios."
        )
    return backup_fips


def _read_fips_status(node, binary, *, godebug=None):
    """Run `<binary> --version` on `node` and return `(status, output)`.

    `status` is the lower-cased value after `FIPS 140-3:` in the
    `--version` output, or `None` if the line is absent. `output` is
    the full command output for use in error messages.

    :param godebug: optional value for `GODEBUG` (e.g. `"fips140=only"`);
        pass `None` (default) to leave `GODEBUG` unset and rely on the
        FIPS binary's build-time default.
    """
    env_prefix = f"env GODEBUG={godebug} " if godebug else ""
    result = node.cmd(f"{env_prefix}{binary} --version")
    for line in result.output.splitlines():
        normalized = line.strip()
        if normalized.lower().startswith(FIPS_VERSION_LABEL.lower()):
            value = normalized.split(":", 1)[1].strip().lower() if ":" in normalized else ""
            return value, result.output
    return None, result.output


@TestStep(Check)
def _check_tls_handshake(self, node, target, tls_flag, cipher=None, ciphersuites=None,
                         expected_success=True):
    """Try to open a TLS connection with `openssl s_client` and check the result.

    The function runs the OpenSSL command-line client inside `node`, points
    it at `target` (`host:port`), forces a specific TLS protocol version
    (`tls_flag`) and optionally restricts the cipher / cipher suite that
    the client is allowed to offer. It then asserts that the TLS handshake
    either completes (`expected_success=True`) or is rejected
    (`expected_success=False`).

    :param node: cluster Node from which `openssl` is invoked (the
        `clickhouse_backup_fips` container, which has `openssl`
        installed and shares the docker network with the API listener).
    :param target: `host:port` of the TLS server, e.g. `localhost:7172`.
    :param tls_flag: `openssl s_client` protocol switch:
        `-tls1`, `-tls1_1`, `-tls1_2` or `-tls1_3`.
    :param cipher: TLSv1.2 OpenSSL cipher name (use with `-tls1_2`),
        e.g. `ECDHE-RSA-AES128-GCM-SHA256`.
    :param ciphersuites: TLSv1.3 IANA suite name (use with `-tls1_3`),
        e.g. `TLS_AES_128_GCM_SHA256`.
    :param expected_success: `True` if the handshake should complete,
        `False` if the server is expected to reject it.
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
        f"{tls_flag} {cipher_arg} </dev/null 2>&1"  # `2>&1` redirects stderr to stdout.
    )
    result = node.cmd(cmd, no_checks=True)
    output = (result.output or "")
    output_lower = output.lower()

    cipher_unavailable = (
        "no cipher match" in output_lower
        or "error setting cipher list" in output_lower
        or "no ciphers available" in output_lower
        or "cipher_list" in output_lower and "no cipher" in output_lower
    )

    handshake_failed = result.exitcode != 0 and (
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
        assert result.exitcode == 0, error(
            f"Expected handshake to succeed, exit={result.exitcode}\n{output}"
        )
    else:
        assert handshake_failed, error(
            f"Expected handshake rejection, exit={result.exitcode}\n{output}"
        )


@TestStep(Check)
def _check_outbound_tls_handshake(self, node, command, expected_success):
    """Run `command` inside `node` and assert on the *outbound* TLS outcome.

    `command` invokes `clickhouse-backup-fips` with
    `GODEBUG=fips140=only` against an `openssl s_server` whose offered
    cipher we want the FIPS policy to accept or reject. The function wraps
    `command` in `timeout <N>` so the retry loop is bounded; the check
    is **TLS-policy-only**:

    * `expected_success=True`  - the FIPS policy must NOT reject the
      handshake.

    * `expected_success=False` - the FIPS policy MUST reject the
      handshake.
    """
    bounded = f"timeout {CLI_CMD_TIMEOUT_SEC} {command}"
    result = node.cmd(bounded, no_checks=True)
    output = (result.output or "")
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


@TestStep(Check)
def _assert_s_server_logs_match_outcome(self, cluster, aux_name, expected_success):
    """Check the s_server's stdout confirms the client-side outcome.

    The client probe only sees what `clickhouse-backup-fips` printed, so this
    helper reads the s_server container's stdout and looks for handshake
    markers. Mismatches are reported via `note(...)` since
    OpenSSL log formatting varies across Alpine builds and TLS versions.

    Variables:
      * `cluster`           - test Cluster, used to fetch container logs.
      * `aux_name`          - name of the auxiliary openssl s_server container.
      * `expected_success`  - True if the handshake should have negotiated
                                a cipher; False if it should have been rejected.
      * `s_server_log`      - captured stdout of the s_server container.
      * `success_markers`   - substrings that prove cipher negotiation
                                completed (e.g. `Cipher : <name>`).
      * `failure_markers`   - substrings that prove the server rejected the
                                handshake (e.g. `no shared cipher`).
    """
    try:
        s_server_log = cluster.container_logs(aux_name)
    except Exception as exc:
        note(f"could not fetch s_server logs for `{aux_name}`: {exc}")
        return

    log_lower = (s_server_log or "").lower()
    success_markers = ("cipher    :", "cipher   :", "shared ciphers:")
    failure_markers = (
        "no shared cipher",
        "no suitable signature algorithm",
        "tlsv1 alert handshake failure",
        "ssl handshake failure",
        "wrong version number",
    )

    has_success_marker = any(marker in log_lower for marker in success_markers)
    has_failure_marker = any(marker in log_lower for marker in failure_markers)
    log_tail = s_server_log[-1000:] if s_server_log else "<empty>"

    if expected_success and not has_success_marker:
        note(
            f"s_server `{aux_name}` log lacks a cipher-negotiation marker even "
            f"though the client-side check passed. Tail of log:\n{log_tail}"
        )
    elif not expected_success and not has_failure_marker:
        note(
            f"s_server `{aux_name}` log lacks a handshake-rejection marker even "
            f"though the client-side check expected failure. Tail of log:\n{log_tail}"
        )


@TestStep(Check)
def _check_outbound_tls_with_cipher(self, cluster, backup_fips, *, listen, command,
                                    expected_success, tls_version,
                                    cipher=None, ciphersuites=None,
                                    aux_name='openssl_server'):
    """One outbound TLS handshake check for one cipher profile.

    Brings up an `openssl s_server` container, runs `command` inside
    the FIPS backup container, asserts the FIPS-policy outcome via
    `check_outbound_tls_handshake`, confirms the outcome by
    inspecting the s_server's stdout (`_assert_s_server_logs_match_outcome`),
    and stops the aux container in `Finally`. The aux container reuses
    the cluster's static SSL fixtures
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
        _check_outbound_tls_handshake(
            node=backup_fips,
            command=command,
            expected_success=expected_success,
        )
        _assert_s_server_logs_match_outcome(
            cluster=cluster,
            aux_name=aux_name,
            expected_success=expected_success,
        )
    finally:
        cluster.stop_auxiliary_container(aux_name)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GoCryptographicModule("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Binary("1.0"),
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
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Binary("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_Status("1.0"),
)
def clickhouse_backup_fips_version_output_negative_check(self):
    """Self-check for `clickhouse_backup_fips_version_output`.

    Run the same `--version` parser as in clickhouse_backup_fips_version_output 
    against the regular (non-FIPS) `clickhouse-backup` binary,
     which is exactly what `make build-race` produces. The status line must
    report `false`.
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
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Build_GOFIPS140("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_BuildSetting("1.0")
)
def gofips140_build_flags_present(self):
    """Validate `go version -m clickhouse-backup-fips` output contains `build\tGOFIPS140=v1.0.0`.

    Confirms the FIPS binary was built against the Go FIPS 140-3 module.

    The `go` toolchain is invoked inside the `clickhouse_backup_fips`
    container (where `golang-go` is installed by `helpers/cluster.py`).
    """

    FIPS_GO_BUILD_SETTING = "build\tGOFIPS140=v1.0.0"
    backup_fips = _require_fips_container(self)

    with When("I run `go version -m` against the FIPS binary"):
        result = backup_fips.cmd(f"go version -m {FIPS_BINARY_IN_CONTAINER}")
        debug(f"STDOUT:\n{result.output}")
        debug(f"EXIT CODE: {result.exitcode}")
        debug(f"FIPS_GO_BUILD_SETTING: {FIPS_GO_BUILD_SETTING!r}")

    with Then(f"the output contains the build setting `{FIPS_GO_BUILD_SETTING}`"):
        assert FIPS_GO_BUILD_SETTING in result.output, error(
            f"`go version -m {FIPS_BINARY_IN_CONTAINER}` output is missing "
            f"`{FIPS_GO_BUILD_SETTING}`:\n{result.output}"
        )


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GODEBUG_Unset("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GODEBUG_On("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_GODEBUG_Only("1.0"),
)
def godebug_fips140_modes(self):
    """Validate the three Go FIPS runtime modes for `clickhouse-backup-fips`.

    For each Go FIPS runtime mode listed in `FIPS_GODEBUG_MODES`:

    * `GODEBUG` not set      - FIPS active by build-time default (`GOFIPS140=v1.0.0`).
    * `GODEBUG=fips140=on`   - FIPS active without strict enforcement.
    * `GODEBUG=fips140=only` - FIPS active with strict enforcement
                               (expected error or panic).

    Run two checks against the FIPS-compatible Altinity ClickHouse
    server for each mode (for example `altinity/clickhouse-server:25.3.8.30001.altinityfips`) on
    secure native TCP `9440`:

    1. `clickhouse-backup-fips --version` reports `FIPS 140-3: true`.
    2. `clickhouse-backup-fips -c <config> tables` exits `0`.
    The CH server container is brought up once and reused across all three modes.
    """

    FIPS_GODEBUG_MODES = (None, "fips140=on", "fips140=only")
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

        with When("for each Go FIPS runtime mode I check `--version` and `tables`"):
            for godebug in FIPS_GODEBUG_MODES:
                label = godebug if godebug else "unset"  # Show "unset" when GODEBUG is not set.

                with Check(f"GODEBUG {label}"):
                    status, output = _read_fips_status(
                        backup_fips, FIPS_BINARY_IN_CONTAINER, godebug=godebug,
                    )

                    with Then(
                        f"`clickhouse-backup-fips --version` reports "
                        f"`{FIPS_VERSION_LABEL} {FIPS_VERSION_TRUE}` (GODEBUG={label})"
                    ):
                        assert status is not None, error(
                            f"`{FIPS_VERSION_LABEL}` line missing from "
                            f"`--version` (GODEBUG={label}):\n{output}"
                        )
                        assert status == FIPS_VERSION_TRUE, error(
                            f"Expected `{FIPS_VERSION_LABEL} {FIPS_VERSION_TRUE}` "
                            f"(GODEBUG={label}), got `{status}`.\n{output}"
                        )

                    with Then(
                        f"`clickhouse-backup-fips tables` exits 0 against the "
                        f"FIPS ClickHouse server (GODEBUG={label})"
                    ):
                        _assert_tables_succeeds(
                            backup_fips=backup_fips,
                            config=FIPS_CONNECTIVITY_FIPS_CONFIG_PATH,
                            godebug=godebug,
                        )
    finally:
        cluster.stop_auxiliary_container(FIPS_CH_SERVER_NAME)


@TestStep(Check)
def _assert_tables_succeeds(self, backup_fips, *, config, godebug):
    """Run `clickhouse-backup-fips -c <config> tables` and assert it exits 0.

    Wrapped in `timeout` so a hung handshake or DNS failure does not
    block the tests for a long time; on success the command is fast.

    :param godebug: value for `GODEBUG` (e.g. `"fips140=only"`); pass
        `None` to leave `GODEBUG` unset and rely on the FIPS binary's
        build-time default.
    """
    env_prefix = f"env GODEBUG={godebug} " if godebug else ""
    cmd = (
        f"timeout {CLI_CMD_TIMEOUT_SEC} {env_prefix}"
        f"{FIPS_BINARY_IN_CONTAINER} -c {config} tables"
    )
    result = backup_fips.cmd(cmd, no_checks=True)
    label = godebug if godebug else "unset"
    assert result.exitcode == 0, error(
        f"`clickhouse-backup-fips tables` against `{config}` failed "
        f"(exit={result.exitcode}, GODEBUG={label}):\n{result.output}"
    )


@TestStep(Check)
def _assert_tables_fails(self, backup_fips, *, config, godebug, reason):
    """Run `clickhouse-backup-fips -c <config> tables` and assert it fails.

    Negative counterpart to `_assert_tables_succeeds`: require non-zero
    exit and ensure no table-list success markers are present.
    """
    env_prefix = f"env GODEBUG={godebug} " if godebug else ""
    cmd = (
        f"timeout {CLI_CMD_TIMEOUT_SEC} {env_prefix}"
        f"{FIPS_BINARY_IN_CONTAINER} -c {config} tables 2>&1"  # `2>&1` redirects stderr to stdout.
    )
    result = backup_fips.cmd(cmd, no_checks=True)
    output = result.output or ""
    label = godebug if godebug else "unset"

    assert result.exitcode != 0, error(
        f"`clickhouse-backup-fips tables` unexpectedly succeeded against `{config}` "
        f"(GODEBUG={label}). Expected failure reason: {reason}\n{output}"
    )

    output_lower = output.lower()
    assert "atomic" not in output_lower and "ordinary" not in output_lower, error(
        f"`tables` output looks successful (database-listing marker present) "
        f"for `{config}`. Expected failure reason: {reason}\n{output}"
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
                backup_fips=backup_fips,
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
    """Validate FIPS clickhouse-backup cannot connect to a non-FIPS ClickHouse.

    Brings up a dedicated non-FIPS
    `altinity/clickhouse-server:25.8.16.10002.altinitystable` container
    with image defaults only (plain `tcp_port: 9000`, plain `http_port:
    8123`, no `tcp_port_secure` listener), then runs
    `clickhouse-backup-fips -c <config> tables` from the FIPS backup
    container with `GODEBUG=fips140=only` against a FIPS-compatible
    client config (`clickhouse.secure: true`, `clickhouse.port: 9440`).

    Expected result: the command exits with a non-zero code because the
    non-FIPS server does not expose the secure native TCP port required
    by the FIPS-compatible client config; no table-listing success
    marker appears.
    """
    backup_fips = _require_fips_container(self)
    cluster = self.context.cluster

    try:
        with Given(
            "a non-FIPS Altinity ClickHouse server running with image defaults",
            description="no `tcp_port_secure` listener; only plain `:9000` / `:8123` are bound",
        ):
            cluster.start_clickhouse_server_container(
                name=NON_FIPS_CH_SERVER_NAME,
                image_tag=NON_FIPS_CH_SERVER_IMAGE,
            )
        with When(
            "I run `clickhouse-backup-fips tables` with the FIPS-compatible "
            "client config (`secure: true`, `port: 9440`)"
        ):
            _assert_tables_fails(
                backup_fips=backup_fips,
                config=FIPS_CONNECTIVITY_NONFIPS_CONFIG_PATH,
                godebug="fips140=only",
                reason=(
                    "The non-FIPS ClickHouse server runs with image defaults "
                    "and does not expose the secure native TCP port `:9440` "
                    "required by the FIPS-compatible client config "
                    "(`secure: true`, `port: 9440`)."
                ),
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

    Run `scripts/tamper_go_fips_checksum.sh` (bind-mounted into the FIPS
    container at `/scripts/`) against `clickhouse-backup-fips`. The
    script makes a temporary copy of the ELF, flips one byte inside the
    Go FIPS module checksum (`.go.fipsinfo` section, HMAC offset), runs
    the tampered copy with `GODEBUG=fips140=on`, and exits `0` only
    when the binary aborts with `panic: fips140: verification mismatch`.

    The host-side FIPS binary is bind-mounted read-only, so the original
    is never modified and other FIPS scenarios are unaffected.
    """
    backup_fips = _require_fips_container(self)

    with When("I run the FIPS checksum tamper script against the FIPS binary"):
        result = backup_fips.cmd(
            f"/scripts/tamper_go_fips_checksum.sh {FIPS_BINARY_IN_CONTAINER}",
            no_checks=True,
        )
        output = result.output or ""

    with Then("the tampered binary output includes `panic: fips140: verification mismatch`"):
        # The script prints an informational banner containing the phrase
        # `fips140: verification mismatch` before it executes the binary.
        # Check for the panic marker here.

        assert "panic: fips140: verification mismatch" in output, error(
            f"tamper script did not capture "
            f"`panic: fips140: verification mismatch` in tampered binary output "
            f"(script exit={result.exitcode}).\n{output}"
        )

    with Then("the tamper script exits 0 (its success contract)"):
        assert result.exitcode == 0, error(
            f"tamper script exit={result.exitcode} (expected 0). See script docstring "
            f"for the meaning of non-zero exit codes.\n{output}"
        )

    with And("the tamper script reports its explicit success marker"):
        assert "== OK: FIPS integrity check failed as expected ==" in output, error(
            f"tamper script did not print the expected success marker.\n{output}"
        )


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Approved_TLSProtocolVersions("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Approved_CipherSuites_TLSv12_Approved("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Approved_CipherSuites_TLSv13_Approved("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_ApprovedCiphers("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_NonApprovedCiphers_Reject("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Inbound_RESTAPI_LegacyProtocols_Reject("1.0"),
)
def inbound_tls_cipher_negotiation(self):
    """Validate inbound TLS policy of the `clickhouse-backup-fips` REST API.

    Starts `clickhouse-backup-fips server` inside the dedicated
    `clickhouse_backup_fips` container with `GODEBUG=fips140=only` and
    the static TLS-API config at `/etc/clickhouse-backup/fips/config-fips-api-tls.yml`,
    then runs `openssl s_client` from inside the same container to try a
    TLS connection against the listener on `localhost:7172` for each
    cipher / cipher suite / protocol the SRS calls out, and asserts:

    * FIPS-approved TLSv1.3 / TLSv1.2 cipher suites complete the handshake.
    * Non-approved suites (e.g. ChaCha20-Poly1305, RC4, 3DES) are rejected.
    * Legacy protocols (TLSv1.0, TLSv1.1) are rejected.

    The TLS API config is a static
    file committed under `configs/backup/` and bind-mounted by the 
    copy of that directory; the certificate / key referenced by it
    are the same static fixtures used by the ClickHouse nodes
    (`/etc/clickhouse-server/ssl/server.{crt,key}`), inherited via
    `volumes_from_name="clickhouse1"` on the FIPS container.
    """
    # Non-FIPS TLS1.2 suites
    NON_FIPS_TLS12_INBOUND_REST = (
        "ECDHE-RSA-CHACHA20-POLY1305",
        "RC4-SHA",
        "DES-CBC3-SHA",
    )

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
            for ciphersuite in FIPS_TLS13_APPROVED:  # shared with outbound scenario
                with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be accepted"):
                    _check_tls_handshake(
                        node=backup_fips, target=target, tls_flag="-tls1_3",
                        ciphersuites=ciphersuite, expected_success=True,
                    )

        with And("I try to connect using each FIPS-approved TLSv1.2 cipher"):
            for cipher in FIPS_TLS12_APPROVED:  # shared with outbound scenario
                with Check(f"TLSv1.2 cipher {cipher} should be accepted"):
                    _check_tls_handshake(
                        node=backup_fips, target=target, tls_flag="-tls1_2",
                        cipher=cipher, expected_success=True,
                    )

        with And("I try to connect using each non-FIPS TLSv1.3 cipher suite"):
            for ciphersuite in NON_FIPS_TLS13:
                with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be rejected"):
                    _check_tls_handshake(
                        node=backup_fips, target=target, tls_flag="-tls1_3",
                        ciphersuites=ciphersuite, expected_success=False,
                    )

        with And("I try to connect using each non-FIPS TLSv1.2 cipher"):
            for cipher in NON_FIPS_TLS12_INBOUND_REST:
                with Check(f"TLSv1.2 cipher {cipher} should be rejected"):
                    _check_tls_handshake(
                        node=backup_fips, target=target, tls_flag="-tls1_2",
                        cipher=cipher, expected_success=False,
                    )

        with And("I try to connect using legacy TLSv1.0 / TLSv1.1 protocols and assert they are rejected"):
            with Check("TLSv1.0 handshake should be rejected"):
                _check_tls_handshake(
                    node=backup_fips, target=target, tls_flag="-tls1",
                    expected_success=False,
                )
            with Check("TLSv1.1 handshake should be rejected"):
                _check_tls_handshake(
                    node=backup_fips, target=target, tls_flag="-tls1_1",
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

    The `s_server` container reuses the cluster's static SSL configuration and
    the `clickhouse-backup-fips` config is the static
    `configs/backup/fips/config-fips-outbound-clickhouse-tls.yml` with
    `skip_verify: true` so the assertion stays focused on cipher policy.
    """
    
    FIPS_OUTBOUND_CH_TLS_PORT = 9440 # Standard ClickHouse secure native-TCP port

    
    backup_fips = _require_fips_container(self)
    cluster = self.context.cluster

    cmd = (
        f"env GODEBUG=fips140=only {FIPS_BINARY_IN_CONTAINER} "
        f"-c {FIPS_OUTBOUND_CH_CONFIG_PATH} tables 2>&1"  # `2>&1` redirects stderr to stdout.
    )

    with When("I try each FIPS-approved TLSv1.3 cipher suite on the CH endpoint"):
        for ciphersuite in FIPS_TLS13_APPROVED:
            with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be accepted"):
                _check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=True,
                )

    with And("I try each FIPS-approved TLSv1.2 cipher on the CH endpoint"):
        for cipher in FIPS_TLS12_APPROVED:
            with Check(f"TLSv1.2 cipher {cipher} should be accepted"):
                _check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_2",
                    cipher=cipher, command=cmd, expected_success=True,
                )

    with And("I try each non-FIPS TLSv1.3 cipher suite on the CH endpoint"):
        for ciphersuite in NON_FIPS_TLS13:
            with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be rejected"):
                _check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=False,
                )

    with And("I try each non-FIPS TLSv1.2 cipher on the CH endpoint"):
        for cipher in NON_FIPS_TLS12_OUTBOUND:
            with Check(f"TLSv1.2 cipher {cipher} should be rejected"):
                _check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
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
    non-approved lists, start `openssl s_server` with that profile and run:

        GODEBUG=fips140=only clickhouse-backup-fips -c <config> list remote

    inside the FIPS container, asserting that:

    * approved profiles - the TLS handshake is NOT rejected by the FIPS
      policy (downstream HTTP / S3 protocol errors from `s_server -www`
      are acceptable - it is not a real S3 API).
    * non-approved profiles - the FIPS client refuses the handshake with
      `remote error: tls: handshake failure` / `no shared cipher`.

    The `s_server` container reuses the cluster's static SSL fixtures
    and the `clickhouse-backup-fips` config is the static
    `configs/backup/fips/config-fips-outbound-s3-tls.yml` with
    `s3.disable_cert_verification: true` so the assertion stays
    focused on cipher policy.
    """
    # Default HTTPS port. The S3 outbound scenario must use 443 because the AWS
    # SDK Go v2 generates the FIPS endpoint URL as bare
    # `https://s3-fips.<region>.amazonaws.com` (no port suffix), so the
    # container has to be reachable on the default HTTPS port for the SDK's
    # request to land on it. See `config-fips-outbound-s3-tls.yml` for the
    # full reasoning behind the AWS-style hostname approach.
    FIPS_OUTBOUND_S3_TLS_PORT = 443

    # For the S3 outbound scenario the container is named after the AWS S3 FIPS
    # endpoint hostname so Docker's embedded DNS resolves the SDK-generated
    # URL (``https://s3-fips.us-east-1.amazonaws.com``) to it. ``us-east-1``
    # matches ``s3.region`` in ``config-fips-outbound-s3-tls.yml``.
    OPENSSL_S3_FIPS_AUX_NAME = "s3-fips.us-east-1.amazonaws.com"
    
    backup_fips = _require_fips_container(self)
    cluster = self.context.cluster

    # `env GODEBUG=...` so the `timeout` prefix added inside
    # `_check_outbound_tls_handshake` can exec a real program.
    # cmd - command to run inside the FIPS container
    cmd = (
        f"env GODEBUG=fips140=only {FIPS_BINARY_IN_CONTAINER} "
        f"-c {FIPS_OUTBOUND_S3_CONFIG_PATH} list remote 2>&1"  # `2>&1` redirects stderr to stdout.
    )

    with When("I try each FIPS-approved TLSv1.3 cipher suite on the S3 endpoint"):
        for ciphersuite in FIPS_TLS13_APPROVED:
            with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be accepted"):
                _check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=True,
                )

    with And("I try each FIPS-approved TLSv1.2 cipher on the S3 endpoint"):
        for cipher in FIPS_TLS12_APPROVED:
            with Check(f"TLSv1.2 cipher {cipher} should be accepted"):
                _check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_2",
                    cipher=cipher, command=cmd, expected_success=True,
                )

    with And("I try each non-FIPS TLSv1.3 cipher suite on the S3 endpoint"):
        for ciphersuite in NON_FIPS_TLS13:
            with Check(f"TLSv1.3 ciphersuite {ciphersuite} should be rejected"):
                _check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=False,
                )

    with And("I try each non-FIPS TLSv1.2 cipher on the S3 endpoint"):
        for cipher in NON_FIPS_TLS12_OUTBOUND:
            with Check(f"TLSv1.2 cipher {cipher} should be rejected"):
                _check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
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
    """Validate failfipscast behavior for CASTs reached by `--version`.

    This scenario intentionally tests only startup/core CAST names that are
    expected to be exercised during `clickhouse-backup-fips --version`.
    For each name, we first verify normal startup (`fips140=only`) and then
    verify forced failure (`failfipscast=<NAME>,fips140=only`).
    """
    # Source: Go FIPS `allCASTs` in `crypto/internal/fips140test/cast_test.go`.
    # Keep this scenario focused on startup/core CASTs that should fail
    # deterministically on `--version`.
    FIPS_FAILFIPSCAST_STARTUP_CASTS = (
        "AES-CBC",
        "CTR_DRBG",
        "CounterKDF",
        "HKDF-SHA2-256",
        "HMAC-SHA2-256",
        "PBKDF2",
        "SHA2-256",
        "SHA2-512",
        "TLSv1.2-SHA2-256",
        "TLSv1.3-SHA2-256",
        "cSHAKE128",
    )
    FIPS_FAILFIPSCAST_CONDITIONAL_CASTS = (  # first use casts
        "DetECDSA P-256 SHA2-512 sign",
        "ECDH PCT",
        "ECDSA P-256 SHA2-512 sign and verify",
        "ECDSA PCT",
        "Ed25519 sign and verify",
        "Ed25519 sign and verify PCT",
        "KAS-ECC-SSC P-256",
        "ML-DSA sign and verify PCT",
        "ML-DSA-44",
        "ML-KEM PCT",
        "ML-KEM PCT",
        "ML-KEM-768",
        "RSA sign and verify PCT",
        "RSASSA-PKCS-v1.5 2048-bit sign and verify",
    )
    # This is the marker that Go's FIPS module writes to stderr on a failfipscast-forced CAST.
    # The full line is:
    # `fatal error: FIPS 140-3 self-test failed: <NAME>: simulated CAST failure`.
    FIPS_FAILFIPSCAST_MARKER = "simulated CAST failure"
    FIPS_FAILFIPSCAST_SELFTEST_PREFIX = "self-test failed: "

    backup_fips = _require_fips_container(self)
    debug(
        f"startup CAST names ({len(FIPS_FAILFIPSCAST_STARTUP_CASTS)}): "
        f"{FIPS_FAILFIPSCAST_STARTUP_CASTS}"
    )
    debug(
        f"conditional CAST names ({len(FIPS_FAILFIPSCAST_CONDITIONAL_CASTS)}): "
        f"{FIPS_FAILFIPSCAST_CONDITIONAL_CASTS}"
    )

    with When(
        "I run `clickhouse-backup-fips --version` "
        "with `GODEBUG=fips140=only` as the positive check"
    ):
        cmd = (
            f"env GODEBUG=fips140=only "
            f"{FIPS_BINARY_IN_CONTAINER} --version 2>&1"
        )

        result = backup_fips.cmd(cmd, no_checks=True)
        output = result.output or ""

        assert result.exitcode == 0, error(
            f"baseline startup failed "
            f"(exit={result.exitcode}).\n{output}"
        )

        assert FIPS_FAILFIPSCAST_MARKER not in output, error(
            f"unexpected `{FIPS_FAILFIPSCAST_MARKER}` marker.\n{output}"
        )
        assert FIPS_VERSION_LABEL in output and FIPS_VERSION_TRUE in output.lower(), error(
            f"baseline `--version` output does not show expected FIPS status.\n{output}"
        )

    with Then(
        "for each startup CAST name I run `clickhouse-backup-fips --version` "
        "with `GODEBUG=failfipscast=<NAME>,fips140=only` and expect forced failure"
    ):
        for cast in FIPS_FAILFIPSCAST_STARTUP_CASTS:
            with Check(f"forced failure is reported for CAST `{cast}`"):
                # Single-quote the GODEBUG value so CAST names containing
                # spaces (e.g. `DetECDSA P-256 SHA2-512 sign`) are passed
                # through as one argument to `env`. `2>&1` because Go writes
                # `fatal error:` lines to stderr.
                cmd = (
                    f"env 'GODEBUG=failfipscast={cast},fips140=only' "
                    f"{FIPS_BINARY_IN_CONTAINER} --version 2>&1"
                )
                result = backup_fips.cmd(cmd, no_checks=True)
                output = result.output or ""

                marker_present = FIPS_FAILFIPSCAST_MARKER in output
                selftest_cast_present = (
                    f"{FIPS_FAILFIPSCAST_SELFTEST_PREFIX}{cast}" in output
                )

                assert result.exitcode != 0, error(
                    f"forced startup CAST failure expected non-zero exit for `{cast}` "
                    f".\n{output}"
                )
                assert marker_present, error(
                    f"forced startup CAST failure output missing "
                    f"`{FIPS_FAILFIPSCAST_MARKER}` for `{cast}` "
                    f".\n{output}"
                )
                assert selftest_cast_present, error(
                    f"forced startup CAST failure output missing "
                    f"`{FIPS_FAILFIPSCAST_SELFTEST_PREFIX}{cast}` "
                    f".\n{output}"
                )

    with And(
        "for each conditional CAST name I run `clickhouse-backup-fips --version` "
        "with `GODEBUG=failfipscast=<NAME>,fips140=only`"
    ):
        for cast in FIPS_FAILFIPSCAST_CONDITIONAL_CASTS:
            with Check(f"conditional CAST execution record for `{cast}`"):
                cmd = (
                    f"env 'GODEBUG=failfipscast={cast},fips140=only' "
                    f"{FIPS_BINARY_IN_CONTAINER} --version 2>&1"
                )
                result = backup_fips.cmd(cmd, no_checks=True)
                output = result.output or ""

                marker_present = FIPS_FAILFIPSCAST_MARKER in output
                selftest_cast_present = (
                    f"{FIPS_FAILFIPSCAST_SELFTEST_PREFIX}{cast}" in output
                )

                if result.exitcode != 0:
                    assert marker_present, error(
                        f"conditional CAST failure output missing "
                        f"`{FIPS_FAILFIPSCAST_MARKER}` for `{cast}` "
                        f".\n{output}"
                    )
                    assert selftest_cast_present, error(
                        f"conditional CAST failure output missing "
                        f"`{FIPS_FAILFIPSCAST_SELFTEST_PREFIX}{cast}` "
                        f".\n{output}"
                    )
                else:
                    assert not marker_present, error(
                        f"conditional CAST `{cast}` showed "
                        f"`{FIPS_FAILFIPSCAST_MARKER}` but exited 0.\n{output}"
                    )

@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Configuration_SecureClickHouse("1.0"),
)
def connection_to_fips_clickhouse_with_nonfips_config(self):
    """Validate that a non-FIPS-correct backup config cannot reach a FIPS CH server's secure port.

    Brings up the dedicated FIPS-compatible Altinity ClickHouse server
    (`altinity/clickhouse-server:25.3.8.30001.altinityfips`) with
    `tcp_port_secure: 9440` enabled by
    `configs/clickhouse_fips_server/config.d/listeners.xml`, then runs:

        env GODEBUG=fips140=only clickhouse-backup-fips -c <misconfig> tables

    where `<misconfig>` points at `clickhouse_fips_server:9440` with
    `secure: false` (i.e. the binary tries the plain native protocol
    against a TLS-only listener).

    Expected result:
    * The command exits with a non-zero code.
    * The output does NOT contain the list-of-tables success marker
      (no Atomic / Ordinary database row).
    """
    # Same FIPS CH host/port as the canonical FIPS connectivity config but with
    # ``secure: false`` - used by `connection_to_fips_clickhouse_with_nonfips_config`
    # to assert that plain native TCP into a TLS-only listener fails.
    FIPS_CONNECTIVITY_FIPS_MISCONFIG_PATH = "/etc/clickhouse-backup/fips/config-fips-connectivity-fips-server-misconfig.yml"
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

        with When(
            "I run `clickhouse-backup-fips tables` with the non-FIPS misconfig "
            "(secure: false) against the secure TLS port"
        ):
            cmd = (
                f"timeout {CLI_CMD_TIMEOUT_SEC} "
                f"env GODEBUG=fips140=only "
                f"{FIPS_BINARY_IN_CONTAINER} "
                f"-c {FIPS_CONNECTIVITY_FIPS_MISCONFIG_PATH} tables 2>&1"  # `2>&1` redirects stderr to stdout.
            )
            result = backup_fips.cmd(cmd, no_checks=True)
            output = result.output or ""

        with Then("the command fails (non-zero exit)"):
            assert result.exitcode != 0, error(
                f"`clickhouse-backup-fips tables` unexpectedly succeeded with the "
                f"non-FIPS misconfig (`secure: false`) against the FIPS CH server's "
                f"TLS-only :9440 listener. A FIPS-deployed CH server MUST NOT be "
                f"reachable via plain native TCP.\n{output}"
            )

        with And("the output does not include the list-of-tables success marker"):
            output_lower = output.lower()
            assert "atomic" not in output_lower and "ordinary" not in output_lower, error(
                f"`tables` produced what looks like a successful database listing "
                f"despite the non-FIPS misconfig. The connection should have failed "
                f"before any rows were returned.\n{output}"
            )
    finally:
        cluster.stop_auxiliary_container(FIPS_CH_SERVER_NAME)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Server_Listener("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_NetworkSurface("1.0"),
)
def server_listens_only_on_fips_api_port(self):
    """Validate `clickhouse-backup-fips server` listens on the FIPS REST API TLS port.

    Starts the FIPS server with `GODEBUG=fips140=only`, lists TCP listeners
    from the container's own net namespace via `/proc/net/tcp{,6}` (same
    source `ss` reads from; no `sudo` needed), and asserts that
    `FIPS_TLS_LISTEN_PORT` (`7172`) is in the listeners.
    """
    backup_fips = _require_fips_container(self)

    try:
        with Given("clickhouse-backup-fips server is started with GODEBUG=fips140=only"):
            backup_fips.start_server(
                binary=FIPS_BINARY_IN_CONTAINER,
                config=FIPS_TLS_CONFIG_PATH,
                extra_env={"GODEBUG": "fips140=only"},
                listen_port=FIPS_TLS_LISTEN_PORT,
                timeout=60,
            )

        with When(f"I check the server listens on port {FIPS_TLS_LISTEN_PORT}"):
            # /proc/net/tcp{,6}: column 2 is "IP:PORT" (hex), column 4 is state ("0A" = LISTEN).
            listeners = backup_fips.cmd(
                "awk 'NR>1 && $4==\"0A\" {print $2}' /proc/net/tcp /proc/net/tcp6" # sudo ss -ltnp | grep "pid=<binary-pid>" 
                # cannot be used because it requires sudo and is not available in the container
            ).output
            ports = [int(addr.rsplit(":", 1)[1], 16) for addr in listeners.splitlines()]

        with Then(f"port {FIPS_TLS_LISTEN_PORT} is in the listeners"):
            assert FIPS_TLS_LISTEN_PORT in ports, error(
                f"port {FIPS_TLS_LISTEN_PORT} not in listeners {ports}:\n{listeners}"
            )
    finally:
        with Finally("I stop the FIPS server"):
            backup_fips.stop_server()


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_ClickHouseEndpoint_Ciphers_Approved("1.0"),
)
def outbound_tls_to_nonfips_clickhouse_with_cipher_profile(self):
    """End-to-end smoke check: FIPS binary talks native-TLS to a non-FIPS ClickHouse.

    Brings up a non-FIPS Altinity ClickHouse on `tcp_port_secure: 9440`
    with its OpenSSL listener configured to offer a FIPS-approved cipher
    (`ECDHE-RSA-AES128-GCM-SHA256` / `TLS_AES_128_GCM_SHA256`), then runs
    `clickhouse-backup-fips tables` end-to-end and asserts the command
    succeeds (TLS + native CH protocol).

    Cipher-policy rejection (FIPS binary refusing non-approved ciphers) is
    covered deterministically by `outbound_tls_cipher_negotiation` using
    `openssl s_server`; replaying the negative case here would be flaky
    because ClickHouse's `<openSSL>` config merging does not strictly
    limit the offered cipher set.
    """
    backup_fips = _require_fips_container(self)
    cluster = self.context.cluster
    listeners_xml = os.path.join(
        cluster.tests_dir,
        "configs/clickhouse_nonfips_server/config.d/listeners-fips-cipher.xml",
    )

    try:
        with Given(
            "a non-FIPS Altinity ClickHouse server offering a FIPS-approved cipher",
            description=f"listeners.xml={listeners_xml}",
        ):
            cluster.start_clickhouse_server_container(
                name=NON_FIPS_CH_SERVER_NAME,
                image_tag=NON_FIPS_CH_SERVER_IMAGE,
                extra_volumes=[
                    (cluster.ssl_certs_dir, "/etc/clickhouse-server/ssl"),
                    (listeners_xml, "/etc/clickhouse-server/config.d/listeners.xml"),
                ],
            )

        with When("I run `clickhouse-backup-fips tables` over secure native TCP 9440"):
            cmd = (
                f"timeout {CLI_CMD_TIMEOUT_SEC} "
                f"env GODEBUG=fips140=only "
                f"{FIPS_BINARY_IN_CONTAINER} "
                f"-c {FIPS_NONFIPS_CH_TLS_FIPSCIPHER_CONFIG_PATH} tables 2>&1"
            )
            result = backup_fips.cmd(cmd, no_checks=True)
            output = result.output or ""

        with Then("the command succeeds (exit 0)"):
            assert result.exitcode == 0, error(
                f"`clickhouse-backup-fips tables` failed unexpectedly "
                f"(exit={result.exitcode}).\n{output}"
            )
    finally:
        cluster.stop_auxiliary_container(NON_FIPS_CH_SERVER_NAME)


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
    # ACVP wrapper scenario opt-in.
    # Set `RUN_ACVP_TESTS=1` locally or in the CI workflow to enable it.

    FIPS_ACVP_ENV_FLAG          = "RUN_ACVP_TESTS"
    FIPS_ACVP_ENV_FLAG_VALUES   = ("1", "true", "yes", "on")
    # Path to the wrapper script, relative to `cluster.tests_dir`
    # (`test/testflows/clickhouse_backup`). The script is part of the ACVP wrapper 
    # and lives at the repository root.
    FIPS_ACVP_SCRIPT_RELPATH    = "../../../pkg/acvpwrapper/run.sh"
    # Oracle line printed by `check_expected.go` when the run passes. Tracked
    # in `pkg/acvpwrapper/README.md` ("Reproduce The Current Result").
    FIPS_ACVP_EXPECTED_OUTPUT   = "38 ACVP tests matched expectations"
    # Timeout for the host shell - first run does Docker image pulls,
    # a boringssl clone, an acvptool build, and then the ACVP run itself.
    FIPS_ACVP_TIMEOUT_SEC       = 30 * 60
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
    # images and clones boringssl + acvp-testdata, hence the extended timeout.
    host = cluster.bash(None)
    prev_timeout = host.timeout
    host.timeout = FIPS_ACVP_TIMEOUT_SEC
    try:
        with When(f"I run `bash {script_path}` on the host"):
            result = host(f"bash {script_path} 2>&1")  # `2>&1` redirects stderr to stdout.
    finally:
        host.timeout = prev_timeout

    output = result.output or ""

    with Then("the script exits 0"):
        assert result.exitcode == 0, error(
            f"`bash {script_path}` exit={result.exitcode}.\n{output}"
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
    Scenario(run=clickhouse_backup_fips_version_output, flags=TE)
    Scenario(run=clickhouse_backup_fips_version_output_negative_check, flags=TE)
    Scenario(run=gofips140_build_flags_present, flags=TE)
    Scenario(run=godebug_fips140_modes, flags=TE)  # move to regression.py
    Scenario(run=connectivity_against_non_fips_clickhouse_server, flags=TE)
    Scenario(run=connectivity_against_fips_clickhouse_server, flags=TE)
    Scenario(run=fips_integrity_self_test_failure_on_tampered_binary, flags=TE)
    Scenario(run=inbound_tls_cipher_negotiation, flags=TE)
    Scenario(run=outbound_tls_cipher_negotiation, flags=TE)
    Scenario(run=outbound_tls_to_s3_endpoint_with_openssl_s_server, flags=TE)
    Scenario(run=outbound_tls_to_nonfips_clickhouse_with_cipher_profile, flags=TE) # new
    Scenario(run=connection_to_fips_clickhouse_with_nonfips_config, flags=TE) # new
    Scenario(run=server_listens_only_on_fips_api_port, flags=TE) # new
    Scenario(run=forced_cast_failures, flags=TE)
    Scenario(run=acvp_tests, flags=TE) # optional, `RUN_ACVP_TESTS=1` (see `pkg/acvpwrapper/README.md`)


if main():
    fips_140_3()
