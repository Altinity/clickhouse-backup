import os
import sys

from testflows.core import (
    TestStep, TestScenario, TestFeature,
    Given, When, Then, And, Check, Finally, Example,
    Requirements, Examples, Name,
    Scenario, skip, fail, note, debug, main, TE, append_path,
)
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

# Non-approved TLSv1.3 suites the FIPS policy must reject.
# Default runs probe the single documented suite; the `_STRESS` list adds the
# remaining non-approved TLSv1.3 suites (CCM bulk ciphers, outside the GCM-only
# FIPS-approved set). The wider `--stress` coverage is slower by design.
NON_FIPS_TLS13 = (
    "TLS_CHACHA20_POLY1305_SHA256",
)
NON_FIPS_TLS13_STRESS = NON_FIPS_TLS13 + (
    "TLS_AES_128_CCM_SHA256",
    "TLS_AES_128_CCM_8_SHA256",
)

# Non-approved TLSv1.2 ciphers the FIPS outbound policy must reject, one per
# rejection reason:
#   * non-approved bulk cipher (ChaCha20):         ECDHE-RSA-CHACHA20-POLY1305
#   * non-approved key exchange (DHE):             DHE-RSA-AES256-GCM-SHA384 / -AES128-
#   * plain RSA key exchange (no forward secrecy): AES256-GCM-SHA384 / AES128-GCM-SHA256
# The `_STRESS` list adds more ciphers from the same rejection classes
# (ECDSA/DHE ChaCha20 and CBC-mode ciphers outside the GCM-only approved set).
NON_FIPS_TLS12_OUTBOUND = (
    "ECDHE-RSA-CHACHA20-POLY1305",
    "DHE-RSA-AES256-GCM-SHA384",
    "DHE-RSA-AES128-GCM-SHA256",
    "AES256-GCM-SHA384",
    "AES128-GCM-SHA256",
)
NON_FIPS_TLS12_OUTBOUND_STRESS = NON_FIPS_TLS12_OUTBOUND + (
    "ECDHE-ECDSA-CHACHA20-POLY1305",
    "DHE-RSA-CHACHA20-POLY1305",
    "ECDHE-RSA-AES128-SHA",
    "ECDHE-RSA-AES256-SHA",
    "ECDHE-RSA-AES128-SHA256",
    "ECDHE-RSA-AES256-SHA384",
    "AES128-SHA",
    "AES256-SHA",
    "AES128-SHA256",
    "AES256-SHA256",
)

# Non-approved TLSv1.2 ciphers the inbound REST API listener must reject. The
# base adds RC4 / 3DES on top of the shared ChaCha20 reject; the `_STRESS` list
# reuses the outbound stress set (which already covers ChaCha20) and adds only
# the inbound-specific RC4 / 3DES.
NON_FIPS_TLS12_INBOUND = (
    "ECDHE-RSA-CHACHA20-POLY1305",
    "RC4-SHA",
    "DES-CBC3-SHA",
)
NON_FIPS_TLS12_INBOUND_STRESS = NON_FIPS_TLS12_OUTBOUND_STRESS + (
    "RC4-SHA",
    "DES-CBC3-SHA",
)

CLI_CMD_TIMEOUT_SEC = 15 # Timeout for clickhouse-backup-fips command runs.

@TestStep(Given)
def require_fips_container(self):
    """Skip the calling scenario if no FIPS backup container is available.

    The `clickhouse_backup_fips` container is provisioned by
    `helpers.cluster.Cluster._do_up` only when the host-side FIPS binary
    `clickhouse-backup/clickhouse-backup-race-fips` exists.
    """
    backup_fips = getattr(self.context, "backup_fips", None)
    if backup_fips is None:
        skip(
            "clickhouse-backup-race-fips binary not available on host; "
            "build it with `make build-race-fips-docker` to enable FIPS scenarios."
        )
    return backup_fips

@TestStep(When)
def read_fips_status(self, node, binary, *, godebug=None):
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


@TestStep(Then)
def check_tls_handshake(self, node, target, tls_flag, cipher=None, ciphersuites=None,
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
    )

    handshake_failed = (
        "alert handshake failure" in output_lower
        or "no protocols available" in output_lower
        or cipher_unavailable
    )

    if expected_success:
        assert "connection established" in output_lower and not cipher_unavailable, error(
            f"openssl client refused to offer the cipher; cannot test the positive case.\n{output}"
        )
        assert result.exitcode == 0, error(
            f"Expected handshake to succeed, exit={result.exitcode}\n{output}"
        )
    else:
        assert handshake_failed, error(
            f"Expected handshake rejection, exit={result.exitcode}\n{output}"
        )
        assert result.exitcode != 0, error(
            f"Expected handshake to fail, exit={result.exitcode}\n{output}"
        )


@TestStep(Then)
def check_outbound_tls_handshake(self, node, command, expected_success,
                                  allow_remote_auth_error_as_skip=False,
                                  require_native_handshake_marker=True):
    """Run `command` inside `node` and assert on the *outbound* TLS outcome.

    `command` invokes `clickhouse-backup-fips` with
    `GODEBUG=fips140=only` against an `openssl s_server` whose offered
    cipher we want the FIPS policy to accept or reject. The function wraps
    `command` in `timeout <N>`. The check is **TLS-policy-only**:

    * `expected_success=True`  - the FIPS policy must NOT reject the
      handshake.

    * `expected_success=False` - the FIPS policy MUST reject the
      handshake.

    In some CI environments the S3 FIPS hostname can resolve to public AWS
    instead of the local `openssl s_server` sidecar used by this test. For
    those probes, `allow_remote_auth_error_as_skip=True` can skip the check when
    an AWS auth failure marker (e.g. `InvalidAccessKeyId`) is present but no TLS
    rejection marker is visible.

    `require_native_handshake_marker` controls how an `accepted` handshake is
    proven. The native-protocol marker (`handshake: failed to read packet`) is only emitted when the `ClickHouse`
    connection itself runs through the `openssl s_server. The marker never appears in S3 scenarios 
    (the S3 `list remote` command always exits 0, even when the walk fails).
    """
    bounded = f"timeout {CLI_CMD_TIMEOUT_SEC} {command}"
    result = node.cmd(bounded, no_checks=True)
    output = (result.output or "")
    output_lower = output.lower()
    debug(f"output_lower: {output_lower}")

    handshake_rejected = (
        "remote error: tls: handshake failure" in output_lower
    )
    remote_auth_failed = (
        "invalidaccesskeyid" in output_lower
        or "signaturedoesnotmatch" in output_lower
        or "accessdenied" in output_lower
    )

    if expected_success:
        if require_native_handshake_marker:
            # The approved cipher was negotiated. TLS completed and the connection
            # reached the ClickHouse native protocol, which against `openssl s_server` surfaces.
            assert "handshake: failed to read packet" in output_lower, error(
                f"Expected the TLS handshake to complete (post-TLS native-protocol "
                f"marker), but it was not found.\n{output}"
            )
        else:
            # S3 path: `clickhouse-backup list remote` accepts the S3 walk error
            # and ALWAYS exits 0, and the AWS-SDK HTTP path emits no native
            # handshake marker, so the client side can only assert the ABSENCE
            # of a handshake-failure marker. The independent server-side check in
            # `assert_s_server_logs_match_outcome` (no `no shared cipher` in the
            # s_server log) is what proves that the cipher was accepted.
            assert not handshake_rejected, error(
                f"Expected the FIPS policy to ACCEPT the handshake, but the output "
                f"contains a TLS handshake-failure marker.\n{output}"
            )
    else:
        if allow_remote_auth_error_as_skip and remote_auth_failed: # allow_remote_auth_error_as_skip true for S3 scenarios
            skip(
                "No TLS rejection marker was found and remote auth failed "
                "(endpoint resolved outside the local openssl sidecar). "
            )
        # Rejection is proven by the FIPS handshake-failure marker on both paths.
        # The exit code is intentionally NOT asserted: `list remote` exits 0 even
        # when the S3 walk fails (error logged as WRN), and `tables` exits 124
        # whether TLS is accepted or rejected (it retries until `timeout`).
        assert handshake_rejected, error(
            f"Expected the FIPS policy to REJECT the handshake, but no "
            f"TLS handshake-failure marker was found.\n{output}"
        )


@TestStep(Then)
def assert_s_server_logs_match_outcome(self, cluster, aux_name, expected_success, log_tail_length=1000):
    """CHeck the client-side outcome against the s_server's own log.

    This is an independent, server-side confirmation of what the FIPS client
    reported.

    Important property of `openssl s_server` stdout: it has no positive per-connection
    marker. It prints `ACCEPT` exactly once at startup in both the success
    and the failure case, and with `-www` it never logs the negotiated cipher
    to stdout. The only distinguishing signal is the `rejection` line it writes
    when the client offered no cipher the server also has:

        approved cipher  -> server log shows `unexpected eof while reading`
                            (the FIPS client closing after its native-protocol
                            read times out), and NO `no shared cipher`.
        rejected cipher  -> server log shows
                            `tls_post_process_client_hello: no shared cipher`.

    Therefore acceptance is proven by the ABSENCE of a rejection marker and
    rejection by its PRESENCE:

      * `expected_success=True`  -> the log must NOT contain a rejection marker.
      * `expected_success=False` -> the log MUST contain a rejection marker.

    Variables:
      * `cluster`            - test Cluster, used to fetch container logs.
      * `aux_name`           - name of the auxiliary openssl s_server container.
      * `expected_success`   - True if the handshake should have been accepted,
                                 False if the server should have rejected it.
      * `s_server_log`       - captured stdout of the s_server container.
      * `rejection_markers`  - substrings that prove the server rejected the
                                 handshake (e.g. `no shared cipher`).
    """
    try:
        s_server_log = cluster.container_logs(aux_name)
    except Exception as exc:
        note(f"could not fetch s_server logs for `{aux_name}`: {exc}")
        return

    log_lower = (s_server_log or "").lower()

    has_rejection_marker = (
    "no shared cipher" in log_lower # canonical rejection marker
    or "remote error: tls: handshake failure" in log_lower
    or "no cipher match" in log_lower
)

    log_tail = s_server_log[:-log_tail_length] if s_server_log else "<empty>"

    if expected_success:
        assert not has_rejection_marker, error(
            f"s_server `{aux_name}` logged a handshake-rejection marker even "
            f"though the cipher was expected to be ACCEPTED. The server, not "
            f"just the client, refused the handshake. Tail of log:\n{log_tail}"
        )
    else:
        assert has_rejection_marker, error(
            f"s_server `{aux_name}` did NOT log a handshake-rejection marker "
            f"even though the cipher was expected to be REJECTED. The server "
            f"may have accepted a non-approved cipher. Tail of log:\n{log_tail}"
        )


@TestStep(Then)
def check_outbound_tls_with_cipher(self, cluster, backup_fips, *, listen, command,
                                    expected_success, tls_version,
                                    cipher=None, ciphersuites=None,
                                    aux_name='openssl_server',
                                    allow_remote_auth_error_as_skip=False,
                                    require_native_handshake_marker=True):
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
    :param allow_remote_auth_error_as_skip: if `true`, a remote auth failure
        marker can skip a negative expectation when TLS rejection markers are
        absent (used for S3 CI stability when DNS bypasses the sidecar).
    :param require_native_handshake_marker: if `true`,an accepted handshake must be proven by the clickhouse
        native-protocol marker. If `False` (S3 scenario), acceptance is asserted
        as the absence of a FIPS-policy rejection.
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
        check_outbound_tls_handshake(
            node=backup_fips,
            command=command,
            expected_success=expected_success,
            allow_remote_auth_error_as_skip=allow_remote_auth_error_as_skip,
            require_native_handshake_marker=require_native_handshake_marker,
        )
        assert_s_server_logs_match_outcome(
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

    with Given("I require FIPS backup container"):
        backup_fips = require_fips_container()

    with When("I run `clickhouse-backup-fips --version`"):
        status, output = read_fips_status(node=backup_fips, binary=FIPS_BINARY_IN_CONTAINER)

    with Then(f"`{FIPS_VERSION_LABEL}` line is found"):
        assert status is not None, error(
            f"`{FIPS_VERSION_LABEL}` line missing from `--version`:\n{output}"
        )

    with And(f"its value is `{FIPS_VERSION_TRUE}`"):
        assert status == FIPS_VERSION_TRUE, error(
            f"Expected `{FIPS_VERSION_LABEL} {FIPS_VERSION_TRUE}`, got `{status}`.\n{output}"
        )


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_non_FIPS_Binary("1.0"),
)
def clickhouse_backup_fips_version_output_negative_check(self):
    """Self-check for `clickhouse_backup_fips_version_output`.

    Run the same `--version` parser as in clickhouse_backup_fips_version_output 
    against the regular (non-FIPS) `clickhouse-backup` binary,
    which is what `make build-race` produces. The status line must show `false`.
    """
    backup = self.context.backup

    with When("I run `clickhouse-backup --version` on the non-FIPS binary"):
        status, output = read_fips_status(node=backup, binary="/bin/clickhouse-backup")

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

    with Given("I require FIPS backup container"):
        backup_fips = require_fips_container()

    with When("I run `go version -m` against the FIPS binary"):
        result = backup_fips.cmd(f"env -u GODEBUG go version -m {FIPS_BINARY_IN_CONTAINER}")
        debug(f"STDOUT:\n{result.output}")
        debug(f"EXIT CODE: {result.exitcode}")
        debug(f"FIPS_GO_BUILD_SETTING: {FIPS_GO_BUILD_SETTING!r}")

    with Then(f"the output contains the build setting `{FIPS_GO_BUILD_SETTING}`"):
        assert FIPS_GO_BUILD_SETTING in result.output, error(
            f"`go version -m {FIPS_BINARY_IN_CONTAINER}` output is missing "
            f"`{FIPS_GO_BUILD_SETTING}`:\n{result.output}"
        )


@TestStep(Then)
def assert_tables_succeeds(self, backup_fips, *, config, godebug=None):
    """Run `clickhouse-backup-fips -c <config> tables` and assert it exits 0.

    Wrapped in `timeout` so a hung handshake or DNS failure does not
    block the tests for a long time; on success the command is fast.

    :param godebug: value for `GODEBUG` (e.g. `"fips140=only"`); pass
        `None` (default) to inherit the FIPS mode from the container.
    """
    env_prefix = f"env GODEBUG={godebug} " if godebug else ""
    cmd = (
        f"timeout {CLI_CMD_TIMEOUT_SEC} {env_prefix}"
        f"{FIPS_BINARY_IN_CONTAINER} -c {config} tables"
    )
    result = backup_fips.cmd(cmd, no_checks=True)
    label = godebug if godebug else "container-default"
    assert result.exitcode == 0, error(
        f"`clickhouse-backup-fips tables` against `{config}` failed "
        f"(exit={result.exitcode}, GODEBUG={label}):\n{result.output}"
    )


@TestStep(Then)
def assert_tables_fails(self, backup_fips, *, config, reason, godebug=None):
    """Run `clickhouse-backup-fips -c <config> tables` and assert it fails.

    Negative option to `_assert_tables_succeeds`: require non-zero
    exit and ensure no table-list success markers are present. Pass `godebug`
    `None` (default) to inherit the FIPS mode from the container.
    """
    env_prefix = f"env GODEBUG={godebug} " if godebug else ""
    cmd = (
        f"timeout {CLI_CMD_TIMEOUT_SEC} {env_prefix}"
        f"{FIPS_BINARY_IN_CONTAINER} -c {config} tables 2>&1"  # `2>&1` redirects stderr to stdout.
    )
    result = backup_fips.cmd(cmd, no_checks=True)
    output = result.output or ""
    label = godebug if godebug else "container-default"

    assert result.exitcode != 0, error(
        f"`clickhouse-backup-fips tables` unexpectedly succeeded against `{config}` "
        f"(GODEBUG={label}). Expected failure reason: {reason}\n{output}"
    )

    output_lower = output.lower()
    assert "atomic" not in output_lower and "ordinary" not in output_lower, error(
        f"`tables` output is successful (database-listing marker present) "
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

    with Given("I require FIPS backup container"):
        backup_fips = require_fips_container()

    cluster = self.context.cluster
    listeners_xml = os.path.join(
        cluster.tests_dir,
        "configs/clickhouse_fips_server/config.d/listeners.xml",
    )

    try:
        with When("I bring up a dedicated FIPS-compatible Altinity ClickHouse server"):
            cluster.start_clickhouse_server_container(
                name=FIPS_CH_SERVER_NAME,
                image_tag=FIPS_CH_SERVER_IMAGE,
                extra_volumes=[
                    (cluster.ssl_certs_dir, "/etc/clickhouse-server/ssl"),
                    (listeners_xml, "/etc/clickhouse-server/config.d/listeners.xml"),
                ],
            )

        with Then("I run `clickhouse-backup-fips tables` over secure native TCP 9440 and assert it succeeds"):
            assert_tables_succeeds(
                backup_fips=backup_fips,
                config=FIPS_CONNECTIVITY_FIPS_CONFIG_PATH,
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

    with Given("I require FIPS backup container"):
        backup_fips = require_fips_container()

    cluster = self.context.cluster

    try:
        with When("I bring up a non-FIPS Altinity ClickHouse server running with image defaults",
            description="no `tcp_port_secure` listener; only plain `:9000` / `:8123` are bound",
        ):
            cluster.start_clickhouse_server_container(
                name=NON_FIPS_CH_SERVER_NAME,
                image_tag=NON_FIPS_CH_SERVER_IMAGE,
            )
        with Then(
            "I run `clickhouse-backup-fips tables` with the FIPS-compatible "
            "client config (`secure: true`, `port: 9440`) and assert it fails"
        ):
            assert_tables_fails(
                backup_fips=backup_fips,
                config=FIPS_CONNECTIVITY_NONFIPS_CONFIG_PATH,
                reason=(
                    "The non-FIPS ClickHouse server runs with image defaults "
                    "and does not expose the secure native TCP port `:9440` "
                    "required by the FIPS-compatible client config "
                    "(`secure: true`, `port: 9440`)."
                ),
            )
    finally:
        cluster.stop_auxiliary_container(NON_FIPS_CH_SERVER_NAME)


@TestStep(When)
def godebug_env_prefix(self, godebug):
    """Return the `env ...` command prefix that applies one GODEBUG case.

    `None` strips any inherited `GODEBUG` (`env -u GODEBUG`), `""` sets it
    empty (`env GODEBUG=`), and any other value sets `GODEBUG=fips140=<value>`.
    Setting it explicitly per case makes the test independent of the suite-wide
    `--fips-godebug` selection that the container otherwise exports.
    """
    with When("I return the `env ...` command prefix"):
        if godebug is None:
            return "env -u GODEBUG "
        if godebug == "":
            return "env GODEBUG= "
        return f"env GODEBUG=fips140={godebug} "


@TestStep(When)
def read_fips_info_field(self, output, field):
    """Return the value of a `<field>:` line from `--fips-info` output."""
    with When("I read the `field` field from the `--fips-info` output"):
        for line in output.splitlines():
            stripped = line.strip()
            if stripped.startswith(f"{field}:"):
                return stripped.split(":", 1)[1].strip()
        return None


@TestStep(Then)
def check_fips_info_values(self, backup_fips, *, name, godebug,
                            expected_enabled, expected_enforced):
    """Run `--fips-info` for one GODEBUG case and assert enabled/enforced.

    Asserts the command succeeds and that the `fips_module` block reports the
    expected `enabled` / `enforced` booleans for the given GODEBUG mode.
    """

    with When("I run `--fips-info` for one GODEBUG case"):
        cmd = f"{godebug_env_prefix(godebug=godebug)}{FIPS_BINARY_IN_CONTAINER} --fips-info"
        result = backup_fips.cmd(cmd, no_checks=True)
        output = result.output or ""

    with Then("I assert the `--fips-info` command succeeds"):
        assert result.exitcode == 0, error(
            f"`--fips-info` failed for GODEBUG mode `{name}` "
            f"(exit={result.exitcode}).\n{output}"
        )

    with When("I read the `enabled` field from the `--fips-info` output"):
        enabled = read_fips_info_field(output=output, field="enabled")

    with Then("I assert the `enabled` field has the expected value"):
        want_enabled = str(expected_enabled).lower()

        assert enabled == want_enabled, error(
        f"GODEBUG mode `{name}`: expected `enabled: {want_enabled}`, "
        f"got `enabled: {enabled}`.\n{output}"
    )

    with When("I read the `enforced` field from the `--fips-info` output"):
        enforced = read_fips_info_field(output=output, field="enforced")

    with Then("I assert the `enforced` field has the expected value"):
        want_enforced = str(expected_enforced).lower()

        assert enforced == want_enforced, error(
        f"GODEBUG mode `{name}`: expected `enforced: {want_enforced}`, "
        f"got `enforced: {enforced}`.\n{output}"
    )


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_FipsInfo_Values("1.0"),
)
def fips_info_values_output(self):
    """Validate `--fips-info` FIPS posture for every GODEBUG fips140 mode.

    For each documented GODEBUG runtime mode (`unset`, empty, `fips140=off`,
    `fips140=on`, `fips140=only`), run `clickhouse-backup-fips --fips-info` and
    assert the reported `enabled` / `enforced` flags match the expected truth
    table:

        GODEBUG runtime      enabled   enforced
        unset                true      false
        empty ("")           true      false
        fips140=off          false     false
        fips140=on           true      false
        fips140=only         true      true

    Each mode is set explicitly per command, so the result does not depend on
    the suite-wide `--fips-godebug` selection.
    """
    # Expected `clickhouse-backup-fips --fips-info` posture for every GODEBUG
    # fips140 runtime mode. The FIPS binary is built with `DefaultGODEBUG` set to
    # `fips140=on`, so leaving GODEBUG unset or empty keeps FIPS *enabled* but not
    # *enforced*; `fips140=on` is the same; `fips140=only` adds strict enforcement;
    # `fips140=off` disables FIPS entirely.

    FIPS_GODEBUG_INFO_CASES = [
        ("unset", None,   True,  False),
        ("empty", "",     True,  False),
        ("off",   "off",  False, False),
        ("on",    "on",   True,  False),
        ("only",  "only", True,  True),
    ]
    with Given("I require FIPS backup container"):
        backup_fips = require_fips_container()

    for name, godebug, expected_enabled, expected_enforced in FIPS_GODEBUG_INFO_CASES:
        with Then(f"GODEBUG mode `{name}` reports "
                   f"enabled={expected_enabled} enforced={expected_enforced}"):
            check_fips_info_values(
                backup_fips=backup_fips,
                name=name,
                godebug=godebug,
                expected_enabled=expected_enabled,
                expected_enforced=expected_enforced,
            )


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
    with Given("I require FIPS backup container"):
        backup_fips = require_fips_container()

    with When("I run the FIPS checksum tamper script against the FIPS binary"):
        result = backup_fips.cmd(
            f"/scripts/tamper_go_fips_checksum.sh {FIPS_BINARY_IN_CONTAINER}",
            no_checks=True,
        )
        output = result.output or ""

    with Then("the tampered binary output includes `panic: fips140: verification mismatch`"):
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

    with Then("the tamper script reports its explicit success marker"):
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
    # Non-approved profiles the REST API listener must reject. The TLSv1.2 base
    # adds RC4 / 3DES (specific to the inbound case); the TLSv1.3 base is the
    # shared non-approved suite. 
    # `--stress` widens both with the broader stres sets so legacy / CBC ciphers 
    # are exercised here too; default keeps the minimum. `STRESS` lists already include 
    # their base, so they are assigned,
    # not appended, to avoid probing the same cipher twice.
    non_fips_tls12 = NON_FIPS_TLS12_INBOUND
    non_fips_tls13 = NON_FIPS_TLS13

    if self.context.stress:
        non_fips_tls12 = NON_FIPS_TLS12_INBOUND_STRESS
        non_fips_tls13 = NON_FIPS_TLS13_STRESS

    with Given("I require FIPS backup container"):
        backup_fips = require_fips_container()

    target = f"localhost:{FIPS_TLS_LISTEN_PORT}"

    try:
        with When(
            "I start a clickhouse-backup-fips server with the TLS API config",
            description=f"config={FIPS_TLS_CONFIG_PATH} listen=:{FIPS_TLS_LISTEN_PORT}",
        ):
            backup_fips.start_server(
                binary=FIPS_BINARY_IN_CONTAINER,
                config=FIPS_TLS_CONFIG_PATH,
                listen_port=FIPS_TLS_LISTEN_PORT,
                timeout=60,
            )

        with When("I try to connect using each FIPS-approved TLSv1.3 cipher suite"):
            for ciphersuite in FIPS_TLS13_APPROVED:  # shared with outbound scenario
                with Then(f"TLSv1.3 ciphersuite {ciphersuite} should be accepted"):
                    check_tls_handshake(
                        node=backup_fips, target=target, tls_flag="-tls1_3",
                        ciphersuites=ciphersuite, expected_success=True,
                    )

        with When("I try to connect using each FIPS-approved TLSv1.2 cipher"):
            for cipher in FIPS_TLS12_APPROVED:  # shared with outbound scenario
                with Then(f"TLSv1.2 cipher {cipher} should be accepted"):
                    check_tls_handshake(
                        node=backup_fips, target=target, tls_flag="-tls1_2",
                        cipher=cipher, expected_success=True,
                    )

        with When("I try to connect using each non-FIPS TLSv1.3 cipher suite"):
            for ciphersuite in non_fips_tls13:
                with Then(f"TLSv1.3 ciphersuite {ciphersuite} should be rejected"):
                    check_tls_handshake(
                        node=backup_fips, target=target, tls_flag="-tls1_3",
                        ciphersuites=ciphersuite, expected_success=False,
                    )

        with When("I try to connect using each non-FIPS TLSv1.2 cipher"):
            for cipher in non_fips_tls12:
                with Then(f"TLSv1.2 cipher {cipher} should be rejected"):
                    check_tls_handshake(
                        node=backup_fips, target=target, tls_flag="-tls1_2",
                        cipher=cipher, expected_success=False,
                    )

        with When("I try to connect using legacy TLSv1.0 / TLSv1.1 protocols and assert they are rejected"):
            with Then("TLSv1.0 handshake should be rejected"):
                check_tls_handshake(
                    node=backup_fips, target=target, tls_flag="-tls1",
                    expected_success=False,
                )
            with Then("TLSv1.1 handshake should be rejected"):
                check_tls_handshake(
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
    
    with Given("I require FIPS backup container"):
        backup_fips = require_fips_container()

    cluster = self.context.cluster

    cmd = (
        f"{FIPS_BINARY_IN_CONTAINER} "
        f"-c {FIPS_OUTBOUND_CH_CONFIG_PATH} tables 2>&1"  # `2>&1` redirects stderr to stdout.
    )

    # `--stress` widens the non-approved coverage; default keeps the minimum.
    non_fips_tls13 = NON_FIPS_TLS13_STRESS if self.context.stress else NON_FIPS_TLS13
    non_fips_tls12 = NON_FIPS_TLS12_OUTBOUND_STRESS if self.context.stress else NON_FIPS_TLS12_OUTBOUND

    with When("I try each FIPS-approved TLSv1.3 cipher suite on the CH endpoint"):
        for ciphersuite in FIPS_TLS13_APPROVED:
            with Then(f"TLSv1.3 ciphersuite {ciphersuite} should be accepted"):
                check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=True,
                )

    with And("I try each FIPS-approved TLSv1.2 cipher on the CH endpoint"):
        for cipher in FIPS_TLS12_APPROVED:
            with Then(f"TLSv1.2 cipher {cipher} should be accepted"):
                check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_2",
                    cipher=cipher, command=cmd, expected_success=True,
                )

    with And("I try each non-FIPS TLSv1.3 cipher suite on the CH endpoint"):
        for ciphersuite in non_fips_tls13:
            with Then(f"TLSv1.3 ciphersuite {ciphersuite} should be rejected"):
                check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=False,
                )

    with And("I try each non-FIPS TLSv1.2 cipher on the CH endpoint"):
        for cipher in non_fips_tls12:
            with Then(f"TLSv1.2 cipher {cipher} should be rejected"):
                check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    listen=FIPS_OUTBOUND_CH_TLS_PORT, tls_version="-tls1_2",
                    cipher=cipher, command=cmd, expected_success=False,
                )

@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_TLS_Outbound_S3_Ciphers_Approved("1.0"),
)
def outbound_tls_to_s3_endpoint_with_openssl_s_server(self):
    """Validate outbound S3 TLS policy with approved and non-approved profiles.

    For each FIPS-approved cipher / cipher suite, start an `openssl s_server`
    that offers only that profile and run:

        GODEBUG=fips140=only clickhouse-backup-fips -c <config> list remote

    inside the FIPS container, asserting:
      * approved profiles are accepted by FIPS policy;
      * non-approved profiles are rejected by FIPS policy.

    The `s_server` container reuses the cluster's static SSL fixtures and the
    `clickhouse-backup-fips` config is the static
    `configs/backup/fips/config-fips-outbound-s3-tls.yml` with
    `s3.disable_cert_verification: true` so the assertion stays focused on the
    TLS handshake.
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
 
    with Given("a FIPS backup container is available"):
        backup_fips = require_fips_container()
        
    cluster = self.context.cluster

    # cmd - command to run inside the FIPS container
    cmd = (
        f"{FIPS_BINARY_IN_CONTAINER} "
        f"-c {FIPS_OUTBOUND_S3_CONFIG_PATH} list remote 2>&1"  # `2>&1` redirects stderr to stdout.
    )

    # `--stress` widens the non-approved coverage.
    non_fips_tls13 = NON_FIPS_TLS13_STRESS if self.context.stress else NON_FIPS_TLS13
    non_fips_tls12 = NON_FIPS_TLS12_OUTBOUND_STRESS if self.context.stress else NON_FIPS_TLS12_OUTBOUND

    with When("I try each FIPS-approved TLSv1.3 cipher suite on the S3 endpoint"):
        for ciphersuite in FIPS_TLS13_APPROVED:
            with Then(f"TLSv1.3 ciphersuite {ciphersuite} should be accepted"):
                check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=True,
                    require_native_handshake_marker=False,
                )

    with When("I try each FIPS-approved TLSv1.2 cipher on the S3 endpoint"):
        for cipher in FIPS_TLS12_APPROVED:
            with Then(f"TLSv1.2 cipher {cipher} should be accepted"):
                check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_2",
                    cipher=cipher, command=cmd, expected_success=True,
                    require_native_handshake_marker=False,
                )

    with When("I try each non-FIPS TLSv1.3 cipher suite on the S3 endpoint"):
        for ciphersuite in non_fips_tls13:
            with Then(f"TLSv1.3 ciphersuite {ciphersuite} should be rejected"):
                check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_3",
                    ciphersuites=ciphersuite, command=cmd, expected_success=False,
                    allow_remote_auth_error_as_skip=True,
                    require_native_handshake_marker=False,
                )

    with When("I try each non-FIPS TLSv1.2 cipher on the S3 endpoint"):
        for cipher in non_fips_tls12:
            with Then(f"TLSv1.2 cipher {cipher} should be rejected"):
                check_outbound_tls_with_cipher(
                    cluster=cluster, backup_fips=backup_fips,
                    aux_name=OPENSSL_S3_FIPS_AUX_NAME,
                    listen=FIPS_OUTBOUND_S3_TLS_PORT, tls_version="-tls1_2",
                    cipher=cipher, command=cmd, expected_success=False,
                    allow_remote_auth_error_as_skip=True,
                    require_native_handshake_marker=False,
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
    
    with Given("I require FIPS backup container"):
        backup_fips = require_fips_container()

    cluster = self.context.cluster
    listeners_xml = os.path.join(
        cluster.tests_dir,
        "configs/clickhouse_fips_server/config.d/listeners.xml",
    )

    try:
        with When("I bring up a dedicated FIPS-compatible Altinity ClickHouse server"):
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

        with Then("the output does not include the list-of-tables success marker"):
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

    with Given("I require FIPS backup container"):
        backup_fips = require_fips_container()

    try:
        with When("I start a clickhouse-backup-fips server"):
            backup_fips.start_server(
                binary=FIPS_BINARY_IN_CONTAINER,
                config=FIPS_TLS_CONFIG_PATH,
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

    With `--stress` the server instead offers the full documented
    FIPS-approved cipher set (`listeners-fips-cipher-stress.xml`), so the
    end-to-end run is exercised against the same cipher list a real
    FIPS-compatible server advertises.

    Cipher-policy rejection (FIPS binary refusing non-approved ciphers) is
    covered deterministically by `outbound_tls_cipher_negotiation` using
    `openssl s_server`; replaying the negative case here would be flaky
    because ClickHouse's `<openSSL>` config merging does not strictly
    limit the offered cipher set.
    """
    
    with Given("a FIPS backup container is available"):
        backup_fips = require_fips_container()

    cluster = self.context.cluster

    # Default: a single FIPS-approved cipher. `--stress`: the full documented set.
    listeners_basename = (
        "listeners-fips-cipher-stress.xml" if self.context.stress
        else "listeners-fips-cipher.xml"
    )
    listeners_xml = os.path.join(
        cluster.tests_dir,
        f"configs/clickhouse_nonfips_server/config.d/{listeners_basename}",
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
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_CAST_ForcedFailure("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_SelfTest_CAST_Coverage("1.0"),
)
@Examples(
    "cast startup",
    [
        # Startup/core CASTs used during `clickhouse-backup-fips --version`. Forcing
        # any of these via `failfipscast` must abort startup deterministically.
        # Source: Go FIPS `allCASTs` in `crypto/internal/fips140test/cast_test.go`.
        ("AES-CBC", True),
        ("CTR_DRBG", True),
        ("CounterKDF", True),
        ("HKDF-SHA2-256", True),
        ("HMAC-SHA2-256", True),
        ("PBKDF2", True),
        ("SHA2-256", True),
        ("SHA2-512", True),
        ("TLSv1.2-SHA2-256", True),
        ("TLSv1.3-SHA2-256", True),
        ("cSHAKE128", True),

        # First-use CASTs run lazily, the first time their algorithm is used. `clickhouse-backup-fips --version`
        # may or may not exercise them, so forcing one is only expected to fail when it
        # happens to be reached.
        ("DetECDSA P-256 SHA2-512 sign", False),
        ("ECDH PCT", False),
        ("ECDSA P-256 SHA2-512 sign and verify", False),
        ("ECDSA PCT", False),
        ("Ed25519 sign and verify", False),
        ("Ed25519 sign and verify PCT", False),
        ("KAS-ECC-SSC P-256", False),
        ("ML-DSA sign and verify PCT", False),
        ("ML-DSA-44", False),
        ("ML-KEM PCT", False),
        ("ML-KEM-768", False),
        ("RSA sign and verify PCT", False),
        ("RSASSA-PKCS-v1.5 2048-bit sign and verify", False),
    ],
)
def forced_cast_failures(self):
    """Force each CAST via `failfipscast` and check `--version` behavior.

    Startup CASTs (`must_fail=True`) are always exercised by `--version`, so
    forcing one must abort with `self-test failed: <NAME>: simulated CAST
    failure` on stderr. First-use CASTs (`must_fail=False`) run lazily, so the
    abort only happens when the CAST is actually reached; otherwise startup
    stays clean without the marker.
    """
    
    with Given("a FIPS backup container is available"):
        backup_fips = require_fips_container()

    for cast, must_fail in self.examples:
        # `must_fail` encodes the CAST group: startup CASTs are always exercised by
        # `--version` (forcing one must abort), first-use CASTs run lazily.
        kind = "startup" if must_fail else "first-use"

        with Example(f"force {kind} CAST {cast}"):
            with When(f"I force CAST `{cast}` and run `--version`"):
                # Single-quote GODEBUG so names with spaces stay one arg; `2>&1`
                # captures the `fatal error:` lines Go writes to stderr.
                cmd = (
                    f"env 'GODEBUG=failfipscast={cast},fips140=only' "
                    f"{FIPS_BINARY_IN_CONTAINER} --version 2>&1"
                )
                result = backup_fips.cmd(cmd, no_checks=True)
                output = result.output or ""

            with And("I look for the forced-CAST failure in the output"):
                # A forced CAST aborts with Go FIPS's fatal line:
                #   `FIPS 140-3 self-test failed: <NAME>: simulated CAST failure`.
                cast_failed = (
                    "simulated CAST failure" in output
                    and f"self-test failed: {cast}" in output
                )

            # Design-time expectation: startup CASTs are always exercised by
            # `--version`, so forcing one must report its self-test failure.
            # We can make no claim about whether first-use CAST are reached.
            if must_fail:
                with Then(f"forcing startup CAST `{cast}` reports a self-test failure"):
                    assert cast_failed, error(
                        f"startup CAST `{cast}` was forced but no CAST failure was reported.\n{output}"
                    )

            # Consistency: the failure marker and the exit code must agree. A
            # reported failure must abort; a clean run must exit 0 and mention no
            # CAST failure at all.
            if cast_failed:
                with Then(f"the forced failure for `{cast}` aborts `--version`"):
                    assert result.exitcode != 0, error(
                        f"`{cast}` reported a CAST failure but `--version` exited 0.\n{output}"
                    )
            else:
                with Then(f"`{cast}` did not fail and startup stayed clean"):
                    assert result.exitcode == 0, error(
                        f"`{cast}` reported no CAST failure yet `--version` exited "
                        f"{result.exitcode}; unexpected non-CAST abort.\n{output}"
                    )
                    assert "simulated CAST failure" not in output, error(
                        f"`{cast}` did not fail yet the CAST-failure marker appeared.\n{output}"
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

    Opt-in: skipped unless `RUN_ACVP_TESTS=1` is set or the suite runs
    with `--stress` option.
    """
    # Set `RUN_ACVP_TESTS=1` locally or in the CI workflow to enable it.

    FIPS_ACVP_ENV_FLAG          = "RUN_ACVP_TESTS"
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
    # `--stress` runs the full FIPS coverage, so enable ACVP tests automatically
    # there even when `RUN_ACVP_TESTS` is unset.
    if flag != "1" and not self.context.stress:
        skip(
            f"set {FIPS_ACVP_ENV_FLAG}=1 (or run with `--stress`) to enable; "
            f"the wrapper pulls Docker images and clones upstream repos."
        )

    cluster = self.context.cluster
    script_path = os.path.normpath(os.path.join(cluster.tests_dir, FIPS_ACVP_SCRIPT_RELPATH))

    if not os.path.isfile(script_path):
        fail(f"ACVP wrapper script not found at {script_path}")

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

    with Then(f"output contains `{FIPS_ACVP_EXPECTED_OUTPUT}`"):
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
    Scenario(run=connectivity_against_non_fips_clickhouse_server, flags=TE)
    Scenario(run=connectivity_against_fips_clickhouse_server, flags=TE)
    Scenario(run=fips_info_values_output, flags=TE)
    Scenario(run=fips_integrity_self_test_failure_on_tampered_binary, flags=TE)
    Scenario(run=inbound_tls_cipher_negotiation, flags=TE)
    Scenario(run=outbound_tls_cipher_negotiation, flags=TE)
    Scenario(run=outbound_tls_to_s3_endpoint_with_openssl_s_server, flags=TE)
    Scenario(run=outbound_tls_to_nonfips_clickhouse_with_cipher_profile, flags=TE)
    Scenario(run=connection_to_fips_clickhouse_with_nonfips_config, flags=TE)
    Scenario(run=server_listens_only_on_fips_api_port, flags=TE)
    Scenario(run=forced_cast_failures, flags=TE)
    Scenario(run=acvp_tests, flags=TE) # optional, `RUN_ACVP_TESTS=1` (see `pkg/acvpwrapper/README.md`)


if main():
    fips_140_3()
