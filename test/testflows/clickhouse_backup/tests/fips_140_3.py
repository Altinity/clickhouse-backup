import sys

from testflows.core import *
from testflows.asserts import error
append_path(sys.path, "../..")
from clickhouse_backup.requirements.fips.requirements import *


FIPS_BINARY_IN_CONTAINER = "/bin/clickhouse-backup-fips"
FIPS_VERSION_LABEL = "FIPS 140-3:"
FIPS_VERSION_TRUE = "true"


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


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_FIPS_Version_Status("1.0")
)
def clickhouse_backup_fips_version_output(self):
    """Validate that the `clickhouse-backup-fips --version` output reports `FIPS 140-3: true`.

    Check that the binary is built with ``GOFIPS140=v1.0.0`` so FIPS 140-3 mode is the build-time default
    and ``crypto/fips140.Enabled()`` returns ``true``.
    """
    backup_fips = _require_fips_container(self)

    with When("I run `clickhouse-backup-fips --version`"):
        r = backup_fips.cmd(f"{FIPS_BINARY_IN_CONTAINER} --version")

    with Check("the version output contains the FIPS 140-3 status line"):
        assert FIPS_VERSION_LABEL in r.output, error(
            f"Expected `{FIPS_VERSION_LABEL}` in `--version` output, but it was missing.\n{r.output}"
        )

    with Check("the FIPS 140-3 status is `true`"):
        output = r.output
        # Find the status line in `--version` output.
        status_line = None
        for line in output.splitlines():
            normalized = line.strip()
            if normalized.lower().startswith(FIPS_VERSION_LABEL.lower()):
                status_line = normalized
                break

        assert status_line is not None, error(
            f"Could not find a `{FIPS_VERSION_LABEL}` status line in output.\n\n{output}"
        )

        raw_status = status_line.split(":", 1)[1] if ":" in status_line else ""
        status_value = raw_status.strip().lower()
        assert status_value == FIPS_VERSION_TRUE, error(
            f"Expected `{FIPS_VERSION_LABEL} {FIPS_VERSION_TRUE}`, got `{status_line}`.\n\n{output}"
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
def inbound_tls_cipher_negotiation(self):
    """Validate inbound TLS policy for REST API with `openssl s_client`."""
    xfail("not implemented yet")


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
    """FIPS 140-3 automation entrypoint for clickhouse-backup."""

    Scenario(run=clickhouse_backup_fips_version_output, flags=TE)
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
