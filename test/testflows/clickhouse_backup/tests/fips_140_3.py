from testflows.core import *
from testflows.asserts import error

@TestScenario
def clickhouse_backup_fips_version_output(self):
    """Validate `clickhouse-backup-fips --version` output."""

@TestScenario
def gofips140_build_flags_present(self):
    """Validate `go version -m` includes `build\tGOFIPS140=v1.0.0`."""


@TestScenario
def godebug_fips140_modes(self):
    """Validate `GODEBUG` runtime modes (`unset`/`fips140=on`/`fips140=only`)."""


@TestScenario
def connectivity_against_non_fips_clickhouse_server(self):
    """Validate `clickhouse-backup-fips` connectivity against non-FIPS ClickHouse server when `GODEBUG=fips140=on`."""


@TestScenario
def connectivity_against_fips_clickhouse_server(self):
    """Validate `clickhouse-backup-fips` connectivity against FIPS-compatible ClickHouse server when `GODEBUG=fips140=only`."""


@TestScenario
def fips_integrity_self_test_failure_on_tampered_binary(self):
    """Validate startup abort for a tampered `.go.fipsinfo` checksum."""

@TestScenario
def inbound_tls_cipher_negotiation(self):
    """Validate inbound TLS policy for REST API with `openssl s_client`."""

@TestScenario
def outbound_tls_cipher_negotiation(self):
    """Validate outbound TLS policy from `clickhouse-backup-fips` to ClickHouse server with `openssl s_server`."""

@TestScenario
def outbound_tls_to_s3_endpoint_with_openssl_s_server(self):
    """Validate outbound TLS policy to S3 endpoint with `openssl s_server`."""

@TestScenario
def forced_cast_failures(self):
    """Validate forced CAST failures with `GODEBUG=failfipscast=...,fips140=on`."""

@TestScenario
def acvp_tests(self):
    """Validate ACVP tests."""


@TestFeature
@Name("FIPS 140-3 Compatibility")
def fips_ssl_140_3(self):
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
    fips_ssl_140_3()
