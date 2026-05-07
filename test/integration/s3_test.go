//go:build integration

package main

import (
	"strings"
	"testing"
)

func TestS3(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	if shouldProbeKeeperTLS() {
		verifyOut, verifyErr := env.DockerExecOut("clickhouse-backup", "openssl", "verify", "-CAfile", "/etc/clickhouse-server/keeper.crt", "-verify_hostname", "zookeeper", "/etc/clickhouse-server/keeper.crt")
		r.NoError(verifyErr, verifyOut)
		verifyIPOut, verifyIPErr := env.DockerExecOut("clickhouse-backup", "openssl", "verify", "-CAfile", "/etc/clickhouse-server/keeper.crt", "-verify_ip", "127.0.0.1", "/etc/clickhouse-server/keeper.crt")
		r.Error(verifyIPErr, verifyIPOut)
		r.Contains(strings.ToLower(verifyIPOut), "ip address mismatch")
		probeOut, probeErr := env.DockerExecOut(
			"clickhouse-backup",
			"bash", "-ce",
			"LOG_LEVEL=debug ALLOW_EMPTY_BACKUPS=1 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create --rbac-only test_s3_keeper_tls_probe; status=$?; clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local test_s3_keeper_tls_probe || true; exit $status",
		)
		r.NoError(probeErr, probeOut)
		r.Contains(probeOut, "parsed TLS config")
		r.Contains(probeOut, "caPath=/etc/clickhouse-server/keeper_tls_ca_bundle.crt")
		r.Contains(probeOut, "skipChainVerify=false")
		r.Contains(probeOut, "skipHostnameVerify=true")
	}
	env.checkObjectStorageIsEmpty(t, r, "S3")
	env.runMainIntegrationScenario(t, "S3", "config-s3.yml")
}

func shouldProbeKeeperTLS() bool {
	return !isTestShouldSkip("KEEPER_TLS_ENABLED") && compareVersion(getEnvDefault("CLICKHOUSE_VERSION", "26.3"), "21.9") >= 0
}
