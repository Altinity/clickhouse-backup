//go:build integration

package main

import (
	"testing"
	"time"
)

func TestS3Glacier(t *testing.T) {
	if isTestShouldSkip("GLACIER_TESTS") {
		t.Skip("Skipping GLACIER integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	r.NoError(env.DockerCP("configs/config-s3-glacier.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml.s3glacier-template"))
	env.InstallDebIfNotExists(r, "clickhouse-backup", "curl", "gettext-base", "bsdmainutils", "dnsutils", "git", "ca-certificates")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config.yml.s3glacier-template | envsubst > /etc/clickhouse-backup/config-s3-glacier.yml")
	dockerExecTimeout = 60 * time.Minute
	env.runMainIntegrationScenario(t, "GLACIER", "config-s3-glacier.yml")
	dockerExecTimeout = 3 * time.Minute
	env.Cleanup(t, r)
}
