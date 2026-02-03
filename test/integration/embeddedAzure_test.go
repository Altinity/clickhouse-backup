//go:build integration

package main

import (
	"os"
	"testing"
)

func TestEmbeddedAzure(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "23.3") < 0 {
		t.Skipf("Test skipped, BACKUP/RESTORE not production ready for %s version, look https://github.com/ClickHouse/ClickHouse/issues/39416 for details", version)
	}
	t.Logf("@TODO RESTORE Ordinary with old syntax still not works for %s version, look https://github.com/ClickHouse/ClickHouse/issues/43971", os.Getenv("CLICKHOUSE_VERSION"))
	env, r := NewTestEnvironment(t)

	// === AZURE ===
	// CUSTOM backup create folder in each disk
	env.DockerExecNoError(r, "clickhouse", "rm", "-rf", "/var/lib/clickhouse/disks/backups_azure/backup/")
	env.runMainIntegrationScenario(t, "EMBEDDED_AZURE", "config-azblob-embedded.yml")
	env.DockerExecNoError(r, "clickhouse", "rm", "-rf", "/var/lib/clickhouse/disk_s3")
	if compareVersion(version, "24.8") >= 0 {
		env.runMainIntegrationScenario(t, "EMBEDDED_AZURE_URL", "config-azblob-embedded-url.yml")
	}

	env.Cleanup(t, r)
}
