//go:build integration

package main

import (
	"os"
	"testing"
)

func TestEmbeddedGCSOverS3(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "23.3") < 0 {
		t.Skipf("Test skipped, BACKUP/RESTORE not production ready for %s version, look https://github.com/ClickHouse/ClickHouse/issues/39416 for details", version)
	}
	if compareVersion(version, "23.3") >= 0 && compareVersion(version, "24.3") < 0 {
		t.Logf("@TODO RESTORE Ordinary with old syntax still not works for %s version, look https://github.com/ClickHouse/ClickHouse/issues/43971", os.Getenv("CLICKHOUSE_VERSION"))
	}

	// === GCS over S3 ===
	if compareVersion(version, "24.3") >= 0 && os.Getenv("QA_GCS_OVER_S3_BUCKET") != "" {
		env, r := NewTestEnvironment(t)
		//@todo think about named collections to avoid show credentials in logs look to https://github.com/fsouza/fake-gcs-server/issues/1330, https://github.com/fsouza/fake-gcs-server/pull/1164
		env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "gettext-base")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config-gcs-embedded-url.yml.template | envsubst > /etc/clickhouse-backup/config-gcs-embedded-url.yml")
		env.runMainIntegrationScenario(t, "EMBEDDED_GCS_URL", "config-gcs-embedded-url.yml")
		env.Cleanup(t, r)
	}

}
