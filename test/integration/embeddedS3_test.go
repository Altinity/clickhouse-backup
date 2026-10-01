//go:build integration

package main

import (
	"os"
	"strings"
	"testing"
)

func TestEmbeddedS3(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "23.3") < 0 {
		t.Skipf("Test skipped, BACKUP/RESTORE not production ready for %s version, look https://github.com/ClickHouse/ClickHouse/issues/39416 for details", version)
	}
	t.Logf("@TODO RESTORE Ordinary with old syntax still not works for %s version, look https://github.com/ClickHouse/ClickHouse/issues/43971", os.Getenv("CLICKHOUSE_VERSION"))
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)

	// === S3 ===
	// CUSTOM backup creates folder in each disk, need to clear
	env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/var/lib/clickhouse/disks/backups_s3/backup/")
	env.runMainIntegrationScenario(t, "EMBEDDED_S3", "config-s3-embedded.yml")
	// The scenario deletes the local backups before the remote backups. Verify
	// that remote deletion also removes native data objects, including JSON files.
	out, err := env.DockerExecOut("minio", "bash", "-ce",
		"mc --insecure alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null && "+
			"mc --insecure ls --recursive local/clickhouse/backups_s3/")
	r.NoError(err, "list native backup objects: %s", out)
	r.Empty(strings.TrimSpace(out), "native backup objects remain after deleting local and remote backups")
	// cleanup
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/disk_s3")
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/backups_s3")

	if compareVersion(version, "23.8") >= 0 {
		//CUSTOM backup creates folder in each disk, need to clear
		env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/var/lib/clickhouse/disks/backups_local/backup/")
		env.runMainIntegrationScenario(t, "EMBEDDED_LOCAL", "config-s3-embedded-local.yml")
	}
	if compareVersion(version, "24.3") >= 0 {
		env.runMainIntegrationScenario(t, "EMBEDDED_S3_URL", "config-s3-embedded-url.yml")
	}
	//@TODO think about how to implements embedded backup for s3_plain disks
	//env.DockerExecNoError(r, "clickhouse", "rm", "-rf", "/var/lib/clickhouse/disks/backups_s3_plain/backup/")
	//runMainIntegrationScenario(t, "EMBEDDED_S3_PLAIN", "config-s3-plain-embedded.yml")
}
