//go:build integration

package main

import (
	"os"
	"testing"
)

func TestAlibabaOverS3(t *testing.T) {
	if os.Getenv("QA_ALIBABA_SECRET_KEY") == "" {
		t.Skip("Skipping Alibabacloud integration tests... QA_ALIBABA_SECRET_KEY missing")
		return
	}
	env, r := NewTestEnvironment(t)
	env.InstallDebIfNotExists(r, "clickhouse-backup", "gettext-base")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config-s3-alibabacloud.yml.template | envsubst > /etc/clickhouse-backup/config-s3-alibabacloud.yml")
	env.runMainIntegrationScenario(t, "S3", "config-s3-alibabacloud.yml")
	env.Cleanup(t, r)
}
