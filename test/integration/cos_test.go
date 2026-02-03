//go:build integration

package main

import (
	"os"
	"testing"
)

func TestCOS(t *testing.T) {
	if os.Getenv("QA_TENCENT_SECRET_KEY") == "" {
		t.Skip("Skipping Tencent Cloud Object Storage integration tests... QA_TENCENT_SECRET_KEY missing")
		return
	}
	env, r := NewTestEnvironment(t)
	env.InstallDebIfNotExists(r, "clickhouse-backup", "gettext-base")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config-cos.yml.template | envsubst > /etc/clickhouse-backup/config-cos.yml")
	env.runMainIntegrationScenario(t, "COS", "config-cos.yml")
	env.Cleanup(t, r)
}
