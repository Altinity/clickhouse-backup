//go:build integration

package main

import "testing"

func TestCustomKopia(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "curl")
	env.DockerExecNoError(r, "clickhouse-backup", "update-ca-certificates")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xce", "command -v yq || curl -sL \"https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture)\" -o /usr/bin/yq && chmod +x /usr/bin/yq")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "jq", "bzip2", "pgp", "git")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "curl -sfL https://kopia.io/signing-key | gpg --dearmor -o /usr/share/keyrings/kopia-keyring.gpg")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "echo 'deb [signed-by=/usr/share/keyrings/kopia-keyring.gpg] https://packages.kopia.io/apt/ stable main' > /etc/apt/sources.list.d/kopia.list")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "kopia", "xxd", "bsdmainutils", "parallel")

	env.runIntegrationCustom(t, r, "kopia")
	env.Cleanup(t, r)
}
