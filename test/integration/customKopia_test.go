//go:build integration

package main

import "testing"

func TestCustomKopia(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	// on failure, surface the kopia repository diagnostics (maintenance/epoch state,
	// content stats, snapshot verify) that kopia_diag appended to /tmp/kopia_diag.log
	defer func() {
		if t.Failed() {
			diagOut, _ := env.DockerExecOut("clickhouse-backup", "cat", "/tmp/kopia_diag.log")
			t.Logf("===== TestCustomKopia /tmp/kopia_diag.log =====\n%s", diagOut)
		}
	}()
	env.DockerExecNoError(r, "clickhouse-backup", "rm", "-f", "/tmp/kopia_diag.log")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "curl")
	env.DockerExecNoError(r, "clickhouse-backup", "update-ca-certificates")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xce", "command -v yq || curl -sL --retry 5 --retry-delay 5 --retry-connrefused \"https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture)\" -o /usr/bin/yq && chmod +x /usr/bin/yq")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "jq", "bzip2", "pgp", "git")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "curl -sfL --retry 5 --retry-delay 5 --retry-connrefused https://kopia.io/signing-key | gpg --dearmor -o /usr/share/keyrings/kopia-keyring.gpg")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "echo 'deb [signed-by=/usr/share/keyrings/kopia-keyring.gpg] https://packages.kopia.io/apt/ stable main' > /etc/apt/sources.list.d/kopia.list")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "kopia", "xxd", "bsdmainutils", "parallel")

	env.runIntegrationCustom(t, r, "kopia")
}
