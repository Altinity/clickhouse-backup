//go:build integration

package main

import "testing"

func TestCustomRsync(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.uploadSSHKeys(r, "clickhouse-backup")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "curl")
	env.DockerExecNoError(r, "clickhouse-backup", "update-ca-certificates")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xce", "command -v yq || curl -sL \"https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture)\" -o /usr/bin/yq && chmod +x /usr/bin/yq")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "jq", "openssh-client", "rsync")
	env.runIntegrationCustom(t, r, "rsync")
	env.Cleanup(t, r)
}
