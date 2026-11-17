//go:build integration

package main

import "testing"

func TestCustomRsync(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.uploadSSHKeys(r, "clickhouse-backup")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "curl")
	env.DockerExecNoError(r, "clickhouse-backup", "update-ca-certificates")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xce", "yq --version || { curl -fsSL --retry 10 --retry-delay 5 --retry-connrefused \"https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture)\" -o /usr/bin/yq.tmp && chmod +x /usr/bin/yq.tmp && /usr/bin/yq.tmp --version && mv -f /usr/bin/yq.tmp /usr/bin/yq; }")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "jq", "openssh-client", "rsync")
	// the OpenSSH 10 linuxserver sshd (CH>=23.3) ships without rsync; rsync-over-ssh
	// needs the binary on the server side too (panubo already bundles it)
	if isModernSSHD() {
		env.DockerExecNoError(r, "sshd", "apk", "add", "--no-cache", "rsync")
	}
	env.runIntegrationCustom(t, r, "rsync")
}
