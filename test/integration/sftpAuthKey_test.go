//go:build integration

package main

import "testing"

func TestSFTPAuthKey(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.uploadSSHKeys(r, "clickhouse-backup")
	env.runMainIntegrationScenario(t, "SFTP", "config-sftp-auth-key.yaml")
	env.Cleanup(t, r)
}
