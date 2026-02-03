//go:build integration

package main

import "testing"

func TestSFTPAuthPassword(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.runMainIntegrationScenario(t, "SFTP", "config-sftp-auth-password.yaml")
	env.Cleanup(t, r)
}
