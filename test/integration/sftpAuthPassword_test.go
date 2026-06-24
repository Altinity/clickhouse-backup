//go:build integration

package main

import "testing"

func TestSFTPAuthPassword(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.runMainIntegrationScenario(t, "SFTP", "config-sftp-auth-password.yaml")
}
