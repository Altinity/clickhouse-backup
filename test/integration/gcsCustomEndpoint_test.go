//go:build integration

package main

import "testing"

func TestGCSWithCustomEndpoint(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.runMainIntegrationScenario(t, "GCS_EMULATOR", "config-gcs-custom-endpoint.yml")
}
