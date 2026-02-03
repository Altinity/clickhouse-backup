//go:build integration

package main

import "testing"

func TestGCS(t *testing.T) {
	if isTestShouldSkip("GCS_TESTS") {
		t.Skip("Skipping GCS integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	env.runMainIntegrationScenario(t, "GCS", "config-gcs.yml")
	env.Cleanup(t, r)
}
