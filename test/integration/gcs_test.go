//go:build integration

package main

import "testing"

func TestGCS(t *testing.T) {
	if isTestShouldSkip("GCS_TESTS") {
		t.Skip("Skipping GCS integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.runMainIntegrationScenario(t, "GCS", "config-gcs.yml")
}
