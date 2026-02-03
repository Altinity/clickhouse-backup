//go:build integration

package main

import "testing"

func TestS3(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.checkObjectStorageIsEmpty(t, r, "S3")
	env.runMainIntegrationScenario(t, "S3", "config-s3.yml")
	env.Cleanup(t, r)
}
