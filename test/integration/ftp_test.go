//go:build integration

package main

import (
	"os"
	"testing"
)

func TestFTP(t *testing.T) {
	env, r := NewTestEnvironment(t)
	// 21.8 can't execute SYSTEM RESTORE REPLICA
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") > 1 {
		env.runMainIntegrationScenario(t, "FTP", "config-ftp.yaml")
	} else {
		env.runMainIntegrationScenario(t, "FTP", "config-ftp-old.yaml")
	}
	env.Cleanup(t, r)
}
