//go:build integration

package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestGCS(t *testing.T) {
	if isTestShouldSkip("GCS_TESTS") {
		t.Skip("Skipping GCS integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.runMainIntegrationScenario(t, "GCS", "config-gcs.yml")
}

// TestGCSMultipart exercises the experimental parallel composite upload path
// (gcs.allow_multipart_upload) and the multipart download path (gcs.allow_multipart_download),
// see https://github.com/Altinity/clickhouse-backup/issues/1028.
// run.sh exports GCS_ENCRYPTION_KEY for all containers, but CSEK is not compatible with
// allow_multipart_upload, so every command unsets it explicitly.
func TestGCSMultipart(t *testing.T) {
	if isTestShouldSkip("GCS_TESTS") {
		t.Skip("Skipping GCS integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1500*time.Millisecond, 3*time.Minute)

	backupName := fmt.Sprintf("%s_%d", t.Name(), rand.Int())
	cfgPath := "/etc/clickhouse-backup/config-gcs-parallel.yml"
	dbName := "test_gcs_parallel"

	execCmd := func(cmd string) string {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "GCS_ENCRYPTION_KEY= LOG_LEVEL=debug "+cmd)
		r.NoError(err, "%s\n%s", cmd, out)
		return out
	}

	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE IF NOT EXISTS "+dbName+".big (key UInt64, value String) ENGINE=MergeTree() ORDER BY key")
	// ~30MB of incompressible data, so the tar stream exceeds multipart_upload_min_size
	// and splits into multiple chunk_size parts
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".big SELECT number, randomString(1000) FROM numbers(30000)")

	execCmd(fmt.Sprintf("clickhouse-backup -c %s create --tables=%s.* %s", cfgPath, dbName, backupName))
	out := execCmd(fmt.Sprintf("clickhouse-backup -c %s upload %s", cfgPath, backupName))
	r.Contains(out, "putFileMultipart", "upload must go through the parallel composite upload path")

	execCmd(fmt.Sprintf("clickhouse-backup -c %s delete local %s", cfgPath, backupName))
	env.queryWithNoError(t, r, "DROP DATABASE IF EXISTS "+dbName)

	out = execCmd(fmt.Sprintf("clickhouse-backup -c %s download %s", cfgPath, backupName))
	r.Contains(out, "multipart download", "download must go through the multipart download path")
	execCmd(fmt.Sprintf("clickhouse-backup -c %s restore %s", cfgPath, backupName))

	var count uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&count, "SELECT count() FROM "+dbName+".big"))
	r.Equal(uint64(30000), count)

	execCmd(fmt.Sprintf("clickhouse-backup -c %s delete local %s", cfgPath, backupName))
	execCmd(fmt.Sprintf("clickhouse-backup -c %s delete remote %s", cfgPath, backupName))
	env.queryWithNoError(t, r, "DROP DATABASE IF EXISTS "+dbName)
}
