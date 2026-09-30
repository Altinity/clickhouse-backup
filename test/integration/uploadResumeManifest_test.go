//go:build integration

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestUploadResumeManifestComplete covers https://github.com/Altinity/clickhouse-backup/issues/1569
// the second `upload --resume` skips every file recorded in upload.state2 as already processed,
// it used to upload manifest.bolt.gz with metadata.json only, so download fell back to Walk for every part.
func TestUploadResumeManifestComplete(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	const configFile = "config-s3.yml"
	backupName := "test_upload_resume_manifest"
	dbName := "test_upload_resume_manifest_db"
	tableName := "t1"
	partitionsCount := 4
	backupCmd := "clickhouse-backup -c /etc/clickhouse-backup/" + configFile

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, false, false, configFile)
	defer fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, false, false, configFile)

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.%s (id UInt64) ENGINE=MergeTree() PARTITION BY id %% %d ORDER BY id", dbName, tableName, partitionsCount))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.%s SELECT number FROM numbers(1000)", dbName, tableName))

	// manifest.bolt.gz is used by download only for the directory data format
	const uploadEnv = "S3_COMPRESSION_FORMAT=none LOG_LEVEL=debug "
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", uploadEnv+backupCmd+" create --tables="+dbName+".* "+backupName)
	// first attempt uploads all parts and fills upload.state2
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", uploadEnv+backupCmd+" upload --resume "+backupName)
	// second attempt has every part in upload.state2, so it skips all of them and overwrites manifest.bolt.gz
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", uploadEnv+backupCmd+" upload --resume "+backupName+" 2>&1")
	r.NoError(err, "%s", out)
	r.Contains(out, "already exists on remote, will try to resume upload")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", backupCmd+" delete local "+backupName)
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", uploadEnv+backupCmd+" download "+backupName+" 2>&1")
	r.NoError(err, "%s", out)

	manifestParts, walkParts := 0, 0
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "finish ") || !strings.Contains(line, "/shadow/") {
			continue
		}
		if strings.Contains(line, "(manifest,") {
			manifestParts++
		} else {
			walkParts++
		}
	}
	r.Equal(0, walkParts, "%s\ndownload fell back to Walk for %d parts, manifest.bolt.gz written by resumed upload is incomplete", out, walkParts)
	r.Equal(partitionsCount, manifestParts, "%s\nexpected %d parts downloaded via manifest", out, partitionsCount)
}
