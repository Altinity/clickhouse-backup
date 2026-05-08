//go:build integration

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// injectS3Object writes body to key inside the clickhouse bucket via the `mc`
// client already present in the MinIO container. This is the correct injection
// method: MinIO single-disk mode stores data in a non-trivial on-disk layout,
// so writing raw bytes directly into /minio/data/... is unreliable for LIST.
// Using mc cp through the S3 API guarantees the object is visible to LIST.
//
// We configure the mc alias inline (using the test credentials that match
// config-s3.yml) because the MinIO container only pre-sets the alias when
// minio_nodelete.sh is explicitly invoked.
func (env *TestEnvironment) injectS3Object(r *require.Assertions, key, body string) {
	// Write the body to a temp file then upload via mc cp.
	// Direct filesystem writes into /minio/data/... are not reliable for
	// MinIO LIST; using mc cp via the S3 API guarantees visibility.
	// The mc alias is set up inline because the container only pre-configures
	// it when minio_nodelete.sh is explicitly invoked.
	script := fmt.Sprintf(`
set -e
mc --insecure alias set inject https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null
echo -n '%s' > /tmp/inject_marker_tmp
mc --insecure cp /tmp/inject_marker_tmp inject/clickhouse/%s
rm -f /tmp/inject_marker_tmp
`, body, key)
	out, err := env.DockerExecOut("minio", "bash", "-c", script)
	r.NoError(err, "injectS3Object(%s) failed: %s", key, out)
}

// TestCASUploadRefusesConcurrent verifies that a second cas-upload for
// the same backup name fails cleanly when an inprogress marker is
// already present in the bucket. We pre-populate the marker via mc cp
// into MinIO to simulate a concurrent in-flight upload.
func TestCASUploadRefusesConcurrent(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "concurrent_up")

	const dbName = "cas_concur_up_db"
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", dbName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(10)", dbName))

	env.casBackupNoError(r, "create", "--tables", dbName+".*", "concur_bk")

	// Inject an inprogress marker BEFORE the upload so that the second host
	// simulates a concurrent upload in flight. We do NOT run cas-upload first:
	// if metadata.json already exists, cas-upload refuses with ErrBackupExists
	// (step 4) before it ever reaches the inprogress-marker check (step 5).
	// S3 path: backup/{cluster}/{shard}/cas/{clusterID}/inprogress/{name}.marker
	// casBootstrap used clusterID="concurrent_up"; path is backup/cluster/0/cas/concurrent_up/inprogress/concur_bk.marker
	markerKey := "backup/cluster/0/cas/concurrent_up/inprogress/concur_bk.marker"
	// Use tool="cas-upload" so the diagnostic surfaces the realistic
	// upload-vs-upload conflict (post wave-5 N2, the diagnostic uses the
	// marker's Tool field dynamically).
	markerBody := `{"backup":"concur_bk","host":"other","started_at":"2026-05-08T00:00:00Z","tool":"cas-upload"}`
	env.injectS3Object(r, markerKey, markerBody)

	// Second cas-upload must refuse with a message naming the conflicting tool.
	out, err := env.casBackup("cas-upload", "concur_bk")
	r.Error(err, "second cas-upload must refuse while marker held; out=%s", out)
	r.Contains(out, "another cas-upload is in progress", "out=%s", out)

	r.NoError(env.dropDatabase(dbName, true))
}

// TestCASPruneRefusesConcurrent verifies that a second cas-prune refuses
// when a prune marker is already held, AND that the existing marker
// survives the failed second run.
func TestCASPruneRefusesConcurrent(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "concurrent_pr")

	// Inject a prune marker simulating another prune in flight.
	// S3 path: backup/cluster/0/cas/concurrent_pr/prune.marker
	markerKey := "backup/cluster/0/cas/concurrent_pr/prune.marker"
	markerBody := `{"host":"other","started_at":"2026-05-08T00:00:00Z","run_id":"abcd1234","tool":"test"}`
	env.injectS3Object(r, markerKey, markerBody)

	// cas-prune must refuse.
	out, err := env.casBackup("cas-prune")
	r.Error(err, "cas-prune must refuse while marker held; out=%s", out)
	r.Contains(out, "another prune is in progress", "out=%s", out)

	// The marker must still be present (regression guard for the
	// "deferred-delete races second prune" bug fixed in T10).
	statusOut, err := env.casBackup("cas-status")
	r.NoError(err, "cas-status err=%v out=%s", err, statusOut)
	r.Contains(statusOut, "Prune marker:", "marker should still appear in cas-status; out=%s", statusOut)
}
