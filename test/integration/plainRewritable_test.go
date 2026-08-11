//go:build integration

package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// runPlainRewritableScenario executes one full backup/restore cycle for a MergeTree table living on
// a plain_rewritable disk: create+fill the table, create_remote, delete local backup, drop the
// table, restore_remote and verify row counts.
func (env *TestEnvironment) runPlainRewritableScenario(t *testing.T, r *require.Assertions, config, dbName, storagePolicy string) {
	backupName := fmt.Sprintf("%s_%d", dbName, rand.Int())
	tableName := "data"

	env.queryWithNoError(t, r, "DROP DATABASE IF EXISTS "+dbName+" SYNC")
	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, payload String) ENGINE=MergeTree() PARTITION BY id %% 4 ORDER BY id SETTINGS storage_policy='%s'",
		dbName, tableName, storagePolicy,
	))
	env.queryWithNoError(t, r, fmt.Sprintf(
		"INSERT INTO %s.%s SELECT number, repeat('x', 128) FROM numbers(1000)",
		dbName, tableName,
	))
	env.checkCount(r, 1, 1000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))

	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"clickhouse-backup -c /etc/clickhouse-backup/"+config+" create_remote --tables="+dbName+".* "+backupName)
	log.Debug().Msg(out)
	r.NoError(err, out)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/"+config, "delete", "local", backupName)
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE %s.%s SYNC", dbName, tableName))
	env.queryWithNoError(t, r, "DROP DATABASE "+dbName+" SYNC")

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"clickhouse-backup -c /etc/clickhouse-backup/"+config+" restore_remote "+backupName)
	log.Debug().Msg(out)
	r.NoError(err, out)

	env.checkCount(r, 1, 1000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))
	env.checkCount(r, 1, 500, fmt.Sprintf("SELECT count() FROM %s.%s WHERE id < 500 SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName, tableName))

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, true, true, config)
}

// TestPlainRewritableS3 verifies backup and restore of a MergeTree table whose data lives on a
// s3_plain_rewritable disk (declared with the legacy <type>s3_plain_rewritable</type> spelling).
// Such disks have no local VFS metadata files: parts are enumerated on the bucket level via the
// __meta/<token>/prefix.path layout during create, and restore writes the objects back and reloads
// the server path map via SYSTEM DROP DISK METADATA CACHE.
//
// The disk_s3_plain_rewritable disk and s3_plain_rewritable_only policy are pre-installed by
// test/integration/configs/dynamic_settings.sh (ClickHouse 24.8+).
func TestPlainRewritableS3(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	// restore into plain_rewritable requires SYSTEM DROP DISK METADATA CACHE reload, added in 25.11
	if compareVersion(version, "25.11") < 0 {
		t.Skipf("Test requires ClickHouse >= 25.11 (SYSTEM DROP DISK METADATA CACHE for plain_rewritable), current %s", version)
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	env.runPlainRewritableScenario(t, r, "config-s3.yml", "test_plain_rewritable_s3", "s3_plain_rewritable_only")
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/disk_s3_plain_rewritable")
}

// TestPlainRewritableS3Embedded verifies the same cycle with `use_embedded_backup_restore: true`:
// server-side BACKUP/RESTORE SQL reads and writes plain_rewritable parts through the disk interface
// itself (clickhouse-backup bucket-level plain disk logic is not involved), both with an embedded
// backup disk (config-s3-embedded.yml) and with a direct S3 destination (config-s3-embedded-url.yml).
func TestPlainRewritableS3Embedded(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	// keep the same gate as TestPlainRewritableS3: the disk is provisioned by dynamic_settings.sh
	// since 24.8, embedded restore into plain_rewritable verified on 25.11+
	if compareVersion(version, "25.11") < 0 {
		t.Skipf("Test requires ClickHouse >= 25.11 (restore into plain_rewritable disks), current %s", version)
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	for _, config := range []string{"config-s3-embedded.yml", "config-s3-embedded-url.yml"} {
		env.runPlainRewritableScenario(t, r, config, "test_plain_rewritable_emb", "s3_plain_rewritable_only")
	}
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/disk_s3_plain_rewritable")
}

// TestPlainRewritableGCSOverS3 verifies the same cycle for a real GCS bucket accessed via the S3
// XML API (<type>s3_plain_rewritable</type> + storage.googleapis.com endpoint, same credentials as
// disk_gcs) with a native GCS backup destination (config-gcs.yml): create enumerates parts in the
// GCS bucket, restore server-side copies them back through the disk s3 connection with a streaming
// fallback (allow_object_disk_streaming: true).
//
// The disk_gcs_plain_rewritable disk and gcs_plain_rewritable_only policy are provisioned by
// dynamic_settings.sh only when QA_GCS_OVER_S3_BUCKET is set.
func TestPlainRewritableGCSOverS3(t *testing.T) {
	if isTestShouldSkip("GCS_TESTS") || os.Getenv("QA_GCS_OVER_S3_BUCKET") == "" {
		t.Skip("Skipping GCS over S3 integration tests (GCS_TESTS / QA_GCS_OVER_S3_BUCKET not set)...")
		return
	}
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "25.11") < 0 {
		t.Skipf("Test requires ClickHouse >= 25.11 (SYSTEM DROP DISK METADATA CACHE for plain_rewritable), current %s", version)
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	env.runPlainRewritableScenario(t, r, "config-gcs.yml", "test_plain_rewritable_gcs", "gcs_plain_rewritable_only")
}

// TestPlainRewritableAzure verifies the same cycle for an azure_blob_storage disk with
// <metadata_type>plain_rewritable</metadata_type> living in a dedicated azurite container
// (azure-plain-rewritable-disk); for azure the common_key_prefix defaults to empty, so the
// __meta subtree lands in the container root. The disk_azblob_plain (metadata_type=plain) disk
// declared next to it additionally verifies that credentials parsing tolerates write-once plain disks.
func TestPlainRewritableAzure(t *testing.T) {
	if isTestShouldSkip("AZURE_TESTS") {
		t.Skip("Skipping Azure integration tests...")
		return
	}
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "25.11") < 0 {
		t.Skipf("Test requires ClickHouse >= 25.11 (SYSTEM DROP DISK METADATA CACHE for plain_rewritable), current %s", version)
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	env.runPlainRewritableScenario(t, r, "config-azblob.yml", "test_plain_rewritable_azure", "azure_plain_rewritable_only")
}
