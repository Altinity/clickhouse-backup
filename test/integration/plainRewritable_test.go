//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
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
	// a failed r.NoError aborts the scenario via t.FailNow before the DROP DATABASE below and the
	// leftover database on the plain disk poisons every later test acquiring this pooled env: their
	// `create` picks up a table whose bucket their config can't reach,
	// https://github.com/Altinity/clickhouse-backup/actions/runs/33397256466
	defer func() {
		if t.Failed() {
			if dropErr := env.ch.Query("DROP DATABASE IF EXISTS " + dbName + " SYNC"); dropErr != nil {
				t.Logf("deferred DROP DATABASE %s: %v", dbName, dropErr)
			}
		}
	}()
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
	// 5m query timeout: DROP DATABASE on a real-GCS plain_rewritable disk crawls when the
	// pooled TLS connections to storage.googleapis.com go stale (every first request eats a
	// full SSL_read timeout before the retry succeeds) and exceeds the usual 1m read timeout
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 5*time.Minute)

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

// runPlainRewritableHybridScenario backs up a table of the s3_plain_rewritable_hybrid policy with partition 0 moved to
// the plain_rewritable volume, checks that merges and moves of the table work again after create and that the
// backup metadata lists the local parts and, unless skipPlainDisk put the plain disk into skip_disks, the plain
// ones. With checkRestore the table is then dropped, restored and its row count verified, restore into
// plain_rewritable needs ClickHouse 25.11+.
func (env *TestEnvironment) runPlainRewritableHybridScenario(t *testing.T, r *require.Assertions, dbName string, skipPlainDisk, checkRestore bool) {
	backupName := fmt.Sprintf("%s_%d", dbName, rand.Int())
	tableName := "data"
	backupEnv := ""
	if skipPlainDisk {
		backupEnv = "CLICKHOUSE_SKIP_DISKS=disk_s3_plain_rewritable "
	}

	env.queryWithNoError(t, r, "DROP DATABASE IF EXISTS "+dbName+" SYNC")
	defer func() {
		if t.Failed() {
			if dropErr := env.ch.Query("DROP DATABASE IF EXISTS " + dbName + " SYNC"); dropErr != nil {
				t.Logf("deferred DROP DATABASE %s: %v", dbName, dropErr)
			}
		}
	}()
	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, payload String) ENGINE=MergeTree() PARTITION BY id %% 4 ORDER BY id SETTINGS storage_policy='s3_plain_rewritable_hybrid'",
		dbName, tableName,
	))
	env.queryWithNoError(t, r, fmt.Sprintf(
		"INSERT INTO %s.%s SELECT number, repeat('x', 128) FROM numbers(1000)",
		dbName, tableName,
	))
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.%s MOVE PARTITION ID '0' TO VOLUME 'cold'", dbName, tableName))
	env.checkCount(r, 1, 1, fmt.Sprintf(
		"SELECT count() FROM (SELECT DISTINCT disk_name FROM system.parts WHERE database='%s' AND table='%s' AND active AND disk_name='disk_s3_plain_rewritable')",
		dbName, tableName,
	))

	// without <disk>/shadow create makes it for the hardlinks of the local parts, as root in the clickhouse-backup
	// container: the directory is shared with clickhouse-server and must get its owner, see the FREEZE check below
	env.DockerExecNoError(r, "clickhouse-backup", "rm", "-rf", "/var/lib/clickhouse/shadow")

	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		backupEnv+"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create_remote --tables="+dbName+".* "+backupName)
	log.Debug().Msg(out)
	r.NoError(err, out)

	// SYSTEM STOP MERGES / STOP MOVES of create are undone: an OPTIMIZE and a move succeed
	env.queryWithNoError(t, r, fmt.Sprintf("OPTIMIZE TABLE %s.%s PARTITION ID '1' FINAL", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.%s MOVE PARTITION ID '1' TO VOLUME 'cold'", dbName, tableName))

	// clickhouse-server stats shadow/increment.txt on every FREEZE: a shadow directory left to root by create would
	// fail the FREEZE of every table on the server with Permission denied
	frozenTable := "frozen_after_create"
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.%s (id UInt64) ENGINE=MergeTree() ORDER BY id", dbName, frozenTable))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.%s SELECT number FROM numbers(10)", dbName, frozenTable))
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.%s FREEZE WITH NAME '%s'", dbName, frozenTable, backupName))
	env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.%s UNFREEZE WITH NAME '%s'", dbName, frozenTable, backupName))
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE %s.%s SYNC", dbName, frozenTable))

	// the local parts of partitions 1..3 come from the hardlinked shadow of the default disk, the parts of
	// partition 0 from the bucket of the plain disk
	metaOut, metaErr := env.DockerExecOut("clickhouse-backup", "cat",
		fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata/%s/%s.json", backupName, dbName, tableName))
	r.NoError(metaErr, "can't read table metadata json: %s", metaOut)
	var tableMeta metadata.TableMetadata
	r.NoError(json.Unmarshal([]byte(metaOut), &tableMeta), "can't parse table metadata json: %s", metaOut)
	r.NotEmpty(tableMeta.Parts["default"], "local parts must be in the backup: %s", metaOut)
	if skipPlainDisk {
		r.Empty(tableMeta.Parts["disk_s3_plain_rewritable"], "the skipped plain disk must not be in the backup: %s", metaOut)
	} else {
		r.NotEmpty(tableMeta.Parts["disk_s3_plain_rewritable"], "plain disk parts must be in the backup: %s", metaOut)
	}
	for diskName, diskParts := range tableMeta.Parts {
		for _, p := range diskParts {
			if strings.SplitN(p.Name, "_", 2)[0] == "0" {
				r.Equal("disk_s3_plain_rewritable", diskName, "partition 0 was moved to the plain disk: %s", metaOut)
			}
		}
	}

	if !checkRestore {
		fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, true, true, "config-s3.yml")
		return
	}
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE %s.%s SYNC", dbName, tableName))
	env.queryWithNoError(t, r, "DROP DATABASE "+dbName+" SYNC")

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		backupEnv+"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore_remote "+backupName)
	log.Debug().Msg(out)
	r.NoError(err, out)

	expectedColdRows := uint64(250)
	if skipPlainDisk {
		expectedColdRows = 0
	}
	env.checkCount(r, 1, 750+expectedColdRows, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))
	env.checkCount(r, 1, expectedColdRows, fmt.Sprintf("SELECT count() FROM %s.%s WHERE id %% 4 = 0 SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName, tableName))

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, true, true, "config-s3.yml")
}

// TestPlainRewritableHybridS3 verifies a table whose storage policy keeps a hot volume on a local disk and a cold
// volume on a plain_rewritable disk (s3_plain_rewritable_hybrid in dynamic_settings.sh). ClickHouse rejects FREEZE for
// such a table, so create hardlinks the local parts with merges and moves stopped and copies the plain parts from
// the bucket. With the plain disk in skip_disks only the local parts (partitions 1..3, 750 rows) are backed up and
// restored; without it the whole table is backed up on every version, and comes back on restore into
// plain_rewritable (25.11+).
func TestPlainRewritableHybridS3(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	// before 25.4 ALTER TABLE MOVE|ATTACH PARTITION|PART is rejected for a table with a plain_rewritable disk in its
	// storage policy, https://github.com/ClickHouse/ClickHouse/commit/b2a1123c13e
	if compareVersion(version, "25.4") < 0 {
		t.Skipf("Test requires ClickHouse >= 25.4 (MOVE and ATTACH PART on a storage policy with plain_rewritable disks), current %s", version)
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	env.runPlainRewritableHybridScenario(t, r, "test_plain_rewritable_hybrid", true, true)
	env.runPlainRewritableHybridScenario(t, r, "test_plain_rewritable_hybrid", false, compareVersion(version, "25.11") >= 0)
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/disk_s3_plain_rewritable")
}
