//go:build integration

package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestCacheDiskBackup verifies that backup and restore work correctly when a
// ClickHouse cache disk wraps an S3 object disk. Both disks share the same
// metadata path in system.disks, so GROUP BY d.path collapses them into one
// row. Before the fix, any(d.name) could pick the cache wrapper name (e.g.
// "s3_cache") instead of the underlying disk name ("s3_disk"). Since
// system.parts always reports the underlying disk name, this mismatch caused
// fetchHashOfAllFiles to fail with "part not found after FREEZE".
//
// The fix uses argMin(d.name, d.cache_path != ”) to always prefer the
// underlying disk (empty cache_path) over the cache wrapper (non-empty
// cache_path).
//
// See: https://github.com/Altinity/clickhouse-backup/issues/1396
func TestCacheDiskBackup(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "22.8") < 0 {
		t.Skipf("Test requires ClickHouse >= 22.8 for `type: cache` disk support, current version %s", version)
	}

	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	backupName := fmt.Sprintf("test_cache_disk_%d", rand.Int())
	dbName := "test_cache_disk_" + t.Name()
	tableName := "data"

	// Step 1: Configure ClickHouse with an S3 object disk wrapped by a cache disk.
	// Both disks will share the same metadata path, reproducing the bug scenario.
	env.DockerExecNoError(r, "clickhouse", "bash", "-xc", `
cat > /etc/clickhouse-server/config.d/cache_disk_test.xml <<'XML'
<clickhouse>
  <storage_configuration>
    <disks>
      <s3_disk>
        <type>s3</type>
        <endpoint>https://minio:9000/clickhouse/cache_disk_test/</endpoint>
        <access_key_id>access_key</access_key_id>
        <secret_access_key>it_is_my_super_secret_key</secret_access_key>
        <skip_access_check>true</skip_access_check>
      </s3_disk>
      <s3_cache>
        <type>cache</type>
        <disk>s3_disk</disk>
        <path>/var/lib/clickhouse/filesystem_caches/s3_cache_test/</path>
        <max_size>1073741824</max_size>
      </s3_cache>
    </disks>
    <policies>
      <s3_tiered>
        <volumes>
          <main>
            <disk>default</disk>
          </main>
          <cold>
            <disk>s3_cache</disk>
          </cold>
        </volumes>
      </s3_tiered>
    </policies>
  </storage_configuration>
</clickhouse>
XML
`)
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)

	// Verify both disks are visible and share the same path
	var diskCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&diskCount,
		"SELECT count() FROM system.disks WHERE name IN ('s3_disk','s3_cache')"))
	r.Equal(uint64(2), diskCount, "expected both s3_disk and s3_cache in system.disks")

	// Step 2: Create a table using the s3_tiered policy and insert data.
	// Use TTL to move some data to the cold (S3) volume, and keep some on default (local).
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, dt Date, value String) "+
			"ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id "+
			"TTL dt + INTERVAL 1 DAY TO VOLUME 'cold' "+
			"SETTINGS storage_policy='s3_tiered'",
		dbName, tableName,
	))

	// Insert old data (will move to S3 via TTL) and recent data (stays on local)
	env.queryWithNoError(r, fmt.Sprintf(
		"INSERT INTO %s.%s SELECT number, toDate('2020-01-01'), toString(number) FROM numbers(1000)",
		dbName, tableName,
	))
	env.queryWithNoError(r, fmt.Sprintf(
		"INSERT INTO %s.%s SELECT number+1000, today(), toString(number+1000) FROM numbers(500)",
		dbName, tableName,
	))

	// Force TTL to move old data to S3
	env.queryWithNoError(r, fmt.Sprintf(
		"OPTIMIZE TABLE %s.%s FINAL", dbName, tableName,
	))
	env.queryWithNoError(r, fmt.Sprintf(
		"ALTER TABLE %s.%s MATERIALIZE TTL", dbName, tableName,
	))

	// Poll system.parts until active parts appear on either disk.
	// TTL moves are asynchronous; on slow CI runners a fixed 10s sleep is not
	// always sufficient. Wait up to 60s for at least one active part to exist.
	var localParts, s3Parts uint64
	deadline := time.Now().Add(60 * time.Second)
	for {
		r.NoError(env.ch.SelectSingleRowNoCtx(&localParts,
			fmt.Sprintf("SELECT count() FROM system.parts WHERE database='%s' AND `table`='%s' AND active AND disk_name='default'", dbName, tableName)))
		r.NoError(env.ch.SelectSingleRowNoCtx(&s3Parts,
			fmt.Sprintf("SELECT count() FROM system.parts WHERE database='%s' AND `table`='%s' AND active AND disk_name LIKE 's3%%'", dbName, tableName)))
		if s3Parts > 0 && localParts > 0 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(1 * time.Second)
	}
	log.Debug().Msgf("Parts distribution: local=%d, s3=%d", localParts, s3Parts)
	if s3Parts == 0 || localParts == 0 {
		var diag string
		r.NoError(env.ch.SelectSingleRowNoCtx(&diag,
			fmt.Sprintf("SELECT toString(groupArray((disk_name, name, active))) FROM system.parts WHERE database='%s' AND `table`='%s'", dbName, tableName)))
		log.Warn().Msgf("Parts diagnostic: %s", diag)
	}
	r.True(s3Parts > 0 || localParts > 0, "expected at least some parts to exist")

	totalRows := uint64(1500)
	env.checkCount(r, 1, totalRows, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))

	// Step 3: Create backup — this is where the bug manifested.
	// Before the fix, any(d.name) could pick "s3_cache" instead of "s3_disk",
	// causing "part not found in system.parts after FREEZE".
	log.Debug().Msg("Creating backup with cache disk present")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml",
		"create", "--tables="+dbName+".*", backupName)

	// Step 4: Upload backup
	log.Debug().Msg("Uploading backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml",
		"upload", backupName)

	// Step 5: Delete local backup and drop the table
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml",
		"delete", "local", backupName)
	env.queryWithNoError(r, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC", dbName, tableName))

	// Step 6: Download and restore
	log.Debug().Msg("Downloading backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml",
		"download", backupName)
	log.Debug().Msg("Restoring backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml",
		"restore", "--tables="+dbName+".*", backupName)

	// Step 7: Verify data integrity after restore
	env.checkCount(r, 1, totalRows, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))
	log.Debug().Msg("Data integrity verified after restore")

	// Step 8: Cleanup
	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"},
		[]string{"test_cache_disk"}, true, true, true, "config-s3.yml")
	env.DockerExecNoError(r, "clickhouse", "rm", "-f", "/etc/clickhouse-server/config.d/cache_disk_test.xml")
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/cache_disk_test")
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
	env.Cleanup(t, r)
}
