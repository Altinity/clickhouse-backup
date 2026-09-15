//go:build integration

package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestRelativeDataPathTieredS3 verifies backup and restore of a tiered `default` -> s3 storage policy on a
// clickhouse-server started with a relative `<path>./</path>`. Such a server reports relative paths in
// `system.disks.path` and `system.tables.data_paths`; clickhouse-backup used to rewrite them into the
// plain-disk pseudo path `disks/<name>/`, after which no disk path was a prefix of any data path, every data
// path silently fell back to the `default` disk and the last one overwrote it, so `default` parts were
// hardlinked into the object disk metadata tree and `ATTACH PART` failed with code 27.
//
// clickhouse-server 26.8 canonicalises a relative `<path>` to an absolute path, there the test degenerates
// into a regular tiered backup/restore regression run instead of being skipped.
//
// See: https://github.com/Altinity/clickhouse-backup/issues/1121
func TestRelativeDataPathTieredS3(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "22.8") < 0 {
		t.Skipf("Test requires ClickHouse >= 22.8 for `type: cache` disk support, current version %s", version)
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	backupName := fmt.Sprintf("test_relative_path_%d", rand.Int())
	dbName := "test_relative_path_" + t.Name()
	tableName := "data"

	// Step 1: relative server `<path>` (zz_ prefix, so it merges after configs/storage_configuration.xml)
	// plus the reporter's tiered policy: a cache disk wrapping an s3 disk, both with relative metadata paths.
	env.DockerExecNoError(r, "clickhouse", "bash", "-xc", `
cat > /etc/clickhouse-server/config.d/zz_relative_path_test.xml <<'XML'
<clickhouse>
  <path>./</path>
  <storage_configuration>
    <disks>
      <relative_s3>
        <type>s3</type>
        <endpoint>https://minio:9000/clickhouse/relative_path_test/</endpoint>
        <access_key_id>access_key</access_key_id>
        <secret_access_key>it_is_my_super_secret_key</secret_access_key>
        <skip_access_check>true</skip_access_check>
      </relative_s3>
      <relative_cache>
        <type>cache</type>
        <disk>relative_s3</disk>
        <path>./disks/relative_cache/</path>
        <max_size>1073741824</max_size>
      </relative_cache>
    </disks>
    <policies>
      <relative_tiered>
        <volumes>
          <default>
            <disk>default</disk>
          </default>
          <cold>
            <disk>relative_cache</disk>
          </cold>
        </volumes>
      </relative_tiered>
    </policies>
  </storage_configuration>
</clickhouse>
XML
`)
	// the relative `<path>` is server wide, a leftover would break every test which later acquires this
	// pooled environment, so restore the container even when an assertion below calls t.FailNow.
	// `defer env.Cleanup` above was registered first, so this runs before the env returns to the pool
	defer func() {
		env.DockerExecNoError(r, "clickhouse", "rm", "-f", "/etc/clickhouse-server/config.d/zz_relative_path_test.xml")
		env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/relative_path_test")
		env.ch.Close()
		r.NoError(env.tc.RestartContainer(t, "clickhouse"))
		env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
	}()
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)

	var defaultDiskPath string
	r.NoError(env.ch.SelectSingleRowNoCtx(&defaultDiskPath, "SELECT path FROM system.disks WHERE name='default'"))
	isRelative := !strings.HasPrefix(defaultDiskPath, "/")
	log.Info().Msgf("system.disks default path=%q relative=%v", defaultDiskPath, isRelative)

	// Step 2: all parts stay on the `default` volume, the bug corrupted exactly those
	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, dt Date, value String) "+
			"ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id "+
			"SETTINGS storage_policy='relative_tiered'",
		dbName, tableName,
	))
	env.queryWithNoError(t, r, fmt.Sprintf(
		"INSERT INTO %s.%s SELECT number, toDate('2020-01-01'), toString(number) FROM numbers(1000)",
		dbName, tableName,
	))
	var defaultParts uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&defaultParts, fmt.Sprintf(
		"SELECT count() FROM system.parts WHERE database='%s' AND `table`='%s' AND active AND disk_name='default' SETTINGS empty_result_for_aggregation_by_empty_set=0",
		dbName, tableName)))
	r.Greater(defaultParts, uint64(0), "expected active parts on the `default` disk")
	env.checkCount(r, 1, 1000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))

	// Step 3: without disk_mapping clickhouse-backup can't know the clickhouse-server working directory,
	// it must say so instead of hardlinking parts relative to its own cwd
	if isRelative {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf(
			"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create --tables=%s.* %s_negative 2>&1", dbName, backupName))
		log.Info().Msg(out)
		r.Error(err, "create without disk_mapping must fail, got: %s", out)
		r.Contains(out, "relative path")
		r.Contains(out, "disk_mapping")
	}

	// Step 4: disk_mapping["default"] is the absolute server data path and resolves every sibling disk
	diskMappingEnv := "CLICKHOUSE_DISK_MAPPING=default:/var/lib/clickhouse"
	runBackup := func(args ...string) string {
		cmd := fmt.Sprintf("%s LOG_LEVEL=debug clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml %s 2>&1",
			diskMappingEnv, strings.Join(args, " "))
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", cmd)
		r.NoError(err, "%s failed: %s", cmd, out)
		return out
	}
	runBackup("create", "--tables="+dbName+".*", backupName)
	runBackup("upload", backupName)
	runBackup("delete", "local", backupName)
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC", dbName, tableName))
	runBackup("download", backupName)
	restoreOut := runBackup("restore", "--tables="+dbName+".*", backupName)

	// `Link <abs backup path> -> disks/<name>/store/...` is the issue symptom: a relative hardlink target,
	// created relative to the clickhouse-backup working directory instead of the clickhouse-server one
	r.NotContains(restoreOut, "-> disks/")
	env.checkCount(r, 1, 1000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))

	// Step 5: cleanup, the config.d override and the minio prefix are removed by the defer above.
	// the negative run must not leave a backup behind, drop it best effort in case a version
	// unexpectedly gets past checkDisksConsistency
	if _, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local "+backupName+"_negative 2>&1 || true"); err != nil {
		log.Warn().Msgf("delete local %s_negative: %v", backupName, err)
	}
	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"},
		[]string{"test_relative_path"}, true, true, true, "config-s3.yml")
}
