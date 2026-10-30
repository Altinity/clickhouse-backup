//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestAntalyaCASDisk verifies embedded create_remote / restore_remote for a table on an Antalya
// content-addressed disk (`metadata_type=cas`) wrapped by a cache disk. Such a disk has no local
// directory behind system.disks.path, so the local disk pre-flight check must not reject it.
// See: https://github.com/Altinity/clickhouse-backup/issues/1562
func TestAntalyaCASDisk(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if !strings.Contains(version, "antalya") || compareVersion(version, "26.6") < 0 {
		t.Skipf("Test requires Altinity Antalya ClickHouse >= 26.6 for `metadata_type: cas` disks, current version %s", version)
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	backupName := "test_antalya_cas_disk"
	dbName := "test_antalya_cas_disk"
	tableName := "data"
	configFile := "/etc/clickhouse-backup/config-s3-embedded.yml"

	// MinIO OSS fails the CAS mount capability probe, skip_access_check bypasses it for s3 backend,
	// same as https://github.com/Altinity/clickhouse-backup/issues/1562 reporter config
	env.DockerExecNoError(r, "clickhouse", "bash", "-xc", `
cat > /etc/clickhouse-server/config.d/zz_antalya_cas_disk.xml <<'XML'
<clickhouse>
  <storage_configuration>
    <disks>
      <cas>
        <type>object_storage</type>
        <object_storage_type>s3</object_storage_type>
        <metadata_type>cas</metadata_type>
        <endpoint>https://minio:9000/clickhouse/antalya_cas_disk/</endpoint>
        <access_key_id>access_key</access_key_id>
        <secret_access_key>it_is_my_super_secret_key</secret_access_key>
        <cas_server_root_id>{replica}</cas_server_root_id>
        <skip_access_check>true</skip_access_check>
      </cas>
      <cas_cache>
        <type>cache</type>
        <disk>cas</disk>
        <path>/var/lib/clickhouse/disks/cas_cache/</path>
        <max_size>1073741824</max_size>
      </cas_cache>
    </disks>
    <policies>
      <cas>
        <move_factor>0</move_factor>
        <volumes>
          <main>
            <disk>cas_cache</disk>
          </main>
        </volumes>
      </cas>
    </policies>
  </storage_configuration>
</clickhouse>
XML
`)
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)

	var diskCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&diskCount, "SELECT count() FROM system.disks WHERE name IN ('cas','cas_cache')"))
	r.Equal(uint64(2), diskCount, "expected both cas and cas_cache in system.disks")

	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.%s (id UInt64, value String) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='cas'", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.%s SELECT number, toString(number) FROM numbers(1000)", dbName, tableName))
	totalRows := uint64(1000)
	env.checkCount(r, 1, totalRows, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))
	env.checkCount(r, 1, 0, fmt.Sprintf("SELECT count() FROM system.parts WHERE database='%s' AND `table`='%s' AND active AND disk_name NOT IN ('cas','cas_cache') SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName, tableName))

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "create_remote", "--tables="+dbName+".*", backupName)
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE %s.%s SYNC", dbName, tableName))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "restore_remote", "--tables="+dbName+".*", backupName)
	env.checkCount(r, 1, totalRows, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))
	env.checkCount(r, 1, 0, fmt.Sprintf("SELECT count() FROM system.parts WHERE database='%s' AND `table`='%s' AND active AND disk_name NOT IN ('cas','cas_cache') SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName, tableName))

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, true, true, "config-s3-embedded.yml")
	env.DockerExecNoError(r, "clickhouse", "rm", "-f", "/etc/clickhouse-server/config.d/zz_antalya_cas_disk.xml")
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/antalya_cas_disk")
}
