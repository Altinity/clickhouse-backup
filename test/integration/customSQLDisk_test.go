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

	"github.com/rs/zerolog/log"
)

// TestCustomSQLDisk verifies backup/restore of MergeTree tables whose storage is
// declared inline in the DDL via `SETTINGS disk = disk(...)` instead of a
// storage policy defined in the server configuration.
//
// Such disks are registered by ClickHouse under a generated name
// `__tmp_internal_<hash of the canonical AST>` (or under `name = '...'` when the
// definition provides one). They are absent from
// `preprocessed_configs/config.xml`, so clickhouse-backup has to take the object
// storage credentials from the table DDL itself.
//
// See: https://github.com/Altinity/clickhouse-backup/issues/943
func TestCustomSQLDisk(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	// `disk(...)` exists since 23.2 and `name = ` since 23.8, but the
	// `__tmp_internal_` custom disk registry was reworked in 24.8; before that
	// the generated names and their system.disks rows are not stable enough to
	// assert on. https://github.com/Altinity/clickhouse-backup/issues/943
	if compareVersion(version, "24.8") < 0 {
		t.Skipf("Test requires ClickHouse >= 24.8 for stable `SETTINGS disk = disk(...)` custom disk registry, current version %s", version)
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	backupName := fmt.Sprintf("test_custom_sql_disk_%d", rand.Int())
	dbName := "test_custom_sql_disk_" + t.Name()
	mappedDBName := dbName + "_mapped"

	const (
		// skip_access_check mirrors the s3 disks of cacheDiskBackup_test.go and serverAPI_test.go,
		// the disk access-check probe can block CREATE TABLE for minutes when the background schedule pool is
		// saturated right after a container restart, the probe removal waits for blob removal without a timeout
		s3Creds     = "access_key_id = 'access_key', secret_access_key = 'it_is_my_super_secret_key', skip_access_check = true"
		s3EndPrefix = "https://minio:9000/clickhouse/custom_sql_disk/"
	)

	// A cached custom disk is rejected unless the server config declares a base
	// directory for it, see ClickHouse RegisterDiskCache.cpp - the fallback to
	// <path>/caches applies to ATTACH only.
	env.DockerExecNoError(r, "clickhouse", "bash", "-xc", `
cat > /etc/clickhouse-server/config.d/custom_sql_disk_test.xml <<'XML'
<clickhouse>
  <custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
</clickhouse>
XML
`)
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)

	env.queryWithNoError(t, r, "DROP DATABASE IF EXISTS "+dbName+" SYNC")
	env.queryWithNoError(t, r, "DROP DATABASE IF EXISTS "+mappedDBName+" SYNC")
	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)

	// table name -> number of rows, all four disk shapes described in issue #943
	tables := map[string]uint64{
		"t_plain_s3":     1000,
		"t_cache_s3":     500,
		"t_named_s3":     700,
		"t_encrypted_s3": 300,
	}

	// (a) plain s3 custom disk with an explicit metadata_path
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.t_plain_s3 (id UInt64, s String) ENGINE=MergeTree() ORDER BY id "+
			"SETTINGS disk = disk(type = s3, endpoint = '%st_plain_s3/', %s, metadata_path = '/var/lib/clickhouse/disks/custom_sql_plain_s3/')",
		dbName, s3EndPrefix, s3Creds))

	// (b) cache wrapper over a nested s3 custom disk
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.t_cache_s3 (id UInt64, s String) ENGINE=MergeTree() ORDER BY id "+
			"SETTINGS disk = disk(type = cache, max_size = 1073741824, path = '/var/lib/clickhouse/caches/custom_sql_cache/', "+
			"disk = disk(type = s3, endpoint = '%st_cache_s3/', %s))",
		dbName, s3EndPrefix, s3Creds))

	// (c) explicitly named s3 custom disk, storage_policy becomes '__custom_named_s3'
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.t_named_s3 (id UInt64, s String) ENGINE=MergeTree() ORDER BY id "+
			"SETTINGS disk = disk(name = 'custom_named_s3', type = s3, endpoint = '%st_named_s3/', %s)",
		dbName, s3EndPrefix, s3Creds))

	// (d) encrypted wrapper over a nested s3 custom disk
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.t_encrypted_s3 (id UInt64, s String) ENGINE=MergeTree() ORDER BY id "+
			"SETTINGS disk = disk(type = encrypted, key = '1234567812345678', path = 'enc/', "+
			"disk = disk(type = s3, endpoint = '%st_encrypted_s3/', %s))",
		dbName, s3EndPrefix, s3Creds))

	for tableName, rows := range tables {
		env.queryWithNoError(t, r, fmt.Sprintf(
			"INSERT INTO %s.%s SELECT number, toString(number) FROM numbers(%d)", dbName, tableName, rows))
		env.checkCount(r, 1, rows, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))
	}

	// every table must use its own generated (or named) disk, none of them may fall back to `default`
	// system.parts.disk_name reports the cache wrapper name on some versions and the wrapped disk name on others,
	// both share one system.disks.path, so the parts disks are compared with the backup metadata by path
	var partDisks string
	r.NoError(env.ch.SelectSingleRowNoCtx(&partDisks, fmt.Sprintf(
		"SELECT arrayStringConcat(arraySort(groupUniqArray(concat(p.disk_name, '=', d.path))), ',') "+
			"FROM system.parts AS p INNER JOIN system.disks AS d ON d.name = p.disk_name "+
			"WHERE p.database='%s' AND p.active SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName)))
	log.Debug().Msgf("custom SQL disks used by %s parts: %s", dbName, partDisks)
	r.NotEmpty(partDisks, "expect active parts on custom SQL disks in %s", dbName)
	partDiskPaths := map[string]string{}
	for _, nameAndPath := range strings.Split(partDisks, ",") {
		diskName, diskPath, found := strings.Cut(nameAndPath, "=")
		r.True(found, "unexpected system.parts disk entry %s", nameAndPath)
		r.NotEqual("default", diskName, "custom SQL disk tables must not store parts on `default`")
		partDiskPaths[diskName] = diskPath
	}
	r.Contains(partDiskPaths, "custom_named_s3", "expect the explicitly named custom disk in system.parts")

	// the leak check at the end of the test is only trustworthy when this same command can see objects,
	// the minio container has bash and mc but serves a self signed certificate, so mc needs --insecure
	lsCustomSQLDiskObjects := func() string {
		const mcAliasCmd = "mc --insecure alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null"
		out, execErr := env.DockerExecOut("minio", "bash", "-c", mcAliasCmd+" && mc --insecure ls -r local/clickhouse/custom_sql_disk/ 2>&1 || true")
		if execErr != nil {
			t.Logf("mc ls of clickhouse/custom_sql_disk/ failed: %v, output: %s", execErr, out)
		}
		return strings.TrimSpace(out)
	}
	r.NotEmpty(lsCustomSQLDiskObjects(), "the inserted rows must be visible as objects under clickhouse/custom_sql_disk/, otherwise the leftover check below is vacuous")

	log.Debug().Msg("create_remote backup of custom SQL disk tables")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml",
		"create_remote", "--tables="+dbName+".*", backupName)

	// the generated disk names must be recorded in the backup metadata
	metaOut, metaErr := env.DockerExecOut("clickhouse-backup", "cat", "/var/lib/clickhouse/backup/"+backupName+"/metadata.json")
	r.NoError(metaErr, "can't read backup metadata.json: %s", metaOut)
	backupMeta := struct {
		Disks     map[string]string `json:"disks"`
		DiskTypes map[string]string `json:"disk_types"`
	}{}
	r.NoError(json.Unmarshal([]byte(metaOut), &backupMeta), "can't parse backup metadata.json: %s", metaOut)
	for diskName, diskPath := range partDiskPaths {
		metaDiskName := ""
		for name, metaPath := range backupMeta.Disks {
			if metaPath == diskPath {
				metaDiskName = name
				break
			}
		}
		r.NotEmpty(metaDiskName, "backup metadata.json `disks` must contain custom SQL disk %s (%s), metadata: %s", diskName, diskPath, metaOut)
		r.Contains(backupMeta.DiskTypes, metaDiskName, "backup metadata.json `disk_types` must contain custom SQL disk %s, metadata: %s", metaDiskName, metaOut)
		r.NotEqual("local", backupMeta.DiskTypes[metaDiskName], "custom SQL disk %s must not be detected as `local`, metadata: %s", metaDiskName, metaOut)
	}

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	env.queryWithNoError(t, r, "DROP DATABASE "+dbName+" SYNC")

	log.Debug().Msg("restore_remote custom SQL disk tables into the original database")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml",
		"restore_remote", "--tables="+dbName+".*", backupName)
	for tableName, rows := range tables {
		env.checkCount(r, 1, rows, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))
	}

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)

	log.Debug().Msg("restore_remote custom SQL disk tables with --restore-database-mapping")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml",
		"restore_remote", "--restore-database-mapping", dbName+":"+mappedDBName,
		"--tables="+dbName+".*", backupName)
	for tableName, rows := range tables {
		env.checkCount(r, 1, rows, fmt.Sprintf("SELECT count() FROM %s.%s", mappedDBName, tableName))
	}

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"},
		[]string{dbName, mappedDBName}, false, true, true, "config-s3.yml")

	// DROP DATABASE ... SYNC and `delete remote/local` must leave nothing behind: no objects under the custom disk
	// prefix in the bucket and no files under the custom disk metadata / cache directories, a leftover is a leak
	// of blobs or metadata. CH 26.3+ deletes blobs asynchronously (BlobKillerThread), wait for it per custom disk.
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "26.2") > 0 {
		var customDisks []struct {
			Name string `ch:"name"`
		}
		// an `encrypted` wrapper is rejected with "is not an object storage disk", its blobs belong to the wrapped disk
		r.NoError(env.ch.Select(&customDisks, "SELECT name FROM system.disks WHERE (name LIKE '__tmp_internal_%' OR name = 'custom_named_s3') AND is_encrypted = 0"))
		for _, disk := range customDisks {
			if err := env.ch.Query(fmt.Sprintf("SYSTEM WAIT BLOBS CLEANUP '%s'", disk.Name)); err != nil {
				t.Logf("SYSTEM WAIT BLOBS CLEANUP '%s': %v", disk.Name, err)
			}
		}
	}
	leftoverObjects := lsCustomSQLDiskObjects()
	r.Empty(leftoverObjects, "expected no objects under clickhouse/custom_sql_disk/ after cleanup, got:\n%s", leftoverObjects)
	// `status` is the bookkeeping file of the filesystem cache itself, not cached data
	leftoverFiles, _ := env.DockerExecOut("clickhouse", "bash", "-c", "find /var/lib/clickhouse/disks/custom_sql_plain_s3 /var/lib/clickhouse/caches/custom_sql_cache -type f ! -name status 2>/dev/null || true")
	r.Empty(strings.TrimSpace(leftoverFiles), "expected no files under the custom disk metadata and cache directories after cleanup, got:\n%s", leftoverFiles)

	env.DockerExecNoError(r, "clickhouse", "rm", "-f", "/etc/clickhouse-server/config.d/custom_sql_disk_test.xml")
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "rm -rf /var/lib/clickhouse/disks/custom_sql_plain_s3 /var/lib/clickhouse/caches/custom_sql_cache")
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/custom_sql_disk")
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
}
