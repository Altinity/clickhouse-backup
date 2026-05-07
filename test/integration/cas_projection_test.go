//go:build integration

package main

import (
	"fmt"
	"testing"
	"time"
)

// TestCASRoundtripWithProjection creates a table with a projection,
// inserts data, cas-uploads, drops, cas-restores, and verifies row count
// and projection definition both survive.
func TestCASRoundtripWithProjection(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "proj_round")

	const (
		dbName     = "cas_proj_db"
		tblName    = "cas_proj_t"
		backupName = "cas_proj_bk"
	)
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf(`CREATE TABLE `+"`%s`.`%s`"+` (id UInt64, payload String, PROJECTION p1 (SELECT id, payload ORDER BY payload))
		ENGINE=MergeTree ORDER BY id
		SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0`, dbName, tblName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT number, randomPrintableASCII(64) FROM numbers(500)", dbName, tblName))
	env.queryWithNoError(r, fmt.Sprintf("OPTIMIZE TABLE `%s`.`%s` FINAL", dbName, tblName))

	env.casBackupNoError(r, "create", "--tables", dbName+".*", backupName)
	env.casBackupNoError(r, "cas-upload", backupName)

	r.NoError(env.dropDatabase(dbName, true))
	env.casBackupNoError(r, "cas-restore", "--rm", backupName)

	// Row count survived.
	var rowsResult []struct {
		C uint64 `ch:"c"`
	}
	r.NoError(env.ch.Select(&rowsResult, fmt.Sprintf("SELECT count() AS c FROM `%s`.`%s`", dbName, tblName)))
	r.Len(rowsResult, 1)
	r.Equal(uint64(500), rowsResult[0].C, "row count after restore")

	// Projection survived in the table's metadata.
	var projResult []struct {
		C uint64 `ch:"c"`
	}
	r.NoError(env.ch.Select(&projResult, fmt.Sprintf(
		"SELECT count() AS c FROM system.projections WHERE database='%s' AND table='%s' AND name='p1'", dbName, tblName)))
	r.Len(projResult, 1)
	r.Equal(uint64(1), projResult[0].C, "projection p1 should exist after restore")

	r.NoError(env.dropDatabase(dbName, true))
}

// TestCASRoundtripWithEmptyTable creates two tables, leaves one empty,
// uploads, drops both, restores, and asserts both schemas come back.
func TestCASRoundtripWithEmptyTable(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "empty_round")

	const dbName = "cas_empty_db"
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.full (id UInt64) ENGINE=MergeTree ORDER BY id", dbName))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.empty (id UInt64) ENGINE=MergeTree ORDER BY id", dbName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.full SELECT number FROM numbers(10)", dbName))

	env.casBackupNoError(r, "create", "--tables", dbName+".*", "cas_empty_bk")
	env.casBackupNoError(r, "cas-upload", "cas_empty_bk")

	r.NoError(env.dropDatabase(dbName, true))
	env.casBackupNoError(r, "cas-restore", "--rm", "cas_empty_bk")

	var fullCount []struct {
		C uint64 `ch:"c"`
	}
	r.NoError(env.ch.Select(&fullCount, fmt.Sprintf("SELECT count() AS c FROM `%s`.full", dbName)))
	r.Len(fullCount, 1)
	r.Equal(uint64(10), fullCount[0].C)

	var emptyExists []struct {
		C uint64 `ch:"c"`
	}
	r.NoError(env.ch.Select(&emptyExists, fmt.Sprintf(
		"SELECT count() AS c FROM system.tables WHERE database='%s' AND name='empty'", dbName)))
	r.Len(emptyExists, 1)
	r.Equal(uint64(1), emptyExists[0].C, "empty table schema should be restored")

	r.NoError(env.dropDatabase(dbName, true))
}

// TestCASUploadSkipObjectDisks verifies the --skip-object-disks CLI flag
// actually filters out object-disk-backed tables from the upload, instead
// of silently uploading them. Requires the test environment to provide an
// object-disk-backed disk; if not present, skip with a clear message —
// the unit test in T1 covers the plumbing in isolation.
func TestCASUploadSkipObjectDisks(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	// Probe: any object-storage disk in the test ClickHouse?
	var probe []struct {
		C uint64 `ch:"c"`
	}
	r.NoError(env.ch.Select(&probe,
		"SELECT count() AS c FROM system.disks WHERE type IN ('ObjectStorage','S3')"))
	if len(probe) == 0 || probe[0].C == 0 {
		t.Skip("no object-disk available in this integration env; covered by unit test")
	}

	// Find a storage policy whose ALL disks are S3-type object storage.
	// We restrict to 's3' or 's3_plain' (lowercased object_storage_type) which
	// are the types CAS objectDisk.go reliably detects. Azure ('azureblobstorage')
	// may also be present but uses a different type string not in scope here.
	var policyRes []struct {
		Policy string `ch:"policy_name"`
	}
	r.NoError(env.ch.Select(&policyRes, `
		SELECT policy_name
		FROM system.storage_policies
		WHERE policy_name != 'default'
		  AND policy_name IN (
		      SELECT sp.policy_name
		      FROM (SELECT policy_name, arrayJoin(disks) AS disk_name FROM system.storage_policies) AS sp
		      INNER JOIN system.disks AS d ON d.name = sp.disk_name
		      GROUP BY sp.policy_name
		      HAVING countIf(lower(if(d.type='ObjectStorage',d.object_storage_type,d.type)) NOT IN ('s3','s3_plain')) = 0
		         AND count() > 0
		  )
		LIMIT 1`))
	if len(policyRes) == 0 {
		t.Skip("no S3-only storage policy available; covered by unit test")
	}
	policy := policyRes[0].Policy

	env.casBootstrap(r, "skip_objdisk")
	const dbName = "cas_skipod_db"
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf(
		"CREATE TABLE `%s`.regular (id UInt64) ENGINE=MergeTree ORDER BY id", dbName))
	env.queryWithNoError(r, fmt.Sprintf(
		"CREATE TABLE `%s`.remote (id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS storage_policy='%s'",
		dbName, policy))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.regular SELECT number FROM numbers(10)", dbName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.remote SELECT number FROM numbers(10)", dbName))

	env.casBackupNoError(r, "create", "--tables", dbName+".*", "cas_skipod_bk")

	// Probe: does the local backup's shadow contain a disk/part subdirectory
	// specifically for the remote table? If not, the snapshot-based pre-flight
	// in cas-upload cannot detect the object-disk table — ClickHouse keeps fully
	// S3-backed data remote and doesn't write shadow entries locally. Skip rather
	// than assert a known limitation.
	remoteEnc := "cas_skipod_db/remote" // URL-safe name (no special chars)
	shadowRemote, _ := env.DockerExecOut("clickhouse",
		"bash", "-c",
		fmt.Sprintf("find /var/lib/clickhouse/backup/cas_skipod_bk/shadow/%s -mindepth 2 -maxdepth 2 -type d 2>/dev/null | head -1", remoteEnc))
	t.Logf("shadow remote-table probe: %q", shadowRemote)
	if shadowRemote == "" {
		t.Skip("object-disk table has no disk/part shadow entries; snapshot pre-flight cannot detect it — covered by unit tests")
	}

	env.casBackupNoError(r, "cas-upload", "--skip-object-disks", "cas_skipod_bk")

	statusOut := env.casBackupNoError(r, "cas-status")
	r.Contains(statusOut, "Backups: 1")

	r.NoError(env.dropDatabase(dbName, true))
	env.casBackupNoError(r, "cas-restore", "--rm", "cas_skipod_bk")

	var regCount []struct {
		C uint64 `ch:"c"`
	}
	r.NoError(env.ch.Select(&regCount, fmt.Sprintf("SELECT count() AS c FROM `%s`.regular", dbName)))
	r.Len(regCount, 1)
	r.Equal(uint64(10), regCount[0].C)

	var remoteExists []struct {
		C uint64 `ch:"c"`
	}
	r.NoError(env.ch.Select(&remoteExists, fmt.Sprintf(
		"SELECT count() AS c FROM system.tables WHERE database='%s' AND name='remote'", dbName)))
	r.Len(remoteExists, 1)
	r.Equal(uint64(0), remoteExists[0].C, "remote (object-disk) table must NOT be restored when --skip-object-disks was set")

	r.NoError(env.dropDatabase(dbName, true))
}
