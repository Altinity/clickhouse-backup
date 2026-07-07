//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/stretchr/testify/require"
)

// TestDiffFromChecksums covers https://github.com/Altinity/clickhouse-backup/issues/1307:
// when --diff-from-remote finds a backup part by name, the Required flag must
// be set only if the content fingerprint (hash_of_all_files, or legacy
// checksums when hash_of_all_files is absent) also matches. The scenario is
// exercised against both a local-disk table and an s3 object-disk table so the
// object_disk CopyObject path is covered too.
func TestDiffFromChecksums(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	if !isAdvancedMode() {
		t.Skip("requires advanced mode with storage policies")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") < 0 {
		t.Skip("requires ClickHouse >= 21.8 for s3_only storage policy")
	}

	// Case 1: same part name, DIFFERENT content → Required must be false and
	// shadow files for the part must be present in the local backup directory
	// so the part is uploaded instead of being inherited from the diff source.
	runDiffFromChecksumsCase(t, r, env, "test_diff_cksum_mismatch", false)

	// Case 2: same part name, SAME content → Required must be true and no
	// part files should be linked into the local backup directory.
	runDiffFromChecksumsCase(t, r, env, "test_diff_cksum_match", true)
}

func runDiffFromChecksumsCase(t *testing.T, r *require.Assertions, env *TestEnvironment, dbName string, sameData bool) {
	tableLocal := "t_local"
	tableS3 := "t_s3"
	fullBackup := dbName + "_full"
	incrementBackup := dbName + "_increment"

	type partMeta struct {
		Name     string `json:"name"`
		Required bool   `json:"required,omitempty"`
	}
	type tableMetaJSON struct {
		Checksums      map[string]uint64     `json:"checksums"`
		HashOfAllFiles map[string]string     `json:"hash_of_all_files"`
		Parts          map[string][]partMeta `json:"parts"`
	}

	// Clean any leftovers from previous runs
	for _, b := range []string{fullBackup, incrementBackup} {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote "+b+" 2>/dev/null || true")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local "+b+" 2>/dev/null || true")
	}
	r.NoError(env.dropDatabase(dbName, true))

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+"."+tableLocal+" (id UInt64, v String) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+"."+tableS3+" (id UInt64, v String) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='s3_only'")

	// First insert: deterministic part `all_1_1_0` on each disk
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+"."+tableLocal+" SELECT number, 'A' FROM numbers(100)")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+"."+tableS3+" SELECT number, 'A' FROM numbers(100)")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create_remote", "--tables="+dbName+".*", fullBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", fullBackup)

	// DROP+recreate resets the non-replicated MergeTree block counter so the
	// re-inserted part lands under the same `all_1_1_0` name as before.
	env.queryWithNoError(t, r, "DROP TABLE "+dbName+"."+tableLocal+" SYNC")
	env.queryWithNoError(t, r, "DROP TABLE "+dbName+"."+tableS3+" SYNC")
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+"."+tableLocal+" (id UInt64, v String) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+"."+tableS3+" (id UInt64, v String) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='s3_only'")

	payload := "A"
	if !sameData {
		payload = "B"
	}
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.%s SELECT number, '%s' FROM numbers(100)", dbName, tableLocal, payload))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.%s SELECT number, '%s' FROM numbers(100)", dbName, tableS3, payload))

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create_remote", "--tables="+dbName+".*", "--diff-from-remote="+fullBackup, incrementBackup)

	verifyRequired := func(table, expectedDisk string) {
		metaPath := path.Join("/var/lib/clickhouse/backup", incrementBackup, "metadata", common.TablePathEncode(dbName), common.TablePathEncode(table)+".json")
		out, err := env.DockerExecOut("clickhouse-backup", "cat", metaPath)
		r.NoError(err, "cat %s: %s", metaPath, out)
		var tm tableMetaJSON
		r.NoError(json.Unmarshal([]byte(out), &tm))
		parts, ok := tm.Parts[expectedDisk]
		r.Truef(ok, "table %s.%s has no parts on disk %s; metadata=%s", dbName, table, expectedDisk, out)
		r.NotEmptyf(parts, "table %s.%s: expected at least one part on disk %s", dbName, table, expectedDisk)
		for _, p := range parts {
			if sameData {
				r.Truef(p.Required, "table %s.%s part %s: expected Required=true (matching content) but got false; metadata=%s", dbName, table, p.Name, out)
			} else {
				r.Falsef(p.Required, "table %s.%s part %s: expected Required=false (different content) but got true; metadata=%s", dbName, table, p.Name, out)
			}
		}
	}

	verifyRequired(tableLocal, "default")
	verifyRequired(tableS3, "disk_s3")

	// On mismatch, demoted parts must have been re-linked from shadow into the
	// local backup tree (covers both local disk and object disk paths).
	if !sameData {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", path.Join("/var/lib/clickhouse/backup", incrementBackup, "shadow", common.TablePathEncode(dbName), common.TablePathEncode(tableLocal), "default", "all_1_1_0"))
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", path.Join("/var/lib/clickhouse/disks/disk_s3/backup", incrementBackup, "shadow", common.TablePathEncode(dbName), common.TablePathEncode(tableS3), "disk_s3", "all_1_1_0"))
	}

	// End-to-end: restore the increment and verify the data round-trips for
	// both the local-disk table and the s3 object-disk table.
	env.queryWithNoError(t, r, "DROP TABLE "+dbName+"."+tableLocal+" SYNC")
	env.queryWithNoError(t, r, "DROP TABLE "+dbName+"."+tableS3+" SYNC")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", incrementBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore_remote", "--rm", incrementBackup)
	env.checkCount(r, 1, 100, "SELECT count() FROM "+dbName+"."+tableLocal)
	env.checkCount(r, 1, 100, "SELECT count() FROM "+dbName+"."+tableS3)

	// Cleanup
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", incrementBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", fullBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", incrementBackup)
	r.NoError(env.dropDatabase(dbName, true))
	_ = t
}
