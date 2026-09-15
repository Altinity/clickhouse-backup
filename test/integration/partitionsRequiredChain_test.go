//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

// This test reproduces https://github.com/Altinity/clickhouse-backup/issues/1045:
// a 3-level chain full -> inc1 -> inc2 where the local copy of the intermediate backup inc1 was
// produced by an explicit `download --partitions=2 inc1` and therefore lists only the parts of
// partition 2. `downloadTableMetadataIfNotExists` used to trust that local file, so resolving the
// required part 1_1_1_0 of inc2 stopped at inc1 instead of recursing to full, and the download
// failed with "not found on <inc1> and all required backups sequence".
func TestDownloadPartitionsRequiredChainS3(t *testing.T) {
	const configFile = "config-s3.yml"
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	dbName := strings.ToLower("test_partitions_required_chain")
	tableName := "t"
	fullBackup := dbName + "_full"
	incr1Backup := dbName + "_inc1"
	incr2Backup := dbName + "_inc2"
	backups := []string{fullBackup, incr1Backup, incr2Backup}

	defer func() {
		fullCleanup(t, r, env, backups, []string{"remote", "local"}, []string{dbName}, false, false, false, configFile)
	}()

	for _, backupName := range childrenFirst(backups) {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+configFile+" delete remote "+backupName+" 2>/dev/null || true")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+configFile+" delete local "+backupName+" 2>/dev/null || true")
	}
	r.NoError(env.dropDatabase(dbName, true))

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+"."+tableName+" (p UInt8, v UInt64) ENGINE=MergeTree() PARTITION BY p ORDER BY v")
	// one part per partition with deterministic names 1_1_1_0, 2_2_2_0, 3_3_3_0
	env.queryWithNoError(t, r, "SYSTEM STOP MERGES "+dbName+"."+tableName)

	env.queryWithNoError(t, r, "INSERT INTO "+dbName+"."+tableName+" SELECT 1, number FROM numbers(100)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "create_remote", "--tables="+dbName+".*", fullBackup)

	env.queryWithNoError(t, r, "INSERT INTO "+dbName+"."+tableName+" SELECT 2, number FROM numbers(200)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "create_remote", "--diff-from-remote="+fullBackup, "--tables="+dbName+".*", incr1Backup)

	env.queryWithNoError(t, r, "INSERT INTO "+dbName+"."+tableName+" SELECT 3, number FROM numbers(300)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "create_remote", "--diff-from-remote="+incr1Backup, "--tables="+dbName+".*", incr2Backup)

	for _, backupName := range childrenFirst(backups) {
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "delete", "--force", "local", backupName)
	}

	// the stale intermediate state: an explicit filtered download of inc1 leaves a local table
	// metadata that lists only partition 2
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "download", "--partitions="+dbName+"."+tableName+":2", incr1Backup)
	incr1TableMeta := "/var/lib/clickhouse/backup/" + incr1Backup + "/metadata/" + dbName + "/" + tableName + ".json"
	staleMeta, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "cat "+incr1TableMeta)
	r.NoError(err, staleMeta)
	// only `parts` and `files` are narrowed by the partitions filter, `hash_of_all_files` keeps every
	// part of the backup, so assert on the parsed structure instead of on the raw json
	var staleTM metadata.TableMetadata
	r.NoError(json.Unmarshal([]byte(staleMeta), &staleTM), staleMeta)
	stalePartNames := make([]string, 0, len(staleTM.Parts["default"]))
	for _, part := range staleTM.Parts["default"] {
		stalePartNames = append(stalePartNames, part.Name)
	}
	r.Equal([]string{"2_2_2_0"}, stalePartNames, "local inc1 metadata must stay filtered to reproduce the issue")
	r.Equal([]string{"default_2_2_2_0.tar"}, staleTM.Files["default"], "local inc1 metadata must stay filtered to reproduce the issue")

	inc2ShadowDir := "/var/lib/clickhouse/backup/" + incr2Backup + "/shadow/" + dbName + "/" + tableName + "/default/"
	// the reproducer: resolving required part 1_1_1_0 has to skip the filtered inc1 and reach full
	downloadOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("DOWNLOAD_BY_PART=true LOG_LEVEL=debug clickhouse-backup -c /etc/clickhouse-backup/%s download --partitions=%s.%s:1 %s 2>&1", configFile, dbName, tableName, incr2Backup))
	r.NoError(err, downloadOut)
	// guard against passing trivially: the part must be resolved by walking the required chain,
	// not by the big-files fallback or a hardlink from another local backup
	r.Contains(downloadOut, "findDiffRecursive", "the 2-hop required-chain resolution did not run")
	shadowOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "ls -1 "+inc2ShadowDir)
	r.NoError(err, shadowOut)
	r.Equal("1_1_1_0", strings.TrimSpace(shadowOut))

	// the user's explicitly filtered local inc1 must not be rewritten by the inc2 download
	staleMetaAfter, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "cat "+incr1TableMeta)
	r.NoError(err, staleMetaAfter)
	r.Equal(staleMeta, staleMetaAfter, "local inc1 table metadata was rewritten by the inc2 download")

	dropSQL := "DROP TABLE " + dbName + "." + tableName
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		dropSQL += " SYNC"
	}
	env.queryWithNoError(t, r, dropSQL)
	restoreOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s restore --rm --partitions=%s.%s:1 %s 2>&1", configFile, dbName, tableName, incr2Backup))
	r.NoError(err, restoreOut)
	env.checkCount(r, 1, 100, "SELECT count() FROM "+dbName+"."+tableName)
	env.checkCount(r, 1, 100, "SELECT count() FROM "+dbName+"."+tableName+" WHERE p=1 SETTINGS empty_result_for_aggregation_by_empty_set=0")

	// the same stale inc1 with download_by_part disabled, so `Download` first recurses into inc1.
	// use_resumable_state is on by default, so the recursion resumes the stale local inc1 instead of
	// returning ErrBackupIsAlreadyExists, re-downloads full and hardlinks 1_1_1_0 into inc1 over the
	// leftover copy which the preceding download materialised there.
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "delete", "--force", "local", incr2Backup)
	downloadOut, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("DOWNLOAD_BY_PART=false LOG_LEVEL=debug clickhouse-backup -c /etc/clickhouse-backup/%s download --partitions=%s.%s:1 %s 2>&1", configFile, dbName, tableName, incr2Backup))
	r.NoError(err, downloadOut)
	r.Contains(downloadOut, "findDiffRecursive", "the 2-hop required-chain resolution did not run")
	shadowOut, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "ls -1 "+inc2ShadowDir)
	r.NoError(err, shadowOut)
	r.Equal("1_1_1_0", strings.TrimSpace(shadowOut))

	// restore_remote (Download + Restore) in the same stale state
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "delete", "--force", "local", incr2Backup)
	env.queryWithNoError(t, r, dropSQL)
	restoreOut, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s restore_remote --rm --partitions=%s.%s:1 %s 2>&1", configFile, dbName, tableName, incr2Backup))
	r.NoError(err, restoreOut)
	env.checkCount(r, 1, 100, "SELECT count() FROM "+dbName+"."+tableName)

	fullCleanup(t, r, env, backups, []string{"remote", "local"}, []string{dbName}, false, false, true, configFile)
}
