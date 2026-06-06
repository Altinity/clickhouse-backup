//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests reproduce https://github.com/Altinity/clickhouse-backup/issues/1384 (point 1):
// a fresh `download <incremental>` recurses into `b.Download(RequiredBackup)` on the SAME
// *Backuper, the child overwrites b.dst and closes it via its deferred Close on return, so
// the parent is left with a closed b.dst and fails to download the increment's own new parts.
//
// The failure is storage specific: S3.Close()/AzureBlob.Close() are no-ops, so the bug is
// latent there, while GCS/FTP/SFTP actually close the underlying client/pool and surface the
// use-after-close. The tests run across all five backends so the matrix documents both the
// failing and the (accidentally) passing backends, and all of them must pass after the fix.

func TestDownloadRequiredBackupConnectionS3(t *testing.T) {
	runDownloadRequiredBackupConnectionCase(t, s3RestoreResolveIncrementCase())
}

func TestDownloadRequiredBackupConnectionFTP(t *testing.T) {
	runDownloadRequiredBackupConnectionCase(t, ftpRestoreResolveIncrementCase())
}

func TestDownloadRequiredBackupConnectionSFTP(t *testing.T) {
	runDownloadRequiredBackupConnectionCase(t, sftpRestoreResolveIncrementCase())
}

func TestDownloadRequiredBackupConnectionGCSEmulator(t *testing.T) {
	runDownloadRequiredBackupConnectionCase(t, gcsEmulatorRestoreResolveIncrementCase())
}

func TestDownloadRequiredBackupConnectionAzure(t *testing.T) {
	runDownloadRequiredBackupConnectionCase(t, azblobRestoreResolveIncrementCase())
}

func runDownloadRequiredBackupConnectionCase(t *testing.T, tc restoreResolveIncrementCase) {
	if tc.skip != nil && tc.skip() {
		t.Skip(tc.skipReason)
		return
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)
	if tc.setup != nil {
		tc.setup(env, r)
	}

	dbName := strings.ToLower("test_download_required_conn_" + tc.name)
	tableName := "data"
	fullBackup := dbName + "_full"
	incrBackup := dbName + "_incr"
	backups := []string{fullBackup, incrBackup}

	defer func() {
		fullCleanup(t, r, env, backups, []string{"remote", "local"}, []string{dbName}, false, false, false, tc.configFile)
	}()

	for _, backupName := range backups {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+tc.configFile+" delete remote "+backupName+" 2>/dev/null || true")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+tc.configFile+" delete local "+backupName+" 2>/dev/null || true")
	}
	r.NoError(env.dropDatabase(dbName, true))

	env.queryWithNoError(r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(r, "CREATE TABLE "+dbName+"."+tableName+" (id UInt64) ENGINE=MergeTree() ORDER BY id")
	// keep the increment's new rows in their own part so the increment has a non-required part
	// that the parent must download via b.dst after the recursive RequiredBackup download
	env.queryWithNoError(r, "SYSTEM STOP MERGES "+dbName+"."+tableName)
	env.queryWithNoError(r, "INSERT INTO "+dbName+"."+tableName+" SELECT number FROM numbers(100)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+tc.configFile, "create_remote", "--delete-source", "--tables="+dbName+".*", fullBackup)

	env.queryWithNoError(r, "INSERT INTO "+dbName+"."+tableName+" SELECT number+100 FROM numbers(100)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+tc.configFile, "create_remote", "--delete-source", "--diff-from-remote="+fullBackup, "--tables="+dbName+".*", incrBackup)

	// make sure nothing is cached locally so the download must pull both backups from remote
	for _, backupName := range backups {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+tc.configFile+" delete local "+backupName+" 2>/dev/null || true")
	}

	// fresh download of the increment: triggers the recursive RequiredBackup download that
	// corrupts b.dst on master for GCS/FTP/SFTP, then fails on the increment's own new parts.
	// DOWNLOAD_BY_PART=false is required: the recursion at download.go is gated by
	// !DownloadByPart, and DownloadByPart defaults to true, which otherwise resolves required
	// parts per-part without recursion and hides the bug.
	downloadOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("DOWNLOAD_BY_PART=false LOG_LEVEL=debug clickhouse-backup -c /etc/clickhouse-backup/%s download --resume %s 2>&1", tc.configFile, incrBackup))
	r.NoError(err, downloadOut)
	// guard against the test passing trivially: the recursive RequiredBackup download must run
	r.Contains(downloadOut, fullBackup, "recursive RequiredBackup download was not exercised")

	dropSQL := "DROP TABLE " + dbName + "." + tableName
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		dropSQL += " SYNC"
	}
	env.queryWithNoError(r, dropSQL)
	restoreOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s restore --rm --tables=%s.%s %s 2>&1", tc.configFile, dbName, tableName, incrBackup))
	r.NoError(err, restoreOut)
	env.checkCount(r, 1, 200, "SELECT count() FROM "+dbName+"."+tableName)

	fullCleanup(t, r, env, backups, []string{"remote", "local"}, []string{dbName}, false, false, true, tc.configFile)
}
