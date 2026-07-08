//go:build integration

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// https://github.com/Altinity/clickhouse-backup/issues/871
func TestKeepBackupRemoteAndDiffFromRemote(t *testing.T) {
	if isTestShouldSkip("RUN_ADVANCED_TESTS") {
		t.Skip("Skipping Advanced integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	backupNames := make([]string, 5)
	for i := 0; i < 5; i++ {
		backupNames[i] = fmt.Sprintf("keep_remote_backup_%d", i)
	}
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameReplicated, dbNameMySQL, dbNamePostgreSQL, Issue331Issue1091Atomic, Issue331Issue1091Ordinary}
	fullCleanup(t, r, env, backupNames, []string{"remote", "local"}, databaseList, true, false, false, "config-s3.yml")
	incrementData := defaultIncrementData()
	generateTestData(t, r, env, "S3", false, defaultTestData())
	for backupNumber, backupName := range backupNames {
		if backupNumber == 0 {
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create_remote %s", backupName))
		} else if backupNumber == 3 {
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create_remote --diff-from-remote=%s %s", backupNames[backupNumber-1], backupName))
		} else {
			incrementData = generateIncrementTestData(t, r, env, "S3", false, incrementData, backupNumber)
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create_remote --diff-from-remote=%s %s", backupNames[backupNumber-1], backupName))
		}
	}
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list local")
	r.NoError(err, "%s\nunexpected list local error: %v", out, err)
	for _, backupName := range backupNames {
		r.Contains(out, backupName)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	}
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list remote")
	r.NoError(err, "%s\nunexpected list remote error: %v", out, err)
	// shall not delete any backup on remote, cause all deleted backups have links as required in other backups
	for _, backupName := range backupNames {
		r.Regexp("(?m)^"+backupName, out)
	}

	latestIncrementBackup := fmt.Sprintf("keep_remote_backup_%d", len(backupNames)-1)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", latestIncrementBackup)
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list local")
	r.NoError(err, "%s\nunexpected list local error: %v", out, err)
	prevIncrementBackup := fmt.Sprintf("keep_remote_backup_%d", len(backupNames)-2)
	for _, backupName := range backupNames {
		if backupName == latestIncrementBackup {
			r.Regexp("(?m)^"+backupName, out)
			r.NotContains(out, "+"+backupName)
		} else if backupName == prevIncrementBackup {
			r.NotRegexp("(?m)^"+backupName, out)
			r.Contains(out, "+"+backupName)
		} else {
			r.NotContains(out, backupName)
		}
	}
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", latestIncrementBackup)
	var res uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&res, fmt.Sprintf("SELECT count() FROM `%s_%s`.`%s_%s`", Issue331Issue1091Atomic, t.Name(), Issue331Issue1091Atomic, t.Name())))
	numBackupsWithData := 3
	r.Equal(uint64(100+20*numBackupsWithData), res)
	fullCleanup(t, r, env, []string{latestIncrementBackup}, []string{"local"}, nil, false, true, true, "config-s3.yml")
	fullCleanup(t, r, env, backupNames, []string{"remote"}, databaseList, true, true, true, "config-s3.yml")
	env.checkObjectStorageIsEmpty(t, r, "S3", "config-s3.yml")
}

// TestKeepBackupRemoteWithRebase - `rebase_before_remove_old_remote: true` makes `backups_to_keep_remote`
// a strict limit: the oldest kept increment referencing an out-of-window backup is rebased (becomes full),
// so the out-of-window chain loses `required_backup` links and gets deleted, in contrast with
// TestKeepBackupRemoteAndDiffFromRemote where the whole chain stays protected
func TestKeepBackupRemoteWithRebase(t *testing.T) {
	if isTestShouldSkip("RUN_ADVANCED_TESTS") {
		t.Skip("Skipping Advanced integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	backupNames := make([]string, 5)
	for i := 0; i < 5; i++ {
		backupNames[i] = fmt.Sprintf("rebase_keep_remote_backup_%d", i)
	}
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameReplicated, dbNameMySQL, dbNamePostgreSQL, Issue331Issue1091Atomic, Issue331Issue1091Ordinary}
	fullCleanup(t, r, env, backupNames, []string{"remote", "local"}, databaseList, true, false, false, "config-s3.yml")
	incrementData := defaultIncrementData()
	generateTestData(t, r, env, "S3", false, defaultTestData())
	createEnv := "BACKUPS_TO_KEEP_REMOTE=3 REBASE_BEFORE_REMOVE_OLD_REMOTE=true CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml"
	for backupNumber, backupName := range backupNames {
		var createCmd string
		if backupNumber == 0 {
			createCmd = fmt.Sprintf("%s clickhouse-backup create_remote %s", createEnv, backupName)
		} else if backupNumber == 3 {
			createCmd = fmt.Sprintf("%s clickhouse-backup create_remote --diff-from-remote=%s %s", createEnv, backupNames[backupNumber-1], backupName)
		} else {
			incrementData = generateIncrementTestData(t, r, env, "S3", false, incrementData, backupNumber)
			createCmd = fmt.Sprintf("%s clickhouse-backup create_remote --diff-from-remote=%s %s", createEnv, backupNames[backupNumber-1], backupName)
		}
		out, createErr := env.DockerExecOut("clickhouse-backup", "bash", "-ce", createCmd)
		r.NoError(createErr, "%s\nunexpected create_remote error: %v", out, createErr)
		if strings.Contains(out, "can't rebase") {
			for _, line := range strings.Split(out, "\n") {
				if strings.Contains(line, "can't rebase") {
					t.Log(line)
				}
			}
			t.Fatalf("unexpected `can't rebase` warning during create_remote %s", backupName)
		}
		// backups 0..2 fill the keep window, starting from backup 3 every upload must rebase the oldest kept increment
		if backupNumber >= 3 && !strings.Contains(out, "rebase to allow delete backups outside backups_to_keep_remote") {
			t.Fatalf("expected rebase during create_remote %s", backupName)
		}
		// remote retention runs during upload while the same-name local backup still exists, delete local right away
		// so cleanEmbeddedAndObjectDiskRemoteIfSameLocalNotPresent can fully clean object disks of deleted remote backups
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	}
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list remote")
	r.NoError(err, "%s\nunexpected list remote error: %v", out, err)
	// backups_to_keep_remote is a strict limit: 0 and 1 rebased away and deleted, only the last 3 stay
	for _, backupName := range backupNames[:2] {
		r.NotContains(out, backupName)
	}
	for _, backupName := range backupNames[2:] {
		r.Regexp("(?m)^"+backupName, out)
	}

	// the kept chain must stay restorable after its out-of-window ancestors are deleted
	latestIncrementBackup := backupNames[len(backupNames)-1]
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", latestIncrementBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", latestIncrementBackup)
	var res uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&res, fmt.Sprintf("SELECT count() FROM `%s_%s`.`%s_%s`", Issue331Issue1091Atomic, t.Name(), Issue331Issue1091Atomic, t.Name())))
	numBackupsWithData := 3
	r.Equal(uint64(100+20*numBackupsWithData), res)
	// backups 0 and 1 are already deleted by retention; download of the latest increment also fetched
	// its required ancestors locally, they must be deleted before the remote cleanup, otherwise
	// cleanEmbeddedAndObjectDiskRemoteIfSameLocalNotPresent keeps object disk blobs of the rebased backups
	// (checkDeleteErr=false: only the latest increment and its immediate required backup exist locally)
	fullCleanup(t, r, env, backupNames[2:], []string{"local"}, nil, false, false, false, "config-s3.yml")
	fullCleanup(t, r, env, backupNames[2:], []string{"remote"}, databaseList, true, true, true, "config-s3.yml")
	env.checkObjectStorageIsEmpty(t, r, "S3", "config-s3.yml")
}
