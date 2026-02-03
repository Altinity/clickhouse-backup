//go:build integration

package main

import (
	"fmt"
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
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	backupNames := make([]string, 5)
	for i := 0; i < 5; i++ {
		backupNames[i] = fmt.Sprintf("keep_remote_backup_%d", i)
	}
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameReplicated, dbNameMySQL, dbNamePostgreSQL, Issue331Issue1091Atomic, Issue331Issue1091Ordinary}
	fullCleanup(t, r, env, backupNames, []string{"remote", "local"}, databaseList, true, false, false, "config-s3.yml")
	incrementData := defaultIncrementData
	generateTestData(t, r, env, "S3", false, defaultTestData)
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
	env.checkObjectStorageIsEmpty(t, r, "S3")
	env.Cleanup(t, r)
}
