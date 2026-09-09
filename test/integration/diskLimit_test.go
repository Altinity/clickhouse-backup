//go:build integration

package main

import (
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// https://github.com/Altinity/clickhouse-backup/issues/1458
func TestDownloadDiskLimit(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	backupName := "test_disk_limit"
	dbName := "test_disk_limit_db_" + t.Name()
	cfg := "/etc/clickhouse-backup/config-s3.yml"

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t SELECT number FROM numbers(1000)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "create_remote", "--tables="+dbName+".*", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "local", backupName)

	// any real disk is used more than 1%, the download shall be refused before any data is fetched
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", cfg, "download", "--disk-limit=1", backupName)
	log.Debug().Msg(out)
	r.Error(err)
	r.Contains(out, "exceeds --disk-limit=1%")
	// the refused download shall leave nothing behind, otherwise the next run resumes and skips the check
	out, err = env.DockerExecOut("clickhouse-backup", "ls", "/var/lib/clickhouse/backup/"+backupName)
	r.Error(err, "local backup shall be removed after refusal, got: %s", out)

	// the same limit from config, CLI flag absent
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "DOWNLOAD_DISK_LIMIT=1 clickhouse-backup -c "+cfg+" download "+backupName)
	log.Debug().Msg(out)
	r.Error(err)
	r.Contains(out, "exceeds --disk-limit=1%")
	// CLI flag overrides config
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "DOWNLOAD_DISK_LIMIT=1 clickhouse-backup -c "+cfg+" download --disk-limit=100 "+backupName)
	r.NoError(err, out)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "local", backupName)
	// config validation rejects out of range value
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "DOWNLOAD_DISK_LIMIT=101 clickhouse-backup -c "+cfg+" download "+backupName)
	r.Error(err)
	r.Contains(out, "download_disk_limit=101 is invalid")
	// out of range value is rejected
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", cfg, "download", "--disk-limit=101", backupName)
	r.Error(err)
	r.Contains(out, "--disk-limit shall be between 1 and 100")

	// 100% never refuses, same as disabled
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore_remote", "--rm", "--disk-limit=100", backupName)
	env.checkCount(r, 1, 1000, "SELECT count() FROM "+dbName+".t")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "local", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "remote", backupName)
	env.queryWithNoError(t, r, "DROP DATABASE "+dbName+" SYNC")
}
