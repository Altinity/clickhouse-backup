//go:build integration

package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

// TestServerWatchRBACConfigs - `server --watch --rbac --configs --named-collections` shall pass RBAC,
// configs and named collections flags into the watch goroutine, see https://github.com/Altinity/clickhouse-backup/issues/955
func TestServerWatchRBACConfigs(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.4") < 0 {
		t.Skipf("Test skipped, RBAC not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	withNamedCollections := compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.12") >= 0
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	prefix := "watch955"
	dbName := "test_watch955"
	cleanBackups := func() {
		for _, location := range []string{"local", "remote"} {
			out, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list "+location+" 2>/dev/null | cut -d ' ' -f 1 | grep '^"+prefix+"-' || true")
			for _, backupName := range strings.Fields(out) {
				env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete "+location+" "+backupName+" 2>/dev/null || true")
			}
		}
		// backups killed mid-flight may not show up in `list local`, remove leftovers from all backup disks
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "rm -rf /var/lib/clickhouse/backup/"+prefix+"-* /hdd1_data/backup/"+prefix+"-* /hdd2_data/backup/"+prefix+"-* 2>/dev/null || true")
	}
	cleanBackups()
	r.NoError(env.dropDatabase(dbName, true))

	// the pooled env has no user tables, and create_remote fails with `no tables for backup` without at least one
	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t1 (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t1 SELECT number FROM numbers(1000)")
	// at least one SQL-managed RBAC object, so rbac_size in the backup metadata is guaranteed > 0
	env.queryWithNoError(t, r, "CREATE USER IF NOT EXISTS test_watch_rbac_955 IDENTIFIED WITH no_password")
	watchFlags := "--rbac --configs"
	if withNamedCollections {
		env.queryWithNoError(t, r, "CREATE NAMED COLLECTION IF NOT EXISTS test_watch_nc_955 AS key1 = 'value1'")
		watchFlags += " --named-collections"
	}

	log.Debug().Msgf("Run `clickhouse-backup server --watch %s` in background", watchFlags)
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce",
		"BACKUPS_TO_KEEP_REMOTE=0 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml server --watch --watch-interval=30m --full-interval=24h --watch-backup-name-template="+prefix+"-{type}-{time:20060102150405} "+watchFlags+" &>>/tmp/watch_rbac_configs.log")
	defer func() {
		// [c]lickhouse regexp bracket trick, so pkill doesn't match its own `bash -ce` command line and kill itself with SIGTERM;
		// wait until the server process actually exits, a backup in flight during pkill can re-create local files after cleanup
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "pkill -f '[c]lickhouse-backup.*server' || true; for i in $(seq 1 30); do pgrep -f '[c]lickhouse-backup.*server' >/dev/null || break; sleep 1; done")
		out, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "cat /tmp/watch_rbac_configs.log; rm -f /tmp/watch_rbac_configs.log")
		log.Debug().Msg(out)
		cleanBackups()
		if dropErr := env.ch.Query("DROP USER IF EXISTS test_watch_rbac_955"); dropErr != nil {
			log.Warn().Msgf("DROP USER test_watch_rbac_955 error: %v", dropErr)
		}
		if withNamedCollections {
			if dropErr := env.ch.Query("DROP NAMED COLLECTION IF EXISTS test_watch_nc_955"); dropErr != nil {
				log.Warn().Msgf("DROP NAMED COLLECTION test_watch_nc_955 error: %v", dropErr)
			}
		}
		r.NoError(env.dropDatabase(dbName, true))
	}()

	// watch creates the first full backup right after server startup, wait until create_remote + delete local finishes
	backupName := ""
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		out, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list remote 2>/dev/null | cut -d ' ' -f 1 | grep '^"+prefix+"-full-' || true")
		if names := strings.Fields(out); len(names) > 0 {
			backupName = names[0]
			break
		}
		time.Sleep(5 * time.Second)
	}
	r.NotEmpty(backupName, "expect full backup created by `server --watch`")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", backupName)
	metadataStr, err := env.DockerExecOut("clickhouse-backup", "cat", "/var/lib/clickhouse/backup/"+backupName+"/metadata.json")
	r.NoError(err, "cat metadata.json: %s", metadataStr)
	var backupMetadata metadata.BackupMetadata
	r.NoError(json.Unmarshal([]byte(metadataStr), &backupMetadata))
	r.Greater(backupMetadata.RBACSize, uint64(0), "expect rbac_size > 0 in %s: %s", backupName, metadataStr)
	r.Greater(backupMetadata.ConfigSize, uint64(0), "expect config_size > 0 in %s: %s", backupName, metadataStr)
	if withNamedCollections {
		r.Greater(backupMetadata.NamedCollectionsSize, uint64(0), "expect named_collections_size > 0 in %s: %s", backupName, metadataStr)
	}
}
