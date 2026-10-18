//go:build integration

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestWatchEnvOverride - `watch` reloads config on every iteration, the reload shall keep --env overrides,
// otherwise backups land in the config file remote path instead of the --env one
func TestWatchEnvOverride(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	dbName := "test_watch_env_override"
	prefix := "envwatch"
	// keep {version} macro, it isolates concurrent CI runs in the shared bucket
	envArgs := "--env S3_PATH=backup_watch_env/{cluster}/{shard}/{version}"

	listMatched := func(args string) []string {
		out, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml "+args+" list remote 2>/dev/null | cut -d ' ' -f 1 | grep '^"+prefix+"-' || true")
		return strings.Fields(out)
	}
	cleanBackups := func() {
		for _, args := range []string{envArgs, ""} {
			for _, backupName := range listMatched(args) {
				env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml "+args+" delete --force remote "+backupName+" 2>/dev/null || true")
			}
		}
		// backups killed mid-flight by timeout may not show up in `list local`, remove leftovers from all backup disks
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "rm -rf /var/lib/clickhouse/backup/"+prefix+"-* /hdd1_data/backup/"+prefix+"-* /hdd2_data/backup/"+prefix+"-* 2>/dev/null || true")
	}
	cleanBackups()
	r.NoError(env.dropDatabase(dbName, true))
	defer func() {
		cleanBackups()
		r.NoError(env.dropDatabase(dbName, true))
	}()

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t1 (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t1 SELECT number FROM numbers(1000)")

	// timeout exits with 124 after killing watch, ignore it, the assertions below check what watch uploaded
	out, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"timeout 45 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml "+envArgs+" watch --watch-interval=10s --full-interval=1h --watch-backup-name-template="+prefix+"-{type}-{time:20060102150405} --tables="+dbName+".* 2>&1 || true")
	log.Debug().Msg(out)

	r.GreaterOrEqual(len(listMatched(envArgs)), 2, "every watch iteration shall upload into S3_PATH from --env\n%s", out)
	r.Empty(listMatched(""), "watch shall not upload into s3->path from the config file when --env S3_PATH is passed")
}
