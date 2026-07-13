//go:build integration

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestWatchSchedule - cron driven watch mode, see https://github.com/Altinity/clickhouse-backup/issues/1354
// one schedule covers the whole feature set: first increment tick promotes to full (empty chain),
// increments chain via --diff-from-remote, full_type=rebase creates the next full as increment + rebase,
// delete_previous_cycle removes the previous chain after a successful full
func TestWatchSchedule(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	dbName := "test_watch_schedule"
	prefix := "sched1354"

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

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t1 (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t1 SELECT number FROM numbers(1000)")

	// full at second 50 once per minute, increment at seconds 0/15/30/45 - `*/45` in the seconds field means {0,45}
	// and would always coincide with increment ticks where full wins, so increments would never run
	schedule := fmt.Sprintf("name=%s,full=50 * * * * *,increment=*/15 * * * * *,full_type=rebase,delete_previous_cycle=true", prefix)
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce",
		"BACKUPS_TO_KEEP_REMOTE=0 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml watch --tables="+dbName+".* --schedule \""+schedule+"\" &>>/tmp/watch_schedule.log")
	defer func() {
		// [c]lickhouse regexp bracket trick, so pkill doesn't match its own `bash -ce` command line and kill itself with SIGTERM;
		// wait until the watch process actually exits, a backup in flight during pkill can re-create local files after cleanup
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "pkill -f '[c]lickhouse-backup.*watch' || true; for i in $(seq 1 30); do pgrep -f '[c]lickhouse-backup.*watch' >/dev/null || break; sleep 1; done")
		out, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "cat /tmp/watch_schedule.log; rm -f /tmp/watch_schedule.log")
		log.Debug().Msg(out)
		cleanBackups()
		leftovers, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "ls -d /var/lib/clickhouse/backup/"+prefix+"-* /hdd1_data/backup/"+prefix+"-* /hdd2_data/backup/"+prefix+"-* 2>/dev/null || true")
		r.Empty(strings.TrimSpace(leftovers), "local backup leftovers shall not leak into the pooled env: %s", leftovers)
		r.NoError(env.dropDatabase(dbName, true))
	}()

	listMatched := func() []string {
		out, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list remote 2>/dev/null | cut -d ' ' -f 1 | grep '^"+prefix+"-' || true")
		return strings.Fields(out)
	}
	isFull := func(backupName string) bool {
		return strings.Contains(backupName, "-full-")
	}

	// wait until the second full backup replaces the first chain:
	// delete_previous_cycle shall leave exactly one full backup, and increments shall appear between fulls
	observedFulls := map[string]bool{}
	observedIncrement := false
	deletePreviousCycleApplied := false
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Second)
		matched := listMatched()
		fulls := make([]string, 0)
		for _, backupName := range matched {
			if isFull(backupName) {
				fulls = append(fulls, backupName)
				observedFulls[backupName] = true
			} else {
				observedIncrement = true
			}
		}
		log.Debug().Msgf("observedFulls=%v, matched=%v", observedFulls, matched)
		if observedIncrement && len(observedFulls) >= 2 && len(fulls) == 1 {
			deletePreviousCycleApplied = true
			break
		}
	}
	r.GreaterOrEqual(len(observedFulls), 2, "expect at least two full backups created by cron schedule")
	r.True(observedIncrement, "expect at least one increment backup created by cron schedule")
	r.True(deletePreviousCycleApplied, "expect delete_previous_cycle to leave exactly one full backup after a new full")
}
