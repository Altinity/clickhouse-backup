//go:build integration

package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestWatchScheduleServerMetrics - reproduce https://github.com/Altinity/clickhouse-backup/issues/1502
// `server --watch --schedule` shall refresh clickhouse_backup_number_backups_remote after every scheduled backup
func TestWatchScheduleServerMetrics(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "curl")

	dbName := "test_watch_schedule_metrics"
	prefix := "sched1502"

	cleanBackups := func() {
		for _, location := range []string{"local", "remote"} {
			out, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list "+location+" 2>/dev/null | cut -d ' ' -f 1 | grep '^"+prefix+"-' || true")
			for _, backupName := range strings.Fields(out) {
				env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete --force "+location+" "+backupName+" 2>/dev/null || true")
			}
		}
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "rm -rf /var/lib/clickhouse/backup/"+prefix+"-* /hdd1_data/backup/"+prefix+"-* /hdd2_data/backup/"+prefix+"-* 2>/dev/null || true")
	}
	cleanBackups()
	r.NoError(env.dropDatabase(dbName, true))

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t1 (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t1 SELECT number FROM numbers(1000)")

	// mirror issue #1502: single full-only schedule, delete_previous_cycle=false, backups_to_keep_remote=7
	schedule := fmt.Sprintf("name=%s,full=*/30 * * * * *,delete_previous_cycle=false", prefix)
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce",
		"BACKUPS_TO_KEEP_REMOTE=7 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml server --watch --schedule \""+schedule+"\" &>>/tmp/watch_schedule_server.log")
	defer func() {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "pkill -f '[c]lickhouse-backup.*server' || true; for i in $(seq 1 30); do pgrep -f '[c]lickhouse-backup.*server' >/dev/null || break; sleep 1; done")
		out, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "cat /tmp/watch_schedule_server.log; rm -f /tmp/watch_schedule_server.log")
		if t.Failed() {
			t.Logf("watch server log:\n%s", out)
		}
		cleanBackups()
		r.NoError(env.dropDatabase(dbName, true))
	}()

	listRemote := func() []string {
		out, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list remote 2>/dev/null | cut -d ' ' -f 1 | grep -v '^$' || true")
		return strings.Fields(out)
	}
	matchedCount := func(names []string) int {
		count := 0
		for _, name := range names {
			if strings.HasPrefix(name, prefix+"-") {
				count++
			}
		}
		return count
	}

	// wait until at least 3 scheduled full backups uploaded
	deadline := time.Now().Add(4 * time.Minute)
	var remoteNames []string
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Second)
		remoteNames = listRemote()
		t.Logf("remote backups: %v", remoteNames)
		if matchedCount(remoteNames) >= 3 {
			break
		}
	}
	r.GreaterOrEqual(matchedCount(remoteNames), 3, "expect at least 3 scheduled full backups on remote")

	// metric shall match the real remote backup count, poll briefly to skip in-flight refresh races
	metricRE := regexp.MustCompile(`(?m)^clickhouse_backup_number_backups_remote (\d+)$`)
	metricValue := ""
	expected := ""
	metricsDeadline := time.Now().Add(1 * time.Minute)
	for time.Now().Before(metricsDeadline) {
		remoteNames = listRemote()
		expected = fmt.Sprintf("%d", len(remoteNames))
		out, err := env.DockerExecOut("clickhouse-backup", "curl", "-sSL", "http://localhost:7171/metrics")
		r.NoError(err)
		if m := metricRE.FindStringSubmatch(out); m != nil {
			metricValue = m[1]
		}
		t.Logf("clickhouse_backup_number_backups_remote=%s, list remote count=%s", metricValue, expected)
		if metricValue == expected {
			break
		}
		time.Sleep(5 * time.Second)
	}
	r.Equal(expected, metricValue, "clickhouse_backup_number_backups_remote shall match `list remote` count")
}
