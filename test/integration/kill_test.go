//go:build integration

package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// TestKill reproduces https://github.com/Altinity/clickhouse-backup/issues/1365.
// An `upload` action is started via the REST API and killed while in-progress; the
// .pid file must be removed by the kill handler so that subsequent operations
// (delete in particular) do not falsely report "another command is already running".
// Also verifies that /backup/kill blocks until the upload goroutine actually
// finished (sync wait), observable via the upload_finish metric.
func TestKill(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	env.InstallDebIfNotExists(r, "clickhouse-backup", "curl", "jq")
	defer env.Cleanup(t, r)

	const dbName = "kill_test_db"
	const backupName = "kill_test_backup"

	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, "CREATE DATABASE "+dbName)
	// Many partitions => many upload chunks => upload stays in-progress
	// long enough for the test to observe and kill it.
	env.queryWithNoError(r, fmt.Sprintf(
		"CREATE TABLE %s.t1 (id UInt64, s String) ENGINE=MergeTree() PARTITION BY (id %% 64) ORDER BY id",
		dbName))
	payload := strings.Repeat("x", 2048)
	env.queryWithNoError(r, fmt.Sprintf(
		"INSERT INTO %s.t1 SELECT number, '%s' FROM numbers(800000)", dbName, payload))

	log.Debug().Msg("start clickhouse-backup server with UPLOAD_CONCURRENCY=1 in background")
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce",
		"UPLOAD_CONCURRENCY=1 clickhouse-backup server &>>/tmp/clickhouse-backup-server.log")
	defer func() {
		_ = env.DockerExec("clickhouse-backup", "pkill", "-n", "-f", "clickhouse-backup")
	}()
	defer func() {
		if out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "delete", "remote", backupName); err != nil {
			log.Warn().Err(err).Msgf("TestKill teardown: delete remote %s: %s", backupName, out)
		}
		if err := env.dropDatabase(dbName, true); err != nil {
			log.Warn().Err(err).Msgf("TestKill teardown: drop database %s", dbName)
		}
	}()
	time.Sleep(3 * time.Second)

	pidPath := fmt.Sprintf("/tmp/clickhouse-backup.%s.pid", backupName)

	// 1. Create the local backup, wait for completion.
	createOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/create?table=%s.*&name=%s'", dbName, backupName))
	r.NoError(err, "%s\nunexpected POST /backup/create error: %v", createOut, err)
	r.NotContains(createOut, "\"status\":\"error\"")
	waitForActionStatus(r, env, "create", backupName, "success", 60*time.Second)

	// 2. Kick off upload (async).
	uploadOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/upload/%s'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/upload error: %v", uploadOut, err)
	r.Contains(uploadOut, "acknowledged")

	// 3. Wait until upload is observably in-progress AND pid file exists.
	deadline := time.Now().Add(15 * time.Second)
	pidSeen := false
	for time.Now().Before(deadline) {
		statusOut, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
			"curl -sfL 'http://localhost:7171/backup/actions?filter=upload'")
		lsOut, lsErr := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "ls "+pidPath+" 2>/dev/null || true")
		if strings.Contains(statusOut, `"status":"in progress"`) && lsErr == nil && strings.Contains(lsOut, backupName) {
			pidSeen = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	r.True(pidSeen, "expected to observe upload in-progress with pid file %s present", pidPath)

	// Snapshot upload finish metric before kill — used below to verify kill
	// blocked until the upload goroutine finished (sync-wait behavior).
	finishBefore := readUploadFinishMetric(r, env)

	// 4. Kill the in-progress upload. Time it — sync wait should make this
	// observably longer than a trivial round-trip.
	killStart := time.Now()
	killOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		fmt.Sprintf("curl -sfL 'http://localhost:7171/backup/kill?command=upload+%s'", backupName))
	killElapsed := time.Since(killStart)
	r.NoError(err, "%s\nunexpected GET /backup/kill error: %v", killOut, err)
	r.Contains(killOut, "\"status\":\"success\"", "kill should succeed: %s", killOut)
	log.Info().Msgf("kill returned in %s", killElapsed)

	// Sync-wait check: after kill returned, the upload metric must reflect
	// that the goroutine ran to completion (m.Finish in ExecuteWithMetrics
	// runs AFTER cliApp.Run returns, BEFORE Stop closes Done). Without
	// sync wait, kill would return earlier and the metric would still hold
	// its previous value at this point.
	finishAfter := readUploadFinishMetric(r, env)
	r.Greater(finishAfter, finishBefore,
		"clickhouse_backup_last_upload_finish must advance during kill (before=%d after=%d); "+
			"sync wait did not block until the upload goroutine returned",
		finishBefore, finishAfter)

	// 5. Pid file must be gone immediately after kill — this is the regression check.
	checkOut, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"if [ -f "+pidPath+" ]; then echo EXISTS; cat "+pidPath+"; else echo GONE; fi")
	r.Contains(checkOut, "GONE",
		"pid file %s must be removed by kill, got: %s", pidPath, checkOut)

	// 6. Delete must NOT trip on a stale pid file.
	deleteOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/local/%s'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/delete error: %v", deleteOut, err)
	r.NotContains(deleteOut, "another clickhouse-backup",
		"delete must not see a stale pid lock: %s", deleteOut)
	r.NotContains(deleteOut, "\"status\":\"error\"", "delete must succeed: %s", deleteOut)

	// Remote backup, database, and env-pool return are handled by the defers
	// registered above so they run on both success and mid-test failure.
}

// readUploadFinishMetric scrapes /metrics and parses the value of
// clickhouse_backup_last_upload_finish (a unix-timestamp gauge updated by
// metrics.ExecuteWithMetrics when the upload goroutine returns).
func readUploadFinishMetric(r *require.Assertions, env *TestEnvironment) int64 {
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"curl -sfL http://localhost:7171/metrics | grep -E '^clickhouse_backup_last_upload_finish '")
	r.NoError(err, "/metrics scrape failed: %s", out)
	// Format: `clickhouse_backup_last_upload_finish <float>`
	matches := regexp.MustCompile(`clickhouse_backup_last_upload_finish\s+([0-9.eE+\-]+)`).FindStringSubmatch(out)
	r.Len(matches, 2, "could not parse upload_finish metric: %q", out)
	v, err := strconv.ParseFloat(strings.TrimSpace(matches[1]), 64)
	r.NoError(err, "parse %q", matches[1])
	return int64(v)
}

// waitForActionStatus polls /backup/actions and returns once a row whose
// command starts with cmdPrefix and contains nameNeedle is observed with
// the expected status.
func waitForActionStatus(r *require.Assertions, env *TestEnvironment, cmdPrefix, nameNeedle, expected string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			r.FailNow(fmt.Sprintf("timeout waiting for %s ... %s to reach status %q", cmdPrefix, nameNeedle, expected))
		}
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
			"curl -sfL 'http://localhost:7171/backup/actions'")
		r.NoError(err)
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, `"command":"`+cmdPrefix) &&
				strings.Contains(line, nameNeedle) &&
				strings.Contains(line, `"status":"`+expected+`"`) {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
}
