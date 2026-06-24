//go:build integration

package main

import (
	"fmt"
	"os"
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
		if out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "delete", "remote", backupName); err != nil && !strings.Contains(out, fmt.Sprintf("'%s' is not found on remote storage", backupName)) {
			t.Errorf("TestKill teardown error=%+v: delete remote %s: %s", err, backupName, out)
		}
		// The killed mid-flight upload leaves an incomplete remote backup that
		// `delete remote` reports as "not found" and never removes. Purge the
		// residue directly so it does not pollute the next test that reuses this
		// env from the pool (observed breaking TestS3NoDeletePermission, which
		// asserts the remote backup path is empty).
		_ = env.DockerExec("minio", "rm", "-rf", env.minioBackupFSPath(r, "config-s3.yml", backupName))
		if err := env.dropDatabase(dbName, true); err != nil {
			t.Errorf("TestKill teardown: drop database %s, error=%+v", dbName, err)
		}
	}()
	time.Sleep(3 * time.Second)

	pidPath := fmt.Sprintf("/tmp/clickhouse-backup.%s.pid", backupName)

	// 1. Create the local backup, wait for completion.
	createOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		execCurlWithFailBody(fmt.Sprintf("-XPOST 'http://127.0.0.1:7171/backup/create?table=%s.*&name=%s'", dbName, backupName)))
	r.NoError(err, "%s\nunexpected POST /backup/create error: %v", createOut, err)
	r.NotContains(createOut, "\"status\":\"error\"")
	waitForActionStatus(r, env, "create", backupName, "success", 60*time.Second)

	// 2. Kick off upload (async).
	uploadOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		execCurlWithFailBody(fmt.Sprintf("-XPOST 'http://127.0.0.1:7171/backup/upload/%s'", backupName)))
	r.NoError(err, "%s\nunexpected POST /backup/upload error: %v", uploadOut, err)
	r.Contains(uploadOut, "acknowledged")

	// 3. Wait until upload is observably in-progress AND pid file exists.
	deadline := time.Now().Add(15 * time.Second)
	pidSeen := false
	for time.Now().Before(deadline) {
		statusOut, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
			execCurlWithFailBody("'http://127.0.0.1:7171/backup/actions?filter=upload'"))
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
		execCurlWithFailBody(fmt.Sprintf("'http://127.0.0.1:7171/backup/kill?command=upload+%s'", backupName)))
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
		execCurlWithFailBody(fmt.Sprintf("-XPOST 'http://127.0.0.1:7171/backup/delete/local/%s'", backupName)))
	r.NoError(err, "%s\nunexpected POST /backup/delete error: %v", deleteOut, err)
	r.NotContains(deleteOut, "another clickhouse-backup",
		"delete must not see a stale pid lock: %s", deleteOut)
	r.NotContains(deleteOut, "\"status\":\"error\"", "delete must succeed: %s", deleteOut)

	// Remote backup, database, and env-pool return are handled by the defers
	// registered above so they run on both success and mid-test failure.
}

// TestKillDownload kills an in-progress streaming download and verifies the
// download goroutine actually stops. Reproduces the class of bug seen in
// production (https://github.com/Altinity/clickhouse-backup/issues/1365 follow
// up): with allow_multipart_download=false + download_by_part=true the data is
// streamed S3 -> nio pipe -> tar.Extract, and a worker stuck in a read that
// ignores context cancellation made /backup/kill block the full
// cancel_operation_timeout (default 1800s) while the download kept running.
// A short API_CANCEL_OPERATION_TIMEOUT makes that failure mode fail fast here.
func TestKillDownload(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	env.InstallDebIfNotExists(r, "clickhouse-backup", "curl", "jq")
	defer env.Cleanup(t, r)

	const dbName = "kill_download_db"
	const backupName = "kill_download_backup"

	killSetupTable(r, env, dbName)

	log.Debug().Msg("start clickhouse-backup server for TestKillDownload")
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce",
		"S3_ALLOW_MULTIPART_DOWNLOAD=false DOWNLOAD_CONCURRENCY=1 API_CANCEL_OPERATION_TIMEOUT=15s "+
			"clickhouse-backup server &>>/tmp/clickhouse-backup-server.log")
	defer func() {
		_ = env.DockerExec("clickhouse-backup", "pkill", "-n", "-f", "clickhouse-backup")
	}()
	defer func() {
		_ = env.DockerExec("clickhouse-backup", "clickhouse-backup", "delete", "local", backupName)
		if out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "delete", "remote", backupName); err != nil && !strings.Contains(out, fmt.Sprintf("'%s' is not found on remote storage", backupName)) {
			t.Errorf("TestKillDownload teardown error=%+v: delete remote %s: %s", err, backupName, out)
		}
		_ = env.DockerExec("minio", "rm", "-rf", env.minioBackupFSPath(r, "config-s3.yml", backupName))
		if err := env.dropDatabase(dbName, true); err != nil {
			t.Errorf("TestKillDownload teardown: drop database %s, error=%+v", dbName, err)
		}
	}()
	time.Sleep(3 * time.Second)

	// 1. create local backup, push it remote, then drop local so download works.
	runActionWait(r, env, fmt.Sprintf("create --tables=%s.* %s", dbName, backupName), "create", backupName, 60*time.Second)
	runActionWait(r, env, "upload "+backupName, "upload", backupName, 120*time.Second)
	delOut := postAction(r, env, "delete local "+backupName)
	r.Contains(delOut, "\"status\":\"success\"", "delete local must succeed: %s", delOut)

	// 2. start download and kill it mid-flight (start happens inside observeInProgressAndKill).
	observeInProgressAndKill(r, env, "download "+backupName, backupName, "download", 15*time.Second)

	// 3. a follow-up delete must not trip on a stale pid lock.
	delOut = postAction(r, env, "delete local "+backupName)
	r.NotContains(delOut, "another clickhouse-backup", "delete must not see a stale pid lock: %s", delOut)
}

// TestKillCreate kills an in-progress create and verifies the creation goroutine
// stops (pid removed, last_create_finish advances, kill returns fast).
func TestKillCreate(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	env.InstallDebIfNotExists(r, "clickhouse-backup", "curl", "jq")
	defer env.Cleanup(t, r)

	const dbName = "kill_create_db"
	const backupName = "kill_create_backup"

	killSetupTable(r, env, dbName)

	log.Debug().Msg("start clickhouse-backup server for TestKillCreate")
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce",
		"API_CANCEL_OPERATION_TIMEOUT=15s clickhouse-backup server &>>/tmp/clickhouse-backup-server.log")
	defer func() {
		_ = env.DockerExec("clickhouse-backup", "pkill", "-n", "-f", "clickhouse-backup")
	}()
	defer func() {
		_ = env.DockerExec("clickhouse-backup", "clickhouse-backup", "delete", "local", backupName)
		if err := env.dropDatabase(dbName, true); err != nil {
			t.Errorf("TestKillCreate teardown: drop database %s, error=%+v", dbName, err)
		}
	}()
	time.Sleep(3 * time.Second)

	// start happens inside observeInProgressAndKill so a fast create cannot
	// finish before the kill is issued.
	observeInProgressAndKill(r, env, fmt.Sprintf("create --tables=%s.* %s", dbName, backupName), backupName, "create", 15*time.Second)

	// a follow-up delete must not trip on a stale pid lock.
	delOut := postAction(r, env, "delete local "+backupName)
	r.NotContains(delOut, "another clickhouse-backup", "delete must not see a stale pid lock: %s", delOut)
}

// TestKillRestore kills an in-progress restore and verifies the restore
// goroutine stops (pid removed, last_restore_finish advances, kill returns fast).
func TestKillRestore(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	env.InstallDebIfNotExists(r, "clickhouse-backup", "curl", "jq")
	defer env.Cleanup(t, r)

	const dbName = "kill_restore_db"
	const backupName = "kill_restore_backup"

	killSetupTable(r, env, dbName)

	log.Debug().Msg("start clickhouse-backup server for TestKillRestore")
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce",
		"API_CANCEL_OPERATION_TIMEOUT=15s clickhouse-backup server &>>/tmp/clickhouse-backup-server.log")
	defer func() {
		_ = env.DockerExec("clickhouse-backup", "pkill", "-n", "-f", "clickhouse-backup")
	}()
	defer func() {
		_ = env.DockerExec("clickhouse-backup", "clickhouse-backup", "delete", "local", backupName)
		if err := env.dropDatabase(dbName, true); err != nil {
			t.Errorf("TestKillRestore teardown: drop database %s, error=%+v", dbName, err)
		}
	}()
	time.Sleep(3 * time.Second)

	// create a local backup, drop the table so restore has to recreate+attach.
	runActionWait(r, env, fmt.Sprintf("create --tables=%s.* %s", dbName, backupName), "create", backupName, 60*time.Second)
	// SYNC keyword not supported before 21.x
	dropSQL := fmt.Sprintf("DROP TABLE %s.t1", dbName)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		dropSQL += " SYNC"
	}
	env.queryWithNoError(r, dropSQL)

	// start happens inside observeInProgressAndKill so a fast restore cannot
	// finish before the kill is issued.
	observeInProgressAndKill(r, env, "restore "+backupName, backupName, "restore", 15*time.Second)
}

// readUploadFinishMetric scrapes /metrics and parses the value of
// clickhouse_backup_last_upload_finish (a unix-timestamp gauge updated by
// metrics.ExecuteWithMetrics when the upload goroutine returns).
func readUploadFinishMetric(r *require.Assertions, env *TestEnvironment) int64 {
	return readActionFinishMetric(r, env, "upload")
}

// readActionFinishMetric scrapes /metrics and parses the value of
// clickhouse_backup_last_<command>_finish (a unix-timestamp gauge updated by
// metrics.ExecuteWithMetrics when the command goroutine returns).
func readActionFinishMetric(r *require.Assertions, env *TestEnvironment, command string) int64 {
	metric := "clickhouse_backup_last_" + command + "_finish"
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"curl -sSL http://127.0.0.1:7171/metrics | grep -E '^"+metric+" '")
	r.NoError(err, "/metrics scrape failed: %s", out)
	matches := regexp.MustCompile(metric + `\s+([0-9.eE+\-]+)`).FindStringSubmatch(out)
	r.Len(matches, 2, "could not parse %s metric: %q", metric, out)
	v, err := strconv.ParseFloat(strings.TrimSpace(matches[1]), 64)
	r.NoError(err, "parse %q", matches[1])
	return int64(v)
}

// execCurlWithFailBody builds a shell command that runs curl, prints the
// response body, and exits non-zero on failure — so callers' r.NoError(err)
// trips on errors while the body stays visible for diagnosis instead of just an
// exit code. curl runs inside the ClickHouse server container, whose curl
// version tracks CLICKHOUSE_VERSION:
//
//   - CLICKHOUSE_VERSION >= 25.3 (image base moved to Ubuntu 22.04, curl 7.81)
//     ships `curl --fail-with-body`, so use it directly.
//   - older images (Ubuntu <= 20.04, curl <= 7.68) lack --fail-with-body, so
//     emulate it: capture %{http_code} and exit 22 (curl's
//     CURLE_HTTP_RETURNED_ERROR) on HTTP >= 400. curl's own exit code (e.g. 7
//     connection-refused, 28 timeout) is preserved for transport errors.
//
// args is everything after `curl` (flags, -d data, and the quoted URL).
func execCurlWithFailBody(args string) string {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.3") >= 0 {
		return "curl -sSL --fail-with-body " + args
	}
	return `rc=0; resp=$(curl -sSL -w '\n%{http_code}' ` + args + `) || rc=$?; ` +
		`code="${resp##*$'\n'}"; printf '%s' "${resp%$'\n'*}"; ` +
		`if [ "$rc" -ne 0 ]; then exit "$rc"; fi; ` +
		`if [ "${code:-000}" -ge 400 ]; then exit 22; fi`
}

// postAction POSTs a single command to /backup/actions and returns the raw
// response body. The command value is JSON-encoded via %q; since no command
// used by these tests contains a single quote, the JSON is safely wrapped in
// shell single quotes.
func postAction(r *require.Assertions, env *TestEnvironment, command string) string {
	body := fmt.Sprintf(`{"command":%q}`, command)
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		execCurlWithFailBody("-XPOST 'http://127.0.0.1:7171/backup/actions' -d '"+body+"'"))
	r.NoError(err, "%s\nPOST /backup/actions %q error: %v", out, command, err)
	return out
}

// runActionWait starts an async action and blocks until it reports success.
func runActionWait(r *require.Assertions, env *TestEnvironment, command, cmdPrefix, nameNeedle string, timeout time.Duration) {
	out := postAction(r, env, command)
	r.Contains(out, "acknowledged", "%q expected acknowledged: %s", command, out)
	waitForActionStatus(r, env, cmdPrefix, nameNeedle, "success", timeout)
}

// killSetupTable (re)creates a table of 100 partitions, ~100KB each. The
// archive is stored uncompressed (compression_format: tar), so a fixed 1KiB
// payload generated on the Go side keeps the tar archive large enough that
// create/upload/download/restore remain observably in-progress long enough to
// be killed mid-flight.
func killSetupTable(r *require.Assertions, env *TestEnvironment, dbName string) {
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(r, fmt.Sprintf(
		"CREATE TABLE %s.t1 (id UInt64, s String) ENGINE=MergeTree() PARTITION BY (id %% 100) ORDER BY id",
		dbName))
	// 10000 rows / 100 partitions = 100 rows per partition * 1KiB ≈ 100KiB each.
	payload := strings.Repeat("x", 1024)
	env.queryWithNoError(r, fmt.Sprintf(
		"INSERT INTO %s.t1 SELECT number, '%s' FROM numbers(10000)", dbName, payload))
}

// observeInProgressKillScript is a bash program run inside the clickhouse-backup
// container that STARTS `command`, waits until it is in-progress (its pid file
// exists), then fires the kill and captures the proof of cancellation — all
// without leaving the container.
//
// Keeping curl off the critical path is essential. A fast action (~230ms create
// on old ClickHouse) is comparable to how long a single curl takes to even spawn
// when the old amd64-only ClickHouse image runs under QEMU emulation (process
// startup ~100ms+, measured). Polling /backup/actions with curl to detect
// "in progress" would therefore consume the whole window before the kill is sent
// (the kill then reports "command not found"). Instead:
//   - the before-metric is read BEFORE the start, off the critical path;
//   - "in progress" is detected with a pure-bash wait on the pid file (no
//     process spawn) — pidlock creates the file for the whole duration of
//     create/download/restore and removes it when the worker returns, so its
//     presence is an exact in-progress proxy;
//   - only the start and the kill spawn curl, so the kill is issued ~one curl
//     spawn after the pid appears, well inside the in-progress window.
//
// This is reliable on every ClickHouse version and architecture regardless of
// how quickly the worker runs.
//
// Placeholders are substituted in Go (none contain a single quote, so
// single-quote shell wrapping is safe). awk extracts the metric so a missing
// line yields "" instead of a non-zero exit that bash -e would abort on. SECONDS
// is a bash builtin (no spawn), so the wait loop adds no latency to detection.
const observeInProgressKillScript = `
pid="__PID__"
metric="__METRIC__"
before=$(curl -sSL 'http://127.0.0.1:7171/metrics' 2>/dev/null | awk -v m="$metric" '$1==m {print $2}')
echo "FINISH_BEFORE=$before"
start_resp=$(curl -sSL -XPOST 'http://127.0.0.1:7171/backup/actions' -d '__STARTBODY__' 2>/dev/null || true)
printf 'START_RESP=%s\n' "$(printf '%s' "$start_resp" | tr -d '\n')"
SECONDS=0
observed=0
while [ "$SECONDS" -lt 30 ]; do
  if [ -f "$pid" ]; then observed=1; break; fi
done
echo "OBSERVED=$observed"
[ "$observed" -eq 1 ] || exit 0
start=$(date +%s%N)
kill_resp=$(curl -sSL -XPOST 'http://127.0.0.1:7171/backup/actions' -d '__KILLBODY__' 2>/dev/null || true)
end=$(date +%s%N)
echo "KILL_ELAPSED_MS=$(( (end - start) / 1000000 ))"
after=$(curl -sSL 'http://127.0.0.1:7171/metrics' 2>/dev/null | awk -v m="$metric" '$1==m {print $2}')
echo "FINISH_AFTER=$after"
if [ -f "$pid" ]; then echo "PID=EXISTS"; else echo "PID=GONE"; fi
printf 'KILL_RESP=%s\n' "$(printf '%s' "$kill_resp" | tr -d '\n')"
`

// observeInProgressAndKill starts `command` via /backup/actions, waits until it
// is observably in-progress with its pid file present, kills it, and asserts the
// kill behaved correctly. metricCommand is the bare command name ("create",
// "download", "restore") whose clickhouse_backup_last_<command>_finish gauge is
// used as the proof that the worker goroutine actually returned. The start and
// kill run in one in-container script (see observeInProgressKillScript) so a
// fast worker cannot finish before the kill is issued.
//
// Two assertions discriminate a real cancellation from a hung worker:
//  1. kill returns well under cancel_operation_timeout — a worker that ignores
//     context cancellation makes status.waitDone block the whole timeout.
//  2. the *_finish gauge advances — it is only updated by ExecuteWithMetrics
//     after cliApp.Run returns; if waitDone merely timed out it stays put.
func observeInProgressAndKill(r *require.Assertions, env *TestEnvironment, command, backupName, metricCommand string, cancelTimeout time.Duration) {
	pidPath := fmt.Sprintf("/tmp/clickhouse-backup.%s.pid", backupName)
	metric := "clickhouse_backup_last_" + metricCommand + "_finish"
	startBody := fmt.Sprintf(`{"command":%q}`, command)
	killBody := fmt.Sprintf(`{"command":%q}`, fmt.Sprintf("kill %q", command))
	script := strings.NewReplacer(
		"__PID__", pidPath,
		"__METRIC__", metric,
		"__STARTBODY__", startBody,
		"__KILLBODY__", killBody,
	).Replace(observeInProgressKillScript)

	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", script)
	r.NoError(err, "observe+kill script failed:\n%s", out)

	r.Contains(scriptField(out, "START_RESP="), "acknowledged",
		"%q must be acknowledged:\n%s", command, out)
	r.Contains(out, "OBSERVED=1",
		"expected to observe %q in-progress with pid file %s present:\n%s", command, pidPath, out)

	finishBefore := parseFinishField(r, out, "FINISH_BEFORE=")
	finishAfter := parseFinishField(r, out, "FINISH_AFTER=")
	killElapsed := time.Duration(parseIntField(r, out, "KILL_ELAPSED_MS=")) * time.Millisecond
	killResp := scriptField(out, "KILL_RESP=")
	log.Info().Msgf("kill %q returned in %s", command, killElapsed)

	r.Contains(killResp, "\"status\":\"success\"", "kill should succeed: %s", killResp)

	r.Less(killElapsed, cancelTimeout-2*time.Second,
		"kill %q returned in %s; a worker that ignored context cancellation makes "+
			"status.waitDone block until cancel_operation_timeout=%s", command, killElapsed, cancelTimeout)

	r.Greater(finishAfter, finishBefore,
		"clickhouse_backup_last_%s_finish must advance during kill (before=%d after=%d); "+
			"the %s goroutine did not return", metricCommand, finishBefore, finishAfter, metricCommand)

	r.Contains(out, "PID=GONE", "pid file %s must be removed by kill:\n%s", pidPath, out)
}

// scriptField returns the value after the first line of observe+kill script
// output that starts with prefix (e.g. "FINISH_BEFORE="), or "" if absent.
func scriptField(out, prefix string) string {
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}

// parseFinishField parses a clickhouse_backup_last_*_finish unix-timestamp gauge
// (a float such as 1.749e+09) emitted by the observe+kill script into seconds.
func parseFinishField(r *require.Assertions, out, prefix string) int64 {
	field := scriptField(out, prefix)
	v, err := strconv.ParseFloat(field, 64)
	r.NoError(err, "parse %s%q from:\n%s", prefix, field, out)
	return int64(v)
}

// parseIntField parses an integer field emitted by the observe+kill script.
func parseIntField(r *require.Assertions, out, prefix string) int64 {
	field := scriptField(out, prefix)
	v, err := strconv.ParseInt(field, 10, 64)
	r.NoError(err, "parse %s%q from:\n%s", prefix, field, out)
	return v
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
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", execCurlWithFailBody("'http://127.0.0.1:7171/backup/actions'"))
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
