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

// TestDryRun covers `--dry-run` for create, upload, download, restore and delete,
// https://github.com/Altinity/clickhouse-backup/issues/1012
// Every dry-run must exit 0, print the summary line and leave no side effect, so each step
// asserts the state which the corresponding real command would have changed.
func TestDryRun(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	const configFile = "/etc/clickhouse-backup/config-s3.yml"
	dbName := "test_dry_run"
	backupName := "test_dry_run_backup"
	apiBackupName := "test_dry_run_api"
	apiBackupName2 := "test_dry_run_api_actions"
	apiLifecycleName := "test_dry_run_api_lifecycle"
	apiRemoteName := "test_dry_run_api_create_remote"

	// deferred (not t.Cleanup) so it unwinds while the ClickHouse connection is still open and
	// before the pooled env is returned, see the same pattern in runRebaseScenario
	cleanupScenario := func() {
		for _, name := range []string{backupName, apiBackupName, apiBackupName2, apiLifecycleName, apiRemoteName} {
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c "+configFile+" delete remote "+name+" 2>/dev/null || true")
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c "+configFile+" delete local "+name+" 2>/dev/null || true")
		}
		r.NoError(env.dropDatabase(dbName, true))
	}
	defer cleanupScenario()
	cleanupScenario()

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t1 (id UInt64, v String) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t1 SELECT number, toString(number) FROM numbers(5000)")

	assertNoLocalBackup := func(name string) {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "ls -d /var/lib/clickhouse/backup/"+name+" 2>/dev/null || echo NOT_EXISTS")
		r.NoError(err)
		r.Containsf(out, "NOT_EXISTS", "expect no local backup directory for %s, got: %s", name, out)
	}
	assertLocalBackupExists := func(name string) {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "test -d /var/lib/clickhouse/backup/"+name)
	}
	listRemote := func() string {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c "+configFile+" list remote 2>/dev/null | cut -d ' ' -f 1 || true")
		r.NoError(err)
		return out
	}

	// b. create --dry-run shall report a non-zero tables count and create nothing
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "create", "--dry-run", "--tables="+dbName+".*", backupName)
	r.NoError(err, "create --dry-run: %s", out)
	checkDryRunOutput(r, out, "create", 1)
	r.Containsf(out, "hardlink_max_size=", "create --dry-run shall report the hardlink forecast: %s", out)
	assertNoLocalBackup(backupName)

	// c. upload --dry-run of an existing local backup shall upload nothing
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "create", "--tables="+dbName+".*", backupName)
	assertLocalBackupExists(backupName)
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "upload", "--dry-run", backupName)
	r.NoError(err, "upload --dry-run: %s", out)
	checkDryRunOutput(r, out, "upload", 1)
	r.NotContainsf(listRemote(), backupName, "upload --dry-run shall not upload %s", backupName)

	// d. delete local --dry-run shall keep the local backup, urfave/cli v1 needs the flag before the args
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "upload", backupName)
	r.Containsf(listRemote(), backupName, "expect %s on remote after the real upload", backupName)
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "--dry-run", "local", backupName)
	r.NoError(err, "delete --dry-run local: %s", out)
	checkDryRunOutput(r, out, "delete", 1)
	assertLocalBackupExists(backupName)

	// e. download --dry-run shall not create the local backup directory
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", backupName)
	assertNoLocalBackup(backupName)
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "download", "--dry-run", backupName)
	r.NoError(err, "download --dry-run: %s", out)
	checkDryRunOutput(r, out, "download", 1)
	assertNoLocalBackup(backupName)

	// f. restore --dry-run shall not create the dropped table
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "download", backupName)
	assertLocalBackupExists(backupName)
	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
		dropSuffix = " NO DELAY"
	}
	env.queryWithNoError(t, r, "DROP TABLE "+dbName+".t1"+dropSuffix)
	existsTableSQL := fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s' AND name='t1' SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName)
	env.checkCount(r, 1, 0, existsTableSQL)
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "restore", "--dry-run", backupName)
	r.NoError(err, "restore --dry-run: %s", out)
	checkDryRunOutput(r, out, "restore", 1)
	env.checkCount(r, 1, 0, existsTableSQL)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "restore", backupName)
	env.checkCount(r, 1, 5000, "SELECT count() FROM "+dbName+".t1")

	// g. delete remote --dry-run shall keep the backup on remote storage
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "--dry-run", "remote", backupName)
	r.NoError(err, "delete --dry-run remote: %s", out)
	checkDryRunOutput(r, out, "delete", 1)
	r.Containsf(listRemote(), backupName, "delete --dry-run remote shall keep %s on remote", backupName)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "remote", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", backupName)

	// h. REST API: POST /backup/create?dry_run=1 runs synchronously and returns the report
	log.Debug().Msg("Run `clickhouse-backup server` in background")
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c "+configFile+" server &>>/tmp/clickhouse-backup-dry-run-server.log")
	defer func() {
		_ = env.DockerExec("clickhouse-backup", "bash", "-ce", "pkill -f '[c]lickhouse-backup.*server' || true")
	}()
	time.Sleep(3 * time.Second)
	// apiDryRun POSTs a `dry_run=1` request and asserts the synchronously returned report.
	// The container has no curl, wget is available in busybox/debian base images; stderr is merged
	// into the output so an HTTP error is visible in the assertion message.
	apiDryRun := func(path, wantCommand string) string {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
			"wget -O- --post-data='' 'http://localhost:7171"+path+"' 2>&1")
		r.NoError(err, "POST %s: %s", path, out)
		r.Containsf(out, `"command":"`+wantCommand+`"`, "POST %s shall return the %s dry-run report: %s", path, wantCommand, out)
		r.Containsf(out, `"dry_run":true`, "POST %s shall return a dry-run report: %s", path, out)
		return out
	}
	apiDryRun("/backup/create?name="+apiBackupName+"&table="+dbName+".*&dry_run=1", "create")
	assertNoLocalBackup(apiBackupName)

	// i. the same report shall be stored in the `result` field of the status row, so it is
	// readable via /backup/status, GET /backup/actions and system.backup_actions
	// the report is a JSON string inside JSON, so its own quotes arrive escaped
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "wget -qO- 'http://localhost:7171/backup/status'")
	r.NoError(err, "GET /backup/status: %s", out)
	r.Containsf(out, `"result":"{`, "GET /backup/status shall report the dry-run report in `result`: %s", out)
	r.Containsf(out, `\"command\":\"create\"`, "unexpected `result` in GET /backup/status: %s", out)
	r.Containsf(out, `\"dry_run\":true`, "unexpected `result` in GET /backup/status: %s", out)

	// j. POST /backup/actions runs the same command through the in-process CLI, the `result`
	// must be set before the row is marked finished
	actionCommand := "create --dry-run --tables=" + dbName + ".* " + apiBackupName2
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		`wget -qO- --post-data='{"command":"`+actionCommand+`"}' 'http://localhost:7171/backup/actions'`)
	r.NoError(err, "POST /backup/actions: %s", out)
	r.Containsf(out, "acknowledged", "POST /backup/actions shall acknowledge the dry-run command: %s", out)

	actionRow := ""
	for i := 0; i < 20; i++ {
		out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce",
			"wget -qO- 'http://localhost:7171/backup/actions?filter="+apiBackupName2+"'")
		r.NoError(err, "GET /backup/actions: %s", out)
		actionRow = out
		if strings.Contains(out, `"status":"success"`) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	r.Containsf(actionRow, `"status":"success"`, "%s shall finish successfully: %s", actionCommand, actionRow)
	r.Containsf(actionRow, `"result":"{`, "GET /backup/actions shall report the dry-run report in `result`: %s", actionRow)
	r.Containsf(actionRow, `\"dry_run\":true`, "unexpected `result` in GET /backup/actions: %s", actionRow)
	assertNoLocalBackup(apiBackupName2)

	// k. one backup lifecycle covering the remaining dry-run endpoints, each with a valid
	// precondition. The CLI runs against the same config while the server is up, that is safe
	// because the async status lock is per process.
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "create", "--tables="+dbName+".*", apiLifecycleName)
	assertLocalBackupExists(apiLifecycleName)

	// upload --dry-run must run before the real upload, uploading a backup which already exists
	// on remote fails the same way the real command does
	apiDryRun("/backup/upload/"+apiLifecycleName+"?dry_run=1", "upload")
	r.NotContainsf(listRemote(), apiLifecycleName, "POST /backup/upload?dry_run=1 shall not upload %s", apiLifecycleName)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "upload", apiLifecycleName)
	r.Containsf(listRemote(), apiLifecycleName, "expect %s on remote after the real upload", apiLifecycleName)

	apiDryRun("/backup/delete/local/"+apiLifecycleName+"?dry_run=1", "delete")
	assertLocalBackupExists(apiLifecycleName)

	apiDryRun("/backup/delete/remote/"+apiLifecycleName+"?dry_run=1", "delete")
	r.Containsf(listRemote(), apiLifecycleName, "POST /backup/delete/remote?dry_run=1 shall keep %s on remote", apiLifecycleName)

	// create_remote --dry-run while the table still exists, so the report counts a real table
	apiDryRun("/backup/create_remote?name="+apiRemoteName+"&table="+dbName+".*&dry_run=1", "create_remote")
	assertNoLocalBackup(apiRemoteName)
	r.NotContainsf(listRemote(), apiRemoteName, "POST /backup/create_remote?dry_run=1 shall not upload %s", apiRemoteName)

	// restore --dry-run of the local backup shall not recreate the dropped table
	env.queryWithNoError(t, r, "DROP TABLE "+dbName+".t1"+dropSuffix)
	env.checkCount(r, 1, 0, existsTableSQL)
	apiDryRun("/backup/restore/"+apiLifecycleName+"?dry_run=1", "restore")
	env.checkCount(r, 1, 0, existsTableSQL)

	// download --dry-run and restore_remote --dry-run with the local backup deleted, so both
	// estimate from remote and neither may materialize the local directory
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", apiLifecycleName)
	assertNoLocalBackup(apiLifecycleName)

	apiDryRun("/backup/download/"+apiLifecycleName+"?dry_run=1", "download")
	assertNoLocalBackup(apiLifecycleName)

	apiDryRun("/backup/restore_remote/"+apiLifecycleName+"?dry_run=1", "restore_remote")
	assertNoLocalBackup(apiLifecycleName)
	env.checkCount(r, 1, 0, existsTableSQL)
}

var dryRunSummaryRE = regexp.MustCompile(`dry-run: would process (\d+) tables`)

// checkDryRunOutput asserts the single summary line printed by setDryRunResult: the message itself,
// the `dry_run=true`/`operation=<command>` structured fields and a minimal tables count
func checkDryRunOutput(r *require.Assertions, out, command string, minTables int) {
	matches := dryRunSummaryRE.FindStringSubmatch(out)
	r.NotNilf(matches, "no `dry-run: would process N tables` line in %s --dry-run output: %s", command, out)
	tables, err := strconv.Atoi(matches[1])
	r.NoError(err)
	r.GreaterOrEqualf(tables, minTables, "%s --dry-run shall report at least %d tables: %s", command, minTables, out)
	r.Containsf(out, "dry_run=true", "%s --dry-run shall log dry_run=true: %s", command, out)
	r.Containsf(out, "operation="+command, "%s --dry-run shall log operation=%s: %s", command, command, out)
}
