//go:build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const apiBackupNumber = 5

func TestServerAPI(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	fieldTypes := []string{"UInt64", "String", "Int"}
	env.InstallDebIfNotExists(r, "clickhouse-backup", "curl", "jq")
	maxTables := 10
	minFields := 10
	randFields := 10
	fillDatabaseForAPIServer(maxTables, minFields, randFields, env, r, fieldTypes)

	log.Debug().Msg("Run `clickhouse-backup server --watch` in background")
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup server --watch &>>/tmp/clickhouse-backup-server.log")
	time.Sleep(3 * time.Second)

	testAPIBackupVersion(r, env)

	testAPIBackupCreate(r, env)

	testAPIBackupTables(r, env)

	testAPIBackupUpload(r, env)

	testAPIBackupCreateRemote(r, env)

	testAPIBackupTablesRemote(r, env)

	testAPIBackupTablesLocal(r, env)

	testAPIBackupRestoreRemote(r, env)

	testAPIBackupStatus(r, env)

	testAPIBackupList(t, r, env)

	testAPIDeleteLocalDownloadRestore(r, env)

	testAPISkipEmptyTables(r, env)

	testAPIMetrics(r, env)

	testAPIWatchAndKill(r, env)

	testAPIBackupActions(r, env)

	testAPIRestart(r, env)

	testAPIBackupDelete(r, env)

	testAPIBackupClean(r, env)

	testAPIBackupActionsSkipCommands(r, env)

	env.DockerExecNoError(r, "clickhouse-backup", "pkill", "-n", "-f", "clickhouse-backup")
	r.NoError(env.dropDatabase("long_schema", false))
	env.Cleanup(t, r)
}

func testAPIRestart(r *require.Assertions, env *TestEnvironment) {
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL -XPOST 'http://localhost:7171/restart'")
	log.Debug().Msg(out)
	r.NoError(err, "%s\nunexpected POST /restart error %v", out, err)
	r.Contains(out, "acknowledged")

	//some actions need time for restart
	time.Sleep(6 * time.Second)

	var inProgressActions uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&inProgressActions, "SELECT count() FROM system.backup_actions WHERE status!=?", status.CancelStatus))
	r.Equal(uint64(0), inProgressActions)
}

func testAPIBackupStatus(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check system.backup_actions with /backup/actions call")
	env.queryWithNoError(r, "SELECT count() FROM system.backup_actions")

	out, err := env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/backup/status")
	r.NoError(err, "/backup/status unexpected error: %v", err)
	r.True(strings.Trim(out, " \r\n\t") != "", "unexpected empty output for /backup/status")
	r.Contains(out, `"command"`)
	r.Contains(out, `"status"`)
	r.Contains(out, `"start"`)
	r.Contains(out, `"finish"`)
	r.NotContains(out, `"error"`)
}

func testAPIBackupActions(r *require.Assertions, env *TestEnvironment) {
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"create_remote actions_backup1"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"delete local actions_backup1", "restore_remote --rm actions_backup1"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"delete local actions_backup1", "delete remote actions_backup1"}, false)

	runClickHouseClientInsertSystemBackupActions(r, env, []string{"create actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"upload actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"delete local actions_backup2"}, false)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"download actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"restore --rm actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"delete local actions_backup2", "delete remote actions_backup2"}, false)

	inProgressActions := make([]struct {
		Command string `ch:"command"`
		Status  string `ch:"status"`
	}, 0)
	r.NoError(env.ch.StructSelect(&inProgressActions, "SELECT command, status FROM system.backup_actions WHERE command LIKE '%actions%' AND status IN (?,?)", status.InProgressStatus, status.ErrorStatus))
	r.Equal(0, len(inProgressActions), "inProgressActions=%+v", inProgressActions)

	var actionsBackups uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&actionsBackups, "SELECT count() FROM system.backup_list WHERE name LIKE 'backup_action%'"))
	r.Equal(uint64(0), actionsBackups)

	out, err := env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_create_remote_status 1")
	r.Contains(out, "clickhouse_backup_last_create_status 1")
	r.Contains(out, "clickhouse_backup_last_upload_status 1")
	r.Contains(out, "clickhouse_backup_last_delete_status 1")
	r.Contains(out, "clickhouse_backup_last_download_status 1")
	r.Contains(out, "clickhouse_backup_last_restore_status 1")
	r.Regexp(regexp.MustCompile(`clickhouse_backup_local_data_size\s+\d+`), out)
}

func testAPIWatchAndKill(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/watch + /backup/kill")
	runKillCommand := func(command string) {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL 'http://localhost:7171/backup/kill?command=%s'", command))
		r.NoError(err, "%s\nunexpected GET /kill error: %v", out, err)
	}
	checkWatchBackup := func(expectedCount uint64) {
		var watchBackups uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&watchBackups, "SELECT count() FROM system.backup_list WHERE name LIKE 'shard%'"))
		r.Equal(expectedCount, watchBackups)
	}

	checkCanceledCommand := func(expectedCount int) {
		canceledCommands := make([]struct {
			Status  string `ch:"status"`
			Command string `ch:"command"`
		}, 0)
		r.NoError(env.ch.StructSelect(&canceledCommands, "SELECT status, command FROM system.backup_actions WHERE command LIKE 'watch%'"))
		r.Equal(expectedCount, len(canceledCommands))
		for i := range canceledCommands {
			r.Equal("watch", canceledCommands[i].Command)
			r.Equal(status.CancelStatus, canceledCommands[i].Status)
		}
	}

	checkWatchBackup(1)
	runKillCommand("watch")
	checkCanceledCommand(1)

	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/watch'")
	r.NoError(err, "%s\nunexpected GET /backup/watch error: %v", out, err)
	time.Sleep(7 * time.Second)

	checkWatchBackup(1)
	runKillCommand("watch")
	checkCanceledCommand(2)
}

func testAPIBackupDelete(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/delete/{where}/{name}")
	for i := 1; i <= apiBackupNumber; i++ {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/local/z_backup_%d'", i))
		r.NoError(err, "%s\nunexpected POST /backup/delete/local error: %v", out, err)
		r.NotContains(out, "another operation is currently running")
		r.NotContains(out, "\"status\":\"error\"")
		out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/remote/z_backup_%d'", i))
		r.NoError(err, "%s\nunexpected POST /backup/delete/remote error: %v", out, err)
		r.NotContains(out, "another operation is currently running")
		r.NotContains(out, "\"status\":\"error\"")
	}
	out, err := env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_delete_status 1")

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XGET 'http://localhost:7171/backup/list'"))
	r.NoError(err, "%s\nunexpected GET /backup/list error: %v", out, err)
	scanner := bufio.NewScanner(strings.NewReader(out))
	for scanner.Scan() {
		type backupJSON struct {
			Name           string `json:"name"`
			Created        string `json:"created"`
			Size           uint64 `json:"size,omitempty"`
			Location       string `json:"location"`
			RequiredBackup string `json:"required"`
			Desc           string `json:"desc"`
		}
		listItem := backupJSON{}
		r.NoError(json.Unmarshal(scanner.Bytes(), &listItem))
		out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/%s/%s'", listItem.Location, listItem.Name))
		r.NoError(err, "%s\nunexpected POST /backup/delete/%s/%s error: %v", out, listItem.Location, listItem.Name, err)
	}

	r.NoError(scanner.Err())

}

func testAPIBackupClean(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/clean/, /backup/clean_remote_broken/, backup/clean_local_broken/  and /backup/actions fot these two commands")

	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/clean'"))
	r.NoError(err, "%s\nunexpected POST /backup/clean error: %v", out, err)
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/clean/remote_broken'"))
	r.NoError(err, "%s\nunexpected POST /backup/clean/remote_broken error: %v", out, err)
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/clean/local_broken'"))
	r.NoError(err, "%s\nunexpected POST /backup/clean/local_broken error: %v", out, err)
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")

	runClickHouseClientInsertSystemBackupActions(r, env, []string{"clean", "clean_remote_broken", "clean_local_broken"}, false)
}

// testAPIBackupActionsSkipCommands verifies https://github.com/Altinity/clickhouse-backup/issues/1359
// when api.backup_actions_skip_commands contains "list", neither GET /backup/list nor
// INSERT INTO system.backup_actions ('list ...') must produce rows in system.backup_actions.
func testAPIBackupActionsSkipCommands(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check api.backup_actions_skip_commands excludes 'list' from system.backup_actions")

	// Restart deterministically: kill the previous `server --watch`, wait until it
	// actually exits (so it releases :7171 before the new process binds), then start
	// the skip-enabled server and poll until it serves requests. Fixed sleeps here
	// flaked in CI because the old server hadn't released the port / the new one
	// wasn't ready before commands were issued.
	env.DockerExecNoError(r, "clickhouse-backup", "pkill", "-n", "-f", "clickhouse-backup")
	// `[c]lickhouse-backup` so pgrep does not match its own `bash -ce` command line.
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "for i in $(seq 1 30); do pgrep -f '[c]lickhouse-backup server' >/dev/null 2>&1 || exit 0; sleep 1; done; echo 'previous backup server did not exit'; exit 1")
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce", "API_BACKUP_ACTIONS_SKIP_COMMANDS=list clickhouse-backup server &>>/tmp/clickhouse-backup-server.log")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "for i in $(seq 1 30); do curl -sfL 'http://localhost:7171/backup/list' >/dev/null 2>&1 && exit 0; sleep 1; done; echo 'restarted clickhouse-backup server is not ready'; exit 1")

	// snapshot after restart — in-memory async status is cleared on restart
	var listRowsBefore uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&listRowsBefore, "SELECT count() FROM system.backup_actions WHERE command LIKE 'list%'"))

	for i := 0; i < 3; i++ {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL -XGET 'http://localhost:7171/backup/list'")
		r.NoError(err, "%s\nunexpected GET /backup/list error: %v", out, err)
	}
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL -XGET 'http://localhost:7171/backup/list/local'")
	r.NoError(err, "%s\nunexpected GET /backup/list/local error: %v", out, err)

	// system.backup_list is a URL table engine pointing at GET /backup/list,
	// so this also exercises the skip path through ClickHouse itself.
	for i := 0; i < 3; i++ {
		env.queryWithNoError(r, "SELECT * FROM system.backup_list FORMAT Null")
	}
	time.Sleep(2 * time.Second)

	var listRowsAfter uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&listRowsAfter, "SELECT count() FROM system.backup_actions WHERE command LIKE 'list%'"))
	r.Equal(listRowsBefore, listRowsAfter, "expected no new 'list%%' rows in system.backup_actions, before=%d after=%d", listRowsBefore, listRowsAfter)

	var createRows uint64
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"create skip_commands_test"}, true)
	r.NoError(env.ch.SelectSingleRowNoCtx(&createRows, "SELECT count() FROM system.backup_actions WHERE command='create skip_commands_test' AND status=?", status.SuccessStatus))
	if createRows != 1 {
		// The command was recorded but ended non-success (CI-only flake). Surface the
		// actual status/error and the server log so the root cause is diagnosable.
		createActions := make([]struct {
			Status string `ch:"status"`
			Error  string `ch:"error"`
		}, 0)
		r.NoError(env.ch.StructSelect(&createActions, "SELECT status, error FROM system.backup_actions WHERE command='create skip_commands_test'"))
		logOut, _ := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "tail -n 200 /tmp/clickhouse-backup-server.log")
		log.Error().Msgf("create skip_commands_test did not succeed, actions=%+v\nclickhouse-backup server log tail:\n%s", createActions, logOut)
	}
	r.Equal(uint64(1), createRows, "non-skipped commands must still be recorded in system.backup_actions")
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"delete local skip_commands_test"}, false)
}

func testAPIMetrics(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /metrics clickhouse_backup_last_backup_size_remote")
	var lastRemoteSize uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&lastRemoteSize, "SELECT size FROM system.backup_list WHERE name='z_backup_5' AND location='remote'"))

	var longSchemaTotalBytes uint64
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		r.NoError(env.ch.SelectSingleRowNoCtx(&longSchemaTotalBytes, "SELECT sum(total_bytes) FROM system.tables WHERE database='long_schema'"))
	} else {
		r.NoError(env.ch.SelectSingleRowNoCtx(&longSchemaTotalBytes, "SELECT sum(bytes_on_disk) FROM system.parts WHERE database='long_schema'"))
	}
	var metricsTotalBytes float64
	r.NoError(env.ch.SelectSingleRowNoCtx(&metricsTotalBytes, "SELECT value FROM system.asynchronous_metrics WHERE metric='TotalBytesOfMergeTreeTables'"))

	r.Greater(longSchemaTotalBytes, uint64(0))
	r.Greater(lastRemoteSize, longSchemaTotalBytes)

	// UpdateBackupMetrics is now synchronous in the request handlers (before status=success / before HTTP response),
	// but the background watch loop may still be refreshing metrics on its own cadence. Poll briefly so we
	// don't race a transient snapshot taken between a watch iteration's create and its metrics refresh.
	expectedNumberLocal := fmt.Sprintf("clickhouse_backup_number_backups_local %d", apiBackupNumber)
	expectedNumberRemote := fmt.Sprintf("clickhouse_backup_number_backups_remote %d", apiBackupNumber+1) // +1 watch backup
	expectedLastRemoteSize := fmt.Sprintf("clickhouse_backup_last_backup_size_remote %d", lastRemoteSize)

	var out string
	var err error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, err = env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/metrics")
		if err == nil &&
			strings.Contains(out, expectedLastRemoteSize) &&
			strings.Contains(out, expectedNumberLocal) &&
			strings.Contains(out, expectedNumberRemote) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, expectedLastRemoteSize)

	log.Debug().Msg("Check /metrics clickhouse_backup_number_backups_*")
	if !strings.Contains(out, expectedNumberLocal) {
		listOut, listErr := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "list", "local")
		r.NoError(listErr)
		log.Error().Msg(listOut)
		env.tc.dumpContainerInfo(context.Background(), "clickhouse-backup")
		env.tc.dumpContainerInfo(context.Background(), "clickhouse")
	}
	r.Contains(out, expectedNumberLocal)

	if !strings.Contains(out, expectedNumberRemote) {
		listOut, listErr := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "list", "remote")
		r.NoError(listErr)
		log.Error().Msg(listOut)
		env.tc.dumpContainerInfo(context.Background(), "clickhouse-backup")
		env.tc.dumpContainerInfo(context.Background(), "clickhouse")
	}
	r.Contains(out, expectedNumberRemote)
	r.Contains(out, "clickhouse_backup_number_backups_local_expected 0")
	r.Contains(out, "clickhouse_backup_number_backups_remote_expected 0")
	r.Regexp(`clickhouse_backup_local_data_size \d+`, out)
}

func testAPIDeleteLocalDownloadRestore(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/delete/local/{name} + /backup/download/{name} + /backup/restore/{name}?rm=1")
	out := strings.Builder{}
	for i := 1; i <= apiBackupNumber; i++ {
		deleteOut, err := env.DockerExecOut(
			"clickhouse-backup",
			"bash", "-ce",
			fmt.Sprintf("curl -sfL -XPOST \"http://localhost:7171/backup/delete/local/z_backup_%d\"", i),
		)
		out.WriteString(deleteOut)
		r.NoError(err, "%s\nunexpected POST /backup/delete/local error: %v", deleteOut, err)

		downloadOut, err := env.DockerExecOut(
			"clickhouse-backup",
			"bash", "-ce",
			fmt.Sprintf("curl -sfL -XPOST \"http://localhost:7171/backup/download/z_backup_%d?hardlink_exists_files=true\"", i),
		)
		out.WriteString(downloadOut)
		r.NoError(err, "%s\nunexpected POST /backup/download error: %v", downloadOut, err)
		downloadOperationId := parseAPIOperationID(r, downloadOut)
		waitForAPIOperationStatus(r, env, downloadOperationId, "download", 60*time.Second)

		restoreOut, err := env.DockerExecOut(
			"clickhouse-backup",
			"bash", "-ce",
			fmt.Sprintf("curl -sfL -XPOST \"http://localhost:7171/backup/restore/z_backup_%d?rm=1&drop=true\"", i),
		)
		out.WriteString(restoreOut)
		r.NoError(err, "%s\nunexpected POST /backup/restore error: %v", restoreOut, err)
		restoreOperationId := parseAPIOperationID(r, restoreOut)
		waitForAPIOperationStatus(r, env, restoreOperationId, "restore", 60*time.Second)
	}
	outText := out.String()
	r.NotContains(outText, "another operation is currently running")
	r.NotContains(outText, "\"status\":\"error\"")

	outActions, err := env.DockerExecOut("clickhouse-backup", "curl", "-sfL", "http://localhost:7171/backup/actions?filter=download")
	r.NoError(err, "%s\nunexpected GET /backup/actions?filter=download error: %v", outActions, err)
	r.NotContains(outActions, "\"status\":\"error\"")

	waitForAPIMetricsContains(r, env, 30*time.Second,
		"clickhouse_backup_last_delete_status 1",
		"clickhouse_backup_last_download_status 1",
		"clickhouse_backup_last_restore_status 1",
	)
}

// testAPISkipEmptyTables tests the skip-empty-tables query parameter for restore and restore_remote API endpoints
// https://github.com/Altinity/clickhouse-backup/issues/1265
func testAPISkipEmptyTables(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/restore and /backup/restore_remote with skip-empty-tables parameter")

	backupName := "api_skip_empty_backup"

	// Create test database with tables - one with data and one empty
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS test_api_skip_empty")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_api_skip_empty.table_with_data (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_api_skip_empty.empty_table (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "INSERT INTO test_api_skip_empty.table_with_data SELECT number FROM numbers(50)")

	// Create and upload backup via API
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/create?name=%s'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/create error: %v", out, err)
	r.Contains(out, "acknowledged")
	waitForAPIOperationStatus(r, env, parseAPIOperationID(r, out), "create", 60*time.Second)

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/upload/%s'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/upload error: %v", out, err)
	r.Contains(out, "acknowledged")
	waitForAPIOperationStatus(r, env, parseAPIOperationID(r, out), "upload", 60*time.Second)

	// Drop database
	r.NoError(env.dropDatabase("test_api_skip_empty", true))

	// Test restore with skip-empty-tables parameter
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/restore/%s?rm=1&drop=true&skip-empty-tables=true'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/restore error: %v", out, err)
	r.Contains(out, "acknowledged")
	waitForAPIOperationStatus(r, env, parseAPIOperationID(r, out), "restore", 60*time.Second)

	// Verify only non-empty table exists (empty table should be skipped entirely)
	var tableCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&tableCount, "SELECT count() FROM system.tables WHERE database='test_api_skip_empty'"))
	r.Equal(uint64(1), tableCount, "Only table_with_data should be restored with --skip-empty-tables")

	// Verify data in table_with_data
	var dataCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&dataCount, "SELECT count() FROM test_api_skip_empty.table_with_data"))
	r.Equal(uint64(50), dataCount, "table_with_data should have 50 rows")

	// Test restore_remote with skip_empty_tables parameter (underscore version)
	r.NoError(env.dropDatabase("test_api_skip_empty", true))
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/local/%s'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/delete/local error: %v", out, err)

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/restore_remote/%s?rm=1&drop=true&skip_empty_tables=true'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/restore_remote error: %v", out, err)
	r.Contains(out, "acknowledged")
	waitForAPIOperationStatus(r, env, parseAPIOperationID(r, out), "restore_remote", 60*time.Second)

	// Verify only non-empty table exists
	r.NoError(env.ch.SelectSingleRowNoCtx(&tableCount, "SELECT count() FROM system.tables WHERE database='test_api_skip_empty'"))
	r.Equal(uint64(1), tableCount, "Only table_with_data should be restored with restore_remote --skip-empty-tables")

	// Verify data in table_with_data
	r.NoError(env.ch.SelectSingleRowNoCtx(&dataCount, "SELECT count() FROM test_api_skip_empty.table_with_data"))
	r.Equal(uint64(50), dataCount, "table_with_data should have 50 rows with restore_remote")

	// Clean up
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/local/%s'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/delete/local error: %v", out, err)
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/remote/%s'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/delete/remote error: %v", out, err)
	r.NoError(env.dropDatabase("test_api_skip_empty", true))
}

func testAPIBackupList(t *testing.T, r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/list")
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list'")
	r.NoError(err, "%s\nunexpected GET /backup/list error: %v", out, err)
	localListFormat := "{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"data_size\":\\d+,\"metadata_size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"regular\"}"
	remoteListFormat := "{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"data_size\":\\d+,\"metadata_size\":\\d+,\"compressed_size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"(tar|directory), regular\"}"
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf(localListFormat, i)), out))
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf(remoteListFormat, i)), out))
	}

	log.Debug().Msg("Check /backup/list/local")
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list/local'")
	r.NoError(err, "%s\nunexpected GET /backup/list/local error: %v", out, err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf(localListFormat, i)), out))
		r.True(assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf(remoteListFormat, i)), out))
	}

	log.Debug().Msg("Check /backup/list/remote")
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list/remote'")
	r.NoError(err, "%s\nunexpected GET /backup/list/remote error: %v", out, err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf(localListFormat, i)), out))
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf(remoteListFormat, i)), out))
	}
}

func testAPIBackupUpload(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/upload")
	out := strings.Builder{}
	for i := 1; i <= apiBackupNumber; i++ {
		uploadOut, err := env.DockerExecOut(
			"clickhouse-backup",
			"bash", "-ce",
			fmt.Sprintf("curl -sfL -XPOST \"http://localhost:7171/backup/upload/z_backup_%d\"", i),
		)
		out.WriteString(uploadOut)
		r.NoError(err, "%s\nunexpected POST /backup/upload error: %v", uploadOut, err)
		waitForAPIOperationStatus(r, env, parseAPIOperationID(r, uploadOut), "upload", 60*time.Second)
	}
	outText := out.String()
	r.NotContains(outText, "error")
	r.NotContains(outText, "another operation is currently running")
	r.NotContains(outText, "command is already running")

	outActions, err := env.DockerExecOut("clickhouse-backup", "curl", "-sfL", "http://localhost:7171/backup/actions?filter=upload")
	r.NoError(err, "%s\nunexpected GET /backup/actions?filter=upload error: %v", outActions, err)
	r.NotContains(outActions, "error")

	waitForAPIMetricsContains(r, env, 30*time.Second, "clickhouse_backup_last_upload_status 1")
}

func testAPIBackupTables(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/tables")
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables\"",
	)
	r.NoError(err, "%s\nunexpected GET /backup/tables error: %v", out, err)
	r.Contains(out, "long_schema")
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	r.NotContains(out, "system")
	r.NotContains(out, "INFORMATION_SCHEMA")
	r.NotContains(out, "information_schema")

	log.Debug().Msg("Check /backup/tables/all")
	out, err = env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables/all\"",
	)
	r.NoError(err, "%s\nunexpected GET /backup/tables/all error: %v", out, err)
	r.Contains(out, "long_schema")
	r.Contains(out, "system")
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.3") >= 0 {
		r.Contains(out, "INFORMATION_SCHEMA")
		r.Contains(out, "information_schema")
	}
}

func testAPIBackupTablesRemote(r *require.Assertions, env *TestEnvironment) {

	log.Debug().Msg("Check /backup/tables?remote_backup=z_backup_1")
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables?remote_backup=z_backup_1\"",
	)
	r.NoError(err, "%s\nunexpected GET /backup/tables?remote_backup=z_backup_1 error: %v", out, err)
	r.Contains(out, "long_schema")
	r.NotContains(out, "system")
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	r.NotContains(out, "INFORMATION_SCHEMA")
	r.NotContains(out, "information_schema")
	r.NotContains(out, "command is already running")
	// /backup/tables?remote_backup=... must include per-table size and parts (https://github.com/Altinity/clickhouse-backup/issues/1388).
	r.Contains(out, `"size":`)
	r.Contains(out, `"parts":`)
	r.Contains(out, `"total_bytes":`)
	r.Contains(out, `"disks":[`)
}

// testAPIBackupTablesLocal exercises /backup/tables?local_backup=<name>
// (https://github.com/Altinity/clickhouse-backup/issues/1388) — listing tables
// from a local backup without a live ClickHouse query, with size and parts.
func testAPIBackupTablesLocal(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/tables?local_backup=z_backup_1")
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables?local_backup=z_backup_1\"",
	)
	r.NoError(err, "%s\nunexpected GET /backup/tables?local_backup=z_backup_1 error: %v", out, err)
	r.Contains(out, "long_schema")
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	r.NotContains(out, "system")
	r.NotContains(out, "INFORMATION_SCHEMA")
	r.NotContains(out, "information_schema")
	r.Contains(out, `"size":`)
	r.Contains(out, `"parts":`)
	r.Contains(out, `"total_bytes":`)
	r.Contains(out, `"disks":[`)

	// Filtered by table pattern.
	out, err = env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables?local_backup=z_backup_1&table=long_schema.t0\"",
	)
	r.NoError(err, "%s\nunexpected GET /backup/tables?local_backup=z_backup_1&table=long_schema.t0 error: %v", out, err)
	r.Contains(out, `"table":"t0"`)
	r.NotContains(out, `"table":"t1"`)
}

func testAPIBackupVersion(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/version")
	cliVersion, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup --version 2>/dev/null --version | grep 'Version' | cut -d ':' -f 2 | xargs")
	r.NoError(err)
	apiVersion, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sL http://localhost:7171/backup/version | jq -r .version")
	r.NoError(err)
	if cliVersion != apiVersion {
		debugLog, debugErr := env.DockerExecOut("clickhouse-backup", "cat", "/tmp/clickhouse-backup-server.log")
		r.NoError(debugErr)
		log.Error().Msg(debugLog)
	}
	r.Equal(cliVersion, apiVersion)
	tablesVersion, err := env.DockerExecOut("clickhouse", "bash", "-ce", "clickhouse client -q 'SELECT * FROM system.backup_version FORMAT TSVRaw'")
	r.NoError(err)
	if cliVersion != tablesVersion {
		debugLog, debugErr := env.DockerExecOut("clickhouse-backup", "cat", "/tmp/clickhouse-backup-server.log")
		r.NoError(debugErr)
		log.Error().Msg(debugLog)
	}
	r.Equal(cliVersion, tablesVersion)
}

func testAPIBackupCreate(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/create")
	time.Sleep(3 * time.Second)
	out := strings.Builder{}
	for i := 1; i <= apiBackupNumber; i++ {
		createOut, err := env.DockerExecOut(
			"clickhouse-backup",
			"bash", "-ce",
			fmt.Sprintf("curl -sfL -XPOST \"http://localhost:7171/backup/create?table=long_schema.*&name=z_backup_%d\"", i),
		)
		out.WriteString(createOut)
		serverLog := ""
		if err != nil {
			serverLog, _ = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "tail -n 200 /tmp/clickhouse-backup-server.log 2>&1 || true")
		}
		r.NoError(err, "%s\nunexpected POST /backup/create?table=long_schema.*&name=z_backup_%d error: %v\n/tmp/clickhouse-backup-server.log tail:\n%s", createOut, i, err, serverLog)

		waitForAPIOperationStatus(r, env, parseAPIOperationID(r, createOut), "create", 60*time.Second)
	}
	outText := out.String()
	r.NotContains(outText, "Connection refused")
	r.NotContains(outText, "another operation is currently running")
	r.NotContains(outText, "\"status\":\"error\"")
	waitForAPIMetricsContains(r, env, 30*time.Second, "clickhouse_backup_last_create_status 1")
}

func parseAPIOperationID(r *require.Assertions, out string) string {
	var resp struct {
		OperationId string `json:"operation_id"`
	}
	r.NoError(json.Unmarshal([]byte(out), &resp))
	_, err := uuid.Parse(strings.TrimSpace(resp.OperationId))
	r.NoError(err, "operation_id is not a valid UUID: %s", resp.OperationId)
	return resp.OperationId
}

func waitForAPIOperationStatus(r *require.Assertions, env *TestEnvironment, operationId, operation string, timeout time.Duration) {
	startTime := time.Now()
	for {
		if time.Since(startTime) > timeout {
			r.FailNowf("timeout waiting for API operation", "operation=%s operation_id=%s", operation, operationId)
		}
		statusOut, err := env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/backup/status?operationid="+operationId)
		r.NoError(err)

		var action status.ActionRowStatus
		r.NoError(json.Unmarshal([]byte(strings.TrimSpace(statusOut)), &action))
		if action.Status != status.InProgressStatus {
			r.Equal(status.SuccessStatus, action.Status, "command '%s' failed with error: %s", action.Command, action.Error)
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func waitForAPIMetricsContains(r *require.Assertions, env *TestEnvironment, timeout time.Duration, expected ...string) {
	deadline := time.Now().Add(timeout)
	var out string
	var err error
	for time.Now().Before(deadline) {
		out, err = env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/metrics")
		if err == nil {
			allFound := true
			for _, item := range expected {
				if !strings.Contains(out, item) {
					allFound = false
					break
				}
			}
			if allFound {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	for _, item := range expected {
		r.Contains(out, item)
	}
}

func fillDatabaseForAPIServer(maxTables int, minFields int, randFields int, ch *TestEnvironment, r *require.Assertions, fieldTypes []string) {
	log.Debug().Msgf("Create %d `long_schema`.`t%%d` tables with with %d..%d fields...", maxTables, minFields, minFields+randFields)
	ch.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS long_schema")
	for i := 0; i < maxTables; i++ {
		sql := fmt.Sprintf("CREATE TABLE long_schema.t%d (id UInt64", i)
		fieldsCount := minFields + rand.Intn(randFields)
		for j := 0; j < fieldsCount; j++ {
			fieldType := fieldTypes[rand.Intn(len(fieldTypes))]
			sql += fmt.Sprintf(", f%d %s", j, fieldType)
		}
		sql += ") ENGINE=MergeTree() ORDER BY id"
		ch.queryWithNoError(r, sql)
		sql = fmt.Sprintf("INSERT INTO long_schema.t%d(id) SELECT number FROM numbers(100)", i)
		ch.queryWithNoError(r, sql)
	}
	log.Debug().Msg("...DONE")
}

func testAPIBackupCreateRemote(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/create_remote")
	backupName := "z_backup_remote_api"
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-ec",
		fmt.Sprintf("curl -sfL -XPOST \"http://localhost:7171/backup/create_remote?table=long_schema.*&name=%s\"", backupName),
	)
	r.NoError(err, "%s\nunexpected POST /backup/create_remote error: %v", out, err)
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")

	waitForAPIOperationStatus(r, env, parseAPIOperationID(r, out), "create_remote", 60*time.Second)

	waitForAPIMetricsContains(r, env, 30*time.Second, "clickhouse_backup_last_create_remote_status 1")
}

func testAPIBackupRestoreRemote(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/restore_remote/{name}")
	backupName := "z_backup_remote_api"
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-ce",
		fmt.Sprintf("curl -sfL -XPOST \"http://localhost:7171/backup/restore_remote/%s?hardlink_exists_files=true&drop=true&rm=true\"", backupName),
	)
	r.NoError(err, "%s\nunexpected POST /backup/restore_remote error: %v", out, err)
	r.NotContains(out, "error")
	r.NotContains(out, "another operation is currently running")

	waitForAPIOperationStatus(r, env, parseAPIOperationID(r, out), "restore_remote", 60*time.Second)

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "-sfL", "http://localhost:7171/backup/actions?filter=restore_remote")
	r.NoError(err, "%s\nunexpected GET /backup/actions?filter=restore_remote error: %v", out, err)
	r.NotContains(out, "error")
	r.Contains(out, "success")
	r.Contains(out, backupName)

	waitForAPIMetricsContains(r, env, 30*time.Second, "clickhouse_backup_last_restore_remote_status 1")

	// cleanup
	_, err = env.DockerExecOut(
		"clickhouse-backup", "bash", "-xe", "-c",
		"curl -sfL -XPOST \"http://localhost:7171/backup/delete/remote/z_backup_remote_api\"",
	)
	r.NoError(err)
	_, err = env.DockerExecOut(
		"clickhouse-backup", "bash", "-xe", "-c",
		"curl -sfL -XPOST \"http://localhost:7171/backup/delete/local/z_backup_remote_api\"",
	)
	r.NoError(err)
}
