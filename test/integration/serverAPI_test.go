//go:build integration

package main

import (
	"bufio"
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

func runClickHouseClientInsertSystemBackupActions(r *require.Assertions, env *TestEnvironment, commands []string, needWait bool) {
	sql := "INSERT INTO system.backup_actions(command) " + "VALUES ('" + strings.Join(commands, "'),('") + "')"
	out, err := env.DockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("clickhouse client --echo -mn -q \"%s\"", sql))
	r.NoError(err, "%s -> %s unexpected error: %v", sql, out, err)
	if needWait {
		for _, command := range commands {
			for {
				time.Sleep(500 * time.Millisecond)
				var commandStatus string
				r.NoError(env.ch.SelectSingleRowNoCtx(&commandStatus, "SELECT status FROM system.backup_actions WHERE command=?", command))
				if commandStatus != status.InProgressStatus {
					break
				}
			}
		}
	}
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

	out, err := env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
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
	out, err := env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
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

	out, err := env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, fmt.Sprintf("clickhouse_backup_last_backup_size_remote %d", lastRemoteSize))

	log.Debug().Msg("Check /metrics clickhouse_backup_number_backups_*")
	if !strings.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_local %d", apiBackupNumber)) {
		listOut, listErr := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "list", "local")
		r.NoError(listErr)
		log.Error().Msg(listOut)
	}
	r.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_local %d", apiBackupNumber))

	// +1 watch backup
	if !strings.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_remote %d", apiBackupNumber+1)) {
		listOut, listErr := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "list", "local")
		r.NoError(listErr)
		log.Error().Msg(listOut)
	}
	r.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_remote %d", apiBackupNumber+1))
	r.Contains(out, "clickhouse_backup_number_backups_local_expected 0")
	r.Contains(out, "clickhouse_backup_number_backups_remote_expected 0")
	r.Regexp(`clickhouse_backup_local_data_size \d+`, out)
}

func testAPIDeleteLocalDownloadRestore(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/delete/local/{name} + /backup/download/{name} + /backup/restore/{name}?rm=1")
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c",
		fmt.Sprintf("for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/delete/local/z_backup_$i\"; curl -sfL -XPOST \"http://localhost:7171/backup/download/z_backup_$i?hardlink_exists_files=true\"; sleep 2; curl -sfL -XPOST \"http://localhost:7171/backup/restore/z_backup_$i?rm=1&drop=true\"; sleep 8; done", apiBackupNumber),
	)
	r.NoError(err, "%s\nunexpected POST /backup/delete/local error: %v", out, err)
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "error")

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "-sfL", "http://localhost:7171/backup/actions?filter=download")
	r.NoError(err, "%s\nunexpected GET /backup/actions?filter=download error: %v", out, err)
	r.NotContains(out, "\"status\":\"error\"")

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_delete_status 1")
	r.Contains(out, "clickhouse_backup_last_download_status 1")
	r.Contains(out, "clickhouse_backup_last_restore_status 1")
}

// testAPISkipEmptyTables tests the skip-empty-tables query parameter for restore and restore_remote API endpoints
// https://github.com/Altinity/clickhouse-backup/issues/1265
func testAPISkipEmptyTables(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/restore and /backup/restore_remote with skip-empty-tables parameter")

	backupName := "api_skip_empty_backup"

	// Helper function to wait for operation to complete
	waitForOperation := func(operationName string, timeout time.Duration) {
		startTime := time.Now()
		for {
			if time.Since(startTime) > timeout {
				r.Fail("timeout waiting for " + operationName)
			}
			statusOut, err := env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/backup/status")
			r.NoError(err)

			var lastFoundAction *status.ActionRowStatus
			scanner := bufio.NewScanner(strings.NewReader(statusOut))
			for scanner.Scan() {
				line := scanner.Bytes()
				if len(line) == 0 {
					continue
				}
				var action status.ActionRowStatus
				if unmarshalErr := json.Unmarshal(line, &action); unmarshalErr != nil {
					continue
				}
				// Check if command contains the backup name and starts with the operation name
				if strings.Contains(action.Command, backupName) && strings.HasPrefix(action.Command, operationName) {
					currentAction := action
					lastFoundAction = &currentAction
				}
			}

			if lastFoundAction != nil && lastFoundAction.Status != status.InProgressStatus {
				r.Equal(status.SuccessStatus, lastFoundAction.Status, "command '%s' failed with error: %s", lastFoundAction.Command, lastFoundAction.Error)
				return
			}
			time.Sleep(1 * time.Second)
		}
	}

	// Create test database with tables - one with data and one empty
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS test_api_skip_empty")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_api_skip_empty.table_with_data (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_api_skip_empty.empty_table (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "INSERT INTO test_api_skip_empty.table_with_data SELECT number FROM numbers(50)")

	// Create and upload backup via API
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/create?name=%s'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/create error: %v", out, err)
	r.Contains(out, "acknowledged")
	waitForOperation("create", 60*time.Second)

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/upload/%s'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/upload error: %v", out, err)
	r.Contains(out, "acknowledged")
	waitForOperation("upload", 60*time.Second)

	// Drop database
	r.NoError(env.dropDatabase("test_api_skip_empty", true))

	// Test restore with skip-empty-tables parameter
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/restore/%s?rm=1&drop=true&skip-empty-tables=true'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/restore error: %v", out, err)
	r.Contains(out, "acknowledged")
	waitForOperation("restore", 60*time.Second)

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
	time.Sleep(2 * time.Second)

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/restore_remote/%s?rm=1&drop=true&skip_empty_tables=true'", backupName))
	r.NoError(err, "%s\nunexpected POST /backup/restore_remote error: %v", out, err)
	r.Contains(out, "acknowledged")
	waitForOperation("restore_remote", 60*time.Second)

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
	remoteListFormat := "{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"data_size\":\\d+,\"metadata_size\":\\d+,\"compressed_size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar, regular\"}"
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
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c",
		fmt.Sprintf("for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/upload/z_backup_$i\"; sleep 2; done", apiBackupNumber),
	)
	r.NoError(err, "%s\nunexpected POST /backup/upload error: %v", out, err)
	r.NotContains(out, "error")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "command is already running")

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "-sfL", "http://localhost:7171/backup/actions?filter=upload")
	r.NoError(err, "%s\nunexpected GET /backup/actions?filter=upload error: %v", out, err)
	r.NotContains(out, "error")

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_upload_status 1")
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
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c",
		fmt.Sprintf("sleep 3; for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/create?table=long_schema.*&name=z_backup_$i\"; sleep 1.5; done", apiBackupNumber),
	)
	r.NoError(err, "%s\nunexpected POST /backup/create?table=long_schema.*&name=z_backup_$i error: %v", out, err)
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	out, err = env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_create_status 1")
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

	var resp struct {
		OperationId string `json:"operation_id"`
	}
	r.NoError(json.Unmarshal([]byte(out), &resp))
	_, err = uuid.Parse(strings.TrimSpace(resp.OperationId))
	r.NoError(err, "operation_id is not a valid UUID: %s", resp.OperationId)

	// poll status
	startTime := time.Now()
	for {
		if time.Since(startTime) > 60*time.Second {
			r.Fail("timeout waiting for create_remote")
		}
		statusOut, err := env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/backup/status")
		r.NoError(err)

		var lastFoundAction *status.ActionRowStatus
		scanner := bufio.NewScanner(strings.NewReader(statusOut))
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var action status.ActionRowStatus
			err := json.Unmarshal(line, &action)
			r.NoError(err)
			if strings.Contains(action.Command, backupName) && strings.Contains(action.Command, "create_remote") {
				currentAction := action
				lastFoundAction = &currentAction
			}
		}
		if lastFoundAction != nil && lastFoundAction.Status != status.InProgressStatus {
			r.Equal(status.SuccessStatus, lastFoundAction.Status, "command '%s' failed with error: %s", lastFoundAction.Command, lastFoundAction.Error)
			break
		}
		time.Sleep(1 * time.Second)
	}

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_create_remote_status 1")
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

	var resp struct {
		OperationId string `json:"operation_id"`
	}
	r.NoError(json.Unmarshal([]byte(out), &resp))
	_, err = uuid.Parse(strings.TrimSpace(resp.OperationId))
	r.NoError(err, "operation_id is not a valid UUID: %s", resp.OperationId)

	// poll status
	startTime := time.Now()
	for {
		if time.Since(startTime) > 60*time.Second {
			r.Fail("timeout waiting for restore_remote")
		}
		statusOut, err := env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/backup/status")
		r.NoError(err)

		var lastFoundAction *status.ActionRowStatus
		scanner := bufio.NewScanner(strings.NewReader(statusOut))
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var action status.ActionRowStatus
			err := json.Unmarshal(line, &action)
			r.NoError(err)
			if strings.Contains(action.Command, backupName) && strings.Contains(action.Command, "restore_remote") {
				currentAction := action
				lastFoundAction = &currentAction
			}
		}

		if lastFoundAction != nil && lastFoundAction.Status != status.InProgressStatus {
			r.Equal(status.SuccessStatus, lastFoundAction.Status, "command '%s' failed with error: %s", lastFoundAction.Command, lastFoundAction.Error)
			break
		}
		time.Sleep(1 * time.Second)
	}

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "-sfL", "http://localhost:7171/backup/actions?filter=restore_remote")
	r.NoError(err, "%s\nunexpected GET /backup/actions?filter=restore_remote error: %v", out, err)
	r.NotContains(out, "error")
	r.Contains(out, "success")
	r.Contains(out, backupName)

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_restore_remote_status 1")

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
