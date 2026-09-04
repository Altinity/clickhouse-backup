//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStreamingCreateRestoreRemote covers `create_remote --streaming` and `restore_remote --streaming`,
// https://github.com/Altinity/clickhouse-backup/issues/780
func TestStreamingCreateRestoreRemote(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	const configFile = "config-s3.yml"
	const configArg = "-c /etc/clickhouse-backup/" + configFile
	dbName := "test_streaming_" + t.Name()
	fullBackup := "streaming_full"
	incrementBackup := "streaming_increment"
	plainBackup := "streaming_plain"
	backupNames := []string{fullBackup, incrementBackup, plainBackup}
	fullCleanup(t, r, env, backupNames, []string{"remote", "local"}, []string{dbName}, false, false, false, configFile)

	// several parts per table: PARTITION BY id % 3 gives 3 parts per INSERT
	tables := map[string]string{
		"mt":   "(id UInt64) ENGINE=MergeTree() PARTITION BY id % 3 ORDER BY id",
		"rmt":  "(id UInt64, v UInt64) ENGINE=ReplacingMergeTree() PARTITION BY id % 3 ORDER BY id",
		"repl": "(id UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') PARTITION BY id % 3 ORDER BY id",
	}
	// old 1.x clickhouse versions don't have {database}/{table} macros, same check as createTestSchema
	var isMacrosExists uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&isMacrosExists, "SELECT count() FROM system.functions WHERE name='getMacro'"))
	if isMacrosExists == 0 {
		tables["repl"] = strings.NewReplacer("{database}", dbName, "{table}", "repl").Replace(tables["repl"])
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		tables["s3"] = "(id UInt64) ENGINE=MergeTree() PARTITION BY id % 3 ORDER BY id SETTINGS storage_policy='s3_only'"
	}
	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	for table, schema := range tables {
		env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE `%s`.`%s` %s", dbName, table, schema))
	}
	expected := make(map[string]uint64, len(tables))
	insertRows := func(offset, rows int) {
		for table := range tables {
			// unique ids across inserts, so no block deduplication and ReplacingMergeTree keeps every row
			columns := fmt.Sprintf("number + %d", offset)
			if table == "rmt" {
				columns = fmt.Sprintf("number + %d, number + %d", offset, offset)
			}
			env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT %s FROM numbers(%d)", dbName, table, columns, rows))
			expected[table] += uint64(rows)
		}
	}
	insertRows(0, 100)
	insertRows(100, 100)
	fullExpected := make(map[string]uint64, len(expected))
	for table, count := range expected {
		fullExpected[table] = count
	}

	// b. streaming full backup: nothing stays locally, everything is on remote
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup %s create_remote --streaming --tables='%s.*' %s", configArg, dbName, fullBackup))
	env.assertStreamingBackupNotLocal(r, configArg, fullBackup)
	env.assertRemoteBackupTables(r, configFile, configArg, fullBackup, "", dbName, tables)

	// c. streaming increment
	insertRows(200, 50)
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup %s create_remote --streaming --diff-from-remote=%s --tables='%s.*' %s", configArg, fullBackup, dbName, incrementBackup))
	env.assertStreamingBackupNotLocal(r, configArg, incrementBackup)
	env.assertRemoteBackupTables(r, configFile, configArg, incrementBackup, fullBackup, dbName, tables)
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup %s list remote", configArg))
	r.NoError(err, "%s\nunexpected list remote error: %v", out, err)
	r.Regexp("(?m)^"+fullBackup+`\s`, out)
	r.Regexp("(?m)^"+incrementBackup+`\s`, out)

	// d. streaming restore of the increment after everything is dropped
	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup %s restore_remote --streaming --tables='%s.*' %s", configArg, dbName, incrementBackup))
	env.assertStreamingBackupNotLocal(r, configArg, incrementBackup)
	env.assertStreamingBackupNotLocal(r, configArg, fullBackup)
	for table, count := range expected {
		env.checkCount(r, 1, count, fmt.Sprintf("SELECT count() FROM `%s`.`%s`", dbName, table))
		var maxId uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&maxId, fmt.Sprintf("SELECT max(id) FROM `%s`.`%s`", dbName, table)))
		r.Equal(count-1, maxId, "unexpected max(id) in %s.%s", dbName, table)
	}

	// e. --tables filtering on streaming restore: only `mt` from the full backup
	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup %s restore_remote --streaming --tables='%s.mt' %s", configArg, dbName, fullBackup))
	env.assertStreamingBackupNotLocal(r, configArg, fullBackup)
	env.checkCount(r, 1, 1, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s' SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName))
	env.checkCount(r, 1, fullExpected["mt"], fmt.Sprintf("SELECT count() FROM `%s`.`mt`", dbName))

	// f. non-streaming regression guard: without the flag the local backup is kept and `delete local` still works
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup %s create_remote --tables='%s.*' %s", configArg, dbName, plainBackup))
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/"+plainBackup+"/metadata.json")
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup %s list local", configArg))
	r.NoError(err, "%s\nunexpected list local error: %v", out, err)
	r.Regexp("(?m)^"+plainBackup+`\s`, out)
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup %s delete local %s", configArg, plainBackup))
	env.assertStreamingBackupNotLocal(r, configArg, plainBackup)

	// g. cleanup
	fullCleanup(t, r, env, backupNames, []string{"remote"}, []string{dbName}, false, true, true, configFile)
	env.checkObjectStorageIsEmpty(t, r, "S3", configFile)
}

// assertStreamingBackupNotLocal checks that no local copy of backupName is left behind
func (env *TestEnvironment) assertStreamingBackupNotLocal(r *require.Assertions, configArg, backupName string) {
	r.Error(env.DockerExec("clickhouse-backup", "ls", "/var/lib/clickhouse/backup/"+backupName), "local backup dir %s shall not exist", backupName)
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup %s list local", configArg))
	r.NoError(err, "%s\nunexpected list local error: %v", out, err)
	r.NotContains(out, backupName)
}

// assertRemoteBackupTables checks the remote metadata.json lists exactly the streamed tables and the expected required_backup
func (env *TestEnvironment) assertRemoteBackupTables(r *require.Assertions, configFile, configArg, backupName, requiredBackup, dbName string, tables map[string]string) {
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("clickhouse-backup %s list remote", configArg))
	r.NoError(err, "%s\nunexpected list remote error: %v", out, err)
	r.Regexp("(?m)^"+backupName+`\s`, out)

	// minio stores objects as directories with xl.meta, read via `mc cat` like namedCollections_test.go
	const mcAliasCmd = "mc alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null 2>&1"
	cfgPath, _ := env.resolveConfigPaths(r, configFile)
	metadataPath := "local/clickhouse/" + cfgPath + "/" + backupName + "/metadata.json"
	out, err = env.DockerExecOut("minio", "bash", "-c", mcAliasCmd+" && mc cat "+metadataPath)
	r.NoError(err, "%s\ncan't read %s: %v", out, metadataPath, err)
	var backupMeta struct {
		RequiredBackup string `json:"required_backup"`
		Tables         []struct {
			Database string `json:"database"`
			Table    string `json:"table"`
		} `json:"tables"`
	}
	r.NoError(json.Unmarshal([]byte(strings.TrimSpace(out)), &backupMeta), "invalid metadata.json: %s", out)
	r.Equal(requiredBackup, backupMeta.RequiredBackup)
	r.Len(backupMeta.Tables, len(tables), "unexpected tables in %s metadata.json: %+v", backupName, backupMeta.Tables)
	for _, tableTitle := range backupMeta.Tables {
		r.Equal(dbName, tableTitle.Database)
		r.Contains(tables, tableTitle.Table)
	}
}
