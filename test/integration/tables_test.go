//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestTablesCommand exercises `clickhouse-backup tables` flags introduced together with
// https://github.com/Altinity/clickhouse-backup/issues/1388 — `--local-backup`, `--remote-backup`,
// and `--format` — verifying per-table size/parts breakdown for both local and remote backups.
func TestTablesCommand(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	testBackupName := "test_backup_tables_cmd"
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameReplicated, dbNameMySQL, dbNamePostgreSQL, Issue331Issue1091Atomic, Issue331Issue1091Ordinary}
	dbNameAtomicTest := dbNameAtomic + "_" + t.Name()

	fullCleanup(t, r, env, []string{testBackupName}, []string{"remote", "local"}, databaseList, true, false, false, "config-s3.yml")
	generateTestData(t, r, env, "S3", false, defaultTestData())

	// Live tables -- the no-backup case still works.
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", "--tables", " "+dbNameAtomicTest+".*")
	r.NoError(err, "%s\nunexpected tables error: %v", out, err)
	r.Contains(out, dbNameAtomicTest)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create_remote", testBackupName)

	// Local backup, text format -- should include the new size/parts/disks columns.
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", "--local-backup", testBackupName)
	r.NoError(err, "%s\nunexpected tables --local-backup error: %v", out, err)
	r.Contains(out, dbNameAtomicTest)

	// Local backup, JSON format -- wrapped InfoResult with aggregate totals + tables[] array.
	// Logs go to stderr; isolate stdout so the JSON parse is not polluted by INFO lines.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --local-backup "+testBackupName+" --format json 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --local-backup --format=json error: %v", out, err)
	var localResult map[string]interface{}
	r.NoError(json.Unmarshal([]byte(out), &localResult), "json output is not parseable: %s", out)
	r.Equal(testBackupName, localResult["backup_name"], "backup_name mismatch: %v", localResult)
	r.Equal("local", localResult["backup_type"], "backup_type mismatch: %v", localResult)
	r.Contains(localResult, "total_bytes")
	r.Contains(localResult, "total_size")
	r.Contains(localResult, "total_parts")
	r.Contains(localResult, "table_count")
	localRowsRaw, ok := localResult["tables"].([]interface{})
	r.True(ok, "expected 'tables' array in InfoResult: %v", localResult)
	r.NotEmpty(localRowsRaw, "expected at least one table row for local backup")
	hasAtomicTest := false
	for _, raw := range localRowsRaw {
		row, _ := raw.(map[string]interface{})
		db, _ := row["database"].(string)
		tbl, _ := row["table"].(string)
		if db == dbNameAtomicTest {
			hasAtomicTest = true
			_, sizeOK := row["size"]
			_, partsOK := row["parts"]
			_, disksOK := row["disks"].([]interface{})
			r.True(sizeOK, "missing size for %s.%s in JSON output: %v", db, tbl, row)
			r.True(partsOK, "missing parts for %s.%s in JSON output: %v", db, tbl, row)
			r.True(disksOK, "disks should be array, got %T: %v", row["disks"], row)
		}
	}
	r.True(hasAtomicTest, "expected database %s in local-backup JSON output: %s", dbNameAtomicTest, out)

	// Local backup with --tables pattern -- pattern must be applied.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --local-backup "+testBackupName+" --tables '"+dbNameAtomicTest+".*' --format json 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --local-backup --tables error: %v", out, err)
	var localFiltered map[string]interface{}
	r.NoError(json.Unmarshal([]byte(out), &localFiltered))
	r.Equal(dbNameAtomicTest+".*", localFiltered["table_pattern"], "table_pattern should be echoed in result: %v", localFiltered)
	filteredRowsRaw, _ := localFiltered["tables"].([]interface{})
	for _, raw := range filteredRowsRaw {
		row, _ := raw.(map[string]interface{})
		db, _ := row["database"].(string)
		r.Equal(dbNameAtomicTest, db, "filtered output should only contain %s, got row=%v", dbNameAtomicTest, row)
	}

	// Local backup, CSV format -- header line must include size and parts columns.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --local-backup "+testBackupName+" --format csv 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --local-backup --format=csv error: %v", out, err)
	csvHead := strings.SplitN(out, "\n", 2)[0]
	r.Contains(csvHead, "size")
	r.Contains(csvHead, "parts")
	r.Contains(out, dbNameAtomicTest)

	// Remote backup, JSON format -- size/parts must come back too.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --remote-backup "+testBackupName+" --format json 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --remote-backup --format=json error: %v", out, err)
	var remoteResult map[string]interface{}
	r.NoError(json.Unmarshal([]byte(out), &remoteResult), "json output is not parseable: %s", out)
	r.Equal("remote", remoteResult["backup_type"], "backup_type mismatch: %v", remoteResult)
	remoteRowsRaw, ok := remoteResult["tables"].([]interface{})
	r.True(ok, "expected 'tables' array in InfoResult: %v", remoteResult)
	r.NotEmpty(remoteRowsRaw, "expected at least one table row for remote backup")
	hasAtomicTest = false
	for _, raw := range remoteRowsRaw {
		row, _ := raw.(map[string]interface{})
		db, _ := row["database"].(string)
		if db == dbNameAtomicTest {
			hasAtomicTest = true
			_, sizeOK := row["size"]
			_, partsOK := row["parts"]
			r.True(sizeOK, "missing size for remote row: %v", row)
			r.True(partsOK, "missing parts for remote row: %v", row)
		}
	}
	r.True(hasAtomicTest, "expected database %s in remote-backup JSON output: %s", dbNameAtomicTest, out)

	// Both --local-backup and --remote-backup -- JSON output is now an array of two InfoResults.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --local-backup "+testBackupName+" --remote-backup "+testBackupName+" --format json 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --local-backup --remote-backup --format=json error: %v", out, err)
	var allResults []map[string]interface{}
	r.NoError(json.Unmarshal([]byte(out), &allResults), "json output is not parseable: %s", out)
	r.Len(allResults, 2, "expected two InfoResult entries (local + remote), got: %s", out)
	types := []string{}
	for _, sec := range allResults {
		bt, _ := sec["backup_type"].(string)
		types = append(types, bt)
	}
	r.Contains(types, "local", "missing local section, types=%v", types)
	r.Contains(types, "remote", "missing remote section, types=%v", types)

	// Local backup, text format -- must include `Backup: name (local)` header and TOTAL row.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --local-backup "+testBackupName+" 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --local-backup text error: %v", out, err)
	r.Contains(out, "Backup:")
	r.Contains(out, "(local)")
	r.Contains(out, "TABLE")
	r.Contains(out, "TOTAL (")
	r.Contains(out, dbNameAtomicTest)

	// Unknown format must fail with a clear error.
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", "--local-backup", testBackupName, "--format", "bogus")
	r.Error(err, "expected error for unknown format, got: %s", out)
	r.Contains(strings.ToLower(out+fmt.Sprint(err)), "unknown format")

	fullCleanup(t, r, env, []string{testBackupName}, []string{"remote", "local"}, databaseList, true, true, true, "config-s3.yml")
	env.checkObjectStorageIsEmpty(t, r, "S3", "config-s3.yml")
}

// TestTablesCommandListParts exercises `clickhouse-backup tables --list-parts` (alias --parts)
// and `--partitions` (alias --list-partitions). The two flags are independent of each other and
// both work standalone against the live server, --local-backup, and --remote-backup alike.
// Against the live server, both are read straight from `system.parts`. Against
// --local-backup/--remote-backup, partition_id is derived from each part's name (the
// `_`-delimited prefix, same convention as filesystemhelper.IsPartInPartition), so the
// human-readable partition value and any size are only available against the live server.
func TestTablesCommandListParts(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	testBackupName := "test_backup_tables_list_parts"
	dbName := "test_list_parts_db_" + t.Name()
	tableName := "part_table"
	fullTableName := dbName + "." + tableName

	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE `%s`.`%s` (id UInt64, dt Date) ENGINE=MergeTree PARTITION BY toYYYYMM(dt) ORDER BY id", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT number, '2022-01-01' FROM numbers(10)", dbName, tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT number, '2022-02-01' FROM numbers(10)", dbName, tableName))

	findRow := func(rows []interface{}) map[string]interface{} {
		for _, raw := range rows {
			row, _ := raw.(map[string]interface{})
			if row["database"] == dbName {
				return row
			}
		}
		return nil
	}

	// Live, --list-parts alone -- one PartRow per physical part, partitions key absent.
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-c",
		"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --tables '"+fullTableName+"' --list-parts --format json 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --list-parts error: %v", out, err)
	var liveRows []interface{}
	r.NoError(json.Unmarshal([]byte(out), &liveRows), "json output is not parseable: %s", out)
	liveRow := findRow(liveRows)
	r.NotNil(liveRow, "expected row for %s in: %s", fullTableName, out)
	livePartsRaw, ok := liveRow["parts_list"].([]interface{})
	r.True(ok, "expected parts_list array: %v", liveRow)
	r.Len(livePartsRaw, 2, "expected 2 parts, got: %v", livePartsRaw)
	r.NotContains(liveRow, "partitions", "--list-parts alone must not attach partitions: %v", liveRow)
	for _, raw := range livePartsRaw {
		p, _ := raw.(map[string]interface{})
		r.NotEmpty(p["name"], "missing part name: %v", p)
		r.NotEmpty(p["partition_id"], "missing partition_id: %v", p)
		r.NotEmpty(p["size"], "missing size on live part: %v", p)
	}

	// Live, --partitions alone (alias --list-partitions) -- one PartitionRow per partition_id,
	// with partition/parts/size populated; parts_list key absent.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c",
		"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --tables '"+fullTableName+"' --list-partitions --format json 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --list-partitions error: %v", out, err)
	var livePartitionRows []interface{}
	r.NoError(json.Unmarshal([]byte(out), &livePartitionRows), "json output is not parseable: %s", out)
	livePartitionRow := findRow(livePartitionRows)
	r.NotNil(livePartitionRow, "expected row for %s in: %s", fullTableName, out)
	livePartitionsRaw, ok := livePartitionRow["partitions"].([]interface{})
	r.True(ok, "expected partitions array: %v", livePartitionRow)
	r.Len(livePartitionsRaw, 2, "expected 2 partitions, got: %v", livePartitionsRaw)
	r.NotContains(livePartitionRow, "parts_list", "--partitions alone must not attach parts_list: %v", livePartitionRow)
	for _, raw := range livePartitionsRaw {
		p, _ := raw.(map[string]interface{})
		r.NotEmpty(p["partition_id"], "missing partition_id: %v", p)
		r.NotEmpty(p["partition"], "missing partition: %v", p)
		r.EqualValues(1, p["parts"], "expected 1 part per partition: %v", p)
		r.NotEmpty(p["size"], "missing size on live partition: %v", p)
	}

	// Live, --parts --list-partitions (both aliases at once) -- both keys attached together.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c",
		"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --tables '"+fullTableName+"' --parts --list-partitions --format json 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --parts --list-partitions error: %v", out, err)
	var bothRows []interface{}
	r.NoError(json.Unmarshal([]byte(out), &bothRows), "json output is not parseable: %s", out)
	bothRow := findRow(bothRows)
	r.NotNil(bothRow, "expected row for %s in: %s", fullTableName, out)
	_, hasParts := bothRow["parts_list"].([]interface{})
	_, hasPartitions := bothRow["partitions"].([]interface{})
	r.True(hasParts, "expected parts_list when both flags set: %v", bothRow)
	r.True(hasPartitions, "expected partitions when both flags set: %v", bothRow)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create_remote", "--tables", fullTableName, testBackupName)

	// --local-backup, --list-parts -- partition_id derived from part names, no size available.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c",
		"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --local-backup "+testBackupName+" --list-parts --format json 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --local-backup --list-parts error: %v", out, err)
	var localPartsResult map[string]interface{}
	r.NoError(json.Unmarshal([]byte(out), &localPartsResult), "json output is not parseable: %s", out)
	localPartsRows, _ := localPartsResult["tables"].([]interface{})
	localPartsRow := findRow(localPartsRows)
	r.NotNil(localPartsRow, "expected row for %s in local backup: %s", fullTableName, out)
	localParts, _ := localPartsRow["parts_list"].([]interface{})
	r.Len(localParts, 2, "expected 2 parts from local backup metadata, got: %v", localParts)
	for _, raw := range localParts {
		p, _ := raw.(map[string]interface{})
		r.NotEmpty(p["name"], "missing part name: %v", p)
		r.NotEmpty(p["partition_id"], "missing partition_id: %v", p)
	}

	// --local-backup, --partitions -- aggregated from part names, no partition value/size.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c",
		"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --local-backup "+testBackupName+" --partitions --format json 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --local-backup --partitions error: %v", out, err)
	var localPartitionsResult map[string]interface{}
	r.NoError(json.Unmarshal([]byte(out), &localPartitionsResult), "json output is not parseable: %s", out)
	localPartitionsRows, _ := localPartitionsResult["tables"].([]interface{})
	localPartitionsRow := findRow(localPartitionsRows)
	r.NotNil(localPartitionsRow, "expected row for %s in local backup: %s", fullTableName, out)
	localPartitions, _ := localPartitionsRow["partitions"].([]interface{})
	r.Len(localPartitions, 2, "expected 2 partitions from local backup metadata, got: %v", localPartitions)
	for _, raw := range localPartitions {
		p, _ := raw.(map[string]interface{})
		r.NotEmpty(p["partition_id"], "missing partition_id: %v", p)
		r.EqualValues(1, p["parts"], "expected 1 part per partition: %v", p)
	}

	// --remote-backup, --list-parts and --partitions -- same behavior as --local-backup.
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c",
		"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml tables --remote-backup "+testBackupName+" --list-parts --partitions --format json 2>/dev/null")
	r.NoError(err, "%s\nunexpected tables --remote-backup --list-parts --partitions error: %v", out, err)
	var remoteResult map[string]interface{}
	r.NoError(json.Unmarshal([]byte(out), &remoteResult), "json output is not parseable: %s", out)
	remoteRows, _ := remoteResult["tables"].([]interface{})
	remoteRow := findRow(remoteRows)
	r.NotNil(remoteRow, "expected row for %s in remote backup: %s", fullTableName, out)
	remoteParts, _ := remoteRow["parts_list"].([]interface{})
	remotePartitions, _ := remoteRow["partitions"].([]interface{})
	r.Len(remoteParts, 2, "expected 2 parts from remote backup metadata, got: %v", remoteParts)
	r.Len(remotePartitions, 2, "expected 2 partitions from remote backup metadata, got: %v", remotePartitions)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", testBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", testBackupName)
	r.NoError(env.dropDatabase(dbName, true))
	env.checkObjectStorageIsEmpty(t, r, "S3", "config-s3.yml")
}
