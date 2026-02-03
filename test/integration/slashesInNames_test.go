//go:build integration

package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
)

// https://github.com/Altinity/clickhouse-backup/issues/1151
func TestSlashesInDatabaseAndTableNamesAndTableQuery(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") < 0 {
		t.Skipf("version %s is too old for this test", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	version, err := env.ch.GetVersion(t.Context())
	r.NoError(err)

	dbName := `db\db2/db3`
	tableName := `z\z2/z3`
	createSchemaSQL := "(`s`" + ` String DEFAULT replaceRegexpAll('test', '(\\\\=|\\\\\\\\)', '\\\\\\\\\\\\1')) ENGINE = MergeTree ORDER BY s`
	createTableSQL := fmt.Sprintf("CREATE TABLE `%s`.`%s` "+createSchemaSQL, dbName, tableName)
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, createTableSQL)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create", "--env", "CLICKHOUSE_HOST=clickhouse", "--tables", dbName+".*", t.Name())

	backupTableFile := fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata/%s/%s.json", t.Name(), common.TablePathEncode(dbName), common.TablePathEncode(tableName))
	backupContent, err := env.DockerExecOut("clickhouse-backup", "cat", backupTableFile)
	r.NoError(err)
	escapedCreateTableSQL := fmt.Sprintf("CREATE TABLE `%s`.`%s`", strings.ReplaceAll(dbName, `\`, `\\`), strings.ReplaceAll(tableName, `\`, `\\`))
	escapedSchemaSQL := strings.ReplaceAll(createSchemaSQL, `\`, `\\`)
	r.Contains(backupContent, escapedCreateTableSQL)
	r.Contains(backupContent, escapedSchemaSQL)

	r.NoError(env.ch.DropOrDetachTable(clickhouse.Table{Database: dbName, Name: tableName, CreateTableQuery: createTableSQL}, createTableSQL, "", false, version, "", false, ""))
	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore", "--config", "/etc/clickhouse-backup/config-s3.yml", "--tables", dbName+".*", t.Name())
	restoredSQL := ""
	r.NoError(env.ch.SelectSingleRow(t.Context(), &restoredSQL, fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", dbName, tableName)))

	r.Contains(restoredSQL, escapedCreateTableSQL)
	//SHOW CREATE SQL transform original query
	restoredSQL = regexp.MustCompile(`[\n\s]+`).ReplaceAllString(restoredSQL, " ")
	restoredSQL = regexp.MustCompile(`\(\s+`).ReplaceAllString(restoredSQL, "(")
	restoredSQL = regexp.MustCompile(`\s+\)`).ReplaceAllString(restoredSQL, ")")
	r.Contains(restoredSQL, createSchemaSQL)

	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "--env", "CLICKHOUSE_HOST=clickhouse", t.Name())
	env.Cleanup(t, r)
}
