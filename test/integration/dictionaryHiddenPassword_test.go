//go:build integration

package main

import (
	"os"
	"testing"
	"time"
)

// TestDictionaryHiddenPassword covers https://github.com/Altinity/clickhouse-backup/issues/1537
// `system.tables.create_table_query` masks dictionary source credentials as '[HIDDEN]' (23.3+),
// clickhouse-backup must read the real definition from the metadata `.sql` file on disk,
// the path was wrong when `tables`/API called getMetadataPath before GetVersion or when
// system.build_options is not accessible, so backup metadata contained literal '[HIDDEN]'.
func TestDictionaryHiddenPassword(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") < 0 {
		t.Skipf("'[HIDDEN]' masking in create_table_query not exists in version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	dbName := "test_dict_hidden_" + t.Name()
	backupName := "test_dict_hidden_" + t.Name()
	// user `backup` with real password from configs/backup-user.xml, so dictionary really loads after restore
	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`")
	env.queryWithNoError(t, r, "CREATE TABLE `"+dbName+"`.src (id UInt64, v String) ENGINE=MergeTree ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO `"+dbName+"`.src SELECT number, toString(number) FROM numbers(100)")
	env.queryWithNoError(t, r, "CREATE DICTIONARY `"+dbName+"`.dict (id UInt64, v String) PRIMARY KEY id "+
		"SOURCE(CLICKHOUSE(host 'localhost' port 9000 db '"+dbName+"' table 'src' user 'backup' password 'meow=& 123?*%# МЯУ')) "+
		"LAYOUT(HASHED()) LIFETIME(0)")

	var createQuery string
	r.NoError(env.ch.SelectSingleRowNoCtx(&createQuery, "SELECT create_table_query FROM system.tables WHERE database='"+dbName+"' AND name='dict'"))
	r.Contains(createQuery, "[HIDDEN]", "expected masked password in system.tables: %s", createQuery)

	// `tables` command resolves metadata path before version is known
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", "--tables", dbName+".*")
	r.NoError(err, "%s\nunexpected tables error: %v", out, err)
	r.NotContains(out, "can't read", "metadata .sql path resolved wrong: %s", out)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+dbName+".*", backupName)
	out, err = env.DockerExecOut("clickhouse-backup", "cat", "/var/lib/clickhouse/backup/"+backupName+"/metadata/"+dbName+"/dict.json")
	r.NoError(err, "%s\ncan't read dictionary metadata: %v", out, err)
	r.NotContains(out, "[HIDDEN]", "backup metadata contains masked password: %s", out)
	r.Contains(out, "meow=", "backup metadata doesn't contain real password: %s", out)

	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", backupName)

	var cnt uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&cnt, "SELECT count() FROM `"+dbName+"`.dict"))
	r.Equal(uint64(100), cnt)

	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
}
