//go:build integration

package main

import (
	"encoding/json"
	"os"
	"path"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
)

// TestDiffFromRemoteMovedParts covers https://github.com/Altinity/clickhouse-backup/issues/1663:
// a part moved to another disk after the diff backup keeps name and hash_of_all_files. Between local disks it
// must be referenced as Required, to object and encrypted disks it must be uploaded again, because their part
// files in the backup are object metadata and ciphertext, and restore must return the same data in both cases.
func TestDiffFromRemoteMovedParts(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	if !isAdvancedMode() {
		t.Skip("requires advanced mode with storage policies")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.1") < 0 {
		t.Skip("requires ClickHouse >= 22.1 for mixed_disk_types storage policy")
	}
	dbName := "test_diff_moved_parts"
	tableName := "t"
	fullBackup := dbName + "_full"
	incrementBackup := dbName + "_increment"
	backupCmd := func(args ...string) {
		env.DockerExecNoError(r, "clickhouse-backup", append([]string{"clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml"}, args...)...)
	}
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+"."+tableName+" (id UInt64, v String) ENGINE=MergeTree() PARTITION BY id % 4 ORDER BY id SETTINGS storage_policy='mixed_disk_types'")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+"."+tableName+" SELECT number, toString(number) FROM numbers(1000)")
	backupCmd("create_remote", "--tables="+dbName+".*", fullBackup)

	env.queryWithNoError(t, r, "ALTER TABLE "+dbName+"."+tableName+" MOVE PARTITION 1 TO DISK 'hdd1'")
	env.queryWithNoError(t, r, "ALTER TABLE "+dbName+"."+tableName+" MOVE PARTITION 2 TO DISK 'disk_s3'")
	env.queryWithNoError(t, r, "ALTER TABLE "+dbName+"."+tableName+" MOVE PARTITION 3 TO DISK 'hdd3_encrypted'")
	backupCmd("create_remote", "--tables="+dbName+".*", "--diff-from-remote="+fullBackup, incrementBackup)

	metaPath := path.Join("/var/lib/clickhouse/backup", incrementBackup, "metadata", common.TablePathEncode(dbName), common.TablePathEncode(tableName)+".json")
	out, err := env.DockerExecOut("clickhouse-backup", "cat", metaPath)
	r.NoError(err, "cat %s: %s", metaPath, out)
	var tableMeta struct {
		Parts map[string][]struct {
			Name     string `json:"name"`
			Required bool   `json:"required,omitempty"`
		} `json:"parts"`
	}
	r.NoError(json.Unmarshal([]byte(out), &tableMeta))
	expectedRequired := map[string]bool{"default": true, "hdd1": true, "disk_s3": false, "hdd3_encrypted": false}
	for disk, required := range expectedRequired {
		r.NotEmptyf(tableMeta.Parts[disk], "no parts on disk %s, metadata=%s", disk, out)
		for _, p := range tableMeta.Parts[disk] {
			r.Equalf(required, p.Required, "disk %s part %s, metadata=%s", disk, p.Name, out)
		}
	}

	checkData := func() {
		env.checkCount(r, 1, 1000, "SELECT count() FROM "+dbName+"."+tableName+" WHERE toString(id)=v")
		env.checkCount(r, 1, 499500, "SELECT sum(id) AS `count()` FROM "+dbName+"."+tableName)
	}
	// restore local increment, required parts are hardlinked from local full backup or downloaded
	env.queryWithNoError(t, r, "DROP TABLE "+dbName+"."+tableName+" SYNC")
	backupCmd("restore", "--rm", incrementBackup)
	checkData()

	// restore increment downloaded from remote
	backupCmd("delete", "local", incrementBackup)
	backupCmd("delete", "local", fullBackup)
	backupCmd("restore_remote", "--rm", incrementBackup)
	checkData()

	for _, b := range []string{incrementBackup, fullBackup} {
		backupCmd("delete", "remote", b)
	}
	backupCmd("delete", "local", incrementBackup)
	r.NoError(env.dropDatabase(dbName, true))
}
