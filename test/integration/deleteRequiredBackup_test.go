//go:build integration

package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDeleteRequiredBackup - `delete local|remote <backup_name>` must refuse to break a `required_backup`
// chain, `--force` must keep the legacy chain-breaking behavior and `general.rebase_during_delete: true`
// must rebase the dependent increments first, see https://github.com/Altinity/clickhouse-backup/issues/1493
func TestDeleteRequiredBackup(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	configFile := "/etc/clickhouse-backup/config-s3.yml"
	dbName := "test_delete_required"
	fullBackup := dbName + "_full"
	incBackup := dbName + "_inc"

	// deferred (not t.Cleanup) so it unwinds while the ClickHouse connection is still open,
	// the pooled env is returned to the pool only after Cleanup
	cleanupScenario := func() {
		for _, b := range []string{incBackup, fullBackup} {
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c "+configFile+" delete --force remote "+b+" 2>/dev/null || true")
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c "+configFile+" delete --force local "+b+" 2>/dev/null || true")
		}
		r.NoError(env.dropDatabase(dbName, true))
	}
	defer cleanupScenario()
	cleanupScenario()

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t1 (id UInt64) ENGINE=MergeTree() ORDER BY id")
	// keep part names stable between backups, so unchanged parts get the `required` attribute in the increment
	env.queryWithNoError(t, r, "SYSTEM STOP MERGES "+dbName+".t1")

	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t1 SELECT number FROM numbers(100)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "create_remote", "--tables="+dbName+".*", fullBackup)
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t1 SELECT number+100 FROM numbers(100)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "create_remote", "--tables="+dbName+".*", "--diff-from-remote="+fullBackup, incBackup)

	// `create --diff-from-remote` keeps `required_backup` in the local metadata too
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", fullBackup)
	r.Error(err, "delete local %s expected error, got: %s", fullBackup, out)
	r.Contains(out, "is required by local backup(s) "+incBackup)
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "list", "local")
	r.NoError(err, "list local: %s", out)
	r.Contains(out, fullBackup, "the refused `delete local` must keep the backup")

	// --force breaks the chain on purpose
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "--force", "local", fullBackup)
	// the increment is the last backup of the chain, nothing depends on it
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", incBackup)

	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "remote", fullBackup)
	r.Error(err, "delete remote %s expected error, got: %s", fullBackup, out)
	r.Contains(out, "is required by remote backup(s) "+incBackup)
	r.Contains(findRemoteBackup(t, r, env, configFile, incBackup).RequiredBackup, fullBackup, "the refused `delete remote` must keep `required_backup`")

	// rebase_during_delete rebases the dependent increment instead of refusing
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "REBASE_DURING_DELETE=true clickhouse-backup -c "+configFile+" delete remote "+fullBackup)
	r.NoError(err, "delete remote %s with REBASE_DURING_DELETE=true: %s", fullBackup, out)
	r.Contains(out, "rebase dependent backup before delete")
	r.Empty(findRemoteBackup(t, r, env, configFile, incBackup).RequiredBackup, "expected empty required_backup after rebase during delete")

	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "list", "remote")
	r.NoError(err, "list remote: %s", out)
	r.NotContains(out, fullBackup)

	// the rebased increment must stay restorable without its ancestor
	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
		dropSuffix = " NO DELAY"
	}
	env.queryWithNoError(t, r, "DROP TABLE "+dbName+".t1"+dropSuffix)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "restore_remote", "--rm", incBackup)
	env.checkCount(r, 1, 200, "SELECT count() FROM "+dbName+".t1")
}

// findRemoteBackup - read one backup entry from `list remote --format=json`, json avoids scraping the
// human `text` table, whose tabwriter output can be spliced mid-row by a concurrent zerolog write
func findRemoteBackup(t *testing.T, r *require.Assertions, env *TestEnvironment, configFile, backupName string) remoteBackupJSON {
	t.Helper()
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "list", "remote", "--format=json")
	r.NoError(err, "list remote --format=json: %s", out)
	// `out` also contains clickhouse-backup's own log lines around the JSON payload,
	// the JSON array itself is written as a single line via fmt.Fprintln
	jsonLine := ""
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "[") {
			jsonLine = line
			break
		}
	}
	r.NotEmptyf(jsonLine, "no JSON array found in `list remote --format=json` output: %s", out)
	var remoteBackups []remoteBackupJSON
	r.NoError(json.Unmarshal([]byte(jsonLine), &remoteBackups), "list remote --format=json: %s", out)
	for i := range remoteBackups {
		if remoteBackups[i].BackupName == backupName {
			return remoteBackups[i]
		}
	}
	r.FailNowf("backup not found", "%s not found in `list remote --format=json` output: %s", backupName, out)
	return remoteBackupJSON{}
}

type remoteBackupJSON struct {
	BackupName     string `json:"BackupName"`
	RequiredBackup string `json:"RequiredBackup"`
}
