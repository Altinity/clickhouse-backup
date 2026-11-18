//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// TestRBACInactiveLocalDirectory - `*.sql` left in the access path after a switch from `local_directory`
// to `replicated` user directory are not read by clickhouse-server, they shall not be backed up,
// otherwise restore writes every RBAC object twice, a backup which already contains them
// shall be restored from the keeper dump only,
// https://github.com/Altinity/clickhouse-backup/issues/1666
func TestRBACInactiveLocalDirectory(t *testing.T) {
	chVersion := os.Getenv("CLICKHOUSE_VERSION")
	// `replicated` user directory is available from 21.9, the integration env is replicated-only since then
	if compareVersion(chVersion, "21.9") < 0 {
		t.Skipf("Test skipped, `replicated` user_directories not available for %s version", chVersion)
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)

	config := "/etc/clickhouse-backup/config-s3.yml"
	const backupName = "test_rbac_inactive_local"
	const accessPath = "/var/lib/clickhouse/access"
	// system.zookeeper path is relative to the <zookeeper><root> chroot, so it works for both env shapes
	const rbacUuidNodesQuery = "SELECT count() AS cnt FROM system.zookeeper WHERE path='/clickhouse/access/uuid' AND value LIKE '%test.rbac-name%' SETTINGS empty_result_for_aggregation_by_empty_set=0"
	rbacNames := map[string]string{
		"PROFILES": "test.rbac-name",
		"QUOTAS":   "test.rbac-name",
		"POLICIES": "`test.rbac-name` ON test_rbac.test_rbac",
		"ROLES":    "test.rbac-name",
		"USERS":    "test.rbac-name",
	}
	dropRBACQueries := []string{
		"DROP SETTINGS PROFILE IF EXISTS `test.rbac-name`",
		"DROP QUOTA IF EXISTS `test.rbac-name`",
		"DROP ROW POLICY IF EXISTS `test.rbac-name` ON test_rbac.test_rbac",
		"DROP ROLE IF EXISTS `test.rbac-name`",
		"DROP USER IF EXISTS `test.rbac-name`",
	}
	inactiveSQLFiles := make([]string, 0)

	// t.Cleanup runs after the env is returned to the shared pool, drop leftovers here,
	// a polluted env breaks later tests which run in the same container
	defer func() {
		if !env.ch.IsOpen {
			if err := env.connect(t, "60s"); err != nil {
				log.Warn().Msgf("TestRBACInactiveLocalDirectory cleanup connect error: %v", err)
				return
			}
		}
		for _, q := range append(dropRBACQueries, "DROP TABLE IF EXISTS test_rbac.test_rbac SYNC", "DROP DATABASE IF EXISTS test_rbac SYNC") {
			if err := env.ch.Query(q); err != nil {
				log.Warn().Msgf("TestRBACInactiveLocalDirectory cleanup query %q error: %v", q, err)
			}
		}
		if len(inactiveSQLFiles) > 0 {
			if err := env.DockerExec("clickhouse", append([]string{"rm", "-fv"}, inactiveSQLFiles...)...); err != nil {
				log.Warn().Msgf("TestRBACInactiveLocalDirectory cleanup rm %v error: %v", inactiveSQLFiles, err)
			}
		}
		if err := env.DockerExec("clickhouse-backup", "bash", "-c", "CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup delete local "+backupName); err != nil {
			log.Warn().Msgf("TestRBACInactiveLocalDirectory cleanup delete local %s error: %v", backupName, err)
		}
	}()

	env.connectWithWait(t, r, 1*time.Second, 1*time.Second, 1*time.Minute)

	// showRBACContains - poll `SHOW <type>` until it contains (or, when expected is false, no longer contains) the object
	showRBACContains := func(rbacType, expectedValue string, expected bool, timeout time.Duration) bool {
		deadline := time.Now().Add(timeout)
		for {
			var rbacRows []struct {
				Name string `ch:"name"`
			}
			err := env.ch.Select(&rbacRows, fmt.Sprintf("SHOW %s", rbacType))
			if err != nil {
				log.Warn().Msgf("SHOW %s error: %v", rbacType, err)
			}
			found := false
			for _, row := range rbacRows {
				if row.Name == expectedValue {
					found = true
					break
				}
			}
			if found == expected || time.Now().After(deadline) {
				if found != expected {
					log.Warn().Msgf("SHOW %s = %#v, expect contains %s = %v", rbacType, rbacRows, expectedValue, expected)
				}
				return found
			}
			time.Sleep(1 * time.Second)
		}
	}
	checkAllRBACObjects := func(expected bool, timeout time.Duration) {
		for rbacType, expectedValue := range rbacNames {
			r.Equalf(expected, showRBACContains(rbacType, expectedValue, expected, timeout), "SHOW %s contains %s shall be %v", rbacType, expectedValue, expected)
		}
	}
	// ClickHouse `<replicated>` access storage has a race, see TestRBAC createRBACQuery for details
	createRBACQuery := func(query string) {
		var err error
		for attempt := 1; attempt <= 10; attempt++ {
			if err = env.ch.Query(query); err == nil {
				return
			}
			if !strings.Contains(err.Error(), "code: 511") && !strings.Contains(err.Error(), "code: 180") {
				break
			}
			log.Warn().Msgf("createRBACQuery(%s) attempt %d failed: %v, retrying", query, attempt, err)
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
		r.NoError(err)
	}

	log.Debug().Msg("prepare test_rbac database and RBAC objects in the replicated-only user directory")
	r.NoError(env.dropDatabase("test_rbac", true))
	env.queryWithNoError(t, r, "CREATE DATABASE test_rbac")
	env.queryWithNoError(t, r, "CREATE TABLE test_rbac.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")
	for _, q := range dropRBACQueries {
		env.queryWithNoError(t, r, q)
	}
	// drop *.sql leftovers from previous tests in the shared env, only the files created below shall be reported as skipped
	env.DockerExecNoError(r, "clickhouse", "bash", "-c", "rm -fv "+accessPath+"/*.sql "+accessPath+"/*.list")
	createRBACQuery("CREATE SETTINGS PROFILE `test.rbac-name` SETTINGS max_execution_time=60")
	createRBACQuery("CREATE ROLE `test.rbac-name` SETTINGS PROFILE `test.rbac-name`")
	createRBACQuery("CREATE USER `test.rbac-name` IDENTIFIED BY 'inactive_password' DEFAULT ROLE `test.rbac-name`")
	createRBACQuery("CREATE QUOTA `test.rbac-name` KEYED BY user_name FOR INTERVAL 1 hour NO LIMITS TO `test.rbac-name`")
	createRBACQuery("CREATE ROW POLICY `test.rbac-name` ON test_rbac.test_rbac USING v>=0 AS RESTRICTIVE TO `test.rbac-name`")

	log.Debug().Msg("leave the same RBAC objects with other uuids in the access path, as a switch from local_directory to replicated does")
	var liveObjects []struct {
		Value string `ch:"value"`
	}
	r.NoError(env.ch.Select(&liveObjects, "SELECT value FROM system.zookeeper WHERE path='/clickhouse/access/uuid' AND value LIKE '%test.rbac-name%'"))
	r.Len(liveObjects, len(rbacNames))
	for _, liveObject := range liveObjects {
		sqlFile := accessPath + "/" + uuid.NewString() + ".sql"
		env.DockerExecNoError(r, "clickhouse", "bash", "-c", fmt.Sprintf("cat > %s <<'EOT'\n%sEOT\nchown clickhouse:clickhouse %s", sqlFile, liveObject.Value, sqlFile))
		inactiveSQLFiles = append(inactiveSQLFiles, sqlFile)
	}
	// the live user differs from the inactive copy
	env.queryWithNoError(t, r, "ALTER USER `test.rbac-name` IDENTIFIED BY 'live_password'")

	log.Debug().Msg("backup RBAC, inactive *.sql shall be skipped")
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup create --rbac --rbac-only "+backupName)
	log.Debug().Msg(out)
	r.NoError(err, "%s\nunexpected create --rbac-only error: %v", out, err)
	r.Contains(out, fmt.Sprintf("skip backup of %d RBAC *.sql files from %s", len(inactiveSQLFiles), accessPath))
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls -1 /var/lib/clickhouse/backup/"+backupName+"/access/")
	r.NoError(err, out)
	log.Debug().Msgf("%s access content: %s", backupName, out)
	r.Contains(out, ".jsonl")
	r.NotContains(out, ".sql")

	// restoreRBACOnce - drop RBAC objects, restore them from the backup, every object shall be restored once from the keeper dump
	restoreRBACOnce := func() string {
		for _, q := range dropRBACQueries {
			env.queryWithNoError(t, r, q)
		}
		checkAllRBACObjects(false, 30*time.Second)

		// config-s3.yml `restart_command` ends with `sql:SYSTEM SHUTDOWN`, replace it with a no-op,
		// ReplicatedAccessStorage keeper watches shall apply the change without restart
		restoreOut, restoreErr := env.DockerExecOut("clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_RESTART_COMMAND='exec:true' clickhouse-backup -c "+config+" restore --rbac-only "+backupName)
		log.Debug().Msg(restoreOut)
		r.NoError(restoreErr, "%s\nunexpected RBAC error: %v", restoreOut, restoreErr)
		r.Contains(restoreOut, "RBAC successfully restored")
		checkAllRBACObjects(true, 60*time.Second)

		var uuidNodes uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&uuidNodes, rbacUuidNodesQuery))
		r.Equal(uint64(len(rbacNames)), uuidNodes, "expect %d /clickhouse/access/uuid znodes, one per restored RBAC object", len(rbacNames))

		// ReplicatedAccessStorage refreshes asynchronously, retry with backoff
		var loginOut string
		var loginErr error
		for attempt := 1; attempt <= 10; attempt++ {
			loginOut, loginErr = env.DockerExecOut("clickhouse", "clickhouse-client", "--user", "test.rbac-name", "--password", "live_password", "-q", "SELECT 1")
			if loginErr == nil {
				break
			}
			log.Warn().Msgf("login test.rbac-name attempt %d failed: %v, output: %s, retrying", attempt, loginErr, loginOut)
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		r.NoError(loginErr, "%s\nexpect login with live_password success: %v", loginOut, loginErr)
		loginOut, loginErr = env.DockerExecOut("clickhouse", "clickhouse-client", "--user", "test.rbac-name", "--password", "inactive_password", "-q", "SELECT 1")
		r.Error(loginErr, "%s\nexpect login with inactive_password fail, user shall not be restored from inactive *.sql", loginOut)
		return restoreOut
	}

	log.Debug().Msg("restore RBAC from the backup without inactive *.sql")
	restoreRBACOnce()

	log.Debug().Msg("restore RBAC from the backup with inactive *.sql next to the keeper dump, as create stored it before")
	env.DockerExecNoError(r, "clickhouse-backup", append(append([]string{"cp", "-v"}, inactiveSQLFiles...), "/var/lib/clickhouse/backup/"+backupName+"/access/")...)
	out = restoreRBACOnce()
	r.Contains(out, "will restore only keeper dump copy")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "delete", "local", backupName)
	env.DockerExecNoError(r, "clickhouse", append([]string{"rm", "-fv"}, inactiveSQLFiles...)...)
	for _, q := range dropRBACQueries {
		env.queryWithNoError(t, r, q)
	}
	env.queryWithNoError(t, r, "DROP TABLE IF EXISTS test_rbac.test_rbac SYNC")
	r.NoError(env.dropDatabase("test_rbac", true))
}
