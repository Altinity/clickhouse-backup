//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

const localUserDirectoriesConfig = `<yandex>
  <user_directories replace="replace">
    <users_xml>
      <path>users.xml</path>
    </users_xml>
    <local_directory>
      <path>/var/lib/clickhouse/access/</path>
    </local_directory>
  </user_directories>
</yandex>
`

// TestRBACCrossUserDirectories - restore RBAC objects backed up from a `replicated` user directory
// into a server which has only a `local_directory` one and vice versa,
// https://github.com/Altinity/clickhouse-backup/issues/881
func TestRBACCrossUserDirectories(t *testing.T) {
	chVersion := os.Getenv("CLICKHOUSE_VERSION")
	// `replicated` user directory is available from 21.9, the integration env is replicated-only since then
	if compareVersion(chVersion, "21.9") < 0 {
		t.Skipf("Test skipped, `replicated` user_directories not available for %s version", chVersion)
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)

	config := "/etc/clickhouse-backup/config-s3.yml"
	const localConfigFile = "/etc/clickhouse-server/config.d/zz_local_user_directories.xml"
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

	// teardown must return the container to the stock replicated-only shape,
	// t.Cleanup runs after the env is returned to the shared pool so use docker exec / defer here
	defer func() {
		if err := env.DockerExec("clickhouse", "rm", "-fv", localConfigFile); err != nil {
			log.Warn().Msgf("TestRBACCrossUserDirectories cleanup rm %s error: %v", localConfigFile, err)
		}
		env.ch.Close()
		if err := env.tc.RestartContainer(t, "clickhouse"); err != nil {
			log.Warn().Msgf("TestRBACCrossUserDirectories cleanup restart error: %v", err)
		}
		if err := env.connect(t, "60s"); err != nil {
			log.Warn().Msgf("TestRBACCrossUserDirectories cleanup connect error: %v", err)
			return
		}
		for _, q := range append(dropRBACQueries, "DROP TABLE IF EXISTS test_rbac.test_rbac SYNC", "DROP DATABASE IF EXISTS test_rbac SYNC") {
			if err := env.ch.Query(q); err != nil {
				log.Warn().Msgf("TestRBACCrossUserDirectories cleanup query %q error: %v", q, err)
			}
		}
		for _, backupName := range []string{"test_rbac_replicated", "test_rbac_local"} {
			if err := env.DockerExec("clickhouse-backup", "bash", "-c", "CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup delete local "+backupName); err != nil {
				log.Warn().Msgf("TestRBACCrossUserDirectories cleanup delete local %s error: %v", backupName, err)
			}
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
	// ClickHouse `<replicated>` access storage has a race, a freshly created role/profile can be
	// transiently evicted from the in-memory cache, so resolving it fails with UNKNOWN_ROLE (code 511), retry
	createRBACQuery := func(query string) {
		var err error
		for attempt := 1; attempt <= 10; attempt++ {
			if err = env.ch.Query(query); err == nil {
				return
			}
			if !strings.Contains(err.Error(), "code: 511") {
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
	// drop *.sql leftovers from previous tests in the shared env, the backup below shall contain only *.jsonl
	env.DockerExecNoError(r, "clickhouse", "bash", "-c", "rm -fv /var/lib/clickhouse/access/*.sql /var/lib/clickhouse/access/*.list")
	createRBACQuery("CREATE SETTINGS PROFILE `test.rbac-name` SETTINGS max_execution_time=60")
	createRBACQuery("CREATE ROLE `test.rbac-name` SETTINGS PROFILE `test.rbac-name`")
	createRBACQuery("CREATE USER `test.rbac-name` IDENTIFIED BY 'test_rbac_password' DEFAULT ROLE `test.rbac-name`")
	createRBACQuery("CREATE QUOTA `test.rbac-name` KEYED BY user_name FOR INTERVAL 1 hour NO LIMITS TO `test.rbac-name`")
	createRBACQuery("CREATE ROW POLICY `test.rbac-name` ON test_rbac.test_rbac USING v>=0 AS RESTRICTIVE TO `test.rbac-name`")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup create --rbac --rbac-only test_rbac_replicated")
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls -1 /var/lib/clickhouse/backup/test_rbac_replicated/access/")
	r.NoError(err, out)
	log.Debug().Msgf("test_rbac_replicated access content: %s", out)
	r.Contains(out, ".jsonl")
	r.NotContains(out, ".sql")

	log.Debug().Msg("switch clickhouse-server to the local_directory only user directory")
	env.DockerExecNoError(r, "clickhouse", "bash", "-c", fmt.Sprintf("cat > %s <<'EOT'\n%sEOT", localConfigFile, localUserDirectoriesConfig))
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 2*time.Second, 2*time.Second, 1*time.Minute)
	env.DockerExecNoError(r, "clickhouse", "clickhouse-client", "-q", "SELECT * FROM system.user_directories FORMAT Vertical")
	// the objects still live in keeper which is no longer read by clickhouse-server
	checkAllRBACObjects(false, 10*time.Second)

	log.Debug().Msg("restore replicated RBAC backup into the local_directory only user directory")
	// config-s3.yml `restart_command` ends with `sql:SYSTEM SHUTDOWN`, replace it with a no-op,
	// the container restart is done explicitly below
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_RESTART_COMMAND='exec:true' clickhouse-backup -c "+config+" restore --rbac-only test_rbac_replicated")
	log.Debug().Msg(out)
	r.NoError(err, "%s\nunexpected RBAC error: %v", out, err)
	r.Contains(out, "RBAC successfully restored")
	r.Contains(out, "replicated RBAC objects")

	out, err = env.DockerExecOut("clickhouse", "bash", "-c", "ls -1 /var/lib/clickhouse/access/*.sql | wc -l")
	r.NoError(err, out)
	r.Equal("5", strings.TrimSpace(out), "expect 5 *.sql files converted from the keeper dump, got: %s", out)

	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 2*time.Second, 2*time.Second, 1*time.Minute)
	checkAllRBACObjects(true, 30*time.Second)

	log.Debug().Msg("backup RBAC from the local_directory only user directory")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup create --rbac --rbac-only test_rbac_local")
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls -1 /var/lib/clickhouse/backup/test_rbac_local/access/")
	r.NoError(err, out)
	log.Debug().Msgf("test_rbac_local access content: %s", out)
	r.Contains(out, ".sql")
	r.NotContains(out, ".jsonl")

	// drop the local copies, the backup is already created
	for _, q := range dropRBACQueries {
		env.queryWithNoError(t, r, q)
	}

	log.Debug().Msg("switch clickhouse-server back to the replicated only user directory")
	env.DockerExecNoError(r, "clickhouse", "rm", "-fv", localConfigFile)
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 2*time.Second, 2*time.Second, 1*time.Minute)
	env.DockerExecNoError(r, "clickhouse", "clickhouse-client", "-q", "SELECT * FROM system.user_directories FORMAT Vertical")
	// the objects created in the very first step are still in keeper, drop them to restore without conflicts
	for _, q := range dropRBACQueries {
		env.queryWithNoError(t, r, q)
	}
	checkAllRBACObjects(false, 30*time.Second)

	log.Debug().Msg("restore local RBAC backup into the replicated only user directory")
	// no restart and no `SYSTEM RELOAD USERS` here on purpose, ReplicatedAccessStorage keeper watches shall apply the change
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_RESTART_COMMAND='exec:true' clickhouse-backup -c "+config+" restore --rbac-only test_rbac_local")
	log.Debug().Msg(out)
	r.NoError(err, "%s\nunexpected RBAC error: %v", out, err)
	r.Contains(out, "RBAC successfully restored")
	r.Contains(out, "local RBAC *.sql objects")

	// ReplicatedAccessStorage watches keeper, so no restart is required here
	checkAllRBACObjects(true, 60*time.Second)

	// system.zookeeper path is relative to the <zookeeper><root> chroot, so it works for both env shapes
	var uuidNodes uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&uuidNodes, "SELECT count() AS cnt FROM system.zookeeper WHERE path='/clickhouse/access/uuid' SETTINGS empty_result_for_aggregation_by_empty_set=0"))
	r.Equal(uint64(5), uuidNodes, "expect 5 /clickhouse/access/uuid znodes converted from backup *.sql")
}
