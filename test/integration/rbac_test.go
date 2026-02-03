//go:build integration

package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

func TestRBAC(t *testing.T) {
	chVersion := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(chVersion, "20.4") < 0 {
		t.Skipf("Test skipped, RBAC not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)

	testRBACScenario := func(config string) {
		env.connectWithWait(t, r, 1*time.Second, 1*time.Second, 1*time.Minute)

		env.queryWithNoError(r, "CREATE DATABASE test_rbac")
		createTableSQL := "CREATE TABLE test_rbac.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()"
		env.queryWithNoError(r, createTableSQL)
		env.queryWithNoError(r, "INSERT INTO test_rbac.test_rbac SELECT number FROM numbers(10)")
		env.queryWithNoError(r, "DROP SETTINGS PROFILE IF EXISTS `test.rbac-name`")
		env.queryWithNoError(r, "DROP QUOTA IF EXISTS `test.rbac-name`")
		env.queryWithNoError(r, "DROP ROW POLICY IF EXISTS `test.rbac-name` ON test_rbac.test_rbac")
		env.queryWithNoError(r, "DROP ROLE IF EXISTS `test.rbac-name`")
		env.queryWithNoError(r, "DROP USER IF EXISTS `test.rbac-name`")

		createRBACObjects := func(drop bool) {
			if drop {
				log.Debug().Msg("drop all RBAC related objects")
				env.queryWithNoError(r, "DROP SETTINGS PROFILE `test.rbac-name`")
				env.queryWithNoError(r, "DROP QUOTA `test.rbac-name`")
				env.queryWithNoError(r, "DROP ROW POLICY `test.rbac-name` ON test_rbac.test_rbac")
				env.queryWithNoError(r, "DROP ROLE `test.rbac-name`")
				env.queryWithNoError(r, "DROP USER `test.rbac-name`")
			}
			log.Debug().Msg("create RBAC related objects")
			env.queryWithNoError(r, "CREATE SETTINGS PROFILE `test.rbac-name` SETTINGS max_execution_time=60")
			env.queryWithNoError(r, "CREATE ROLE `test.rbac-name` SETTINGS PROFILE `test.rbac-name`")
			env.queryWithNoError(r, "CREATE USER `test.rbac-name` IDENTIFIED BY 'test_rbac_password' DEFAULT ROLE `test.rbac-name`")
			env.queryWithNoError(r, "CREATE QUOTA `test.rbac-name` KEYED BY user_name FOR INTERVAL 1 hour NO LIMITS TO `test.rbac-name`")
			env.queryWithNoError(r, "CREATE ROW POLICY `test.rbac-name` ON test_rbac.test_rbac USING v>=0 AS RESTRICTIVE TO `test.rbac-name`")
		}
		createRBACObjects(false)
		env.DockerExecNoError(r, "clickhouse", "clickhouse-client", "-mn", "-q", "SELECT * FROM system.user_directories FORMAT Vertical; SELECT * FROM system.users FORMAT Vertical; SELECT * FROM system.roles FORMAT Vertical; SELECT * FROM system.settings_profiles FORMAT Vertical; SELECT * FROM system.quotas FORMAT Vertical")
		//--rbac + data
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "create_remote", "--rbac", "test_rbac_backup_with_data")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup delete local test_rbac_backup_with_data")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup restore_remote --rm --rbac test_rbac_backup_with_data")
		env.ch.Close()
		env.connectWithWait(t, r, 2*time.Second, 2*time.Second, 1*time.Minute)
		env.queryWithNoError(r, "CREATE ROW POLICY `test_rbac_for_default` ON test_rbac.test_rbac USING v>=0 TO `default`")
		env.checkCount(r, 1, 10, "SELECT count() FROM test_rbac.test_rbac")

		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup delete remote test_rbac_backup_with_data")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup delete local test_rbac_backup_with_data")

		//--rbac-only
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "create", "--rbac", "--rbac-only", "--env", "S3_COMPRESSION_FORMAT=zstd", "test_rbac_backup")
		r.NoError(env.dropDatabase("test_rbac", false))
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup upload test_rbac_backup")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "delete", "local", "test_rbac_backup")
		env.DockerExecNoError(r, "clickhouse", "ls", "-lah", "/var/lib/clickhouse/access")

		log.Debug().Msg("create conflicted RBAC objects")
		createRBACObjects(true)

		env.DockerExecNoError(r, "clickhouse", "ls", "-lah", "/var/lib/clickhouse/access")

		log.Debug().Msg("download+restore RBAC")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup download test_rbac_backup")

		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 clickhouse-backup -c "+config+" restore --rm --rbac test_rbac_backup")
		log.Debug().Msg(out)
		r.Contains(out, "RBAC successfully restored")
		r.NoError(err, "%s\nunexpected RBAC error: %v", out, err)

		out, err = env.DockerExecOut("clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 clickhouse-backup -c "+config+" restore --rm --rbac-only test_rbac_backup")
		log.Debug().Msg(out)
		r.Contains(out, "RBAC successfully restored")
		r.NoError(err, "%s\nunexpected RBAC error: %v", out, err)
		env.DockerExecNoError(r, "clickhouse", "ls", "-lah", "/var/lib/clickhouse/access")

		env.ch.Close()
		// r.NoError(utils.ExecCmd(t.Context(), 180*time.Second, append(env.GetDefaultComposeCommand(), "restart", "clickhouse")))
		env.connectWithWait(t, r, 2*time.Second, 2*time.Second, 1*time.Minute)

		env.DockerExecNoError(r, "clickhouse", "ls", "-lah", "/var/lib/clickhouse/access")

		rbacTypes := map[string]string{
			"PROFILES": "test.rbac-name",
			"QUOTAS":   "test.rbac-name",
			"POLICIES": "`test.rbac-name` ON test_rbac.test_rbac",
			"ROLES":    "test.rbac-name",
			"USERS":    "test.rbac-name",
		}
		for rbacType, expectedValue := range rbacTypes {
			var rbacRows []struct {
				Name string `ch:"name"`
			}
			err := env.ch.Select(&rbacRows, fmt.Sprintf("SHOW %s", rbacType))
			r.NoError(err)
			found := false
			for _, row := range rbacRows {
				log.Debug().Msgf("rbacType=%s expectedValue=%s row.Name=%s", rbacType, expectedValue, row.Name)
				if expectedValue == row.Name {
					found = true
					break
				}
			}
			if !found {
				//env.DockerExecNoError(r, "clickhouse", "cat", "/var/log/clickhouse-server/clickhouse-server.log")
				r.Failf("wrong RBAC", "SHOW %s, %#v doesn't contain %#v", rbacType, rbacRows, expectedValue)
			}
		}
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "delete", "local", "test_rbac_backup")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "delete", "remote", "test_rbac_backup")

		env.checkCount(r, 1, 0, "SELECT count() FROM system.tables WHERE database='default' AND name='test_rbac' SETTINGS empty_result_for_aggregation_by_empty_set=0")

		env.queryWithNoError(r, "DROP SETTINGS PROFILE `test.rbac-name`")
		env.queryWithNoError(r, "DROP QUOTA `test.rbac-name`")
		env.queryWithNoError(r, "DROP ROW POLICY `test.rbac-name` ON test_rbac.test_rbac")
		env.queryWithNoError(r, "DROP ROLE `test.rbac-name`")
		env.queryWithNoError(r, "DROP USER `test.rbac-name`")
		env.queryWithNoError(r, "DROP TABLE IF EXISTS test_rbac.test_rbac")
		env.queryWithNoError(r, "DROP ROW POLICY `test_rbac_for_default` ON test_rbac.test_rbac")

		r.NoError(env.dropDatabase("test_rbac", true))
		env.ch.Close()
	}
	if compareVersion(chVersion, "24.1") >= 0 {
		testRBACScenario("/etc/clickhouse-backup/config-s3-embedded.yml")
		testRBACScenario("/etc/clickhouse-backup/config-s3-embedded-url.yml")
		testRBACScenario("/etc/clickhouse-backup/config-azblob-embedded.yml")
	}
	if compareVersion(chVersion, "24.2") >= 0 {
		testRBACScenario("/etc/clickhouse-backup/config-azblob-embedded-url.yml")
	}
	testRBACScenario("/etc/clickhouse-backup/config-s3.yml")
	env.Cleanup(t, r)
}
