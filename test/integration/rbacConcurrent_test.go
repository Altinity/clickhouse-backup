//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

const rbacConcurrentUsersCount = 100

// system.zookeeper path is relative to the <zookeeper><root> chroot, so it works for both env shapes
const uuidNodesCountQuery = "SELECT count() AS cnt FROM system.zookeeper WHERE path='/clickhouse/access/uuid' SETTINGS empty_result_for_aggregation_by_empty_set=0"

// TestRBACConcurrentRestore - two `restore --rbac-only` of the same backup started at the same moment
// against one keeper must both succeed, they write byte identical znodes,
// https://github.com/Altinity/clickhouse-backup/issues/1048
func TestRBACConcurrentRestore(t *testing.T) {
	chVersion := os.Getenv("CLICKHOUSE_VERSION")
	// `replicated` user directory is available from 21.9, the integration env is replicated-only since then
	if compareVersion(chVersion, "21.9") < 0 {
		t.Skipf("Test skipped, `replicated` user_directories not available for %s version", chVersion)
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)

	config := "/etc/clickhouse-backup/config-s3.yml"
	const backupName = "test_rbac_concurrent"
	// the amount of written znodes is what makes the concurrent write race deterministic
	concurrentUserNames := make([]string, rbacConcurrentUsersCount)
	for i := range concurrentUserNames {
		concurrentUserNames[i] = fmt.Sprintf("test_rbac_concurrent_%d", i+1)
	}
	concurrentUsers := strings.Join(concurrentUserNames, ", ")

	dropRBACQueries := []string{
		"DROP SETTINGS PROFILE IF EXISTS `test.rbac-name`",
		"DROP QUOTA IF EXISTS `test.rbac-name`",
		"DROP ROW POLICY IF EXISTS `test.rbac-name` ON test_rbac.test_rbac",
		"DROP ROLE IF EXISTS `test.rbac-name`",
		"DROP USER IF EXISTS `test.rbac-name`",
		"DROP USER IF EXISTS " + concurrentUsers,
	}

	// t.Cleanup runs after the env is returned to the shared pool, drop leftovers here,
	// a polluted env breaks later tests which run in the same container
	defer func() {
		if !env.ch.IsOpen {
			var connectErr error
			for attempt := 1; attempt <= 30; attempt++ {
				if connectErr = env.connect(t, "60s"); connectErr == nil {
					break
				}
				log.Warn().Msgf("TestRBACConcurrentRestore cleanup connect attempt %d error: %v", attempt, connectErr)
				time.Sleep(2 * time.Second)
			}
			// infrastructure failure, fail the test instead of returning a polluted env to the pool
			r.NoError(connectErr, "TestRBACConcurrentRestore cleanup connect error")
		}
		for _, q := range append(dropRBACQueries, "DROP TABLE IF EXISTS test_rbac.test_rbac SYNC", "DROP DATABASE IF EXISTS test_rbac SYNC") {
			if err := env.ch.Query(q); err != nil {
				log.Warn().Msgf("TestRBACConcurrentRestore cleanup query %q error: %v", q, err)
			}
		}
		if err := env.DockerExec("clickhouse-backup", "bash", "-c", "CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup delete local "+backupName); err != nil {
			log.Warn().Msgf("TestRBACConcurrentRestore cleanup delete local %s error: %v", backupName, err)
		}
	}()

	env.connectWithWait(t, r, 1*time.Second, 1*time.Second, 1*time.Minute)

	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS test_rbac")
	env.queryWithNoError(t, r, "CREATE TABLE IF NOT EXISTS test_rbac.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")
	for _, q := range dropRBACQueries {
		env.queryWithNoError(t, r, q)
	}

	// ClickHouse `<replicated>` access storage (RBAC in keeper) has a race:
	// a freshly created role/profile can be transiently evicted from the in-memory cache by the background refresh,
	// so resolving it in a TO / DEFAULT ROLE / SETTINGS PROFILE clause fails
	// with UNKNOWN_ROLE (code 511) or THERE_IS_NO_PROFILE (code 180), retry until it settles
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
	createRBACObjects := func() {
		createRBACQuery("CREATE SETTINGS PROFILE `test.rbac-name` SETTINGS max_execution_time=60")
		createRBACQuery("CREATE ROLE `test.rbac-name` SETTINGS PROFILE `test.rbac-name`")
		createRBACQuery("CREATE USER `test.rbac-name` IDENTIFIED BY 'test_rbac_password' DEFAULT ROLE `test.rbac-name`")
		createRBACQuery("CREATE QUOTA `test.rbac-name` KEYED BY user_name FOR INTERVAL 1 hour NO LIMITS TO `test.rbac-name`")
		createRBACQuery("CREATE ROW POLICY `test.rbac-name` ON test_rbac.test_rbac USING v>=0 AS RESTRICTIVE TO `test.rbac-name`")
		createRBACQuery("CREATE USER " + concurrentUsers)
	}
	// total RBAC objects created by this test = 5 `test.rbac-name` objects + rbacConcurrentUsersCount users
	expectedObjects := uint64(5 + rbacConcurrentUsersCount)
	// the count is for the whole container, take a baseline so leftovers of earlier tests in the pooled env don't fail the assert
	var baselineUuidNodes uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&baselineUuidNodes, uuidNodesCountQuery))

	createRBACObjects()
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "create", "--rbac", "--rbac-only", backupName)

	// two processes, two `/tmp` directories, so pidlock.CheckAndCreatePidFile doesn't reject the second restore,
	// one keeper and one clickhouse-server, which is the "two replicas restore the same backup" shape of the bug
	restoreContainers := []string{"clickhouse-backup", "clickhouse"}
	restoreConcurrently := func() {
		outs := make([]string, len(restoreContainers))
		errs := make([]error, len(restoreContainers))
		wg := sync.WaitGroup{}
		for i, container := range restoreContainers {
			wg.Add(1)
			go func(i int, container string) {
				defer wg.Done()
				outs[i], errs[i] = env.DockerExecOut(container, "bash", "-xec",
					"ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_RESTART_COMMAND='exec:true' LOG_LEVEL=debug clickhouse-backup -c "+config+" restore --rbac-only "+backupName)
			}(i, container)
		}
		wg.Wait()
		for i, container := range restoreContainers {
			log.Debug().Msgf("restore --rbac-only in %s:\n%s", container, outs[i])
			r.NoError(errs[i], "%s\nunexpected concurrent RBAC restore error in %s: %v", outs[i], container, errs[i])
			r.Contains(outs[i], "RBAC successfully restored", "no success message from concurrent RBAC restore in %s", container)
		}
	}

	// "absent" - objects dropped before the restore, both processes create the same znodes,
	// "present" - objects still there, `rbac_conflict_resolution: recreate` drops and recreates them concurrently
	for _, shape := range []string{"absent", "present", "absent", "present"} {
		log.Debug().Msgf("concurrent restore --rbac-only, RBAC objects are %s", shape)
		if shape == "absent" {
			for _, q := range dropRBACQueries {
				env.queryWithNoError(t, r, q)
			}
		}
		restoreConcurrently()

		for _, rbacType := range []string{"PROFILES", "QUOTAS", "POLICIES", "ROLES", "USERS"} {
			expectedValue := "test.rbac-name"
			if rbacType == "POLICIES" {
				expectedValue = "`test.rbac-name` ON test_rbac.test_rbac"
			}
			// replicated access storage refreshes asynchronously, poll until the object shows up
			found := false
			for attempt := 1; attempt <= 30 && !found; attempt++ {
				var rbacRows []struct {
					Name string `ch:"name"`
				}
				if err := env.ch.Select(&rbacRows, "SHOW "+rbacType); err != nil {
					log.Warn().Msgf("SHOW %s attempt %d error: %v", rbacType, attempt, err)
				}
				for _, row := range rbacRows {
					if row.Name == expectedValue {
						found = true
						break
					}
				}
				if !found {
					time.Sleep(1 * time.Second)
				}
			}
			r.Truef(found, "SHOW %s doesn't contain %#v after concurrent restore, RBAC objects were %s", rbacType, expectedValue, shape)
		}

		// every restored user must be materialized by ClickHouse, not only present as a znode
		var restoredUsers uint64
		for attempt := 1; attempt <= 30; attempt++ {
			r.NoError(env.ch.SelectSingleRowNoCtx(&restoredUsers, "SELECT count() AS cnt FROM system.users WHERE name LIKE 'test\\_rbac\\_concurrent\\_%' SETTINGS empty_result_for_aggregation_by_empty_set=0"))
			if restoredUsers == uint64(rbacConcurrentUsersCount) {
				break
			}
			time.Sleep(1 * time.Second)
		}
		r.Equalf(uint64(rbacConcurrentUsersCount), restoredUsers, "expect %d test_rbac_concurrent_* users after concurrent restore, RBAC objects were %s", rbacConcurrentUsersCount, shape)

		var uuidNodes uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&uuidNodes, uuidNodesCountQuery))
		r.Equalf(baselineUuidNodes+expectedObjects, uuidNodes, "expect %d /clickhouse/access/uuid znodes after concurrent restore, RBAC objects were %s", baselineUuidNodes+expectedObjects, shape)
	}

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "delete", "local", backupName)
	for _, q := range dropRBACQueries {
		env.queryWithNoError(t, r, q)
	}
	env.queryWithNoError(t, r, "DROP TABLE IF EXISTS test_rbac.test_rbac SYNC")
	r.NoError(env.dropDatabase("test_rbac", true))
}
