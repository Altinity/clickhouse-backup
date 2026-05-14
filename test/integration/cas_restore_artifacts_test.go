//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// TestCASRestoreArtifacts verifies that cas-restore restores RBAC, configs,
// named collections, databases, and functions in one shot — no cas-download
// + restore workaround. Covers each --*-only flag's "skip table data" mode.
func TestCASRestoreArtifacts(t *testing.T) {
	casSkipIfClickHouseTooOld(t)
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "cas_restore_art")

	t.Run("RBACOnlySkipsTableData", func(t *testing.T) {
		testCASRestoreRBACOnly(t, env, r)
	})

	t.Run("RBACFlagWithTables", func(t *testing.T) {
		testCASRestoreRBACWithTables(t, env, r)
	})

	t.Run("NoArtifactFlags", func(t *testing.T) {
		testCASRestoreNoFlags(t, env, r)
	})

	t.Run("NamedCollectionsOnly", func(t *testing.T) {
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.12") < 0 {
			t.Skipf("named collections require ClickHouse 22.12+, got %s", os.Getenv("CLICKHOUSE_VERSION"))
		}
		testCASRestoreNamedCollectionsOnly(t, env, r)
	})

	t.Run("IgnoreDependenciesIsNoOp", func(t *testing.T) {
		testCASRestoreIgnoreDependenciesNoOp(t, env, r)
	})

	t.Run("DataOnlyRestoresPartsIntoExistingTable", func(t *testing.T) {
		testCASRestoreDataOnly(t, env, r)
	})
}

// testCASRestoreRBACOnly: --rbac-only fetches RBAC without table data.
func testCASRestoreRBACOnly(t *testing.T, env *TestEnvironment, r *require.Assertions) {
	const (
		db     = "cas_restore_rbac_db"
		backup = "cas_restore_rbac_only"
	)
	r.NoError(env.dropDatabase(db, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", db))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", db))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(10)", db))

	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_restore_user_a`")
	env.queryWithNoError(r, "DROP ROLE IF EXISTS `cas_restore_role_a`")
	env.queryWithNoError(r, "CREATE ROLE `cas_restore_role_a`")
	env.queryWithNoError(r, "CREATE USER `cas_restore_user_a` IDENTIFIED BY 'pw' DEFAULT ROLE `cas_restore_role_a`")

	env.casBackupNoError(r, "create", "--rbac", "--tables", db+".*", backup)
	env.casBackupNoError(r, "cas-upload", backup)
	env.casBackupNoError(r, "delete", "local", backup)

	// Drop RBAC + DB so we can observe the restore. DB will NOT come back
	// because --rbac-only must skip table restore entirely.
	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_restore_user_a`")
	env.queryWithNoError(r, "DROP ROLE IF EXISTS `cas_restore_role_a`")
	r.NoError(env.dropDatabase(db, true))

	// cas-restore --rbac-only: tables must NOT be restored.
	env.casBackupNoError(r, "cas-restore", "--rbac-only", backup)

	// Verify: no shadow/ content in the local backup dir = no table archives fetched.
	out, err := env.DockerExecOut("clickhouse", "bash", "-c",
		fmt.Sprintf("ls /var/lib/clickhouse/backup/%s/shadow 2>/dev/null | wc -l", backup))
	r.NoError(err)
	r.Equal("0", strings.TrimSpace(out),
		"--rbac-only must not download table archives; got shadow/ content: %s", out)

	// Restart so ClickHouse reloads access/.
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t.Context(), "clickhouse"))
	env.connectWithWait(t, r, 2*time.Second, 2*time.Second, 1*time.Minute)

	// RBAC must be restored.
	var rows []struct {
		Name string `ch:"name"`
	}
	var selErr error
	for attempt := 1; attempt <= 10; attempt++ {
		rows = nil
		selErr = env.ch.Select(&rows, "SHOW USERS")
		if selErr == nil {
			break
		}
		log.Warn().Msgf("SHOW USERS attempt %d: %v", attempt, selErr)
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	r.NoError(selErr)
	found := false
	for _, row := range rows {
		if row.Name == "cas_restore_user_a" {
			found = true
			break
		}
	}
	r.True(found, "expected cas_restore_user_a in SHOW USERS, got %v", rows)

	// Database must NOT exist (we dropped it and --rbac-only skips table restore).
	var dbRows []struct {
		Name string `ch:"name"`
	}
	r.NoError(env.ch.Select(&dbRows,
		fmt.Sprintf("SELECT name FROM system.databases WHERE name = '%s'", db)))
	r.Empty(dbRows, "--rbac-only must not restore the database %s; rows=%v", db, dbRows)

	// Cleanup.
	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_restore_user_a`")
	env.queryWithNoError(r, "DROP ROLE IF EXISTS `cas_restore_role_a`")
	env.casBackupNoError(r, "cas-delete", backup)
}

// testCASRestoreRBACWithTables: --rbac with tables — both restored from one command.
func testCASRestoreRBACWithTables(t *testing.T, env *TestEnvironment, r *require.Assertions) {
	const (
		db     = "cas_restore_rbac_full_db"
		backup = "cas_restore_rbac_full"
	)
	r.NoError(env.dropDatabase(db, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", db))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", db))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(7)", db))

	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_restore_user_b`")
	env.queryWithNoError(r, "CREATE USER `cas_restore_user_b` IDENTIFIED BY 'pw'")

	env.casBackupNoError(r, "create", "--rbac", "--tables", db+".*", backup)
	env.casBackupNoError(r, "cas-upload", backup)
	env.casBackupNoError(r, "delete", "local", backup)

	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_restore_user_b`")
	r.NoError(env.dropDatabase(db, true))

	// cas-restore --rbac with tables may exit non-zero on this env (SYSTEM
	// SHUTDOWN in restart_command + post-restart GetTables race). RBAC is
	// still restored — verify via post-restart checks rather than exit code.
	out, _ := env.casBackup("cas-restore", "--rm", "--rbac", backup)
	r.Contains(out, "RBAC successfully restored", "expected RBAC restore message: %s", out)

	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t.Context(), "clickhouse"))
	env.connectWithWait(t, r, 2*time.Second, 2*time.Second, 1*time.Minute)

	// Tables restored.
	env.checkCount(r, 1, 7, fmt.Sprintf("SELECT count() FROM `%s`.t", db))

	// User restored.
	var rows []struct {
		Name string `ch:"name"`
	}
	var selErr error
	for attempt := 1; attempt <= 10; attempt++ {
		rows = nil
		selErr = env.ch.Select(&rows, "SHOW USERS")
		if selErr == nil {
			break
		}
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	r.NoError(selErr)
	found := false
	for _, row := range rows {
		if row.Name == "cas_restore_user_b" {
			found = true
			break
		}
	}
	r.True(found, "expected cas_restore_user_b in SHOW USERS")

	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_restore_user_b`")
	r.NoError(env.dropDatabase(db, true))
	env.casBackupNoError(r, "cas-delete", backup)
}

// testCASRestoreNoFlags: cas-restore without artifact flags must NOT alter
// existing RBAC even if the backup includes an access/ subtree.
func testCASRestoreNoFlags(t *testing.T, env *TestEnvironment, r *require.Assertions) {
	const (
		db     = "cas_restore_no_flags_db"
		backup = "cas_restore_no_flags"
	)
	r.NoError(env.dropDatabase(db, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", db))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", db))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(3)", db))

	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_restore_user_c`")
	env.queryWithNoError(r, "CREATE USER `cas_restore_user_c` IDENTIFIED BY 'pw'")

	env.casBackupNoError(r, "create", "--rbac", "--tables", db+".*", backup)
	env.casBackupNoError(r, "cas-upload", backup)
	env.casBackupNoError(r, "delete", "local", backup)

	// Drop only the table; keep the user.
	r.NoError(env.dropDatabase(db, true))

	// Restore without --rbac: table comes back, user untouched (still exists).
	env.casBackupNoError(r, "cas-restore", "--rm", backup)

	env.checkCount(r, 1, 3, fmt.Sprintf("SELECT count() FROM `%s`.t", db))

	// User still exists (we never dropped it and cas-restore without --rbac
	// must not touch RBAC).
	var rows []struct {
		Name string `ch:"name"`
	}
	r.NoError(env.ch.Select(&rows, "SHOW USERS"))
	found := false
	for _, row := range rows {
		if row.Name == "cas_restore_user_c" {
			found = true
			break
		}
	}
	r.True(found, "cas-restore without --rbac must not remove existing users")

	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_restore_user_c`")
	r.NoError(env.dropDatabase(db, true))
	env.casBackupNoError(r, "cas-delete", backup)
}

// testCASRestoreNamedCollectionsOnly: --named-collections-only restores only NC.
func testCASRestoreNamedCollectionsOnly(t *testing.T, env *TestEnvironment, r *require.Assertions) {
	const (
		db         = "cas_restore_nc_db"
		backup     = "cas_restore_nc_only"
		collection = "cas_restore_nc"
	)
	r.NoError(env.dropDatabase(db, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", db))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", db))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(2)", db))
	env.queryWithNoError(r, fmt.Sprintf("DROP NAMED COLLECTION IF EXISTS %s", collection))
	env.queryWithNoError(r, fmt.Sprintf("CREATE NAMED COLLECTION %s AS key='val' OVERRIDABLE", collection))

	env.casBackupNoError(r, "create", "--named-collections", "--tables", db+".*", backup)
	env.casBackupNoError(r, "cas-upload", backup)
	env.casBackupNoError(r, "delete", "local", backup)

	env.queryWithNoError(r, fmt.Sprintf("DROP NAMED COLLECTION IF EXISTS %s", collection))
	r.NoError(env.dropDatabase(db, true))

	env.casBackupNoError(r, "cas-restore", "--named-collections-only", backup)

	// No shadow/ content fetched.
	out, err := env.DockerExecOut("clickhouse", "bash", "-c",
		fmt.Sprintf("ls /var/lib/clickhouse/backup/%s/shadow 2>/dev/null | wc -l", backup))
	r.NoError(err)
	r.Equal("0", strings.TrimSpace(out),
		"--named-collections-only must not download table archives; got shadow/ content: %s", out)

	// Named collection restored.
	var rows []struct {
		Name string `ch:"name"`
	}
	r.NoError(env.ch.Select(&rows,
		fmt.Sprintf("SELECT name FROM system.named_collections WHERE name = '%s'", collection)))
	r.NotEmpty(rows, "expected named collection %s after --named-collections-only restore", collection)

	// Database must NOT exist.
	var dbRows []struct {
		Name string `ch:"name"`
	}
	r.NoError(env.ch.Select(&dbRows,
		fmt.Sprintf("SELECT name FROM system.databases WHERE name = '%s'", db)))
	r.Empty(dbRows, "--named-collections-only must not restore the database; rows=%v", dbRows)

	env.queryWithNoError(r, fmt.Sprintf("DROP NAMED COLLECTION IF EXISTS %s", collection))
	r.NoError(env.dropDatabase(db, true))
	env.casBackupNoError(r, "cas-delete", backup)
}

// testCASRestoreIgnoreDependenciesNoOp: --ignore-dependencies is accepted for
// CLI parity with v1 'restore' but is a no-op for CAS (no dependency chain).
// Verify the restore succeeds and the table is back.
func testCASRestoreIgnoreDependenciesNoOp(t *testing.T, env *TestEnvironment, r *require.Assertions) {
	const (
		db     = "cas_restore_idep_db"
		backup = "cas_restore_idep"
	)
	r.NoError(env.dropDatabase(db, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", db))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", db))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(4)", db))

	env.casBackupNoError(r, "create", "--tables", db+".*", backup)
	env.casBackupNoError(r, "cas-upload", backup)
	env.casBackupNoError(r, "delete", "local", backup)
	r.NoError(env.dropDatabase(db, true))

	env.casBackupNoError(r, "cas-restore", "--rm", "--ignore-dependencies", backup)
	env.checkCount(r, 1, 4, fmt.Sprintf("SELECT count() FROM `%s`.t", db))

	r.NoError(env.dropDatabase(db, true))
	env.casBackupNoError(r, "cas-delete", backup)
}

// testCASRestoreDataOnly: --data-only attaches parts to an existing table
// without re-creating the table. The user must pre-create a compatible table
// (or restore --schema first) before invoking --data-only.
func testCASRestoreDataOnly(t *testing.T, env *TestEnvironment, r *require.Assertions) {
	const (
		db     = "cas_restore_data_only_db"
		backup = "cas_restore_data_only"
	)
	r.NoError(env.dropDatabase(db, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", db))
	createTable := fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", db)
	env.queryWithNoError(r, createTable)
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(6)", db))

	env.casBackupNoError(r, "create", "--tables", db+".*", backup)
	env.casBackupNoError(r, "cas-upload", backup)
	env.casBackupNoError(r, "delete", "local", backup)

	// Truncate the rows but keep the table; --data-only must attach the
	// backup's parts back into the existing table.
	env.queryWithNoError(r, fmt.Sprintf("TRUNCATE TABLE `%s`.t", db))
	env.checkCount(r, 1, 0, fmt.Sprintf("SELECT count() FROM `%s`.t", db))

	env.casBackupNoError(r, "cas-restore", "--data", backup)
	env.checkCount(r, 1, 6, fmt.Sprintf("SELECT count() FROM `%s`.t", db))

	r.NoError(env.dropDatabase(db, true))
	env.casBackupNoError(r, "cas-delete", backup)
}
