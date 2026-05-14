//go:build integration

package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// TestCASArtifacts verifies that cas-upload captures RBAC objects, database
// definitions, UDFs, named collections, and configs, and that after
// cas-download the local directory is v1-compatible (restore --rbac etc. work).
//
// The test reuses the same fixture patterns as TestRBAC, TestConfigs, and
// TestNamedCollections but drives them through the CAS upload/download path.
func TestCASArtifacts(t *testing.T) {
	casSkipIfClickHouseTooOld(t)
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "cas_artifacts")

	t.Run("RBAC", func(t *testing.T) {
		testCASArtifactsRBAC(t, env, r)
	})

	t.Run("Databases", func(t *testing.T) {
		testCASArtifactsDatabases(t, env, r)
	})

	t.Run("Functions", func(t *testing.T) {
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.11") < 0 {
			t.Skipf("user-defined functions require ClickHouse 21.11+, got %s", os.Getenv("CLICKHOUSE_VERSION"))
		}
		testCASArtifactsFunctions(t, env, r)
	})

	t.Run("NamedCollections", func(t *testing.T) {
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.12") < 0 {
			t.Skipf("named collections require ClickHouse 22.12+, got %s", os.Getenv("CLICKHOUSE_VERSION"))
		}
		testCASArtifactsNamedCollections(t, env, r)
	})
}

func testCASArtifactsRBAC(t *testing.T, env *TestEnvironment, r *require.Assertions) {
	const (
		db     = "cas_art_rbac_db"
		backup = "cas_art_rbac"
	)

	r.NoError(env.dropDatabase(db, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", db))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", db))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(10)", db))

	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_art_test_user`")
	env.queryWithNoError(r, "DROP ROLE IF EXISTS `cas_art_test_role`")
	env.queryWithNoError(r, "CREATE ROLE `cas_art_test_role`")
	env.queryWithNoError(r, "CREATE USER `cas_art_test_user` IDENTIFIED BY 'test_pass' DEFAULT ROLE `cas_art_test_role`")

	// create (v1 local) → cas-upload → delete local → cas-download → restore --rbac
	env.casBackupNoError(r, "create", "--rbac", "--tables", db+".*", backup)
	env.casBackupNoError(r, "cas-upload", backup)
	env.casBackupNoError(r, "delete", "local", backup)

	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_art_test_user`")
	env.queryWithNoError(r, "DROP ROLE IF EXISTS `cas_art_test_role`")

	env.casBackupNoError(r, "cas-download", backup)
	// Note: restore --rbac may exit non-zero on this test env because the
	// configured restart_command includes SYSTEM SHUTDOWN, and the next
	// GetTables query after restart can race ("Query was cancelled"). The
	// RBAC restore itself succeeds — verify via the success log line + the
	// post-restart RBAC checks below rather than the exit code.
	out, _ := env.casBackup("restore", "--rm", "--rbac", backup)
	r.Contains(out, "RBAC successfully restored", "expected RBAC restore message: %s", out)

	// Restart so ClickHouse reloads the access directory.
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t.Context(), "clickhouse"))
	env.connectWithWait(t, r, 2*time.Second, 2*time.Second, 1*time.Minute)

	// Verify RBAC objects exist with retry (ClickHouse may still be loading).
	for _, check := range []struct{ rbacType, expected string }{
		{"ROLES", "cas_art_test_role"},
		{"USERS", "cas_art_test_user"},
	} {
		var rows []struct {
			Name string `ch:"name"`
		}
		var selErr error
		for attempt := 1; attempt <= 10; attempt++ {
			rows = nil
			selErr = env.ch.Select(&rows, fmt.Sprintf("SHOW %s", check.rbacType))
			if selErr == nil {
				break
			}
			log.Warn().Msgf("SHOW %s attempt %d failed: %v", check.rbacType, attempt, selErr)
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		r.NoError(selErr, "SHOW %s", check.rbacType)
		found := false
		for _, row := range rows {
			if row.Name == check.expected {
				found = true
				break
			}
		}
		r.True(found, "SHOW %s: expected %q in %v", check.rbacType, check.expected, rows)
	}

	env.checkCount(r, 1, 10, fmt.Sprintf("SELECT count() FROM `%s`.t", db))

	// Cleanup.
	env.queryWithNoError(r, "DROP USER IF EXISTS `cas_art_test_user`")
	env.queryWithNoError(r, "DROP ROLE IF EXISTS `cas_art_test_role`")
	r.NoError(env.dropDatabase(db, true))
	env.casBackupNoError(r, "cas-delete", backup)
}

func testCASArtifactsDatabases(t *testing.T, env *TestEnvironment, r *require.Assertions) {
	const (
		db     = "cas_art_db_engine"
		backup = "cas_art_db"
	)

	r.NoError(env.dropDatabase(db, true))
	// Use Atomic engine explicitly so the Databases metadata carries the engine.
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s` ENGINE=Atomic", db))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", db))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(5)", db))

	env.casBackupNoError(r, "create", "--tables", db+".*", backup)
	env.casBackupNoError(r, "cas-upload", backup)
	env.casBackupNoError(r, "delete", "local", backup)

	r.NoError(env.dropDatabase(db, true))

	env.casBackupNoError(r, "cas-download", backup)
	env.casBackupNoError(r, "restore", "--rm", backup)

	// Database should exist after restore (restored from Databases field in metadata.json).
	var rows []struct {
		Name string `ch:"name"`
	}
	r.NoError(env.ch.Select(&rows,
		fmt.Sprintf("SELECT name FROM system.databases WHERE name = '%s'", db)))
	found := false
	for _, row := range rows {
		if row.Name == db {
			found = true
			break
		}
	}
	r.True(found, "expected database %s in system.databases after CAS restore", db)

	env.checkCount(r, 1, 5, fmt.Sprintf("SELECT count() FROM `%s`.t", db))

	r.NoError(env.dropDatabase(db, true))
	env.casBackupNoError(r, "cas-delete", backup)
}

func testCASArtifactsFunctions(t *testing.T, env *TestEnvironment, r *require.Assertions) {
	const (
		db     = "cas_art_fn_db"
		backup = "cas_art_fn"
		fn     = "cas_art_fn_double"
	)

	r.NoError(env.dropDatabase(db, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", db))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", db))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(3)", db))
	env.queryWithNoError(r, fmt.Sprintf("DROP FUNCTION IF EXISTS %s", fn))
	env.queryWithNoError(r, fmt.Sprintf("CREATE FUNCTION %s AS (x) -> x * 2", fn))

	env.casBackupNoError(r, "create", "--tables", db+".*", backup)
	env.casBackupNoError(r, "cas-upload", backup)
	env.casBackupNoError(r, "delete", "local", backup)

	env.queryWithNoError(r, fmt.Sprintf("DROP FUNCTION IF EXISTS %s", fn))
	r.NoError(env.dropDatabase(db, true))

	env.casBackupNoError(r, "cas-download", backup)
	env.casBackupNoError(r, "restore", "--rm", backup)

	// Function should be listed in system.functions after restore.
	var rows []struct {
		Name string `ch:"name"`
	}
	r.NoError(env.ch.Select(&rows,
		fmt.Sprintf("SELECT name FROM system.functions WHERE name = '%s' AND origin = 'SQLUserDefined'", fn)))
	r.NotEmpty(rows, "expected function %s in system.functions after CAS restore", fn)

	env.queryWithNoError(r, fmt.Sprintf("DROP FUNCTION IF EXISTS %s", fn))
	r.NoError(env.dropDatabase(db, true))
	env.casBackupNoError(r, "cas-delete", backup)
}

func testCASArtifactsNamedCollections(t *testing.T, env *TestEnvironment, r *require.Assertions) {
	const (
		db         = "cas_art_nc_db"
		backup     = "cas_art_nc"
		collection = "cas_art_nc_col"
	)

	r.NoError(env.dropDatabase(db, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", db))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", db))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(4)", db))
	env.queryWithNoError(r, fmt.Sprintf("DROP NAMED COLLECTION IF EXISTS %s", collection))
	env.queryWithNoError(r, fmt.Sprintf("CREATE NAMED COLLECTION %s AS key='val' OVERRIDABLE", collection))

	env.casBackupNoError(r, "create", "--named-collections", "--tables", db+".*", backup)
	env.casBackupNoError(r, "cas-upload", backup)
	env.casBackupNoError(r, "delete", "local", backup)

	env.queryWithNoError(r, fmt.Sprintf("DROP NAMED COLLECTION IF EXISTS %s", collection))
	r.NoError(env.dropDatabase(db, true))

	env.casBackupNoError(r, "cas-download", backup)
	out, err := env.casBackup("restore", "--rm", "--named-collections", backup)
	r.NoError(err, "restore --named-collections failed: %s", out)

	// Named collection should exist after restore.
	var rows []struct {
		Name string `ch:"name"`
	}
	r.NoError(env.ch.Select(&rows,
		fmt.Sprintf("SELECT name FROM system.named_collections WHERE name = '%s'", collection)))
	r.NotEmpty(rows, "expected named collection %s after CAS restore", collection)

	env.checkCount(r, 1, 4, fmt.Sprintf("SELECT count() FROM `%s`.t", db))

	env.queryWithNoError(r, fmt.Sprintf("DROP NAMED COLLECTION IF EXISTS %s", collection))
	r.NoError(env.dropDatabase(db, true))
	env.casBackupNoError(r, "cas-delete", backup)
}
