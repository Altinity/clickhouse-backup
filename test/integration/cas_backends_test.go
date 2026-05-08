//go:build integration

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// runCASBackendSmoke runs the same upload → status → restore →
// verify-rows → delete → prune cycle that all per-backend smoke tests
// use. Caller is responsible for casBootstrap; this routine handles the
// rest.
//
// dbName, tableName, backupName must be unique per backend so concurrent
// tests don't collide on the local backup namespace.
func runCASBackendSmoke(t *testing.T, env *TestEnvironment, r *require.Assertions, dbName, tableName, backupName string) {
	t.Helper()
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf(
		"CREATE TABLE `%s`.`%s` (id UInt64, payload String) ENGINE=MergeTree ORDER BY id "+
			"SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0", dbName, tableName))
	env.queryWithNoError(r, fmt.Sprintf(
		"INSERT INTO `%s`.`%s` SELECT number, randomPrintableASCII(64) FROM numbers(200)",
		dbName, tableName))

	env.casBackupNoError(r, "create", "--tables", dbName+".*", backupName)
	env.casBackupNoError(r, "cas-upload", backupName)

	statusOut := env.casBackupNoError(r, "cas-status")
	r.Contains(statusOut, "Backups: 1", "cas-status should show 1 backup; got: %s", statusOut)

	r.NoError(env.dropDatabase(dbName, true))
	env.casBackupNoError(r, "cas-restore", "--rm", backupName)

	var rowsResult []struct {
		C uint64 `ch:"c"`
	}
	r.NoError(env.ch.Select(&rowsResult, fmt.Sprintf("SELECT count() AS c FROM `%s`.`%s`", dbName, tableName)))
	r.Len(rowsResult, 1)
	r.Equal(uint64(200), rowsResult[0].C, "row count after restore")

	env.casBackupNoError(r, "cas-delete", backupName)
	env.casBackupNoError(r, "cas-prune", "--grace-blob=0s")

	finalStatus := env.casBackupNoError(r, "cas-status")
	r.Contains(finalStatus, "Backups: 0", "after delete + prune, expected 0 backups: %s", finalStatus)

	r.NoError(env.dropDatabase(dbName, true))
}

// TestCASSmokeGCS exercises the full CAS lifecycle against the
// fake-gcs-server emulator. Verifies the GCS backend's
// PutFileAbsoluteIfAbsent (Conditions{DoesNotExist: true}) path
// works end-to-end against a real-ish server.
func TestCASSmokeGCS(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrapWith(r, "smoke_gcs", "config-gcs-emulator.yml", "")
	runCASBackendSmoke(t, env, r,
		"cas_smoke_gcs_db", "cas_smoke_gcs_t", "cas_smoke_gcs_bk")
}

// TestCASSmokeAzure exercises the full CAS lifecycle against Azurite.
// Verifies the Azure backend's PutFileAbsoluteIfAbsent (If-None-Match)
// path added in Phase 4 T4.
func TestCASSmokeAzure(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrapWith(r, "smoke_azure", "config-azblob.yml", "")
	runCASBackendSmoke(t, env, r,
		"cas_smoke_azure_db", "cas_smoke_azure_t", "cas_smoke_azure_bk")
}
