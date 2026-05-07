//go:build integration

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestCASPruneSmoke is the integration-level wiring test for cas-prune.
// Covers the full real-MinIO + real-ClickHouse path:
//
//  1. cas-upload of a fresh backup. cas-prune with no deletes finds no
//     orphans and exits cleanly.
//  2. cas-prune --dry-run is safe to run any time and never writes a marker.
//  3. cas-delete then cas-prune --grace-hours 0. Marker must be released so
//     the very next cas-prune does not refuse with "prune in progress".
//  4. --unlock errors out cleanly when there is no marker to clear.
//
// Marker corner cases (abandoned in-progress markers, --unlock for a stranded
// prune.marker, fail-closed when a live backup is unreadable) are covered by
// pkg/cas/prune_test.go against a fakedst Backend; they require direct
// object-store mutations that MinIO's erasure-coded storage layout does not
// allow us to inject reliably from a filesystem write.
func TestCASPruneSmoke(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "prune_smoke")

	const (
		dbName     = "cas_prune_smoke_db"
		backupName = "cas_prune_smoke_bk"
	)

	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64, payload String) ENGINE=MergeTree ORDER BY id "+
		"SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0", dbName))
	env.queryWithNoError(r, fmt.Sprintf(
		"INSERT INTO `%s`.t SELECT number, randomPrintableASCII(64) FROM numbers(500)", dbName))
	env.casBackupNoError(r, "create", "--tables", dbName+".*", backupName)
	env.casBackupNoError(r, "cas-upload", backupName)

	pruneOut := env.casBackupNoError(r, "cas-prune")
	t.Logf("cas-prune (live):\n%s", pruneOut)
	r.Contains(pruneOut, "Live backups        : 1", "expected 1 live backup; got: %s", pruneOut)
	r.Contains(pruneOut, "Orphans deleted     : 0", "no orphans expected before delete; got: %s", pruneOut)

	dryOut := env.casBackupNoError(r, "cas-prune", "--dry-run")
	t.Logf("cas-prune --dry-run:\n%s", dryOut)
	r.Contains(dryOut, "cas-prune (dry-run):", "dry-run header missing; got: %s", dryOut)

	env.casBackupNoError(r, "cas-delete", backupName)
	pruneOut2 := env.casBackupNoError(r, "cas-prune", "--grace-blob=0s")
	t.Logf("cas-prune (after delete, grace=0):\n%s", pruneOut2)
	r.Contains(pruneOut2, "Live backups        : 0", "expected 0 live backups; got: %s", pruneOut2)

	probe, err := env.casBackup("cas-prune")
	r.NoError(err, "cas-prune after a successful prune must not refuse; got: %s", probe)
	r.NotContains(probe, "prune in progress", "no stranded marker expected; got: %s", probe)

	unlockOut, err := env.casBackup("cas-prune", "--unlock")
	r.Error(err, "cas-prune --unlock without a marker must error; got: %s", unlockOut)
	r.True(strings.Contains(unlockOut, "no prune.marker present"),
		"expected no-marker error; got: %s", unlockOut)

	r.NoError(env.dropDatabase(dbName, true))
}

// TestCASPruneEndToEndDedupeReclaim is the realistic mutation-heavy scenario:
// three backups whose payload column is hardlinked across waves (so the
// payload blob is shared); the marker column is rewritten each wave, so
// each backup has a small set of unique blobs. Deleting the middle backup +
// pruning must reclaim its unique blobs but keep the shared ones. After
// deleting all backups + pruning, every blob must be reclaimed.
func TestCASPruneEndToEndDedupeReclaim(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "prune_e2e")

	const (
		dbName  = "cas_prune_e2e_db"
		tblName = "cas_prune_e2e_t"
	)

	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf(`CREATE TABLE `+"`%s`.`%s`"+` (id UInt64, payload String, marker String)
		ENGINE=MergeTree ORDER BY id
		SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0`, dbName, tblName))

	for i, marker := range []string{"v1", "v2", "v3"} {
		bk := fmt.Sprintf("cas_prune_e2e_bk%d", i+1)
		env.queryWithNoError(r, fmt.Sprintf(
			"INSERT INTO `%s`.`%s` SELECT number, randomPrintableASCII(64), '%s' FROM numbers(1000)",
			dbName, tblName, marker))
		env.queryWithNoError(r, fmt.Sprintf("OPTIMIZE TABLE `%s`.`%s` FINAL", dbName, tblName))
		env.casBackupNoError(r, "create", "--tables", dbName+".*", bk)
		env.casBackupNoError(r, "cas-upload", bk)
	}

	statusBefore := env.casBackupNoError(r, "cas-status")
	t.Logf("statusBefore:\n%s", statusBefore)
	r.Contains(statusBefore, "Backups: 3", "expected 3 backups uploaded; got: %s", statusBefore)

	// Delete the middle backup; prune must reclaim ONLY blobs unique to it.
	env.casBackupNoError(r, "cas-delete", "cas_prune_e2e_bk2")
	pruneMid := env.casBackupNoError(r, "cas-prune", "--grace-blob=0s")
	t.Logf("first cas-prune:\n%s", pruneMid)
	r.Contains(pruneMid, "Live backups        : 2", "expected 2 live backups; got: %s", pruneMid)

	statusMid := env.casBackupNoError(r, "cas-status")
	t.Logf("statusMid:\n%s", statusMid)
	r.Contains(statusMid, "Backups: 2", "expected Backups: 2; got: %s", statusMid)
	r.NotContains(statusMid, "Blobs:   0 ", "shared blobs from bk1+bk3 must survive; got: %s", statusMid)

	// Delete everything; prune must reclaim every remaining blob.
	env.casBackupNoError(r, "cas-delete", "cas_prune_e2e_bk1")
	env.casBackupNoError(r, "cas-delete", "cas_prune_e2e_bk3")
	pruneFinal := env.casBackupNoError(r, "cas-prune", "--grace-blob=0s")
	t.Logf("final cas-prune:\n%s", pruneFinal)
	r.Contains(pruneFinal, "Live backups        : 0", "expected 0 live backups; got: %s", pruneFinal)

	finalStatus := env.casBackupNoError(r, "cas-status")
	t.Logf("finalStatus:\n%s", finalStatus)
	r.Contains(finalStatus, "Backups: 0", "expected 0 backups after full delete; got: %s", finalStatus)
	r.Contains(finalStatus, "Blobs:   0 ", "expected 0 blobs after full delete + prune; got: %s", finalStatus)

	r.NoError(env.dropDatabase(dbName, true))
}
