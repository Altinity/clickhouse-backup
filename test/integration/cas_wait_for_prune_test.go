//go:build integration

package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestCASUploadWaitsForPrune injects a prune marker, schedules its removal
// after a few seconds, and verifies cas-upload --wait-for-prune polls past
// the obstruction.
func TestCASUploadWaitsForPrune(t *testing.T) {
	casSkipIfClickHouseTooOld(t)
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "wait_prune")

	const (
		dbName  = "cas_waitprune_db"
		tblName = "t"
		bk      = "cas_waitprune_bk"
	)
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.`%s` (id UInt64) ENGINE=MergeTree ORDER BY id", dbName, tblName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT number FROM numbers(100)", dbName, tblName))
	env.casBackupNoError(r, "create", "--tables", dbName+".*", bk)

	// Inject a prune marker.
	markerKey := "backup/cluster/0/cas/wait_prune/prune.marker"
	markerBody := `{"host":"other","started_at":"2026-05-08T00:00:00Z","run_id":"abcd1234","tool":"test"}`
	env.injectS3Object(r, markerKey, markerBody)

	// Schedule marker removal 5s in via cas-prune --unlock.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Second)
		out, err := env.casBackup("cas-prune", "--unlock")
		if err != nil {
			t.Logf("cas-prune --unlock failed (expected only if marker already removed): %v out=%s", err, out)
		}
	}()

	start := time.Now()
	out := env.casBackupNoError(r, "cas-upload", "--wait-for-prune=30s", bk)
	elapsed := time.Since(start)
	wg.Wait()

	r.GreaterOrEqual(elapsed, 4*time.Second, "upload should have waited >= 4s; got %s", elapsed)
	r.Less(elapsed, 20*time.Second, "upload took too long; out=%s", out)
	r.Contains(out, "uploaded now", "upload output should report bytes uploaded; out=%s", out)

	env.casBackupNoError(r, "cas-delete", bk)
	r.NoError(env.dropDatabase(dbName, true))
}

// TestCASUploadWaitTimeout verifies the timeout path.
func TestCASUploadWaitTimeout(t *testing.T) {
	casSkipIfClickHouseTooOld(t)
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "wait_timeout")

	const (
		dbName  = "cas_waittimeout_db"
		tblName = "t"
		bk      = "cas_waittimeout_bk"
	)
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.`%s` (id UInt64) ENGINE=MergeTree ORDER BY id", dbName, tblName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT number FROM numbers(10)", dbName, tblName))
	env.casBackupNoError(r, "create", "--tables", dbName+".*", bk)

	markerKey := "backup/cluster/0/cas/wait_timeout/prune.marker"
	markerBody := `{"host":"other","started_at":"2026-05-08T00:00:00Z","run_id":"deadbeef","tool":"test"}`
	env.injectS3Object(r, markerKey, markerBody)

	start := time.Now()
	out, err := env.casBackup("cas-upload", "--wait-for-prune=2s", bk)
	elapsed := time.Since(start)

	r.Error(err, "cas-upload should fail after 2s timeout; out=%s", out)
	r.Contains(out, "prune still in progress", "out=%s", out)
	r.GreaterOrEqual(elapsed, 2*time.Second, "should have waited at least 2s; elapsed=%s", elapsed)
	r.Less(elapsed, 8*time.Second, "should not wait too much past 2s; elapsed=%s", elapsed)

	// Cleanup: unlock and delete.
	_, _ = env.casBackup("cas-prune", "--unlock")
	_, _ = env.casBackup("delete", "local", bk)
	r.NoError(env.dropDatabase(dbName, true))
}
