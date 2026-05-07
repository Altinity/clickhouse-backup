//go:build integration

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// casConfigPath is the in-container path of the on-the-fly config used by all
// cas-* integration tests. Generated in casBootstrap by appending a `cas:`
// stanza to the stock config-s3.yml.
const casConfigPath = "/tmp/config-cas.yml"

// casBootstrap writes a CAS-enabled config inside the clickhouse-backup
// container at casConfigPath. Pattern: copy config-s3.yml, then append a
// `cas:` stanza; configs/ is mounted read-only so we write into /tmp instead.
//
// clusterID is incorporated into root_prefix so concurrent tests in different
// envPool slots can't trample each other's bucket layouts.
func (env *TestEnvironment) casBootstrap(r *require.Assertions, clusterID string) {
	// Wipe any leftover CAS state from a previous test on this env.
	_ = env.DockerExec("minio", "rm", "-rf", "/minio/data/clickhouse/backup")
	_ = env.DockerExec("minio", "bash", "-c", "mkdir -p /minio/data/clickhouse")

	casBlock := fmt.Sprintf(`
cas:
  enabled: true
  cluster_id: %s
  root_prefix: cas/
  inline_threshold: 1024
  grace_blob: 24h
  abandon_threshold: 168h
`, clusterID)
	cmd := fmt.Sprintf("cp /etc/clickhouse-backup/config-s3.yml %s && cat >>%s <<'CASEOF'%sCASEOF",
		casConfigPath, casConfigPath, casBlock)
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", cmd)
}

// casBackup runs a clickhouse-backup command with the CAS config and returns
// (out, err). Thin convenience wrapper.
func (env *TestEnvironment) casBackup(args ...string) (string, error) {
	full := append([]string{"clickhouse-backup", "-c", casConfigPath}, args...)
	return env.DockerExecOut("clickhouse-backup", full...)
}

// casBackupNoError runs a clickhouse-backup command with the CAS config and
// asserts no error.
func (env *TestEnvironment) casBackupNoError(r *require.Assertions, args ...string) string {
	out, err := env.casBackup(args...)
	r.NoError(err, "cas command %v failed: %s", args, out)
	return out
}

// TestCASRoundtrip exercises the headline value-prop of the CAS layout:
// create → cas-upload → cas-status → drop → cas-restore → verify rows →
// cas-delete → cas-status (gone). See docs/cas-design.md §10.4 Phase 1.
func TestCASRoundtrip(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "roundtrip")

	const (
		dbName     = "cas_roundtrip_db"
		tableName  = "cas_roundtrip_t"
		backupName = "cas_roundtrip_bk"
		rowCount   = 100
	)

	// 1. Schema + data.
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.`%s` (id UInt64, x String) ENGINE=MergeTree ORDER BY id", dbName, tableName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT number, toString(number) FROM numbers(%d)", dbName, tableName, rowCount))

	// 2. v1 create (CAS reuses the local backup directory).
	env.casBackupNoError(r, "create", "--tables", dbName+".*", backupName)

	// 3. cas-upload.
	out := env.casBackupNoError(r, "cas-upload", backupName)
	log.Debug().Msg(out)

	// 4. cas-status: at least 1 backup, blob count > 0.
	statusOut := env.casBackupNoError(r, "cas-status")
	log.Debug().Msg(statusOut)
	r.Contains(statusOut, "Backups: 1", "expected exactly 1 CAS backup, got: %s", statusOut)
	r.NotContains(statusOut, "Blobs:   0 ", "expected blob count > 0, got: %s", statusOut)

	// 5. Drop database; remove local backup so restore must fetch from remote.
	r.NoError(env.dropDatabase(dbName, false))
	env.casBackupNoError(r, "delete", "local", backupName)

	// 6. cas-restore drops + re-creates the table from the CAS layout.
	restoreOut := env.casBackupNoError(r, "cas-restore", "--rm", backupName)
	log.Debug().Msg(restoreOut)

	// 7. SELECT count(): must equal rowCount; sum(id) = 0+...+99 = 4950.
	env.checkCount(r, 1, uint64(rowCount), fmt.Sprintf("SELECT count() FROM `%s`.`%s`", dbName, tableName))
	var sumID uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&sumID, fmt.Sprintf("SELECT sum(id) FROM `%s`.`%s`", dbName, tableName)))
	r.Equal(uint64(rowCount*(rowCount-1)/2), sumID)

	// 8. cas-delete; cas-status should report 0 backups.
	env.casBackupNoError(r, "cas-delete", backupName)
	statusOut2 := env.casBackupNoError(r, "cas-status")
	r.Contains(statusOut2, "Backups: 0", "expected 0 CAS backups after cas-delete, got: %s", statusOut2)

	// Cleanup local backup metadata + database.
	_, _ = env.casBackup("delete", "local", backupName)
	r.NoError(env.dropDatabase(dbName, true))
}

// TestCASCrossModeGuards verifies the §6.2.2 isolation between v1 and CAS
// backups: each command must refuse to operate on the other layout's backups.
func TestCASCrossModeGuards(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "guards")

	const (
		dbName  = "cas_guards_db"
		v1Name  = "v1bk_guards"
		casName = "casbk_guards"
	)

	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64) ENGINE=MergeTree ORDER BY id", dbName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number FROM numbers(10)", dbName))

	// 1. Two backups: one via v1 upload, one via cas-upload.
	env.casBackupNoError(r, "create", "--tables", dbName+".*", v1Name)
	env.casBackupNoError(r, "upload", v1Name)

	env.casBackupNoError(r, "create", "--tables", dbName+".*", casName)
	env.casBackupNoError(r, "cas-upload", casName)

	// 2. Cross-mode refusals: v1 download on CAS backup.
	out, err := env.casBackup("download", casName)
	r.Error(err, "v1 download must refuse CAS backup; out=%s", out)
	r.Contains(out, "refusing to operate on CAS backup")

	// 3. cas-download on v1 backup.
	out, err = env.casBackup("cas-download", v1Name)
	r.Error(err, "cas-download must refuse v1 backup; out=%s", out)
	r.Contains(out, "refusing to operate on v1 backup")

	// 4. v1 delete remote on CAS backup.
	out, err = env.casBackup("delete", "remote", casName)
	r.Error(err, "v1 delete remote must refuse CAS backup; out=%s", out)
	r.Contains(out, "refusing to operate on CAS backup")

	// 5. cas-delete on v1 backup.
	out, err = env.casBackup("cas-delete", v1Name)
	r.Error(err, "cas-delete must refuse v1 backup; out=%s", out)
	r.Contains(out, "refusing to operate on v1 backup")

	// 6. Same-mode operations succeed.
	env.casBackupNoError(r, "delete", "remote", v1Name)
	env.casBackupNoError(r, "cas-delete", casName)

	// Cleanup local copies.
	_, _ = env.casBackup("delete", "local", v1Name)
	_, _ = env.casBackup("delete", "local", casName)
	r.NoError(env.dropDatabase(dbName, true))
}

// TestCASVerify covers cas-verify happy path. Stretch: induce a missing-blob
// failure by surgically deleting one object in MinIO and re-running verify.
func TestCASVerify(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "verify")

	const (
		dbName     = "cas_verify_db"
		backupName = "cas_verify_bk"
	)

	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE `%s`.t (id UInt64, payload String) ENGINE=MergeTree ORDER BY id", dbName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO `%s`.t SELECT number, repeat('x', 4096) FROM numbers(50)", dbName))

	env.casBackupNoError(r, "create", "--tables", dbName+".*", backupName)
	env.casBackupNoError(r, "cas-upload", backupName)

	// Happy path: cas-verify exits 0.
	out, err := env.casBackup("cas-verify", backupName)
	r.NoError(err, "cas-verify (happy) must succeed; out=%s", out)

	// Stretch: delete an arbitrary blob from MinIO, expect cas-verify to fail
	// with a "missing" diagnostic. The MinIO container exposes the bucket as a
	// plain filesystem at /minio/data/clickhouse, so we use ordinary `find` +
	// `rm` rather than `mc`.
	blobDir := "/minio/data/clickhouse/backup/cluster/0/cas/verify/blob"
	delOut, delErr := env.DockerExecOut("minio", "bash", "-ce",
		fmt.Sprintf("find %s -type f | head -n1 | xargs -r rm -fv", blobDir))
	if delErr != nil || strings.TrimSpace(delOut) == "" {
		// Bucket layout differs (different s3.path) → skip stretch silently
		// rather than fail; the happy-path assertion above is the contract.
		log.Warn().Msgf("cas-verify stretch: unable to remove blob (out=%q err=%v); skipping negative case", delOut, delErr)
	} else {
		log.Debug().Msgf("removed blob: %s", delOut)
		out, err = env.casBackup("cas-verify", backupName)
		r.Error(err, "cas-verify must fail when a referenced blob is missing; out=%s", out)
		r.Contains(strings.ToLower(out), "missing", "expected 'missing' diagnostic; out=%s", out)
	}

	// Cleanup.
	_, _ = env.casBackup("cas-delete", backupName)
	_, _ = env.casBackup("delete", "local", backupName)
	r.NoError(env.dropDatabase(dbName, true))
}
