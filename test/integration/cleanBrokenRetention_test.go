//go:build integration

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestCleanBrokenRetention verifies that `clean_broken_retention`:
//   - lists orphans (dry-run) without deleting,
//   - on --commit removes orphans from both `path` and `object_disks_path`,
//   - preserves the live backup and entries matched by --keep globs.
//
// Runs against S3 (minio) — the remote-storage-agnostic logic lives in pkg/backup
// and reuses the BatchDeleter pipeline already exercised by every backend.
func TestCleanBrokenRetention(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))

	const tableName = "default.clean_broken_retention_test"
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id UInt64) ENGINE=MergeTree() ORDER BY id", tableName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO %s SELECT number FROM numbers(50)", tableName))

	keepBackup := fmt.Sprintf("keep_backup_%d", time.Now().UnixNano())
	orphanPath := fmt.Sprintf("orphan_path_%d", time.Now().UnixNano())
	orphanObj := fmt.Sprintf("orphan_obj_%d", time.Now().UnixNano())
	orphanKept := fmt.Sprintf("orphan_keep_%d", time.Now().UnixNano())

	// macros from configs/clickhouse-config.xml: {cluster}=cluster, {shard}=0
	const pathPrefix = "/minio/data/clickhouse/backup/cluster/0"
	const objPrefix = "/minio/data/clickhouse/object_disk/cluster/0"

	log.Debug().Msg("Create a live backup that must survive the cleanup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create_remote", "--tables", tableName, keepBackup)

	log.Debug().Msg("Plant orphans directly on the minio filesystem")
	plant := func(parent, name string) {
		env.DockerExecNoError(r, "minio", "bash", "-c", fmt.Sprintf(
			"mkdir -p %s/%s/sub && echo garbage > %s/%s/data.bin && echo garbage > %s/%s/sub/nested.bin",
			parent, name, parent, name, parent, name,
		))
	}
	plant(pathPrefix, orphanPath)
	plant(objPrefix, orphanObj)
	plant(pathPrefix, orphanKept)
	plant(objPrefix, orphanKept)

	assertExists := func(parent, name string) {
		out, err := env.DockerExecOut("minio", "ls", parent+"/"+name)
		r.NoError(err, "expected %s/%s to exist, output: %s", parent, name, out)
	}
	assertGone := func(parent, name string) {
		out, err := env.DockerExecOut("minio", "bash", "-c", fmt.Sprintf("ls %s/%s 2>/dev/null || true", parent, name))
		r.Empty(strings.TrimSpace(out), "expected %s/%s to be deleted, but ls returned: %s", parent, name, out)
		_ = err
	}

	assertExists(pathPrefix, orphanPath)
	assertExists(objPrefix, orphanObj)
	assertExists(pathPrefix, orphanKept)
	assertExists(objPrefix, orphanKept)
	assertExists(pathPrefix, keepBackup)

	log.Debug().Msg("Dry-run: should report orphans but not delete them")
	dryRunOut, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "clean_broken_retention")
	r.NoError(err, "dry-run failed: %s", dryRunOut)
	r.Contains(dryRunOut, orphanPath, "dry-run output must mention path orphan")
	r.Contains(dryRunOut, orphanObj, "dry-run output must mention object disk orphan")
	r.Contains(dryRunOut, "would delete", "dry-run must announce planned deletions")
	r.NotContains(dryRunOut, "clean_broken_retention: deleting", "dry-run must not perform deletion")
	r.NotContains(dryRunOut, fmt.Sprintf("would delete %s", keepBackup), "live backup must never be marked as orphan")

	assertExists(pathPrefix, orphanPath)
	assertExists(objPrefix, orphanObj)

	log.Debug().Msg("Commit with --keep glob: orphan_keep_* must survive, the other two must be removed")
	keepGlob := strings.SplitN(orphanKept, "_", 3)[0] + "_" + strings.SplitN(orphanKept, "_", 3)[1] + "_*"
	commitOut, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "clean_broken_retention", "--commit", "--keep="+keepGlob)
	r.NoError(err, "commit failed: %s", commitOut)
	r.Contains(commitOut, "clean_broken_retention: deleting")

	assertGone(pathPrefix, orphanPath)
	assertGone(objPrefix, orphanObj)
	assertExists(pathPrefix, orphanKept)
	assertExists(objPrefix, orphanKept)
	assertExists(pathPrefix, keepBackup)

	log.Debug().Msg("Cleanup: drop the kept orphan via second commit run (no --keep), then remove the live backup and table")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean_broken_retention", "--commit")
	assertGone(pathPrefix, orphanKept)
	assertGone(objPrefix, orphanKept)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", keepBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", keepBackup)
	env.queryWithNoError(r, "DROP TABLE IF EXISTS "+tableName+" NO DELAY")
	env.checkObjectStorageIsEmpty(t, r, "S3")
}

