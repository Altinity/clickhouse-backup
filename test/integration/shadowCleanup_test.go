//go:build integration

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestShadowCleanup - test that backup create only cleans its own shadow UUIDs, not foreign ones
// https://github.com/Altinity/clickhouse-backup/issues/1345
func TestShadowCleanup(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))

	backupName := fmt.Sprintf("test_shadow_cleanup_%d", time.Now().UnixNano())

	log.Debug().Msg("Create test table and insert data")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS default.shadow_test(id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "INSERT INTO default.shadow_test SELECT number FROM numbers(100)")

	log.Debug().Msg("Clean shadow directory to avoid cross-test contamination")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean")

	log.Debug().Msg("Create a foreign shadow directory to simulate another process")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", "mkdir -p /var/lib/clickhouse/shadow/foreign_shadow_dir/store/abc/test && chown -R clickhouse:clickhouse /var/lib/clickhouse/shadow/ && echo test > /var/lib/clickhouse/shadow/foreign_shadow_dir/store/abc/test/data.txt")

	// verify foreign shadow exists before backup
	out, err := env.DockerExecOut("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/shadow/foreign_shadow_dir/store/abc/test/data.txt")
	r.NoError(err, "foreign shadow should exist before backup: %s", out)

	log.Debug().Msg("Create backup (should only clean its own shadow UUIDs)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create", backupName)

	log.Debug().Msg("Verify foreign shadow directory still exists after backup create")
	out, err = env.DockerExecOut("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/shadow/foreign_shadow_dir/store/abc/test/data.txt")
	r.NoError(err, "foreign shadow directory should still exist after backup create, output: %s", out)

	log.Debug().Msg("Verify no backup-specific shadow UUIDs remain (only foreign_shadow_dir and increment.txt allowed)")
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls /var/lib/clickhouse/shadow/ | grep -v foreign_shadow_dir | grep -v increment.txt || true")
	r.NoError(err)
	filtered := strings.TrimSpace(out)
	r.Empty(filtered, "only foreign_shadow_dir and increment.txt should remain in shadow, but found: %s", filtered)

	log.Debug().Msg("Verify explicit 'clean' command removes all shadows including foreign ones")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean")
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls /var/lib/clickhouse/shadow/ 2>/dev/null || true")
	r.NoError(err)
	r.Empty(strings.TrimSpace(out), "explicit clean should remove all shadows, but found: %s", out)

	log.Debug().Msg("Cleanup")
	env.queryWithNoError(r, "DROP TABLE IF EXISTS default.shadow_test NO DELAY")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", backupName)
}

// TestShadowCleanupOnFailure - test that failed backup only cleans its own shadow UUIDs
// https://github.com/Altinity/clickhouse-backup/issues/1345
func TestShadowCleanupOnFailure(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))

	log.Debug().Msg("Create test table and insert data")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS default.shadow_fail_test(id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "INSERT INTO default.shadow_fail_test SELECT number FROM numbers(100)")

	log.Debug().Msg("Clean shadow directory to avoid cross-test contamination")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean")

	log.Debug().Msg("Create a foreign shadow directory to simulate another process")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", "mkdir -p /var/lib/clickhouse/shadow/foreign_shadow_fail/store/abc/test && chown -R clickhouse:clickhouse /var/lib/clickhouse/shadow/ && echo test > /var/lib/clickhouse/shadow/foreign_shadow_fail/store/abc/test/data.txt")

	log.Debug().Msg("Create a file at backup path to trigger failure after freeze")
	failBackupName := fmt.Sprintf("test_shadow_fail_%d", time.Now().UnixNano())
	// create a regular file (not directory) at the backup path - backup will fail when trying to write metadata inside it
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", fmt.Sprintf("mkdir -p /var/lib/clickhouse/backup && echo 'x' > /var/lib/clickhouse/backup/%s", failBackupName))

	log.Debug().Msg("Attempt backup create (should fail because backup path is a file, not a directory)")
	err := env.DockerExec("clickhouse-backup", "clickhouse-backup", "create", failBackupName)
	r.Error(err, "backup create should fail because backup path is a regular file")

	log.Debug().Msg("Verify foreign shadow directory still exists after failed backup")
	out, err := env.DockerExecOut("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/shadow/foreign_shadow_fail/store/abc/test/data.txt")
	r.NoError(err, "foreign shadow directory should survive failed backup, output: %s", out)

	log.Debug().Msg("Verify no backup-specific shadow UUIDs remain after failed backup cleanup")
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls /var/lib/clickhouse/shadow/ | grep -v foreign_shadow_fail | grep -v increment.txt || true")
	r.NoError(err)
	filtered := strings.TrimSpace(out)
	r.Empty(filtered, "only foreign_shadow_fail and increment.txt should remain in shadow after failed backup, but found: %s", filtered)

	log.Debug().Msg("Cleanup")
	env.queryWithNoError(r, "DROP TABLE IF EXISTS default.shadow_fail_test NO DELAY")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", fmt.Sprintf("rm -f /var/lib/clickhouse/backup/%s", failBackupName))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean")
}
