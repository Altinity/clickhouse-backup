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

// TestShadowCleanup - test that backup create only cleans its own shadow UUIDs, not foreign ones
// https://github.com/Altinity/clickhouse-backup/issues/1345
func TestShadowCleanup(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))

	backupName := fmt.Sprintf("test_shadow_cleanup_%d", time.Now().UnixNano())

	log.Debug().Msg("Create test table and insert data")
	env.queryWithNoError(t, r, "CREATE TABLE IF NOT EXISTS default.shadow_test(id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO default.shadow_test SELECT number FROM numbers(100)")

	log.Debug().Msg("Clean shadow directory to avoid cross-test contamination")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean", "--all")

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

	log.Debug().Msg("Verify default 'clean' keeps foreign shadow directories, https://github.com/Altinity/clickhouse-backup/issues/1563")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean")
	out, err = env.DockerExecOut("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/shadow/foreign_shadow_dir/store/abc/test/data.txt")
	r.NoError(err, "foreign shadow directory should survive default clean, output: %s", out)

	log.Debug().Msg("Verify 'clean --older-than' removes only old unrecorded shadow directories")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", "mkdir -p /var/lib/clickhouse/shadow/old_foreign_shadow_dir/store && touch -t 202001010000 /var/lib/clickhouse/shadow/old_foreign_shadow_dir")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean", "--older-than=24h", "--dry-run")
	out, err = env.DockerExecOut("clickhouse-backup", "ls", "-d", "/var/lib/clickhouse/shadow/old_foreign_shadow_dir")
	r.NoError(err, "dry-run must not remove anything, output: %s", out)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean", "--older-than=24h")
	out, err = env.DockerExecOut("clickhouse-backup", "ls", "-d", "/var/lib/clickhouse/shadow/old_foreign_shadow_dir")
	r.Error(err, "old unrecorded shadow directory must be removed by --older-than, output: %s", out)
	out, err = env.DockerExecOut("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/shadow/foreign_shadow_dir/store/abc/test/data.txt")
	r.NoError(err, "fresh foreign shadow directory should survive --older-than=24h, output: %s", out)

	log.Debug().Msg("Verify 'clean --all' removes all shadows including foreign ones")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean", "--all")
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls /var/lib/clickhouse/shadow/ 2>/dev/null || true")
	r.NoError(err)
	r.Empty(strings.TrimSpace(out), "clean --all should remove all shadows, but found: %s", out)

	log.Debug().Msg("Cleanup")
	dropQuery := "DROP TABLE IF EXISTS default.shadow_test"
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
		dropQuery += " NO DELAY"
	}
	env.queryWithNoError(t, r, dropQuery)
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
	env.queryWithNoError(t, r, "CREATE TABLE IF NOT EXISTS default.shadow_fail_test(id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO default.shadow_fail_test SELECT number FROM numbers(100)")

	log.Debug().Msg("Clean shadow directory to avoid cross-test contamination")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean", "--all")

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
	dropQuery := "DROP TABLE IF EXISTS default.shadow_fail_test"
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
		dropQuery += " NO DELAY"
	}
	env.queryWithNoError(t, r, dropQuery)
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", fmt.Sprintf("rm -f /var/lib/clickhouse/backup/%s", failBackupName))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean", "--all")
}

// shadowKillScript runs inside the clickhouse-backup container: it starts `create` in the background,
// waits with a pure-bash loop until the backup recorded its first FREEZE (freezes.tmp exists) and the
// frozen shadow/<uuid> directory appeared, then sends __SIGNAL__ to the create process.
// CLICKHOUSE_FREEZE_BY_PART=true turns the FREEZE of the 500-partition table into 500 queries,
// so the window between FREEZE and UNFREEZE lasts seconds and the signal reliably lands inside it.
const shadowKillScript = `
export CLICKHOUSE_FREEZE_BY_PART=true
clickhouse-backup create --tables=default.shadow_kill_test __NAME__ &>/tmp/shadow_kill_create.log &
cpid=$!
SECONDS=0
observed=0
while [ "$SECONDS" -lt 60 ]; do
  if [ -f /var/lib/clickhouse/backup/__NAME__/freezes.tmp ]; then
    for d in /var/lib/clickhouse/shadow/*/; do
      case "$d" in *foreign*) ;; *) observed=1 ;; esac
    done
  fi
  [ "$observed" -eq 1 ] && break
done
echo "OBSERVED=$observed"
kill -__SIGNAL__ $cpid
wait $cpid && echo "EXIT=0" || echo "EXIT=$?"
if [ -f /var/lib/clickhouse/backup/__NAME__/freezes.tmp ]; then echo "FREEZES=EXISTS"; else echo "FREEZES=GONE"; fi
echo "SHADOW=$(ls /var/lib/clickhouse/shadow/ | grep -v foreign | grep -v increment.txt | tr '\n' ' ')"
echo "=== create log"
cat /tmp/shadow_kill_create.log
`

func shadowKillSetup(t *testing.T, r *require.Assertions, env *TestEnvironment) {
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	env.queryWithNoError(t, r, "CREATE TABLE IF NOT EXISTS default.shadow_kill_test(id UInt64, v String) ENGINE=MergeTree() PARTITION BY id % 500 ORDER BY id")
	env.queryWithNoError(t, r, "INSERT INTO default.shadow_kill_test SELECT number, repeat('x', 128) FROM numbers(5000) SETTINGS max_partitions_per_insert_block=500")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean", "--all")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", "mkdir -p /var/lib/clickhouse/shadow/foreign_shadow_kill/store && chown -R clickhouse:clickhouse /var/lib/clickhouse/shadow/")
}

func shadowKillTeardown(t *testing.T, r *require.Assertions, env *TestEnvironment) {
	dropQuery := "DROP TABLE IF EXISTS default.shadow_kill_test"
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
		dropQuery += " NO DELAY"
	}
	env.queryWithNoError(t, r, dropQuery)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean", "--all")
}

func runShadowKill(r *require.Assertions, env *TestEnvironment, backupName, signal string) string {
	script := strings.NewReplacer("__NAME__", backupName, "__SIGNAL__", signal).Replace(shadowKillScript)
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-c", script)
	r.NoError(err, "kill script failed:\n%s", out)
	r.Contains(out, "OBSERVED=1", "expected to observe freezes.tmp and a frozen shadow directory:\n%s", out)
	return out
}

func shadowUUIDDirs(r *require.Assertions, env *TestEnvironment) string {
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls /var/lib/clickhouse/shadow/ | grep -v foreign | grep -v increment.txt || true")
	r.NoError(err)
	return strings.TrimSpace(out)
}

// TestShadowCleanupAfterKill - a `create` killed with SIGKILL leaves shadow/<uuid> and <backup>/freezes.tmp behind,
// `clean` and `clean_local_broken` must unfreeze exactly that shadow and keep foreign directories
// https://github.com/Altinity/clickhouse-backup/issues/1563
func TestShadowCleanupAfterKill(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)
	shadowKillSetup(t, r, env)
	defer shadowKillTeardown(t, r, env)

	backupName := fmt.Sprintf("test_shadow_kill_%d", time.Now().UnixNano())
	log.Debug().Msg("Kill create with SIGKILL after the first FREEZE")
	out := runShadowKill(r, env, backupName, "KILL")
	r.Contains(out, "FREEZES=EXISTS", "freezes.tmp must survive SIGKILL:\n%s", out)
	r.NotEmpty(shadowUUIDDirs(r, env), "the frozen shadow must be orphaned after SIGKILL:\n%s", out)

	log.Debug().Msg("clean --dry-run must not remove anything")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean", "--dry-run")
	r.NotEmpty(shadowUUIDDirs(r, env), "dry-run must keep the orphaned shadow")

	log.Debug().Msg("clean must unfreeze the orphaned shadow, keep the foreign directory and remove freezes.tmp")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean")
	r.Empty(shadowUUIDDirs(r, env), "orphaned shadow must be removed by clean")
	out, err := env.DockerExecOut("clickhouse-backup", "ls", "-d", "/var/lib/clickhouse/shadow/foreign_shadow_kill")
	r.NoError(err, "foreign shadow directory must survive clean: %s", out)
	out, err = env.DockerExecOut("clickhouse-backup", "ls", fmt.Sprintf("/var/lib/clickhouse/backup/%s/freezes.tmp", backupName))
	r.Error(err, "freezes.tmp must be removed after clean: %s", out)

	log.Debug().Msg("Kill create again, clean_local_broken must remove the broken backup together with its shadow")
	out = runShadowKill(r, env, backupName, "KILL")
	r.Contains(out, "FREEZES=EXISTS", "freezes.tmp must survive SIGKILL:\n%s", out)
	r.NotEmpty(shadowUUIDDirs(r, env), "the frozen shadow must be orphaned after SIGKILL:\n%s", out)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean_local_broken")
	r.Empty(shadowUUIDDirs(r, env), "orphaned shadow must be removed by clean_local_broken")
	out, err = env.DockerExecOut("clickhouse-backup", "ls", "-d", fmt.Sprintf("/var/lib/clickhouse/backup/%s", backupName))
	r.Error(err, "broken backup must be removed by clean_local_broken: %s", out)
	out, err = env.DockerExecOut("clickhouse-backup", "ls", "-d", "/var/lib/clickhouse/shadow/foreign_shadow_kill")
	r.NoError(err, "foreign shadow directory must survive clean_local_broken: %s", out)
}

// TestShadowCleanupOnSigterm - SIGTERM cancels the running CLI `create`, which unfreezes its own shadow before exit
// https://github.com/Altinity/clickhouse-backup/issues/1563
func TestShadowCleanupOnSigterm(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)
	shadowKillSetup(t, r, env)
	defer shadowKillTeardown(t, r, env)

	backupName := fmt.Sprintf("test_shadow_sigterm_%d", time.Now().UnixNano())
	out := runShadowKill(r, env, backupName, "TERM")
	r.NotContains(out, "EXIT=0", "create must exit with error after SIGTERM:\n%s", out)
	r.Contains(out, "FREEZES=GONE", "create must remove freezes.tmp on SIGTERM:\n%s", out)
	r.Empty(shadowUUIDDirs(r, env), "create must unfreeze its shadow on SIGTERM:\n%s", out)
	out, err := env.DockerExecOut("clickhouse-backup", "ls", "-d", "/var/lib/clickhouse/shadow/foreign_shadow_kill")
	r.NoError(err, "foreign shadow directory must survive SIGTERM cleanup: %s", out)
	_ = env.DockerExec("clickhouse-backup", "clickhouse-backup", "delete", "local", backupName)
}

// TestShadowCleanupSkipsInProgress - `clean` must not touch the shadow of a `create` which is still running
// https://github.com/Altinity/clickhouse-backup/issues/1563
func TestShadowCleanupSkipsInProgress(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)
	shadowKillSetup(t, r, env)
	defer shadowKillTeardown(t, r, env)

	backupName := fmt.Sprintf("test_shadow_inprogress_%d", time.Now().UnixNano())
	script := strings.NewReplacer("__NAME__", backupName).Replace(`
export CLICKHOUSE_FREEZE_BY_PART=true
clickhouse-backup create --tables=default.shadow_kill_test __NAME__ &>/tmp/shadow_inprogress_create.log &
cpid=$!
SECONDS=0
observed=0
while [ "$SECONDS" -lt 60 ]; do
  if [ -f /var/lib/clickhouse/backup/__NAME__/freezes.tmp ]; then observed=1; break; fi
done
echo "OBSERVED=$observed"
echo "=== clean output"
CLICKHOUSE_FREEZE_BY_PART=false clickhouse-backup clean --older-than=1ms 2>&1
echo "=== clean done"
wait $cpid && echo "EXIT=0" || echo "EXIT=$?"
echo "=== create log"
cat /tmp/shadow_inprogress_create.log
`)
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-c", script)
	r.NoError(err, "script failed:\n%s", out)
	r.Contains(out, "OBSERVED=1", "expected to observe freezes.tmp of the running create:\n%s", out)
	r.Contains(out, "another clickhouse-backup process", "clean must skip the backup in progress:\n%s", out)
	r.Contains(out, "skip --older-than cleanup", "clean must skip the --older-than sweep while a create is in progress:\n%s", out)
	r.Contains(out, "EXIT=0", "create must finish successfully despite the concurrent clean:\n%s", out)
	r.Empty(shadowUUIDDirs(r, env), "successful create must leave no shadow")
	out, err = env.DockerExecOut("clickhouse-backup", "ls", fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata.json", backupName))
	r.NoError(err, "backup must be complete: %s", out)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", backupName)
}
