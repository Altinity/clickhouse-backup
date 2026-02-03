//go:build integration

package main

import (
	"os"
	"testing"
	"time"
)

func TestProjections(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") == -1 {
		t.Skipf("Test skipped, PROJECTION available only 21.8+, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	var err error
	var counts uint64
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "24.3") >= 0 {
		isProjectionExists := func(expectedErr bool) {
			err = env.DockerExec("clickhouse-backup", "bash", "-ec", "ls -l /var/lib/clickhouse/backup/test_skip_projections/shadow/default/table_with_projection/default/*/*.proj/*.*")
			if expectedErr {
				r.Error(err)
			} else {
				r.NoError(err)
			}
		}
		// create --skip-projection
		env.queryWithNoError(r, "CREATE TABLE default.table_with_projection(dt DateTime, v UInt64, PROJECTION x (SELECT toStartOfMonth(dt) m, sum(v) GROUP BY m)) ENGINE=MergeTree() PARTITION BY toYYYYMMDD(dt) ORDER BY dt")
		env.queryWithNoError(r, "INSERT INTO default.table_with_projection SELECT today() - INTERVAL number DAY, number FROM numbers(5)")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create", "--skip-projections", "default.*", "test_skip_projections")
		isProjectionExists(true)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "upload", "--delete-source", "test_skip_projections")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "download", "test_skip_projections")
		isProjectionExists(true)
		env.queryWithNoError(r, "DROP TABLE default.table_with_projection NO DELAY")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore", "test_skip_projections")
		counts = 0
		r.NoError(env.ch.SelectSingleRowNoCtx(&counts, "SELECT count() FROM default.table_with_projection"))
		r.Equal(uint64(5), counts)
		env.queryWithNoError(r, "DROP TABLE default.table_with_projection NO DELAY")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_skip_projections")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", "test_skip_projections")

		// upload --skip-projection
		env.queryWithNoError(r, "CREATE TABLE default.table_with_projection(dt DateTime, v UInt64, PROJECTION x (SELECT toStartOfMonth(dt) m, sum(v) GROUP BY m)) ENGINE=MergeTree() PARTITION BY toYYYYMMDD(dt) ORDER BY dt")
		env.queryWithNoError(r, "INSERT INTO default.table_with_projection SELECT today() - INTERVAL number DAY, number FROM numbers(5)")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create", "test_skip_projections")
		isProjectionExists(false)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "upload", "--skip-projections", "default.*", "--delete-source", "test_skip_projections")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "download", "test_skip_projections")
		isProjectionExists(true)
		env.queryWithNoError(r, "DROP TABLE default.table_with_projection NO DELAY")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore", "test_skip_projections")
		counts = 0
		r.NoError(env.ch.SelectSingleRowNoCtx(&counts, "SELECT count() FROM default.table_with_projection"))
		r.Equal(uint64(5), counts)
		env.queryWithNoError(r, "DROP TABLE default.table_with_projection NO DELAY")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_skip_projections")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", "test_skip_projections")

		// restore --skip-projection
		env.queryWithNoError(r, "CREATE TABLE default.table_with_projection(dt DateTime, v UInt64, PROJECTION x (SELECT toStartOfMonth(dt) m, sum(v) GROUP BY m)) ENGINE=MergeTree() PARTITION BY toYYYYMMDD(dt) ORDER BY dt")
		env.queryWithNoError(r, "INSERT INTO default.table_with_projection SELECT today() - INTERVAL number DAY, number FROM numbers(5)")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create", "test_skip_projections")
		isProjectionExists(false)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "upload", "--delete-source", "test_skip_projections")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "download", "test_skip_projections")
		isProjectionExists(false)
		env.queryWithNoError(r, "DROP TABLE default.table_with_projection NO DELAY")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore", "--skip-projections", "default.*", "test_skip_projections")
		counts = 0
		r.NoError(env.ch.SelectSingleRowNoCtx(&counts, "SELECT count() FROM default.table_with_projection"))
		r.Equal(uint64(5), counts)
		env.queryWithNoError(r, "DROP TABLE default.table_with_projection NO DELAY")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_skip_projections")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", "test_skip_projections")
	}

	// other cases
	env.queryWithNoError(r, "CREATE TABLE default.table_with_projection(dt DateTime, v UInt64, PROJECTION x (SELECT toStartOfMonth(dt) m, sum(v) GROUP BY m)) ENGINE=MergeTree() PARTITION BY toYYYYMMDD(dt) ORDER BY dt")
	env.queryWithNoError(r, "INSERT INTO default.table_with_projection SELECT today() - INTERVAL number DAY, number FROM numbers(5)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create_remote", "test_backup_projection_full")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_backup_projection_full")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore_remote", "--rm", "test_backup_projection_full")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_backup_projection_full")

	env.queryWithNoError(r, "INSERT INTO default.table_with_projection SELECT today() - INTERVAL number WEEK, number FROM numbers(5)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create_remote", "--diff-from-remote", "test_backup_projection_full", "test_backup_projection_increment")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_backup_projection_increment")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore_remote", "--rm", "test_backup_projection_increment")

	counts = 0
	r.NoError(env.ch.SelectSingleRowNoCtx(&counts, "SELECT count() FROM default.table_with_projection"))
	r.Equal(uint64(10), counts)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.9") >= 0 {
		counts = 0
		r.NoError(env.ch.SelectSingleRowNoCtx(&counts, "SELECT count() FROM system.parts WHERE database='default' AND table='table_with_projection' AND has(projections,'x')"))
		r.Equal(uint64(10), counts)
	}

	env.queryWithNoError(r, "DROP TABLE default.table_with_projection NO DELAY")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", "test_backup_projection_increment")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", "test_backup_projection_full")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_backup_projection_increment")

	env.Cleanup(t, r)
}
