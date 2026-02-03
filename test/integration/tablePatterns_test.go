//go:build integration

package main

import (
	"fmt"
	"testing"
	"time"
)

func TestTablePatterns(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	testBackupName := "test_backup_patterns"
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameReplicated, dbNameMySQL, dbNamePostgreSQL, Issue331Issue1091Atomic, Issue331Issue1091Ordinary}
	var dbNameOrdinaryTest = dbNameOrdinary + "_" + t.Name()
	var dbNameAtomicTest = dbNameAtomic + "_" + t.Name()
	for _, createPattern := range []bool{true, false} {
		for _, restorePattern := range []bool{true, false} {
			fullCleanup(t, r, env, []string{testBackupName}, []string{"remote", "local"}, databaseList, true, false, false, "config-s3.yml")
			generateTestData(t, r, env, "S3", false, defaultTestData)
			if createPattern {
				env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create_remote", "--rbac", "--tables", " "+dbNameOrdinaryTest+".*", testBackupName)
				out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", "--tables", " "+dbNameOrdinaryTest+".*", testBackupName)
				r.NoError(err, "%s\nunexpected tables error: %v", out, err)
				r.Contains(out, dbNameOrdinaryTest)
				r.NotContains(out, dbNameAtomicTest)
				out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", "--remote-backup", testBackupName, "--tables", " "+dbNameOrdinaryTest+".*", testBackupName)
				r.NoError(err, "%s\nunexpected tables --remote-backup error: %v", out, err)
				r.Contains(out, dbNameOrdinaryTest)
				r.NotContains(out, dbNameAtomicTest)
			} else {
				env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create_remote", "--rbac", testBackupName)
				out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", testBackupName)
				r.NoError(err, "%s\nunexpected tables error: %v", out, err)
				r.Contains(out, dbNameOrdinaryTest)
				r.Contains(out, dbNameAtomicTest)
				out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", "--remote-backup", testBackupName, testBackupName)
				r.NoError(err, "%s\nunexpected tables --remote-backup error: %v", out, err)
				r.Contains(out, dbNameOrdinaryTest)
				r.Contains(out, dbNameAtomicTest)
			}

			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", testBackupName)
			dropDatabasesFromTestDataDataSet(t, r, env, databaseList, true)

			if restorePattern {
				env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore_remote", "--tables", " "+dbNameOrdinaryTest+".*", testBackupName)
			} else {
				env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore_remote", testBackupName)
			}

			restored := uint64(0)
			r.NoError(env.ch.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameOrdinaryTest)))
			r.NotZero(restored)

			if createPattern || restorePattern {
				restored = 0
				r.NoError(env.ch.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameAtomicTest)))
				// todo, old versions of clickhouse will return empty recordset
				r.Zero(restored)

				restored = 0
				r.NoError(env.ch.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.databases WHERE name='%s'", dbNameAtomicTest)))
				// todo, old versions of clickhouse will return empty recordset
				r.Zero(restored)
			} else {
				restored = 0
				r.NoError(env.ch.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameAtomicTest)))
				r.NotZero(restored)
			}

			fullCleanup(t, r, env, []string{testBackupName}, []string{"remote", "local"}, databaseList, true, true, true, "config-s3.yml")

		}
	}
	env.checkObjectStorageIsEmpty(t, r, "S3")
	env.Cleanup(t, r)
}
