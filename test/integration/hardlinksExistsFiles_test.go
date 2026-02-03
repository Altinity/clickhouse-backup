//go:build integration

package main

import (
	"encoding/json"
	"os"
	"path"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/rs/zerolog/log"
)

func TestHardlinksExistsFiles(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	for _, compression := range []string{"tar", "none"} {
		baseBackupName := "test_hardlinks_base_" + compression
		incrementBackupName := "test_hardlinks_increment_" + compression
		dbNameShort := "test_hardlinks_db"
		dbNameFull := dbNameShort + "_" + t.Name()
		tableName := "test_hardlinks_table"

		// Create table and data
		settings := ""
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.1") >= 0 {
			settings = " SETTINGS storage_policy='hot_and_cold'"
		}

		env.queryWithNoError(r, "CREATE DATABASE "+dbNameFull)
		env.queryWithNoError(r, "CREATE TABLE "+dbNameFull+"."+tableName+" (id UInt64) ENGINE=MergeTree() ORDER BY id"+settings)
		env.queryWithNoError(r, "INSERT INTO "+dbNameFull+"."+tableName+" SELECT number FROM numbers(100)")

		// Create base backup
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+dbNameFull+".*", baseBackupName)

		// Check checksums in metadata for base backup
		metadataFile := path.Join("/var/lib/clickhouse/backup", baseBackupName, "metadata", common.TablePathEncode(dbNameFull), common.TablePathEncode(tableName)+".json")
		out, err := env.DockerExecOut("clickhouse-backup", "cat", metadataFile)
		r.NoError(err)
		var tableMeta struct {
			Checksums map[string]uint64 `json:"checksums"`
			Parts     map[string][]struct {
				Name string `json:"name"`
			} `json:"parts"`
		}
		r.NoError(json.Unmarshal([]byte(out), &tableMeta))
		r.NotEmpty(tableMeta.Checksums, "checksums should not be empty")
		r.Greater(len(tableMeta.Parts["default"]), 0)
		for _, part := range tableMeta.Parts["default"] {
			r.Contains(tableMeta.Checksums, part.Name)
		}

		// Upload base backup
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "S3_COMPRESSION_FORMAT="+compression+" clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload "+baseBackupName)

		// Add more data for increment
		env.queryWithNoError(r, "INSERT INTO "+dbNameFull+"."+tableName+" SELECT number+100 FROM numbers(100)")

		// Create increment backup
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+dbNameFull+".*", "--diff-from-remote="+baseBackupName, incrementBackupName)

		// Upload increment backup
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "S3_COMPRESSION_FORMAT="+compression+" clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload --diff-from="+baseBackupName+" "+incrementBackupName)

		// move parts to another disk
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.1") >= 0 {
			env.queryWithNoError(r, "ALTER TABLE "+dbNameFull+"."+tableName+" MOVE PART 'all_1_1_0' TO DISK 'hdd2'")
			env.queryWithNoError(r, "ALTER TABLE "+dbNameFull+"."+tableName+" MOVE PART 'all_2_2_0' TO DISK 'hdd1'")
		}

		// Delete local backups
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", baseBackupName)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", incrementBackupName)

		// Download increment with --hardlink-exists-files and disk rebalance
		downloadOut, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", "--hardlink-exists-files", incrementBackupName)
		log.Debug().Msg(downloadOut)
		r.NoError(err, downloadOut)
		r.Contains(downloadOut, "Found existing part")
		r.Contains(downloadOut, "creating hardlinks")

		// Restore increment to check data integrity
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--tables="+dbNameFull+"."+tableName, incrementBackupName)
		// Should have 200 rows (100 from base + 100 from increment)
		env.checkCount(r, 1, 200, "SELECT count() FROM "+dbNameFull+"."+tableName)

		// Download base with --hardlink-exists-files and disk rebalance
		downloadOut, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", "--hardlink-exists-files", baseBackupName)
		log.Debug().Msg(downloadOut)
		r.NoError(err, downloadOut)
		r.Contains(downloadOut, "Found existing part")
		r.Contains(downloadOut, "creating hardlinks")

		// Restore increment to check data integrity
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--tables="+dbNameFull+"."+tableName, baseBackupName)
		// Should have 200 rows (100 from base + 100 from increment)
		env.checkCount(r, 1, 100, "SELECT count() FROM "+dbNameFull+"."+tableName)

		// Cleanup after test
		fullCleanup(t, r, env, []string{baseBackupName, incrementBackupName}, []string{"remote", "local"}, []string{dbNameShort}, true, true, true, "config-s3.yml")
	}
	env.Cleanup(t, r)
}
