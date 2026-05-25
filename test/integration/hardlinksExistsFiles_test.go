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

		// Check checksums in metadata for base backup. ClickHouse versions >= 19.11
		// expose system.parts.hash_of_all_files, so clickhouse-backup populates the
		// new "hash_of_all_files" map instead of the legacy CRC64 "checksums" map.
		metadataFile := path.Join("/var/lib/clickhouse/backup", baseBackupName, "metadata", common.TablePathEncode(dbNameFull), common.TablePathEncode(tableName)+".json")
		out, err := env.DockerExecOut("clickhouse-backup", "cat", metadataFile)
		r.NoError(err)
		var tableMeta struct {
			Checksums      map[string]uint64 `json:"checksums"`
			HashOfAllFiles map[string]string `json:"hash_of_all_files"`
			Parts          map[string][]struct {
				Name string `json:"name"`
			} `json:"parts"`
		}
		r.NoError(json.Unmarshal([]byte(out), &tableMeta))
		r.Greater(len(tableMeta.Parts["default"]), 0)
		useHashOfAllFiles := compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.11") >= 0
		if useHashOfAllFiles {
			r.Empty(tableMeta.Checksums, "checksums should be empty for ClickHouse >= 19.11")
			r.NotEmpty(tableMeta.HashOfAllFiles, "hash_of_all_files should not be empty for ClickHouse >= 19.11")
			for _, part := range tableMeta.Parts["default"] {
				r.Contains(tableMeta.HashOfAllFiles, part.Name)
				hexStr := tableMeta.HashOfAllFiles[part.Name]
				r.Len(hexStr, 32, "hash_of_all_files must be 32 hex chars for part %s, got %q", part.Name, hexStr)
				// Stored value is exactly what ClickHouse prints in system.parts; we just
				// verify the post-FREEZE SELECT survived the round-trip into metadata.
				var liveHash string
				r.NoError(env.ch.SelectSingleRowNoCtx(&liveHash, "SELECT lower(hash_of_all_files) FROM system.parts WHERE database=? AND `table`=? AND name=? AND active LIMIT 1", dbNameFull, tableName, part.Name))
				r.Equal(liveHash, hexStr, "hash_of_all_files for part %s must match system.parts", part.Name)
			}
		} else {
			r.NotEmpty(tableMeta.Checksums, "checksums should not be empty for ClickHouse < 19.11")
			r.Empty(tableMeta.HashOfAllFiles, "hash_of_all_files should be empty for ClickHouse < 19.11")
			for _, part := range tableMeta.Parts["default"] {
				r.Contains(tableMeta.Checksums, part.Name)
			}
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
		if useHashOfAllFiles {
			r.Contains(downloadOut, "hash_of_all_files match")
		} else {
			r.Contains(downloadOut, "Found existing part")
			r.Contains(downloadOut, "creating hardlinks")
		}

		// Restore increment to check data integrity
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--tables="+dbNameFull+"."+tableName, incrementBackupName)
		// Should have 200 rows (100 from base + 100 from increment)
		env.checkCount(r, 1, 200, "SELECT count() FROM "+dbNameFull+"."+tableName)

		// Download base with --hardlink-exists-files and disk rebalance
		downloadOut, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", "--hardlink-exists-files", baseBackupName)
		log.Debug().Msg(downloadOut)
		r.NoError(err, downloadOut)
		if useHashOfAllFiles {
			r.Contains(downloadOut, "hash_of_all_files match")
		} else {
			r.Contains(downloadOut, "Found existing part")
			r.Contains(downloadOut, "creating hardlinks")
		}

		// Restore increment to check data integrity
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--tables="+dbNameFull+"."+tableName, baseBackupName)
		// Should have 200 rows (100 from base + 100 from increment)
		env.checkCount(r, 1, 100, "SELECT count() FROM "+dbNameFull+"."+tableName)

		// Branch: download must hardlink from an existing live part whose name
		// differs from backup metadata but whose hash_of_all_files matches.
		// TRUNCATE+re-INSERT advances block numbers, so identical data lands
		// under a new part name while hash_of_all_files stays the same.
		if useHashOfAllFiles {
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", baseBackupName)

			env.queryWithNoError(r, "TRUNCATE TABLE "+dbNameFull+"."+tableName)
			env.queryWithNoError(r, "INSERT INTO "+dbNameFull+"."+tableName+" SELECT number FROM numbers(100)")
			var renamedPart string
			r.NoError(env.ch.SelectSingleRowNoCtx(&renamedPart, "SELECT name FROM system.parts WHERE database=? AND `table`=? AND active ORDER BY name LIMIT 1", dbNameFull, tableName))
			r.NotEqual("all_1_1_0", renamedPart, "expected new part name after TRUNCATE+INSERT but got %q", renamedPart)

			downloadOut, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", "--hardlink-exists-files", baseBackupName)
			log.Debug().Msg(downloadOut)
			r.NoError(err, downloadOut)
			r.Contains(downloadOut, "hash_of_all_files match", "expected hash_of_all_files match for renamed live part")
			r.Contains(downloadOut, "live part \""+renamedPart+"\"", "expected hash match to point at renamed live part %q", renamedPart)

			// Restore on top of the truncated+reinserted table to confirm the hardlinked parts are usable.
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--tables="+dbNameFull+"."+tableName, baseBackupName)
			env.checkCount(r, 1, 100, "SELECT count() FROM "+dbNameFull+"."+tableName)
		}

		// Cleanup after test
		fullCleanup(t, r, env, []string{baseBackupName, incrementBackupName}, []string{"remote", "local"}, []string{dbNameShort}, true, true, true, "config-s3.yml")
	}
	env.Cleanup(t, r)
}
