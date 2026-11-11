//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestDownloadSplitPartsAcrossArchives covers https://github.com/Altinity/clickhouse-backup/issues/1633 with
// upload_by_part=false: a small max_file_size splits the files of one part over several archives, all archives
// of a table are extracted concurrently into one directory, so extraction must merge into an existing part
// directory instead of replacing it. Downloading an incremental backup goes through the diff flow, which
// downloads every archive of the required backup into the same table directory of the required backup.
func TestDownloadSplitPartsAcrossArchives(t *testing.T) {
	chVersion := os.Getenv("CLICKHOUSE_VERSION")
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	const configFile = "/etc/clickhouse-backup/config-s3.yml"
	cfgPath, _ := env.resolveConfigPaths(r, "config-s3.yml")
	// MinIO only sees objects that went through its S3 API, so use `mc`; config path is relative to bucket
	root := "local/clickhouse/" + cfgPath
	const mcAliasCmd = "mc alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null 2>&1"

	suffix := fmt.Sprintf("%s_%d", strings.ReplaceAll(chVersion, ".", "_"), time.Now().UnixNano())
	dbName := "test_split_parts_" + suffix
	tableShort := "t"
	tableName := dbName + "." + tableShort
	fullBackup := "split_parts_full_" + suffix
	incrBackup := "split_parts_incr_" + suffix
	// many small archives, each wide part spans several of them
	const uploadEnv = "UPLOAD_BY_PART=false MAX_FILE_SIZE=16384 UPLOAD_CONCURRENCY=4"
	backupCmd := func(envVars string, args ...string) string {
		cmd := fmt.Sprintf("%s LOG_LEVEL=debug clickhouse-backup -c %s %s 2>&1", envVars, configFile, strings.Join(args, " "))
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", cmd)
		r.NoError(err, "%s: %s", cmd, out)
		return out
	}

	// runs before env.Cleanup returns the environment to the pool, children first for the required_backup guard
	defer func() {
		for _, cmd := range []string{
			"delete local " + incrBackup,
			"delete local " + fullBackup,
			"delete remote " + incrBackup,
			"delete remote " + fullBackup,
		} {
			if out, err := env.DockerExecOut("clickhouse-backup", "bash", "-c", fmt.Sprintf("clickhouse-backup -c %s %s", configFile, cmd)); err != nil {
				log.Warn().Err(err).Str("cmd", cmd).Msgf("downloadSplitPartsAcrossArchives teardown: %s", out)
			}
		}
		if err := env.dropDatabase(dbName, true); err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("downloadSplitPartsAcrossArchives teardown: drop database")
		}
	}()

	wideSettings := ""
	if compareVersion(chVersion, "20.3") >= 0 {
		wideSettings = " SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0"
	}
	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s (id UInt64, s String, f Float64, d Date, a Array(UInt32), n Nullable(Int64)) ENGINE=MergeTree() ORDER BY id%s", tableName, wideSettings))
	// keep the parts of the full backup alive, so the incremental marks them as required
	env.queryWithNoError(t, r, "SYSTEM STOP MERGES "+tableName)
	insert := func(from, count int) {
		env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s SELECT number, toString(cityHash64(number)), number / 7, toDate('2026-01-01') + number %% 300, range(number %% 10), if(number %% 3 = 0, NULL, toInt64(number)) FROM numbers(%d, %d)", tableName, from, count))
	}
	for i := 0; i < 3; i++ {
		insert(i*20000, 20000)
	}

	backupCmd(uploadEnv, "create_remote", "--tables="+tableName, fullBackup)
	var fullParts uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&fullParts, fmt.Sprintf("SELECT count() FROM system.parts WHERE database='%s' AND table='%s' AND active SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName, tableShort)))
	r.Equal(uint64(3), fullParts)

	// archives hold whole parts only if there are not more archives than parts
	shadowDir := fmt.Sprintf("%s/%s/shadow/%s/%s/", root, fullBackup, dbName, tableShort)
	lsOut, err := env.DockerExecOut("minio", "bash", "-c", fmt.Sprintf("%s && mc ls %s", mcAliasCmd, shadowDir))
	r.NoError(err, "mc ls %s: %s", shadowDir, lsOut)
	archives := 0
	for _, line := range strings.Split(strings.TrimSpace(lsOut), "\n") {
		if fields := strings.Fields(line); len(fields) > 0 && strings.HasPrefix(fields[len(fields)-1], "default_") {
			archives++
		}
	}
	r.Greater(archives, int(fullParts), "expected parts split across archives in %s, got: %s", shadowDir, lsOut)

	for i := 3; i < 5; i++ {
		insert(i*20000, 20000)
	}
	backupCmd(uploadEnv, "create_remote", "--tables="+tableName, "--diff-from-remote="+fullBackup, incrBackup)

	var expectedCount, expectedHash uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&expectedCount, "SELECT count() FROM "+tableName))
	r.NoError(env.ch.SelectSingleRowNoCtx(&expectedHash, "SELECT sum(cityHash64(*)) FROM "+tableName))
	r.Equal(uint64(100000), expectedCount)

	backupCmd("", "delete", "local", incrBackup)
	backupCmd("", "delete", "local", fullBackup)

	// download_by_part=true downloads the required parts through the diff flow
	backupCmd("DOWNLOAD_BY_PART=true DOWNLOAD_CONCURRENCY=4", "download", incrBackup)
	leftovers, err := env.DockerExecOut("clickhouse-backup", "bash", "-c", "find /var/lib/clickhouse/backup -name '.tmp_extract_*'")
	r.NoError(err, leftovers)
	r.Empty(strings.TrimSpace(leftovers), "temporary extraction directories must not remain after download")

	backupCmd("", "restore", "--rm", "--tables="+tableName, incrBackup)
	var restoredCount, restoredHash uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&restoredCount, "SELECT count() FROM "+tableName))
	r.NoError(env.ch.SelectSingleRowNoCtx(&restoredHash, "SELECT sum(cityHash64(*)) FROM "+tableName))
	r.Equal(expectedCount, restoredCount)
	r.Equal(expectedHash, restoredHash)
}
