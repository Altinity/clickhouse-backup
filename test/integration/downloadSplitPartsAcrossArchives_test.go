//go:build integration

package main

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
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
	// before 20.3 every part is wide, in 20.3 min_bytes_for_wide_part defaults to 0
	if compareVersion(chVersion, "20.3") > 0 {
		wideSettings = " SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0"
	}
	createTable := func(name string) {
		env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s (id UInt64, s String, f Float64, d Date, a Array(UInt8), n Nullable(Int64)) ENGINE=MergeTree() ORDER BY id%s", name, wideSettings))
	}
	insert := func(name string, from, count uint64) {
		env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s SELECT number, toString(cityHash64(number)), number / 7, toDate('2026-01-01') + number %% 300, range(number %% 10), if(number %% 3 = 0, NULL, toInt64(number)) FROM numbers(%d, %d)", name, from, count))
	}
	partsBytesQuery := "SELECT %s(bytes_on_disk) FROM system.parts WHERE active %s SETTINGS empty_result_for_aggregation_by_empty_set=0"
	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)

	// upload with upload_by_part=false raises max_file_size to 1.02*max(bytes_on_disk) of all active parts on the server
	// (Backuper.CalculateMaxSize), so pick a limit above that and make every part ~70% of it: the first archive then
	// holds part 1 and the head of part 2, the second one the tail of part 2 and the head of part 3, and so on
	var serverMaxPart uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&serverMaxPart, fmt.Sprintf(partsBytesQuery, "max", "")))
	maxFileSize := 2 * serverMaxPart
	if maxFileSize < 1024*1024 {
		maxFileSize = 1024 * 1024
	}
	calibrationTable := dbName + ".calibration"
	createTable(calibrationTable)
	const calibrationRows = 20000
	insert(calibrationTable, 0, calibrationRows)
	var calibrationBytes uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&calibrationBytes, fmt.Sprintf(partsBytesQuery, "sum", fmt.Sprintf("AND database='%s' AND table='calibration'", dbName))))
	// no SYNC, old versions reject it
	env.queryWithNoError(t, r, "DROP TABLE "+calibrationTable)
	rowsPerPart := maxFileSize * 7 / 10 * calibrationRows / calibrationBytes
	uploadEnv := fmt.Sprintf("UPLOAD_BY_PART=false MAX_FILE_SIZE=%d UPLOAD_CONCURRENCY=4", maxFileSize)

	createTable(tableName)
	// keep the parts of the full backup alive, so the incremental marks them as required
	env.queryWithNoError(t, r, "SYSTEM STOP MERGES "+tableName)
	for i := uint64(0); i < 3; i++ {
		insert(tableName, i*rowsPerPart, rowsPerPart)
	}
	var fullParts, minPart, maxPart uint64
	tableFilter := fmt.Sprintf("AND database='%s' AND table='%s'", dbName, tableShort)
	r.NoError(env.ch.SelectSingleRowNoCtx(&fullParts, fmt.Sprintf(partsBytesQuery, "count", tableFilter)))
	r.NoError(env.ch.SelectSingleRowNoCtx(&minPart, fmt.Sprintf(partsBytesQuery, "min", tableFilter)))
	r.NoError(env.ch.SelectSingleRowNoCtx(&maxPart, fmt.Sprintf(partsBytesQuery, "max", tableFilter)))
	r.Equal(uint64(3), fullParts)
	r.Greater(minPart, maxFileSize/2, "part must be bigger than half of max_file_size=%d", maxFileSize)
	r.Less(maxPart*102/100, maxFileSize, "part must fit into max_file_size=%d", maxFileSize)

	backupCmd(uploadEnv, "create_remote", "--tables="+tableName, fullBackup)

	// read the archive listings back, at least one part directory must be spread over several archives
	shadowDir := fmt.Sprintf("%s/%s/shadow/%s/%s/", root, fullBackup, dbName, tableShort)
	lsOut, err := env.DockerExecOut("minio", "bash", "-c", fmt.Sprintf("%s && mc ls %s", mcAliasCmd, shadowDir))
	r.NoError(err, "mc ls %s: %s", shadowDir, lsOut)
	archivesByPart := make(map[string]map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(lsOut), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.HasPrefix(fields[len(fields)-1], "default_") {
			continue
		}
		archive := fields[len(fields)-1]
		encoded, catErr := env.DockerExecOut("minio", "bash", "-c", fmt.Sprintf("%s && mc cat %s%s | base64 -w0", mcAliasCmd, shadowDir, archive))
		r.NoError(catErr, "mc cat %s%s: %s", shadowDir, archive, encoded)
		body, decodeErr := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		r.NoError(decodeErr, "decode %s", archive)
		tr := tar.NewReader(bytes.NewReader(body))
		for {
			header, nextErr := tr.Next()
			if nextErr == io.EOF {
				break
			}
			r.NoError(nextErr, "read %s", archive)
			partName := strings.SplitN(strings.TrimPrefix(header.Name, "/"), "/", 2)[0]
			if archivesByPart[partName] == nil {
				archivesByPart[partName] = make(map[string]bool)
			}
			archivesByPart[partName][archive] = true
		}
	}
	r.Len(archivesByPart, int(fullParts), "unexpected parts in archives: %v", archivesByPart)
	splitParts := 0
	for _, archives := range archivesByPart {
		if len(archives) > 1 {
			splitParts++
		}
	}
	r.Greater(splitParts, 0, "expected parts split across archives with max_file_size=%d, got: %v", maxFileSize, archivesByPart)

	for i := uint64(3); i < 5; i++ {
		insert(tableName, i*rowsPerPart, rowsPerPart)
	}
	// --diff-from-remote requires upload_by_part=true, the full backup stays size-bundled, so the diff flow can't find
	// a per-part archive of a required part and downloads every archive of the full backup into one directory
	backupCmd("UPLOAD_BY_PART=true", "create_remote", "--tables="+tableName, "--diff-from-remote="+fullBackup, incrBackup)

	var expectedCount, expectedHash uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&expectedCount, "SELECT count() FROM "+tableName))
	r.NoError(env.ch.SelectSingleRowNoCtx(&expectedHash, "SELECT sum(cityHash64(id, s, f, d, a, n)) FROM "+tableName))
	r.Equal(5*rowsPerPart, expectedCount)

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
	r.NoError(env.ch.SelectSingleRowNoCtx(&restoredHash, "SELECT sum(cityHash64(id, s, f, d, a, n)) FROM "+tableName))
	r.Equal(expectedCount, restoredCount)
	r.Equal(expectedHash, restoredHash)
}
