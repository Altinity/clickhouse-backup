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

// TestAllowMissingFilesOnDownload reproduces https://github.com/Altinity/clickhouse-backup/issues/1456:
//   - upload a backup with two parts (upload_by_part=true, one archive per part),
//   - delete one part archive on remote storage (MinIO via `mc`),
//   - `download` without the flag must fail fast with a not-found error instead of burning the retry backoff,
//   - `download --allow-missing-files` must succeed, skip the missing part with an error log,
//     drop it from the local table metadata, and `restore` must bring back the surviving part only.
func TestAllowMissingFilesOnDownload(t *testing.T) {
	chVer := strings.ReplaceAll(os.Getenv("CLICKHOUSE_VERSION"), ".", "_")
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	r.NoError(env.DockerCP("configs/config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	cfgPath, _ := env.resolveConfigPaths(r, "config-s3.yml")
	// MinIO only sees objects that went through its S3 API, so use `mc`; config path is relative to bucket
	root := "local/clickhouse/" + cfgPath
	const mcAliasCmd = "mc alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null 2>&1"

	tableShort := "test_allow_missing_files"
	tableName := "default." + tableShort
	backupName := fmt.Sprintf("allow_missing_files_%s_%d", chVer, time.Now().UnixNano())
	// two partitions -> two parts -> two archives, background merges never cross partitions
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id UInt64, p UInt8) ENGINE=MergeTree() PARTITION BY p ORDER BY id", tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s SELECT number, 1 FROM numbers(100)", tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s SELECT number, 2 FROM numbers(50)", tableName))
	t.Cleanup(func() {
		dropQ := "DROP TABLE IF EXISTS " + tableName
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
			dropQ += " NO DELAY"
		}
		if _, err := env.DockerExecOut("clickhouse", "clickhouse", "client", "-q", dropQ); err != nil {
			log.Warn().Err(err).Str("table", tableName).Msg("t.Cleanup: failed to drop table")
		}
	})
	defer func() {
		for _, cmd := range [][]string{
			{"clickhouse-backup", "delete", "local", backupName},
			{"clickhouse-backup", "delete", "remote", backupName},
		} {
			if out, err := env.DockerExecOut("clickhouse-backup", cmd...); err != nil {
				log.Warn().Err(err).Str("cmd", strings.Join(cmd, " ")).Msgf("allowMissingFiles teardown: %s", out)
			}
		}
	}()

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create", "--tables", tableName, backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "upload", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", backupName)

	// partition 2 lives in part 2_*; delete exactly that archive on remote storage
	shadowDir := fmt.Sprintf("%s/%s/shadow/default/%s/", root, backupName, tableShort)
	lsOut, err := env.DockerExecOut("minio", "bash", "-c", fmt.Sprintf("%s && mc ls %s", mcAliasCmd, shadowDir))
	r.NoError(err, "mc ls %s: %s", shadowDir, lsOut)
	// `mc ls` prints "[date] [time] [size] [name]" per object, the archive name is the last field
	archives := make([]string, 0, 2)
	missingArchive := ""
	for _, line := range strings.Split(strings.TrimSpace(lsOut), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		f := fields[len(fields)-1]
		archives = append(archives, f)
		if strings.HasPrefix(f, "default_2_") {
			missingArchive = f
		}
	}
	r.NotEmpty(missingArchive, "expected an archive for partition 2 in %s, got: %s", shadowDir, lsOut)
	r.Len(archives, 2, "expected exactly two part archives, got: %s", lsOut)
	env.DockerExecNoError(r, "minio", "bash", "-c", fmt.Sprintf("%s && mc rm %s%s", mcAliasCmd, shadowDir, missingArchive))

	log.Debug().Msg("download without --allow-missing-files must fail fast (no retry backoff)")
	start := time.Now()
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "download", backupName)
	elapsed := time.Since(start)
	log.Debug().Msg(out)
	r.Error(err, "download must fail when a part archive is missing on remote storage, output: %s", out)
	r.Contains(strings.ToLower(out), "not found", "download must report the missing archive as not-found, output: %s", out)
	// "Will wait near Ns and retry" (pkg/backup/backuper.go) is logged on every retry attempt
	r.NotContains(out, "and retry", "download must not retry on a permanent 404, output: %s", out)
	r.Less(elapsed, 20*time.Second, "download must not burn the retry backoff on a permanent 404, took %s, output: %s", elapsed, out)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", backupName)

	log.Debug().Msg("download --allow-missing-files must skip the missing part and succeed")
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "download", "--allow-missing-files", backupName)
	log.Debug().Msg(out)
	r.NoError(err, "download --allow-missing-files must succeed, output: %s", out)
	r.Contains(out, "skip it because allow_missing_files_on_download=true", "missing part must be reported with an error log, output: %s", out)
	r.Contains(out, "1 data parts were missing on remote storage and skipped", "summary must count the skipped part, output: %s", out)
	r.NotContains(out, "and retry", "download must not retry on a permanent 404, output: %s", out)

	localMeta, err := env.DockerExecOut("clickhouse-backup", "cat", fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata/default/%s.json", backupName, tableShort))
	r.NoError(err, "read local table metadata: %s", localMeta)
	r.NotContains(localMeta, missingArchive, "missing archive must be dropped from table metadata files: %s", localMeta)
	r.NotContains(localMeta, `"name": "2_`, "missing part must be dropped from table metadata parts: %s", localMeta)
	r.Contains(localMeta, `"name": "1_`, "surviving part must stay in table metadata: %s", localMeta)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore", "--rm", backupName)
	env.checkCount(r, 1, 100, fmt.Sprintf("SELECT count() FROM %s SETTINGS empty_result_for_aggregation_by_empty_set=0", tableName))
}
