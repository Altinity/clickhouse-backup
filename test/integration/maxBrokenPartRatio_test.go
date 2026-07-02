//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// Each TestMaxBrokenPartRatio* function covers general.max_broken_part_ratio
// (https://github.com/Altinity/clickhouse-backup/issues/1418) against a real object disk:
//   - create a table with an object-disk storage policy and 4 data parts,
//   - delete the object storage blobs of exactly one part directly in the backend,
//     so `create` hits a broken part during uploadObjectDiskParts,
//   - MAX_BROKEN_PART_RATIO=0 (legacy default) must abort on the broken part,
//   - MAX_BROKEN_PART_RATIO=0.1 < observed ratio 1/4 must abort with the explicit
//     "backup aborted ... exceeds max_broken_part_ratio" error,
//   - MAX_BROKEN_PART_RATIO=0.5 >= 1/4 must produce a successful partial backup that
//     drops ONLY the broken part; restore must bring back the 3 healthy parts.
//
// Each backend is its own top-level test so they can be run independently
// (e.g. `RUN_TESTS=TestMaxBrokenPartRatioS3 ./test/integration/run.sh`).

// maxBrokenPartRatioCase wires one object-disk backend to the shared scenario.
type maxBrokenPartRatioCase struct {
	name         string
	configFile   string // clickhouse-backup config under /etc/clickhouse-backup/
	policy       string // storage_policy for the test table
	minCHVersion string // minimal CLICKHOUSE_VERSION with this policy in dynamic_settings.sh
	skip         func() bool
	skipReason   string
	setup        func(env *TestEnvironment, r *require.Assertions)
	// deleteObjects removes the given object keys from the disk's object storage backend.
	// Keys are StorageObject.ObjectPath values (relative to the disk endpoint) parsed from
	// the part's metadata stub files, exactly what uploadObjectDiskParts will try to copy.
	deleteObjects func(env *TestEnvironment, r *require.Assertions, objectPaths []string)
}

func TestMaxBrokenPartRatioS3(t *testing.T) {
	runMaxBrokenPartRatioCase(t, maxBrokenPartRatioS3Case())
}

func TestMaxBrokenPartRatioGCS(t *testing.T) {
	runMaxBrokenPartRatioCase(t, maxBrokenPartRatioGCSCase())
}

func TestMaxBrokenPartRatioAzure(t *testing.T) {
	runMaxBrokenPartRatioCase(t, maxBrokenPartRatioAzureCase())
}

func runMaxBrokenPartRatioCase(t *testing.T, tc maxBrokenPartRatioCase) {
	if !isAdvancedMode() {
		t.Skipf("object disks require advanced mode, CLICKHOUSE_VERSION=%s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), tc.minCHVersion) < 0 {
		t.Skipf("storage_policy=%s requires ClickHouse >= %s", tc.policy, tc.minCHVersion)
	}
	if tc.skip != nil && tc.skip() {
		t.Skip(tc.skipReason)
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	if tc.setup != nil {
		tc.setup(env, r)
	}

	chVer := strings.ReplaceAll(os.Getenv("CLICKHOUSE_VERSION"), ".", "_")
	dbName := "test_max_broken_ratio_" + strings.ToLower(tc.name)
	tableName := dbName + ".broken_parts"
	backupPrefix := fmt.Sprintf("max_broken_ratio_%s_%s_%d", strings.ToLower(tc.name), chVer, time.Now().UnixNano())
	config := "/etc/clickhouse-backup/" + tc.configFile

	env.queryWithNoError(t, r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	// t.Cleanup runs after the deferred env.Cleanup returned the pooled env (its
	// clickhouse connection is already closed), so drop via docker exec instead
	t.Cleanup(func() {
		if out, err := env.DockerExecOut("clickhouse", "clickhouse", "client", "-q", "DROP DATABASE IF EXISTS "+dbName+" SYNC"); err != nil {
			log.Warn().Err(err).Str("database", dbName).Msgf("t.Cleanup: failed to drop database: %s", out)
		}
	})
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s (id UInt64) ENGINE=MergeTree() PARTITION BY id %% 4 ORDER BY id SETTINGS storage_policy='%s'",
		tableName, tc.policy,
	))
	// single INSERT over 4 partitions -> exactly 4 data parts, 100 rows each
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s SELECT number FROM numbers(400)", tableName))

	var totalParts uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&totalParts, fmt.Sprintf(
		"SELECT count() FROM system.parts WHERE database='%s' AND table='broken_parts' AND active", dbName,
	)))
	r.Equal(uint64(4), totalParts, "expected exactly 4 active parts")

	// break exactly one part: delete every object storage blob its metadata stubs reference
	var brokenPartPath, brokenPartitionID, brokenPartName string
	partCondition := fmt.Sprintf("database='%s' AND table='broken_parts' AND active", dbName)
	r.NoError(env.ch.SelectSingleRowNoCtx(&brokenPartPath, fmt.Sprintf(
		"SELECT path FROM system.parts WHERE %s ORDER BY name LIMIT 1", partCondition,
	)))
	r.NoError(env.ch.SelectSingleRowNoCtx(&brokenPartitionID, fmt.Sprintf(
		"SELECT partition_id FROM system.parts WHERE %s ORDER BY name LIMIT 1", partCondition,
	)))
	r.NoError(env.ch.SelectSingleRowNoCtx(&brokenPartName, fmt.Sprintf(
		"SELECT name FROM system.parts WHERE %s ORDER BY name LIMIT 1", partCondition,
	)))
	objectPaths := collectPartObjectPaths(t, env, r, brokenPartPath)
	r.NotEmpty(objectPaths, "expected object storage keys in part metadata under %s", brokenPartPath)
	log.Debug().Str("part", brokenPartPath).Strs("objects", objectPaths).Msg("deleting object disk blobs to break the part")
	tc.deleteObjects(env, r, objectPaths)

	// best-effort teardown: aborted creates auto-remove their local backup, but don't
	// leak the partial one if an assertion fails mid-test
	partialBackup := backupPrefix + "_partial"
	defer func() {
		for _, name := range []string{backupPrefix + "_abort_default", backupPrefix + "_abort_low", partialBackup} {
			if out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", config, "delete", "local", name); err != nil {
				log.Debug().Err(err).Msgf("maxBrokenPartRatio teardown: %s", out)
			}
		}
	}()

	createCmd := func(ratio, backupName string) (string, error) {
		return env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf(
			"MAX_BROKEN_PART_RATIO=%s RETRIES_ON_FAILURE=1 clickhouse-backup -c %s create --tables='%s.*' %s",
			ratio, config, dbName, backupName,
		))
	}

	log.Debug().Str("backend", tc.name).Msg("MAX_BROKEN_PART_RATIO=0 (legacy default) must abort on the broken part")
	out, err := createCmd("0", backupPrefix+"_abort_default")
	r.Error(err, "create must fail with default max_broken_part_ratio=0, output: %s", out)
	r.Contains(out, "uploadObjectDiskParts", "create must fail inside object disk upload, output: %s", out)

	log.Debug().Str("backend", tc.name).Msg("MAX_BROKEN_PART_RATIO=0.1 < 1/4 must abort with the ratio error")
	out, err = createCmd("0.1", backupPrefix+"_abort_low")
	r.Error(err, "create must fail with max_broken_part_ratio=0.1, output: %s", out)
	r.Contains(out, "backup aborted", "create must report the ratio abort, output: %s", out)
	// per-part granularity: exactly the one broken part is counted, not the whole disk/table
	r.Contains(out, "1 of 4 data parts are broken", "only the broken part must be counted, output: %s", out)
	r.Contains(out, "exceeds max_broken_part_ratio", "create must name the configured limit, output: %s", out)

	log.Debug().Str("backend", tc.name).Msg("MAX_BROKEN_PART_RATIO=0.5 >= 1/4 must produce a successful partial backup")
	out, err = createCmd("0.5", partialBackup)
	r.NoError(err, "create must succeed with max_broken_part_ratio=0.5, output: %s", out)
	r.Contains(out, "partial backup", "create must warn about the partial backup, output: %s", out)

	log.Debug().Str("backend", tc.name).Msg("table metadata must list the broken part under broken_parts, not parts")
	out, err = env.DockerExecOut("clickhouse", "cat",
		fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata/%s/broken_parts.json", partialBackup, dbName))
	r.NoError(err, "can't read table metadata json: %s", out)
	var tm metadata.TableMetadata
	r.NoError(json.Unmarshal([]byte(out), &tm), "can't parse table metadata json: %s", out)
	var metaParts, metaBrokenParts []string
	for _, diskParts := range tm.Parts {
		for _, p := range diskParts {
			metaParts = append(metaParts, p.Name)
		}
	}
	for _, diskParts := range tm.BrokenParts {
		for _, p := range diskParts {
			metaBrokenParts = append(metaBrokenParts, p.Name)
		}
	}
	r.Equal([]string{brokenPartName}, metaBrokenParts, "broken_parts must contain exactly the broken part")
	r.Len(metaParts, 3, "parts must contain only the 3 healthy parts")
	r.NotContains(metaParts, brokenPartName, "the broken part must not be listed under parts")

	log.Debug().Str("backend", tc.name).Msg("restore the partial backup and verify only the broken part is lost")
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", config, "restore", "--rm", "--tables="+dbName+".*", partialBackup)
	r.NoError(err, "restore must succeed, output: %s", out)

	var restoredRows, restoredParts, brokenPartitionRows uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&restoredRows, "SELECT count() FROM "+tableName))
	r.Equal(uint64(300), restoredRows, "restore must bring back the 3 healthy parts (100 rows each)")
	r.NoError(env.ch.SelectSingleRowNoCtx(&restoredParts, fmt.Sprintf(
		"SELECT count() FROM system.parts WHERE %s", partCondition,
	)))
	r.Equal(uint64(3), restoredParts, "the broken part must be absent from the restored table")
	r.NoError(env.ch.SelectSingleRowNoCtx(&brokenPartitionRows, fmt.Sprintf(
		"SELECT count() FROM %s WHERE _partition_id='%s'", tableName, brokenPartitionID,
	)))
	r.Equal(uint64(0), brokenPartitionRows, "the broken partition must contain no rows after restore")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "delete", "local", partialBackup)
}

// collectPartObjectPaths cats every metadata stub file of the given data part inside the
// clickhouse container and parses the referenced object storage keys with the production
// object_disk metadata parser (handles all metadata format versions).
func collectPartObjectPaths(t *testing.T, env *TestEnvironment, r *require.Assertions, partPath string) []string {
	t.Helper()
	out, err := env.DockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf(
		`find '%s' -type f | while read -r f; do echo "@@@ $f"; cat "$f"; echo; done`, partPath,
	))
	r.NoError(err, "can't read part metadata stubs under %s: %s", partPath, out)
	var objectPaths []string
	for _, chunk := range strings.Split(out, "@@@ ")[1:] {
		newLine := strings.Index(chunk, "\n")
		r.Greater(newLine, 0, "unexpected stub chunk: %q", chunk)
		fName := strings.TrimSpace(chunk[:newLine])
		meta, parseErr := object_disk.ReadMetadataFromReader(io.NopCloser(strings.NewReader(chunk[newLine+1:])), fName)
		r.NoError(parseErr, "can't parse object disk metadata %s", fName)
		for _, storageObject := range meta.StorageObjects {
			if storageObject.ObjectSize > 0 {
				objectPaths = append(objectPaths, storageObject.ObjectPath)
			}
		}
	}
	return objectPaths
}

func maxBrokenPartRatioS3Case() maxBrokenPartRatioCase {
	// MinIO only sees objects that went through its S3 API, so use `mc`.
	const mcAliasCmd = "mc alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null 2>&1"
	return maxBrokenPartRatioCase{
		name:         "S3",
		configFile:   "config-s3.yml",
		policy:       "s3_only",
		minCHVersion: "21.8",
		deleteObjects: func(env *TestEnvironment, r *require.Assertions, objectPaths []string) {
			// disk_s3 endpoint is https://minio:9000/clickhouse/disk_s3/{cluster}/{shard}/
			prefix, err := env.ch.ApplyMacros(context.Background(), "disk_s3/{cluster}/{shard}")
			r.NoError(err, "ApplyMacros(disk_s3 prefix)")
			cmds := []string{mcAliasCmd}
			for _, objectPath := range objectPaths {
				cmds = append(cmds, fmt.Sprintf("mc rm --force 'local/clickhouse/%s/%s'", prefix, objectPath))
			}
			env.DockerExecNoError(r, "minio", "bash", "-ce", strings.Join(cmds, " && "))
		},
	}
}

func maxBrokenPartRatioGCSCase() maxBrokenPartRatioCase {
	const image = "gcr.io/google.com/cloudsdktool/google-cloud-cli:slim"
	const authPrefix = "gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS >/dev/null 2>&1 && "
	return maxBrokenPartRatioCase{
		name:         "GCS",
		configFile:   "config-gcs.yml",
		policy:       "gcs_only",
		minCHVersion: "22.6",
		skip: func() bool {
			return isTestShouldSkip("GCS_TESTS") || os.Getenv("QA_GCS_OVER_S3_BUCKET") == ""
		},
		skipReason: "Skipping GCS integration tests (GCS_TESTS / QA_GCS_OVER_S3_BUCKET not set)",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.tc.pullImageIfNeeded(context.Background(), image)
		},
		deleteObjects: func(env *TestEnvironment, r *require.Assertions, objectPaths []string) {
			// disk_gcs endpoint is
			// https://storage.googleapis.com/${QA_GCS_OVER_S3_BUCKET}/clickhouse_backup_disk_gcs_over_s3/${HOSTNAME}/{cluster}/{shard}/
			// where HOSTNAME is the clickhouse container hostname at dynamic_settings.sh time.
			bucket := os.Getenv("QA_GCS_OVER_S3_BUCKET")
			hostname, err := env.DockerExecOut("clickhouse", "hostname")
			r.NoError(err, "can't resolve clickhouse container hostname")
			prefix, err := env.ch.ApplyMacros(context.Background(),
				fmt.Sprintf("clickhouse_backup_disk_gcs_over_s3/%s/{cluster}/{shard}", strings.TrimSpace(hostname)))
			r.NoError(err, "ApplyMacros(disk_gcs prefix)")
			gsURLs := make([]string, 0, len(objectPaths))
			for _, objectPath := range objectPaths {
				gsURLs = append(gsURLs, fmt.Sprintf("'gs://%s/%s/%s'", bucket, prefix, objectPath))
			}
			out, err := utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker",
				"run", "--rm", "--network", env.tc.networkName,
				"--volumes-from", env.tc.GetContainerID("clickhouse-backup"),
				"-e", "GOOGLE_APPLICATION_CREDENTIALS=/etc/clickhouse-backup/credentials.json",
				image, "bash", "-c", authPrefix+"gsutil -q rm "+strings.Join(gsURLs, " "),
			)
			r.NoError(err, "gsutil rm failed: %s", out)
		},
	}
}

func maxBrokenPartRatioAzureCase() maxBrokenPartRatioCase {
	// disk_azblob keeps its blobs in the azurite `azure-disk` container (see dynamic_settings.sh),
	// blob names are the ObjectPath values because storage_account_url has no path prefix.
	const diskContainer = "azure-disk"
	const accountName = "devstoreaccount1"
	const accountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	const azureCliImage = "mcr.microsoft.com/azure-cli:latest"
	azConnString := fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://azure:10000/%s;",
		accountName, accountKey, accountName,
	)
	return maxBrokenPartRatioCase{
		name:         "AZBLOB",
		configFile:   "config-azblob.yml",
		policy:       "azure_only",
		minCHVersion: "23.3",
		skip:         func() bool { return isTestShouldSkip("AZURE_TESTS") },
		skipReason:   "Skipping AZBLOB integration tests (AZURE_TESTS not set)",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.tc.pullImageIfNeeded(context.Background(), azureCliImage)
		},
		deleteObjects: func(env *TestEnvironment, r *require.Assertions, objectPaths []string) {
			cmds := make([]string, 0, len(objectPaths))
			for _, objectPath := range objectPaths {
				cmds = append(cmds, fmt.Sprintf(
					"az storage blob delete --only-show-errors --container-name %s --name '%s'", diskContainer, objectPath,
				))
			}
			out, err := utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker",
				"run", "--rm", "--network", env.tc.networkName,
				"-e", "AZURE_STORAGE_CONNECTION_STRING="+azConnString,
				azureCliImage, "bash", "-c", strings.Join(cmds, " && "),
			)
			r.NoError(err, "az storage blob delete failed: %s", out)
		},
	}
}
