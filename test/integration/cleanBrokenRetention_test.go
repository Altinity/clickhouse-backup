//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

const cleanBrokenRetentionExcludeGlob = "cbr_orphan_keep_*"
const cleanBrokenRetentionIncludeGlob = "cbr_*"

// Each TestCleanBrokenRetention* function verifies that `clean_broken_retention`:
//   - lists orphans in object_disks_path (dry-run) without deleting,
//   - preserves entries under backup `path` that BackupList discovers as broken
//     (in-progress uploads with no metadata.json),
//   - by default removes orphans from `object_disks_path` only,
//   - preserves the live backup and entries matched by --exclude globs,
//   - uses --include=cbr_* to isolate the test from other tests sharing the same bucket.
//
// Each backend is its own top-level test so they can be run independently
// (e.g. `RUN_TESTS=TestCleanBrokenRetentionS3 ./test/integration/run.sh`).
// Backends that need cloud credentials skip themselves when the corresponding env
// var (GCS_TESTS, AZURE_TESTS, QA_TENCENT_SECRET_KEY/QA_TENCENT_SECRET_ID) is unset.

// cleanBrokenRetentionCase wires one remote-storage backend to the shared scenario.
type cleanBrokenRetentionCase struct {
	name           string
	configFile     string
	pathRoot       string
	objRoot        string
	skip           func() bool
	skipReason     string
	setup          func(env *TestEnvironment, r *require.Assertions)
	plant          orphanAction
	assertExists   orphanAction
	assertGone     orphanAction
	finalEmptyType string
}

type orphanAction func(env *TestEnvironment, r *require.Assertions, root, name string)

func TestCleanBrokenRetentionS3(t *testing.T) {
	runCleanBrokenRetentionCase(t, s3CleanBrokenRetentionCase())
}
func TestCleanBrokenRetentionSFTP(t *testing.T) {
	runCleanBrokenRetentionCase(t, sftpCleanBrokenRetentionCase())
}
func TestCleanBrokenRetentionFTP(t *testing.T) {
	runCleanBrokenRetentionCase(t, ftpCleanBrokenRetentionCase())
}
func TestCleanBrokenRetentionGCSEmulator(t *testing.T) {
	runCleanBrokenRetentionCase(t, gcsEmulatorCleanBrokenRetentionCase())
}
func TestCleanBrokenRetentionAZBLOB(t *testing.T) {
	runCleanBrokenRetentionCase(t, azblobCleanBrokenRetentionCase())
}
func TestCleanBrokenRetentionGCS(t *testing.T) {
	runCleanBrokenRetentionCase(t, gcsRealCleanBrokenRetentionCase())
}
func TestCleanBrokenRetentionCOS(t *testing.T) {
	runCleanBrokenRetentionCase(t, cosCleanBrokenRetentionCase())
}

func runCleanBrokenRetentionCase(t *testing.T, tc cleanBrokenRetentionCase) {
	if tc.skip != nil && tc.skip() {
		t.Skip(tc.skipReason)
		return
	}
	runCleanBrokenRetentionScenario(t, tc)
}

func runCleanBrokenRetentionScenario(t *testing.T, tc cleanBrokenRetentionCase) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	r.NoError(env.DockerCP("configs/"+tc.configFile, "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	if tc.setup != nil {
		tc.setup(env, r)
	}

	tableName := fmt.Sprintf("default.clean_broken_retention_%s", strings.ToLower(tc.name))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id UInt64) ENGINE=MergeTree() ORDER BY id", tableName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO %s SELECT number FROM numbers(50)", tableName))

	suffix := time.Now().UnixNano()
	keepBackup := fmt.Sprintf("cbr_keep_%d", suffix)
	brokenPath := fmt.Sprintf("cbr_broken_%d", suffix)
	orphanObj := fmt.Sprintf("cbr_orphan_obj_%d", suffix)
	orphanKept := fmt.Sprintf("cbr_orphan_keep_%d", suffix)

	log.Debug().Str("backend", tc.name).Msg("Create a live backup that must survive the cleanup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create_remote", "--tables", tableName, keepBackup)

	log.Debug().Str("backend", tc.name).Msg("Plant broken entry under backup path and orphans under object_disks_path")
	// brokenPath: no metadata.json → BackupList treats it as a broken (in-progress) backup → must be KEPT.
	tc.plant(env, r, tc.pathRoot, brokenPath)
	tc.assertExists(env, r, tc.pathRoot, brokenPath)
	// Genuine orphans live only under object_disks_path, which BackupList does not scan.
	for _, name := range []string{orphanObj, orphanKept} {
		tc.plant(env, r, tc.objRoot, name)
		tc.assertExists(env, r, tc.objRoot, name)
	}

	log.Debug().Str("backend", tc.name).Msg("Dry-run lists object disk orphans but preserves broken backup path entries")
	dryRunOut, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "clean_broken_retention", "--include="+cleanBrokenRetentionIncludeGlob)
	r.NoError(err, "dry-run failed: %s", dryRunOut)
	r.NotContains(dryRunOut, fmt.Sprintf("orphan=%s", brokenPath), "broken backup path entry must not appear as orphan")
	r.Contains(dryRunOut, orphanObj, "dry-run must mention object disk orphan")
	r.Contains(dryRunOut, "would delete", "dry-run must announce planned deletions")
	r.NotContains(dryRunOut, "clean_broken_retention: deleting", "dry-run must not delete")
	r.NotContains(dryRunOut, fmt.Sprintf("\"orphan\":\"%s\"", keepBackup), "live backup must not appear as orphan")
	tc.assertExists(env, r, tc.pathRoot, brokenPath)
	tc.assertExists(env, r, tc.objRoot, orphanObj)

	log.Debug().Str("backend", tc.name).Msg("--exclude glob preserves matched object disk orphans")
	commitOut, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "clean_broken_retention", "--commit", "--include="+cleanBrokenRetentionIncludeGlob, "--exclude="+cleanBrokenRetentionExcludeGlob)
	r.NoError(err, "commit failed: %s", commitOut)
	r.Contains(commitOut, "clean_broken_retention: deleting")
	tc.assertExists(env, r, tc.pathRoot, brokenPath)
	tc.assertGone(env, r, tc.objRoot, orphanObj)
	tc.assertExists(env, r, tc.objRoot, orphanKept)

	log.Debug().Str("backend", tc.name).Msg("Second run without --exclude clears the remaining object disk orphan")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean_broken_retention", "--commit", "--include="+cleanBrokenRetentionIncludeGlob)
	tc.assertExists(env, r, tc.pathRoot, brokenPath)
	tc.assertGone(env, r, tc.objRoot, orphanKept)

	log.Debug().Str("backend", tc.name).Msg("Cleanup live backup, broken entry, and table")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", keepBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", keepBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean_remote_broken")
	dropQuery := "DROP TABLE IF EXISTS " + tableName
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
		dropQuery += " NO DELAY"
	}
	env.queryWithNoError(r, dropQuery)
	if tc.finalEmptyType != "" {
		env.checkObjectStorageIsEmpty(t, r, tc.finalEmptyType)
	}
}

// containerFSCase builds a case for a backend that maps its remote storage to a
// path on the given docker container's filesystem.
func containerFSCase(name, configFile, container, pathRoot, objRoot, finalEmptyType string) cleanBrokenRetentionCase {
	plant := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		env.DockerExecNoError(r, container, "sh", "-c", fmt.Sprintf(
			"mkdir -p %s/%s/sub && echo garbage > %s/%s/data.bin && echo garbage > %s/%s/sub/nested.bin && chmod -R 777 %s/%s",
			root, name, root, name, root, name, root, name))
	}
	exists := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		out, err := env.DockerExecOut(container, "ls", root+"/"+name)
		r.NoError(err, "expected %s/%s to exist on %s, output: %s", root, name, container, out)
	}
	gone := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		out, _ := env.DockerExecOut(container, "sh", "-c", fmt.Sprintf("ls %s/%s 2>/dev/null || true", root, name))
		r.Empty(strings.TrimSpace(out), "expected %s/%s on %s to be removed, ls returned: %s", root, name, container, out)
	}
	return cleanBrokenRetentionCase{
		name:           name,
		configFile:     configFile,
		pathRoot:       pathRoot,
		objRoot:        objRoot,
		plant:          plant,
		assertExists:   exists,
		assertGone:     gone,
		finalEmptyType: finalEmptyType,
	}
}

func s3CleanBrokenRetentionCase() cleanBrokenRetentionCase {
	// Plant via `mc cp` instead of direct FS writes — MinIO ignores raw files on disk and
	// only sees objects that went through its S3 API.
	const mcAliasCmd = "mc alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null 2>&1"
	const bucketPath = "local/clickhouse/backup/cluster/0"
	const objBucketPath = "local/clickhouse/object_disk/cluster/0"
	plant := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		env.DockerExecNoError(r, "minio", "bash", "-c", fmt.Sprintf(
			"%s && echo garbage > /tmp/data.bin && mc cp /tmp/data.bin %s/%s/data.bin >/dev/null && mc cp /tmp/data.bin %s/%s/sub/nested.bin >/dev/null",
			mcAliasCmd, root, name, root, name,
		))
	}
	exists := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		out, err := env.DockerExecOut("minio", "bash", "-c", fmt.Sprintf("%s && mc ls %s/%s/", mcAliasCmd, root, name))
		r.NoError(err, "mc ls failed: %s", out)
		r.Contains(out, "data.bin", "expected data.bin under %s/%s, got: %s", root, name, out)
	}
	gone := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		out, _ := env.DockerExecOut("minio", "bash", "-c", fmt.Sprintf("%s && mc ls -r %s/%s/ 2>&1 || true", mcAliasCmd, root, name))
		r.NotContains(out, "data.bin", "expected no objects under %s/%s, got: %s", root, name, out)
		r.NotContains(out, "nested.bin", "expected no objects under %s/%s, got: %s", root, name, out)
	}
	return cleanBrokenRetentionCase{
		name:           "S3",
		configFile:     "config-s3.yml",
		pathRoot:       bucketPath,
		objRoot:        objBucketPath,
		plant:          plant,
		assertExists:   exists,
		assertGone:     gone,
		finalEmptyType: "S3",
	}
}

func sftpCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	tc := containerFSCase("SFTP", "config-sftp-auth-key.yaml", "sshd", "/root", "/object_disk", "")
	tc.setup = func(env *TestEnvironment, r *require.Assertions) {
		env.uploadSSHKeys(r, "clickhouse-backup")
		env.DockerExecNoError(r, "sshd", "mkdir", "-p", "/object_disk")
	}
	return tc
}

func ftpCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	home := "/home/test_backup"
	if isAdvancedMode() {
		home = "/home/ftpusers/test_backup"
	}
	tc := containerFSCase("FTP", "config-ftp.yaml", "ftp", home+"/backup", home+"/object_disk", "")
	tc.skip = func() bool { return compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") <= 0 }
	tc.skipReason = "FTP scenario only validated on ClickHouse > 21.8"
	tc.setup = func(env *TestEnvironment, r *require.Assertions) {
		// proftpd/vsftpd containers don't create `test_backup` as a system user; uid 1000 owns the home dir.
		env.DockerExecNoError(r, "ftp", "sh", "-c", fmt.Sprintf("mkdir -p %s/backup %s/object_disk && chown -R 1000:1000 %s && chmod -R 0777 %s", home, home, home, home))
	}
	return tc
}

func gcsEmulatorCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	const bucket = "altinity-qa-test"
	const baseURL = "http://localhost:8080"
	setup := func(env *TestEnvironment, r *require.Assertions) {
		env.DockerExecNoError(r, "gcs", "apk", "add", "-q", "curl")
	}
	plant := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		obj := root + "/" + name
		env.DockerExecNoError(r, "gcs", "sh", "-c", fmt.Sprintf(
			`echo garbage > /tmp/data.bin && `+
				`curl -s -o /dev/null -X POST "%s/upload/storage/v1/b/%s/o?name=%s/data.bin&uploadType=media" -H "Content-Type: application/octet-stream" --data-binary @/tmp/data.bin && `+
				`curl -s -o /dev/null -X POST "%s/upload/storage/v1/b/%s/o?name=%s/sub/nested.bin&uploadType=media" -H "Content-Type: application/octet-stream" --data-binary @/tmp/data.bin`,
			baseURL, bucket, obj, baseURL, bucket, obj))
	}
	assertExists := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		out, err := env.DockerExecOut("gcs", "sh", "-c", fmt.Sprintf(
			`curl -s "%s/storage/v1/b/%s/o?prefix=%s/"`, baseURL, bucket, root+"/"+name))
		r.NoError(err, "assertExists list failed: %s", out)
		r.Contains(out, "data.bin", "expected data.bin under %s/%s", root, name)
	}
	assertGone := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		out, _ := env.DockerExecOut("gcs", "sh", "-c", fmt.Sprintf(
			`curl -s "%s/storage/v1/b/%s/o?prefix=%s/"`, baseURL, bucket, root+"/"+name))
		r.NotContains(out, "data.bin", "expected no blobs under %s/%s", root, name)
	}
	return cleanBrokenRetentionCase{
		name:           "GCS_EMULATOR",
		configFile:     "config-gcs-custom-endpoint.yml",
		pathRoot:       "backup/cluster/0",
		objRoot:        "object_disks/cluster/0",
		setup:          setup,
		plant:          plant,
		assertExists:   assertExists,
		assertGone:     assertGone,
		finalEmptyType: "GCS_EMULATOR",
	}
}

func gcsRealCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	const bucket = "altinity-qa-test"
	const image = "google/cloud-sdk:slim"
	// All gsutil invocations need the service account activated first.
	const authPrefix = "gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS >/dev/null 2>&1 && "
	gsutil := func(env *TestEnvironment, r *require.Assertions, sh string) string {
		// --volumes-from gives us /etc/clickhouse-backup/credentials.json from the backup container.
		args := []string{
			"run", "--rm", "--network", env.tc.networkName,
			"--volumes-from", env.tc.GetContainerID("clickhouse-backup"),
			"-e", "GOOGLE_APPLICATION_CREDENTIALS=/etc/clickhouse-backup/credentials.json",
			image, "bash", "-c", authPrefix + sh,
		}
		out, err := utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker", args...)
		r.NoError(err, "gsutil failed: %s", out)
		return out
	}
	return cleanBrokenRetentionCase{
		name:       "GCS",
		configFile: "config-gcs.yml",
		pathRoot:   "backup/cluster/0",
		objRoot:    "object_disks/cluster/0",
		skip:       func() bool { return isTestShouldSkip("GCS_TESTS") },
		skipReason: "Skipping GCS integration tests (GCS_TESTS not set)",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.tc.pullImageIfNeeded(context.Background(), image)
		},
		plant: func(env *TestEnvironment, r *require.Assertions, root, name string) {
			obj := root + "/" + name
			gsutil(env, r, fmt.Sprintf(
				"echo garbage > /tmp/data.bin && gsutil -q cp /tmp/data.bin gs://%s/%s/data.bin && gsutil -q cp /tmp/data.bin gs://%s/%s/sub/nested.bin",
				bucket, obj, bucket, obj))
		},
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, name string) {
			obj := root + "/" + name
			out := gsutil(env, r, fmt.Sprintf("gsutil ls gs://%s/%s/", bucket, obj))
			r.Contains(out, "gs://"+bucket+"/"+obj+"/", "expected listing to contain %s", obj)
		},
		assertGone: func(env *TestEnvironment, r *require.Assertions, root, name string) {
			obj := root + "/" + name
			out := gsutil(env, r, fmt.Sprintf("gsutil ls gs://%s/%s/** 2>&1 || true", bucket, obj))
			r.NotContains(out, "gs://"+bucket+"/"+obj, "expected no blobs under gs://%s/%s, got: %s", bucket, obj, out)
		},
	}
}

func cosCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	// COS exposes an S3-compatible API on its regional endpoint.
	const bucket = "clickhouse-backup-1336113806"
	const endpoint = "https://cos.na-ashburn.myqcloud.com"
	const image = "amazon/aws-cli:latest"
	// Tencent COS rejects path-style addressing (PathStyleDomainForbidden); force virtual-hosted style.
	const awsPrefix = "aws configure set default.s3.addressing_style virtual >/dev/null && "
	awsRun := func(env *TestEnvironment, r *require.Assertions, sh string) string {
		// --entrypoint sh overrides aws-cli's default `aws` entrypoint.
		out, err := utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker",
			"run", "--rm", "--network", env.tc.networkName,
			"-e", "AWS_ACCESS_KEY_ID="+os.Getenv("QA_TENCENT_SECRET_ID"),
			"-e", "AWS_SECRET_ACCESS_KEY="+os.Getenv("QA_TENCENT_SECRET_KEY"),
			"-e", "AWS_DEFAULT_REGION=na-ashburn",
			"--entrypoint", "sh", image, "-c", awsPrefix+sh)
		r.NoError(err, "aws-cli failed: %s", out)
		return out
	}
	return cleanBrokenRetentionCase{
		name:       "COS",
		configFile: "config-cos.yml",
		pathRoot:   "backup/cluster/0",
		objRoot:    "object_disk/cluster/0",
		skip: func() bool {
			return os.Getenv("QA_TENCENT_SECRET_KEY") == "" || os.Getenv("QA_TENCENT_SECRET_ID") == ""
		},
		skipReason: "Skipping COS integration tests (QA_TENCENT_SECRET_ID / QA_TENCENT_SECRET_KEY not set)",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.tc.pullImageIfNeeded(context.Background(), image)
			env.InstallDebIfNotExists(r, "clickhouse-backup", "gettext-base")
			// config.yml was copied raw and still has ${QA_TENCENT_SECRET_*} placeholders.
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec",
				"envsubst < /etc/clickhouse-backup/config.yml > /tmp/c.yml && mv /tmp/c.yml /etc/clickhouse-backup/config.yml")
		},
		plant: func(env *TestEnvironment, r *require.Assertions, root, name string) {
			obj := root + "/" + name
			awsRun(env, r, fmt.Sprintf(
				"echo garbage > /tmp/data.bin && aws --endpoint-url=%s s3 cp /tmp/data.bin s3://%s/%s/data.bin >/dev/null && aws --endpoint-url=%s s3 cp /tmp/data.bin s3://%s/%s/sub/nested.bin >/dev/null",
				endpoint, bucket, obj, endpoint, bucket, obj))
		},
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, name string) {
			obj := root + "/" + name
			out := awsRun(env, r, fmt.Sprintf("aws --endpoint-url=%s s3 ls s3://%s/%s/", endpoint, bucket, obj))
			r.Contains(out, "data.bin", "expected data.bin under %s, got: %s", obj, out)
		},
		assertGone: func(env *TestEnvironment, r *require.Assertions, root, name string) {
			obj := root + "/" + name
			out := awsRun(env, r, fmt.Sprintf("aws --endpoint-url=%s s3 ls s3://%s/%s/ 2>&1 || true", endpoint, bucket, obj))
			r.NotContains(out, "data.bin", "expected no blobs under s3://%s/%s, got: %s", bucket, obj, out)
			r.NotContains(out, "nested.bin", "expected no blobs under s3://%s/%s, got: %s", bucket, obj, out)
		},
	}
}

func azblobCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	const container = "container1"
	const accountName = "devstoreaccount1"
	const accountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	const azureCliImage = "mcr.microsoft.com/azure-cli:latest"
	blobPath := func(root, name string) string { return strings.TrimPrefix(root+"/"+name, "/") }
	uploadTimeout := 2 * time.Minute

	azConnString := fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://azure:10000/%s;",
		accountName, accountKey, accountName,
	)

	azRun := func(env *TestEnvironment, args ...string) (string, error) {
		dockerArgs := append([]string{
			"run", "--rm", "--network", env.tc.networkName,
			"-e", "AZURE_STORAGE_CONNECTION_STRING=" + azConnString,
			azureCliImage, "az",
		}, args...)
		return utils.ExecCmdOut(context.Background(), uploadTimeout, "docker", dockerArgs...)
	}

	setup := func(env *TestEnvironment, r *require.Assertions) {
		env.tc.pullImageIfNeeded(context.Background(), azureCliImage)
	}

	plant := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		p := blobPath(root, name)
		tmpDir, err := os.MkdirTemp("", "azblob-plant-*")
		r.NoError(err)
		defer func() {
			if deleteErr := os.RemoveAll(tmpDir); deleteErr != nil {
				log.Error().Err(deleteErr).Stack().Msgf("can't remove tmpDir=%s", tmpDir)
			}
		}()

		r.NoError(os.WriteFile(tmpDir+"/data.bin", []byte("garbage"), 0644))
		out, err := utils.ExecCmdOut(context.Background(), uploadTimeout, "docker",
			"run", "--rm", "--network", env.tc.networkName,
			"-e", "AZURE_STORAGE_CONNECTION_STRING="+azConnString,
			"-v", tmpDir+"/data.bin:/data.bin:ro",
			azureCliImage, "az",
			"storage", "blob", "upload",
			"--container-name", container,
			"--name", p+"/data.bin",
			"--file", "/data.bin",
		)
		r.NoError(err, "azblob plant data.bin: %s", out)

		r.NoError(os.MkdirAll(tmpDir+"/sub", 0755))
		r.NoError(os.WriteFile(tmpDir+"/sub/nested.bin", []byte("garbage"), 0644))
		out, err = utils.ExecCmdOut(context.Background(), uploadTimeout, "docker",
			"run", "--rm", "--network", env.tc.networkName,
			"-e", "AZURE_STORAGE_CONNECTION_STRING="+azConnString,
			"-v", tmpDir+"/sub/nested.bin:/nested.bin:ro",
			azureCliImage, "az",
			"storage", "blob", "upload",
			"--container-name", container,
			"--name", p+"/sub/nested.bin",
			"--file", "/nested.bin",
		)
		r.NoError(err, "azblob plant nested.bin: %s", out)
	}

	blobShow := func(env *TestEnvironment, r *require.Assertions, root, name string) (string, error) {
		return azRun(env,
			"storage", "blob", "show",
			"--container-name", container,
			"--name", blobPath(root, name)+"/data.bin",
		)
	}

	return cleanBrokenRetentionCase{
		name:       "AZBLOB",
		configFile: "config-azblob.yml",
		pathRoot:   "backup",
		objRoot:    "object_disks",
		skip:       func() bool { return isTestShouldSkip("AZURE_TESTS") },
		skipReason: "Skipping AZBLOB integration tests (AZURE_TESTS not set)",
		setup:      setup,
		plant:      plant,
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, name string) {
			out, err := blobShow(env, r, root, name)
			r.NoError(err, "azblob assertExists failed: %s", out)
		},
		assertGone: func(env *TestEnvironment, r *require.Assertions, root, name string) {
			out, err := blobShow(env, r, root, name)
			r.Error(err, "expected %s to be gone, got: %s", blobPath(root, name), out)
		},
	}
}
