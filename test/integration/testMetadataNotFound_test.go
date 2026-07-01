//go:build integration

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// Each TestMetadataNotFound* function reproduces https://github.com/Altinity/clickhouse-backup/issues/1379:
//   - create a backup with a single table and upload it,
//   - delete that table's metadata/<db>/<table>.json on remote storage,
//   - run `download` and assert it fails fast with a "not found" error.
//
// "Fast" is the key check: a 404 on the metadata file is permanent, so the
// retry loop must break out immediately instead of burning the exponential
// backoff (RetriesOnFailure=3 at 5s base ≈ 35s). A failure well under 20s
// proves the retry loop was skipped.
//
// Each backend is its own top-level test so they can be run independently
// (e.g. `RUN_TESTS=TestMetadataNotFoundS3 ./test/integration/run.sh`).
// Backends that need cloud credentials skip themselves when the corresponding
// env var (GCS_TESTS, AZURE_TESTS, QA_TENCENT_SECRET_KEY/QA_TENCENT_SECRET_ID)
// is unset.

// metadataNotFoundCase wires one remote-storage backend to the shared scenario.
//
// root is derived in runMetadataNotFoundScenario from configFile:
// env.resolveConfigPaths reads `path` from the YAML and asks ClickHouse to
// expand {cluster}/{shard}/{version} via ApplyMacros. pathPrefix is prepended —
// used by backends where the config path is logical (mc alias, ftp chroot home)
// rather than physical. file is the metadata json key relative to `path`,
// i.e. "<backup>/metadata/<db>/<table>.json".
//
// When fsContainer is non-empty, runMetadataNotFoundScenario will `mkdir -p root`
// inside that container before the backup runs.
type metadataNotFoundCase struct {
	name        string
	configFile  string
	pathPrefix  string
	fsContainer string
	skip        func() bool
	skipReason  string
	setup       func(env *TestEnvironment, r *require.Assertions)
	// assertExists verifies the metadata json is present on remote before deletion.
	assertExists func(env *TestEnvironment, r *require.Assertions, root, file string)
	// deleteFile removes the single metadata json at root/file on remote storage.
	deleteFile func(env *TestEnvironment, r *require.Assertions, root, file string)
}

func TestMetadataNotFoundS3(t *testing.T) {
	runMetadataNotFoundCase(t, s3MetadataNotFoundCase())
}
func TestMetadataNotFoundSFTP(t *testing.T) {
	runMetadataNotFoundCase(t, sftpMetadataNotFoundCase())
}
func TestMetadataNotFoundFTP(t *testing.T) {
	runMetadataNotFoundCase(t, ftpMetadataNotFoundCase())
}
func TestMetadataNotFoundGCSEmulator(t *testing.T) {
	runMetadataNotFoundCase(t, gcsEmulatorMetadataNotFoundCase())
}
func TestMetadataNotFoundAzure(t *testing.T) {
	runMetadataNotFoundCase(t, azblobMetadataNotFoundCase())
}
func TestMetadataNotFoundGCS(t *testing.T) {
	runMetadataNotFoundCase(t, gcsRealMetadataNotFoundCase())
}
func TestMetadataNotFoundCOS(t *testing.T) {
	runMetadataNotFoundCase(t, cosMetadataNotFoundCase())
}

func runMetadataNotFoundCase(t *testing.T, tc metadataNotFoundCase) {
	if tc.skip != nil && tc.skip() {
		t.Skip(tc.skipReason)
		return
	}
	runMetadataNotFoundScenario(t, tc)
}

func runMetadataNotFoundScenario(t *testing.T, tc metadataNotFoundCase) {
	chVer := strings.ReplaceAll(os.Getenv("CLICKHOUSE_VERSION"), ".", "_")

	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	r.NoError(env.DockerCP("configs/"+tc.configFile, "clickhouse-backup:/etc/clickhouse-backup/config.yml"))

	cfgPath, _ := env.resolveConfigPaths(r, tc.configFile)
	root := tc.pathPrefix + cfgPath

	if tc.fsContainer != "" {
		env.DockerExecNoError(r, tc.fsContainer, "mkdir", "-p", root)
	}
	if tc.setup != nil {
		tc.setup(env, r)
	}

	suffix := time.Now().UnixNano()
	tableShort := fmt.Sprintf("test_metadata_not_found_%s", strings.ToLower(tc.name))
	tableName := "default." + tableShort
	backupName := fmt.Sprintf("metadata_not_found_%s_%d", chVer, suffix)
	// Remote metadata json key relative to the configured `path`. Plain ASCII
	// db/table names are encoded as-is by common.TablePathEncode.
	metaFile := fmt.Sprintf("%s/metadata/default/%s.json", backupName, tableShort)

	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id UInt64) ENGINE=MergeTree() ORDER BY id", tableName))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s SELECT number FROM numbers(100)", tableName))
	t.Cleanup(func() {
		dropQ := "DROP TABLE IF EXISTS " + tableName
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
			dropQ += " NO DELAY"
		}
		if _, err := env.DockerExecOut("clickhouse", "clickhouse", "client", "-q", dropQ); err != nil {
			log.Warn().Err(err).Str("table", tableName).Msg("t.Cleanup: failed to drop table")
		}
	})

	// Best-effort teardown so a mid-test failure does not leak the backup into a
	// shared bucket. download is expected to fail, so it may leave a partial
	// local backup — delete local first, ignore errors.
	defer func() {
		for _, cmd := range [][]string{
			{"clickhouse-backup", "delete", "local", backupName},
			{"clickhouse-backup", "delete", "remote", backupName},
		} {
			if out, err := env.DockerExecOut("clickhouse-backup", cmd...); err != nil {
				log.Warn().Err(err).Str("cmd", strings.Join(cmd, " ")).Msgf("metadataNotFound teardown: %s", out)
			}
		}
	}()

	log.Debug().Str("backend", tc.name).Msg("Create and upload a single-table backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create", "--tables", tableName, backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "upload", backupName)
	// Drop the local copy so download must read metadata from remote.
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", backupName)

	log.Debug().Str("backend", tc.name).Msg("Delete the table metadata json on remote storage")
	tc.assertExists(env, r, root, metaFile)
	tc.deleteFile(env, r, root, metaFile)

	log.Debug().Str("backend", tc.name).Msg("Download must fail fast with a not-found error (no retry backoff)")
	start := time.Now()
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "download", backupName)
	elapsed := time.Since(start)
	log.Debug().Msg(out)
	r.Error(err, "download must fail when remote table metadata is missing, output: %s", out)
	// The fail-fast path emits this distinctive message; the raw backend errors
	// (e.g. "no such file or directory", "BlobNotFound") do not contain it, so a
	// match proves isRemoteMetadataNotFound classified the 404 correctly.
	r.Contains(strings.ToLower(out), "not found on remote storage", "download must report the missing metadata as not-found, output: %s", out)
	// "Will wait near Ns and retry" (pkg/backup/backuper.go) is logged on every
	// retry attempt; its absence proves the permanent 404 broke out immediately.
	r.NotContains(out, "and retry", "download must not retry on a permanent 404, output: %s", out)
	// Defense in depth: RetriesOnFailure=3 with 5s exponential backoff would add
	// ~35s (5+10+20); a fast failure confirms the retry loop was skipped.
	r.Less(elapsed, 20*time.Second, "download must not burn the retry backoff on a permanent 404, took %s, output: %s", elapsed, out)
}

// containerFSMetadataCase builds a case for a backend that maps its remote
// storage to a path on the given docker container's filesystem.
func containerFSMetadataCase(name, configFile, container string) metadataNotFoundCase {
	return metadataNotFoundCase{
		name:        name,
		configFile:  configFile,
		fsContainer: container,
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			out, err := env.DockerExecOut(container, "ls", "-l", root+"/"+file)
			r.NoError(err, "expected %s/%s to exist on %s, output: %s", root, file, container, out)
		},
		deleteFile: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			env.DockerExecNoError(r, container, "rm", "-f", root+"/"+file)
		},
	}
}

func s3MetadataNotFoundCase() metadataNotFoundCase {
	// MinIO only sees objects that went through its S3 API, so use `mc`.
	const mcAliasCmd = "mc alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null 2>&1"
	return metadataNotFoundCase{
		name:       "S3",
		configFile: "config-s3.yml",
		pathPrefix: "local/clickhouse/", // mc alias + bucket; config path is relative to bucket
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			out, err := env.DockerExecOut("minio", "bash", "-c", fmt.Sprintf("%s && mc stat %s/%s", mcAliasCmd, root, file))
			r.NoError(err, "expected %s/%s to exist, got: %s", root, file, out)
		},
		deleteFile: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			env.DockerExecNoError(r, "minio", "bash", "-c", fmt.Sprintf("%s && mc rm %s/%s", mcAliasCmd, root, file))
		},
	}
}

func sftpMetadataNotFoundCase() metadataNotFoundCase {
	tc := containerFSMetadataCase("SFTP", "config-sftp-auth-key.yaml", "sshd")
	tc.setup = func(env *TestEnvironment, r *require.Assertions) {
		env.uploadSSHKeys(r, "clickhouse-backup")
	}
	return tc
}

func ftpMetadataNotFoundCase() metadataNotFoundCase {
	home := "/home/test_backup"
	if isAdvancedMode() {
		home = "/home/ftpusers/test_backup"
	}
	tc := containerFSMetadataCase("FTP", "config-ftp.yaml", "ftp")
	// FTP server chroots users to `home`; config paths like `/backup` resolve to
	// `<home>/backup` on the container filesystem.
	tc.pathPrefix = home
	tc.skip = func() bool { return compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") <= 0 }
	tc.skipReason = "FTP scenario only validated on ClickHouse > 21.8"
	tc.setup = func(env *TestEnvironment, r *require.Assertions) {
		// proftpd/vsftpd containers don't create `test_backup` as a system user; uid 1000 owns the home dir.
		env.DockerExecNoError(r, "ftp", "sh", "-c", fmt.Sprintf("chown -R 1000:1000 %s && chmod -R 0777 %s", home, home))
	}
	return tc
}

func gcsEmulatorMetadataNotFoundCase() metadataNotFoundCase {
	const bucket = "altinity-qa-test"
	const baseURL = "http://localhost:8080"
	// fake-gcs-server addresses single objects at /o/<url-encoded-name>, with
	// slashes escaped as %2F (url.QueryEscape).
	objURL := func(root, file string) string {
		return fmt.Sprintf("%s/storage/v1/b/%s/o/%s", baseURL, bucket, url.QueryEscape(root+"/"+file))
	}
	return metadataNotFoundCase{
		name:       "GCS_EMULATOR",
		configFile: "config-gcs-custom-endpoint.yml",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.DockerExecNoError(r, "gcs", "apk", "add", "-q", "curl")
		},
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			out, err := env.DockerExecOut("gcs", "sh", "-c", fmt.Sprintf(`curl -g -s "%s"`, objURL(root, file)))
			r.NoError(err, "assertExists curl failed: %s", out)
			r.Contains(out, `"name"`, "expected object metadata for %s/%s, got: %s", root, file, out)
		},
		deleteFile: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			env.DockerExecNoError(r, "gcs", "sh", "-c", fmt.Sprintf(`curl -g -s -X DELETE "%s"`, objURL(root, file)))
		},
	}
}

func azblobMetadataNotFoundCase() metadataNotFoundCase {
	const container = "container1"
	const accountName = "devstoreaccount1"
	const accountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	const azureCliImage = "mcr.microsoft.com/azure-cli:latest"
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
		return utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker", dockerArgs...)
	}
	blobName := func(root, file string) string { return strings.TrimPrefix(root+"/"+file, "/") }
	return metadataNotFoundCase{
		name:       "AZBLOB",
		configFile: "config-azblob.yml",
		skip:       func() bool { return isTestShouldSkip("AZURE_TESTS") },
		skipReason: "Skipping AZBLOB integration tests (AZURE_TESTS not set)",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.tc.pullImageIfNeeded(context.Background(), azureCliImage)
		},
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			out, err := azRun(env, "storage", "blob", "show", "--container-name", container, "--name", blobName(root, file))
			r.NoError(err, "expected azblob %s to exist, got: %s", blobName(root, file), out)
		},
		deleteFile: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			out, err := azRun(env, "storage", "blob", "delete", "--container-name", container, "--name", blobName(root, file))
			r.NoError(err, "azblob delete %s: %s", blobName(root, file), out)
		},
	}
}

func gcsRealMetadataNotFoundCase() metadataNotFoundCase {
	const bucket = "altinity-qa-test"
	const image = "gcr.io/google.com/cloudsdktool/google-cloud-cli:slim"
	const authPrefix = "gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS >/dev/null 2>&1 && "
	gsutil := func(env *TestEnvironment, r *require.Assertions, sh string) string {
		args := []string{
			"run", "--rm", "--network", env.tc.networkName,
			"--volumes-from", env.tc.GetContainerID("clickhouse-backup"),
			"-e", "GOOGLE_APPLICATION_CREDENTIALS=/etc/clickhouse-backup/credentials.json",
			image, "bash", "-c", authPrefix + sh,
		}
		out, err := utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker", args...)
		r.NoError(err, "gsutil command `%s` failed: %s", sh, out)
		return out
	}
	return metadataNotFoundCase{
		name:       "GCS",
		configFile: "config-gcs.yml",
		skip:       func() bool { return isTestShouldSkip("GCS_TESTS") },
		skipReason: "Skipping GCS integration tests (GCS_TESTS not set)",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.tc.pullImageIfNeeded(context.Background(), image)
		},
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			out := gsutil(env, r, fmt.Sprintf("gsutil ls gs://%s/%s/%s", bucket, root, file))
			r.Contains(out, "gs://"+bucket+"/"+root+"/"+file, "expected listing to contain %s/%s", root, file)
		},
		deleteFile: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			gsutil(env, r, fmt.Sprintf("gsutil -q rm gs://%s/%s/%s", bucket, root, file))
		},
	}
}

func cosMetadataNotFoundCase() metadataNotFoundCase {
	// COS exposes an S3-compatible API on its regional endpoint.
	const bucket = "clickhouse-backup-1336113806"
	const endpoint = "https://cos.na-ashburn.myqcloud.com"
	const image = "amazon/aws-cli:latest"
	// Tencent COS rejects path-style addressing; force virtual-hosted style.
	const awsPrefix = "aws configure set default.s3.addressing_style virtual >/dev/null && "
	awsRun := func(env *TestEnvironment, r *require.Assertions, sh string) string {
		out, err := utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker",
			"run", "--rm", "--network", env.tc.networkName,
			"-e", "AWS_ACCESS_KEY_ID="+os.Getenv("QA_TENCENT_SECRET_ID"),
			"-e", "AWS_SECRET_ACCESS_KEY="+os.Getenv("QA_TENCENT_SECRET_KEY"),
			"-e", "AWS_DEFAULT_REGION=na-ashburn",
			"--entrypoint", "sh", image, "-c", awsPrefix+sh)
		r.NoError(err, "aws-cli failed: %s", out)
		return out
	}
	return metadataNotFoundCase{
		name:       "COS",
		configFile: "config-cos.yml",
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
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			out := awsRun(env, r, fmt.Sprintf("aws --endpoint-url=%s s3 ls s3://%s/%s/%s", endpoint, bucket, root, file))
			r.Contains(out, file[strings.LastIndex(file, "/")+1:], "expected %s/%s on COS, got: %s", root, file, out)
		},
		deleteFile: func(env *TestEnvironment, r *require.Assertions, root, file string) {
			awsRun(env, r, fmt.Sprintf("aws --endpoint-url=%s s3 rm s3://%s/%s/%s", endpoint, bucket, root, file))
		},
	}
}
