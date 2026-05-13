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

// TestCleanBrokenRetention verifies that `clean_broken_retention`:
//   - lists orphans (dry-run) without deleting,
//   - on --commit removes orphans from both `path` and `object_disks_path`,
//   - preserves the live backup and entries matched by --keep globs.
//
// The body of the test is shared across all supported remote-storage backends.
// Backends that need cloud credentials skip themselves when the corresponding env
// var (GCS_TESTS, AZURE_TESTS, QA_TENCENT_SECRET_KEY) is unset.
func TestCleanBrokenRetention(t *testing.T) {
	cases := []cleanBrokenRetentionCase{
		s3CleanBrokenRetentionCase(),
		sftpCleanBrokenRetentionCase(),
		ftpCleanBrokenRetentionCase(),
		gcsEmulatorCleanBrokenRetentionCase(),
		azblobCleanBrokenRetentionCase(),
		gcsRealCleanBrokenRetentionCase(),
		cosCleanBrokenRetentionCase(),
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip() {
				t.Skip(tc.skipReason)
				return
			}
			runCleanBrokenRetentionScenario(t, tc)
		})
	}
}

// cleanBrokenRetentionCase wires one remote-storage backend to the shared scenario.
// plantOrphan, assertExists and assertGone are responsible for *physically* putting
// or observing a top-level entry under the given root path on that backend.
type cleanBrokenRetentionCase struct {
	name           string
	storageType    string
	configFile     string
	pathRoot       string // top-level prefix used by plantOrphan to simulate path orphans
	objRoot        string // top-level prefix used by plantOrphan to simulate object-disk orphans
	skip           func() bool
	skipReason     string
	setup          func(env *TestEnvironment, r *require.Assertions)
	plantOrphan    func(env *TestEnvironment, r *require.Assertions, root, name string)
	assertExists   func(env *TestEnvironment, r *require.Assertions, root, name string)
	assertGone     func(env *TestEnvironment, r *require.Assertions, root, name string)
	finalEmptyType string // value passed to env.checkObjectStorageIsEmpty at end (empty = skip)
}

func runCleanBrokenRetentionScenario(t *testing.T, tc cleanBrokenRetentionCase) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	r.NoError(env.DockerCP("configs/"+tc.configFile, "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	if tc.setup != nil {
		tc.setup(env, r)
	}

	tableName := fmt.Sprintf("default.clean_broken_retention_%s", strings.ToLower(tc.storageType))
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id UInt64) ENGINE=MergeTree() ORDER BY id", tableName))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO %s SELECT number FROM numbers(50)", tableName))

	suffix := time.Now().UnixNano()
	keepBackup := fmt.Sprintf("cbr_keep_%d", suffix)
	orphanPath := fmt.Sprintf("cbr_orphan_path_%d", suffix)
	orphanObj := fmt.Sprintf("cbr_orphan_obj_%d", suffix)
	orphanKept := fmt.Sprintf("cbr_orphan_keep_%d", suffix)
	keepGlob := fmt.Sprintf("cbr_orphan_keep_*")

	log.Debug().Str("backend", tc.name).Msg("Create a live backup that must survive the cleanup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create_remote", "--tables", tableName, keepBackup)

	log.Debug().Str("backend", tc.name).Msg("Plant orphans")
	tc.plantOrphan(env, r, tc.pathRoot, orphanPath)
	tc.plantOrphan(env, r, tc.objRoot, orphanObj)
	tc.plantOrphan(env, r, tc.pathRoot, orphanKept)
	tc.plantOrphan(env, r, tc.objRoot, orphanKept)

	tc.assertExists(env, r, tc.pathRoot, orphanPath)
	tc.assertExists(env, r, tc.objRoot, orphanObj)
	tc.assertExists(env, r, tc.pathRoot, orphanKept)
	tc.assertExists(env, r, tc.objRoot, orphanKept)

	log.Debug().Str("backend", tc.name).Msg("Dry-run lists orphans but deletes nothing")
	dryRunOut, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "clean_broken_retention")
	r.NoError(err, "dry-run failed: %s", dryRunOut)
	r.Contains(dryRunOut, orphanPath, "dry-run must mention path orphan")
	r.Contains(dryRunOut, orphanObj, "dry-run must mention object disk orphan")
	r.Contains(dryRunOut, "would delete", "dry-run must announce planned deletions")
	r.NotContains(dryRunOut, "clean_broken_retention: deleting", "dry-run must not delete")
	r.NotContains(dryRunOut, fmt.Sprintf("\"orphan\":\"%s\"", keepBackup), "live backup must not appear as orphan")
	tc.assertExists(env, r, tc.pathRoot, orphanPath)
	tc.assertExists(env, r, tc.objRoot, orphanObj)

	log.Debug().Str("backend", tc.name).Msg("--commit with --keep glob deletes only unmatched orphans")
	commitOut, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "clean_broken_retention", "--commit", "--keep="+keepGlob)
	r.NoError(err, "commit failed: %s", commitOut)
	r.Contains(commitOut, "clean_broken_retention: deleting")
	tc.assertGone(env, r, tc.pathRoot, orphanPath)
	tc.assertGone(env, r, tc.objRoot, orphanObj)
	tc.assertExists(env, r, tc.pathRoot, orphanKept)
	tc.assertExists(env, r, tc.objRoot, orphanKept)

	log.Debug().Str("backend", tc.name).Msg("Second --commit without --keep clears the remaining orphan")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean_broken_retention", "--commit")
	tc.assertGone(env, r, tc.pathRoot, orphanKept)
	tc.assertGone(env, r, tc.objRoot, orphanKept)

	log.Debug().Str("backend", tc.name).Msg("Cleanup live backup and table")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", keepBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", keepBackup)
	dropQuery := "DROP TABLE IF EXISTS " + tableName
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
		dropQuery += " NO DELAY"
	}
	env.queryWithNoError(r, dropQuery)
	if tc.finalEmptyType != "" {
		env.checkObjectStorageIsEmpty(t, r, tc.finalEmptyType)
	}
}

// ---- helpers for backends that map remote storage to a docker-container filesystem ----

func plantOnContainerFS(container string) func(env *TestEnvironment, r *require.Assertions, root, name string) {
	return func(env *TestEnvironment, r *require.Assertions, root, name string) {
		env.DockerExecNoError(r, container, "bash", "-c", fmt.Sprintf(
			"mkdir -p %s/%s/sub && echo garbage > %s/%s/data.bin && echo garbage > %s/%s/sub/nested.bin",
			root, name, root, name, root, name,
		))
	}
}
func assertContainerFSExists(container string) func(env *TestEnvironment, r *require.Assertions, root, name string) {
	return func(env *TestEnvironment, r *require.Assertions, root, name string) {
		out, err := env.DockerExecOut(container, "ls", root+"/"+name)
		r.NoError(err, "expected %s/%s to exist on %s, output: %s", root, name, container, out)
	}
}
func assertContainerFSGone(container string) func(env *TestEnvironment, r *require.Assertions, root, name string) {
	return func(env *TestEnvironment, r *require.Assertions, root, name string) {
		out, err := env.DockerExecOut(container, "bash", "-c", fmt.Sprintf("ls %s/%s 2>/dev/null || true", root, name))
		r.Empty(strings.TrimSpace(out), "expected %s/%s on %s to be removed, ls returned: %s", root, name, container, out)
		_ = err
	}
}

// ---- per-backend case factories ----

func s3CleanBrokenRetentionCase() cleanBrokenRetentionCase {
	return cleanBrokenRetentionCase{
		name:           "S3",
		storageType:    "S3",
		configFile:     "config-s3.yml",
		pathRoot:       "/minio/data/clickhouse/backup/cluster/0",
		objRoot:        "/minio/data/clickhouse/object_disk/cluster/0",
		skip:           func() bool { return false },
		plantOrphan:    plantOnContainerFS("minio"),
		assertExists:   assertContainerFSExists("minio"),
		assertGone:     assertContainerFSGone("minio"),
		finalEmptyType: "S3",
	}
}

func sftpCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	return cleanBrokenRetentionCase{
		name:        "SFTP",
		storageType: "SFTP",
		configFile:  "config-sftp-auth-key.yaml",
		pathRoot:    "/root",
		objRoot:     "/object_disk",
		skip:        func() bool { return false },
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.uploadSSHKeys(r, "clickhouse-backup")
			env.DockerExecNoError(r, "sshd", "mkdir", "-p", "/object_disk")
		},
		plantOrphan:    plantOnContainerFS("sshd"),
		assertExists:   assertContainerFSExists("sshd"),
		assertGone:     assertContainerFSGone("sshd"),
		finalEmptyType: "",
	}
}

func ftpCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	homePrefix := "/home/test_backup"
	if isAdvancedMode() {
		homePrefix = "/home/ftpusers/test_backup"
	}
	return cleanBrokenRetentionCase{
		name:        "FTP",
		storageType: "FTP",
		configFile:  "config-ftp.yaml",
		pathRoot:    homePrefix + "/backup",
		objRoot:     homePrefix + "/object_disk",
		skip:        func() bool { return compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") <= 0 },
		skipReason:  "FTP scenario only validated on ClickHouse > 21.8",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.DockerExecNoError(r, "ftp", "sh", "-c", fmt.Sprintf("mkdir -p %s/backup %s/object_disk && chown -R test_backup:test_backup %s", homePrefix, homePrefix, homePrefix))
		},
		plantOrphan:    plantOnContainerFS("ftp"),
		assertExists:   assertContainerFSExists("ftp"),
		assertGone:     assertContainerFSGone("ftp"),
		finalEmptyType: "",
	}
}

func gcsEmulatorCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	return cleanBrokenRetentionCase{
		name:           "GCS_EMULATOR",
		storageType:    "GCS_EMULATOR",
		configFile:     "config-gcs-custom-endpoint.yml",
		pathRoot:       "/data/altinity-qa-test/backup/cluster/0",
		objRoot:        "/data/altinity-qa-test/object_disks/cluster/0",
		skip:           func() bool { return false },
		plantOrphan:    plantOnContainerFS("gcs"),
		assertExists:   assertContainerFSExists("gcs"),
		assertGone:     assertContainerFSGone("gcs"),
		finalEmptyType: "GCS_EMULATOR",
	}
}

func gcsRealCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	return cleanBrokenRetentionCase{
		name:        "GCS",
		storageType: "GCS",
		configFile:  "config-gcs.yml",
		skip:        func() bool { return isTestShouldSkip("GCS_TESTS") },
		skipReason:  "Skipping GCS integration tests (GCS_TESTS not set)",
		plantOrphan: func(env *TestEnvironment, r *require.Assertions, root, name string) {
			// real GCS — would require gsutil; out of scope without credentials infra.
			r.FailNow("plantOrphan for real GCS is not implemented; gate this case behind credentials")
		},
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, name string) {},
		assertGone:   func(env *TestEnvironment, r *require.Assertions, root, name string) {},
	}
}

func cosCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	return cleanBrokenRetentionCase{
		name:        "COS",
		storageType: "COS",
		configFile:  "config-cos.yml",
		skip:        func() bool { return os.Getenv("QA_TENCENT_SECRET_KEY") == "" },
		skipReason:  "Skipping COS integration tests (QA_TENCENT_SECRET_KEY not set)",
		plantOrphan: func(env *TestEnvironment, r *require.Assertions, root, name string) {
			r.FailNow("plantOrphan for COS is not implemented; gate this case behind credentials")
		},
		assertExists: func(env *TestEnvironment, r *require.Assertions, root, name string) {},
		assertGone:   func(env *TestEnvironment, r *require.Assertions, root, name string) {},
	}
}

func azblobCleanBrokenRetentionCase() cleanBrokenRetentionCase {
	const account = "devstoreaccount1"
	const accountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	const container = "container1"
	connectionString := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://devstoreaccount1.blob.azure:10000/devstoreaccount1;", account, accountKey)

	plant := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		blobPath := strings.TrimPrefix(root+"/"+name, "/")
		cmd := []string{
			"run", "--rm", "--network", env.tc.networkName,
			"-e", "AZURE_STORAGE_CONNECTION_STRING=" + connectionString,
			"mcr.microsoft.com/azure-cli:latest",
			"sh", "-c",
			fmt.Sprintf("echo garbage > /tmp/data.bin && az storage blob upload --container-name %s --name %s/data.bin --file /tmp/data.bin --overwrite >/dev/null && az storage blob upload --container-name %s --name %s/sub/nested.bin --file /tmp/data.bin --overwrite >/dev/null",
				container, blobPath, container, blobPath),
		}
		out, err := utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker", cmd...)
		r.NoError(err, "azblob plantOrphan failed: %s", out)
	}
	exists := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		blobPath := strings.TrimPrefix(root+"/"+name, "/")
		out, err := utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker",
			"run", "--rm", "--network", env.tc.networkName,
			"-e", "AZURE_STORAGE_CONNECTION_STRING="+connectionString,
			"mcr.microsoft.com/azure-cli:latest",
			"sh", "-c",
			fmt.Sprintf("az storage blob list --container-name %s --prefix %s/ --num-results 1 --query '[].name' -o tsv", container, blobPath),
		)
		r.NoError(err, "azblob list failed: %s", out)
		r.NotEmpty(strings.TrimSpace(out), "expected blobs under %s/, got empty list", blobPath)
	}
	gone := func(env *TestEnvironment, r *require.Assertions, root, name string) {
		blobPath := strings.TrimPrefix(root+"/"+name, "/")
		out, err := utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker",
			"run", "--rm", "--network", env.tc.networkName,
			"-e", "AZURE_STORAGE_CONNECTION_STRING="+connectionString,
			"mcr.microsoft.com/azure-cli:latest",
			"sh", "-c",
			fmt.Sprintf("az storage blob list --container-name %s --prefix %s/ --num-results 1 --query '[].name' -o tsv", container, blobPath),
		)
		r.NoError(err, "azblob list failed: %s", out)
		r.Empty(strings.TrimSpace(out), "expected no blobs under %s/, got: %s", blobPath, out)
	}
	return cleanBrokenRetentionCase{
		name:        "AZBLOB",
		storageType: "AZBLOB",
		configFile:  "config-azblob.yml",
		// per config-azblob.yml: path=backup, object_disk_path=object_disks (no macros)
		pathRoot:    "backup",
		objRoot:     "object_disks",
		skip:        func() bool { return isTestShouldSkip("AZURE_TESTS") },
		skipReason:  "Skipping AZBLOB integration tests (AZURE_TESTS not set)",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.tc.pullImageIfNeeded(context.Background(), "mcr.microsoft.com/azure-cli:latest")
		},
		plantOrphan:    plant,
		assertExists:   exists,
		assertGone:     gone,
		finalEmptyType: "",
	}
}
