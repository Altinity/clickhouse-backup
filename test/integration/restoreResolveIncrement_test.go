//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/stretchr/testify/require"
)

type restoreResolveIncrementCase struct {
	name       string
	configFile string
	setup      func(*TestEnvironment, *require.Assertions)
	copyRemote func(*testing.T, *require.Assertions, *TestEnvironment, string, string)
	skip       func() bool
	skipReason string
}

func TestRestoreResolveIncrementS3(t *testing.T) {
	runRestoreResolveIncrementCase(t, s3RestoreResolveIncrementCase())
}

func TestRestoreResolveIncrementFTP(t *testing.T) {
	runRestoreResolveIncrementCase(t, ftpRestoreResolveIncrementCase())
}

func TestRestoreResolveIncrementSFTP(t *testing.T) {
	runRestoreResolveIncrementCase(t, sftpRestoreResolveIncrementCase())
}

func TestRestoreResolveIncrementGCSEmulator(t *testing.T) {
	runRestoreResolveIncrementCase(t, gcsEmulatorRestoreResolveIncrementCase())
}

func TestRestoreResolveIncrementAzure(t *testing.T) {
	runRestoreResolveIncrementCase(t, azblobRestoreResolveIncrementCase())
}

func runRestoreResolveIncrementCase(t *testing.T, tc restoreResolveIncrementCase) {
	if tc.skip != nil && tc.skip() {
		t.Skip(tc.skipReason)
		return
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)
	if tc.setup != nil {
		tc.setup(env, r)
	}

	runRestoreResolveIncrementScenario(t, r, env, tc, "remote_chain", true)
	runRestoreResolveIncrementScenario(t, r, env, tc, "local_full", false)
}

func runRestoreResolveIncrementScenario(t *testing.T, r *require.Assertions, env *TestEnvironment, tc restoreResolveIncrementCase, suffix string, fullRemote bool) {
	dbName := strings.ToLower("test_restore_resolve_" + tc.name + "_" + suffix)
	tableName := "data"
	fullBackup := dbName + "_full"
	increment1Backup := dbName + "_increment1"
	increment2Backup := dbName + "_increment2"
	backups := []string{fullBackup, increment1Backup, increment2Backup}

	defer func() {
		fullCleanup(t, r, env, backups, []string{"remote", "local"}, []string{dbName}, false, false, false, tc.configFile)
	}()

	for _, backupName := range backups {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+tc.configFile+" delete remote "+backupName+" 2>/dev/null || true")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+tc.configFile+" delete local "+backupName+" 2>/dev/null || true")
	}
	r.NoError(env.dropDatabase(dbName, true))

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+"."+tableName+" (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(t, r, "SYSTEM STOP MERGES "+dbName+"."+tableName)
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+"."+tableName+" SELECT number FROM numbers(100)")

	if fullRemote {
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+tc.configFile, "create_remote", "--delete-source", "--tables="+dbName+".*", fullBackup)
	} else {
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+tc.configFile, "create", "--tables="+dbName+".*", fullBackup)
	}

	env.queryWithNoError(t, r, "INSERT INTO "+dbName+"."+tableName+" SELECT number+100 FROM numbers(100)")
	if fullRemote {
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+tc.configFile, "create_remote", "--delete-source", "--diff-from-remote="+fullBackup, "--tables="+dbName+".*", increment1Backup)
	} else {
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+tc.configFile, "create_remote", "--delete-source", "--diff-from="+fullBackup, "--tables="+dbName+".*", increment1Backup)
	}

	env.queryWithNoError(t, r, "INSERT INTO "+dbName+"."+tableName+" SELECT number+200 FROM numbers(100)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+tc.configFile, "create_remote", "--delete-source", "--diff-from-remote="+increment1Backup, "--tables="+dbName+".*", increment2Backup)

	tc.copyRemote(t, r, env, tc.configFile, increment2Backup)
	verifyRestoreResolveRequiredMetadata(r, env, increment2Backup, increment1Backup, dbName, tableName)

	dropSQL := "DROP TABLE " + dbName + "." + tableName
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		dropSQL += " SYNC"
	}
	env.queryWithNoError(t, r, dropSQL)
	restoreOut, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("LOG_LEVEL=debug clickhouse-backup -c /etc/clickhouse-backup/%s restore --rm --tables=%s.%s %s 2>&1", tc.configFile, dbName, tableName, increment2Backup))
	r.NoError(err, restoreOut)
	r.Contains(restoreOut, "restore required part download")
	r.Contains(restoreOut, increment1Backup)
	if fullRemote {
		r.Contains(restoreOut, fullBackup)
	} else {
		r.Contains(restoreOut, "restore required part via hardlink")
		r.Contains(restoreOut, fullBackup)
	}
	env.checkCount(r, 1, 300, "SELECT count() FROM "+dbName+"."+tableName)

	fullCleanup(t, r, env, backups, []string{"remote", "local"}, []string{dbName}, false, false, true, tc.configFile)
}

func verifyRestoreResolveRequiredMetadata(r *require.Assertions, env *TestEnvironment, backupName, requiredBackup, dbName, tableName string) {
	metadataFile := path.Join("/var/lib/clickhouse/backup", backupName, "metadata.json")
	tableMetadataFile := path.Join("/var/lib/clickhouse/backup", backupName, "metadata", common.TablePathEncode(dbName), common.TablePathEncode(tableName)+".json")
	out, err := env.DockerExecOut("clickhouse-backup", "cat", metadataFile)
	r.NoError(err, out)
	var backupMetadata struct {
		Required string `json:"required_backup"`
	}
	r.NoError(json.Unmarshal([]byte(out), &backupMetadata))
	r.Equal(requiredBackup, backupMetadata.Required)

	out, err = env.DockerExecOut("clickhouse-backup", "cat", tableMetadataFile)
	r.NoError(err, out)
	var tableMetadata struct {
		Parts map[string][]struct {
			Required bool `json:"required,omitempty"`
		} `json:"parts"`
	}
	r.NoError(json.Unmarshal([]byte(out), &tableMetadata))
	requiredParts := 0
	for _, parts := range tableMetadata.Parts {
		for _, part := range parts {
			if part.Required {
				requiredParts++
			}
		}
	}
	r.Greater(requiredParts, 0, "expected required parts in %s", tableMetadataFile)
}

func copyRemoteBackupFromContainerFS(r *require.Assertions, env *TestEnvironment, container, remoteRoot, backupName string) {
	tmpDir, err := os.MkdirTemp("", "restore-resolve-increment-*")
	r.NoError(err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	localCopy := filepath.Join(tmpDir, backupName)
	r.NoError(env.DockerCP(container+":"+path.Join(remoteRoot, backupName), localCopy))
	env.DockerExecNoError(r, "clickhouse-backup", "rm", "-rf", path.Join("/var/lib/clickhouse/backup", backupName))
	r.NoError(env.DockerCP(localCopy, "clickhouse-backup:"+path.Join("/var/lib/clickhouse/backup", backupName)))
}

func s3RestoreResolveIncrementCase() restoreResolveIncrementCase {
	return restoreResolveIncrementCase{
		name:       "s3",
		configFile: "config-s3.yml",
		copyRemote: func(t *testing.T, r *require.Assertions, env *TestEnvironment, configFile, backupName string) {
			cfgPath, _ := env.resolveConfigPaths(r, configFile)
			remotePath := path.Join("local/clickhouse", cfgPath, backupName)
			tmpPath := path.Join("/tmp/restore-resolve-increment", backupName)
			env.DockerExecNoError(r, "minio", "bash", "-ce", fmt.Sprintf("mc alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null 2>&1 && rm -rf %[2]s && mc mirror %[1]s %[2]s", remotePath, tmpPath))
			copyRemoteBackupFromContainerFS(r, env, "minio", "/tmp/restore-resolve-increment", backupName)
		},
	}
}

func ftpRestoreResolveIncrementCase() restoreResolveIncrementCase {
	home := "/home/test_backup"
	if isAdvancedMode() {
		home = "/home/ftpusers/test_backup"
	}
	configFile := "config-ftp.yaml"
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") < 0 {
		configFile = "config-ftp-old.yaml"
	}
	return restoreResolveIncrementCase{
		name:       "ftp",
		configFile: configFile,
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.DockerExecNoError(r, "ftp", "sh", "-c", fmt.Sprintf("chown -R 1000:1000 %s && chmod -R 0777 %s", home, home))
			env.InstallDebIfNotExists(r, "clickhouse-backup", "lftp")
		},
		copyRemote: func(t *testing.T, r *require.Assertions, env *TestEnvironment, configFile, backupName string) {
			cfgPath, _ := env.resolveConfigPaths(r, configFile)
			remotePath := path.Join(cfgPath, backupName)
			localPath := path.Join("/var/lib/clickhouse/backup", backupName)
			env.DockerExecNoError(r, "clickhouse-backup", "rm", "-rf", localPath)
			env.DockerExecNoError(r, "clickhouse-backup", "mkdir", "-p", localPath)
			env.DockerExecNoError(r, "clickhouse-backup", "sh", "-ce", fmt.Sprintf(`
lftp -d -u test_backup,test_backup ftp://ftp -e '
debug -o /dev/stderr 10
set cmd:trace yes
set ftp:ssl-allow no
set ftp:prefer-epsv yes
set net:timeout 30
set net:max-retries 3
set cmd:fail-exit yes
mirror --verbose --only-missing --parallel=1 %[1]s %[2]s
bye
'
`, remotePath, localPath))
		},
	}
}

func sftpRestoreResolveIncrementCase() restoreResolveIncrementCase {
	return restoreResolveIncrementCase{
		name:       "sftp",
		configFile: "config-sftp-auth-key.yaml",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.uploadSSHKeys(r, "clickhouse-backup")
		},
		copyRemote: func(t *testing.T, r *require.Assertions, env *TestEnvironment, configFile, backupName string) {
			cfgPath, _ := env.resolveConfigPaths(r, configFile)
			copyRemoteBackupFromContainerFS(r, env, "sshd", cfgPath, backupName)
		},
	}
}

func gcsEmulatorRestoreResolveIncrementCase() restoreResolveIncrementCase {
	return restoreResolveIncrementCase{
		name:       "gcs_emulator",
		configFile: "config-gcs-custom-endpoint.yml",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.DockerExecNoError(r, "gcs", "apk", "add", "-q", "curl", "jq")
		},
		copyRemote: func(t *testing.T, r *require.Assertions, env *TestEnvironment, configFile, backupName string) {
			tmpPath := path.Join("/tmp/restore-resolve-increment-gcs", backupName)
			env.DockerExecNoError(r, "gcs", "sh", "-ce", fmt.Sprintf(`
rm -rf %[2]s
mkdir -p %[2]s
curl -s 'http://localhost:8080/storage/v1/b/altinity-qa-test/o' | jq -r '.items[] | select(.name | contains("/%[1]s/")) | .name | [. , @uri] | @tsv' |
while IFS='	' read -r name encoded; do
  rel="${name#*/%[1]s/}"
  mkdir -p "%[2]s/$(dirname "$rel")"
  curl -s -o "%[2]s/$rel" "http://localhost:8080/download/storage/v1/b/altinity-qa-test/o/$encoded?alt=media"
done
`, backupName, tmpPath))
			copyRemoteBackupFromContainerFS(r, env, "gcs", "/tmp/restore-resolve-increment-gcs", backupName)
		},
	}
}

func azblobRestoreResolveIncrementCase() restoreResolveIncrementCase {
	const image = "mcr.microsoft.com/azure-cli:latest"
	const container = "container1"
	const accountName = "devstoreaccount1"
	const accountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	azConnString := fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://azure:10000/%s;",
		accountName, accountKey, accountName,
	)
	return restoreResolveIncrementCase{
		name:       "azblob",
		configFile: "config-azblob.yml",
		skip:       func() bool { return isTestShouldSkip("AZURE_TESTS") },
		skipReason: "Skipping AZBLOB integration tests (AZURE_TESTS not set)",
		setup: func(env *TestEnvironment, r *require.Assertions) {
			env.tc.pullImageIfNeeded(context.Background(), image)
		},
		copyRemote: func(t *testing.T, r *require.Assertions, env *TestEnvironment, configFile, backupName string) {
			cfgPath, _ := env.resolveConfigPaths(r, configFile)
			remotePrefix := strings.TrimPrefix(path.Join(cfgPath, backupName), "/")
			localPath := path.Join("/var/lib/clickhouse/backup", backupName)
			env.DockerExecNoError(r, "clickhouse-backup", "rm", "-rf", localPath)
			env.DockerExecNoError(r, "clickhouse-backup", "mkdir", "-p", localPath)
			cmd := fmt.Sprintf(
				"az storage blob download-batch --source %[1]s --pattern '%[2]s/*' --destination %[3]s && cp -a %[3]s/%[2]s/. %[3]s/ && rm -rf %[3]s/%[4]s",
				container,
				remotePrefix,
				localPath,
				strings.Split(remotePrefix, "/")[0],
			)
			args := []string{
				"run", "--rm", "--network", env.tc.networkName,
				"--volumes-from", env.tc.GetContainerID("clickhouse-backup"),
				"-e", "AZURE_STORAGE_CONNECTION_STRING=" + azConnString,
				image, "bash", "-ce", cmd,
			}
			out, err := utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker", args...)
			r.NoError(err, "azblob download-batch failed: %s", out)
		},
	}
}
