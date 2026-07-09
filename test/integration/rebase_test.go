//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/stretchr/testify/require"
)

// The TestRebase* family covers the `rebase` command: required parts from the
// whole `required_backup` chain (full <- inc1 <- inc2) must be copied into the
// rebased increment via server-side CopyObject, `required` attributes and the
// `required_backup` dependency must be removed, so the increment becomes a
// full backup restorable after all its ancestors are deleted.
// Each storage backend is a separate top-level test so they run in parallel
// (NewTestEnvironment acquires a dedicated env from the pool and calls t.Parallel()).
// S3 covers the archive (tar) data_format, S3Directory covers the `directory`
// data_format over native server-side CopyObject including object disk blobs,
// SFTP covers the `directory` data_format (compression_format: none) and the
// `copy-data`/hardlink chain,
// FTP covers the `SITE CPFR`/`SITE CPTO` (ProFTPD mod_copy) / FXP / streaming chain,
// GCS emulator / AZBLOB (azurite) / real GCS / real COS cover the native server-side copy implementations.
// Tables with s3_only/gcs_only/azure_only storage policies additionally cover
// object disk blobs copying under `object_disk_path`.

// rebaseCHVersion returns the ClickHouse version under test, defaulting to "head".
func rebaseCHVersion() string {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if version == "" {
		version = "head"
	}
	return version
}

func TestRebaseS3(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	// storage policies for object disks exist only in advanced mode configs, version gates match generateTestData
	s3Policy := ""
	if isAdvancedMode() && compareVersion(rebaseCHVersion(), "21.8") >= 0 {
		s3Policy = "s3_only"
	}
	runRebaseScenario(t, r, env, "test_rebase_s3", "/etc/clickhouse-backup/config-s3.yml", s3Policy, "tar")
}

// TestRebaseS3Directory covers `data_format: directory` (compression_format: none) over native
// server-side CopyObject: rebaseCopyPart copies the part file-by-file via Walk instead of one
// archive per part, and object disk blob metadata is read via Walk+GetFileReader instead of
// WalkCompressedStream. SFTP/FTP also use the `directory` data_format but have no object disk
// storage policies, so this branch is only reachable on S3.
func TestRebaseS3Directory(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	s3Policy := ""
	if isAdvancedMode() && compareVersion(rebaseCHVersion(), "21.8") >= 0 {
		s3Policy = "s3_only"
	}
	runRebaseScenario(t, r, env, "test_rebase_s3_dir", "/etc/clickhouse-backup/config-s3.yml", s3Policy, "directory", "--env", "S3_COMPRESSION_FORMAT=none")
}

func TestRebaseSFTP(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	runRebaseScenario(t, r, env, "test_rebase_sftp", "/etc/clickhouse-backup/config-sftp-auth-password.yaml", "", "directory")
}

func TestRebaseFTP(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	// old versions can't execute SYSTEM RESTORE REPLICA, so restore_as_attach must stay disabled (config-ftp-old.yaml)
	ftpConfig := "/etc/clickhouse-backup/config-ftp.yaml"
	if compareVersion(rebaseCHVersion(), "21.8") < 0 {
		ftpConfig = "/etc/clickhouse-backup/config-ftp-old.yaml"
	}
	runRebaseScenario(t, r, env, "test_rebase_ftp", ftpConfig, "", "directory")
}

func TestRebaseGCS(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	gcsPolicy := ""
	if isAdvancedMode() && compareVersion(rebaseCHVersion(), "22.6") >= 0 && os.Getenv("QA_GCS_OVER_S3_BUCKET") != "" {
		gcsPolicy = "gcs_only"
	}
	runRebaseScenario(t, r, env, "test_rebase_gcs_emulator", "/etc/clickhouse-backup/config-gcs-custom-endpoint.yml", gcsPolicy, "tar")
	if !isTestShouldSkip("GCS_TESTS") {
		runRebaseScenario(t, r, env, "test_rebase_gcs", "/etc/clickhouse-backup/config-gcs.yml", gcsPolicy, "tar")
	} else {
		t.Log("skip test_rebase_gcs scenario, GCS_TESTS missing")
	}
}

func TestRebaseAzblob(t *testing.T) {
	if isTestShouldSkip("AZURE_TESTS") {
		t.Skip("skip TestRebaseAzblob, AZURE_TESTS missing")
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	azurePolicy := ""
	if isAdvancedMode() && compareVersion(rebaseCHVersion(), "23.3") >= 0 {
		azurePolicy = "azure_only"
	}
	runRebaseScenario(t, r, env, "test_rebase_azblob", "/etc/clickhouse-backup/config-azblob.yml", azurePolicy, "tar")
}

func TestRebaseCOS(t *testing.T) {
	if os.Getenv("QA_TENCENT_SECRET_KEY") == "" {
		t.Skip("skip TestRebaseCOS, QA_TENCENT_SECRET_KEY missing")
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	env.InstallDebIfNotExists(r, "clickhouse-backup", "gettext-base")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config-cos.yml.template | envsubst > /etc/clickhouse-backup/config-cos.yml")
	// config-cos.yml.template doesn't pin compression_format, skip the data_format check
	runRebaseScenario(t, r, env, "test_rebase_cos", "/etc/clickhouse-backup/config-cos.yml", "", "")
}

// expectedDataFormat - when non-empty, the rebased backup metadata.json must keep this data_format
// (guards against the `--env <REMOTE>_COMPRESSION_FORMAT=...` override in createExtraArgs silently
// degrading to the config file default); createExtraArgs are appended to every create_remote call
// so the whole chain is uploaded in the same data_format, as `rebase` requires.
func runRebaseScenario(t *testing.T, r *require.Assertions, env *TestEnvironment, dbName, configFile, objectDiskPolicy, expectedDataFormat string, createExtraArgs ...string) {
	tables := []string{"t1"}
	if objectDiskPolicy != "" {
		tables = append(tables, "t2_"+objectDiskPolicy)
	}
	fullBackup := dbName + "_full"
	inc1Backup := dbName + "_inc1"
	inc2Backup := dbName + "_inc2"

	for _, b := range []string{fullBackup, inc1Backup, inc2Backup} {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c "+configFile+" delete remote "+b+" 2>/dev/null || true")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup -c "+configFile+" delete local "+b+" 2>/dev/null || true")
	}
	r.NoError(env.dropDatabase(dbName, true))

	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	for _, table := range tables {
		policySettings := ""
		if table != "t1" {
			policySettings = fmt.Sprintf(" SETTINGS storage_policy='%s'", objectDiskPolicy)
		}
		env.queryWithNoError(t, r, "CREATE TABLE "+dbName+"."+table+" (id UInt64, v String) ENGINE=MergeTree() ORDER BY id"+policySettings)
		// keep part names stable between backups, so unchanged parts get `required` attribute in increments
		env.queryWithNoError(t, r, "SYSTEM STOP MERGES "+dbName+"."+table)
	}

	insertRound := func(offset int, payload string) {
		for _, table := range tables {
			env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.%s SELECT number+%d, '%s' FROM numbers(100)", dbName, table, offset, payload))
		}
	}

	createRemote := func(backupName, diffFromRemote string) {
		cmd := []string{"clickhouse-backup", "-c", configFile, "create_remote", "--tables=" + dbName + ".*"}
		if diffFromRemote != "" {
			cmd = append(cmd, "--diff-from-remote="+diffFromRemote)
		}
		cmd = append(cmd, createExtraArgs...)
		cmd = append(cmd, backupName)
		env.DockerExecNoError(r, "clickhouse-backup", cmd...)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", backupName)
	}

	insertRound(0, "full")
	createRemote(fullBackup, "")

	insertRound(100, "inc1")
	createRemote(inc1Backup, fullBackup)

	insertRound(200, "inc2")
	createRemote(inc2Backup, inc1Backup)

	// rebase on a backup without `required_backup` must fail
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "rebase", fullBackup)
	r.Error(err, "rebase %s expected error, got: %s", fullBackup, out)
	r.Contains(out, "nothing to rebase")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "rebase", inc2Backup)

	// the rebased backup on remote storage must keep the chain data_format and lose `required_backup`,
	// check via `list remote` (local metadata.json is unusable here - download clears `data_format`)
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "list", "remote")
	r.NoError(err, "list remote: %s", out)
	inc2Line := ""
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, inc2Backup+" ") {
			inc2Line = line
			break
		}
	}
	r.NotEmptyf(inc2Line, "%s not found in `list remote` output: %s", inc2Backup, out)
	r.NotContainsf(inc2Line, "+", "expected empty required_backup after rebase")
	if expectedDataFormat != "" {
		r.Containsf(inc2Line, expectedDataFormat, "expected data_format=%s after rebase", expectedDataFormat)
	}

	// ancestors are not needed anymore
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "remote", inc1Backup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "remote", fullBackup)

	// synchronous `DROP TABLE ... NO DELAY` is available after 20.3, older versions use a plain DROP
	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
		dropSuffix = " NO DELAY"
	}
	for _, table := range tables {
		env.queryWithNoError(t, r, "DROP TABLE "+dbName+"."+table+dropSuffix)
	}
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "restore_remote", "--rm", inc2Backup)
	for _, table := range tables {
		env.checkCount(r, 1, 300, "SELECT count() FROM "+dbName+"."+table)
	}

	// downloaded table metadata must not contain `required` parts anymore
	type partMeta struct {
		Name     string `json:"name"`
		Required bool   `json:"required,omitempty"`
	}
	type tableMetaJSON struct {
		Parts          map[string][]partMeta `json:"parts"`
		HashOfAllFiles map[string]string     `json:"hash_of_all_files"`
	}
	for _, table := range tables {
		metaPath := path.Join("/var/lib/clickhouse/backup", inc2Backup, "metadata", common.TablePathEncode(dbName), common.TablePathEncode(table)+".json")
		out, err = env.DockerExecOut("clickhouse-backup", "cat", metaPath)
		r.NoError(err, "cat %s: %s", metaPath, out)
		var tm tableMetaJSON
		r.NoError(json.Unmarshal([]byte(out), &tm))
		partsCount := 0
		for disk, parts := range tm.Parts {
			partsCount += len(parts)
			for _, p := range parts {
				r.Falsef(p.Required, "%s disk %s part %s: expected Required=false after rebase; metadata=%s", table, disk, p.Name, out)
			}
		}
		r.Equalf(3, partsCount, "%s: expected 3 parts after rebase; metadata=%s", table, out)
	}

	// Cleanup
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "remote", inc2Backup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", inc2Backup)
	r.NoError(env.dropDatabase(dbName, true))
}
