//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/stretchr/testify/require"
)

// TestRebase covers the `rebase` command: required parts from the whole
// `required_backup` chain (full <- inc1 <- inc2) must be copied into the
// rebased increment via server-side CopyObject, `required` attributes and the
// `required_backup` dependency must be removed, so the increment becomes a
// full backup restorable after all its ancestors are deleted.
// S3 covers the archive (tar) data_format, SFTP covers the `directory`
// data_format (compression_format: none) and the `copy-data`/hardlink chain,
// FTP covers the `SITE CPFR`/`SITE CPTO` (ProFTPD mod_copy) / FXP / streaming chain,
// GCS emulator / AZBLOB (azurite) / real GCS / real COS cover the native server-side copy implementations.
// Tables with s3_only/gcs_only/azure_only storage policies additionally cover
// object disk blobs copying under `object_disk_path`.
func TestRebase(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	version := os.Getenv("CLICKHOUSE_VERSION")
	if version == "" {
		version = "head"
	}
	// storage policies for object disks exist only in advanced mode configs, version gates match generateTestData
	s3Policy, gcsPolicy, azurePolicy := "", "", ""
	if isAdvancedMode() {
		if compareVersion(version, "21.8") >= 0 {
			s3Policy = "s3_only"
		}
		if compareVersion(version, "22.6") >= 0 && os.Getenv("QA_GCS_OVER_S3_BUCKET") != "" {
			gcsPolicy = "gcs_only"
		}
		if compareVersion(version, "23.3") >= 0 {
			azurePolicy = "azure_only"
		}
	}

	runRebaseScenario(t, r, env, "test_rebase_s3", "/etc/clickhouse-backup/config-s3.yml", s3Policy)
	runRebaseScenario(t, r, env, "test_rebase_sftp", "/etc/clickhouse-backup/config-sftp-auth-password.yaml", "")
	runRebaseScenario(t, r, env, "test_rebase_ftp", "/etc/clickhouse-backup/config-ftp.yaml", "")
	runRebaseScenario(t, r, env, "test_rebase_gcs_emulator", "/etc/clickhouse-backup/config-gcs-custom-endpoint.yml", gcsPolicy)
	if !isTestShouldSkip("AZURE_TESTS") {
		runRebaseScenario(t, r, env, "test_rebase_azblob", "/etc/clickhouse-backup/config-azblob.yml", azurePolicy)
	} else {
		t.Log("skip test_rebase_azblob scenario, AZURE_TESTS missing")
	}
	if !isTestShouldSkip("GCS_TESTS") {
		runRebaseScenario(t, r, env, "test_rebase_gcs", "/etc/clickhouse-backup/config-gcs.yml", gcsPolicy)
	} else {
		t.Log("skip test_rebase_gcs scenario, GCS_TESTS missing")
	}
	if os.Getenv("QA_TENCENT_SECRET_KEY") != "" {
		env.InstallDebIfNotExists(r, "clickhouse-backup", "gettext-base")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config-cos.yml.template | envsubst > /etc/clickhouse-backup/config-cos.yml")
		runRebaseScenario(t, r, env, "test_rebase_cos", "/etc/clickhouse-backup/config-cos.yml", "")
	} else {
		t.Log("skip test_rebase_cos scenario, QA_TENCENT_SECRET_KEY missing")
	}
}

func runRebaseScenario(t *testing.T, r *require.Assertions, env *TestEnvironment, dbName, configFile, objectDiskPolicy string) {
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

	insertRound(0, "full")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "create_remote", "--tables="+dbName+".*", fullBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", fullBackup)

	insertRound(100, "inc1")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "create_remote", "--tables="+dbName+".*", "--diff-from-remote="+fullBackup, inc1Backup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", inc1Backup)

	insertRound(200, "inc2")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "create_remote", "--tables="+dbName+".*", "--diff-from-remote="+inc1Backup, inc2Backup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "local", inc2Backup)

	// rebase on a backup without `required_backup` must fail
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", configFile, "rebase", fullBackup)
	r.Error(err, "rebase %s expected error, got: %s", fullBackup, out)
	r.Contains(out, "nothing to rebase")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "rebase", inc2Backup)

	// ancestors are not needed anymore
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "remote", inc1Backup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", "remote", fullBackup)

	for _, table := range tables {
		env.queryWithNoError(t, r, "DROP TABLE "+dbName+"."+table+" SYNC")
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
