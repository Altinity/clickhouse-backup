//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type backupMetadataSizes struct {
	ConfigSize           uint64 `json:"config_size"`
	NamedCollectionsSize uint64 `json:"named_collections_size"`
}

func readBackupMetadataSizes(env *TestEnvironment, r *require.Assertions, container string, cmd ...string) backupMetadataSizes {
	out, err := env.DockerExecOut(container, cmd...)
	r.NoError(err, "%s on %s: %s", strings.Join(cmd, " "), container, out)
	var sizes backupMetadataSizes
	r.NoError(json.Unmarshal([]byte(out), &sizes), "unmarshal metadata.json from %s: %s", container, out)
	return sizes
}

func TestNamedCollections(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.12") < 0 {
		t.Skipf("Named collections not supported in version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.7") < 0 {
		t.Skipf("DROP/CREATE NAMED COLLECTIONS .. ON CLUSTER doesn't work for version less 23.7, look https://github.com/ClickHouse/ClickHouse/issues/51609")
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	backupName := "test_named_collections_backup"

	testCases := []struct {
		name                   string
		createArgs             []string
		namedCollectionsEnvVar string
		expectCollectionExists bool
		remote                 bool
	}{
		// create + upload
		{
			name:                   "create_with_named_collections_flag",
			createArgs:             []string{"--named-collections"},
			expectCollectionExists: true,
		},
		{
			name:                   "create_with_named_collections_only_flag",
			createArgs:             []string{"--named-collections-only"},
			expectCollectionExists: true,
		},
		{
			name:                   "create_with_env_var_true",
			createArgs:             []string{},
			namedCollectionsEnvVar: "true",
			expectCollectionExists: true,
		},
		{
			name:                   "create_with_env_var_false",
			createArgs:             []string{},
			namedCollectionsEnvVar: "false",
			expectCollectionExists: false,
		},
		{
			name:                   "create_default",
			createArgs:             []string{},
			expectCollectionExists: false,
		},
		// create_remote
		{
			name:                   "create_remote_with_named_collections_flag",
			createArgs:             []string{"--named-collections"},
			expectCollectionExists: true,
			remote:                 true,
		},
		{
			name:                   "create_remote_with_named_collections_only_flag",
			createArgs:             []string{"--named-collections-only"},
			expectCollectionExists: true,
			remote:                 true,
		},
		{
			name:                   "create_remote_with_env_var_true",
			createArgs:             []string{},
			namedCollectionsEnvVar: "true",
			expectCollectionExists: true,
			remote:                 true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			backupArg := backupName + "_" + tc.name
			// setup
			env.queryWithNoError(t, r, "CREATE NAMED COLLECTION test_named_collection AS access_key_id = 'access_key', secret_access_key = 'it_is_my_super_secret_key', format = 'CSV', url = 'https://minio:9000/clickhouse/test_named_collection.csv'")
			env.queryWithNoError(t, r, "CREATE DATABASE test_named_collection")
			env.queryWithNoError(t, r, "CREATE TABLE test_named_collection.test_named_collection (id UInt64) ENGINE=S3(test_named_collection)")
			env.queryWithNoError(t, r, "INSERT INTO test_named_collection.test_named_collection SELECT number FROM numbers(10) SETTINGS s3_truncate_on_insert=1")

			envVar := ""
			if tc.namedCollectionsEnvVar != "" {
				envVar = "NAMED_COLLECTIONS_BACKUP_ALWAYS=" + tc.namedCollectionsEnvVar + " "
			}
			backupEnvVar := envVar
			if strings.Contains(tc.name, "only") {
				backupEnvVar += " ALLOW_EMPTY_BACKUPS=1 "
			}

			// create backup
			createCmdArgs := make([]string, len(tc.createArgs))
			copy(createCmdArgs, tc.createArgs)
			createCmdArgs = append(createCmdArgs, backupArg)

			if tc.remote {
				cmd := fmt.Sprintf("%sclickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create_remote %s", backupEnvVar, strings.Join(createCmdArgs, " "))
				env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", cmd)
			} else {
				cmd := fmt.Sprintf("%sclickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create %s", backupEnvVar, strings.Join(createCmdArgs, " "))
				env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", cmd)

				cmd = fmt.Sprintf("%sclickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload %s", backupEnvVar, backupArg)
				env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", cmd)
			}
			// upload used to write the named collections size into config_size instead of named_collections_size,
			// verify the remote metadata.json carries the sizes in the right fields; minio only sees objects
			// that went through its S3 API, so read via `mc cat`, not from the container filesystem
			const mcAliasCmd = "mc alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null 2>&1"
			cfgPath, _ := env.resolveConfigPaths(r, "config-s3.yml")
			remoteMeta := readBackupMetadataSizes(env, r, "minio", "bash", "-c",
				mcAliasCmd+" && mc cat local/clickhouse/"+cfgPath+"/"+backupArg+"/metadata.json")
			r.Zero(remoteMeta.ConfigSize, "configs are not backed up in this test, remote config_size shall stay 0")
			if tc.expectCollectionExists {
				r.Greater(remoteMeta.NamedCollectionsSize, uint64(0), "remote named_collections_size shall contain the uploaded named collections size")
			} else {
				r.Zero(remoteMeta.NamedCollectionsSize, "backup without named collections shall keep remote named_collections_size=0")
			}

			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupArg)

			// cleanup before restore — drop database first because CH 26.3+ forbids
			// DROP NAMED COLLECTION while tables referencing it exist
			r.NoError(env.dropDatabase("test_named_collection", false))
			env.queryWithNoError(t, r, "DROP NAMED COLLECTION IF EXISTS test_named_collection")

			// restore backup
			restoreArgs := []string{"-c", "/etc/clickhouse-backup/config-s3.yml"}
			if tc.remote {
				restoreArgs = append(restoreArgs, "restore_remote")
			} else {
				cmd := fmt.Sprintf("%sclickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download %s", backupEnvVar, backupArg)
				env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", cmd)
				restoreArgs = append(restoreArgs, "restore")
			}

			if strings.Contains(tc.name, "only") {
				restoreArgs = append(restoreArgs, "--named-collections-only")
			} else if tc.expectCollectionExists {
				restoreArgs = append(restoreArgs, "--named-collections")
			}

			restoreArgs = append(restoreArgs, backupArg)
			if !tc.expectCollectionExists && !strings.Contains(tc.name, "only") {
				out, err := env.DockerExecOut("clickhouse-backup", append([]string{"clickhouse-backup"}, restoreArgs...)...)
				r.Error(err)
				r.Contains(out, "NAMED_COLLECTION_DOESNT_EXIST")
			} else {
				if tc.remote {
					cmd := fmt.Sprintf("%sclickhouse-backup %s", backupEnvVar, strings.Join(restoreArgs, " "))
					env.DockerExecNoError(r, "clickhouse-backup", "bash", "-c", cmd)
				} else {
					env.DockerExecNoError(r, "clickhouse-backup", append([]string{"clickhouse-backup"}, restoreArgs...)...)
				}
				// check results
				if tc.expectCollectionExists {
					var expected uint64
					if !strings.Contains(tc.name, "only") {
						r.NoError(env.ch.SelectSingleRowNoCtx(&expected, "SELECT count() FROM test_named_collection.test_named_collection"))
						r.Equal(uint64(10), expected, "expect count=10")
					}
				}
			}

			// download used to drop the named collections size when rewriting the local metadata.json,
			// verify the re-downloaded backup carries it too
			localMeta := readBackupMetadataSizes(env, r, "clickhouse-backup", "cat", "/var/lib/clickhouse/backup/"+backupArg+"/metadata.json")
			r.Zero(localMeta.ConfigSize, "configs are not backed up in this test, local config_size shall stay 0")
			if tc.expectCollectionExists {
				r.Greater(localMeta.NamedCollectionsSize, uint64(0), "local named_collections_size shall contain the downloaded named collections size")
			} else {
				r.Zero(localMeta.NamedCollectionsSize, "backup without named collections shall keep local named_collections_size=0")
			}

			// cleanup — drop database before named collection (CH 26.3+ forbids drop while tables reference it)
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupArg)
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", backupArg)
			r.NoError(env.dropDatabase("test_named_collection", true))
			env.queryWithNoError(t, r, "DROP NAMED COLLECTION IF EXISTS test_named_collection")
		})
	}
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/test_named_collection.csv")
	env.checkObjectStorageIsEmpty(t, r, "S3", "config-s3.yml")
}
