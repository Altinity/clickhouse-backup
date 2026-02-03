//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNamedCollections(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.12") < 0 {
		t.Skipf("Named collections not supported in version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.7") < 0 {
		t.Skipf("DROP/CREATE NAMED COLLECTIONS .. ON CLUSTER doesn't work for version less 23.7, look https://github.com/ClickHouse/ClickHouse/issues/51609")
	}
	env, r := NewTestEnvironment(t)
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
			env.queryWithNoError(r, "CREATE NAMED COLLECTION test_named_collection AS access_key_id = 'access_key', secret_access_key = 'it_is_my_super_secret_key', format = 'CSV', url = 'https://minio:9000/clickhouse/test_named_collection.csv'")
			env.queryWithNoError(r, "CREATE DATABASE test_named_collection")
			env.queryWithNoError(r, "CREATE TABLE test_named_collection.test_named_collection (id UInt64) ENGINE=S3(test_named_collection)")
			env.queryWithNoError(r, "INSERT INTO test_named_collection.test_named_collection SELECT number FROM numbers(10) SETTINGS s3_truncate_on_insert=1")

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
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupArg)

			// cleanup before restore
			env.queryWithNoError(r, "DROP NAMED COLLECTION IF EXISTS test_named_collection")
			r.NoError(env.dropDatabase("test_named_collection", false))

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
					env.queryWithNoError(r, "DROP NAMED COLLECTION test_named_collection")
				}
			}

			// cleanup
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupArg)
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", backupArg)
			r.NoError(env.dropDatabase("test_named_collection", true))
		})
	}
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/test_named_collection.csv")
	env.checkObjectStorageIsEmpty(t, r, "S3")
	env.Cleanup(t, r)
}
