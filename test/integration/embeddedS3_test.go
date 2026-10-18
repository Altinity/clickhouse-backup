//go:build integration

package main

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedS3(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "23.3") < 0 {
		t.Skipf("Test skipped, BACKUP/RESTORE not production ready for %s version, look https://github.com/ClickHouse/ClickHouse/issues/39416 for details", version)
	}
	t.Logf("@TODO RESTORE Ordinary with old syntax still not works for %s version, look https://github.com/ClickHouse/ClickHouse/issues/43971", os.Getenv("CLICKHOUSE_VERSION"))
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)

	// === S3 ===
	// CUSTOM backup creates folder in each disk, need to clear
	env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/var/lib/clickhouse/disks/backups_s3/backup/")
	env.runMainIntegrationScenario(t, "EMBEDDED_S3", "config-s3-embedded.yml")
	// cleanup
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/disk_s3")
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/backups_s3")

	if compareVersion(version, "23.8") >= 0 {
		//CUSTOM backup creates folder in each disk, need to clear
		env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/var/lib/clickhouse/disks/backups_local/backup/")
		env.runMainIntegrationScenario(t, "EMBEDDED_LOCAL", "config-s3-embedded-local.yml")
	}
	if compareVersion(version, "24.3") >= 0 {
		env.runMainIntegrationScenario(t, "EMBEDDED_S3_URL", "config-s3-embedded-url.yml")
	}
	//@TODO think about how to implements embedded backup for s3_plain disks
	//env.DockerExecNoError(r, "clickhouse", "rm", "-rf", "/var/lib/clickhouse/disks/backups_s3_plain/backup/")
	//runMainIntegrationScenario(t, "EMBEDDED_S3_PLAIN", "config-s3-plain-embedded.yml")
}

func TestEmbeddedS3Cleanup(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") < 0 {
		t.Skip("native backup cleanup requires ClickHouse 23.3 or newer")
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0, time.Second, time.Minute)

	dbName := "test_embedded_cleanup"
	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	defer func() { r.NoError(env.dropDatabase(dbName, true)) }()
	// Sparse serialization makes ClickHouse write a real serialization.json.
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t1 (id UInt64, value UInt64) ENGINE=MergeTree ORDER BY id "+
		"SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0, ratio_of_defaults_for_sparse_serialization=0.9")
	env.queryWithNoError(t, r, "INSERT INTO "+dbName+".t1 SELECT number, 0 FROM numbers(1000)")
	configFile := "/etc/clickhouse-backup/config-s3-embedded.yml"
	// backups_s3 endpoint is https://minio:9000/clickhouse/backups_s3/{cluster}/{shard}/
	var objectPrefix string
	r.NoError(env.ch.SelectSingleRowNoCtx(&objectPrefix, "SELECT concat('backups_s3/', getMacro('cluster'), '/', getMacro('shard'), '/')"))

	for _, first := range []string{"local", "remote"} {
		t.Run(first+"_first", func(t *testing.T) {
			r := require.New(t)
			backupName := "embedded_cleanup_" + first
			defer fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, nil, false, false, false, "config-s3-embedded.yml")
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "create", "--tables="+dbName+".t1", backupName)

			// Check the objects named by this backup. ClickHouse 23.3 ON CLUSTER
			// can leave unreferenced objects while creating a backup, independently
			// of deletion: https://github.com/ClickHouse/ClickHouse/pull/51299.
			dataDir := t.TempDir()
			r.NoError(env.DockerCP("clickhouse-backup:/var/lib/clickhouse/disks/backups_s3/"+backupName+"/shards/1/replicas/1/data/.", dataDir))
			objects := make(map[string]string)
			jsonObjects := 0
			r.NoError(filepath.WalkDir(dataDir, func(file string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				meta, err := object_disk.ReadMetadataFromFile(file)
				if err != nil {
					return err
				}
				for _, object := range meta.StorageObjects {
					objects[object.ObjectPath] = file
					if entry.Name() == "serialization.json" {
						jsonObjects++
					}
				}
				return nil
			}))
			r.NotEmpty(objects)
			r.Positive(jsonObjects, "the backup must contain a native JSON object")

			listObjects := func() map[string]bool {
				out, err := env.DockerExecOut("minio", "bash", "-ce",
					"mc --insecure alias set local https://localhost:9000 access_key it_is_my_super_secret_key >/dev/null && "+
						"mc --insecure ls --recursive --json local/clickhouse/"+objectPrefix)
				r.NoError(err, "list native objects: %s", out)
				keys := make(map[string]bool)
				for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
					if line == "" {
						continue
					}
					var object struct {
						Key string `json:"key"`
					}
					r.NoError(json.Unmarshal([]byte(line), &object))
					keys[object.Key] = true
				}
				return keys
			}
			before := listObjects()
			for key, file := range objects {
				r.True(before[key], "native object %s referenced by %s must exist", key, file)
			}
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "upload", backupName)
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", first, backupName)
			afterFirst := listObjects()
			for key := range objects {
				r.True(afterFirst[key], "the remaining backup still needs native object %s", key)
			}
			last := "remote"
			if first == "remote" {
				last = "local"
			}
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", configFile, "delete", last, backupName)
			afterLast := listObjects()
			for key, file := range objects {
				r.False(afterLast[key], "native object %s referenced by %s remains after deletion", key, file)
			}
		})
	}
}
