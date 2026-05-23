//go:build integration

package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestS3SSEC verifies that clickhouse-backup can back up and restore a
// MergeTree table whose data lives on an S3 object_disk encrypted with
// SSE-C (<server_side_encryption_customer_key_base64>).
//
// Regression: https://github.com/Altinity/clickhouse-backup/issues/1374
// Before the fix the streaming upload path failed with HeadObject 400 Bad
// Request because the source S3 client only carried the customer key, not
// the algorithm and key MD5 that SSE-C requires.
//
// The disk_s3_ssec disk and s3_only_ssec policy are pre-installed by
// test/integration/configs/dynamic_settings.sh; MinIO serves SSE-C over the
// stack's TLS endpoint.
func TestS3SSEC(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "21.8") < 0 {
		t.Skipf("Test requires ClickHouse >= 21.8 for stable S3 SSE-C object_disk, current %s", version)
	}

	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	backupName := fmt.Sprintf("test_s3_ssec_%d", rand.Int())
	dbName := "test_s3_ssec"
	tableName := "data"

	env.queryWithNoError(r, "DROP DATABASE IF EXISTS "+dbName+" SYNC")
	env.queryWithNoError(r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(r, fmt.Sprintf(
		"CREATE TABLE %s.%s (id UInt64, payload String) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy='s3_only_ssec'",
		dbName, tableName,
	))
	env.queryWithNoError(r, fmt.Sprintf(
		"INSERT INTO %s.%s SELECT number, repeat('x', 1024) FROM numbers(2000)",
		dbName, tableName,
	))
	env.checkCount(r, 1, 2000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))

	// Before the fix this fails inside object-disk streaming with:
	// srcStorage.StatFileReaderAbsolute(...) HeadObject 400.
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create_remote --tables="+dbName+".* "+backupName)
	log.Debug().Msg(out)
	r.NoError(err, out)
	r.NotContains(out, "HeadObject, https response error", "SSE-C source HeadObject still rejected — the fix did not apply")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c",
		"/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	env.queryWithNoError(r, fmt.Sprintf("DROP TABLE %s.%s SYNC", dbName, tableName))
	env.queryWithNoError(r, "DROP DATABASE "+dbName+" SYNC")

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore_remote "+backupName)
	log.Debug().Msg(out)
	r.NoError(err, out)

	env.checkCount(r, 1, 2000, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName))

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, true, true, true, "config-s3.yml")
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/disk_s3_ssec")
	env.Cleanup(t, r)
}
