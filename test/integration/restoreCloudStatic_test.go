//go:build integration

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"

	"github.com/stretchr/testify/require"
)

// TestRestoreCloudStaticS3 / TestRestoreCloudStaticGCS / TestRestoreCloudStaticAzblob /
// TestRestoreCloudStaticS3IAMRole / TestRestoreRemoteCloudStaticAutoDetect are the restore-only twins of the
// TestRestoreCloud* tests from restoreCloud_test.go for the case when no real ClickHouse Cloud service is
// available (QA_AWS_CLOUD_ENDPOINT is empty, e.g. the trial is over): instead of creating a backup with
// `BACKUP TABLE ... TO S3/AzureBlobStorage` in the Cloud service, they restore the static backup of the
// SharedMergeTree table default.t1 (Packed storage format, a single data.packed part) exported by
// ClickHouse Cloud once and stored under staticCloudBackupPrefix in the QA bucket / container.
// When QA_AWS_CLOUD_ENDPOINT is set, these tests skip and the dynamic TestRestoreCloud* tests run instead.
//
// Not covered without a Cloud service: `partitions` filter (t1 is not partitioned), a real
// `BACKUP ... ON CLUSTER` layout and an incremental backup on top of a Cloud backup.
//
// To regenerate the static backup (ClickHouse Cloud service required), run in the Cloud service:
//
//	CREATE TABLE default.t1 (id UInt64) ORDER BY id;
//	INSERT INTO default.t1 SELECT number FROM numbers(staticCloudBackupRows);
//	BACKUP TABLE default.t1 TO S3('https://s3.<region>.amazonaws.com/<bucket>/clickhouse_cloud_backup/test_backup', '<key>', '<secret>');
//	BACKUP TABLE default.t1 TO S3('https://storage.googleapis.com/<bucket>/clickhouse_cloud_backup/test_backup', '<hmac key>', '<hmac secret>');
//	BACKUP TABLE default.t1 TO AzureBlobStorage('<connection string>', '<container>', 'clickhouse_cloud_backup/test_backup/');

const (
	staticCloudBackupPrefix = "clickhouse_cloud_backup/test_backup"
	staticCloudBackupTable  = "t1"
	staticCloudBackupRows   = 10000
)

// staticCloudSkip - static tests run only when the dynamic ones can't, i.e. without a real ClickHouse Cloud service
func staticCloudSkip(t *testing.T) {
	if os.Getenv("QA_AWS_CLOUD_ENDPOINT") != "" {
		t.Skip("QA_AWS_CLOUD_ENDPOINT is set, the dynamic TestRestoreCloud* tests run instead of " + t.Name())
	}
	realCloudPackedVersionGate(t)
}

// runRestoreCloudStatic - the shared scenario: restore the static Cloud backup locally via CLI and via
// `POST /backup/restore_cloud`, verify the conversion, --restore-on-cluster on the plain (non `ON CLUSTER`) backup.
// storageType names the config file, configYAML is the in-container config (env vars are
// propagated into the container by commonClickHouseEnv).
func runRestoreCloudStatic(t *testing.T, storageType, configYAML string) {
	staticCloudSkip(t)
	r := require.New(t)
	table, prefix := staticCloudBackupTable, staticCloudBackupPrefix

	env, _ := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	defer env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))

	configName := "config-cloud-static-" + storageType + ".yml"
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "cat > /etc/clickhouse-backup/"+configName+" <<EOF\n"+configYAML+"\nEOF")

	// CLI restore
	env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configName, "restore_cloud", prefix)
	checkCloudRestored(env, r, table, staticCloudBackupRows)

	// re-run into the non-empty table fails with code 608, --drop recreates it
	nonEmptyOut, nonEmptyErr := env.DockerExecOut("clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configName, "restore_cloud", prefix)
	r.Error(nonEmptyErr, "restore_cloud into non-empty table shall fail: %s", nonEmptyOut)
	r.Contains(nonEmptyOut, "already contains some data")
	env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configName, "restore_cloud", "--drop", "--parallel=2", prefix)
	checkCloudRestored(env, r, table, staticCloudBackupRows)

	// the same restore via POST /backup/restore_cloud
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	serverLog := "/tmp/clickhouse-backup-server-cloud-static-" + storageType + ".log"
	env.DockerExecBackgroundNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+configName+" server &>>"+serverLog)
	defer func() {
		r.NoError(env.DockerExec("clickhouse", "pkill", "-n", "-f", "clickhouse-backup"))
	}()
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "for i in $(seq 1 30); do wget -q -O - http://localhost:7171/backup/status >/dev/null 2>&1 && exit 0; sleep 1; done; echo 'API server did not start'; cat "+serverLog+"; exit 1")
	apiOut, err := env.DockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("wget -q -O - --post-data='' 'http://localhost:7171/backup/restore_cloud?prefix=%s'", prefix))
	r.NoError(err, "POST /backup/restore_cloud output: %s", apiOut)
	r.Contains(apiOut, "acknowledged")
	// restore_cloud runs async, wait until the command leaves in-progress state
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "for i in $(seq 1 60); do wget -q -O - http://localhost:7171/backup/status | grep -q 'in progress' || exit 0; sleep 1; done; echo 'restore_cloud is still in progress'; exit 1")
	statusOut, err := env.DockerExecOut("clickhouse", "bash", "-ce", "wget -q -O - http://localhost:7171/backup/status")
	r.NoError(err)
	r.Contains(statusOut, `"status":"success"`, "unexpected /backup/status: %s", statusOut)
	r.Contains(statusOut, "restore_cloud", "unexpected /backup/status: %s", statusOut)
	checkCloudRestored(env, r, table, staticCloudBackupRows)

	// the static backup is a plain (non `ON CLUSTER`) one: --restore-on-cluster with the {cluster} macro
	// resolves to the 1 shard x 1 replica test cluster and adds ON CLUSTER to CREATE and RESTORE
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configName, "restore_cloud", "--restore-on-cluster={cluster}", prefix)
	checkCloudRestored(env, r, table, staticCloudBackupRows)
	// the topology pre-check rejects an unknown cluster
	out, err := env.DockerExecOut("clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configName, "restore_cloud", "--restore-on-cluster=no_such_cluster", prefix)
	r.Error(err, "restore_cloud with unknown cluster must fail: %s", out)
	r.Contains(out, "not found in system.clusters")
}

func TestRestoreCloudStaticS3(t *testing.T) {
	if os.Getenv("QA_AWS_CLOUD_BUCKET") == "" || os.Getenv("QA_AWS_CLOUD_ACCESS_KEY") == "" {
		t.Skip("QA_AWS_CLOUD_BUCKET or QA_AWS_CLOUD_ACCESS_KEY is empty, TestRestoreCloudStaticS3 will skip")
	}
	runRestoreCloudStatic(t, "s3", cloudTestClickHouseYAML+`
s3:
  access_key: ${QA_AWS_CLOUD_ACCESS_KEY}
  secret_key: ${QA_AWS_CLOUD_SECRET_KEY}
  bucket: ${QA_AWS_CLOUD_BUCKET}
  region: ${QA_AWS_CLOUD_REGION:-us-west-2}`)
}

// TestRestoreCloudStaticS3IAMRole - the local restore runs with s3->assume_role_arn so both the manifest
// reads and `RESTORE ... FROM S3(url, key, secret, extra_credentials(role_arn='...'))` access the
// bucket with the role's permissions, the QA keys only sign the STS AssumeRole call.
func TestRestoreCloudStaticS3IAMRole(t *testing.T) {
	if os.Getenv("QA_AWS_CLOUD_BUCKET") == "" || os.Getenv("QA_AWS_CLOUD_ROLE_ARN") == "" || os.Getenv("QA_AWS_CLOUD_ACCESS_KEY") == "" {
		t.Skip("QA_AWS_CLOUD_BUCKET, QA_AWS_CLOUD_ROLE_ARN or QA_AWS_CLOUD_ACCESS_KEY is empty, TestRestoreCloudStaticS3IAMRole will skip")
	}
	staticCloudSkip(t)
	r := require.New(t)
	roleARN := os.Getenv("QA_AWS_CLOUD_ROLE_ARN")
	table, prefix := staticCloudBackupTable, staticCloudBackupPrefix

	env, _ := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	defer env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))

	configYAML := cloudTestClickHouseYAML + `
s3:
  access_key: ${QA_AWS_CLOUD_ACCESS_KEY}
  secret_key: ${QA_AWS_CLOUD_SECRET_KEY}
  assume_role_arn: ${QA_AWS_CLOUD_ROLE_ARN}
  bucket: ${QA_AWS_CLOUD_BUCKET}
  region: ${QA_AWS_CLOUD_REGION:-us-west-2}`
	configName := "config-cloud-static-s3-iam-role.yml"
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "cat > /etc/clickhouse-backup/"+configName+" <<EOF\n"+configYAML+"\nEOF")

	out, err := env.DockerExecOut("clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configName, "restore_cloud", prefix)
	r.NoError(err, "restore_cloud with assume_role_arn output: %s", out)
	// the executed RESTORE statement must carry the role
	r.Contains(out, fmt.Sprintf("extra_credentials(role_arn = '%s')", roleARN), "RESTORE must use the IAM role: %s", out)
	checkCloudRestored(env, r, table, staticCloudBackupRows)
}

func TestRestoreCloudStaticGCS(t *testing.T) {
	if os.Getenv("QA_GCS_OVER_S3_ACCESS_KEY") == "" {
		t.Skip("QA_GCS_OVER_S3_ACCESS_KEY is empty, TestRestoreCloudStaticGCS will skip")
	}
	runRestoreCloudStatic(t, "gcs", cloudTestClickHouseYAML+`
s3:
  access_key: ${QA_GCS_OVER_S3_ACCESS_KEY}
  secret_key: ${QA_GCS_OVER_S3_SECRET_KEY}
  bucket: ${QA_GCS_OVER_S3_BUCKET}
  endpoint: https://storage.googleapis.com
  force_path_style: true`)
}

func TestRestoreCloudStaticAzblob(t *testing.T) {
	if os.Getenv("QA_AZBLOB_ACCOUNT_KEY") == "" {
		t.Skip("QA_AZBLOB_ACCOUNT_KEY is empty, TestRestoreCloudStaticAzblob will skip")
	}
	runRestoreCloudStatic(t, "azblob", `general:
  remote_storage: azblob
`+cloudTestClickHouseYAML+`
azblob:
  account_name: ${QA_AZBLOB_ACCOUNT_NAME}
  account_key: ${QA_AZBLOB_ACCOUNT_KEY}
  container: ${QA_AZBLOB_CONTAINER}
  endpoint_suffix: core.windows.net
  endpoint_schema: https
  assume_container_exists: true`)
}

// TestRestoreRemoteCloudStaticAutoDetect - the static Cloud backup located in s3->path next to a regular
// backup is detected by layout (`.backup` without metadata.json),
// https://github.com/Altinity/clickhouse-backup/issues/1574:
// `list remote` shows it as `cloud` instead of broken, `clean_remote_broken` and backups_to_keep_remote retention keep it,
// `download` refuses it, `restore_remote` (CLI and POST /backup/restore_remote) switches to restore_cloud.
// The static backup is server-side copied into a per-run s3->path, because the regular backup upload with
// backups_to_keep_remote retention must not touch backups of parallel CI jobs which share the bucket.
func TestRestoreRemoteCloudStaticAutoDetect(t *testing.T) {
	if os.Getenv("QA_AWS_CLOUD_BUCKET") == "" || os.Getenv("QA_AWS_CLOUD_ACCESS_KEY") == "" {
		t.Skip("QA_AWS_CLOUD_BUCKET or QA_AWS_CLOUD_ACCESS_KEY is empty, TestRestoreRemoteCloudStaticAutoDetect will skip")
	}
	staticCloudSkip(t)
	r := require.New(t)
	bucket, region := os.Getenv("QA_AWS_CLOUD_BUCKET"), getEnvDefault("QA_AWS_CLOUD_REGION", "us-west-2")
	accessKey, secretKey := os.Getenv("QA_AWS_CLOUD_ACCESS_KEY"), os.Getenv("QA_AWS_CLOUD_SECRET_KEY")
	id := rand.Int31()
	table := staticCloudBackupTable
	regularTable := fmt.Sprintf("test_restore_remote_regular_%d", id)
	cloudBackupName := fmt.Sprintf("cloud_backup_%d", id)
	regularBackupName := fmt.Sprintf("regular_backup_%d", id)
	// GITHUB_RUN_ID isolates parallel CI jobs which share the bucket
	remotePath := fmt.Sprintf("restore_cloud_e2e/static_autodetect_%s_%d", os.Getenv("GITHUB_RUN_ID"), id)

	s3Client := &storage.S3{Config: &config.S3Config{
		AccessKey: accessKey, SecretKey: secretKey, Bucket: bucket, Region: region,
	}, Concurrency: 1}
	ctx := context.Background()
	r.NoError(s3Client.Connect(ctx))
	defer deleteCloudBackup(r, s3Client, remotePath)
	r.NoError(s3Client.WalkAbsolute(ctx, staticCloudBackupPrefix, true, func(ctx context.Context, f storage.RemoteFile) error {
		_, err := s3Client.CopyObject(ctx, f.Size(), bucket, path.Join(staticCloudBackupPrefix, f.Name()), path.Join(remotePath, cloudBackupName, f.Name()))
		return err
	}))

	env, _ := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	defer env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE default.%s (id UInt64) ENGINE=MergeTree ORDER BY id", regularTable))
	defer env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", regularTable))
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO default.%s SELECT number FROM numbers(100)", regularTable))

	configYAML := `general:
  remote_storage: s3
  backups_to_keep_remote: 1
` + cloudTestClickHouseYAML + `
s3:
  access_key: ${QA_AWS_CLOUD_ACCESS_KEY}
  secret_key: ${QA_AWS_CLOUD_SECRET_KEY}
  bucket: ${QA_AWS_CLOUD_BUCKET}
  region: ${QA_AWS_CLOUD_REGION:-us-west-2}
  path: ` + remotePath
	configFile := "/etc/clickhouse-backup/config-cloud-static-autodetect.yml"
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "cat > "+configFile+" <<EOF\n"+configYAML+"\nEOF")
	chb := func(args ...string) (string, error) {
		return env.DockerExecOut("clickhouse", append([]string{"clickhouse-backup", "-c", configFile}, args...)...)
	}
	checkListRemote := func() {
		out, err := chb("list", "remote")
		r.NoError(err, "list remote output: %s", out)
		cloudLine := ""
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, cloudBackupName+" ") {
				cloudLine = line
			}
		}
		r.NotEmpty(cloudLine, "cloud backup is not listed: %s", out)
		r.Contains(cloudLine, "cloud", "unexpected list remote line: %s", cloudLine)
		r.NotContains(cloudLine, "broken", "unexpected list remote line: %s", cloudLine)
		r.NotContains(cloudLine, "???", "cloud backup size is unknown: %s", cloudLine)
		r.Contains(out, regularBackupName, "regular backup is not listed: %s", out)
	}

	// retention with backups_to_keep_remote=1 after the upload shall not count and delete the older cloud backup
	out, err := chb("create_remote", "--tables=default."+regularTable, regularBackupName)
	r.NoError(err, "create_remote output: %s", out)
	defer func() {
		_, _ = chb("delete", "local", regularBackupName)
	}()
	checkListRemote()

	// the cloud backup is not broken, clean_remote_broken keeps it
	out, err = chb("clean_remote_broken")
	r.NoError(err, "clean_remote_broken output: %s", out)
	checkListRemote()

	out, err = chb("download", cloudBackupName)
	r.Error(err, "download of the cloud backup shall fail: %s", out)
	r.Contains(out, "can't be downloaded")

	out, err = chb("restore_remote", "--restore-table-mapping="+table+":"+table+"_copy", cloudBackupName)
	r.Error(err, "restore_remote with table mapping of the cloud backup shall fail: %s", out)
	r.Contains(out, "doesn't support --restore-table-mapping")

	out, err = chb("restore_remote", cloudBackupName)
	r.NoError(err, "restore_remote output: %s", out)
	r.Contains(out, "switching to restore_cloud")
	checkCloudRestored(env, r, table, staticCloudBackupRows)

	// --rm recreates the table
	out, err = chb("restore_remote", "--rm", cloudBackupName)
	r.NoError(err, "restore_remote --rm output: %s", out)
	checkCloudRestored(env, r, table, staticCloudBackupRows)

	// the same restore via POST /backup/restore_remote
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	serverLog := "/tmp/clickhouse-backup-server-cloud-static-autodetect.log"
	env.DockerExecBackgroundNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup -c "+configFile+" server &>>"+serverLog)
	defer func() {
		r.NoError(env.DockerExec("clickhouse", "pkill", "-n", "-f", "clickhouse-backup"))
	}()
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "for i in $(seq 1 30); do wget -q -O - http://localhost:7171/backup/status >/dev/null 2>&1 && exit 0; sleep 1; done; echo 'API server did not start'; cat "+serverLog+"; exit 1")
	// the server lists remote backups for metrics at startup, POST returns 423 until it finishes
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "for i in $(seq 1 60); do wget -q -O - http://localhost:7171/backup/status | grep -q 'in progress' || exit 0; sleep 1; done; echo 'startup operations are still in progress'; exit 1")
	apiOut, err := env.DockerExecOut("clickhouse", "bash", "-ce", "wget -q -O - --content-on-error --post-data='' http://localhost:7171/backup/restore_remote/"+cloudBackupName+" || (cat "+serverLog+"; exit 1)")
	r.NoError(err, "POST /backup/restore_remote output: %s", apiOut)
	r.Contains(apiOut, "acknowledged")
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "for i in $(seq 1 60); do wget -q -O - http://localhost:7171/backup/status | grep -q 'in progress' || exit 0; sleep 1; done; echo 'restore_remote is still in progress'; exit 1")
	statusOut, err := env.DockerExecOut("clickhouse", "bash", "-ce", "wget -q -O - http://localhost:7171/backup/status")
	r.NoError(err)
	r.Contains(statusOut, `"status":"success"`, "unexpected /backup/status: %s", statusOut)
	checkCloudRestored(env, r, table, staticCloudBackupRows)
}
