//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"

	"github.com/stretchr/testify/require"
)

// TestRestoreCloudS3 / TestRestoreCloudGCS / TestRestoreCloudAzblob verify `restore_cloud`
// (https://github.com/Altinity/clickhouse-backup/issues/1508) against REAL ClickHouse Cloud backups:
// each test creates a SharedMergeTree table with 10000 rows in a real ClickHouse Cloud service
// (QA_AWS_CLOUD_ENDPOINT), exports it there via native `BACKUP TABLE ... TO S3/AzureBlobStorage`,
// then restores it on the local test ClickHouse via CLI and via `POST /backup/restore_cloud`
// (full and with a `partitions` filter) and checks the SharedMergeTree -> ReplicatedMergeTree
// conversion and the row count.
//
// The conversion is performed on the fly: restore_cloud only reads the backup (manifest + metadata
// blobs), the bucket/container content is never modified.

const cloudTestClickHouseYAML = `clickhouse:
  host: clickhouse
  port: 9440
  username: backup
  password: "meow=& 123?*%# МЯУ"
  secure: true
  skip_verify: true
  timeout: 5m
api:
  listen: :7171`

// realCloudPackedVersionGate - ClickHouse Cloud backups store small parts in the Packed storage
// format (a single data.packed archive), the loader is available in OSS since 26.8,
// https://github.com/ClickHouse/ClickHouse/pull/108118
func realCloudPackedVersionGate(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if version != "head" && compareVersion(version, "26.8") < 0 {
		t.Skipf("ClickHouse Cloud backup contains Packed parts (data.packed), require 26.8+, version %s too old", version)
	}
}

// cloudQuery executes one SQL statement on the real ClickHouse Cloud service over HTTPS,
// QA_AWS_CLOUD_ENDPOINT format is https://<user>:<password>@<host>:8443
func cloudQuery(r *require.Assertions, query string, secrets ...string) string {
	endpoint, err := url.Parse(os.Getenv("QA_AWS_CLOUD_ENDPOINT"))
	r.NoError(err, "can't parse QA_AWS_CLOUD_ENDPOINT")
	user := endpoint.User.Username()
	password, _ := endpoint.User.Password()
	endpoint.User = nil
	req, err := http.NewRequest(http.MethodPost, endpoint.String(), strings.NewReader(query))
	r.NoError(err)
	req.SetBasicAuth(user, password)
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	r.NoError(err, "cloud query failed")
	body, err := io.ReadAll(resp.Body)
	r.NoError(resp.Body.Close())
	r.NoError(err)
	redacted := string(body)
	for _, secret := range secrets {
		redacted = strings.ReplaceAll(redacted, secret, "***")
	}
	r.Equal(http.StatusOK, resp.StatusCode, "cloud query HTTP %d: %s", resp.StatusCode, redacted)
	return string(body)
}

// cloudBackupCleaner is satisfied by storage.S3 and storage.AzureBlob, their clients handle
// provider quirks (e.g. GCS SigV4 recalculation) which raw aws-sdk clients do not
type cloudBackupCleaner interface {
	Connect(ctx context.Context) error
	WalkAbsolute(ctx context.Context, prefix string, recursive bool, process func(context.Context, storage.RemoteFile) error) error
	DeleteFile(ctx context.Context, key string) error
}

// deleteCloudBackup removes the backup objects, object-by-object because the GCS XML API does
// not support the S3 batch-delete operation
func deleteCloudBackup(r *require.Assertions, client cloudBackupCleaner, prefix string) {
	ctx := context.Background()
	r.NoError(client.Connect(ctx))
	r.NoError(client.WalkAbsolute(ctx, prefix, true, func(ctx context.Context, f storage.RemoteFile) error {
		return client.DeleteFile(ctx, path.Join(prefix, f.Name()))
	}))
}

// checkCloudRestored - the Cloud DDL is `CREATE TABLE default.<table> UUID '...' (id UInt64)
// ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') PARTITION BY id % 4 ORDER BY id`
func checkCloudRestored(env *TestEnvironment, r *require.Assertions, table string, expectedRows uint64) {
	engines := make([]struct {
		Engine string `ch:"engine"`
	}, 0)
	r.NoError(env.ch.Select(&engines, "SELECT engine FROM system.tables WHERE database='default' AND name=?", table))
	r.Len(engines, 1)
	r.Equal("ReplicatedMergeTree", engines[0].Engine)
	env.checkCount(r, 1, expectedRows, fmt.Sprintf("SELECT count() FROM default.%s", table))
}

// runRestoreCloud - the shared scenario: create a fresh SharedMergeTree table with 10000 rows in
// the Cloud service, export it there via native BACKUP into backupDestinationSQL(prefix), restore
// it locally via CLI and via `POST /backup/restore_cloud` (full, then partitions 0,1 only),
// verify the conversion, clean everything up.
// storageType names the test objects, configYAML is the in-container config (env vars are
// propagated into the container by commonClickHouseEnv), cleaner deletes the backup objects.
func runRestoreCloud(t *testing.T, storageType string, backupDestinationSQL func(prefix string) (string, []string), configYAML string, cleaner cloudBackupCleaner) {
	realCloudPackedVersionGate(t)
	r := require.New(t)
	id := rand.Int31()
	table := fmt.Sprintf("test_restore_cloud_%s_%d", storageType, id)
	// GITHUB_RUN_ID isolates parallel CI jobs which share the bucket/container
	prefix := fmt.Sprintf("restore_cloud_e2e/%s_%s_%d", storageType, os.Getenv("GITHUB_RUN_ID"), id)
	destinationSQL, secrets := backupDestinationSQL(prefix)

	cloudQuery(r, fmt.Sprintf("CREATE TABLE default.%s (id UInt64) PARTITION BY id %% 4 ORDER BY id", table))
	defer cloudQuery(r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s", table))
	cloudQuery(r, fmt.Sprintf("INSERT INTO default.%s SELECT number FROM numbers(10000)", table))
	cloudQuery(r, fmt.Sprintf("BACKUP TABLE default.%s TO %s", table, destinationSQL), secrets...)
	defer deleteCloudBackup(r, cleaner, prefix)

	env, _ := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	defer env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))

	configName := "config-cloud-" + storageType + ".yml"
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "cat > /etc/clickhouse-backup/"+configName+" <<EOF\n"+configYAML+"\nEOF")

	// CLI restore
	env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configName, "restore_cloud", prefix)
	checkCloudRestored(env, r, table, 10000)

	// the same restore via POST /backup/restore_cloud
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	serverLog := "/tmp/clickhouse-backup-server-cloud-" + storageType + ".log"
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
	checkCloudRestored(env, r, table, 10000)

	// partial restore of partitions 0 and 1 (of id % 4) via the `partitions` REST parameter
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	apiOut, err = env.DockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("wget -q -O - --post-data='' 'http://localhost:7171/backup/restore_cloud?prefix=%s&partitions=0,1'", prefix))
	r.NoError(err, "POST /backup/restore_cloud with partitions output: %s", apiOut)
	r.Contains(apiOut, "acknowledged")
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "for i in $(seq 1 60); do wget -q -O - http://localhost:7171/backup/status | grep -q 'in progress' || exit 0; sleep 1; done; echo 'restore_cloud is still in progress'; exit 1")
	statusOut, err = env.DockerExecOut("clickhouse", "bash", "-ce", "wget -q -O - http://localhost:7171/backup/status")
	r.NoError(err)
	r.Contains(statusOut, `--partitions=\"0,1\"`, "unexpected /backup/status: %s", statusOut)
	r.NotContains(statusOut, `"status":"error"`, "unexpected /backup/status: %s", statusOut)
	checkCloudRestored(env, r, table, 5000)

	// a `BACKUP ... ON CLUSTER` backup (shards/<N>/replicas/<M>/ layout) restored with
	// --restore-on-cluster, the {cluster} macro resolves to the 1 shard x 1 replica test cluster
	prefixOnCluster := prefix + "_on_cluster"
	destinationOnClusterSQL, _ := backupDestinationSQL(prefixOnCluster)
	cloudQuery(r, fmt.Sprintf("BACKUP TABLE default.%s ON CLUSTER 'default' TO %s", table, destinationOnClusterSQL), secrets...)
	defer deleteCloudBackup(r, cleaner, prefixOnCluster)
	env.queryWithNoError(t, r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s SYNC", table))
	env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configName, "restore_cloud", "--restore-on-cluster={cluster}", prefixOnCluster)
	checkCloudRestored(env, r, table, 10000)
	// the topology pre-check rejects an unknown cluster
	out, err := env.DockerExecOut("clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configName, "restore_cloud", "--restore-on-cluster=no_such_cluster", prefixOnCluster)
	r.Error(err, "restore_cloud with unknown cluster must fail: %s", out)
	r.Contains(out, "not found in system.clusters")
}

func TestRestoreCloudS3(t *testing.T) {
	if os.Getenv("QA_AWS_CLOUD_ENDPOINT") == "" || os.Getenv("QA_AWS_CLOUD_BUCKET") == "" || os.Getenv("QA_AWS_CLOUD_ACCESS_KEY") == "" {
		t.Skip("QA_AWS_CLOUD_ENDPOINT, QA_AWS_CLOUD_BUCKET or QA_AWS_CLOUD_ACCESS_KEY is empty, TestRestoreCloudS3 will skip")
	}
	bucket, region := os.Getenv("QA_AWS_CLOUD_BUCKET"), getEnvDefault("QA_AWS_CLOUD_REGION", "us-west-2")
	accessKey, secretKey := os.Getenv("QA_AWS_CLOUD_ACCESS_KEY"), os.Getenv("QA_AWS_CLOUD_SECRET_KEY")
	runRestoreCloud(t, "s3",
		func(prefix string) (string, []string) {
			return fmt.Sprintf("S3('https://s3.%s.amazonaws.com/%s/%s','%s','%s')", region, bucket, prefix, accessKey, secretKey), []string{accessKey, secretKey}
		},
		cloudTestClickHouseYAML+`
s3:
  access_key: ${QA_AWS_CLOUD_ACCESS_KEY}
  secret_key: ${QA_AWS_CLOUD_SECRET_KEY}
  bucket: ${QA_AWS_CLOUD_BUCKET}
  region: ${QA_AWS_CLOUD_REGION:-us-west-2}`,
		&storage.S3{Config: &config.S3Config{
			AccessKey: accessKey, SecretKey: secretKey, Bucket: bucket, Region: region,
		}, Concurrency: 1})
}

// TestRestoreCloudS3IAMRole - the whole flow runs through the AWS IAM role
// (aws_iam_clickhouse_cloud.sh, trust policy = Cloud service role + the QA_AWS_CLOUD_ACCESS_KEY user):
// ClickHouse Cloud writes the backup via `BACKUP ... TO S3(url, extra_credentials(role_arn='...'))`
// without static keys, and the local restore runs with s3->assume_role_arn so both the manifest
// reads and `RESTORE ... FROM S3(url, key, secret, extra_credentials(role_arn='...'))` access the
// bucket with the role's permissions, the QA keys only sign the STS AssumeRole call.
func TestRestoreCloudS3IAMRole(t *testing.T) {
	if os.Getenv("QA_AWS_CLOUD_ENDPOINT") == "" || os.Getenv("QA_AWS_CLOUD_BUCKET") == "" || os.Getenv("QA_AWS_CLOUD_ROLE_ARN") == "" || os.Getenv("QA_AWS_CLOUD_ACCESS_KEY") == "" {
		t.Skip("QA_AWS_CLOUD_ENDPOINT, QA_AWS_CLOUD_BUCKET, QA_AWS_CLOUD_ROLE_ARN or QA_AWS_CLOUD_ACCESS_KEY is empty, TestRestoreCloudS3IAMRole will skip")
	}
	realCloudPackedVersionGate(t)
	r := require.New(t)
	bucket, region := os.Getenv("QA_AWS_CLOUD_BUCKET"), getEnvDefault("QA_AWS_CLOUD_REGION", "us-west-2")
	accessKey, secretKey := os.Getenv("QA_AWS_CLOUD_ACCESS_KEY"), os.Getenv("QA_AWS_CLOUD_SECRET_KEY")
	roleARN := os.Getenv("QA_AWS_CLOUD_ROLE_ARN")
	id := rand.Int31()
	table := fmt.Sprintf("test_restore_cloud_s3_iam_role_%d", id)
	// GITHUB_RUN_ID isolates parallel CI jobs which share the bucket
	prefix := fmt.Sprintf("restore_cloud_e2e/s3_iam_role_%s_%d", os.Getenv("GITHUB_RUN_ID"), id)

	cloudQuery(r, fmt.Sprintf("CREATE TABLE default.%s (id UInt64) PARTITION BY id %% 4 ORDER BY id", table))
	defer cloudQuery(r, fmt.Sprintf("DROP TABLE IF EXISTS default.%s", table))
	cloudQuery(r, fmt.Sprintf("INSERT INTO default.%s SELECT number FROM numbers(10000)", table))
	// no static keys: the Cloud service role assumes the IAM role via its trust policy
	cloudQuery(r, fmt.Sprintf(
		"BACKUP TABLE default.%s TO S3('https://s3.%s.amazonaws.com/%s/%s', extra_credentials(role_arn = '%s'))",
		table, region, bucket, prefix, roleARN,
	))
	defer deleteCloudBackup(r, &storage.S3{Config: &config.S3Config{
		AccessKey: accessKey, SecretKey: secretKey, Bucket: bucket, Region: region,
	}, Concurrency: 1}, prefix)

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
	configName := "config-cloud-s3-iam-role.yml"
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "cat > /etc/clickhouse-backup/"+configName+" <<EOF\n"+configYAML+"\nEOF")

	out, err := env.DockerExecOut("clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configName, "restore_cloud", prefix)
	r.NoError(err, "restore_cloud with assume_role_arn output: %s", out)
	// the executed RESTORE statement must carry the role
	r.Contains(out, fmt.Sprintf("extra_credentials(role_arn = '%s')", roleARN), "RESTORE must use the IAM role: %s", out)
	checkCloudRestored(env, r, table, 10000)
}

func TestRestoreCloudGCS(t *testing.T) {
	if os.Getenv("QA_AWS_CLOUD_ENDPOINT") == "" || os.Getenv("QA_GCS_OVER_S3_ACCESS_KEY") == "" {
		t.Skip("QA_AWS_CLOUD_ENDPOINT or QA_GCS_OVER_S3_ACCESS_KEY is empty, TestRestoreCloudGCS will skip")
	}
	bucket := os.Getenv("QA_GCS_OVER_S3_BUCKET")
	accessKey, secretKey := os.Getenv("QA_GCS_OVER_S3_ACCESS_KEY"), os.Getenv("QA_GCS_OVER_S3_SECRET_KEY")
	runRestoreCloud(t, "gcs",
		func(prefix string) (string, []string) {
			return fmt.Sprintf("S3('https://storage.googleapis.com/%s/%s','%s','%s')", bucket, prefix, accessKey, secretKey), []string{accessKey, secretKey}
		},
		cloudTestClickHouseYAML+`
s3:
  access_key: ${QA_GCS_OVER_S3_ACCESS_KEY}
  secret_key: ${QA_GCS_OVER_S3_SECRET_KEY}
  bucket: ${QA_GCS_OVER_S3_BUCKET}
  endpoint: https://storage.googleapis.com
  force_path_style: true`,
		&storage.S3{Config: &config.S3Config{
			AccessKey: accessKey, SecretKey: secretKey, Bucket: bucket, Region: "us-east1",
			Endpoint: "https://storage.googleapis.com", ForcePathStyle: true,
		}, Concurrency: 1})
}

func TestRestoreCloudAzblob(t *testing.T) {
	if os.Getenv("QA_AWS_CLOUD_ENDPOINT") == "" || os.Getenv("QA_AZBLOB_ACCOUNT_KEY") == "" {
		t.Skip("QA_AWS_CLOUD_ENDPOINT or QA_AZBLOB_ACCOUNT_KEY is empty, TestRestoreCloudAzblob will skip")
	}
	accountName, accountKey := os.Getenv("QA_AZBLOB_ACCOUNT_NAME"), os.Getenv("QA_AZBLOB_ACCOUNT_KEY")
	container := os.Getenv("QA_AZBLOB_CONTAINER")
	runRestoreCloud(t, "azblob",
		func(prefix string) (string, []string) {
			connectionString := fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;BlobEndpoint=https://%s.blob.core.windows.net;", accountName, accountKey, accountName)
			return fmt.Sprintf("AzureBlobStorage('%s','%s','%s/')", connectionString, container, prefix), []string{accountKey}
		},
		`general:
  remote_storage: azblob
`+cloudTestClickHouseYAML+`
azblob:
  account_name: ${QA_AZBLOB_ACCOUNT_NAME}
  account_key: ${QA_AZBLOB_ACCOUNT_KEY}
  container: ${QA_AZBLOB_CONTAINER}
  endpoint_suffix: core.windows.net
  endpoint_schema: https
  assume_container_exists: true`,
		&storage.AzureBlob{Config: &config.AzureBlobConfig{
			AccountName:           accountName,
			AccountKey:            accountKey,
			Container:             container,
			EndpointSchema:        "https",
			EndpointSuffix:        "core.windows.net",
			AssumeContainerExists: true,
			Timeout:               "1m",
		}})
}
