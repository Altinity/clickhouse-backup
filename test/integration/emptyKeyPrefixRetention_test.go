//go:build integration

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog/log"
)

// minioS3Client - S3 client to the test minio container via its mapped host port,
// chainguard/minio has no curl and `mc` refuses trailing-slash object names, https://github.com/Altinity/clickhouse-backup/issues/1394
func (env *TestEnvironment) minioS3Client(ctx context.Context) (*s3.Client, error) {
	host, port, err := env.tc.GetMappedPort(ctx, "minio", "9000")
	if err != nil {
		return nil, err
	}
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(fmt.Sprintf("https://%s:%d", host, port)),
		Region:       "us-east-1",
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("access_key", "it_is_my_super_secret_key", ""),
		HTTPClient:   &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}},
	}), nil
}

// https://github.com/Altinity/clickhouse-backup/issues/1524
// A zero-byte "folder placeholder" object whose key is exactly `<s3.path>/` (created by
// cloud consoles "Create folder", `aws s3api put-object --key prefix/`, etc.) is returned
// by BackupList as a broken backup with an empty name. Remote retention then deletes
// that entry, and `path.Join("", ...)` collapses to the prefix root, wiping every backup.
func TestEmptyKeyPrefixRetention(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	const configFile = "config-s3.yml"
	r.NoError(env.DockerCP("configs/"+configFile, "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	cfgPath, _ := env.resolveConfigPaths(r, configFile)
	markerKey := cfgPath + "/"
	s3Client, err := env.minioS3Client(t.Context())
	r.NoError(err, "minio s3 client")
	defer func() {
		_, err := s3Client.DeleteObject(t.Context(), &s3.DeleteObjectInput{Bucket: aws.String("clickhouse"), Key: aws.String(markerKey)})
		r.NoError(err, "DeleteObject %s", markerKey)
		env.checkObjectStorageIsEmpty(t, r, "S3", configFile)
	}()

	chVer := strings.ReplaceAll(os.Getenv("CLICKHOUSE_VERSION"), ".", "_")
	tableName := "default.empty_key_prefix_retention_" + chVer
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id UInt64) ENGINE=MergeTree() ORDER BY id", tableName))
	defer func() {
		// runs before env.Cleanup returns the shared environment to the pool, a leaked table breaks TestListFormat
		dropQ := "DROP TABLE IF EXISTS " + tableName
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
			dropQ += " NO DELAY"
		}
		env.DockerExecNoError(r, "clickhouse", "clickhouse", "client", "-q", dropQ)
	}()
	env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s SELECT number FROM numbers(50)", tableName))

	backupNames := []string{"empty_key_prefix_1_" + chVer, "empty_key_prefix_2_" + chVer}
	defer func() {
		for _, name := range backupNames {
			for _, loc := range []string{"remote", "local"} {
				if out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "delete", loc, name); err != nil {
					log.Warn().Err(err).Msgf("teardown delete %s %s: %s", loc, name, out)
				}
			}
		}
	}()

	log.Debug().Msg("Plant zero-byte object with key == s3.path + '/' (cloud console 'Create folder' emulation)")
	_, err = s3Client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("clickhouse"), Key: aws.String(markerKey), Body: strings.NewReader("")})
	r.NoError(err, "PutObject %s", markerKey)

	log.Debug().Msg("list remote must not report the placeholder as a nameless broken backup")
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "list", "remote")
	r.NoError(err, "list remote: %s", out)
	r.NotContains(out, "broken", "placeholder object leaked into backup list as broken entry:\n%s", out)

	log.Debug().Msg("Retention with BACKUPS_TO_KEEP_REMOTE=2 shall delete nothing while only two real backups exist")
	for _, name := range backupNames {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=2 clickhouse-backup create_remote --tables=%s %s", tableName, name))
	}
	checkBackupsAlive := func(stage string) {
		out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "list", "remote")
		r.NoError(err, "list remote after %s: %s", stage, out)
		for _, name := range backupNames {
			r.Regexp("(?m)^"+name+" ", out, "backup %s was wiped after %s:\n%s", name, stage, out)
		}
		r.NotContains(out, "broken", "unexpected broken entries after %s:\n%s", stage, out)
	}
	checkBackupsAlive("retention")

	log.Debug().Msg("delete remote with empty name (and '/', which sanitizes to empty) shall fail instead of wiping the remote path")
	for _, emptyName := range []string{"", "/"} {
		out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "delete", "remote", emptyName)
		r.Error(err, "delete remote %q must fail, output: %s", emptyName, out)
	}
	checkBackupsAlive("delete remote with empty name")

	log.Debug().Msg("clean_remote_broken shall ignore the placeholder and keep real backups")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "clean_remote_broken")
	checkBackupsAlive("clean_remote_broken")

	log.Debug().Msg("placeholder object itself is untouched by clickhouse-backup")
	_, err = s3Client.HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: aws.String("clickhouse"), Key: aws.String(markerKey)})
	r.NoError(err, "HeadObject %s", markerKey)
}
