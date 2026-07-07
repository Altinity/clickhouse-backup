//go:build integration

package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// e2e coverage for download_max_bytes_per_second / upload_max_bytes_per_second
// (https://github.com/Altinity/clickhouse-backup/issues/1377) across every remote_storage
// type that runs in CI. We do NOT assert on wall-clock duration (flaky under CI load and
// the token-bucket burst); instead we run clickhouse-backup with LOG_LEVEL=debug and assert
// the rate limiter actually engaged from its log output. `remote_storage: custom` is not
// covered: throttling can't apply there because transfer happens inside the external command.
const bwLimitRateBytesPerSecond = 4 * 1024 * 1024 // 4 MiB/s aggregate throttle

func TestBwLimitS3(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	// also enable multipart download to guard the #1377 regression: multipart buffers the whole
	// object to a temp file at full speed, so the limiter must force the streaming path instead
	env.runBwLimitScenario(t, r, "S3", "config-s3.yml", false, "S3_ALLOW_MULTIPART_DOWNLOAD=true S3_CONCURRENCY=4")
}

func TestBwLimitGCS(t *testing.T) {
	if isTestShouldSkip("GCS_TESTS") {
		t.Skip("Skipping GCS integration tests because GCS_TESTS=false")
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.runBwLimitScenario(t, r, "GCS_EMULATOR", "config-gcs-custom-endpoint.yml", false, "")
}

func TestBwLimitAzure(t *testing.T) {
	if isTestShouldSkip("AZURE_TESTS") {
		t.Skip("Skipping Azure integration tests because AZURE_TESTS=false")
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.runBwLimitScenario(t, r, "AZBLOB", "config-azblob.yml", false, "")
}

func TestBwLimitFTP(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	// 21.8 can't execute SYSTEM RESTORE REPLICA, so restore_as_attach must stay disabled (config-ftp-old.yaml)
	backupConfig := "config-ftp.yaml"
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") < 1 {
		backupConfig = "config-ftp-old.yaml"
	}
	env.runBwLimitScenario(t, r, "FTP", backupConfig, false, "")
}

func TestBwLimitSFTP(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.runBwLimitScenario(t, r, "SFTP", "config-sftp-auth-password.yaml", false, "")
}

// skipUnlessEmbeddedBwLimitSupported skips when ClickHouse is older than 25.1, where
// max_backup_bandwidth started being honored inside the BACKUP/RESTORE SETTINGS clause (PR #72665).
func skipUnlessEmbeddedBwLimitSupported(t *testing.T) {
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "25.1") < 0 {
		t.Skipf("Test skipped, embedded max_backup_bandwidth in SETTINGS requires ClickHouse 25.1+, current %s", version)
	}
}

// TestBwLimitEmbeddedS3 validates that throttling is delegated to ClickHouse via the
// max_backup_bandwidth BACKUP/RESTORE SETTINGS when use_embedded_backup_restore=true.
func TestBwLimitEmbeddedS3(t *testing.T) {
	skipUnlessEmbeddedBwLimitSupported(t)
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/var/lib/clickhouse/disks/backups_s3/backup/")
	env.runBwLimitScenario(t, r, "EMBEDDED_S3", "config-s3-embedded.yml", true, "")
	// the TestBwLimit* name doesn't match Cleanup()'s TestEmbedded prefix, clear the bucket explicitly
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/disk_s3")
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/backups_s3")
}

func TestBwLimitEmbeddedAzure(t *testing.T) {
	skipUnlessEmbeddedBwLimitSupported(t)
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.DockerExecNoError(r, "clickhouse", "rm", "-rf", "/var/lib/clickhouse/disks/backups_azure/backup/")
	env.runBwLimitScenario(t, r, "EMBEDDED_AZURE", "config-azblob-embedded.yml", true, "")
	env.DockerExecNoError(r, "clickhouse", "rm", "-rf", "/var/lib/clickhouse/disk_s3")
}

// TestBwLimitEmbeddedGCS uses the GCS-over-S3 URL embedded backend, which requires real
// credentials (QA_GCS_OVER_S3_BUCKET) and a config generated from template, like TestEmbeddedGCSOverS3.
func TestBwLimitEmbeddedGCS(t *testing.T) {
	skipUnlessEmbeddedBwLimitSupported(t)
	if os.Getenv("QA_GCS_OVER_S3_BUCKET") == "" {
		t.Skip("Skipping embedded GCS-over-S3 test because QA_GCS_OVER_S3_BUCKET is not set")
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "gettext-base")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config-gcs-embedded-url.yml.template | envsubst > /etc/clickhouse-backup/config-gcs-embedded-url.yml")
	env.runBwLimitScenario(t, r, "EMBEDDED_GCS_URL", "config-gcs-embedded-url.yml", true, "")
}

// runBwLimitScenario generates the standard object-storage test dataset (createTestSchema /
// createTestData via generateTestData — this also creates object-disk tables, exercising
// object_disk.CopyObjectStreaming), then runs a backup round-trip with a bandwidth limit set
// and asserts from LOG_LEVEL=debug output that the limiter engaged:
//   - non-embedded: clickhouse-backup itself throttles, so its bwlimit log lines must appear
//     (upload/download "rate limiter active", and on download the multipart-bypass streaming line);
//   - embedded: ClickHouse throttles via max_backup_bandwidth, so the generated BACKUP/RESTORE SQL
//     (logged by the clickhouse client) must contain the max_backup_bandwidth setting.
func (env *TestEnvironment) runBwLimitScenario(t *testing.T, r *require.Assertions, remoteStorageType, backupConfig string, embedded bool, extraDownloadEnv string) {
	env.connectWithWait(t, r, 500*time.Millisecond, 1500*time.Millisecond, 3*time.Minute)
	log.Debug().Msgf("bwlimit scenario storage=%s config=%s embedded=%v", remoteStorageType, backupConfig, embedded)

	backupName := fmt.Sprintf("%s_%d", t.Name(), rand.Int())
	cfgPath := "/etc/clickhouse-backup/" + backupConfig
	tablesPattern := fmt.Sprintf("*_%s.*", t.Name())
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameReplicated, dbNameMySQL, dbNamePostgreSQL, Issue331Issue1091Atomic, Issue331Issue1091Ordinary}
	maxBackupBandwidth := fmt.Sprintf("max_backup_bandwidth=%d", bwLimitRateBytesPerSecond)
	// ALLOW_OBJECT_DISK_STREAMING=1 forces object-disk transfers through clickhouse-backup
	// (e.g. FTP reading from an S3 disk) via object_disk.CopyObjectStreaming instead of server-side
	// CopyObject, so the rate limiter governs those bytes too.
	uploadEnv := fmt.Sprintf("LOG_LEVEL=debug ALLOW_OBJECT_DISK_STREAMING=1 UPLOAD_MAX_BYTES_PER_SECOND=%d", bwLimitRateBytesPerSecond)
	downloadEnv := fmt.Sprintf("LOG_LEVEL=debug ALLOW_OBJECT_DISK_STREAMING=1 DOWNLOAD_MAX_BYTES_PER_SECOND=%d", bwLimitRateBytesPerSecond)
	if extraDownloadEnv != "" {
		downloadEnv = extraDownloadEnv + " " + downloadEnv
	}

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, databaseList, true, false, false, backupConfig)
	createAllTypesOfObjectTables := !strings.Contains(remoteStorageType, "CUSTOM")
	testData := generateTestData(t, r, env, remoteStorageType, createAllTypesOfObjectTables, defaultTestData())

	if embedded {
		out := env.runBwLimitCommand(r, fmt.Sprintf("%s clickhouse-backup -c %s create_remote --tables %s %s", uploadEnv, cfgPath, tablesPattern, backupName))
		r.Contains(out, maxBackupBandwidth, "embedded BACKUP must pass %s in the BACKUP SETTINGS clause", maxBackupBandwidth)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfgPath, "delete", "local", backupName)
		dropDatabasesFromTestDataDataSet(t, r, env, databaseList, true)

		out = env.runBwLimitCommand(r, fmt.Sprintf("%s clickhouse-backup -c %s restore_remote --rm %s", downloadEnv, cfgPath, backupName))
		r.Contains(out, maxBackupBandwidth, "embedded RESTORE must pass %s in the RESTORE SETTINGS clause", maxBackupBandwidth)
	} else {
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfgPath, "create", "--tables", tablesPattern, backupName)

		out := env.runBwLimitCommand(r, fmt.Sprintf("%s clickhouse-backup -c %s upload %s", uploadEnv, cfgPath, backupName))
		r.Contains(out, "bwlimit: upload rate limiter active", "upload must activate the rate limiter")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfgPath, "delete", "local", backupName)
		dropDatabasesFromTestDataDataSet(t, r, env, databaseList, true)

		out = env.runBwLimitCommand(r, fmt.Sprintf("%s clickhouse-backup -c %s download %s", downloadEnv, cfgPath, backupName))
		r.Contains(out, "bwlimit: download rate limiter active", "download must activate the rate limiter")
		// multipart download exists only for S3/COS; that's the only backend where the limiter must
		// force the streaming reader (the #1377 regression guard). Other backends always stream.
		if remoteStorageType == "S3" || remoteStorageType == "COS" {
			r.Contains(out, "using streaming reader (multipart bypassed)", "S3 download with a rate limit must bypass multipart and force the streaming reader")
		}
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfgPath, "restore", "--rm", backupName)
	}

	for i := range testData {
		if testData[i].CheckDatabaseOnly {
			r.NoError(env.checkDatabaseEngine(t, testData[i]))
		} else if !isTableSkip(env, testData[i], true) {
			r.NoError(env.checkData(t, r, testData[i]))
		}
	}

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, databaseList, true, true, false, backupConfig)
}

func (env *TestEnvironment) runBwLimitCommand(r *require.Assertions, cmd string) string {
	log.Debug().Msg(cmd)
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", cmd)
	r.NoError(err, "%s\n%s", cmd, out)
	return out
}
