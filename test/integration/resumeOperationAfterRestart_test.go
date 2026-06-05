//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type resumeAfterRestartCase struct {
	name       string
	configFile string
	setup      func(*testing.T, *TestEnvironment, *require.Assertions)
	skip       func(*testing.T)
}

const resumeAfterRestartServerLog = "/tmp/clickhouse-backup-resume-after-restart-server.log"

func TestResumeOperationsAfterRestartS3(t *testing.T) {
	runResumeOperationsAfterRestart(t, resumeAfterRestartCase{name: "S3", configFile: "config-s3.yml"})
}

func TestResumeOperationsAfterRestartGCS(t *testing.T) {
	runResumeOperationsAfterRestart(t, resumeAfterRestartCase{
		name:       "GCS",
		configFile: "config-gcs.yml",
		skip: func(t *testing.T) {
			if isTestShouldSkip("GCS_TESTS") {
				t.Skip("Skipping GCS integration tests...")
			}
		},
	})
}

func TestResumeOperationsAfterRestartAzure(t *testing.T) {
	runResumeOperationsAfterRestart(t, resumeAfterRestartCase{
		name:       "AZBLOB",
		configFile: "config-azblob.yml",
		skip: func(t *testing.T) {
			if isTestShouldSkip("AZURE_TESTS") {
				t.Skip("Skipping Azure integration tests...")
			}
		},
	})
}

func TestResumeOperationsAfterRestartFTP(t *testing.T) {
	configFile := "config-ftp.yaml"
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") < 0 {
		configFile = "config-ftp-old.yaml"
	}
	runResumeOperationsAfterRestart(t, resumeAfterRestartCase{name: "FTP", configFile: configFile})
}

func TestResumeOperationsAfterRestartSFTP(t *testing.T) {
	runResumeOperationsAfterRestart(t, resumeAfterRestartCase{name: "SFTP", configFile: "config-sftp-auth-password.yaml"})
}

func TestResumeOperationsAfterRestartCOS(t *testing.T) {
	runResumeOperationsAfterRestart(t, resumeAfterRestartCase{
		name:       "COS",
		configFile: "config-cos.yml",
		skip: func(t *testing.T) {
			if os.Getenv("QA_TENCENT_SECRET_KEY") == "" {
				t.Skip("Skipping Tencent Cloud Object Storage integration tests... QA_TENCENT_SECRET_KEY missing")
			}
		},
		setup: func(t *testing.T, env *TestEnvironment, r *require.Assertions) {
			env.InstallDebIfNotExists(r, "clickhouse-backup", "gettext-base")
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config-cos.yml.template | envsubst > /etc/clickhouse-backup/config-cos.yml")
		},
	})
}

func runResumeOperationsAfterRestart(t *testing.T, tc resumeAfterRestartCase) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") < 0 {
		t.Skip("ResumeOperationsAfterRestart create/restore coverage requires ClickHouse >= 21.8")
	}
	if tc.skip != nil {
		tc.skip(t)
	}

	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1500*time.Millisecond, 3*time.Minute)
	if tc.setup != nil {
		tc.setup(t, env, r)
	}
	testID := safeResumeIdentifier(t.Name())
	dbName := "resume_restart_" + testID
	tableName := "events"
	tablePattern := dbName + ".*"
	baseBackup := testID + "_base"
	resumeBackup := testID + "_resume"
	storagePolicy := resumeStoragePolicy(t, tc.name)
	rowsPerStep := uint64(5000)
	partitionsCount := uint64(4)

	fullCleanup(t, r, env, []string{baseBackup, resumeBackup}, []string{"remote", "local"}, []string{dbName}, false, false, false, tc.configFile)
	defer fullCleanup(t, r, env, []string{baseBackup, resumeBackup}, []string{"remote", "local"}, []string{dbName}, false, false, false, tc.configFile)

	env.queryWithNoError(r, "DROP DATABASE IF EXISTS "+dbName+" SYNC")
	env.queryWithNoError(r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE %s.%s (id UInt64, payload String) ENGINE=MergeTree() PARTITION BY id %% %d ORDER BY id SETTINGS storage_policy='%s'", dbName, tableName, partitionsCount, storagePolicy))
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO %s.%s SELECT number, repeat('x', 1024) FROM numbers(%d)", dbName, tableName, rowsPerStep))

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+tc.configFile, "create", "--tables="+tablePattern, baseBackup)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+tc.configFile, "upload", baseBackup)
	removeResumeState(t, r, env, baseBackup, "create", "upload")
	env.DockerExecNoError(r, "clickhouse-backup", "rm", "-f", resumeAfterRestartServerLog)
	startResumeAfterRestartServer(t, r, env, tc.configFile)
	// stop the background `clickhouse-backup server` so it does not leak into the pooled env and collide on :7171 with the next test
	defer env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "pkill -9 -f '[c]lickhouse-backup server' || true")

	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO %s.%s SELECT number + %d, repeat('y', 1024) FROM numbers(%d)", dbName, tableName, rowsPerStep, rowsPerStep))

	resumeOperationAfterRestart(t, r, env, tc.configFile, "create", resumeBackup,
		resumeAPIRequest(fmt.Sprintf("http://localhost:7171/backup/create?name=%s&table=%s&diff-from-remote=%s&resume=1", resumeBackup, tablePattern, baseBackup)),
		"/var/lib/clickhouse/backup/"+resumeBackup+"/create.state2",
		func() (bool, string) {
			listed, detail := backupListed(env, tc.configFile, "local", resumeBackup)
			if !listed {
				return false, detail
			}
			return backupFileExists(env, resumeBackup, "metadata.json")
		})

	resumeOperationAfterRestart(t, r, env, tc.configFile, "upload", resumeBackup,
		resumeAPIRequest(fmt.Sprintf("http://localhost:7171/backup/upload/%s?resume=1", resumeBackup)),
		"/var/lib/clickhouse/backup/"+resumeBackup+"/upload.state2",
		func() (bool, string) {
			return backupListed(env, tc.configFile, "remote", resumeBackup)
		})

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+tc.configFile, "delete", "local", resumeBackup)
	resumeOperationAfterRestart(t, r, env, tc.configFile, "download", resumeBackup,
		resumeAPIRequest(fmt.Sprintf("http://localhost:7171/backup/download/%s?resume=1", resumeBackup)),
		"/var/lib/clickhouse/backup/"+resumeBackup+"/download.state2",
		func() (bool, string) {
			listed, detail := backupListed(env, tc.configFile, "local", resumeBackup)
			if !listed {
				return false, detail
			}
			return backupFileExists(env, resumeBackup, "metadata.json")
		})

	env.queryWithNoError(r, "DROP DATABASE "+dbName+" SYNC")
	resumeOperationAfterRestart(t, r, env, tc.configFile, "restore", resumeBackup,
		resumeAPIRequest(fmt.Sprintf("http://localhost:7171/backup/restore/%s?rm=1&resume=1", resumeBackup)),
		"/var/lib/clickhouse/backup/"+resumeBackup+"/restore.state2",
		func() (bool, string) {
			var count uint64
			if err := env.ch.SelectSingleRowNoCtx(&count, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName)); err != nil {
				return false, err.Error()
			}
			return count == rowsPerStep*2, fmt.Sprintf("count=%d expected=%d", count, rowsPerStep*2)
		})
}

func resumeAPIRequest(url string) string {
	return fmt.Sprintf("wget -qO- --post-data='' '%s'", url)
}

func startResumeAfterRestartServer(t *testing.T, r *require.Assertions, env *TestEnvironment, configFile string) {
	t.Helper()
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "pkill -9 -f '[c]lickhouse-backup server' || true")
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/%s API_COMPLETE_RESUMABLE_AFTER_RESTART_COMMANDS=create,upload,download,restore clickhouse-backup server &>>%s", configFile, resumeAfterRestartServerLog))
	waitResumeAfterRestartServerReady(t, r, env)
}

func removeResumeState(t *testing.T, r *require.Assertions, env *TestEnvironment, backupName string, commands ...string) {
	t.Helper()
	files := make([]string, 0, len(commands)+1)
	for _, command := range commands {
		files = append(files, fmt.Sprintf("/var/lib/clickhouse/backup/%s/%s.state2", backupName, command))
	}
	files = append(files, fmt.Sprintf("/tmp/clickhouse-backup.%s.pid", backupName))
	env.DockerExecNoError(r, "clickhouse-backup", append([]string{"rm", "-f"}, files...)...)
}

func waitResumeAfterRestartServerReady(t *testing.T, r *require.Assertions, env *TestEnvironment) {
	t.Helper()
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", `
for i in $(seq 1 120); do
  wget -qO- 'http://localhost:7171/backup/version' >/dev/null 2>&1 && exit 0
  if ! pgrep -f '[c]lickhouse-backup server' >/dev/null 2>&1; then
    echo 'clickhouse-backup server is not running'
    tail -n 200 `+resumeAfterRestartServerLog+` || true
    exit 1
  fi
  sleep 1
done
echo 'clickhouse-backup server is not ready'
tail -n 200 `+resumeAfterRestartServerLog+` || true
exit 1
`)
	r.NoError(err, "%s", out)
}

func resumeOperationAfterRestart(t *testing.T, r *require.Assertions, env *TestEnvironment, configFile, command, backupName, apiRequest, stateFile string, assertResumed func() (bool, string)) {
	t.Helper()
	logPath := fmt.Sprintf("/tmp/resume-after-restart-%s-%s.log", backupName, command)
	killedPath := fmt.Sprintf("/tmp/resume-after-restart-%s-%s.killed", backupName, command)
	pidPath := fmt.Sprintf("/tmp/clickhouse-backup.%s.pid", backupName)
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "rm -f "+logPath+" "+killedPath+" "+pidPath)
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf(`
for i in $(seq 1 60000); do
  if [ -f %[1]s ] && [ -f %[2]s ]; then
    pid=$(cut -d'|' -f1 %[1]s)
    if [ -z "$pid" ]; then
      echo "server pid not found" > %[3]s
      exit 3
    fi
    kill -9 "$pid"
    echo "$pid" > %[3]s
    exit 0
  fi
  sleep 0.005
done
exit 1
`, pidPath, stateFile, killedPath))

	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", apiRequest)
	r.NoError(err, "%s\nfailed to start %s %s through API", out, command, backupName)
	r.Contains(out, "acknowledged")
	waitResumeServerInterruptedPoint(t, r, env, killedPath, stateFile, logPath)

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("pid=$(cat %s); for i in $(seq 1 30); do kill -0 \"$pid\" 2>/dev/null || exit 0; sleep 1; done; echo process still alive pid=$pid; exit 1", killedPath))
	r.NoError(err, "%s\nserver process is still alive after SIGKILL during %s %s", out, command, backupName)

	startResumeAfterRestartServer(t, r, env, configFile)
	waitResumeAssertion(t, r, assertResumed)
	removeResumeState(t, r, env, backupName, command)
}

func waitResumeServerInterruptedPoint(t *testing.T, r *require.Assertions, env *TestEnvironment, killedPath, stateFile, logPath string) {
	t.Helper()
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf(`
for i in $(seq 1 120); do
  if [ -f %[1]s ] && [ -f %[2]s ]; then
    exit 0
  fi
  sleep 0.5
done
echo "timeout waiting for %[1]s and %[2]s"
ls -la "$(dirname %[1]s)" "$(dirname %[2]s)" || true
cat %[3]s || true
tail -n 200 %[4]s || true
exit 1
`, killedPath, stateFile, logPath, resumeAfterRestartServerLog))
	r.NoError(err, "%s", out)
}

func waitResumeAssertion(t *testing.T, r *require.Assertions, assertResumed func() (bool, string)) {
	t.Helper()
	lastDetail := ""
	for i := 0; i < 180; i++ {
		ok, detail := assertResumed()
		if ok {
			return
		}
		lastDetail = detail
		time.Sleep(1 * time.Second)
	}
	r.FailNow("timeout waiting for resumed operation", lastDetail)
}

func backupListed(env *TestEnvironment, configFile, location, backupName string) (bool, string) {
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+configFile, "list", location)
	if err != nil {
		return false, out
	}
	return strings.Contains(out, backupName), out
}

func backupFileExists(env *TestEnvironment, backupName, fileName string) (bool, string) {
	path := fmt.Sprintf("/var/lib/clickhouse/backup/%s/%s", backupName, fileName)
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("test -f %s && echo exists || (ls -la %s || true; exit 1)", path, fmt.Sprintf("/var/lib/clickhouse/backup/%s", backupName)))
	if err != nil {
		return false, out
	}
	return true, out
}

func resumeStoragePolicy(t *testing.T, remoteStorageType string) string {
	switch remoteStorageType {
	case "S3":
		return "s3_only"
	case "GCS":
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.6") < 0 {
			t.Skip("Skipping GCS ObjectDisk resume test: gcs_only storage policy (GCS over S3) requires ClickHouse >= 22.6")
		}
		if os.Getenv("QA_GCS_OVER_S3_BUCKET") == "" {
			t.Skip("Skipping GCS ObjectDisk resume test: QA_GCS_OVER_S3_BUCKET missing")
		}
		return "gcs_only"
	case "AZBLOB":
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") < 0 {
			t.Skip("Skipping Azure ObjectDisk resume test: azure_blob_storage policy requires ClickHouse >= 23.3")
		}
		return "azure_only"
	case "COS":
		return "cos_only"
	case "FTP", "SFTP":
		return "s3_only"
	default:
		t.Fatalf("unsupported resume ObjectDisk storage policy for %s", remoteStorageType)
		return ""
	}
}

func safeResumeIdentifier(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, strings.ToLower(s))
}
