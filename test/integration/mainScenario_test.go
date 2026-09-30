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

func (env *TestEnvironment) runIntegrationCustom(t *testing.T, r *require.Assertions, customType string) {
	env.DockerExecNoError(r, "clickhouse-backup", "mkdir", "-pv", "/custom/"+customType)
	r.NoError(env.DockerCP("./"+customType+"/", "clickhouse-backup:/custom/"))
	env.runMainIntegrationScenario(t, "CUSTOM", "config-custom-"+customType+".yml")
}

func (env *TestEnvironment) runMainIntegrationScenario(t *testing.T, remoteStorageType, backupConfig string) {
	var out string
	var err error
	r := require.New(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1500*time.Millisecond, 3*time.Minute)

	// main test scenario
	fullBackupName := fmt.Sprintf("%s_full_%d", t.Name(), rand.Int())
	incrementBackupName := fmt.Sprintf("%s_increment_%d", t.Name(), rand.Int())
	incrementBackupName2 := fmt.Sprintf("%s_increment2_%d", t.Name(), rand.Int())
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameReplicated, dbNameMySQL, dbNamePostgreSQL, Issue331Issue1091Atomic, Issue331Issue1091Ordinary}
	tablesPattern := fmt.Sprintf("*_%s.*", t.Name())
	defer cleanupAfterFailedScenario(t, env, databaseList, backupConfig)
	log.Debug().Msg("Clean before start")
	fullCleanup(t, r, env, []string{fullBackupName, incrementBackupName}, []string{"remote", "local"}, databaseList, true, false, false, backupConfig)
	createAllTypesOfObjectTables := !strings.Contains(remoteStorageType, "CUSTOM")
	testData := generateTestData(t, r, env, remoteStorageType, createAllTypesOfObjectTables, defaultTestData())

	log.Debug().Msg("Create full backup")
	createCmd := "clickhouse-backup -c /etc/clickhouse-backup/" + backupConfig + " create --resume --tables=" + tablesPattern + " " + fullBackupName
	env.checkResumeAlreadyProcessed(createCmd, fullBackupName, "create", r, remoteStorageType)

	log.Debug().Msg("Upload full backup")
	uploadCmd := fmt.Sprintf("%s_COMPRESSION_FORMAT=zstd CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/%s clickhouse-backup upload --resume %s", remoteStorageType, backupConfig, fullBackupName)
	env.checkResumeAlreadyProcessed(uploadCmd, fullBackupName, "upload", r, remoteStorageType)

	// https://github.com/Altinity/clickhouse-backup/issues/1373
	// create a table that exists ONLY in the increment backup (not in the full)
	log.Debug().Msg("Create increment-only table (issue #1373)")
	incrementOnlyData := []TestDataStruct{
		{
			Database: dbNameAtomic, DatabaseEngine: "Atomic",
			Name:   "increment_only_table",
			Schema: "(id UInt64, value String) ENGINE = MergeTree ORDER BY id",
			Rows: []map[string]interface{}{
				{"id": uint64(1), "value": "increment-only-1"},
				{"id": uint64(2), "value": "increment-only-2"},
			},
			Fields:  []string{"id", "value"},
			OrderBy: "id",
		},
	}
	for _, data := range incrementOnlyData {
		r.NoError(env.createTestSchema(t, data, remoteStorageType))
		r.NoError(env.createTestData(t, data))
	}

	log.Debug().Msg("Create increment1 with data")
	incrementData := generateIncrementTestData(t, r, env, remoteStorageType, createAllTypesOfObjectTables, defaultIncrementData(), 1)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--tables", tablesPattern, incrementBackupName)

	// https://github.com/Altinity/clickhouse-backup/pull/900
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		log.Debug().Msg("create --diff-from-remote backup")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--diff-from-remote", fullBackupName, "--tables", tablesPattern, incrementBackupName2)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "upload", incrementBackupName2)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "remote", incrementBackupName2)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", incrementBackupName2)
	}

	log.Debug().Msg("Upload increment")
	uploadCmd = fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s upload %s --diff-from-remote %s --resume", backupConfig, incrementBackupName, fullBackupName)
	env.checkResumeAlreadyProcessed(uploadCmd, incrementBackupName, "upload", r, remoteStorageType)

	backupDir := "/var/lib/clickhouse/backup"
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") && !strings.HasSuffix(remoteStorageType, "_URL") {
		backupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED"))
	}
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "ls -lha "+backupDir+" | grep "+t.Name())
	r.NoError(err)
	r.Equal(2, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect '2' backups exists in backup directory")
	log.Debug().Msg("Delete backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", incrementBackupName)
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "ls -lha "+backupDir)
	r.NoError(err)
	r.NotContains(strings.Trim(out, " \t\r\n"), t.Name(), "expect no backup exists in backup directory")

	dropDatabasesFromTestDataDataSet(t, r, env, databaseList, true)

	log.Debug().Msg("Download")
	replaceStorageDiskNameForReBalance(t, r, env, remoteStorageType, false)
	downloadCmd := fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s download --resume %s", backupConfig, fullBackupName)
	env.checkResumeAlreadyProcessed(downloadCmd, fullBackupName, "download", r, remoteStorageType)

	log.Debug().Msg("Restore schema")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--schema", fullBackupName)

	log.Debug().Msg("Restore data")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--data", fullBackupName)

	log.Debug().Msg("Full restore with rm")
	// --rm needed: previous `restore --data` populated the cluster, safety check from issue #1325 refuses drop without it
	restoreRmCmd := "clickhouse-backup -c /etc/clickhouse-backup/" + backupConfig + " restore --resume --rm " + fullBackupName
	env.checkResumeAlreadyProcessed(restoreRmCmd, fullBackupName, "restore", r, remoteStorageType)

	log.Debug().Msg("Check data")
	for i := range testData {
		if testData[i].CheckDatabaseOnly {
			r.NoError(env.checkDatabaseEngine(t, testData[i]))
		} else {
			if isTableSkip(env, testData[i], true) {
				continue
			}
			r.NoError(env.checkData(t, r, testData[i]))
		}
	}

	dropDatabasesFromTestDataDataSet(t, r, env, databaseList, true)
	log.Debug().Msg("Restore remote with --hardlink-exists-files")
	restoreRemoteCmd := fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s restore_remote --hardlink-exists-files %s", backupConfig, fullBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xce", restoreRemoteCmd)
	log.Debug().Msg("Check data after restore_remote")
	for i := range testData {
		if testData[i].CheckDatabaseOnly {
			r.NoError(env.checkDatabaseEngine(t, testData[i]))
		} else {
			if isTableSkip(env, testData[i], true) {
				continue
			}
			r.NoError(env.checkData(t, r, testData[i]))
		}
	}
	// test increment
	dropDatabasesFromTestDataDataSet(t, r, env, databaseList, true)

	log.Debug().Msg("Delete backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName)

	log.Debug().Msg("Download increment")
	downloadCmd = fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s download --resume %s", backupConfig, incrementBackupName)
	env.checkResumeAlreadyProcessed(downloadCmd, incrementBackupName, "download", r, remoteStorageType)

	log.Debug().Msg("Restore")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--schema", "--data", incrementBackupName)

	log.Debug().Msg("Check increment data")
	for i := range testData {
		testDataItem := testData[i]
		if isTableSkip(env, testDataItem, true) || testDataItem.IsDictionary {
			continue
		}
		for _, incrementDataItem := range incrementData {
			if testDataItem.Database == incrementDataItem.Database && testDataItem.Name == incrementDataItem.Name {
				testDataItem.Rows = append(testDataItem.Rows, incrementDataItem.Rows...)
			}
		}
		if testDataItem.CheckDatabaseOnly {
			r.NoError(env.checkDatabaseEngine(t, testDataItem))
		} else {
			r.NoError(env.checkData(t, r, testDataItem))
		}
	}

	log.Debug().Msg("Check increment-only table data (issue #1373)")
	for _, data := range incrementOnlyData {
		r.NoError(env.checkData(t, r, data))
	}

	// test end
	log.Debug().Msg("Clean after finish")
	// during download increment, partially downloaded full will also clean
	fullCleanup(t, r, env, []string{incrementBackupName}, []string{"local"}, nil, false, true, false, backupConfig)
	fullCleanup(t, r, env, []string{fullBackupName, incrementBackupName}, []string{"remote"}, databaseList, true, true, true, backupConfig)
	replaceStorageDiskNameForReBalance(t, r, env, remoteStorageType, true)

	// 23.3 drop database doesn't cleanup object disk after DROP DATABASE
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") == 0 {
		env.DockerExecNoError(r, "minio", "bash", "-c", "rm -rf /minio/data/clickhouse/disk_s3")
	}
	// test for specified partitions backup
	testBackupSpecifiedPartitions(t, r, env, remoteStorageType, backupConfig)

	env.checkObjectStorageIsEmpty(t, r, remoteStorageType, backupConfig)
}

// cleanupAfterFailedScenario drops the databases and the local and remote backups left behind when a
// step of runMainIntegrationScenario fails and aborts the scenario before its own cleanup, otherwise
// every later test which borrows the same pooled environment fails too (see the TestFTP cascade in
// PR #1559 CI). It has to run as a defer inside the scenario, t.Cleanup is too late: env.Cleanup has
// returned the environment to the pool by then. It never asserts, the original failure is the one to report.
func cleanupAfterFailedScenario(t *testing.T, env *TestEnvironment, databaseList []string, backupConfig string) {
	if !t.Failed() {
		return
	}
	log.Warn().Msgf("%s failed, cleanup leftovers before the environment returns to the pool", t.Name())
	for _, backupType := range []string{"local", "remote"} {
		// stderr carries log lines which mention the test name too, `cut` would take their timestamp as a backup name
		list, listErr := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
			"clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" list "+backupType+" 2>/dev/null | grep -F "+t.Name()+" | cut -d ' ' -f 1")
		if listErr != nil {
			log.Warn().Msgf("can't list %s backups: %v\n%s", backupType, listErr, list)
			continue
		}
		// newest first, an increment has to be deleted before the full backup it requires
		for _, backupName := range childrenFirst(strings.Split(strings.TrimSpace(list), "\n")) {
			if backupName == "" {
				continue
			}
			if out, delErr := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
				"clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" delete "+backupType+" "+backupName); delErr != nil {
				log.Warn().Msgf("can't delete %s %s: %v\n%s", backupType, backupName, delErr, out)
			}
		}
	}
	for _, db := range databaseList {
		if dropErr := env.dropDatabase(db+"_"+t.Name(), true); dropErr != nil {
			log.Warn().Msgf("can't drop database %s_%s: %v", db, t.Name(), dropErr)
		}
	}
}
