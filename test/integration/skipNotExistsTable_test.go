//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestSkipNotExistsTable(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.1") < 0 {
		t.Skip("TestSkipNotExistsTable too small time between `SELECT DISTINCT partition_id` and `ALTER TABLE ... FREEZE PARTITION`")
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	log.Debug().Msg("Check skip not exist errors")
	env.queryWithNoError(r, "CREATE DATABASE freeze_not_exists")
	ifNotExistsCreateSQL := "CREATE TABLE IF NOT EXISTS freeze_not_exists.freeze_not_exists (id UInt64) ENGINE=MergeTree() ORDER BY id"
	ifNotExistsInsertSQL := "INSERT INTO freeze_not_exists.freeze_not_exists SELECT number FROM numbers(1000)"
	chVersion, err := env.ch.GetVersion(t.Context())
	r.NoError(err)

	freezeErrorHandled := false
	iterations := int64(0)
	lastOut := ""
	lastExecErr := error(nil)
	sawCode60 := 0
	sawCantFreeze := 0
	sawNoTables := 0
	sawDeadlock := 0
	pauseChannel := make(chan int64)
	resumeChannel := make(chan int64)
	if os.Getenv("TEST_LOG_LEVEL") == "debug" {
		env.ch.Config.LogSQLQueries = true
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer func() {
			close(pauseChannel)
			wg.Done()
		}()
		// Bisection controller. Goal: drop the table BETWEEN backup's existence
		// check and FREEZE, producing ClickHouse code 60. Drop is too early ->
		// "no tables for backup" -> push pause UP. Drop is too late -> backup
		// finishes cleanly -> push pause DOWN. Both signals converge on the
		// race window.
		pause := int64(0)
		stepUpNs := int64(5 * time.Millisecond)
		stepDownNs := int64(2 * time.Millisecond)
		for i := int64(0); i < 100; i++ {
			testBackupName := fmt.Sprintf("not_exists_%d", i)
			err = env.ch.Query(ifNotExistsCreateSQL)
			r.NoError(err)
			err = env.ch.Query(ifNotExistsInsertSQL)
			r.NoError(err)
			log.Debug().Msgf("pauseChannel <- %d", pause)
			pauseChannel <- pause
			out, execErr := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "LOG_LEVEL=debug CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create --table freeze_not_exists.freeze_not_exists "+testBackupName)
			log.Debug().Msg(out)
			iterations = i + 1
			lastOut = out
			lastExecErr = execErr
			if strings.Contains(out, "code: 60") {
				sawCode60++
			}
			if strings.Contains(out, "can't freeze") {
				sawCantFreeze++
			}
			if strings.Contains(out, "no tables for backup") {
				sawNoTables++
			}
			if strings.Contains(out, "code: 473, message: Possible deadlock avoided") {
				sawDeadlock++
			}
			if strings.Contains(out, "no tables for backup") {
				pause += stepUpNs
			} else if execErr == nil && !strings.Contains(out, "can't freeze") && !strings.Contains(out, "code: 60") {
				pause -= stepDownNs
				if pause < 0 {
					pause = 0
				}
			}
			if execErr != nil {
				if !strings.Contains(out, "no tables for backup") && !strings.Contains(out, "code: 473, message: Possible deadlock avoided") {
					assert.NoError(t, execErr, "%s", out)
				}
			}

			if strings.Contains(out, "code: 60") && execErr == nil {
				freezeErrorHandled = true
				log.Debug().Msg("CODE 60 caught")
				<-resumeChannel
				env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ec", "CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup delete local "+testBackupName)
				break
			}
			if execErr == nil {
				execErr = env.DockerExec("clickhouse-backup", "bash", "-ec", "CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup delete local "+testBackupName)
				assert.NoError(t, execErr)
			}
			<-resumeChannel
		}
	}()
	go func() {
		defer func() {
			close(resumeChannel)
			wg.Done()
		}()
		for pause := range pauseChannel {
			log.Debug().Msgf("%d <- pauseChannel", pause)
			pauseStart := time.Now()
			if pause > 0 {
				time.Sleep(time.Duration(pause) * time.Nanosecond)
			}
			log.Debug().Msgf("pause=%s pauseStart=%s", time.Duration(pause).String(), pauseStart.String())
			err = env.ch.DropOrDetachTable(clickhouse.Table{Database: "freeze_not_exists", Name: "freeze_not_exists"}, ifNotExistsCreateSQL, "", false, chVersion, "", false, "")
			r.NoError(err)
			resumeChannel <- 1
		}
	}()
	wg.Wait()
	lastErrStr := "<nil>"
	if lastExecErr != nil {
		lastErrStr = lastExecErr.Error()
	}
	r.True(freezeErrorHandled,
		"freezeErrorHandled=false: backup never returned 'code: 60' with execErr==nil after %d iterations. "+
			"Occurrences: code:60=%d, can't freeze=%d, no tables for backup=%d, deadlock(473)=%d. "+
			"Last execErr=%s. Last output (tail 4000 chars):\n%s",
		iterations, sawCode60, sawCantFreeze, sawNoTables, sawDeadlock, lastErrStr, tailString(lastOut, 4000))
	r.NoError(env.dropDatabase("test_skip_tables", true))
	r.NoError(env.dropDatabase("freeze_not_exists", true))
	t.Log("TestSkipNotExistsTable DONE, ALL OK")
	env.Cleanup(t, r)
}

func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "...(truncated)...\n" + s[len(s)-n:]
}
