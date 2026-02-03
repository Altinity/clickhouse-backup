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
		pause := int64(0)
		// pausePercent := int64(90)
		for i := int64(0); i < 100; i++ {
			testBackupName := fmt.Sprintf("not_exists_%d", i)
			err = env.ch.Query(ifNotExistsCreateSQL)
			r.NoError(err)
			err = env.ch.Query(ifNotExistsInsertSQL)
			r.NoError(err)
			if i < 5 {
				log.Debug().Msgf("pauseChannel <- %d", 0)
				pauseChannel <- 0
			} else {
				log.Debug().Msgf("pauseChannel <- %d", pause/i)
				pauseChannel <- pause / i
			}
			startTime := time.Now()
			out, execErr := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "LOG_LEVEL=debug CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create --table freeze_not_exists.freeze_not_exists "+testBackupName)
			log.Debug().Msg(out)
			if (execErr != nil && (strings.Contains(out, "can't freeze") || strings.Contains(out, "no tables for backup"))) ||
				(execErr == nil && !strings.Contains(out, "can't freeze")) {
				parseTime := func(line string) time.Time {
					parsedTime, err := time.Parse("2006-01-02 15:04:05.999", line[:23])
					if err != nil {
						r.Failf("Error parsing time", "%s, : %v", line, err)
					}
					return parsedTime
				}
				lines := strings.Split(out, "\n")
				firstTime := parseTime(lines[0])
				var freezeTime time.Time
				for _, line := range lines {
					if strings.Contains(line, "create_table_query") {
						freezeTime = parseTime(line)
						break
					}
					if strings.Contains(line, "SELECT DISTINCT partition_id") {
						freezeTime = parseTime(line)
						break
					}
				}
				pause += (firstTime.Sub(startTime) + freezeTime.Sub(firstTime)).Nanoseconds()
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
			if pause > 0 {
				pauseStart := time.Now()
				time.Sleep(time.Duration(pause) * time.Nanosecond)
				log.Debug().Msgf("pause=%s pauseStart=%s", time.Duration(pause).String(), pauseStart.String())
				err = env.ch.DropOrDetachTable(clickhouse.Table{Database: "freeze_not_exists", Name: "freeze_not_exists"}, ifNotExistsCreateSQL, "", false, chVersion, "", false, "")
				r.NoError(err)
			}
			resumeChannel <- 1
		}
	}()
	wg.Wait()
	r.True(freezeErrorHandled, "freezeErrorHandled false")
	r.NoError(env.dropDatabase("test_skip_tables", true))
	r.NoError(env.dropDatabase("freeze_not_exists", true))
	t.Log("TestSkipNotExistsTable DONE, ALL OK")
	env.Cleanup(t, r)
}
