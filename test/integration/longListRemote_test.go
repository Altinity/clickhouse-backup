//go:build integration

package main

import (
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/rs/zerolog/log"
)

func TestLongListRemote(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	totalCacheCount := 20
	testBackupName := "test_list_remote"

	for i := 0; i < totalCacheCount; i++ {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml ALLOW_EMPTY_BACKUPS=true RBAC_BACKUP_ALWAYS=false clickhouse-backup create_remote %s_%d", testBackupName, i))
	}

	r.NoError(utils.ExecCmd(t.Context(), 180*time.Second, "docker", append(env.GetDefaultComposeCommand(), "restart", "minio")...))
	time.Sleep(2 * time.Second)

	var err error
	var cachedOut, nonCachedOut, clearCacheOut string
	listTimeMsRE := regexp.MustCompile(`list_duration=(\d+.\d+)`)
	extractListTimeMs := func(out string) float64 {
		r.Contains(out, "list_duration=")
		matches := listTimeMsRE.FindStringSubmatch(out)
		r.True(len(matches) == 2)
		log.Debug().Msgf("extractListTimeMs=%s", matches[1])
		result, parseErr := strconv.ParseFloat(matches[1], 64)
		r.NoError(parseErr)
		log.Debug().Msg(out)
		return result
	}
	env.DockerExecNoError(r, "clickhouse-backup", "rm", "-rfv", "/tmp/.clickhouse-backup-metadata.cache.S3")
	nonCachedOut, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "remote")
	r.NoError(err)
	noCacheDuration := extractListTimeMs(nonCachedOut)

	env.DockerExecNoError(r, "clickhouse-backup", "chmod", "-Rv", "+r", "/tmp/.clickhouse-backup-metadata.cache.S3")

	cachedOut, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "remote")
	r.NoError(err)
	cachedDuration := extractListTimeMs(cachedOut)
	if noCacheDuration <= cachedDuration {
		log.Debug().Msg("===== NON CACHED OUT ======")
		log.Debug().Msg(nonCachedOut)
		log.Debug().Msg("===== CACHED OUT ======")
		log.Debug().Msg(cachedOut)
	}
	r.GreaterOrEqualf(noCacheDuration, cachedDuration, "noCacheDuration=%f shall be greater cachedDuration=%f", noCacheDuration, cachedDuration)

	env.DockerExecNoError(r, "clickhouse-backup", "rm", "-Rfv", "/tmp/.clickhouse-backup-metadata.cache.S3")
	clearCacheOut, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "remote")
	cacheClearDuration := extractListTimeMs(clearCacheOut)

	if noCacheDuration <= cacheClearDuration {
		log.Debug().Msg("===== NON CACHED OUT ======")
		log.Debug().Msg(nonCachedOut)
		log.Debug().Msg("===== CLEAR CACHE OUT ======")
		log.Debug().Msg(clearCacheOut)
	}

	r.GreaterOrEqualf(cacheClearDuration, cachedDuration, "cacheClearDuration=%f ms shall be greater cachedDuration=%f ms", cacheClearDuration, cachedDuration)
	log.Debug().Msgf("noCacheDuration=%f cachedDuration=%f cacheClearDuration=%f", noCacheDuration, cachedDuration, cacheClearDuration)

	testListRemoteAllBackups := make([]string, totalCacheCount)
	for i := 0; i < totalCacheCount; i++ {
		testListRemoteAllBackups[i] = fmt.Sprintf("%s_%d", testBackupName, i)
	}
	fullCleanup(t, r, env, testListRemoteAllBackups, []string{"remote", "local"}, nil, false, true, true, "config-s3.yml")
	env.Cleanup(t, r)
}
