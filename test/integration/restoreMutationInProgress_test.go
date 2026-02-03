//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/rs/zerolog/log"
)

func TestRestoreMutationInProgress(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	zkPath := "/clickhouse/tables/{shard}/" + t.Name() + "/test_restore_mutation_in_progress"
	onCluster := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		zkPath = "/clickhouse/tables/{shard}/{database}/{table}"
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.3") >= 0 {
		zkPath = "/clickhouse/tables/{shard}/{database}/{table}/{uuid}"
		onCluster = " ON CLUSTER '{cluster}'"
	}
	createDbSQL := "CREATE DATABASE IF NOT EXISTS " + t.Name()
	env.queryWithNoError(r, createDbSQL)
	version, err := env.ch.GetVersion(t.Context())
	r.NoError(err)

	createSQL := fmt.Sprintf("CREATE TABLE %s.test_restore_mutation_in_progress %s (id UInt64, attr String) ENGINE=ReplicatedMergeTree('%s','{replica}') PARTITION BY id ORDER BY id", t.Name(), onCluster, zkPath)
	env.queryWithNoError(r, createSQL)
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_restore_mutation_in_progress SELECT number, if(number>0,'a',toString(number)) FROM numbers(2)")

	mutationSQL := "ALTER TABLE " + t.Name() + ".test_restore_mutation_in_progress MODIFY COLUMN attr UInt64"
	err = env.ch.QueryContext(t.Context(), mutationSQL)
	if err != nil {
		errStr := strings.ToLower(err.Error())
		r.True(strings.Contains(errStr, "code: 341") || strings.Contains(errStr, "code: 517") || strings.Contains(errStr, "timeout"), "UNKNOWN ERROR: %s", err.Error())
		log.Debug().Msgf("%s RETURN EXPECTED ERROR=%#v", mutationSQL, err)
	}

	attrs := make([]struct {
		Attr uint64 `ch:"attr"`
	}, 0)
	err = env.ch.Select(&attrs, "SELECT attr FROM "+t.Name()+".test_restore_mutation_in_progress ORDER BY id")
	r.NotEqual(nil, err)
	errStr := strings.ToLower(err.Error())
	r.True(strings.Contains(errStr, "code: 53") || strings.Contains(errStr, "code: 6"))
	r.Zero(len(attrs))

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		mutationSQL = "ALTER TABLE " + t.Name() + ".test_restore_mutation_in_progress RENAME COLUMN attr TO attr_1"
		err = env.ch.QueryContext(t.Context(), mutationSQL)
		r.NotEqual(nil, err)
		errStr = strings.ToLower(err.Error())
		r.True(strings.Contains(errStr, "code: 36,") || strings.Contains(errStr, "code: 517") || strings.Contains(errStr, "timeout"), "%s return UNEXPECTED ERROR=%s", mutationSQL, errStr)
		log.Debug().Msgf("%s RETURN EXPECTED ERROR=%#v", mutationSQL, err)
	}
	env.DockerExecNoError(r, "clickhouse", "clickhouse", "client", "-q", "SELECT * FROM system.mutations WHERE is_done=0 FORMAT Vertical")

	// backup with check consistency
	out, createErr := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+t.Name()+".test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
	r.NotEqual(createErr, nil)
	r.Contains(out, "have inconsistent data types")
	log.Debug().Msg(out)

	// backup without check consistency
	out, createErr = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "create", "-c", "/etc/clickhouse-backup/config-s3.yml", "--skip-check-parts-columns", "--tables="+t.Name()+".test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
	log.Debug().Msg(out)
	r.NoError(createErr)
	r.NotContains(out, "have inconsistent data types")

	r.NoError(env.ch.DropOrDetachTable(clickhouse.Table{Database: t.Name(), Name: "test_restore_mutation_in_progress"}, "", "", false, version, "", false, ""))
	var restoreErr error
	restoreErr = env.DockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", "--tables="+t.Name()+".test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.8") < 0 {
		r.NotEqual(restoreErr, nil)
	} else {
		r.NoError(restoreErr)
	}

	attrs = make([]struct {
		Attr uint64 `ch:"attr"`
	}, 0)
	checkRestoredData := "attr"
	if restoreErr == nil {
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
			checkRestoredData = "attr_1 AS attr"
		}
	}
	selectSQL := fmt.Sprintf("SELECT %s FROM "+t.Name()+".test_restore_mutation_in_progress ORDER BY id", checkRestoredData)
	selectErr := env.ch.Select(&attrs, selectSQL)
	expectedSelectResults := make([]struct {
		Attr uint64 `ch:"attr"`
	}, 1)
	expectedSelectResults[0].Attr = 0

	expectedSelectError := "code: 517"

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") < 0 {
		expectedSelectResults = make([]struct {
			Attr uint64 `ch:"attr"`
		}, 2)
		expectedSelectError = ""
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.8") < 0 {
		expectedSelectError = ""
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.8") >= 0 {
		expectedSelectError = "code: 6"
		expectedSelectResults = make([]struct {
			Attr uint64 `ch:"attr"`
		}, 0)
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.8") >= 0 {
		expectedSelectError = "code: 47"
		expectedSelectResults = make([]struct {
			Attr uint64 `ch:"attr"`
		}, 0)
	}
	r.Equal(expectedSelectResults, attrs)
	if expectedSelectError != "" {
		r.Error(selectErr)
		r.Contains(strings.ToLower(selectErr.Error()), expectedSelectError)
		log.Debug().Msgf("%s RETURN EXPECTED ERROR=%#v", selectSQL, selectErr)
	} else {
		r.NoError(selectErr)
	}

	env.DockerExecNoError(r, "clickhouse", "clickhouse", "client", "-q", "SELECT * FROM system.mutations FORMAT Vertical")

	r.NoError(env.ch.DropOrDetachTable(clickhouse.Table{Database: t.Name(), Name: "test_restore_mutation_in_progress"}, "", "", false, version, "", false, ""))
	r.NoError(env.dropDatabase(t.Name(), false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_restore_mutation_in_progress")
	env.Cleanup(t, r)
}
