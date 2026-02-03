//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestChangeReplicationPathIfReplicaExists(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	version, err := env.ch.GetVersion(t.Context())
	r.NoError(err)
	createReplicatedTable := func(table, uuid, engine string) string {
		createSQL := fmt.Sprintf("CREATE TABLE default.%s %s ON CLUSTER '{cluster}' (id UInt64) ENGINE=ReplicatedMergeTree(%s) ORDER BY id", table, uuid, engine)
		env.queryWithNoError(r, createSQL)
		env.queryWithNoError(r, fmt.Sprintf("INSERT INTO default.%s SELECT number FROM numbers(10)", table))
		return createSQL
	}
	createUUID := uuid.New()
	createWithUUIDSQL := ""
	createSQL := createReplicatedTable("test_replica_wrong_path", "", "'/clickhouse/tables/wrong_path','{replica}'")
	minimalChVersionWithNonBugUUID := "21.3"
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), minimalChVersionWithNonBugUUID) >= 0 {
		createWithUUIDSQL = createReplicatedTable("test_replica_wrong_path_uuid", fmt.Sprintf(" UUID '%s' ", createUUID.String()), "")
	}

	r.NoError(env.DockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables", "default.test_replica_wrong_path*", "test_wrong_path"))

	r.NoError(env.ch.DropOrDetachTable(clickhouse.Table{Database: "default", Name: "test_replica_wrong_path"}, createSQL, "", false, version, "", false, ""))
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), minimalChVersionWithNonBugUUID) >= 0 {
		r.NoError(env.ch.DropOrDetachTable(clickhouse.Table{Database: "default", Name: "test_replica_wrong_path_uuid"}, createWithUUIDSQL, "", false, version, "", false, ""))
	}
	// hack for drop tables without drop data from keeper
	_ = createReplicatedTable("test_replica_wrong_path2", "", "'/clickhouse/tables/wrong_path','{replica}'")
	r.NoError(env.DockerExec("clickhouse", "rm", "-fv", "/var/lib/clickhouse/metadata/default/test_replica_wrong_path2.sql"))
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), minimalChVersionWithNonBugUUID) >= 0 {
		_ = createReplicatedTable("test_replica_wrong_path_uuid2", fmt.Sprintf(" UUID '%s' ", createUUID.String()), "")
		r.NoError(env.DockerExec("clickhouse", "rm", "-fv", "/var/lib/clickhouse/metadata/default/test_replica_wrong_path_uuid2.sql"))
		r.NoError(env.DockerExec("clickhouse", "rm", "-rfv", fmt.Sprintf("/var/lib/clickhouse/store/%s/%s", createUUID.String()[:3], createUUID.String())))
	}
	env.ch.Close()
	r.NoError(utils.ExecCmd(t.Context(), 180*time.Second, "docker", append(env.GetDefaultComposeCommand(), "restart", "clickhouse")...))
	env.connectWithWait(t, r, 10*time.Second, 1*time.Second, 1*time.Minute)

	var restoreOut string
	restoreOut, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--tables", "default.test_replica_wrong_path*", "test_wrong_path")
	level := zerolog.DebugLevel
	if err != nil {
		level = zerolog.InfoLevel
	}
	log.WithLevel(level).Msg(restoreOut)
	r.NoError(err)
	r.Contains(restoreOut, "replica /clickhouse/tables/wrong_path/replicas/clickhouse already exists in system.zookeeper will replace to /clickhouse/tables/{cluster}/{shard}/{database}/{table}/replicas/{replica}")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), minimalChVersionWithNonBugUUID) >= 0 {
		r.Contains(restoreOut, fmt.Sprintf("replica /clickhouse/tables/%s/0/replicas/clickhouse already exists in system.zookeeper will replace to /clickhouse/tables/{cluster}/{shard}/{database}/{table}/replicas/{replica}", createUUID.String()))
	}
	checkRestoredTable := func(table string, expectedRows uint64, expectedEngine string) {
		rows := uint64(0)
		r.NoError(env.ch.SelectSingleRowNoCtx(&rows, fmt.Sprintf("SELECT count() FROM default.%s", table)))
		r.Equal(expectedRows, rows)

		engineFull := ""
		//engine_full behavior for different clickhouse version
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") < 0 || compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.7") > 0 {
			expectedEngine = strings.NewReplacer("{database}", "default", "{table}", table).Replace(expectedEngine)
		}
		r.NoError(env.ch.SelectSingleRowNoCtx(&engineFull, "SELECT engine_full FROM system.tables WHERE database=? AND name=?", "default", table))
		r.Contains(engineFull, expectedEngine)

	}
	expectedEngine := "/clickhouse/tables/{cluster}/{shard}/{database}/{table}"
	checkRestoredTable("test_replica_wrong_path", 10, expectedEngine)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		env.queryWithNoError(r, "SYSTEM DROP REPLICA '{replica}' FROM ZKPATH '/clickhouse/tables/wrong_path'")
	}

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), minimalChVersionWithNonBugUUID) >= 0 {
		checkRestoredTable("test_replica_wrong_path_uuid", 10, expectedEngine)
		env.queryWithNoError(r, fmt.Sprintf("SYSTEM DROP REPLICA '{replica}' FROM ZKPATH '/clickhouse/tables/%s/0'", createUUID.String()))
	}

	r.NoError(env.ch.DropOrDetachTable(clickhouse.Table{Database: "default", Name: "test_replica_wrong_path"}, createSQL, "", false, version, "", false, ""))
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), minimalChVersionWithNonBugUUID) >= 0 {
		r.NoError(env.ch.DropOrDetachTable(clickhouse.Table{Database: "default", Name: "test_replica_wrong_path_uuid"}, createWithUUIDSQL, "", false, version, "", false, ""))
	}

	r.NoError(env.DockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_wrong_path"))
	env.Cleanup(t, r)
}
