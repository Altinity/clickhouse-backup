//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestChangeReplicationPathIfReplicaExists(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
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
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
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
}

// TestRebindReplicaPathIfExists covers replica-path rebind decisions in checkReplicaAlreadyExistsAndChangeReplicationPath
// (https://github.com/Altinity/clickhouse-backup/issues/849 and https://github.com/Altinity/clickhouse-backup/issues/1428).
//
// When our own replica entry is absent but the resolved ZK path still has children, three outcomes are possible:
//
//	A) a different LOCAL table already uses that path (RENAME / path reuse on this node) => auto-rebind, HA-safe
//	   because a real HA sibling lives on another node and is therefore absent from node-local system.replicas;
//	B) no local table uses the path and the flag is off => join (the HA-safe default for a remote sibling);
//	C) no local table uses the path and rebind_replica_path_if_exists is on => rebind (purely-stale opt-in).
//
// Single-node infra can't host a real second node, so the "remote sibling / stale" state (cases B, C) is simulated
// by DETACH-ing the occupant: DETACH drops it from system.replicas but keeps its ZK path/replica entry alive.
func TestRebindReplicaPathIfExists(t *testing.T) {
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	// `DROP TABLE ... NO DELAY` (synchronous drop) is available from 20.8; older versions use a plain DROP.
	dropSuffix := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		dropSuffix = " NO DELAY"
	}

	const sharedPath = "/clickhouse/tables/rebind_path"
	// occupant holds sharedPath under a different replica name. While ATTACHED it is a local table visible in
	// system.replicas (case A); DETACH-ing it later keeps the ZK path alive but drops it from system.replicas
	// (cases B, C), simulating a remote sibling / stale leftovers.
	env.queryWithNoError(r, fmt.Sprintf("CREATE TABLE default.rebind_occupant (id UInt64) ENGINE=ReplicatedMergeTree('%s','other-replica') ORDER BY id", sharedPath))
	createSQL := fmt.Sprintf("CREATE TABLE default.test_rebind_path (id UInt64) ENGINE=ReplicatedMergeTree('%s','{replica}') ORDER BY id", sharedPath)
	env.queryWithNoError(r, createSQL)

	r.NoError(env.DockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables", "default.test_rebind_path", "test_rebind_backup"))

	engineFull := func() string {
		out := ""
		r.NoError(env.ch.SelectSingleRowNoCtx(&out, "SELECT engine_full FROM system.tables WHERE database=? AND name=?", "default", "test_rebind_path"))
		return out
	}
	expectedDefaultPath := "/clickhouse/tables/{cluster}/{shard}/{database}/{table}"
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") < 0 || compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.7") > 0 {
		expectedDefaultPath = strings.NewReplacer("{database}", "default", "{table}", "test_rebind_path").Replace(expectedDefaultPath)
	}

	restoreSchema := func(extraArgs ...string) string {
		args := []string{"clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--schema", "--tables", "default.test_rebind_path"}
		args = append(args, extraArgs...)
		args = append(args, "test_rebind_backup")
		out, err := env.DockerExecOut("clickhouse-backup", args...)
		level := zerolog.DebugLevel
		if err != nil {
			level = zerolog.InfoLevel
		}
		log.WithLevel(level).Msg(out)
		r.NoError(err)
		return out
	}

	// Case A: a different local table (occupant, still attached) uses sharedPath => auto-rebind even without the flag.
	env.queryWithNoError(r, "DROP TABLE default.test_rebind_path"+dropSuffix)
	autoOut := restoreSchema()
	r.Contains(autoOut, "is already used by")
	r.Contains(autoOut, "will rebind to fresh replica path")
	r.Contains(engineFull(), expectedDefaultPath)

	// DETACH the occupant: it leaves system.replicas but its ZK path/replica entry stays => no local table at
	// sharedPath, simulating a remote HA sibling / stale leftovers for cases B and C.
	env.queryWithNoError(r, "DETACH TABLE default.rebind_occupant")

	// Case B: no local table at sharedPath, flag off => join the existing path, no rebind (HA-safe default).
	env.queryWithNoError(r, "DROP TABLE default.test_rebind_path"+dropSuffix)
	joinOut := restoreSchema()
	r.NotContains(joinOut, "will rebind to fresh replica path")
	r.Contains(engineFull(), sharedPath)

	// Case C: no local table at sharedPath, --rebind-replica-path-if-exists => rebind to default_replica_path.
	env.queryWithNoError(r, "DROP TABLE default.test_rebind_path"+dropSuffix)
	flagOut := restoreSchema("--rebind-replica-path-if-exists")
	r.Contains(flagOut, "rebind_replica_path_if_exists=true")
	r.Contains(flagOut, "will rebind to fresh replica path")
	r.Contains(engineFull(), expectedDefaultPath)

	// cleanup: re-attach the occupant so DROP ... SYNC can deregister its ZK entry; dropping the last replica of
	// a path also removes the table-level znode.
	env.queryWithNoError(r, "DROP TABLE default.test_rebind_path"+dropSuffix)
	env.queryWithNoError(r, "ATTACH TABLE default.rebind_occupant")
	env.queryWithNoError(r, "DROP TABLE default.rebind_occupant"+dropSuffix)
	r.NoError(env.DockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_rebind_backup"))
}
