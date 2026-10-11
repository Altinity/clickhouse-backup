//go:build integration

package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// log fragments of pkg/backup/kafka_keeper.go, adjust together with the implementation
const (
	kafkaKeeperRestoredLog      = "from backup, offsets:"
	kafkaKeeperActiveReplicaLog = "is used by active replicas"
)

var kafkaKeeperPathRE = regexp.MustCompile(`kafka_keeper_path\s*=\s*'([^']+)'`)

// TestKafkaKeeperOffsets https://github.com/Altinity/clickhouse-backup/issues/1120
// Kafka engine tables with kafka_keeper_path (StorageKafka2) keep committed offsets in Keeper,
// backup shall dump them and restore shall put them back, otherwise consumption restarts from the
// consumer group offset (rows lost) or from earliest (duplicates)
func TestKafkaKeeperOffsets(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "24.8") < 0 {
		t.Skipf("kafka_keeper_path requires ClickHouse 24.8+, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 1*time.Second, 1*time.Second, 1*time.Minute)
	r.NoError(env.tc.EnsureKafka(t.Context()))

	const (
		cfg        = "/etc/clickhouse-backup/config-s3.yml"
		topic      = "test_kafka_keeper_offsets"
		dbName     = "test_kafka_keeper"
		dbCopy     = "test_kafka_keeper_copy"
		dbActive   = "test_kafka_keeper_active"
		dbLive     = "test_kafka_keeper_live"
		dbHold     = "test_kafka_keeper_hold"
		backupName = "test_kafka_keeper_backup"
		backupB2   = "test_kafka_keeper_active_backup"
		backupB3   = "test_kafka_keeper_hold_backup"
		n, m, k, h = 100, 50, 30, 20
	)
	env.DockerExecNoError(r, "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists", "--topic", topic, "--partitions", "2")
	produceKafkaMessages(r, env, topic, 1, n)
	// consumer group coordinator (__consumer_offsets) is created lazily, StorageKafka2 stalls for minutes until it exists
	env.DockerExecNoError(r, "kafka", "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092", "--topic", topic, "--group", "test_kafka_keeper_warmup", "--from-beginning", "--max-messages", "1", "--timeout-ms", "60000")

	env.queryWithNoError(t, r, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ON CLUSTER '{cluster}'", dbName))
	createKafkaPipeline(t, r, env, dbName, topic, "test_kafka_keeper_group", "/clickhouse/{database}/kafka/{uuid}", "{replica}")
	waitKafkaTargetCount(t, r, env, dbName, n)

	// backup dumps offsets and re-attaches the Kafka table
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "create", "--tables", dbName+".*", backupName)
	keeperPath, offsets := readKafkaKeeperBackupOffsets(r, env, backupName, dbName, "kafka")
	r.Contains(keeperPath, "/"+dbName+"/kafka/")
	r.Len(offsets, 2, "expect committed offsets for both partitions: %v", offsets)
	var offsetsSum uint64
	for p, o := range offsets {
		r.Greater(o, uint64(0), "partition %s has no committed offset", p)
		offsetsSum += o
	}
	r.Equal(uint64(n), offsetsSum, "committed offsets %v", offsets)
	var tableCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&tableCount, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s' AND name='kafka' SETTINGS empty_result_for_aggregation_by_empty_set=0", dbName)))
	r.Equal(uint64(1), tableCount, "Kafka table shall be attached back after create")

	produceKafkaMessages(r, env, topic, n+1, n+m)
	waitKafkaTargetCount(t, r, env, dbName, n+m)

	// target is restored to N rows, restored offsets make Kafka table consume exactly M rows again
	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore", "--rm", "--tables", dbName+".*", backupName)
	r.NoError(err, out)
	r.Contains(out, kafkaKeeperRestoredLog)
	waitKafkaTargetCount(t, r, env, dbName, n+m)
	r.Empty(kafkaKeeperBackupLocks(r, env, dbName, "kafka"), "combined restore shall release consumption locks")

	// restore into mapped database while the original is live, both pipelines keep their own offsets
	mappedRestore := []string{"clickhouse-backup", "-c", cfg, "restore", "--rm", "--tables", dbName + ".*", "--restore-database-mapping", dbName + ":" + dbCopy, backupName}
	// before 25.6 StorageKafka2 gets partitions via consumer group rebalance, the copy shares kafka_group_name with the live original
	// and would take partitions from it, restore shall fail until the original is detached
	sharedConsumerGroup := compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.6") < 0
	if sharedConsumerGroup {
		out, err = env.DockerExecOut("clickhouse-backup", mappedRestore...)
		r.Error(err, out)
		r.Contains(out, "https://github.com/ClickHouse/ClickHouse/pull/78726")
		env.queryWithNoError(t, r, fmt.Sprintf("DETACH TABLE %s.kafka", dbName))
	}
	out, err = env.DockerExecOut("clickhouse-backup", mappedRestore...)
	r.NoError(err, out)
	r.NotContains(out, kafkaKeeperActiveReplicaLog)
	copyPath := resolvedKafkaKeeperPath(r, env, dbCopy, "kafka")
	r.Contains(copyPath, "/"+dbCopy+"/")
	produceKafkaMessages(r, env, topic, n+m+1, n+m+k)
	if sharedConsumerGroup {
		waitKafkaTargetCount(t, r, env, dbCopy, n+m+k)
		env.queryWithNoError(t, r, fmt.Sprintf("DETACH TABLE %s.kafka", dbCopy))
		env.queryWithNoError(t, r, fmt.Sprintf("ATTACH TABLE %s.kafka", dbName))
	}
	r.NotEqual(resolvedKafkaKeeperPath(r, env, dbName, "kafka"), copyPath)
	waitKafkaTargetCount(t, r, env, dbName, n+m+k)
	if !sharedConsumerGroup {
		waitKafkaTargetCount(t, r, env, dbCopy, n+m+k)
	}

	// keeper path with an active replica of another table, restore shall not overwrite its offsets
	fixedPath := "/clickhouse/test_kafka_keeper_fixed"
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ON CLUSTER '{cluster}'", dbActive))
	createKafkaPipeline(t, r, env, dbActive, topic, "test_kafka_keeper_active_group", fixedPath, "r1")
	waitKafkaTargetCount(t, r, env, dbActive, n+m+k)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "create", "--tables", dbActive+".*", backupB2)
	_, activeOffsets := readKafkaKeeperBackupOffsets(r, env, backupB2, dbActive, "kafka")
	r.Len(activeOffsets, 2)
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ON CLUSTER '{cluster}'", dbLive))
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.kafka ON CLUSTER '{cluster}' (id UInt64) ENGINE=Kafka SETTINGS kafka_broker_list='kafka:9092', kafka_topic_list='%s', kafka_group_name='test_kafka_keeper_active_group', kafka_format='JSONEachRow', kafka_keeper_path='%s', kafka_replica_name='r2' SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1",
		dbLive, topic, fixedPath,
	))
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore", "--rm", "--tables", dbActive+".*", backupB2)
	r.NoError(err, out)
	r.Contains(out, kafkaKeeperActiveReplicaLog)
	waitKafkaTargetCount(t, r, env, dbActive, n+m+k)

	// separate schema and data restore (Altinity ACM flow), consumption locks hold the Kafka table until data is restored,
	// otherwise consumed rows make the Replicated target non-empty and data restore fails
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ON CLUSTER '{cluster}'", dbHold))
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.target ON CLUSTER '{cluster}' (id UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}/{uuid}','{replica}') ORDER BY id", dbHold))
	createKafkaTableAndMV(t, r, env, dbHold, topic, "test_kafka_keeper_hold_group", "/clickhouse/{database}/kafka/{uuid}", "{replica}")
	waitKafkaTargetCount(t, r, env, dbHold, n+m+k)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "create", "--tables", dbHold+".*", backupB3)
	produceKafkaMessages(r, env, topic, n+m+k+1, n+m+k+h)
	waitKafkaTargetCount(t, r, env, dbHold, n+m+k+h)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore", "--schema", "--rm", "--tables", dbHold+".*", backupB3)
	r.ElementsMatch([]string{topic + "_0.lock", topic + "_1.lock"}, kafkaKeeperBackupLocks(r, env, dbHold, "kafka"))
	produceKafkaMessages(r, env, topic, n+m+k+h+1, n+m+k+2*h)
	time.Sleep(10 * time.Second)
	waitKafkaTargetCount(t, r, env, dbHold, 0)
	r.ElementsMatch([]string{topic + "_0.lock", topic + "_1.lock"}, kafkaKeeperBackupLocks(r, env, dbHold, "kafka"))

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore", "--data", "--tables", dbHold+".*", backupB3)
	r.Empty(kafkaKeeperBackupLocks(r, env, dbHold, "kafka"), "data restore shall release consumption locks")
	waitKafkaTargetCount(t, r, env, dbHold, n+m+k+2*h)

	// --skip-kafka-keeper-hold, consumption starts right after schema restore from the backed up offsets
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore", "--schema", "--rm", "--skip-kafka-keeper-hold", "--tables", dbHold+".*", backupB3)
	r.Empty(kafkaKeeperBackupLocks(r, env, dbHold, "kafka"), "--skip-kafka-keeper-hold shall not create consumption locks")
	waitKafkaTargetCount(t, r, env, dbHold, 2*h)

	// backup from ClickHouse before 25.6 has no topic_partition_locks node, restore shall create the skeleton of the current version,
	// otherwise StorageKafka2 fails on getChildren(topic_partition_locks) and never consumes, --skip-kafka-keeper-hold avoids lock parent creation
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.6") >= 0 {
		stripKafkaKeeperBackupNodes(r, env, backupB3, dbHold, "kafka", "topic_partition_locks")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "restore", "--rm", "--skip-kafka-keeper-hold", "--tables", dbHold+".*", backupB3)
		waitKafkaTargetCount(t, r, env, dbHold, n+m+k+2*h)
	}

	// cleanup
	for _, db := range []string{dbName, dbCopy, dbActive, dbLive, dbHold} {
		r.NoError(env.dropDatabase(db, true))
	}
	for _, b := range []string{backupName, backupB2, backupB3} {
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", cfg, "delete", "local", b)
	}
	env.DockerExecNoError(r, "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--delete", "--topic", topic)
}

// produceKafkaMessages writes JSONEachRow {"id":N} for N in [from, to], keyed by id to spread over partitions
func produceKafkaMessages(r *require.Assertions, env *TestEnvironment, topic string, from, to int) {
	env.DockerExecNoError(r, "kafka", "bash", "-ec", fmt.Sprintf(
		`seq %d %d | sed 's/.*/&|{"id":&}/' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic %s --property parse.key=true --property key.separator='|'`,
		from, to, topic,
	))
}

func createKafkaPipeline(t *testing.T, r *require.Assertions, env *TestEnvironment, db, topic, group, keeperPath, replicaName string) {
	// {uuid} in kafka_keeper_path is allowed only for ON CLUSTER queries
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.target ON CLUSTER '{cluster}' (id UInt64) ENGINE=MergeTree ORDER BY id", db))
	createKafkaTableAndMV(t, r, env, db, topic, group, keeperPath, replicaName)
}

func createKafkaTableAndMV(t *testing.T, r *require.Assertions, env *TestEnvironment, db, topic, group, keeperPath, replicaName string) {
	env.queryWithNoError(t, r, fmt.Sprintf(
		"CREATE TABLE %s.kafka ON CLUSTER '{cluster}' (id UInt64) ENGINE=Kafka SETTINGS kafka_broker_list='kafka:9092', kafka_topic_list='%s', kafka_group_name='%s', kafka_format='JSONEachRow', kafka_keeper_path='%s', kafka_replica_name='%s' SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1",
		db, topic, group, keeperPath, replicaName,
	))
	env.queryWithNoError(t, r, fmt.Sprintf("CREATE MATERIALIZED VIEW %s.mv ON CLUSTER '{cluster}' TO %s.target AS SELECT id FROM %s.kafka", db, db, db))
}

// waitKafkaTargetCount waits until db.target contains exactly expected unique rows and stays stable
func waitKafkaTargetCount(t *testing.T, r *require.Assertions, env *TestEnvironment, db string, expected int) {
	query := fmt.Sprintf("SELECT count() AS cnt, uniqExact(id) AS uniq FROM %s.target", db)
	type countRow struct {
		Cnt  uint64 `ch:"cnt"`
		Uniq uint64 `ch:"uniq"`
	}
	selectCount := func() countRow {
		var rows []countRow
		r.NoError(env.ch.Select(&rows, query))
		r.Len(rows, 1)
		return rows[0]
	}
	deadline := time.Now().Add(3 * time.Minute)
	var row countRow
	for time.Now().Before(deadline) {
		if row = selectCount(); row.Cnt >= uint64(expected) {
			break
		}
		time.Sleep(time.Second)
	}
	r.Equal(uint64(expected), row.Cnt, "%s.target count, last %+v", db, row)
	// more rows could arrive, when offsets point before the restored data
	time.Sleep(5 * time.Second)
	row = selectCount()
	r.Equal(uint64(expected), row.Cnt, "%s.target count is not stable", db)
	r.Equal(row.Cnt, row.Uniq, "%s.target contains duplicates", db)
	t.Logf("%s.target reached %d rows", db, expected)
}

// readKafkaKeeperBackupOffsets returns kafka_keeper.path and committed offset per partition from local backup table metadata
func readKafkaKeeperBackupOffsets(r *require.Assertions, env *TestEnvironment, backupName, db, table string) (string, map[string]uint64) {
	out, err := env.DockerExecOut("clickhouse-backup", "cat", fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata/%s/%s.json", backupName, db, table))
	r.NoError(err, out)
	var tm struct {
		KafkaKeeper *struct {
			Path  string `json:"path"`
			Nodes []struct {
				Path  string `json:"path"`
				Value string `json:"value"`
			} `json:"nodes"`
		} `json:"kafka_keeper"`
	}
	r.NoError(json.Unmarshal([]byte(out), &tm), out)
	r.NotNil(tm.KafkaKeeper, "kafka_keeper not found in %s", out)
	offsets := map[string]uint64{}
	for _, node := range tm.KafkaKeeper.Nodes {
		if !strings.HasSuffix(node.Path, "/committed") {
			continue
		}
		value := node.Value
		if _, parseErr := strconv.ParseUint(value, 10, 64); parseErr != nil {
			decoded, decodeErr := base64.StdEncoding.DecodeString(value)
			r.NoError(decodeErr, "unexpected committed value %q in %s", value, node.Path)
			value = string(decoded)
		}
		offset, parseErr := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		r.NoError(parseErr, "unexpected committed value %q in %s", value, node.Path)
		offsets[strings.TrimSuffix(node.Path, "/committed")] = offset
	}
	return tm.KafkaKeeper.Path, offsets
}

// stripKafkaKeeperBackupNodes removes kafka_keeper nodes with prefix from local backup table metadata, emulates backup from another ClickHouse version
func stripKafkaKeeperBackupNodes(r *require.Assertions, env *TestEnvironment, backupName, db, table, prefix string) {
	metadataFile := fmt.Sprintf("/var/lib/clickhouse/backup/%s/metadata/%s/%s.json", backupName, db, table)
	out, err := env.DockerExecOut("clickhouse-backup", "cat", metadataFile)
	r.NoError(err, out)
	var tm map[string]interface{}
	r.NoError(json.Unmarshal([]byte(out), &tm), out)
	kafkaKeeper, ok := tm["kafka_keeper"].(map[string]interface{})
	r.True(ok, "kafka_keeper not found in %s", out)
	nodes, _ := kafkaKeeper["nodes"].([]interface{})
	keptNodes := make([]interface{}, 0, len(nodes))
	for _, node := range nodes {
		if nodePath, _ := node.(map[string]interface{})["path"].(string); nodePath == prefix || strings.HasPrefix(nodePath, prefix+"/") {
			continue
		}
		keptNodes = append(keptNodes, node)
	}
	r.Less(len(keptNodes), len(nodes), "%s not found in kafka_keeper nodes of %s", prefix, metadataFile)
	kafkaKeeper["nodes"] = keptNodes
	body, err := json.Marshal(tm)
	r.NoError(err)
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ec", fmt.Sprintf("echo %s | base64 -d > %s", base64.StdEncoding.EncodeToString(body), metadataFile))
}

// resolvedKafkaKeeperPath returns kafka_keeper_path of the live table with {uuid} replaced by table UUID
func resolvedKafkaKeeperPath(r *require.Assertions, env *TestEnvironment, db, table string) string {
	var rows []struct {
		EngineFull string `ch:"engine_full"`
		UUID       string `ch:"uuid"`
	}
	r.NoError(env.ch.Select(&rows, fmt.Sprintf("SELECT engine_full, toString(uuid) AS uuid FROM system.tables WHERE database='%s' AND name='%s'", db, table)))
	r.Len(rows, 1)
	matches := kafkaKeeperPathRE.FindStringSubmatch(strings.ReplaceAll(rows[0].EngineFull, `\'`, `'`))
	r.Len(matches, 2, "kafka_keeper_path not found in %s", rows[0].EngineFull)
	return strings.ReplaceAll(matches[1], "{uuid}", rows[0].UUID)
}

// kafkaKeeperBackupLocks returns names of consumption locks placed by clickhouse-backup restore under kafka_keeper_path of the live table
func kafkaKeeperBackupLocks(r *require.Assertions, env *TestEnvironment, db, table string) []string {
	type zkNode struct {
		Name string `ch:"name"`
	}
	keeperPath := resolvedKafkaKeeperPath(r, env, db, table)
	names := make([]string, 0)
	// before 25.6 StorageKafka2 locks partitions via topics/<topic>/partitions/<n>/lock, return them as <topic>_<n>.lock
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.6") < 0 {
		var topics []zkNode
		r.NoError(env.ch.Select(&topics, fmt.Sprintf("SELECT name FROM system.zookeeper WHERE path='%s/topics'", keeperPath)))
		for _, topic := range topics {
			partitionsPath := fmt.Sprintf("%s/topics/%s/partitions", keeperPath, topic.Name)
			var partitions []zkNode
			r.NoError(env.ch.Select(&partitions, fmt.Sprintf("SELECT name FROM system.zookeeper WHERE path='%s'", partitionsPath)))
			for _, partition := range partitions {
				var locks []zkNode
				r.NoError(env.ch.Select(&locks, fmt.Sprintf("SELECT name FROM system.zookeeper WHERE path='%s/%s' AND name='lock' AND value='clickhouse-backup'", partitionsPath, partition.Name)))
				if len(locks) > 0 {
					names = append(names, topic.Name+"_"+partition.Name+".lock")
				}
			}
		}
		return names
	}
	var locks []zkNode
	r.NoError(env.ch.Select(&locks, fmt.Sprintf("SELECT name FROM system.zookeeper WHERE path='%s/topic_partition_locks' AND value='clickhouse-backup'", keeperPath)))
	for _, lock := range locks {
		names = append(names, lock.Name)
	}
	return names
}
