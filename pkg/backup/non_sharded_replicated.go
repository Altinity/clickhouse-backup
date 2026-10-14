package backup

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// replicatedEngineZookeeperPathRE captures the engine and the first quoted argument of Replicated*MergeTree,
// the second group is empty for the short syntax without explicit zookeeper path, e.g. `ReplicatedMergeTree`,
// `ReplicatedMergeTree()` or `ReplicatedReplacingMergeTree(ver)`
var replicatedEngineZookeeperPathRE = regexp.MustCompile(`ENGINE\s*=\s*(Replicated\w*MergeTree)\b(?:\s*\(\s*(?:'((?:[^'\\]|\\.)*)')?)?`)

var macroRE = regexp.MustCompile(`\{[^{}]*}`)

// nonShardedReplicatedInvariantMacros have the same value on every node of one cluster for the same table,
// {uuid} is not here: tables created without ON CLUSTER have different UUIDs on different nodes
var nonShardedReplicatedInvariantMacros = map[string]struct{}{
	"{database}": {},
	"{table}":    {},
	"{cluster}":  {},
}

// replicatedZookeeperPath returns the zookeeper path of the Replicated*MergeTree engine from the CREATE query,
// defaultReplicaPath is used for the short syntax, ok=false for any other engine
func replicatedZookeeperPath(query string, defaultReplicaPath func() (string, error)) (string, bool, error) {
	matches := replicatedEngineZookeeperPathRE.FindStringSubmatch(query)
	if len(matches) == 0 {
		return "", false, nil
	}
	if matches[2] != "" {
		return matches[2], true, nil
	}
	zkPath, err := defaultReplicaPath()
	if err != nil {
		return "", false, err
	}
	return zkPath, true, nil
}

// isNonShardedReplicatedPath reports whether the zookeeper path resolves to the same value on every node of the cluster,
// so all replicas of all shards share one replication queue, any macro which may differ between nodes,
// like {shard}, {replica}, {uuid} or a custom one, makes the table sharded
func isNonShardedReplicatedPath(zkPath string) bool {
	if zkPath == "" {
		return false
	}
	for _, macro := range macroRE.FindAllString(zkPath, -1) {
		if _, isInvariant := nonShardedReplicatedInvariantMacros[macro]; !isInvariant {
			return false
		}
	}
	return true
}

// nonShardedReplicatedTables returns the replicated tables with one zookeeper path on the whole cluster,
// the classification uses only the CREATE query from the backup, the live cluster state is not read
func (b *Backuper) nonShardedReplicatedTables(ctx context.Context, tables ListOfTables) ([]metadata.TableTitle, error) {
	defaultReplicaPath := ""
	defaultReplicaPathResolved := false
	getDefaultReplicaPath := func() (string, error) {
		if defaultReplicaPathResolved {
			return defaultReplicaPath, nil
		}
		settingsValues, err := b.ch.GetSettingsValues(ctx, []interface{}{"default_replica_path"})
		if err != nil {
			return "", errors.Wrap(err, "can't get default_replica_path from system.settings")
		}
		defaultReplicaPath = settingsValues["default_replica_path"]
		defaultReplicaPathResolved = true
		return defaultReplicaPath, nil
	}
	var nonSharded []metadata.TableTitle
	for _, table := range tables {
		zkPath, isReplicated, err := replicatedZookeeperPath(table.Query, getDefaultReplicaPath)
		if err != nil {
			return nil, err
		}
		if isReplicated && isNonShardedReplicatedPath(zkPath) {
			nonSharded = append(nonSharded, metadata.TableTitle{Database: table.Database, Table: table.Table})
		}
	}
	return nonSharded, nil
}

// clusterReplica is one row of system.clusters
type clusterReplica struct {
	ShardNum   uint32 `ch:"shard_num"`
	ReplicaNum uint32 `ch:"replica_num"`
	HostName   string `ch:"host_name"`
	Port       uint16 `ch:"port"`
	IsLocal    uint8  `ch:"is_local"`
}

func (r clusterReplica) String() string {
	return fmt.Sprintf("%s:%d (shard_num=%d, replica_num=%d)", r.HostName, r.Port, r.ShardNum, r.ReplicaNum)
}

// nonShardedReplicatedOwner picks the first replica of the first shard as the node which restores the data,
// replicas ordered by shard_num, replica_num, the result depends only on the cluster configuration
func nonShardedReplicatedOwner(cluster string, replicas []clusterReplica) (clusterReplica, bool, error) {
	if len(replicas) == 0 {
		return clusterReplica{}, false, errors.Errorf("cluster '%s' not found in system.clusters", cluster)
	}
	sort.SliceStable(replicas, func(i, j int) bool {
		if replicas[i].ShardNum != replicas[j].ShardNum {
			return replicas[i].ShardNum < replicas[j].ShardNum
		}
		return replicas[i].ReplicaNum < replicas[j].ReplicaNum
	})
	isInCluster := false
	for _, r := range replicas {
		if r.IsLocal != 0 {
			isInCluster = true
			break
		}
	}
	if !isInCluster {
		return clusterReplica{}, false, errors.Errorf("current node is not a replica of cluster '%s' in system.clusters", cluster)
	}
	return replicas[0], replicas[0].IsLocal != 0, nil
}

// getNonShardedReplicatedTablesSkipData returns the tables whose data shall not be restored on this node:
// replicated tables with one zookeeper path on the whole cluster when this node is not the first replica of the first shard
// of skip_check_non_sharded_replicated_tables_cluster, their schema is restored and the data is replicated from the node
// which restores it, https://github.com/Altinity/clickhouse-backup/issues/1181
func (b *Backuper) getNonShardedReplicatedTablesSkipData(ctx context.Context, tables ListOfTables) (map[metadata.TableTitle]struct{}, error) {
	if !b.cfg.ClickHouse.SkipRestoreNonShardedReplicatedTables {
		return nil, nil
	}
	nonSharded, err := b.nonShardedReplicatedTables(ctx, tables)
	if err != nil {
		return nil, errors.Wrap(err, "skip_restore_non_sharded_replicated_tables")
	}
	if len(nonSharded) == 0 {
		return nil, nil
	}
	clusterName := strings.TrimSpace(b.cfg.ClickHouse.SkipCheckNonShardedReplicatedTablesCluster)
	if clusterName == "" {
		return nil, errors.New("skip_restore_non_sharded_replicated_tables: skip_check_non_sharded_replicated_tables_cluster is empty")
	}
	cluster, err := b.ch.ApplyMacros(ctx, clusterName)
	if err != nil {
		return nil, errors.Wrapf(err, "skip_restore_non_sharded_replicated_tables: can't apply macros to skip_check_non_sharded_replicated_tables_cluster=%s", clusterName)
	}
	var replicas []clusterReplica
	if err = b.ch.SelectContext(ctx, &replicas, "SELECT shard_num, replica_num, host_name, port, is_local FROM system.clusters WHERE cluster=?", cluster); err != nil {
		return nil, errors.Wrap(err, "skip_restore_non_sharded_replicated_tables: can't read system.clusters")
	}
	owner, isOwner, err := nonShardedReplicatedOwner(cluster, replicas)
	if err != nil {
		return nil, errors.Wrap(err, "skip_restore_non_sharded_replicated_tables")
	}
	names := make([]string, len(nonSharded))
	for i, t := range nonSharded {
		names[i] = fmt.Sprintf("`%s`.`%s`", t.Database, t.Table)
	}
	if isOwner {
		log.Info().Msgf("skip_restore_non_sharded_replicated_tables: current node is the first replica of the first shard of cluster '%s', restore data of %d non-sharded replicated tables: %s", cluster, len(nonSharded), strings.Join(names, ", "))
		return nil, nil
	}
	skip := make(map[metadata.TableTitle]struct{}, len(nonSharded))
	for _, t := range nonSharded {
		skip[t] = struct{}{}
	}
	log.Warn().Msgf("skip_restore_non_sharded_replicated_tables: skip data restore of %d non-sharded replicated tables, only schema is restored, data shall be restored on %s of cluster '%s' and is replicated from there: %s", len(nonSharded), owner, cluster, strings.Join(names, ", "))
	return skip, nil
}

// getNonShardedReplicatedTablesSkipDataByPattern resolves the restore table list the same way as restorePrologue does later,
// errors of the table list itself are reported by restorePrologue
func (b *Backuper) getNonShardedReplicatedTablesSkipDataByPattern(ctx context.Context, backupName, metadataPath, tablePattern string, dropExists bool, partitions []string) (map[metadata.TableTitle]struct{}, error) {
	if !b.cfg.ClickHouse.SkipRestoreNonShardedReplicatedTables {
		return nil, nil
	}
	if tablePattern == "" {
		tablePattern = "*"
	}
	tables, _, err := b.getTablesForRestoreLocal(ctx, backupName, metadataPath, tablePattern, dropExists, partitions)
	if err != nil {
		log.Debug().Msgf("skip_restore_non_sharded_replicated_tables: can't get tables for restore: %v", err)
		return nil, nil
	}
	return b.getNonShardedReplicatedTablesSkipData(ctx, tables)
}

// excludeTables returns a new list without the skipped tables, the order is kept and the input is not modified
func excludeTables(tables ListOfTables, skip map[metadata.TableTitle]struct{}) ListOfTables {
	if len(skip) == 0 {
		return tables
	}
	result := make(ListOfTables, 0, len(tables))
	for _, t := range tables {
		if _, isSkipped := skip[metadata.TableTitle{Database: t.Database, Table: t.Table}]; !isSkipped {
			result = append(result, t)
		}
	}
	return result
}
