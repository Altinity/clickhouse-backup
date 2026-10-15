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

var macroRE = regexp.MustCompile(`\{([^{}]*)}`)

// tableIdentityMacros are resolved from the table itself and are the same for one table on every node,
// restore replaces {uuid} with the table UUID from the backup, a table created ON CLUSTER has one UUID on all shards
var tableIdentityMacros = map[string]struct{}{
	"database": {},
	"table":    {},
	"uuid":     {},
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

// pathClusterMacros returns the macros of the zookeeper path which are resolved from system.macros of each node,
// their values may differ between shards, a path without them is the same on every node of the cluster
func pathClusterMacros(zkPath string) []string {
	var macros []string
	for _, m := range macroRE.FindAllStringSubmatch(zkPath, -1) {
		if _, isIdentity := tableIdentityMacros[m[1]]; !isIdentity {
			macros = append(macros, m[1])
		}
	}
	return macros
}

// shardMacros holds system.macros of one replica of each shard, host -> macro -> substitution
type shardMacros map[string]map[string]string

// isNonShardedReplicatedPath reports whether the zookeeper path resolves to the same value on every shard,
// so all replicas of all shards share one replication queue, the macro names don't matter, only their values
func isNonShardedReplicatedPath(zkPath string, macros shardMacros) (bool, error) {
	clusterMacros := pathClusterMacros(zkPath)
	if len(clusterMacros) == 0 {
		return true, nil
	}
	hosts := make([]string, 0, len(macros))
	for host := range macros {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	resolvedPaths := map[string]struct{}{}
	for _, host := range hosts {
		resolved := zkPath
		for _, macro := range clusterMacros {
			value, isDefined := macros[host][macro]
			if !isDefined {
				return false, errors.Errorf("macro {%s} from zookeeper path %s is not defined in system.macros on %s", macro, zkPath, host)
			}
			resolved = strings.ReplaceAll(resolved, "{"+macro+"}", value)
		}
		resolvedPaths[resolved] = struct{}{}
	}
	return len(resolvedPaths) == 1, nil
}

// shardedMacrosWithSameValues returns the macros from always_sharded_macros used in the
// zookeeper paths which have the same value on every shard, such a macro doesn't shard anything on this cluster
func shardedMacrosWithSameValues(declared []string, used map[string]struct{}, macros shardMacros) []string {
	var result []string
	for _, macro := range declared {
		macro = strings.Trim(strings.TrimSpace(macro), "{}")
		if _, isUsed := used[macro]; !isUsed {
			continue
		}
		values := map[string]struct{}{}
		for _, hostMacros := range macros {
			values[hostMacros[macro]] = struct{}{}
		}
		if len(values) == 1 {
			result = append(result, "{"+macro+"}")
		}
	}
	return result
}

// getShardMacros reads system.macros from one replica of each shard, cluster() fails over between replicas of a shard
// and fails when all replicas of a shard are unavailable
func (b *Backuper) getShardMacros(ctx context.Context, cluster string) (shardMacros, error) {
	var hosts []struct {
		Host string `ch:"host"`
	}
	if err := b.ch.SelectContext(ctx, &hosts, fmt.Sprintf("SELECT hostName() AS host FROM cluster(%s, system.one)", chQuote(cluster))); err != nil {
		return nil, errors.Wrapf(err, "can't read hosts of cluster '%s'", cluster)
	}
	var rows []struct {
		Host         string `ch:"host"`
		Macro        string `ch:"macro"`
		Substitution string `ch:"substitution"`
	}
	if err := b.ch.SelectContext(ctx, &rows, fmt.Sprintf("SELECT hostName() AS host, macro, substitution FROM cluster(%s, system.macros)", chQuote(cluster))); err != nil {
		return nil, errors.Wrapf(err, "can't read system.macros of cluster '%s'", cluster)
	}
	macros := make(shardMacros, len(hosts))
	for _, h := range hosts {
		macros[h.Host] = map[string]string{}
	}
	for _, row := range rows {
		if _, exists := macros[row.Host]; !exists {
			macros[row.Host] = map[string]string{}
		}
		macros[row.Host][row.Macro] = row.Substitution
	}
	return macros, nil
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

func countShards(replicas []clusterReplica) int {
	shards := map[uint32]struct{}{}
	for _, r := range replicas {
		shards[r.ShardNum] = struct{}{}
	}
	return len(shards)
}

// nonShardedReplicatedTables returns the replicated tables whose zookeeper path resolves to one value on every shard of the cluster,
// the zookeeper path comes from the CREATE query of the backup, macro values from system.macros of each shard,
// system.macros of other shards is read only when a path contains macros besides {database}, {table} and {uuid}
func (b *Backuper) nonShardedReplicatedTables(ctx context.Context, cluster string, shardsCount int, tables ListOfTables) (map[metadata.TableTitle]struct{}, error) {
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
	var macros shardMacros
	usedMacros := map[string]struct{}{}
	nonSharded := map[metadata.TableTitle]struct{}{}
	for _, table := range tables {
		zkPath, isReplicated, err := replicatedZookeeperPath(table.Query, getDefaultReplicaPath)
		if err != nil {
			return nil, err
		}
		if !isReplicated || zkPath == "" {
			continue
		}
		clusterMacros := pathClusterMacros(zkPath)
		if len(clusterMacros) > 0 && macros == nil {
			if macros, err = b.getShardMacros(ctx, cluster); err != nil {
				return nil, err
			}
		}
		for _, macro := range clusterMacros {
			usedMacros[macro] = struct{}{}
		}
		isNonSharded, err := isNonShardedReplicatedPath(zkPath, macros)
		if err != nil {
			return nil, errors.Wrapf(err, "`%s`.`%s`", table.Database, table.Table)
		}
		if isNonSharded {
			nonSharded[metadata.TableTitle{Database: table.Database, Table: table.Table}] = struct{}{}
		}
	}
	if sameValues := shardedMacrosWithSameValues(b.cfg.ClickHouse.AlwaysShardedMacros, usedMacros, macros); len(sameValues) > 0 {
		// on a single shard every replicated table has one replication queue, nothing to warn about
		if shardsCount > 1 {
			log.Warn().Msgf("skip_restore_non_sharded_replicated_tables: %s listed in always_sharded_macros have the same value on all %d shards of cluster '%s', tables with these macros in zookeeper path are non-sharded, check <macros> and <remote_servers> of the cluster", strings.Join(sameValues, ", "), shardsCount, cluster)
		} else {
			log.Info().Msgf("skip_restore_non_sharded_replicated_tables: cluster '%s' has one shard, %s have the same value on every replica, tables with these macros in zookeeper path are non-sharded", cluster, strings.Join(sameValues, ", "))
		}
	}
	return nonSharded, nil
}

// getNonShardedReplicatedTablesSkipData returns the tables whose data shall not be restored on this node:
// replicated tables with one zookeeper path on the whole cluster when this node is not the first replica of the first shard
// of skip_check_non_sharded_replicated_tables_cluster, their schema is restored and the data is replicated from the node
// which restores it, https://github.com/Altinity/clickhouse-backup/issues/1181
func (b *Backuper) getNonShardedReplicatedTablesSkipData(ctx context.Context, tables ListOfTables) (map[metadata.TableTitle]struct{}, error) {
	b.nonShardedReplicated = nil
	if !b.cfg.ClickHouse.SkipRestoreNonShardedReplicatedTables {
		return nil, nil
	}
	hasReplicated := false
	for _, table := range tables {
		if replicatedEngineZookeeperPathRE.MatchString(table.Query) {
			hasReplicated = true
			break
		}
	}
	if !hasReplicated {
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
	nonSharded, err := b.nonShardedReplicatedTables(ctx, cluster, countShards(replicas), tables)
	if err != nil {
		return nil, errors.Wrap(err, "skip_restore_non_sharded_replicated_tables")
	}
	if len(nonSharded) == 0 {
		return nil, nil
	}
	b.nonShardedReplicated = nonSharded
	names := make([]string, 0, len(nonSharded))
	for t := range nonSharded {
		names = append(names, fmt.Sprintf("`%s`.`%s`", t.Database, t.Table))
	}
	sort.Strings(names)
	if isOwner {
		log.Info().Msgf("skip_restore_non_sharded_replicated_tables: current node is the first replica of the first shard of cluster '%s', restore data of %d non-sharded replicated tables: %s", cluster, len(nonSharded), strings.Join(names, ", "))
		return nil, nil
	}
	log.Warn().Msgf("skip_restore_non_sharded_replicated_tables: skip data restore of %d non-sharded replicated tables, only schema is restored, data shall be restored on %s of cluster '%s' and is replicated from there: %s", len(nonSharded), owner, cluster, strings.Join(names, ", "))
	return nonSharded, nil
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
