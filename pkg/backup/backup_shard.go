package backup

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
)

var (
	// errUnknownBackupShard is returned when sharding assignment is requested for a table for which
	// active replication state is not known.
	errUnknownBackupShard = errors.New("unknown backup shard")

	// errNoActiveReplicas is returned when a table is has no current active replicas
	errNoActiveReplicas = errors.New("no active replicas")

	shardFuncRegistry = map[string]shardFunc{
		"table":         fnvHashModTableShardFunc,
		"database":      fnvHashModDatabaseShardFunc,
		"first-replica": firstReplicaShardFunc,
		"none":          noneShardFunc,
		"":              noneShardFunc,
	}
)

// shardDetermination is an object holding information on whether or not a table is within the
// backup shard
type shardDetermination map[string]bool

// inShard returns whether or not a given table is within a backup shard
func (d shardDetermination) inShard(database, table string) (bool, error) {
	fullName := fmt.Sprintf("`%s`.`%s`", database, table)
	presentInShard, ok := d[fullName]
	if !ok {
		return false, fmt.Errorf("error determining backup shard state for %q: %w", fullName,
			errUnknownBackupShard)
	}
	return presentInShard, nil
}

// backupSharder is an interface which can obtain a shard determination at a given point in time
type backupSharder interface {
	determineShards(ctx context.Context) (shardDetermination, error)
}

// tableReplicaMetadata is data derived from `system.replicas`
type tableReplicaMetadata struct {
	Database    string `ch:"database" json:"database"`
	Table       string `ch:"table" json:"table"`
	ReplicaName string `ch:"replica_name" json:"replica_name"`
	// TODO: Change type to use replica_is_active directly after upgrade to clickhouse-go v2
	ActiveReplicas []string `ch:"active_replicas" json:"replica_is_active"`
}

// fullName returns the table name in the form of `database.table`
func (md *tableReplicaMetadata) fullName() string {
	return fmt.Sprintf("`%s`.`%s`", md.Database, md.Table)
}

// querier is an interface that can query Clickhouse
type querier interface {
	SelectContext(context.Context, interface{}, string, ...interface{}) error
}

// shardFunc is a function that is determines whether or not a given database/table should have its
// data backed up by the replica calling this function
type shardFunc func(md *tableReplicaMetadata) (bool, error)

func shardFuncByName(name string) (shardFunc, error) {
	chosen, ok := shardFuncRegistry[name]
	if !ok {
		validOptions := make([]string, len(shardFuncRegistry))
		for k := range shardFuncRegistry {
			if k == "" {
				continue
			}
			validOptions = append(validOptions, k)
		}
		return nil, fmt.Errorf("unknown backup sharding option %q, valid options: %v", name,
			validOptions)
	}
	return chosen, nil
}

// fnvShardReplicaFromString returns a replica assignment from a slice of active replicas by taking
// an arbitrary string, performing a FNV hash on it (mod NumActiveReplicas), and using the resulting
// number as an index of the sorted slice of active replicas. It is assumed that the active replicas
// slice is provided pre-sorted.
func fnvShardReplicaFromString(str string, activeReplicas []string) (string, error) {
	if len(activeReplicas) == 0 {
		return "", fmt.Errorf("could not determine in-shard state for %s: %w", str,
			errNoActiveReplicas)
	}

	h := fnv.New32a()
	if _, err := h.Write([]byte(str)); err != nil {
		return "", fmt.Errorf("can't write %s to fnv.New32a", str)
	}
	i := h.Sum32() % uint32(len(activeReplicas))
	return activeReplicas[i], nil
}

// fnvHashModTableShardFunc determines whether a replica should handle backing up data based on the
// table name in the form of `database.table`. It is assumed that the active replicas slice is
// provided pre-sorted.
func fnvHashModTableShardFunc(md *tableReplicaMetadata) (bool, error) {
	assignedReplica, err := fnvShardReplicaFromString(md.fullName(), md.ActiveReplicas)
	if err != nil {
		return false, err
	}
	return assignedReplica == md.ReplicaName, nil
}

// fnvHashModDatabaseShardFunc determines whether a replica should handle backing up data based on
// database name. It is assumed that the active replicas slice is provided pre-sorted.
func fnvHashModDatabaseShardFunc(md *tableReplicaMetadata) (bool, error) {
	assignedReplica, err := fnvShardReplicaFromString(md.Database, md.ActiveReplicas)
	if err != nil {
		return false, err
	}
	return assignedReplica == md.ReplicaName, nil
}

// firstReplicaShardFunc determines whether a replica should handle backing up data based on whether
// or not it is the lexicographically first active replica. It is assumed that the active replicas
// slice is provided pre-sorted.
func firstReplicaShardFunc(md *tableReplicaMetadata) (bool, error) {
	if len(md.ActiveReplicas) == 0 {
		return false, fmt.Errorf("could not determine in-shard state for %s: %w", md.fullName(),
			errNoActiveReplicas)
	}
	return md.ReplicaName == md.ActiveReplicas[0], nil
}

// noneShardFunc always returns true
func noneShardFunc(md *tableReplicaMetadata) (bool, error) {
	return true, nil
}

// doesShard returns whether a ShardedOperationMode configuration performs sharding or not
func doesShard(mode string) bool {
	_, ok := shardFuncRegistry[mode]
	if !ok {
		return false
	}
	return mode != "" && mode != "none"
}

// replicaDeterminer is a concrete struct that will query clickhouse to obtain a shard determination
// by examining replica information
type replicaDeterminer struct {
	q  querier
	sf shardFunc
}

// newReplicaDeterminer returns a new shardDeterminer
func newReplicaDeterminer(q querier, sf shardFunc) *replicaDeterminer {
	sd := &replicaDeterminer{
		q:  q,
		sf: sf,
	}
	return sd
}

// getReplicaState obtains the local replication state through a query to `system.replicas`
func (rd *replicaDeterminer) getReplicaState(ctx context.Context) ([]tableReplicaMetadata, error) {
	md := []tableReplicaMetadata{}
	// TODO: Change query to pull replica_is_active after upgrading to clickhouse-go v2
	query := "SELECT t.database, t.name AS table, r.replica_name, arraySort(mapKeys(mapFilter((replica, active) -> (active == 1), r.replica_is_active))) AS active_replicas FROM system.tables t LEFT JOIN system.replicas r ON t.database = r.database AND t.name = r.table"
	if err := rd.q.SelectContext(ctx, &md, query); err != nil {
		return nil, fmt.Errorf("could not determine replication state: %w", err)
	}

	// Handle views and memory tables by putting in stand-in replication metadata
	for i, entry := range md {
		if entry.ReplicaName == "" && len(entry.ActiveReplicas) == 0 {
			md[i].ReplicaName = "no-replicas"
			md[i].ActiveReplicas = []string{"no-replicas"}
		}
	}
	return md, nil
}

func (rd *replicaDeterminer) determineShards(ctx context.Context) (shardDetermination, error) {
	md, err := rd.getReplicaState(ctx)
	if err != nil {
		return nil, err
	}
	sd := shardDetermination{}
	for _, entry := range md {
		assigned, err := rd.sf(&entry)
		if err != nil {
			return nil, err
		}
		sd[entry.fullName()] = assigned
	}
	return sd, nil
}
