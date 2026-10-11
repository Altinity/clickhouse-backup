package backup

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/keeper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// Kafka engine tables with `kafka_keeper_path` (StorageKafka2) store committed offsets in keeper instead of Kafka,
// backup dumps them while the table is detached and restore puts them back before CREATE, look https://github.com/Altinity/clickhouse-backup/issues/1120

const kafkaKeeperPathSetting = "kafka_keeper_path"

var kafkaEngineRE = regexp.MustCompile(`ENGINE\s*=\s*Kafka\b`)

func kafkaSettingRE(name string) *regexp.Regexp {
	return regexp.MustCompile(`(\b` + regexp.QuoteMeta(name) + `\s*=\s*')((?:[^'\\]|\\.)*)(')`)
}

// kafkaSettingValueLoc - absolute [start, end) of the raw setting value, searched only after `ENGINE = Kafka`,
// so column DEFAULT or other engines can't match, nil when not found
func kafkaSettingValueLoc(query, name string) []int {
	engineLoc := kafkaEngineRE.FindStringIndex(query)
	if engineLoc == nil {
		return nil
	}
	loc := kafkaSettingRE(name).FindStringSubmatchIndex(query[engineLoc[1]:])
	if loc == nil {
		return nil
	}
	return []int{engineLoc[1] + loc[4], engineLoc[1] + loc[5]}
}

// unescapeClickHouseString - decode backslash escapes of a single-quoted ClickHouse string literal
func unescapeClickHouseString(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i == len(s)-1 {
			sb.WriteByte(s[i])
			continue
		}
		i++
		switch s[i] {
		case 'n':
			sb.WriteByte('\n')
		case 't':
			sb.WriteByte('\t')
		case 'r':
			sb.WriteByte('\r')
		case '0':
			sb.WriteByte(0)
		default:
			sb.WriteByte(s[i])
		}
	}
	return sb.String()
}

// parseKafkaSetting - return unescaped value of the string setting from Kafka engine SETTINGS clause
func parseKafkaSetting(query, name string) (string, bool) {
	loc := kafkaSettingValueLoc(query, name)
	if loc == nil {
		return "", false
	}
	return unescapeClickHouseString(query[loc[0]:loc[1]]), true
}

// replaceKafkaSetting - apply fn to the raw (escaped) value of the string setting from Kafka engine SETTINGS clause
func replaceKafkaSetting(query, name string, fn func(rawValue string) string) string {
	loc := kafkaSettingValueLoc(query, name)
	if loc == nil {
		return query
	}
	return query[:loc[0]] + fn(query[loc[0]:loc[1]]) + query[loc[1]:]
}

func kafkaKeeperPathFromQuery(engine, query string) (string, bool) {
	if engine != "Kafka" {
		return "", false
	}
	return parseKafkaSetting(query, kafkaKeeperPathSetting)
}

func resolveKafkaKeeperPath(keeperPath, tableUUID, database, table string) string {
	return strings.NewReplacer("{uuid}", tableUUID, "{database}", database, "{table}", table).Replace(keeperPath)
}

// adjustKafkaKeeperPathForMapping - replace the first whole `/<srcName>` path segment with `/<dstName>` in kafka_keeper_path,
// like replicated paths in https://github.com/Altinity/clickhouse-backup/issues/547, but `t` doesn't match `/tmp`
func adjustKafkaKeeperPathForMapping(query, srcName, dstName, tableTitle string) string {
	oldPath, ok := parseKafkaSetting(query, kafkaKeeperPathSetting)
	if !ok {
		return query
	}
	segmentRE := regexp.MustCompile(`/` + regexp.QuoteMeta(srcName) + `(/|$)`)
	query = replaceKafkaSetting(query, kafkaKeeperPathSetting, func(rawValue string) string {
		loc := segmentRE.FindStringSubmatchIndex(rawValue)
		if loc == nil {
			return rawValue
		}
		return rawValue[:loc[0]] + "/" + dstName + rawValue[loc[2]:]
	})
	if newPath, _ := parseKafkaSetting(query, kafkaKeeperPathSetting); newPath != oldPath || strings.Contains(newPath, "{uuid}") {
		return query
	}
	log.Warn().Msgf("%s: %s doesn't contain `/%s` path segment and `{uuid}`, restored table will share keeper path with the source table", tableTitle, kafkaKeeperPathSetting, srcName)
	return query
}

func isKafkaKeeperSubtree(nodePath, root string) bool {
	return nodePath == root || strings.HasPrefix(nodePath, root+"/")
}

// filterKafkaKeeperNodesForBackup - skip keeper path itself, `dropped` subtree which exists only during DROP
// and persistent hold locks of clickhouse-backup left by `restore --schema`
func filterKafkaKeeperNodesForBackup(nodes []keeper.DumpNode) []metadata.KafkaKeeperNode {
	result := make([]metadata.KafkaKeeperNode, 0, len(nodes))
	for _, node := range nodes {
		if node.Path == "" || isKafkaKeeperSubtree(node.Path, "dropped") || (isKafkaKeeperLockNode(node.Path) && string(node.Value) == kafkaKeeperHoldOwner) {
			continue
		}
		result = append(result, metadata.KafkaKeeperNode{Path: node.Path, Value: string(node.Value)})
	}
	return result
}

// filterKafkaKeeperNodesForRestore - replica registrations, partition locks and `dropped` belong to the source replicas,
// `topic_partition_locks` from a ClickHouse 25.6+ backup is skipped on older ClickHouse to keep the native keeper layout,
// `replicas` itself is kept, StorageKafka2 skips keeper skeleton creation and reads `committed` when it exists
func filterKafkaKeeperNodesForRestore(nodes []metadata.KafkaKeeperNode, version int) []metadata.KafkaKeeperNode {
	result := make([]metadata.KafkaKeeperNode, 0, len(nodes))
	for _, node := range nodes {
		if node.Path == "topic_partition_locks" && version < kafkaKeeperPartitionLocksVersion {
			continue
		}
		if node.Path == "" || isKafkaKeeperSubtree(node.Path, "dropped") || strings.HasPrefix(node.Path, "replicas/") || strings.HasPrefix(node.Path, "topic_partition_locks/") || isKafkaKeeperLockNode(node.Path) {
			continue
		}
		result = append(result, node)
	}
	return result
}

// parseKafkaKeeperOffsetNode - match topics/<topic>/partitions/<partition>/{committed|intention}
func parseKafkaKeeperOffsetNode(nodePath string) (topic, partition, kind string, ok bool) {
	parts := strings.Split(nodePath, "/")
	if len(parts) != 5 || parts[0] != "topics" || parts[2] != "partitions" || (parts[4] != "committed" && parts[4] != "intention") {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[4], true
}

func formatKafkaKeeperOffsets(nodes []metadata.KafkaKeeperNode) string {
	offsets := make([]string, 0)
	for _, node := range nodes {
		if topic, partition, kind, ok := parseKafkaKeeperOffsetNode(node.Path); ok && kind == "committed" {
			offsets = append(offsets, fmt.Sprintf("%s/%s=%s", topic, partition, node.Value))
		}
	}
	if len(offsets) == 0 {
		return "none"
	}
	return strings.Join(offsets, ", ")
}

type kafkaKeeperClient interface {
	Exists(nodePath string) (bool, error)
	Get(nodePath string) ([]byte, bool, error)
	Create(nodePath string, value []byte) (bool, error)
	Children(nodePath string) ([]string, error)
	DumpTree(prefix string) ([]keeper.DumpNode, error)
	EnsureNode(nodePath string) error
	Upsert(nodePath string, value []byte) error
	Delete(nodePath string) error
}

// kafkaKeeperHoldOwner - value of persistent partition locks which hold consumption between schema and data restore,
// StorageKafka2 skips a partition when its lock znode already exists, see issues/1120
const kafkaKeeperHoldOwner = "clickhouse-backup"

// kafkaKeeperActivity - return replicas with ephemeral `is_active` and partition locks held by somebody else than clickhouse-backup,
// keeperPath shall be already resolved via ResolvePath
func kafkaKeeperActivity(k kafkaKeeperClient, keeperPath string) ([]string, []string, error) {
	replicas, err := k.Children(path.Join(keeperPath, "replicas"))
	if err != nil {
		return nil, nil, err
	}
	activeReplicas := make([]string, 0)
	for _, replica := range replicas {
		isActive, existsErr := k.Exists(path.Join(keeperPath, "replicas", replica, "is_active"))
		if existsErr != nil {
			return nil, nil, existsErr
		}
		if isActive {
			activeReplicas = append(activeReplicas, replica)
		}
	}
	locks, err := kafkaKeeperLocks(k, keeperPath)
	if err != nil {
		return nil, nil, err
	}
	foreignLocks := make([]string, 0)
	for _, lock := range locks {
		if lock.value != kafkaKeeperHoldOwner {
			foreignLocks = append(foreignLocks, strings.TrimPrefix(lock.path, keeperPath+"/"))
		}
	}
	return activeReplicas, foreignLocks, nil
}

// kafkaKeeperPartitionLocksVersion - ClickHouse 25.6+ (KeeperHandlingConsumer) locks partitions with `topic_partition_locks/<topic>_<partition>.lock`,
// ClickHouse 24.8-25.5 locks them with `topics/<topic>/partitions/<partition>/lock`
const kafkaKeeperPartitionLocksVersion = 25006000

type kafkaKeeperLock struct {
	path  string
	value string
}

// isKafkaKeeperLockNode - relative path of partition lock in any layout
func isKafkaKeeperLockNode(nodePath string) bool {
	parts := strings.Split(nodePath, "/")
	return (len(parts) == 2 && parts[0] == "topic_partition_locks") || (len(parts) == 5 && parts[0] == "topics" && parts[2] == "partitions" && parts[4] == "lock")
}

// kafkaKeeperLocks - existing partition locks in both layouts sorted by keeper path, keeperPath shall be already resolved via ResolvePath
func kafkaKeeperLocks(k kafkaKeeperClient, keeperPath string) ([]kafkaKeeperLock, error) {
	lockPaths := make([]string, 0)
	locksPath := path.Join(keeperPath, "topic_partition_locks")
	lockNames, err := k.Children(locksPath)
	if err != nil {
		return nil, err
	}
	for _, lockName := range lockNames {
		lockPaths = append(lockPaths, path.Join(locksPath, lockName))
	}
	topics, err := k.Children(path.Join(keeperPath, "topics"))
	if err != nil {
		return nil, err
	}
	for _, topic := range topics {
		partitions, partitionsErr := k.Children(path.Join(keeperPath, "topics", topic, "partitions"))
		if partitionsErr != nil {
			return nil, partitionsErr
		}
		for _, partition := range partitions {
			lockPaths = append(lockPaths, path.Join(keeperPath, "topics", topic, "partitions", partition, "lock"))
		}
	}
	sort.Strings(lockPaths)
	locks := make([]kafkaKeeperLock, 0)
	for _, lockPath := range lockPaths {
		value, exists, getErr := k.Get(lockPath)
		if getErr != nil {
			return nil, getErr
		}
		if exists {
			locks = append(locks, kafkaKeeperLock{path: lockPath, value: string(value)})
		}
	}
	return locks, nil
}

// kafkaKeeperHoldLockPaths - relative lock path for each partition with committed offset in backup, layout depends on ClickHouse version
func kafkaKeeperHoldLockPaths(nodes []metadata.KafkaKeeperNode, version int) []string {
	lockPaths := make([]string, 0)
	for _, node := range nodes {
		if topic, partition, kind, ok := parseKafkaKeeperOffsetNode(node.Path); ok && kind == "committed" {
			if version >= kafkaKeeperPartitionLocksVersion {
				lockPaths = append(lockPaths, path.Join("topic_partition_locks", fmt.Sprintf("%s_%s.lock", topic, partition)))
			} else {
				lockPaths = append(lockPaths, path.Join("topics", topic, "partitions", partition, "lock"))
			}
		}
	}
	sort.Strings(lockPaths)
	return lockPaths
}

// placeKafkaKeeperHoldLocks - create persistent partition locks owned by clickhouse-backup, return their keeper paths,
// existing lock of another owner is never overwritten, it could be a live ephemeral lock of a consuming replica
func placeKafkaKeeperHoldLocks(k kafkaKeeperClient, keeperPath, tableTitle string, nodes []metadata.KafkaKeeperNode, version int) ([]string, error) {
	relativeLockPaths := kafkaKeeperHoldLockPaths(nodes, version)
	if len(relativeLockPaths) == 0 {
		return nil, nil
	}
	lockPaths := make([]string, 0, len(relativeLockPaths))
	for _, relativeLockPath := range relativeLockPaths {
		lockPath := path.Join(keeperPath, relativeLockPath)
		if err := k.EnsureNode(path.Dir(lockPath)); err != nil {
			return nil, err
		}
		created, err := k.Create(lockPath, []byte(kafkaKeeperHoldOwner))
		if err != nil {
			return nil, err
		}
		if !created {
			value, exists, getErr := k.Get(lockPath)
			if getErr != nil {
				return nil, getErr
			}
			if exists && string(value) != kafkaKeeperHoldOwner {
				log.Warn().Msgf("%s: kafka partition lock %s is held by another consumer `%s`, skip it", tableTitle, lockPath, string(value))
				continue
			}
			if !exists {
				log.Warn().Msgf("%s: kafka partition lock %s was removed concurrently, skip it", tableTitle, lockPath)
				continue
			}
		}
		lockPaths = append(lockPaths, lockPath)
	}
	log.Info().Msgf("kafka consumption for %s is held via %d partition locks until `restore --data` completes", tableTitle, len(lockPaths))
	return lockPaths, nil
}

// ownKafkaKeeperLocks - keeper paths of partition locks owned by clickhouse-backup in both layouts,
// both are checked regardless of version, the server could be upgraded between schema and data restore
func ownKafkaKeeperLocks(k kafkaKeeperClient, keeperPath string) ([]string, error) {
	locks, err := kafkaKeeperLocks(k, keeperPath)
	if err != nil {
		return nil, err
	}
	ownLocks := make([]string, 0)
	for _, lock := range locks {
		if lock.value == kafkaKeeperHoldOwner {
			ownLocks = append(ownLocks, lock.path)
		}
	}
	return ownLocks, nil
}

// releaseOwnKafkaKeeperLocks - delete partition locks owned by clickhouse-backup, locks of other owners are never touched
func releaseOwnKafkaKeeperLocks(k kafkaKeeperClient, keeperPath, tableTitle string) error {
	ownLocks, err := ownKafkaKeeperLocks(k, keeperPath)
	if err != nil {
		return err
	}
	for _, lockPath := range ownLocks {
		if err = k.Delete(lockPath); err != nil {
			return err
		}
		log.Info().Msgf("%s: released kafka partition lock %s", tableTitle, lockPath)
	}
	return nil
}

// parseKafkaTopicList - topics from comma separated kafka_topic_list setting
func parseKafkaTopicList(query string) []string {
	topicList, ok := parseKafkaSetting(query, "kafka_topic_list")
	if !ok {
		return nil
	}
	topics := make([]string, 0)
	for _, topic := range strings.Split(topicList, ",") {
		if topic = strings.TrimSpace(topic); topic != "" {
			topics = append(topics, topic)
		}
	}
	return topics
}

// ensureKafkaKeeperSkeleton - StorageKafka2 skips skeleton creation when `replicas` exists, so the restored tree shall contain
// the skeleton of the target ClickHouse version even when backup was created on another version,
// ClickHouse 25.6+ lists `topic_partition_locks` every poll cycle and doesn't consume without it, existing znodes are never overwritten
func ensureKafkaKeeperSkeleton(k kafkaKeeperClient, keeperPath string, topics []string, version int) error {
	skeleton := []string{"replicas", "topics"}
	for _, topic := range topics {
		skeleton = append(skeleton, path.Join("topics", topic, "partitions"))
	}
	if version >= kafkaKeeperPartitionLocksVersion {
		skeleton = append(skeleton, "topic_partition_locks")
	}
	for _, nodePath := range skeleton {
		if err := k.EnsureNode(path.Join(keeperPath, nodePath)); err != nil {
			return err
		}
	}
	return nil
}

type kafkaKeeperRestoreAction int

const (
	kafkaKeeperRestoreCreate kafkaKeeperRestoreAction = iota
	kafkaKeeperRestoreOverwrite
	kafkaKeeperRestoreSkip
)

// decideKafkaKeeperRestore - committed offsets are cached by replicas which hold partition locks, never overwrite them under active replicas
func decideKafkaKeeperRestore(pathExists bool, activeReplicas, partitionLocks []string) kafkaKeeperRestoreAction {
	if !pathExists {
		return kafkaKeeperRestoreCreate
	}
	if len(activeReplicas) > 0 || len(partitionLocks) > 0 {
		return kafkaKeeperRestoreSkip
	}
	return kafkaKeeperRestoreOverwrite
}

// applyKafkaKeeperOffsets - restore backup znodes under keeperPath, keeperPath shall be already resolved via ResolvePath,
// hold=true places partition locks to hold consumption until data restore and returns their keeper paths,
// hold=false releases partition locks owned by clickhouse-backup
func applyKafkaKeeperOffsets(k kafkaKeeperClient, keeperPath, tableTitle string, nodes []metadata.KafkaKeeperNode, topics []string, hold bool, version int) ([]string, error) {
	pathExists, err := k.Exists(keeperPath)
	if err != nil {
		return nil, err
	}
	var activeReplicas, partitionLocks []string
	if pathExists {
		if activeReplicas, partitionLocks, err = kafkaKeeperActivity(k, keeperPath); err != nil {
			return nil, err
		}
	}
	upsert := func(nodePath, value string) error {
		fullPath := path.Join(keeperPath, nodePath)
		if ensureErr := k.EnsureNode(path.Dir(fullPath)); ensureErr != nil {
			return ensureErr
		}
		return k.Upsert(fullPath, []byte(value))
	}
	action := decideKafkaKeeperRestore(pathExists, activeReplicas, partitionLocks)
	switch action {
	case kafkaKeeperRestoreCreate:
		if err = k.EnsureNode(keeperPath); err != nil {
			return nil, err
		}
		for _, node := range filterKafkaKeeperNodesForRestore(nodes, version) {
			if err = upsert(node.Path, node.Value); err != nil {
				return nil, err
			}
		}
		log.Info().Msgf("%s: restored %s=%s from backup, offsets: %s", tableTitle, kafkaKeeperPathSetting, keeperPath, formatKafkaKeeperOffsets(nodes))
	case kafkaKeeperRestoreOverwrite:
		currentNodes, dumpErr := k.DumpTree(keeperPath)
		if dumpErr != nil {
			return nil, dumpErr
		}
		currentValues := make(map[string]string, len(currentNodes))
		for _, node := range currentNodes {
			currentValues[node.Path] = string(node.Value)
		}
		changes := make([]string, 0)
		backupOffsetNodes := make(map[string]struct{})
		for _, node := range nodes {
			if _, _, _, ok := parseKafkaKeeperOffsetNode(node.Path); !ok {
				continue
			}
			backupOffsetNodes[node.Path] = struct{}{}
			oldValue, exists := currentValues[node.Path]
			if !exists {
				oldValue = "<none>"
			}
			if err = upsert(node.Path, node.Value); err != nil {
				return nil, err
			}
			changes = append(changes, fmt.Sprintf("%s: %s->%s", node.Path, oldValue, node.Value))
		}
		for _, node := range currentNodes {
			if _, _, kind, ok := parseKafkaKeeperOffsetNode(node.Path); !ok || kind != "intention" {
				continue
			}
			if _, inBackup := backupOffsetNodes[node.Path]; inBackup {
				continue
			}
			if err = k.Delete(path.Join(keeperPath, node.Path)); err != nil {
				return nil, err
			}
			changes = append(changes, fmt.Sprintf("%s: %s->deleted", node.Path, string(node.Value)))
		}
		sort.Strings(changes)
		log.Warn().Msgf("%s: %s=%s already exists without active replicas, overwrite offsets from backup: %s", tableTitle, kafkaKeeperPathSetting, keeperPath, strings.Join(changes, ", "))
	case kafkaKeeperRestoreSkip:
		log.Warn().Msgf("%s: %s=%s is used by active replicas %v, partition locks %v, offsets from backup are skipped, table will join existing state", tableTitle, kafkaKeeperPathSetting, keeperPath, activeReplicas, partitionLocks)
	}
	if action != kafkaKeeperRestoreSkip {
		if err = ensureKafkaKeeperSkeleton(k, keeperPath, topics, version); err != nil {
			return nil, err
		}
	}
	if !hold {
		return nil, releaseOwnKafkaKeeperLocks(k, keeperPath, tableTitle)
	}
	// active replicas already consume, nothing to hold
	if action == kafkaKeeperRestoreSkip {
		return nil, nil
	}
	return placeKafkaKeeperHoldLocks(k, keeperPath, tableTitle, nodes, version)
}

func isClickHouseErrorCode(err error, code int) bool {
	return err != nil && strings.Contains(err.Error(), fmt.Sprintf("code: %d,", code))
}

// isKafkaDetachedConcurrently - DETACH ... PERMANENTLY inside a Replicated database is executed on all replicas and shards of the database,
// UNKNOWN_TABLE means a concurrent backup on another shard already detached the table
func isKafkaDetachedConcurrently(err error, databaseEngine string) bool {
	return strings.HasPrefix(databaseEngine, "Replicated") && isClickHouseErrorCode(err, 60)
}

// isKafkaAttachedConcurrently - TABLE_ALREADY_EXISTS on ATTACH means a concurrent backup already attached the table back
func isKafkaAttachedConcurrently(err error) bool {
	return isClickHouseErrorCode(err, 57)
}

// backupKafkaKeeperOffsets - DETACH Kafka tables with kafka_keeper_path before any FREEZE, so offsets in keeper stay consistent
// with frozen data, and dump their keeper state, returned attachBack re-ATTACH detached tables and returns the first ATTACH error,
// it is safe to call it several times
func (b *Backuper) backupKafkaKeeperOffsets(ctx context.Context, tables []clickhouse.Table, doBackupData bool) (map[metadata.TableTitle]*metadata.KafkaKeeperMetadata, func() error, error) {
	noop := func() error { return nil }
	if !doBackupData {
		return nil, noop, nil
	}
	type kafkaTable struct {
		table      clickhouse.Table
		keeperPath string
	}
	kafkaTables := make([]kafkaTable, 0)
	for _, table := range tables {
		if table.Skip || table.BackupType != clickhouse.ShardBackupFull {
			continue
		}
		keeperPath, ok := kafkaKeeperPathFromQuery(table.Engine, table.CreateTableQuery)
		if !ok {
			continue
		}
		keeperPath, err := b.ch.ApplyMacros(ctx, resolveKafkaKeeperPath(keeperPath, table.UUID, table.Database, table.Name))
		if err != nil {
			return nil, noop, errors.Wrapf(err, "can't apply macros to %s of `%s`.`%s`", kafkaKeeperPathSetting, table.Database, table.Name)
		}
		kafkaTables = append(kafkaTables, kafkaTable{table: table, keeperPath: keeperPath})
	}
	if len(kafkaTables) == 0 {
		return nil, noop, nil
	}
	k := &keeper.Keeper{}
	if err := k.Connect(ctx, b.ch); err != nil {
		return nil, noop, errors.Wrapf(err, "can't connect to keeper for backup %s of Kafka tables", kafkaKeeperPathSetting)
	}
	defer k.Close()

	detached := make([]clickhouse.Table, 0, len(kafkaTables))
	var attachOnce sync.Once
	var attachErr error
	attachBack := func() error {
		attachOnce.Do(func() {
			// context could be already canceled, but detached tables shall be attached back anyway
			attachCtx := context.WithoutCancel(ctx)
			for _, table := range detached {
				query := fmt.Sprintf("ATTACH TABLE `%s`.`%s`", table.Database, table.Name)
				if err := b.ch.QueryContext(attachCtx, query); err != nil {
					if isKafkaAttachedConcurrently(err) {
						log.Info().Msgf("%s: table already attached by a concurrent backup", query)
						continue
					}
					log.Err(err).Msgf("%s failed, table stays detached, run `%s` manually", query, query)
					if attachErr == nil {
						attachErr = errors.Wrapf(err, "%s error", query)
					}
					continue
				}
				log.Info().Msgf("%s done", query)
			}
		})
		return attachErr
	}
	databaseEngines := make(map[string]string)
	for _, t := range kafkaTables {
		databaseEngine, isEngineKnown := databaseEngines[t.table.Database]
		if !isEngineKnown {
			var err error
			if databaseEngine, err = b.ch.GetDatabaseEngine(t.table.Database); err != nil {
				_ = attachBack()
				return nil, noop, errors.Wrapf(err, "can't get database engine for `%s`", t.table.Database)
			}
			databaseEngines[t.table.Database] = databaseEngine
		}
		// DETACH TABLE ... SYNC is rejected inside a Replicated database engine, such databases require PERMANENTLY,
		// which is replicated to all replicas and shards of the database, so concurrent per-shard backups could collide
		query := fmt.Sprintf("DETACH TABLE `%s`.`%s` SYNC", t.table.Database, t.table.Name)
		if strings.HasPrefix(databaseEngine, "Replicated") {
			query = fmt.Sprintf("DETACH TABLE `%s`.`%s` PERMANENTLY SYNC", t.table.Database, t.table.Name)
			log.Info().Msgf("database `%s` engine=%s, %s detaches the table on all replicas and shards of the database", t.table.Database, databaseEngine, query)
		}
		if err := b.ch.QueryContext(ctx, query); err != nil {
			if isKafkaDetachedConcurrently(err, databaseEngine) {
				// the table is detached, offsets are stable, the concurrent backup attaches it back
				log.Warn().Msgf("%s: table already detached by a concurrent backup, keeper offsets will be backed up from %s, table will not be attached back by this backup", query, t.keeperPath)
				continue
			}
			_ = attachBack()
			return nil, noop, errors.Wrapf(err, "%s error", query)
		}
		detached = append(detached, t.table)
		log.Info().Msgf("%s done, keeper offsets will be backed up from %s", query, t.keeperPath)
	}
	result := make(map[metadata.TableTitle]*metadata.KafkaKeeperMetadata, len(kafkaTables))
	for _, t := range kafkaTables {
		tableTitle := fmt.Sprintf("%s.%s", t.table.Database, t.table.Name)
		resolvedPath := k.ResolvePath(t.keeperPath)
		pathExists, err := k.Exists(resolvedPath)
		if err != nil {
			_ = attachBack()
			return nil, noop, errors.Wrapf(err, "%s: can't check %s", tableTitle, kafkaKeeperPathSetting)
		}
		if !pathExists {
			log.Warn().Msgf("%s: %s=%s doesn't exist, nothing to backup", tableTitle, kafkaKeeperPathSetting, t.keeperPath)
			continue
		}
		if activeReplicas, partitionLocks, activityErr := kafkaKeeperActivity(k, resolvedPath); activityErr != nil {
			_ = attachBack()
			return nil, noop, errors.Wrapf(activityErr, "%s: can't check active replicas in %s", tableTitle, kafkaKeeperPathSetting)
		} else if len(activeReplicas) > 0 || len(partitionLocks) > 0 {
			log.Warn().Msgf("%s: %s=%s still has active replicas %v, partition locks %v on other servers, backed up offsets could be inconsistent with backed up data", tableTitle, kafkaKeeperPathSetting, t.keeperPath, activeReplicas, partitionLocks)
		}
		dumpNodes, err := k.DumpTree(resolvedPath)
		if err != nil {
			_ = attachBack()
			return nil, noop, errors.Wrapf(err, "%s: can't dump %s", tableTitle, kafkaKeeperPathSetting)
		}
		kafkaKeeper := &metadata.KafkaKeeperMetadata{Path: t.keeperPath, Nodes: filterKafkaKeeperNodesForBackup(dumpNodes)}
		result[metadata.TableTitle{Database: t.table.Database, Table: t.table.Name}] = kafkaKeeper
		log.Info().Msgf("%s: backup %s=%s, %d znodes, offsets: %s", tableTitle, kafkaKeeperPathSetting, t.keeperPath, len(kafkaKeeper.Nodes), formatKafkaKeeperOffsets(kafkaKeeper.Nodes))
	}
	return result, attachBack, nil
}

// restoreKafkaKeeperOffsets - restore keeper state of the Kafka table right before its CREATE and after DROP of the existing table,
// k is connected lazily and shall be closed by the caller
func (b *Backuper) restoreKafkaKeeperOffsets(ctx context.Context, schema *metadata.TableMetadata, databaseEngine string, version int, k **keeper.Keeper) error {
	hasNodes := schema.KafkaKeeper != nil && len(schema.KafkaKeeper.Nodes) > 0
	tableTitle := fmt.Sprintf("%s.%s", schema.Database, schema.Table)
	keeperPath, ok := parseKafkaSetting(schema.Query, kafkaKeeperPathSetting)
	if !ok {
		if hasNodes {
			log.Warn().Msgf("%s: backup contains keeper offsets, but %s not found in query, skip offsets restore", tableTitle, kafkaKeeperPathSetting)
		}
		return nil
	}
	if strings.Contains(keeperPath, "{uuid}") {
		uuidMatches := uuidRE.FindStringSubmatch(schema.Query)
		if len(uuidMatches) == 0 {
			log.Warn().Msgf("%s: %s=%s contains {uuid}, but query doesn't contain UUID, skip offsets restore", tableTitle, kafkaKeeperPathSetting, keeperPath)
			return nil
		}
		tableUUID := uuidMatches[1]
		if databaseEngine == "" {
			var err error
			if databaseEngine, err = b.ch.GetDatabaseEngine(schema.Database); err != nil {
				return errors.Wrapf(err, "%s: can't get database engine", tableTitle)
			}
		}
		// CreateTable could strip explicit UUID inside a Replicated database engine, look cleanUUIDForReplicatedDatabase,
		// and ClickHouse rejects {uuid} macro in a CREATE without ON CLUSTER even with explicit UUID (code 36),
		// pin {uuid} in kafka_keeper_path to keep keeper path the same as restored znodes, the same way as https://github.com/Altinity/clickhouse-backup/issues/466
		if b.cfg.General.RestoreSchemaOnCluster == "" || strings.HasPrefix(databaseEngine, "Replicated") {
			schema.Query = replaceKafkaSetting(schema.Query, kafkaKeeperPathSetting, func(rawValue string) string {
				return strings.ReplaceAll(rawValue, "{uuid}", tableUUID)
			})
			log.Info().Msgf("%s: database engine=%s, restore_schema_on_cluster=%q, replace {uuid} in %s to %s", tableTitle, databaseEngine, b.cfg.General.RestoreSchemaOnCluster, kafkaKeeperPathSetting, tableUUID)
		}
		keeperPath = strings.ReplaceAll(keeperPath, "{uuid}", tableUUID)
	}
	// {uuid} is pinned for any Kafka table with kafka_keeper_path, keeper state is restored only when backup contains it
	if !hasNodes {
		return nil
	}
	if b.cfg.General.RestoreSchemaOnCluster != "" && (strings.Contains(keeperPath, "{shard}") || strings.Contains(keeperPath, "{replica}")) {
		log.Warn().Msgf("%s: restore_schema_on_cluster=%q and %s=%s contains {shard} or {replica}, offsets are restored only for the current node resolved path", tableTitle, b.cfg.General.RestoreSchemaOnCluster, kafkaKeeperPathSetting, keeperPath)
	}
	keeperPath, err := b.ch.ApplyMacros(ctx, resolveKafkaKeeperPath(keeperPath, "", schema.Database, schema.Table))
	if err != nil {
		return errors.Wrapf(err, "%s: can't apply macros to %s", tableTitle, kafkaKeeperPathSetting)
	}
	if *k == nil {
		connectedKeeper := &keeper.Keeper{}
		if err = connectedKeeper.Connect(ctx, b.ch); err != nil {
			return errors.Wrapf(err, "%s: can't connect to keeper for restore %s offsets", tableTitle, kafkaKeeperPathSetting)
		}
		*k = connectedKeeper
	}
	// kafka_keeper metadata exists only in backups with data, so consumption is held until `restore --data`
	heldLocks, err := applyKafkaKeeperOffsets(*k, (*k).ResolvePath(keeperPath), tableTitle, schema.KafkaKeeper.Nodes, parseKafkaTopicList(schema.Query), !b.SkipKafkaKeeperHold, version)
	if err != nil {
		return errors.Wrapf(err, "%s: can't restore %s=%s", tableTitle, kafkaKeeperPathSetting, keeperPath)
	}
	b.kafkaHeldLocks = append(b.kafkaHeldLocks, heldLocks...)
	return nil
}

// markKafkaKeeperRestored - restore keeper offsets at most once per table, CREATE could be retried several times in restoreSchemaRegular,
// returns false when offsets for this table were already restored
func markKafkaKeeperRestored(restored map[metadata.TableTitle]struct{}, schema *metadata.TableMetadata) bool {
	title := metadata.TableTitle{Database: schema.Database, Table: schema.Table}
	if _, isRestored := restored[title]; isRestored {
		return false
	}
	restored[title] = struct{}{}
	return true
}

// warnKafkaKeeperHeldLocks - explain how consumption held by partition locks placed during schema restore could be released
func (b *Backuper) warnKafkaKeeperHeldLocks(backupName, reason string) {
	if len(b.kafkaHeldLocks) == 0 {
		return
	}
	log.Warn().Msgf("%s, kafka consumption stays held by partition locks %v until `restore --data` runs for backup %s, or until restore reruns with --skip-kafka-keeper-hold, or until these znodes are deleted, e.g. `clickhouse-keeper-client -q 'rm <path>'`", reason, b.kafkaHeldLocks, backupName)
}

// resolveRestoredKafkaKeeperPath - resolve kafka_keeper_path from the live table in system.tables,
// fallback to the restored query from backup metadata when the table doesn't exist on this node
func (b *Backuper) resolveRestoredKafkaKeeperPath(ctx context.Context, schema *metadata.TableMetadata) (string, error) {
	liveTables := make([]struct {
		UUID  string `ch:"uuid"`
		Query string `ch:"create_table_query"`
	}, 0)
	if err := b.ch.SelectContext(ctx, &liveTables, "SELECT toString(uuid) AS uuid, create_table_query FROM system.tables WHERE database=? AND name=?", schema.Database, schema.Table); err != nil {
		return "", errors.Wrap(err, "can't read system.tables")
	}
	query, tableUUID := schema.Query, ""
	if len(liveTables) > 0 {
		query, tableUUID = liveTables[0].Query, liveTables[0].UUID
	} else if uuidMatches := uuidRE.FindStringSubmatch(schema.Query); len(uuidMatches) > 0 {
		tableUUID = uuidMatches[1]
	}
	keeperPath, ok := parseKafkaSetting(query, kafkaKeeperPathSetting)
	if !ok {
		return "", errors.Errorf("%s not found in %s", kafkaKeeperPathSetting, query)
	}
	if strings.Contains(keeperPath, "{uuid}") && tableUUID == "" {
		return "", errors.Errorf("%s=%s contains {uuid}, but table UUID is unknown", kafkaKeeperPathSetting, keeperPath)
	}
	return b.ch.ApplyMacros(ctx, resolveKafkaKeeperPath(keeperPath, tableUUID, schema.Database, schema.Table))
}

// finishKafkaKeeperHolds - release partition locks placed by schema restore after successful data restore,
// keep them and explain how to release them when data restore failed, errors here never fail the restore
func (b *Backuper) finishKafkaKeeperHolds(ctx context.Context, tables ListOfTables, dataErr error) {
	kafkaTables := make(ListOfTables, 0)
	for _, table := range tables {
		if table != nil && table.KafkaKeeper != nil && len(table.KafkaKeeper.Nodes) > 0 {
			kafkaTables = append(kafkaTables, table)
		}
	}
	if len(kafkaTables) == 0 {
		return
	}
	k := &keeper.Keeper{}
	if err := k.Connect(ctx, b.ch); err != nil {
		log.Warn().Msgf("can't connect to keeper to release kafka partition locks with value `%s` under %s/topic_partition_locks of restored Kafka tables, release them manually: %v", kafkaKeeperHoldOwner, kafkaKeeperPathSetting, err)
		return
	}
	defer k.Close()
	for _, table := range kafkaTables {
		tableTitle := fmt.Sprintf("%s.%s", table.Database, table.Table)
		keeperPath, err := b.resolveRestoredKafkaKeeperPath(ctx, table)
		if err != nil {
			log.Warn().Msgf("%s: can't resolve %s to release kafka partition locks: %v", tableTitle, kafkaKeeperPathSetting, err)
			continue
		}
		resolvedPath := k.ResolvePath(keeperPath)
		if dataErr != nil {
			ownLocks, locksErr := ownKafkaKeeperLocks(k, resolvedPath)
			if locksErr != nil {
				log.Warn().Msgf("%s: can't list kafka partition locks under %s: %v", tableTitle, resolvedPath, locksErr)
				continue
			}
			if len(ownLocks) > 0 {
				log.Warn().Msgf("%s: data restore failed, kafka consumption stays held by partition locks %v, rerun `restore --data` for this backup, or rerun restore with --skip-kafka-keeper-hold, or delete these znodes, e.g. `clickhouse-keeper-client -q 'rm <path>'`", tableTitle, ownLocks)
			}
			continue
		}
		if releaseErr := releaseOwnKafkaKeeperLocks(k, resolvedPath, tableTitle); releaseErr != nil {
			log.Warn().Msgf("%s: can't release kafka partition locks with value `%s` under %s/topic_partition_locks: %v", tableTitle, kafkaKeeperHoldOwner, resolvedPath, releaseErr)
		}
	}
}
