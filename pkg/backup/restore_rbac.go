package backup

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/keeper"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// keeperPrefixesRBAC - RBAC object kind to the `unique_char` used by ClickHouse
// ReplicatedAccessStorage, look src/Access/Common/AccessEntityType.cpp
var keeperPrefixesRBAC = map[string]string{
	"ROLE":             "R",
	"ROW POLICY":       "P",
	"SETTINGS PROFILE": "S",
	"QUOTA":            "Q",
	"USER":             "U",
	"MASKING POLICY":   "M",
}

// convertKeeperDumpToLocalSQL - convert `<user_directory>.jsonl` keeper dump to `<accessPath>/<uuid>.sql` files,
// which allow restoring RBAC objects backed up from a `replicated` user directory
// to a server which have only `local_directory` user directory,
// look https://github.com/Altinity/clickhouse-backup/issues/881
func (b *Backuper) convertKeeperDumpToLocalSQL(jsonLFile, accessPath string, ignoredKeeperUuids map[string]struct{}, disks []clickhouse.Disk) (int, error) {
	converted := 0
	walkErr := keeper.WalkDumpFile(jsonLFile, func(node keeper.DumpNode) error {
		if !strings.HasPrefix(node.Path, "uuid/") || len(node.Value) == 0 {
			return nil
		}
		uuid := strings.TrimPrefix(node.Path, "uuid/")
		if _, ignore := ignoredKeeperUuids[uuid]; ignore {
			log.Info().Msgf("skip convert keeper node uuid/%s to %s/%s.sql", uuid, accessPath, uuid)
			return nil
		}
		sqlFile := path.Join(accessPath, uuid+".sql")
		if err := os.WriteFile(sqlFile, node.Value, 0644); err != nil {
			return errors.Wrapf(err, "can't write %s", sqlFile)
		}
		if err := filesystemhelper.Chown(sqlFile, b.ch, disks, false); err != nil {
			return errors.Wrapf(err, "can't chown %s", sqlFile)
		}
		converted++
		return nil
	})
	if walkErr != nil {
		return converted, errors.Wrapf(walkErr, "convertKeeperDumpToLocalSQL(%s)", jsonLFile)
	}
	return converted, nil
}

// planKeeperNodesForSQL - build the keeper nodes which ClickHouse ReplicatedAccessStorage creates
// for one RBAC object, look src/Access/ZooKeeperReplicator.cpp insertEntity,
// returned node paths are relative to the `<zookeeper_path>` of the replicated user directory
func (b *Backuper) planKeeperNodesForSQL(uuid, sql string) ([]keeper.DumpNode, error) {
	kind, name, detectErr := b.detectRBACObject(sql)
	if detectErr != nil {
		return nil, errors.Wrap(detectErr, "planKeeperNodesForSQL -> detectRBACObject")
	}
	keeperRBACTypePrefix, isExists := keeperPrefixesRBAC[kind]
	if !isExists {
		return nil, errors.Errorf("unsupported RBAC kind: %s", kind)
	}
	return []keeper.DumpNode{
		{Path: path.Join("uuid", uuid), Value: []byte(sql)},
		{Path: path.Join(keeperRBACTypePrefix, keeper.EscapeForFileName(name)), Value: []byte(uuid)},
	}, nil
}

// convertLocalSQLToKeeper - convert `<uuid>.sql` files from backup to keeper nodes,
// which allow restoring RBAC objects backed up from a `local_directory` user directory
// to a server which have only `replicated` user directory,
// look https://github.com/Altinity/clickhouse-backup/issues/881
func (b *Backuper) convertLocalSQLToKeeper(backupAccessPath string, ignoredSQLFiles []string, k *keeper.Keeper, replicatedAccessPath string) (int, error) {
	sqlFiles, globErr := filepath.Glob(path.Join(backupAccessPath, "*.sql"))
	if globErr != nil {
		return 0, errors.Wrapf(globErr, "convertLocalSQLToKeeper glob %s", backupAccessPath)
	}
	if len(sqlFiles) == 0 {
		return 0, nil
	}
	prefix := k.ResolvePath(replicatedAccessPath)
	if err := k.EnsureNode(prefix); err != nil {
		return 0, errors.Wrap(err, "convertLocalSQLToKeeper")
	}
	converted := 0
	for _, sqlFile := range sqlFiles {
		if isIgnoredRBACSQLFile(sqlFile, ignoredSQLFiles) {
			log.Info().Msgf("skip convert %s to %s", sqlFile, replicatedAccessPath)
			continue
		}
		sql, readErr := os.ReadFile(sqlFile)
		if readErr != nil {
			return converted, errors.Wrapf(readErr, "can't read %s", sqlFile)
		}
		uuid := strings.TrimSuffix(filepath.Base(sqlFile), ".sql")
		nodes, planErr := b.planKeeperNodesForSQL(uuid, string(sql))
		if planErr != nil {
			return converted, errors.Wrapf(planErr, "can't plan keeper nodes for %s", sqlFile)
		}
		for _, node := range nodes {
			// ensure the `<zookeeper_path>/<type char>` parent node exists, keeper doesn't create it recursively
			if parent := path.Dir(node.Path); parent != "." {
				if err := k.EnsureNode(path.Join(prefix, parent)); err != nil {
					return converted, errors.Wrap(err, "convertLocalSQLToKeeper")
				}
			}
			if err := k.Upsert(path.Join(prefix, node.Path), node.Value); err != nil {
				return converted, errors.Wrap(err, "convertLocalSQLToKeeper")
			}
		}
		converted++
	}
	return converted, nil
}

func isIgnoredRBACSQLFile(sqlFile string, ignoredSQLFiles []string) bool {
	for _, ignored := range ignoredSQLFiles {
		if filepath.Base(sqlFile) == ignored {
			return true
		}
	}
	return false
}
