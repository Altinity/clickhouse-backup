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

// rbacObjectID - kind and full name of RBAC object, identifiers are stored without back quotes,
// database and table are filled only for ROW POLICY and MASKING POLICY
type rbacObjectID struct {
	kind     string
	name     string
	database string
	table    string
}

func (id rbacObjectID) fullName() string {
	if id.table == "" {
		return id.name
	}
	if id.database == "" {
		return id.name + " ON " + id.table
	}
	return id.name + " ON " + id.database + "." + id.table
}

// cutRBACIdentifier - cut one identifier from the beginning of sql, returns it without back quotes and the rest of sql,
// back quoted identifier can contain spaces and dots
func cutRBACIdentifier(sql string) (string, string) {
	if strings.HasPrefix(sql, "`") {
		for i := 1; i < len(sql); i++ {
			switch sql[i] {
			case '\\':
				// skip escaped character
				i++
			case '`':
				return sql[1:i], sql[i+1:]
			}
		}
		return sql[1:], ""
	}
	if end := strings.IndexAny(sql, " .;\n"); end >= 0 {
		return sql[:end], sql[end:]
	}
	return sql, ""
}

// detectRBACObjectID - returns kind and full name of RBAC object from ATTACH query to compare RBAC objects,
// detectRBACObject can't be used for it, it cuts the name on the first space,
// so `john smith` and `john doe` are the same `john`, and keeps back quotes in policy names,
// while clickhouse-server 22.3 writes `ATTACH SETTINGS PROFILE name` and 26.8 writes the same name back quoted
func (b *Backuper) detectRBACObjectID(sql string) (rbacObjectID, error) {
	kind, _, detectErr := b.detectRBACObject(sql)
	if detectErr != nil {
		return rbacObjectID{}, detectErr
	}
	id := rbacObjectID{kind: kind}
	var rest string
	id.name, rest = cutRBACIdentifier(strings.TrimLeft(strings.TrimPrefix(sql, "ATTACH "+kind), " "))
	if (kind == "ROW POLICY" || kind == "MASKING POLICY") && strings.HasPrefix(rest, " ON ") {
		id.table, rest = cutRBACIdentifier(strings.TrimPrefix(rest, " ON "))
		if strings.HasPrefix(rest, ".") {
			id.database = id.table
			id.table, _ = cutRBACIdentifier(strings.TrimPrefix(rest, "."))
		}
	}
	return id, nil
}

// findRBACSQLFilesDuplicatedInKeeperDump - returns base names of backup access `<uuid>.sql` files with RBAC objects,
// which have the same kind and name as the objects in the `<user_directory>.jsonl` keeper dumps of the same backup,
// `create --rbac` stored `*.sql` left in the access path after a switch from `local_directory`
// to `replicated` user directory next to the keeper dump, which contains the live objects,
// look https://github.com/Altinity/clickhouse-backup/issues/1666
func (b *Backuper) findRBACSQLFilesDuplicatedInKeeperDump(sqlFiles, jsonLFiles []string) ([]string, error) {
	duplicatedSQLFiles := make([]string, 0)
	if len(sqlFiles) == 0 || len(jsonLFiles) == 0 {
		return duplicatedSQLFiles, nil
	}
	keeperObjects := make(map[rbacObjectID]string)
	for _, jsonLFile := range jsonLFiles {
		walkErr := keeper.WalkDumpFile(jsonLFile, func(node keeper.DumpNode) error {
			if !strings.HasPrefix(node.Path, "uuid/") || len(node.Value) == 0 {
				return nil
			}
			id, detectErr := b.detectRBACObjectID(string(node.Value))
			if detectErr != nil {
				return errors.Wrapf(detectErr, "detectRBACObjectID %s", node.Path)
			}
			keeperObjects[id] = filepath.Base(jsonLFile)
			return nil
		})
		if walkErr != nil {
			return nil, errors.Wrapf(walkErr, "findRBACSQLFilesDuplicatedInKeeperDump(%s)", jsonLFile)
		}
	}
	for _, sqlFile := range sqlFiles {
		sql, readErr := os.ReadFile(sqlFile)
		if readErr != nil {
			return nil, errors.Wrapf(readErr, "can't read %s", sqlFile)
		}
		id, detectErr := b.detectRBACObjectID(string(sql))
		if detectErr != nil {
			return nil, errors.Wrapf(detectErr, "detectRBACObjectID %s", sqlFile)
		}
		if jsonLFileName, duplicated := keeperObjects[id]; duplicated {
			log.Warn().Msgf("RBAC object kind=%s, name=%s present in backup as %s and in keeper dump %s, will restore only keeper dump copy", id.kind, id.fullName(), filepath.Base(sqlFile), jsonLFileName)
			duplicatedSQLFiles = append(duplicatedSQLFiles, filepath.Base(sqlFile))
		}
	}
	return duplicatedSQLFiles, nil
}

func isIgnoredRBACSQLFile(sqlFile string, ignoredSQLFiles []string) bool {
	for _, ignored := range ignoredSQLFiles {
		if filepath.Base(sqlFile) == ignored {
			return true
		}
	}
	return false
}
