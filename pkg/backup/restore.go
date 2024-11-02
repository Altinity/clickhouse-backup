package backup

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/resumable"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"io/fs"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mattn/go-shellwords"
	recursiveCopy "github.com/otiai10/copy"
	"github.com/yargevad/filepathx"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/keeper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
)

var CreateDatabaseRE = regexp.MustCompile(`(?m)^CREATE DATABASE (\s*)(\S+)(\s*)`)

// Restore - restore tables matched by tablePattern from backupName
func (b *Backuper) Restore(backupName, tablePattern string, databaseMapping, tableMapping, partitions []string, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, resume bool, backupVersion string, commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	startRestore := time.Now()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	if err := b.prepareRestoreMapping(databaseMapping, "database"); err != nil {
		return err
	}
	if err := b.prepareRestoreMapping(tableMapping, "table"); err != nil {
		return err
	}

	doRestoreData := (!schemaOnly && !rbacOnly && !configsOnly) || dataOnly

	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	if backupName == "" {
		_ = b.PrintLocalBackups(ctx, "all")
		return fmt.Errorf("select backup for restore")
	}
	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		return err
	}
	version, err := b.ch.GetVersion(ctx)
	if err != nil {
		return err
	}
	b.DefaultDataPath, err = b.ch.GetDefaultPath(disks)
	if err != nil {
		log.Warn().Msgf("%v", err)
		return ErrUnknownClickhouseDataPath
	}
	if b.cfg.General.RestoreSchemaOnCluster != "" {
		if b.cfg.General.RestoreSchemaOnCluster, err = b.ch.ApplyMacros(ctx, b.cfg.General.RestoreSchemaOnCluster); err != nil {
			log.Warn().Msgf("%v", err)
			return err
		}
	}
	b.adjustResumeFlag(resume)
	backupMetafileLocalPaths := []string{path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")}
	var backupMetadataBody []byte
	b.EmbeddedBackupDataPath, err = b.ch.GetEmbeddedBackupPath(disks)
	if err == nil && b.EmbeddedBackupDataPath != "" {
		backupMetafileLocalPaths = append(backupMetafileLocalPaths, path.Join(b.EmbeddedBackupDataPath, backupName, "metadata.json"))
	} else if b.cfg.ClickHouse.UseEmbeddedBackupRestore && b.cfg.ClickHouse.EmbeddedBackupDisk == "" {
		b.EmbeddedBackupDataPath = b.DefaultDataPath
	} else if err != nil {
		return err
	}
	for _, metadataPath := range backupMetafileLocalPaths {
		backupMetadataBody, err = os.ReadFile(metadataPath)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	backupMetadata := metadata.BackupMetadata{}
	if err := json.Unmarshal(backupMetadataBody, &backupMetadata); err != nil {
		return err
	}
	b.isEmbedded = strings.Contains(backupMetadata.Tags, "embedded")

	if schemaOnly || doRestoreData {
		for _, database := range backupMetadata.Databases {
			targetDB := database.Name
			if !IsInformationSchema(targetDB) {
				if err = b.restoreEmptyDatabase(ctx, targetDB, tablePattern, database, dropExists, schemaOnly, ignoreDependencies, version); err != nil {
					return err
				}
			}
		}
	}
	if len(backupMetadata.Tables) == 0 {
		// corner cases for https://github.com/Altinity/clickhouse-backup/issues/832
		if !restoreRBAC && !rbacOnly && !restoreConfigs && !configsOnly {
			if !b.cfg.General.AllowEmptyBackups {
				err = fmt.Errorf("'%s' doesn't contains tables for restore, if you need it, you can setup `allow_empty_backups: true` in `general` config section", backupName)
				log.Error().Msgf("%v", err)
				return err
			}
			log.Warn().Msgf("'%s' doesn't contains tables for restore", backupName)
			return nil
		}
	}
	needRestart := false
	if rbacOnly || restoreRBAC {
		if err := b.restoreRBAC(ctx, backupName, disks, version, dropExists); err != nil {
			return err
		}
		log.Info().Msgf("RBAC successfully restored")
		needRestart = true
	}
	if configsOnly || restoreConfigs {
		if err := b.restoreConfigs(backupName, disks); err != nil {
			return err
		}
		log.Info().Msgf("CONFIGS successfully restored")
		needRestart = true
	}

	if needRestart {
		if err := b.restartClickHouse(ctx, backupName); err != nil {
			return err
		}
		if rbacOnly || configsOnly {
			return nil
		}
	}
	isObjectDiskPresents := false
	if b.cfg.General.RemoteStorage != "custom" {
		for _, d := range disks {
			if isObjectDiskPresents = b.isDiskTypeObject(d.Type); isObjectDiskPresents {
				break
			}
		}
	}
	if (b.cfg.ClickHouse.UseEmbeddedBackupRestore && b.cfg.ClickHouse.EmbeddedBackupDisk == "") || isObjectDiskPresents {
		if b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, backupName); err != nil {
			return err
		}
		if err = b.dst.Connect(ctx); err != nil {
			return fmt.Errorf("BackupDestination for embedded or object disk: can't connect to %s: %v", b.dst.Kind(), err)
		}
		defer func() {
			if err := b.dst.Close(ctx); err != nil {
				log.Warn().Msgf("can't close BackupDestination error: %v", err)
			}
		}()
		if b.resume {
			needClean := "false"
			if dropExists || !dataOnly {
				needClean = fmt.Sprintf("true.%d", rand.Uint64())
			}
			b.resumableState = resumable.NewState(b.GetStateDir(), backupName, "restore", map[string]interface{}{
				"tablePattern": tablePattern,
				"partitions":   partitions,
				"schemaOnly":   schemaOnly,
				"dataOnly":     dataOnly,
				"dropExists":   dropExists,
				"needClean":    needClean,
			})
			defer b.resumableState.Close()
		}
	}
	var tablesForRestore ListOfTables
	var partitionsNames map[metadata.TableTitle][]string
	if tablePattern == "" {
		tablePattern = "*"
	}
	metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
	if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		metadataPath = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata")
	}

	if !rbacOnly && !configsOnly {
		tablesForRestore, partitionsNames, err = b.getTablesForRestoreLocal(ctx, backupName, metadataPath, tablePattern, dropExists, partitions)
		if err != nil {
			return err
		}
	}
	if schemaOnly || dropExists || (schemaOnly == dataOnly && !rbacOnly && !configsOnly) {
		if err = b.RestoreSchema(ctx, backupName, backupMetadata, disks, tablesForRestore, ignoreDependencies, version); err != nil {
			return err
		}
	}
	// https://github.com/Altinity/clickhouse-backup/issues/756
	if dataOnly && !schemaOnly && !rbacOnly && !configsOnly && len(partitions) > 0 {
		if err = b.dropExistPartitions(ctx, tablesForRestore, partitionsNames, partitions, version); err != nil {
			return err
		}

	}
	if dataOnly || (schemaOnly == dataOnly && !rbacOnly && !configsOnly) {
		if err := b.RestoreData(ctx, backupName, backupMetadata, dataOnly, metadataPath, tablePattern, partitions, disks, version); err != nil {
			return err
		}
	}
	// do not create UDF when use --data, --rbac-only, --configs-only flags, https://github.com/Altinity/clickhouse-backup/issues/697
	if schemaOnly || (schemaOnly == dataOnly && !rbacOnly && !configsOnly) {
		for _, function := range backupMetadata.Functions {
			if err = b.ch.CreateUserDefinedFunction(function.Name, function.CreateQuery, b.cfg.General.RestoreSchemaOnCluster); err != nil {
				return err
			}
		}
	}

	//clean partially downloaded requiredBackup
	if backupMetadata.RequiredBackup != "" {
		if err = b.cleanPartialRequiredBackup(ctx, disks, backupMetadata.BackupName); err != nil {
			return err
		}
	}

	log.Info().Fields(map[string]interface{}{
		"operation": "restore",
		"duration":  utils.HumanizeDuration(time.Since(startRestore)),
		"version":   backupVersion,
	}).Msg("done")
	return nil
}

func (b *Backuper) getTablesForRestoreLocal(ctx context.Context, backupName string, metadataPath string, tablePattern string, dropTable bool, partitions []string) (ListOfTables, map[metadata.TableTitle][]string, error) {
	var tablesForRestore ListOfTables
	var partitionsNames map[metadata.TableTitle][]string
	info, err := os.Stat(metadataPath)
	// corner cases for https://github.com/Altinity/clickhouse-backup/issues/832
	if err != nil {
		if !b.cfg.General.AllowEmptyBackups {
			return nil, nil, err
		}
		if !os.IsNotExist(err) {
			return nil, nil, err
		}
		return nil, nil, nil
	}
	if !info.IsDir() {
		return nil, nil, fmt.Errorf("%s is not a dir", metadataPath)
	}
	tablesForRestore, partitionsNames, err = b.getTableListByPatternLocal(ctx, metadataPath, tablePattern, dropTable, partitions)
	if err != nil {
		return nil, nil, err
	}
	// if restore-database-mapping is specified, create database in mapping rules instead of in backup files.
	if len(b.cfg.General.RestoreDatabaseMapping) > 0 {
		err = changeTableQueryToAdjustDatabaseMapping(&tablesForRestore, b.cfg.General.RestoreDatabaseMapping)
		if err != nil {
			return nil, nil, err
		}
		partitionsNames, err = changePartitionsToAdjustDatabaseMapping(partitionsNames, b.cfg.General.RestoreDatabaseMapping)
		if err != nil {
			return nil, nil, err
		}
	}

	// if restore-table-mapping is specified, create table in mapping rules instead of in backup files.
	// https://github.com/Altinity/clickhouse-backup/issues/937
	if len(b.cfg.General.RestoreTableMapping) > 0 {
		err = changeTableQueryToAdjustTableMapping(&tablesForRestore, b.cfg.General.RestoreTableMapping)
		if err != nil {
			return nil, nil, err
		}
		partitionsNames, err = changePartitionsToAdjustTableMapping(partitionsNames, b.cfg.General.RestoreTableMapping)
		if err != nil {
			return nil, nil, err
		}
	}

	if len(tablesForRestore) == 0 {
		return nil, nil, fmt.Errorf("not found schemas by %s in %s, also check skip_tables and skip_table_engines setting", tablePattern, backupName)
	}
	return tablesForRestore, partitionsNames, nil
}

func (b *Backuper) restartClickHouse(ctx context.Context, backupName string) error {
	log.Warn().Msgf("%s contains `access` or `configs` directory, so we need exec %s", backupName, b.ch.Config.RestartCommand)
	for _, cmd := range strings.Split(b.ch.Config.RestartCommand, ";") {
		cmd = strings.Trim(cmd, " \t\r\n")
		if strings.HasPrefix(cmd, "sql:") {
			cmd = strings.TrimPrefix(cmd, "sql:")
			if err := b.ch.QueryContext(ctx, cmd); err != nil {
				log.Warn().Msgf("restart sql: %s, error: %v", cmd, err)
			}
		}
		if strings.HasPrefix(cmd, "exec:") {
			cmd = strings.TrimPrefix(cmd, "exec:")
			if err := b.executeShellCommandWithTimeout(ctx, cmd); err != nil {
				return err
			}
		}
	}
	b.ch.Close()
	closeCtx, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()

breakByReconnect:
	for i := 1; i <= 60; i++ {
		select {
		case <-closeCtx.Done():
			return fmt.Errorf("reconnect after '%s' timeout exceeded", b.ch.Config.RestartCommand)
		default:
			if err := b.ch.Connect(); err == nil {
				break breakByReconnect
			}
			log.Info().Msg("wait 3 seconds")
			time.Sleep(3 * time.Second)
		}
	}
	return nil
}

func (b *Backuper) executeShellCommandWithTimeout(ctx context.Context, cmd string) error {
	shellCmd, err := shellwords.Parse(cmd)
	if err != nil {
		return err
	}
	shellCtx, shellCancel := context.WithTimeout(ctx, 180*time.Second)
	defer shellCancel()
	log.Info().Msgf("run %s", cmd)
	var out []byte
	if len(shellCmd) > 1 {
		out, err = exec.CommandContext(shellCtx, shellCmd[0], shellCmd[1:]...).CombinedOutput()
	} else {
		out, err = exec.CommandContext(shellCtx, shellCmd[0]).CombinedOutput()
	}
	log.Debug().Msgf(string(out))
	if err != nil {
		log.Warn().Msgf("restart exec: %s, error: %v", cmd, err)
	}
	return nil
}

func (b *Backuper) restoreEmptyDatabase(ctx context.Context, targetDB, tablePattern string, database metadata.DatabasesMeta, dropTable, schemaOnly, ignoreDependencies bool, version int) error {
	isMapped := false
	if targetDB, isMapped = b.cfg.General.RestoreDatabaseMapping[database.Name]; !isMapped {
		targetDB = database.Name
	}
	// https://github.com/Altinity/clickhouse-backup/issues/583
	// https://github.com/Altinity/clickhouse-backup/issues/663
	if ShallSkipDatabase(b.cfg, targetDB, tablePattern) {
		return nil
	}
	// https://github.com/Altinity/clickhouse-backup/issues/514
	if schemaOnly && dropTable {
		onCluster := ""
		if b.cfg.General.RestoreSchemaOnCluster != "" {
			onCluster = fmt.Sprintf(" ON CLUSTER '%s'", b.cfg.General.RestoreSchemaOnCluster)
		}
		// https://github.com/Altinity/clickhouse-backup/issues/651
		settings := ""
		if ignoreDependencies {
			if version >= 21012000 {
				settings = "SETTINGS check_table_dependencies=0"
			}
		}
		if _, err := os.Create(path.Join(b.DefaultDataPath, "/flags/force_drop_table")); err != nil {
			return err
		}
		if err := b.ch.QueryContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s` %s SYNC %s", targetDB, onCluster, settings)); err != nil {
			return err
		}

	}
	substitution := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS ${1}`%s`${3}", targetDB)
	if err := b.ch.CreateDatabaseFromQuery(ctx, CreateDatabaseRE.ReplaceAllString(database.Query, substitution), b.cfg.General.RestoreSchemaOnCluster); err != nil {
		return err
	}
	return nil
}

func (b *Backuper) prepareRestoreMapping(objectMapping []string, objectType string) error {
	if objectType != "database" && objectType != "table" {
		return fmt.Errorf("objectType must be one of `database` or `table`")
	}
	for i := 0; i < len(objectMapping); i++ {
		splitByCommas := strings.Split(objectMapping[i], ",")
		for _, m := range splitByCommas {
			splitByColon := strings.Split(m, ":")
			if len(splitByColon) != 2 {
				objectTypeTitleCase := cases.Title(language.Und).String(objectType)
				return fmt.Errorf("restore-%s-mapping %s should only have src%s:destination%s format for each map rule", objectType, m, objectTypeTitleCase, objectTypeTitleCase)
			}
			if objectType == "database" {
				b.cfg.General.RestoreDatabaseMapping[splitByColon[0]] = splitByColon[1]
			} else {
				b.cfg.General.RestoreTableMapping[splitByColon[0]] = splitByColon[1]
			}
		}
	}
	return nil
}

// restoreRBAC - copy backup_name>/rbac folder to access_data_path
func (b *Backuper) restoreRBAC(ctx context.Context, backupName string, disks []clickhouse.Disk, version int, dropExists bool) error {
	accessPath, err := b.ch.GetAccessManagementPath(ctx, nil)
	if err != nil {
		return err
	}
	var k *keeper.Keeper
	replicatedUserDirectories := make([]clickhouse.UserDirectory, 0)
	if err = b.ch.SelectContext(ctx, &replicatedUserDirectories, "SELECT name FROM system.user_directories WHERE type='replicated'"); err == nil && len(replicatedUserDirectories) > 0 {
		k = &keeper.Keeper{}
		if connErr := k.Connect(ctx, b.ch); connErr != nil {
			return fmt.Errorf("but can't connect to keeper: %v", connErr)
		}
		defer k.Close()
	}

	// https://github.com/Altinity/clickhouse-backup/issues/851
	if err = b.restoreRBACResolveAllConflicts(ctx, backupName, accessPath, version, k, replicatedUserDirectories, dropExists); err != nil {
		return err
	}

	if err = b.restoreBackupRelatedDir(backupName, "access", accessPath, disks, []string{"*.jsonl"}); err == nil {
		markFile := path.Join(accessPath, "need_rebuild_lists.mark")
		log.Info().Msgf("create %s for properly rebuild RBAC after restart clickhouse-server", markFile)
		file, err := os.Create(markFile)
		if err != nil {
			return err
		}
		_ = file.Close()
		_ = filesystemhelper.Chown(markFile, b.ch, disks, false)
		listFilesPattern := path.Join(accessPath, "*.list")
		log.Info().Msgf("remove %s for properly rebuild RBAC after restart clickhouse-server", listFilesPattern)
		if listFiles, err := filepathx.Glob(listFilesPattern); err != nil {
			return err
		} else {
			for _, f := range listFiles {
				if err := os.Remove(f); err != nil {
					return err
				}
			}
		}
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	if err = b.restoreRBACReplicated(backupName, "access", k, replicatedUserDirectories); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (b *Backuper) restoreRBACResolveAllConflicts(ctx context.Context, backupName string, accessPath string, version int, k *keeper.Keeper, replicatedUserDirectories []clickhouse.UserDirectory, dropExists bool) error {
	backupAccessPath := path.Join(b.DefaultDataPath, "backup", backupName, "access")

	walkErr := filepath.Walk(backupAccessPath, func(fPath string, fInfo fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fInfo.IsDir() {
			return nil
		}
		if strings.HasSuffix(fPath, ".sql") {
			sql, readErr := os.ReadFile(fPath)
			if readErr != nil {
				return readErr
			}
			if resolveErr := b.resolveRBACConflictIfExist(ctx, string(sql), accessPath, version, k, replicatedUserDirectories, dropExists); resolveErr != nil {
				return resolveErr
			}
			log.Debug().Msgf("%s b.resolveRBACConflictIfExist(%s) no error", fPath, string(sql))
		}
		if strings.HasSuffix(fPath, ".jsonl") {
			file, openErr := os.Open(fPath)
			if openErr != nil {
				return openErr
			}

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				data := keeper.DumpNode{}
				jsonErr := json.Unmarshal([]byte(line), &data)
				if jsonErr != nil {
					log.Error().Msgf("can't %s json.Unmarshal error: %v line: %s", fPath, line, jsonErr)
					continue
				}
				if strings.HasPrefix(data.Path, "uuid/") {
					if resolveErr := b.resolveRBACConflictIfExist(ctx, data.Value, accessPath, version, k, replicatedUserDirectories, dropExists); resolveErr != nil {
						return resolveErr
					}
					log.Debug().Msgf("%s:%s b.resolveRBACConflictIfExist(%s) no error", fPath, data.Path, data.Value)
				}

			}
			if scanErr := scanner.Err(); scanErr != nil {
				return scanErr
			}

			if closeErr := file.Close(); closeErr != nil {
				log.Warn().Msgf("can't close %s error: %v", fPath, closeErr)
			}

		}
		return nil
	})
	if !os.IsNotExist(walkErr) {
		return walkErr
	}
	return nil
}

func (b *Backuper) resolveRBACConflictIfExist(ctx context.Context, sql string, accessPath string, version int, k *keeper.Keeper, replicatedUserDirectories []clickhouse.UserDirectory, dropExists bool) error {
	kind, name, detectErr := b.detectRBACObject(sql)
	if detectErr != nil {
		return detectErr
	}
	if isExists, existsRBACType, existsRBACObjectIds := b.isRBACExists(ctx, kind, name, accessPath, version, k, replicatedUserDirectories); isExists {
		log.Warn().Msgf("RBAC object kind=%s, name=%s already present, will %s", kind, name, b.cfg.General.RBACConflictResolution)
		if b.cfg.General.RBACConflictResolution == "recreate" || dropExists {
			if dropErr := b.dropExistsRBAC(ctx, kind, name, accessPath, existsRBACType, existsRBACObjectIds, k); dropErr != nil {
				return dropErr
			}
			return nil
		}
		if b.cfg.General.RBACConflictResolution == "fail" {
			return fmt.Errorf("RBAC object kind=%s, name=%s already present, fix current RBAC objects to resolve conflicts", kind, name)
		}
	}
	return nil
}

func (b *Backuper) isRBACExists(ctx context.Context, kind string, name string, accessPath string, version int, k *keeper.Keeper, replicatedUserDirectories []clickhouse.UserDirectory) (bool, string, []string) {
	//search in sql system.users, system.quotas, system.row_policies, system.roles, system.settings_profiles
	if version > 22003000 {
		var rbacSystemTableNames = map[string]string{
			"ROLE":             "roles",
			"ROW POLICY":       "row_policies",
			"SETTINGS PROFILE": "settings_profiles",
			"QUOTA":            "quotas",
			"USER":             "users",
		}
		systemTable, systemTableExists := rbacSystemTableNames[kind]
		if !systemTableExists {
			log.Error().Msgf("unsupported RBAC object kind: %s", kind)
			return false, "", nil
		}
		isRBACExistsSQL := fmt.Sprintf("SELECT toString(id) AS id, name FROM `system`.`%s` WHERE name=? LIMIT 1", systemTable)
		existsRBACRow := make([]clickhouse.RBACObject, 0)
		if err := b.ch.SelectContext(ctx, &existsRBACRow, isRBACExistsSQL, name); err != nil {
			log.Warn().Msgf("RBAC object resolve failed, check SQL GRANTS or <access_management> settings for user which you use to connect to clickhouse-server, kind: %s, name: %s, error: %v", kind, name, err)
			return false, "", nil
		}
		if len(existsRBACRow) != 0 {
			return true, "sql", []string{existsRBACRow[0].Id}
		}
	}

	checkRBACExists := func(sql string) bool {
		existsKind, existsName, detectErr := b.detectRBACObject(sql)
		if detectErr != nil {
			log.Warn().Msgf("isRBACExists error: %v", detectErr)
			return false
		}
		if existsKind == kind && existsName == name {
			return true
		}
		return false
	}

	// search in local user directory
	if sqlFiles, globErr := filepath.Glob(path.Join(accessPath, "*.sql")); globErr == nil {
		existsRBACObjectIds := []string{}
		for _, f := range sqlFiles {
			sql, readErr := os.ReadFile(f)
			if readErr != nil {
				log.Warn().Msgf("read %s error: %v", f, readErr)
				continue
			}
			if checkRBACExists(string(sql)) {
				existsRBACObjectIds = append(existsRBACObjectIds, strings.TrimSuffix(filepath.Base(f), filepath.Ext(f)))
			}
		}
		if len(existsRBACObjectIds) > 0 {
			return true, "local", existsRBACObjectIds
		}
	} else {
		log.Warn().Msgf("access/*.sql error: %v", globErr)
	}

	//search in keeper replicated user directory
	if k != nil && len(replicatedUserDirectories) > 0 {
		var existsObjectIds []string
		for _, userDirectory := range replicatedUserDirectories {
			replicatedAccessPath, getAccessErr := k.GetReplicatedAccessPath(userDirectory.Name)
			if getAccessErr != nil {
				log.Warn().Msgf("b.isRBACExists -> k.GetReplicatedAccessPath error: %v", getAccessErr)
				continue
			}
			walkErr := k.Walk(replicatedAccessPath, "uuid", true, func(node keeper.DumpNode) (bool, error) {
				if node.Value == "" {
					return false, nil
				}
				if checkRBACExists(node.Value) {
					existsObjectId := strings.TrimPrefix(node.Path, path.Join(replicatedAccessPath, "uuid")+"/")
					existsObjectIds = append(existsObjectIds, existsObjectId)
					return true, nil
				}
				return false, nil
			})
			if walkErr != nil {
				log.Warn().Msgf("b.isRBACExists -> k.Walk error: %v", walkErr)
				continue
			}
			if len(existsObjectIds) > 0 {
				return true, userDirectory.Name, existsObjectIds
			}
		}
	}
	return false, "", nil
}

// https://github.com/Altinity/clickhouse-backup/issues/930
var needQuoteRBACRE = regexp.MustCompile(`[^0-9a-zA-Z_]`)

func (b *Backuper) dropExistsRBAC(ctx context.Context, kind string, name string, accessPath string, rbacType string, rbacObjectIds []string, k *keeper.Keeper) error {
	//sql
	if rbacType == "sql" {
		// https://github.com/Altinity/clickhouse-backup/issues/930
		if needQuoteRBACRE.MatchString(name) && !strings.HasPrefix(name, "`") && !strings.HasPrefix(name, `"`) && !strings.HasPrefix(name, "'") && !strings.Contains(name, " ON ") {
			name = "`" + name + "`"
		}
		dropSQL := fmt.Sprintf("DROP %s IF EXISTS %s", kind, name)
		return b.ch.QueryContext(ctx, dropSQL)
	}
	//local
	if rbacType == "local" {
		for _, rbacObjectId := range rbacObjectIds {
			if err := os.Remove(path.Join(accessPath, rbacObjectId+".sql")); err != nil {
				return err
			}
		}
		return nil
	}
	//keeper
	var keeperPrefixesRBAC = map[string]string{
		"ROLE":             "R",
		"ROW POLICY":       "P",
		"SETTINGS PROFILE": "S",
		"QUOTA":            "Q",
		"USER":             "U",
	}
	keeperRBACTypePrefix, isKeeperRBACTypePrefixExists := keeperPrefixesRBAC[kind]
	if !isKeeperRBACTypePrefixExists {
		return fmt.Errorf("unsupported RBAC kind: %s", kind)
	}
	prefix, err := k.GetReplicatedAccessPath(rbacType)
	if err != nil {
		return fmt.Errorf("b.dropExistsRBAC -> k.GetReplicatedAccessPath error: %v", err)
	}
	deletedNodes := make([]string, len(rbacObjectIds))
	for i := range rbacObjectIds {
		deletedNodes[i] = path.Join(prefix, "uuid", rbacObjectIds[i])
	}
	walkErr := k.Walk(prefix, keeperRBACTypePrefix, true, func(node keeper.DumpNode) (bool, error) {
		for _, rbacObjectId := range rbacObjectIds {
			if node.Value == rbacObjectId {
				deletedNodes = append(deletedNodes, node.Path)
			}
		}
		return false, nil
	})
	if walkErr != nil {
		return fmt.Errorf("b.dropExistsRBAC -> k.Walk(%s/%s) error: %v", prefix, keeperRBACTypePrefix, walkErr)
	}

	for _, nodePath := range deletedNodes {
		if deleteErr := k.Delete(nodePath); deleteErr != nil {
			return fmt.Errorf("b.dropExistsRBAC -> k.Delete(%s) error: %v", nodePath, deleteErr)
		}
	}
	return nil
}

func (b *Backuper) detectRBACObject(sql string) (string, string, error) {
	var kind, name string
	var detectErr error

	// Define the map of prefixes and their corresponding kinds.
	prefixes := map[string]string{
		"ATTACH ROLE":             "ROLE",
		"ATTACH ROW POLICY":       "ROW POLICY",
		"ATTACH SETTINGS PROFILE": "SETTINGS PROFILE",
		"ATTACH QUOTA":            "QUOTA",
		"ATTACH USER":             "USER",
	}

	// Iterate over the prefixes to find a match.
	for prefix, k := range prefixes {
		if strings.HasPrefix(sql, prefix) {
			kind = k
			// Extract the name from the SQL query.
			if semicolonIdx := strings.Index(sql, ";"); semicolonIdx >= 0 {
				name = strings.TrimSpace(strings.TrimPrefix(sql[:semicolonIdx], prefix))
			} else {
				name = strings.TrimSpace(strings.TrimPrefix(sql, prefix))
			}
			break
		}
	}

	// If no match is found, return an error.
	if kind == "" {
		detectErr = fmt.Errorf("unable to detect RBAC object kind from SQL query: %s", sql)
		return kind, name, detectErr
	}
	names := strings.SplitN(name, " ", 2)
	if len(names) > 1 && strings.HasPrefix(names[1], "ON ") {
		names = strings.SplitN(name, " ", 4)
		name = strings.Join(names[0:3], " ")
	} else {
		name = names[0]
	}
	if kind != "ROW POLICY" {
		name = strings.Trim(name, "`")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		detectErr = fmt.Errorf("unable to detect RBAC object name from SQL query: %s", sql)
		return kind, name, detectErr
	}
	return kind, name, detectErr
}

// @todo think about restore RBAC from replicated to local *.sql
func (b *Backuper) restoreRBACReplicated(backupName string, backupPrefixDir string, k *keeper.Keeper, replicatedUserDirectories []clickhouse.UserDirectory) error {
	if k == nil || len(replicatedUserDirectories) == 0 {
		return nil
	}
	srcBackupDir := path.Join(b.DefaultDataPath, "backup", backupName, backupPrefixDir)
	info, err := os.Stat(srcBackupDir)
	if err != nil {
		log.Warn().Msgf("stat: %s error: %v", srcBackupDir, err)
		return err
	}

	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", srcBackupDir)
	}
	jsonLFiles, err := filepathx.Glob(path.Join(srcBackupDir, "*.jsonl"))
	if err != nil {
		return err
	}
	if len(jsonLFiles) == 0 {
		return nil
	}
	restoreReplicatedRBACMap := make(map[string]string, len(jsonLFiles))
	for _, jsonLFile := range jsonLFiles {
		for _, userDirectory := range replicatedUserDirectories {
			if strings.HasSuffix(jsonLFile, userDirectory.Name+".jsonl") {
				restoreReplicatedRBACMap[jsonLFile] = userDirectory.Name
			}
		}
		if _, exists := restoreReplicatedRBACMap[jsonLFile]; !exists {
			restoreReplicatedRBACMap[jsonLFile] = replicatedUserDirectories[0].Name
		}
	}
	for jsonLFile, userDirectoryName := range restoreReplicatedRBACMap {
		replicatedAccessPath, err := k.GetReplicatedAccessPath(userDirectoryName)
		if err != nil {
			return err
		}
		log.Info().Msgf("keeper.Restore(%s) -> %s", jsonLFile, replicatedAccessPath)
		if err := k.Restore(jsonLFile, replicatedAccessPath); err != nil {
			return err
		}
	}
	return nil
}

// restoreConfigs - copy backup_name/configs folder to /etc/clickhouse-server/
func (b *Backuper) restoreConfigs(backupName string, disks []clickhouse.Disk) error {
	if err := b.restoreBackupRelatedDir(backupName, "configs", b.ch.Config.ConfigDir, disks, nil); err != nil && os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}

func (b *Backuper) restoreBackupRelatedDir(backupName, backupPrefixDir, destinationDir string, disks []clickhouse.Disk, skipPatterns []string) error {
	srcBackupDir := path.Join(b.DefaultDataPath, "backup", backupName, backupPrefixDir)
	info, err := os.Stat(srcBackupDir)
	if err != nil {
		log.Warn().Msgf("stat: %s error: %v", srcBackupDir, err)
		return err
	}
	existsFiles, _ := os.ReadDir(destinationDir)
	for _, existsF := range existsFiles {
		existsI, _ := existsF.Info()
		log.Debug().Msgf("%s %v %v", path.Join(destinationDir, existsF.Name()), existsI.Size(), existsI.ModTime())
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", srcBackupDir)
	}
	log.Debug().Msgf("copy %s -> %s", srcBackupDir, destinationDir)
	copyOptions := recursiveCopy.Options{
		OnDirExists: func(src, dst string) recursiveCopy.DirExistsAction {
			return recursiveCopy.Merge
		},
		Skip: func(srcinfo os.FileInfo, src, dst string) (bool, error) {
			for _, pattern := range skipPatterns {
				if matched, matchErr := filepath.Match(pattern, filepath.Base(src)); matchErr != nil || matched {
					return true, matchErr
				}
			}
			return false, nil
		},
	}
	if err := recursiveCopy.Copy(srcBackupDir, destinationDir, copyOptions); err != nil {
		return err
	}

	files, err := filepathx.Glob(path.Join(destinationDir, "**"))
	if err != nil {
		return err
	}
	files = append(files, destinationDir)
	for _, localFile := range files {
		if err := filesystemhelper.Chown(localFile, b.ch, disks, false); err != nil {
			return err
		}
	}
	return nil
}

// execute ALTER TABLE db.table DROP PARTITION for corner case when we try to restore backup with the same structure, https://github.com/Altinity/clickhouse-backup/issues/756
func (b *Backuper) dropExistPartitions(ctx context.Context, tablesForRestore ListOfTables, partitionsIdMap map[metadata.TableTitle][]string, partitions []string, version int) error {
	for _, table := range tablesForRestore {
		if !strings.Contains(table.Query, "MergeTree") {
			continue
		}
		partitionsIds, isExists := partitionsIdMap[metadata.TableTitle{Database: table.Database, Table: table.Table}]
		if !isExists {
			return fmt.Errorf("`%s`.`%s` doesn't contains %#v partitions", table.Database, table.Table, partitions)
		}
		partitionsSQL := fmt.Sprintf("DROP PARTITION %s", strings.Join(partitionsIds, ", DROP PARTITION "))
		settings := ""
		if version >= 19017000 {
			settings = "SETTINGS mutations_sync=2"
		}
		err := b.ch.QueryContext(ctx, fmt.Sprintf("ALTER TABLE `%s`.`%s` %s %s", table.Database, table.Table, partitionsSQL, settings))
		if err != nil {
			return err
		}
	}
	return nil
}

// RestoreSchema - restore schemas matched by tablePattern from backupName
func (b *Backuper) RestoreSchema(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, disks []clickhouse.Disk, tablesForRestore ListOfTables, ignoreDependencies bool, version int) error {
	startRestoreSchema := time.Now()
	if dropErr := b.dropExistsTables(tablesForRestore, ignoreDependencies, version); dropErr != nil {
		return dropErr
	}
	var restoreErr error
	if b.isEmbedded {
		restoreErr = b.restoreSchemaEmbedded(ctx, backupName, backupMetadata, disks, tablesForRestore, version)
	} else {
		restoreErr = b.restoreSchemaRegular(ctx, tablesForRestore, version)
	}
	if restoreErr != nil {
		return restoreErr
	}
	log.Info().Fields(map[string]interface{}{
		"backup":    backupName,
		"operation": "restore_schema",
		"duration":  utils.HumanizeDuration(time.Since(startRestoreSchema)),
	}).Msg("done")
	return nil
}

var UUIDWithMergeTreeRE = regexp.MustCompile(`^(.+)(UUID)(\s+)'([^']+)'(.+)({uuid})(.*)`)

var emptyReplicatedMergeTreeRE = regexp.MustCompile(`(?m)Replicated(MergeTree|ReplacingMergeTree|SummingMergeTree|AggregatingMergeTree|CollapsingMergeTree|VersionedCollapsingMergeTree|GraphiteMergeTree)\s*\(([^']*)\)(.*)`)

func (b *Backuper) restoreSchemaEmbedded(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, disks []clickhouse.Disk, tablesForRestore ListOfTables, version int) error {
	var err error
	if tablesForRestore == nil || len(tablesForRestore) == 0 {
		if !b.cfg.General.AllowEmptyBackups {
			return fmt.Errorf("no tables for restore")
		}
		log.Warn().Msgf("no tables for restore in embeddded backup %s/metadata.json", backupName)
		return nil
	}
	if b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		err = b.fixEmbeddedMetadataLocal(ctx, backupName, backupMetadata, disks, version)
	} else {
		err = b.fixEmbeddedMetadataRemote(ctx, backupName, version)
	}
	if err != nil {
		return err
	}
	return b.restoreEmbedded(ctx, backupName, true, false, version, tablesForRestore, nil)
}

func (b *Backuper) fixEmbeddedMetadataRemote(ctx context.Context, backupName string, chVersion int) error {
	objectDiskPath, err := b.getObjectDiskPath()
	if err != nil {
		return err
	}
	if walkErr := b.dst.WalkAbsolute(ctx, path.Join(objectDiskPath, backupName, "metadata"), true, func(ctx context.Context, fInfo storage.RemoteFile) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(fInfo.Name(), ".sql") {
			return nil
		}
		var fReader io.ReadCloser
		remoteFilePath := path.Join(objectDiskPath, backupName, "metadata", fInfo.Name())
		log.Debug().Msgf("read %s", remoteFilePath)
		fReader, err = b.dst.GetFileReaderAbsolute(ctx, path.Join(objectDiskPath, backupName, "metadata", fInfo.Name()))
		if err != nil {
			return err
		}
		var sqlBytes []byte
		sqlBytes, err = io.ReadAll(fReader)
		if err != nil {
			return err
		}
		sqlQuery, sqlMetadataChanged, fixSqlErr := b.fixEmbeddedMetadataSQLQuery(ctx, sqlBytes, remoteFilePath, chVersion)
		if fixSqlErr != nil {
			return fmt.Errorf("b.fixEmbeddedMetadataSQLQuery return error: %v", fixSqlErr)
		}
		log.Debug().Msgf("b.fixEmbeddedMetadataSQLQuery %s changed=%v", remoteFilePath, sqlMetadataChanged)
		if sqlMetadataChanged {
			err = b.dst.PutFileAbsolute(ctx, remoteFilePath, io.NopCloser(strings.NewReader(sqlQuery)))
			if err != nil {
				return err
			}
		}
		return nil
	}); walkErr != nil {
		return walkErr
	}
	return nil
}

func (b *Backuper) fixEmbeddedMetadataLocal(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, disks []clickhouse.Disk, chVersion int) error {
	metadataPath := path.Join(b.EmbeddedBackupDataPath, backupName, "metadata")
	if walkErr := filepath.Walk(metadataPath, func(filePath string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(filePath, ".sql") {
			return nil
		}
		if backupMetadata.DiskTypes[b.cfg.ClickHouse.EmbeddedBackupDisk] == "local" {
			sqlBytes, err := os.ReadFile(filePath)
			if err != nil {
				return err
			}
			sqlQuery, sqlMetadataChanged, fixSqlErr := b.fixEmbeddedMetadataSQLQuery(ctx, sqlBytes, filePath, chVersion)
			if fixSqlErr != nil {
				return fixSqlErr
			}
			if sqlMetadataChanged {
				if err = os.WriteFile(filePath, []byte(sqlQuery), 0644); err != nil {
					return err
				}
				if err = filesystemhelper.Chown(filePath, b.ch, disks, false); err != nil {
					return err
				}
			}
			return nil
		}
		sqlMetadata, err := object_disk.ReadMetadataFromFile(filePath)
		if err != nil {
			return err
		}
		sqlBytes, err := object_disk.ReadFileContent(ctx, b.ch, b.cfg, b.cfg.ClickHouse.EmbeddedBackupDisk, filePath)
		if err != nil {
			return err
		}
		sqlQuery, sqlMetadataChanged, fixSqlErr := b.fixEmbeddedMetadataSQLQuery(ctx, sqlBytes, filePath, chVersion)
		if fixSqlErr != nil {
			return fixSqlErr
		}
		if sqlMetadataChanged {
			if err = object_disk.WriteFileContent(ctx, b.ch, b.cfg, b.cfg.ClickHouse.EmbeddedBackupDisk, filePath, []byte(sqlQuery)); err != nil {
				return err
			}
			sqlMetadata.TotalSize = int64(len(sqlQuery))
			sqlMetadata.StorageObjects[0].ObjectSize = sqlMetadata.TotalSize
			if err = object_disk.WriteMetadataToFile(sqlMetadata, filePath); err != nil {
				return err
			}
		}
		return nil
	}); walkErr != nil {
		return walkErr
	}
	return nil
}

func (b *Backuper) fixEmbeddedMetadataSQLQuery(ctx context.Context, sqlBytes []byte, filePath string, version int) (string, bool, error) {
	sqlQuery := string(sqlBytes)
	sqlMetadataChanged := false
	if strings.Contains(sqlQuery, "{uuid}") {
		if UUIDWithMergeTreeRE.Match(sqlBytes) && version < 23009000 {
			sqlQuery = UUIDWithMergeTreeRE.ReplaceAllString(sqlQuery, "$1$2$3'$4'$5$4$7")
		} else {
			log.Warn().Msgf("%s contains `{uuid}` macro, will replace to `{database}/{table}` see https://github.com/ClickHouse/ClickHouse/issues/42709 for details", filePath)
			filePathParts := strings.Split(filePath, "/")
			database, err := url.QueryUnescape(filePathParts[len(filePathParts)-3])
			if err != nil {
				return "", false, err
			}
			table, err := url.QueryUnescape(filePathParts[len(filePathParts)-2])
			if err != nil {
				return "", false, err
			}
			lastIndex := strings.LastIndex(sqlQuery, "{uuid}")
			sqlQuery = sqlQuery[:lastIndex] + strings.Replace(sqlQuery[lastIndex:], "{uuid}", database+"/"+table, 1)
			// create materialized view corner case
			if strings.Contains(sqlQuery, "{uuid}") {
				sqlQuery = UUIDWithMergeTreeRE.ReplaceAllString(sqlQuery, "$1$2$3'$4'$5$4$7")
			}
		}
		sqlMetadataChanged = true
	}
	if emptyReplicatedMergeTreeRE.MatchString(sqlQuery) {
		replicaXMLSettings := map[string]string{"default_replica_path": "//default_replica_path", "default_replica_name": "//default_replica_name"}
		settings, err := b.ch.GetPreprocessedXMLSettings(ctx, replicaXMLSettings, "config.xml")
		if err != nil {
			return "", false, err
		}
		if len(settings) != 2 {
			log.Fatal().Msgf("can't get %#v from preprocessed_configs/config.xml", replicaXMLSettings)
		}
		log.Warn().Msgf("%s contains `ReplicatedMergeTree()` without parameters, will replace to '%s` and `%s` see https://github.com/ClickHouse/ClickHouse/issues/42709 for details", filePath, settings["default_replica_path"], settings["default_replica_name"])
		matches := emptyReplicatedMergeTreeRE.FindStringSubmatch(sqlQuery)
		substitution := fmt.Sprintf("$1$2('%s','%s')$4", settings["default_replica_path"], settings["default_replica_name"])
		if matches[2] != "" {
			substitution = fmt.Sprintf("$1$2('%s','%s',$3)$4", settings["default_replica_path"], settings["default_replica_name"])
		}
		sqlQuery = emptyReplicatedMergeTreeRE.ReplaceAllString(sqlQuery, substitution)
		sqlMetadataChanged = true
	}
	return sqlQuery, sqlMetadataChanged, nil
}

func (b *Backuper) restoreSchemaRegular(ctx context.Context, tablesForRestore ListOfTables, version int) error {
	totalRetries := len(tablesForRestore)
	restoreRetries := 0
	isDatabaseCreated := common.EmptyMap{}
	var restoreErr error
	for restoreRetries < totalRetries {
		var notRestoredTables ListOfTables
		for _, schema := range tablesForRestore {
			// if metadata.json doesn't contain "databases", we will re-create tables with default engine
			if _, isCreated := isDatabaseCreated[schema.Database]; !isCreated {
				if err := b.ch.CreateDatabase(schema.Database, b.cfg.General.RestoreSchemaOnCluster); err != nil {
					return fmt.Errorf("can't create database '%s': %v", schema.Database, err)
				} else {
					isDatabaseCreated[schema.Database] = struct{}{}
				}
			}
			//materialized and window views should restore via ATTACH
			b.replaceCreateToAttachForView(&schema)
			// https://github.com/Altinity/clickhouse-backup/issues/849
			log.Info().Msgf("SUKA BEFORE!!! schema.Query=%s", schema.Query)
			b.checkReplicaAlreadyExistsAndChangeReplicationPath(ctx, &schema)
			log.Info().Msgf("SUKA AFTER!!! schema.Query=%s", schema.Query)

			// https://github.com/Altinity/clickhouse-backup/issues/466
			b.replaceUUIDMacroValue(&schema)
			restoreErr = b.ch.CreateTable(clickhouse.Table{
				Database: schema.Database,
				Name:     schema.Table,
			}, schema.Query, false, false, b.cfg.General.RestoreSchemaOnCluster, version, b.DefaultDataPath)

			if restoreErr != nil {
				restoreRetries++
				if restoreRetries >= totalRetries {
					return fmt.Errorf(
						"can't create table `%s`.`%s`: %v after %d times, please check your schema dependencies",
						schema.Database, schema.Table, restoreErr, restoreRetries,
					)
				} else {
					log.Warn().Msgf(
						"can't create table '%s.%s': %v, will try again", schema.Database, schema.Table, restoreErr,
					)
				}
				notRestoredTables = append(notRestoredTables, schema)
			}
		}
		tablesForRestore = notRestoredTables
		if len(tablesForRestore) == 0 {
			break
		}
	}
	return nil
}

var replicatedParamsRE = regexp.MustCompile(`(Replicated[a-zA-Z]*MergeTree)\('([^']+)'(\s*,\s*)'([^']+)'\)|(Replicated[a-zA-Z]*MergeTree)\(\)`)
var replicatedUuidRE = regexp.MustCompile(` UUID '([^']+)'`)

func (b *Backuper) checkReplicaAlreadyExistsAndChangeReplicationPath(ctx context.Context, schema *metadata.TableMetadata) {
	if matches := replicatedParamsRE.FindAllStringSubmatch(schema.Query, -1); len(matches) > 0 {
		var err error
		if len(matches[0]) < 1 {
			log.Warn().Msgf("can't find Replicated paramaters in %s", schema.Query)
			return
		}
		shortSyntax := true
		var engine, replicaPath, replicaName, delimiter string
		if len(matches[0]) == 6 && matches[0][5] == "" {
			shortSyntax = false
			engine = matches[0][1]
			replicaPath = matches[0][2]
			delimiter = matches[0][3]
			replicaName = matches[0][4]
		} else {
			engine = matches[0][4]
			var settingsValues map[string]string
			settingsValues, err = b.ch.GetSettingsValues(ctx, []interface{}{"default_replica_path", "default_replica_name"})
			if err != nil {
				log.Fatal().Msgf("can't get from `system.settings` -> `default_replica_path`, `default_replica_name` error: %v", err)
			}
			replicaPath = settingsValues["default_replica_path"]
			replicaName = settingsValues["default_replica_name"]
		}
		var resolvedReplicaPath, resolvedReplicaName string
		if resolvedReplicaPath, err = b.ch.ApplyMacros(ctx, replicaPath); err != nil {
			log.Fatal().Msgf("can't ApplyMacros to %s error: %v", replicaPath, err)
		}
		if resolvedReplicaName, err = b.ch.ApplyMacros(ctx, replicaName); err != nil {
			log.Fatal().Msgf("can't ApplyMacros to %s error: %v", replicaPath, err)
		}
		if matches = replicatedUuidRE.FindAllStringSubmatch(schema.Query, 1); len(matches) > 0 {
			resolvedReplicaPath = strings.Replace(resolvedReplicaPath, "{uuid}", matches[0][1], -1)
		}

		isReplicaPresent := uint64(0)
		fullReplicaPath := path.Join(resolvedReplicaPath, "replicas", resolvedReplicaName)
		if err = b.ch.SelectSingleRow(ctx, &isReplicaPresent, "SELECT count() FROM system.zookeeper WHERE path=?", fullReplicaPath); err != nil {
			log.Warn().Msgf("can't check replica %s in system.zookeeper error: %v", fullReplicaPath, err)
		}
		if isReplicaPresent == 0 {
			return
		}
		newReplicaPath := b.cfg.ClickHouse.DefaultReplicaPath
		newReplicaName := b.cfg.ClickHouse.DefaultReplicaName
		log.Warn().Msgf("replica %s already exists in system.zookeeper will replace to %s", fullReplicaPath, path.Join(newReplicaPath, "replicas", newReplicaName))
		if shortSyntax {
			schema.Query = strings.Replace(schema.Query, engine+"()", engine+"('"+newReplicaPath+"','"+newReplicaName+"')", 1)
		} else {
			schema.Query = strings.Replace(schema.Query, engine+"('"+replicaPath+"'"+delimiter+"'"+replicaName+"')", engine+"('"+newReplicaPath+"', '"+newReplicaName+"')", 1)
		}
	}
}

func (b *Backuper) replaceUUIDMacroValue(schema *metadata.TableMetadata) {
	if b.cfg.General.RestoreSchemaOnCluster == "" && strings.Contains(schema.Query, "{uuid}") && strings.Contains(schema.Query, "Replicated") {
		if !strings.Contains(schema.Query, "UUID") {
			log.Warn().Msgf("table query doesn't contains UUID, can't guarantee properly restore for ReplicatedMergeTree")
		} else {
			schema.Query = UUIDWithMergeTreeRE.ReplaceAllString(schema.Query, "$1$2$3'$4'$5$4$7")
		}
	}
}

func (b *Backuper) replaceCreateToAttachForView(schema *metadata.TableMetadata) {
	schema.Query = strings.Replace(
		schema.Query, "CREATE MATERIALIZED VIEW", "ATTACH MATERIALIZED VIEW", 1,
	)
	schema.Query = strings.Replace(
		schema.Query, "CREATE WINDOW VIEW", "ATTACH WINDOW VIEW", 1,
	)
	schema.Query = strings.Replace(
		schema.Query, "CREATE LIVE VIEW", "ATTACH LIVE VIEW", 1,
	)
}

func (b *Backuper) dropExistsTables(tablesForDrop ListOfTables, ignoreDependencies bool, version int) error {
	var dropErr error
	dropRetries := 0
	totalRetries := len(tablesForDrop)
	for dropRetries < totalRetries {
		var notDroppedTables ListOfTables
		for i, schema := range tablesForDrop {
			if schema.Query == "" {
				possibleQueries := []string{
					fmt.Sprintf("CREATE DICTIONARY `%s`.`%s`", schema.Database, schema.Table),
					fmt.Sprintf("CREATE MATERIALIZED VIEW `%s`.`%s`", schema.Database, schema.Table),
				}
				if len(schema.Parts) > 0 {
					possibleQueries = append([]string{
						fmt.Sprintf("CREATE TABLE `%s`.`%s`", schema.Database, schema.Table),
					}, possibleQueries...)
				}
				for _, query := range possibleQueries {
					dropErr = b.ch.DropTable(clickhouse.Table{
						Database: schema.Database,
						Name:     schema.Table,
					}, query, b.cfg.General.RestoreSchemaOnCluster, ignoreDependencies, version, b.DefaultDataPath)
					if dropErr == nil {
						tablesForDrop[i].Query = query
						break
					}
				}
			} else {
				dropErr = b.ch.DropTable(clickhouse.Table{
					Database: schema.Database,
					Name:     schema.Table,
				}, schema.Query, b.cfg.General.RestoreSchemaOnCluster, ignoreDependencies, version, b.DefaultDataPath)
			}

			if dropErr != nil {
				dropRetries++
				if dropRetries >= totalRetries {
					return fmt.Errorf(
						"can't drop table `%s`.`%s`: %v after %d times, please check your schema dependencies",
						schema.Database, schema.Table, dropErr, dropRetries,
					)
				} else {
					log.Warn().Msgf(
						"can't drop table '%s.%s': %v, will try again", schema.Database, schema.Table, dropErr,
					)
				}
				notDroppedTables = append(notDroppedTables, schema)
			}
		}
		tablesForDrop = notDroppedTables
		if len(tablesForDrop) == 0 {
			break
		}
	}
	return nil
}

// RestoreData - restore data for tables matched by tablePattern from backupName
func (b *Backuper) RestoreData(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, dataOnly bool, metadataPath, tablePattern string, partitions []string, disks []clickhouse.Disk, version int) error {
	var err error
	startRestoreData := time.Now()
	diskMap := make(map[string]string, len(disks))
	diskTypes := make(map[string]string, len(disks))
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
		diskTypes[disk.Name] = disk.Type
	}
	for diskName := range backupMetadata.DiskTypes {
		if _, exists := diskTypes[diskName]; !exists {
			diskTypes[diskName] = backupMetadata.DiskTypes[diskName]
		}
	}
	var tablesForRestore ListOfTables
	var partitionsNameList map[metadata.TableTitle][]string
	tablesForRestore, partitionsNameList, err = b.getTableListByPatternLocal(ctx, metadataPath, tablePattern, false, partitions)
	if err != nil {
		// fix https://github.com/Altinity/clickhouse-backup/issues/832
		if b.cfg.General.AllowEmptyBackups && os.IsNotExist(err) {
			log.Warn().Msgf("b.getTableListByPatternLocal return error: %v", err)
			return nil
		}
		return err
	}
	if len(tablesForRestore) == 0 {
		if b.cfg.General.AllowEmptyBackups {
			log.Warn().Msgf("not found schemas by %s in %s", tablePattern, backupName)
			return nil
		}
		return fmt.Errorf("not found schemas schemas by %s in %s", tablePattern, backupName)
	}
	log.Debug().Msgf("found %d tables with data in backup", len(tablesForRestore))
	if b.isEmbedded {
		err = b.restoreDataEmbedded(ctx, backupName, dataOnly, version, tablesForRestore, partitionsNameList)
	} else {
		err = b.restoreDataRegular(ctx, backupName, backupMetadata, tablePattern, tablesForRestore, diskMap, diskTypes, disks)
	}
	if err != nil {
		return err
	}
	log.Info().Fields(map[string]interface{}{
		"backup":    backupName,
		"operation": "restore_data",
	}).Str("duration", utils.HumanizeDuration(time.Since(startRestoreData))).Msg("done")
	return nil
}

func (b *Backuper) restoreDataEmbedded(ctx context.Context, backupName string, dataOnly bool, version int, tablesForRestore ListOfTables, partitionsNameList map[metadata.TableTitle][]string) error {
	return b.restoreEmbedded(ctx, backupName, false, dataOnly, version, tablesForRestore, partitionsNameList)
}

func (b *Backuper) restoreDataRegular(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, tablePattern string, tablesForRestore ListOfTables, diskMap, diskTypes map[string]string, disks []clickhouse.Disk) error {
	if len(b.cfg.General.RestoreDatabaseMapping) > 0 {
		tablePattern = b.changeTablePatternFromRestoreMapping(tablePattern, "database")
	}
	// https://github.com/Altinity/clickhouse-backup/issues/937
	if len(b.cfg.General.RestoreTableMapping) > 0 {
		tablePattern = b.changeTablePatternFromRestoreMapping(tablePattern, "table")
	}

	if err := b.applyMacrosToObjectDiskPath(ctx); err != nil {
		return err
	}

	chTables, err := b.ch.GetTables(ctx, tablePattern)
	if err != nil {
		return err
	}
	dstTablesMap := b.prepareDstTablesMap(chTables)

	missingTables := b.checkMissingTables(tablesForRestore, chTables)
	if len(missingTables) > 0 {
		return fmt.Errorf("%s is not created. Restore schema first or create missing tables manually", strings.Join(missingTables, ", "))
	}
	restoreBackupWorkingGroup, restoreCtx := errgroup.WithContext(ctx)
	restoreBackupWorkingGroup.SetLimit(max(b.cfg.ClickHouse.MaxConnections, 1))

	for i := range tablesForRestore {
		tableRestoreStartTime := time.Now()
		table := tablesForRestore[i]
		// need mapped database path and original table.Database for HardlinkBackupPartsToStorage.
		dstDatabase := table.Database
		// The same goes for the table
		dstTableName := table.Table
		if len(b.cfg.General.RestoreDatabaseMapping) > 0 {
			if targetDB, isMapped := b.cfg.General.RestoreDatabaseMapping[table.Database]; isMapped {
				dstDatabase = targetDB
				tablesForRestore[i].Database = targetDB
			}
		}
		// https://github.com/Altinity/clickhouse-backup/issues/937
		if len(b.cfg.General.RestoreTableMapping) > 0 {
			if targetTable, isMapped := b.cfg.General.RestoreTableMapping[table.Table]; isMapped {
				dstTableName = targetTable
				tablesForRestore[i].Table = targetTable
			}
		}
		logger := log.With().Str("table", fmt.Sprintf("%s.%s", dstDatabase, dstTableName)).Logger()
		dstTable, ok := dstTablesMap[metadata.TableTitle{
			Database: dstDatabase,
			Table:    dstTableName}]
		if !ok {
			return fmt.Errorf("can't find '%s.%s' in current system.tables", dstDatabase, table.Table)
		}
		idx := i
		restoreBackupWorkingGroup.Go(func() error {
			// https://github.com/Altinity/clickhouse-backup/issues/529
			if b.cfg.ClickHouse.RestoreAsAttach {
				if restoreErr := b.restoreDataRegularByAttach(restoreCtx, backupName, backupMetadata, table, diskMap, diskTypes, disks, dstTable, logger); restoreErr != nil {
					return restoreErr
				}
			} else {
				if restoreErr := b.restoreDataRegularByParts(restoreCtx, backupName, backupMetadata, table, diskMap, diskTypes, disks, dstTable, logger); restoreErr != nil {
					return restoreErr
				}
			}
			// https://github.com/Altinity/clickhouse-backup/issues/529
			for _, mutation := range table.Mutations {
				if err := b.ch.ApplyMutation(restoreCtx, tablesForRestore[idx], mutation); err != nil {
					log.Warn().Msgf("can't apply mutation %s for table `%s`.`%s`	: %v", mutation.Command, tablesForRestore[idx].Database, tablesForRestore[idx].Table, err)
				}
			}
			log.Info().Fields(map[string]interface{}{
				"duration":  utils.HumanizeDuration(time.Since(tableRestoreStartTime)),
				"operation": "restoreDataRegular",
				"database":  dstTable.Database,
				"table":     dstTable.Name,
				"progress":  fmt.Sprintf("%d/%d", idx+1, len(tablesForRestore)),
			}).Msg("done")
			return nil
		})
	}
	if wgWaitErr := restoreBackupWorkingGroup.Wait(); wgWaitErr != nil {
		return fmt.Errorf("one of restoreDataRegular go-routine return error: %v", wgWaitErr)
	}
	return nil
}

func (b *Backuper) restoreDataRegularByAttach(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, table metadata.TableMetadata, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, dstTable clickhouse.Table, logger zerolog.Logger) error {
	if err := filesystemhelper.HardlinkBackupPartsToStorage(backupName, table, disks, diskMap, dstTable.DataPaths, b.ch, false); err != nil {
		return fmt.Errorf("can't copy data to storage '%s.%s': %v", table.Database, table.Table, err)
	}
	logger.Debug().Msg("data to 'storage' copied")
	var size int64
	var err error
	start := time.Now()
	if size, err = b.downloadObjectDiskParts(ctx, backupName, backupMetadata, table, diskMap, diskTypes, disks); err != nil {
		return fmt.Errorf("can't restore object_disk server-side copy data parts '%s.%s': %v", table.Database, table.Table, err)
	}
	if size > 0 {
		logger.Info().Str("duration", utils.HumanizeDuration(time.Since(start))).Str("size", utils.FormatBytes(uint64(size))).Msg("download object_disks finish")
	}
	if err := b.ch.AttachTable(ctx, table, dstTable); err != nil {
		return fmt.Errorf("can't attach table '%s.%s': %v", table.Database, table.Table, err)
	}
	return nil
}

func (b *Backuper) restoreDataRegularByParts(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, table metadata.TableMetadata, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, dstTable clickhouse.Table, logger zerolog.Logger) error {
	if err := filesystemhelper.HardlinkBackupPartsToStorage(backupName, table, disks, diskMap, dstTable.DataPaths, b.ch, true); err != nil {
		return fmt.Errorf("can't copy data to detached '%s.%s': %v", table.Database, table.Table, err)
	}
	logger.Debug().Msg("data to 'detached' copied")
	logger.Info().Msg("download object_disks start")
	var size int64
	var err error
	start := time.Now()
	if size, err = b.downloadObjectDiskParts(ctx, backupName, backupMetadata, table, diskMap, diskTypes, disks); err != nil {
		return fmt.Errorf("can't restore object_disk server-side copy data parts '%s.%s': %v", table.Database, table.Table, err)
	}
	log.Info().Str("duration", utils.HumanizeDuration(time.Since(start))).Str("size", utils.FormatBytes(uint64(size))).Msg("download object_disks finish")
	if err := b.ch.AttachDataParts(table, dstTable); err != nil {
		return fmt.Errorf("can't attach data parts for table '%s.%s': %v", table.Database, table.Table, err)
	}
	return nil
}

func (b *Backuper) downloadObjectDiskParts(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, backupTable metadata.TableMetadata, diskMap, diskTypes map[string]string, disks []clickhouse.Disk) (int64, error) {
	logger := log.With().Fields(map[string]interface{}{
		"operation": "downloadObjectDiskParts",
		"table":     fmt.Sprintf("%s.%s", backupTable.Database, backupTable.Table),
	}).Logger()
	size := int64(0)
	dbAndTableDir := path.Join(common.TablePathEncode(backupTable.Database), common.TablePathEncode(backupTable.Table))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	for diskName, parts := range backupTable.Parts {
		diskType, exists := diskTypes[diskName]
		if !exists {
			return 0, fmt.Errorf("%s disk doesn't present in diskTypes: %v", diskName, diskTypes)
		}
		isObjectDiskEncrypted := false
		if diskType == "encrypted" {
			if diskPath, exists := diskMap[diskName]; !exists {
				for _, part := range parts {
					if part.RebalancedDisk != "" {
						diskPath = diskMap[part.RebalancedDisk]
						if b.isDiskTypeEncryptedObject(clickhouse.Disk{Type: diskTypes[part.RebalancedDisk], Name: part.RebalancedDisk, Path: diskPath}, disks) {
							isObjectDiskEncrypted = true
							break
						}
					}
				}
			} else {
				isObjectDiskEncrypted = b.isDiskTypeEncryptedObject(clickhouse.Disk{Type: diskType, Name: diskName, Path: diskPath}, disks)
			}
		}
		isObjectDisk := b.isDiskTypeObject(diskType)
		if isObjectDisk || isObjectDiskEncrypted {
			if err = config.ValidateObjectDiskConfig(b.cfg); err != nil {
				return 0, err
			}
			if _, exists := diskMap[diskName]; !exists {
				for _, part := range parts {
					if part.RebalancedDisk != "" {
						if err = object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, part.RebalancedDisk); err != nil {
							return 0, err
						}
					}
				}
			} else if err = object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, diskName); err != nil {
				return 0, err
			}
			start := time.Now()
			downloadObjectDiskPartsWorkingGroup, downloadCtx := errgroup.WithContext(ctx)
			downloadObjectDiskPartsWorkingGroup.SetLimit(int(b.cfg.General.ObjectDiskServerSideCopyConcurrency))
			var isCopyFailed atomic.Bool
			isCopyFailed.Store(false)
			for _, part := range parts {
				dstDiskName := diskName
				if part.RebalancedDisk != "" {
					dstDiskName = part.RebalancedDisk
				}
				partPath := path.Join(diskMap[dstDiskName], "backup", backupName, "shadow", dbAndTableDir, dstDiskName, part.Name)
				srcBackupName := backupName
				srcDiskName := diskName
				// copy from required backup for required data parts, https://github.com/Altinity/clickhouse-backup/issues/865
				if part.Required && backupMetadata.RequiredBackup != "" {
					var findRecursiveErr error
					srcBackupName, srcDiskName, findRecursiveErr = b.findObjectDiskPartRecursive(ctx, backupMetadata, backupTable, part, diskName, logger)
					if findRecursiveErr != nil {
						return 0, findRecursiveErr
					}
				}
				walkErr := filepath.Walk(partPath, func(fPath string, fInfo fs.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if fInfo.IsDir() {
						return nil
					}
					// fix https://github.com/Altinity/clickhouse-backup/issues/826
					if strings.Contains(fInfo.Name(), "frozen_metadata") {
						return nil
					}
					if b.resume {
						if isAlreadyProcessed, copiedSize := b.resumableState.IsAlreadyProcessed(path.Join(fPath, fInfo.Name())); isAlreadyProcessed {
							atomic.AddInt64(&size, copiedSize)
							return nil
						}
					}
					objMeta, err := object_disk.ReadMetadataFromFile(fPath)
					if err != nil {
						return err
					}
					if objMeta.StorageObjectCount < 1 && objMeta.Version < object_disk.VersionRelativePath {
						return fmt.Errorf("%s: invalid object_disk.Metadata: %#v", fPath, objMeta)
					}
					//to allow deleting Object Disk Data during DROP TABLE/DATABASE ...SYNC
					if objMeta.RefCount > 0 || objMeta.ReadOnly {
						objMeta.RefCount = 0
						objMeta.ReadOnly = false
						logger.Debug().Msgf("%s %#v set RefCount=0 and ReadOnly=0", fPath, objMeta.StorageObjects)
						if writeMetaErr := object_disk.WriteMetadataToFile(objMeta, fPath); writeMetaErr != nil {
							return fmt.Errorf("%s: object_disk.WriteMetadataToFile return error: %v", fPath, writeMetaErr)
						}
					}
					downloadObjectDiskPartsWorkingGroup.Go(func() error {
						var srcBucket, srcKey string
						for _, storageObject := range objMeta.StorageObjects {
							if storageObject.ObjectSize == 0 {
								continue
							}
							objectDiskPath, objectDiskPathErr := b.getObjectDiskPath()
							if objectDiskPathErr != nil {
								return objectDiskPathErr
							}
							srcKey = path.Join(objectDiskPath, srcBackupName, srcDiskName, storageObject.ObjectRelativePath)
							srcBucket = ""
							if b.cfg.General.RemoteStorage == "s3" {
								srcBucket = b.cfg.S3.Bucket
							} else if b.cfg.General.RemoteStorage == "gcs" {
								srcBucket = b.cfg.GCS.Bucket
							} else if b.cfg.General.RemoteStorage == "azblob" {
								srcBucket = b.cfg.AzureBlob.Container
							}
							copiedSize := int64(0)
							var copyObjectErr error
							if !b.cfg.General.AllowObjectDiskStreaming {
								retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
								copyObjectErr = retry.RunCtx(downloadCtx, func(ctx context.Context) error {
									var retryErr error
									copiedSize, retryErr = object_disk.CopyObject(downloadCtx, dstDiskName, storageObject.ObjectSize, srcBucket, srcKey, storageObject.ObjectRelativePath)
									return retryErr
								})
								if copyObjectErr != nil {
									return fmt.Errorf("object_disk.CopyObject `%s`.`%s` error: %v", backupTable.Database, backupTable.Table, copyObjectErr)
								}
							} else {
								copyObjectErr = nil
								if srcBucket != "" && !isCopyFailed.Load() {
									copiedSize, copyObjectErr = object_disk.CopyObject(downloadCtx, dstDiskName, storageObject.ObjectSize, srcBucket, srcKey, storageObject.ObjectRelativePath)
									if copyObjectErr != nil {
										isCopyFailed.Store(true)
										log.Warn().Msgf("object_disk.CopyObject `%s`.`%s` error: %v, will try streaming via local memory (possible high network traffic)", backupTable.Database, backupTable.Table, copyObjectErr)
									}
								}
								//srcBucket empty when use non CopyObject compatible `remote_storage` type
								if srcBucket == "" || isCopyFailed.Load() {
									srcStorage := b.dst
									dstConnection, connectionExists := object_disk.DisksConnections.Load(dstDiskName)
									if !connectionExists {
										return fmt.Errorf("unknown object_disk.DisksConnections %s", dstDiskName)
									}
									dstStorage := dstConnection.GetRemoteStorage()
									dstKey := path.Join(dstConnection.GetRemoteObjectDiskPath(), storageObject.ObjectRelativePath)
									retry := retrier.New(retrier.ConstantBackoff(b.cfg.General.RetriesOnFailure, b.cfg.General.RetriesDuration), nil)
									copyObjectErr = retry.RunCtx(downloadCtx, func(ctx context.Context) error {
										return object_disk.CopyObjectStreaming(downloadCtx, srcStorage, dstStorage, srcKey, dstKey)
									})
									if copyObjectErr != nil {
										return fmt.Errorf("object_disk.CopyObjectStreaming error: %v", copyObjectErr)
									}
									copiedSize = storageObject.ObjectSize
								}
							}
							atomic.AddInt64(&size, copiedSize)
						}
						if b.resume {
							b.resumableState.AppendToState(path.Join(fPath, fInfo.Name()), objMeta.TotalSize)
						}
						return nil
					})
					return nil
				})
				if walkErr != nil {
					return 0, walkErr
				}
			}
			if wgWaitErr := downloadObjectDiskPartsWorkingGroup.Wait(); wgWaitErr != nil {
				return 0, fmt.Errorf("one of downloadObjectDiskParts go-routine return error: %v", wgWaitErr)
			}
			logger.Info().Str("disk", diskName).Str("duration", utils.HumanizeDuration(time.Since(start))).Str("size", utils.FormatBytes(uint64(size))).Msg("object_disk data downloaded")
		}
	}

	return size, nil
}

func (b *Backuper) findObjectDiskPartRecursive(ctx context.Context, backup metadata.BackupMetadata, table metadata.TableMetadata, part metadata.Part, diskName string, logger zerolog.Logger) (string, string, error) {
	if !part.Required {
		return backup.BackupName, diskName, nil
	}
	if part.Required && backup.RequiredBackup == "" {
		return "", "", fmt.Errorf("part %s have required flag, in %s but backup.RequiredBackup is empty", part.Name, backup.BackupName)
	}
	requiredBackup, err := b.ReadBackupMetadataRemote(ctx, backup.RequiredBackup)
	if err != nil {
		return "", "", err
	}
	var requiredTable *metadata.TableMetadata
	requiredTable, err = b.downloadTableMetadataIfNotExists(ctx, requiredBackup.BackupName, metadata.TableTitle{Database: table.Database, Table: table.Table})
	// @todo think about add check what if disk type could changed (should already restricted, cause upload seek part in the same disk name)
	for requiredDiskName, parts := range requiredTable.Parts {
		for _, requiredPart := range parts {
			if requiredPart.Name == part.Name {
				if requiredPart.Required {
					return b.findObjectDiskPartRecursive(ctx, *requiredBackup, *requiredTable, requiredPart, requiredDiskName, logger)
				}
				return requiredBackup.BackupName, requiredDiskName, nil
			}
		}

	}
	return "", "", fmt.Errorf("part %s have required flag in %s, but not found in %s", part.Name, backup.BackupName, backup.RequiredBackup)
}

func (b *Backuper) checkMissingTables(tablesForRestore ListOfTables, chTables []clickhouse.Table) []string {
	var missingTables []string
	for _, table := range tablesForRestore {
		dstDatabase := table.Database
		dstTable := table.Table
		if len(b.cfg.General.RestoreDatabaseMapping) > 0 {
			if targetDB, isMapped := b.cfg.General.RestoreDatabaseMapping[table.Database]; isMapped {
				dstDatabase = targetDB
			}
		}
		if len(b.cfg.General.RestoreTableMapping) > 0 {
			if targetTable, isMapped := b.cfg.General.RestoreTableMapping[table.Table]; isMapped {
				dstTable = targetTable
			}
		}
		found := false
		for _, chTable := range chTables {
			if (dstDatabase == chTable.Database) && (dstTable == chTable.Name) {
				found = true
				break
			}
		}
		if !found {
			missingTables = append(missingTables, fmt.Sprintf("'%s.%s'", dstDatabase, table.Table))
		}
	}
	return missingTables
}

func (b *Backuper) prepareDstTablesMap(chTables []clickhouse.Table) map[metadata.TableTitle]clickhouse.Table {
	dstTablesMap := map[metadata.TableTitle]clickhouse.Table{}
	for i, chTable := range chTables {
		dstTablesMap[metadata.TableTitle{
			Database: chTables[i].Database,
			Table:    chTables[i].Name,
		}] = chTable
	}
	return dstTablesMap
}

func (b *Backuper) changeTablePatternFromRestoreMapping(tablePattern, objType string) string {
	var mapping map[string]string
	switch objType {
	case "database":
		mapping = b.cfg.General.RestoreDatabaseMapping
	case "table":
		mapping = b.cfg.General.RestoreDatabaseMapping
	default:
		return ""
	}
	for sourceObj, targetObj := range mapping {
		if tablePattern != "" {
			sourceObjRE := regexp.MustCompile(fmt.Sprintf("(^%s.*)|(,%s.*)", sourceObj, sourceObj))
			if sourceObjRE.MatchString(tablePattern) {
				matches := sourceObjRE.FindAllStringSubmatch(tablePattern, -1)
				substitution := targetObj + ".*"
				if strings.HasPrefix(matches[0][1], ",") {
					substitution = "," + substitution
				}
				tablePattern = sourceObjRE.ReplaceAllString(tablePattern, substitution)
			} else {
				tablePattern += "," + targetObj + ".*"
			}
		} else {
			tablePattern += targetObj + ".*"
		}
	}
	return tablePattern
}

func (b *Backuper) restoreEmbedded(ctx context.Context, backupName string, schemaOnly, dataOnly bool, version int, tablesForRestore ListOfTables, partitionsNameList map[metadata.TableTitle][]string) error {
	tablesSQL := ""
	l := len(tablesForRestore)
	for i, t := range tablesForRestore {
		if t.Query != "" {
			kind := "TABLE"
			if strings.Contains(t.Query, " DICTIONARY ") {
				kind = "DICTIONARY"
			}
			if newDb, isMapped := b.cfg.General.RestoreDatabaseMapping[t.Database]; isMapped {
				tablesSQL += fmt.Sprintf("%s `%s`.`%s` AS `%s`.`%s`", kind, t.Database, t.Table, newDb, t.Table)
			} else {
				tablesSQL += fmt.Sprintf("%s `%s`.`%s`", kind, t.Database, t.Table)
			}

			if strings.Contains(t.Query, " VIEW ") {
				kind = "VIEW"
			}

			if kind == "TABLE" && len(partitionsNameList) > 0 {
				if tablePartitions, exists := partitionsNameList[metadata.TableTitle{Table: t.Table, Database: t.Database}]; exists && len(tablePartitions) > 0 {
					if tablePartitions[0] != "*" {
						partitionsSQL := fmt.Sprintf("'%s'", strings.Join(tablePartitions, "','"))
						if strings.HasPrefix(partitionsSQL, "'(") {
							partitionsSQL = strings.Join(tablePartitions, ",")
						}
						tablesSQL += fmt.Sprintf(" PARTITIONS %s", partitionsSQL)
					}
				}
			}
			if i < l-1 {
				tablesSQL += ", "
			}
		}
	}
	settings := b.getEmbeddedRestoreSettings(version)
	if schemaOnly {
		settings = append(settings, "structure_only=1")
	}
	if dataOnly {
		settings = append(settings, "allow_non_empty_tables=1")
	}
	embeddedBackupLocation, err := b.getEmbeddedBackupLocation(ctx, backupName)
	if err != nil {
		return err
	}
	settingsStr := ""
	if len(settings) > 0 {
		settingsStr = "SETTINGS " + strings.Join(settings, ", ")
	}
	restoreSQL := fmt.Sprintf("RESTORE %s FROM %s %s", tablesSQL, embeddedBackupLocation, settingsStr)
	restoreResults := make([]clickhouse.SystemBackups, 0)
	if err := b.ch.SelectContext(ctx, &restoreResults, restoreSQL); err != nil {
		return fmt.Errorf("restore error: %v", err)
	}
	if len(restoreResults) == 0 || restoreResults[0].Status != "RESTORED" {
		return fmt.Errorf("restore wrong result: %v", restoreResults)
	}
	return nil
}
