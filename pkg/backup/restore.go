package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/keeper"
	"github.com/Altinity/clickhouse-backup/pkg/status"
	"github.com/Altinity/clickhouse-backup/pkg/storage"
	"github.com/Altinity/clickhouse-backup/pkg/storage/object_disk"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/pkg/common"

	"github.com/mattn/go-shellwords"

	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/pkg/utils"
	apexLog "github.com/apex/log"
	recursiveCopy "github.com/otiai10/copy"
	"github.com/yargevad/filepathx"
)

var CreateDatabaseRE = regexp.MustCompile(`(?m)^CREATE DATABASE (\s*)(\S+)(\s*)`)

// Restore - restore tables matched by tablePattern from backupName
func (b *Backuper) Restore(backupName, tablePattern string, databaseMapping, partitions []string, schemaOnly, dataOnly, dropTable, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly bool, commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	if err := b.prepareRestoreDatabaseMapping(databaseMapping); err != nil {
		return err
	}

	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "restore",
	})
	doRestoreData := !schemaOnly || dataOnly

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
	defaultDataPath, err := b.ch.GetDefaultPath(disks)
	if err != nil {
		log.Warnf("%v", err)
		return ErrUnknownClickhouseDataPath
	}
	backupMetafileLocalPaths := []string{path.Join(defaultDataPath, "backup", backupName, "metadata.json")}
	var backupMetadataBody []byte
	embeddedBackupPath, err := b.ch.GetEmbeddedBackupPath(disks)
	if err == nil && embeddedBackupPath != "" {
		backupMetafileLocalPaths = append(backupMetafileLocalPaths, path.Join(embeddedBackupPath, backupName, "metadata.json"))
	} else if b.cfg.ClickHouse.UseEmbeddedBackupRestore && b.cfg.ClickHouse.EmbeddedBackupDisk == "" {
		log.Warnf("%v", err)
	} else if err != nil {
		return err
	}
	for _, metadataPath := range backupMetafileLocalPaths {
		backupMetadataBody, err = os.ReadFile(metadataPath)
		if err == nil && embeddedBackupPath != "" {
			b.isEmbedded = strings.HasPrefix(metadataPath, embeddedBackupPath)
			break
		}
	}
	if b.cfg.General.RestoreSchemaOnCluster != "" {
		b.cfg.General.RestoreSchemaOnCluster, err = b.ch.ApplyMacros(ctx, b.cfg.General.RestoreSchemaOnCluster)
	}
	if err == nil {
		backupMetadata := metadata.BackupMetadata{}
		if err := json.Unmarshal(backupMetadataBody, &backupMetadata); err != nil {
			return err
		}

		if schemaOnly || doRestoreData {
			for _, database := range backupMetadata.Databases {
				targetDB := database.Name
				if !IsInformationSchema(targetDB) {
					if err = b.restoreEmptyDatabase(ctx, targetDB, tablePattern, database, dropTable, schemaOnly, ignoreDependencies); err != nil {
						return err
					}
				}
			}
		}
		// do not create UDF when use --data flag, https://github.com/Altinity/clickhouse-backup/issues/697
		if schemaOnly || (schemaOnly == dataOnly) {
			for _, function := range backupMetadata.Functions {
				if err = b.ch.CreateUserDefinedFunction(function.Name, function.CreateQuery, b.cfg.General.RestoreSchemaOnCluster); err != nil {
					return err
				}
			}
		}
		if len(backupMetadata.Tables) == 0 {
			log.Warnf("'%s' doesn't contains tables for restore", backupName)
			if (!restoreRBAC) && (!restoreConfigs) {
				return nil
			}
		}
	} else if !os.IsNotExist(err) { // Legacy backups don't contain metadata.json
		return err
	}
	needRestart := false
	if (rbacOnly || restoreRBAC) && !b.isEmbedded {
		if err := b.restoreRBAC(ctx, backupName, disks); err != nil {
			return err
		}
		needRestart = true
	}
	if (configsOnly || restoreConfigs) && !b.isEmbedded {
		if err := b.restoreConfigs(backupName, disks); err != nil {
			return err
		}
		needRestart = true
	}

	if needRestart {
		if err := b.restartClickHouse(ctx, backupName, log); err != nil {
			return err
		}
		if rbacOnly || configsOnly {
			return nil
		}
	}

	if schemaOnly || (schemaOnly == dataOnly) {
		if err := b.RestoreSchema(ctx, backupName, tablePattern, dropTable, ignoreDependencies, disks); err != nil {
			return err
		}
	}
	if dataOnly || (schemaOnly == dataOnly) {
		if err := b.RestoreData(ctx, backupName, tablePattern, partitions, disks); err != nil {
			return err
		}
	}
	log.Info("done")
	return nil
}

func (b *Backuper) restartClickHouse(ctx context.Context, backupName string, log *apexLog.Entry) error {
	log.Warnf("%s contains `access` or `configs` directory, so we need exec %s", backupName, b.ch.Config.RestartCommand)
	for _, cmd := range strings.Split(b.ch.Config.RestartCommand, ";") {
		cmd = strings.Trim(cmd, " \t\r\n")
		if strings.HasPrefix(cmd, "sql:") {
			cmd = strings.TrimPrefix(cmd, "sql:")
			if err := b.ch.QueryContext(ctx, cmd); err != nil {
				log.Warnf("restart sql: %s, error: %v", cmd, err)
			}
		}
		if strings.HasPrefix(cmd, "exec:") {
			cmd = strings.TrimPrefix(cmd, "exec:")
			if err := b.executeShellCommandWithTimeout(ctx, cmd, log); err != nil {
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
			log.Infof("wait 3 seconds")
			time.Sleep(3 * time.Second)
		}
	}
	return nil
}

func (b *Backuper) executeShellCommandWithTimeout(ctx context.Context, cmd string, log *apexLog.Entry) error {
	shellCmd, err := shellwords.Parse(cmd)
	if err != nil {
		return err
	}
	shellCtx, shellCancel := context.WithTimeout(ctx, 180*time.Second)
	defer shellCancel()
	log.Infof("run %s", cmd)
	var out []byte
	if len(shellCmd) > 1 {
		out, err = exec.CommandContext(shellCtx, shellCmd[0], shellCmd[1:]...).CombinedOutput()
	} else {
		out, err = exec.CommandContext(shellCtx, shellCmd[0]).CombinedOutput()
	}
	if err != nil {
		log.Debug(string(out))
		log.Warnf("restart exec: %s, error: %v", cmd, err)
	}
	return nil
}

func (b *Backuper) restoreEmptyDatabase(ctx context.Context, targetDB, tablePattern string, database metadata.DatabasesMeta, dropTable, schemaOnly, ignoreDependencies bool) error {
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
			version, err := b.ch.GetVersion(ctx)
			if err != nil {
				return err
			}
			if version >= 21012000 {
				settings = "SETTINGS check_table_dependencies=0"
			}
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

func (b *Backuper) prepareRestoreDatabaseMapping(databaseMapping []string) error {
	for i := 0; i < len(databaseMapping); i++ {
		splitByCommas := strings.Split(databaseMapping[i], ",")
		for _, m := range splitByCommas {
			splitByColon := strings.Split(m, ":")
			if len(splitByColon) != 2 {
				return fmt.Errorf("restore-database-mapping %s should only have srcDatabase:destinationDatabase format for each map rule", m)
			}
			b.cfg.General.RestoreDatabaseMapping[splitByColon[0]] = splitByColon[1]
		}
	}
	return nil
}

// restoreRBAC - copy backup_name>/rbac folder to access_data_path
func (b *Backuper) restoreRBAC(ctx context.Context, backupName string, disks []clickhouse.Disk) error {
	log := b.log.WithField("logger", "restoreRBAC")
	accessPath, err := b.ch.GetAccessManagementPath(ctx, nil)
	if err != nil {
		return err
	}
	if err = b.restoreBackupRelatedDir(backupName, "access", accessPath, disks, []string{"*.jsonl"}); err == nil {
		markFile := path.Join(accessPath, "need_rebuild_lists.mark")
		log.Infof("create %s for properly rebuild RBAC after restart clickhouse-server", markFile)
		file, err := os.Create(markFile)
		if err != nil {
			return err
		}
		_ = file.Close()
		_ = filesystemhelper.Chown(markFile, b.ch, disks, false)
		listFilesPattern := path.Join(accessPath, "*.list")
		log.Infof("remove %s for properly rebuild RBAC after restart clickhouse-server", listFilesPattern)
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
	if err = b.restoreRBACReplicated(ctx, backupName, "access", disks); err != nil {
		return err
	}
	return nil
}

func (b *Backuper) restoreRBACReplicated(ctx context.Context, backupName string, backupPrefixDir string, disks []clickhouse.Disk) error {
	log := b.log.WithField("logger", "restoreRBACReplicated")
	defaultDataPath, err := b.ch.GetDefaultPath(disks)
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	srcBackupDir := path.Join(defaultDataPath, "backup", backupName, backupPrefixDir)
	info, err := os.Stat(srcBackupDir)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", srcBackupDir)
	}
	replicatedRBAC := make([]struct {
		Name string `ch:"name"`
	}, 0)
	if err = b.ch.SelectContext(ctx, &replicatedRBAC, "SELECT name FROM system.user_directories WHERE type='replicated'"); err == nil && len(replicatedRBAC) > 0 {
		jsonLFiles, err := filepathx.Glob(path.Join(srcBackupDir, "*.jsonl"))
		if err != nil {
			return err
		}
		if len(jsonLFiles) == 0 {
			return nil
		}
		k := keeper.Keeper{Log: b.log.WithField("logger", "keeper")}
		if err = k.Connect(ctx, b.ch, b.cfg); err != nil {
			return err
		}
		defer k.Close()
		restoreReplicatedRBACMap := make(map[string]string, len(jsonLFiles))
		for _, jsonLFile := range jsonLFiles {
			for _, userDirectory := range replicatedRBAC {
				if strings.HasSuffix(jsonLFile, userDirectory.Name+".jsonl") {
					restoreReplicatedRBACMap[jsonLFile] = userDirectory.Name
				}
			}
			if _, exists := restoreReplicatedRBACMap[jsonLFile]; !exists {
				restoreReplicatedRBACMap[jsonLFile] = replicatedRBAC[0].Name
			}
		}
		for jsonLFile, userDirectoryName := range restoreReplicatedRBACMap {
			replicatedAccessPath, err := k.GetReplicatedAccessPath(userDirectoryName)
			if err != nil {
				return err
			}
			log.Infof("keeper.Restore(%s) -> %s", jsonLFile, replicatedAccessPath)
			if err := k.Restore(jsonLFile, replicatedAccessPath); err != nil {
				return err
			}
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
	log := b.log.WithField("logger", "restoreBackupRelatedDir")
	defaultDataPath, err := b.ch.GetDefaultPath(disks)
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	srcBackupDir := path.Join(defaultDataPath, "backup", backupName, backupPrefixDir)
	info, err := os.Stat(srcBackupDir)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", srcBackupDir)
	}
	log.Debugf("copy %s -> %s", srcBackupDir, destinationDir)
	copyOptions := recursiveCopy.Options{
		OnDirExists: func(src, dest string) recursiveCopy.DirExistsAction {
			return recursiveCopy.Merge
		},
		Skip: func(srcinfo os.FileInfo, src, dest string) (bool, error) {
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

// RestoreSchema - restore schemas matched by tablePattern from backupName
func (b *Backuper) RestoreSchema(ctx context.Context, backupName, tablePattern string, dropTable, ignoreDependencies bool, disks []clickhouse.Disk) error {
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "restore",
	})

	defaultDataPath, err := b.ch.GetDefaultPath(disks)
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	version, err := b.ch.GetVersion(ctx)
	if err != nil {
		return err
	}
	metadataPath := path.Join(defaultDataPath, "backup", backupName, "metadata")
	if b.isEmbedded {
		defaultDataPath, err = b.ch.GetEmbeddedBackupPath(disks)
		if err != nil {
			return err
		}
		metadataPath = path.Join(defaultDataPath, backupName, "metadata")
	}
	info, err := os.Stat(metadataPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", metadataPath)
	}
	if tablePattern == "" {
		tablePattern = "*"
	}
	tablesForRestore, _, err := b.getTableListByPatternLocal(ctx, metadataPath, tablePattern, dropTable, nil)
	if err != nil {
		return err
	}
	// if restore-database-mapping specified, create database in mapping rules instead of in backup files.
	if len(b.cfg.General.RestoreDatabaseMapping) > 0 {
		err = changeTableQueryToAdjustDatabaseMapping(&tablesForRestore, b.cfg.General.RestoreDatabaseMapping)
		if err != nil {
			return err
		}
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("no have found schemas by %s in %s", tablePattern, backupName)
	}
	if dropErr := b.dropExistsTables(tablesForRestore, ignoreDependencies, version, log); dropErr != nil {
		return dropErr
	}
	var restoreErr error
	if b.isEmbedded {
		restoreErr = b.restoreSchemaEmbedded(ctx, backupName, tablesForRestore, defaultDataPath)
	} else {
		restoreErr = b.restoreSchemaRegular(tablesForRestore, version, log)
	}
	if restoreErr != nil {
		return restoreErr
	}
	return nil
}

var UUIDWithMergeTreeRE = regexp.MustCompile(`^(.+)(UUID)(\s+)'([^']+)'(.+)({uuid})(.*)`)

var emptyReplicatedMergeTreeRE = regexp.MustCompile(`(?m)Replicated(MergeTree|ReplacingMergeTree|SummingMergeTree|AggregatingMergeTree|CollapsingMergeTree|VersionedCollapsingMergeTree|GraphiteMergeTree)\s*\(([^']*)\)(.*)`)

func (b *Backuper) restoreSchemaEmbedded(ctx context.Context, backupName string, tablesForRestore ListOfTables, defaultDataPath string) error {
	metadataPath := path.Join(defaultDataPath, backupName, "metadata")
	if err := filepath.Walk(metadataPath, func(filePath string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(filePath, ".sql") {
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
		sqlQuery := string(sqlBytes)
		if strings.Contains(sqlQuery, "{uuid}") {
			if UUIDWithMergeTreeRE.Match(sqlBytes) {
				sqlQuery = UUIDWithMergeTreeRE.ReplaceAllString(sqlQuery, "$1$2$3'$4'$5$4$7")
			} else {
				apexLog.Warnf("%s contains `{uuid}` macro, but not contains UUID in table definition, will replace to `{database}/{table}` see https://github.com/ClickHouse/ClickHouse/issues/42709 for details", filePath)
				filePathParts := strings.Split(filePath, "/")
				database, err := url.QueryUnescape(filePathParts[len(filePathParts)-3])
				if err != nil {
					return err
				}
				table, err := url.QueryUnescape(filePathParts[len(filePathParts)-2])
				if err != nil {
					return err
				}
				sqlQuery = strings.Replace(sqlQuery, "{uuid}", database+"/"+table, 1)
			}
			if err = object_disk.WriteFileContent(ctx, b.ch, b.cfg, b.cfg.ClickHouse.EmbeddedBackupDisk, filePath, []byte(sqlQuery)); err != nil {
				return err
			}
			sqlMetadata.TotalSize = int64(len(sqlQuery))
			sqlMetadata.StorageObjects[0].ObjectSize = sqlMetadata.TotalSize
			if err = object_disk.WriteMetadataToFile(sqlMetadata, filePath); err != nil {
				return err
			}
		}
		if emptyReplicatedMergeTreeRE.MatchString(sqlQuery) {
			replicaXMLSettings := map[string]string{"default_replica_path": "//default_replica_path", "default_replica_name": "//default_replica_name"}
			settings, err := b.ch.GetPreprocessedXMLSettings(ctx, replicaXMLSettings, "config.xml")
			if err != nil {
				return err
			}
			if len(settings) != 2 {
				apexLog.Fatalf("can't get %#v from preprocessed_configs/config.xml", replicaXMLSettings)
			}
			apexLog.Warnf("%s contains `ReplicatedMergeTree()` without parameters, will replace to '%s` and `%s` see https://github.com/ClickHouse/ClickHouse/issues/42709 for details", filePath, settings["default_replica_path"], settings["default_replica_name"])
			matches := emptyReplicatedMergeTreeRE.FindStringSubmatch(sqlQuery)
			substitution := fmt.Sprintf("$1$2('%s','%s')$4", settings["default_replica_path"], settings["default_replica_name"])
			if matches[2] != "" {
				substitution = fmt.Sprintf("$1$2('%s','%s',$3)$4", settings["default_replica_path"], settings["default_replica_name"])
			}
			sqlQuery = emptyReplicatedMergeTreeRE.ReplaceAllString(sqlQuery, substitution)
		}
		return nil
	}); err != nil {
		return err
	}
	return b.restoreEmbedded(ctx, backupName, true, tablesForRestore, nil)
}

func (b *Backuper) restoreSchemaRegular(tablesForRestore ListOfTables, version int, log *apexLog.Entry) error {
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
			schema.Query = strings.Replace(
				schema.Query, "CREATE MATERIALIZED VIEW", "ATTACH MATERIALIZED VIEW", 1,
			)
			schema.Query = strings.Replace(
				schema.Query, "CREATE WINDOW VIEW", "ATTACH WINDOW VIEW", 1,
			)
			schema.Query = strings.Replace(
				schema.Query, "CREATE LIVE VIEW", "ATTACH LIVE VIEW", 1,
			)
			// https://github.com/Altinity/clickhouse-backup/issues/466
			if b.cfg.General.RestoreSchemaOnCluster == "" && strings.Contains(schema.Query, "{uuid}") && strings.Contains(schema.Query, "Replicated") {
				if !strings.Contains(schema.Query, "UUID") {
					log.Warnf("table query doesn't contains UUID, can't guarantee properly restore for ReplicatedMergeTree")
				} else {
					schema.Query = UUIDWithMergeTreeRE.ReplaceAllString(schema.Query, "$1$2$3'$4'$5$4$7")
				}
			}
			restoreErr = b.ch.CreateTable(clickhouse.Table{
				Database: schema.Database,
				Name:     schema.Table,
			}, schema.Query, false, false, b.cfg.General.RestoreSchemaOnCluster, version)

			if restoreErr != nil {
				restoreRetries++
				if restoreRetries >= totalRetries {
					return fmt.Errorf(
						"can't create table `%s`.`%s`: %v after %d times, please check your schema dependencies",
						schema.Database, schema.Table, restoreErr, restoreRetries,
					)
				} else {
					log.Warnf(
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

func (b *Backuper) dropExistsTables(tablesForDrop ListOfTables, ignoreDependencies bool, version int, log *apexLog.Entry) error {
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
					}, query, b.cfg.General.RestoreSchemaOnCluster, ignoreDependencies, version)
					if dropErr == nil {
						tablesForDrop[i].Query = query
					}
				}
			} else {
				dropErr = b.ch.DropTable(clickhouse.Table{
					Database: schema.Database,
					Name:     schema.Table,
				}, schema.Query, b.cfg.General.RestoreSchemaOnCluster, ignoreDependencies, version)
			}

			if dropErr != nil {
				dropRetries++
				if dropRetries >= totalRetries {
					return fmt.Errorf(
						"can't drop table `%s`.`%s`: %v after %d times, please check your schema dependencies",
						schema.Database, schema.Table, dropErr, dropRetries,
					)
				} else {
					log.Warnf(
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
func (b *Backuper) RestoreData(ctx context.Context, backupName string, tablePattern string, partitions []string, disks []clickhouse.Disk) error {
	startRestore := time.Now()
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "restore",
	})
	defaultDataPath, err := b.ch.GetDefaultPath(disks)
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	if b.ch.IsClickhouseShadow(path.Join(defaultDataPath, "backup", backupName, "shadow")) {
		return fmt.Errorf("backups created in v0.0.1 is not supported now")
	}
	backup, _, err := b.getLocalBackup(ctx, backupName, disks)
	if err != nil {
		return fmt.Errorf("can't restore: %v", err)
	}

	diskMap := make(map[string]string, len(disks))
	diskTypes := make(map[string]string, len(disks))
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
		diskTypes[disk.Name] = disk.Type
	}
	for diskName := range backup.Disks {
		if _, exists := diskMap[diskName]; !exists {
			diskMap[diskName] = backup.Disks[diskName]
		}
	}
	for diskName := range backup.DiskTypes {
		if _, exists := diskTypes[diskName]; !exists {
			diskTypes[diskName] = backup.DiskTypes[diskName]
		}
	}
	var tablesForRestore ListOfTables
	var partitionsNameList map[metadata.TableTitle][]string
	metadataPath := path.Join(defaultDataPath, "backup", backupName, "metadata")
	if b.isEmbedded {
		metadataPath = path.Join(diskMap[b.cfg.ClickHouse.EmbeddedBackupDisk], backupName, "metadata")
	}
	if backup.Legacy {
		tablesForRestore, err = b.ch.GetBackupTablesLegacy(backupName, disks)
	} else {
		tablesForRestore, partitionsNameList, err = b.getTableListByPatternLocal(ctx, metadataPath, tablePattern, false, partitions)
	}
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("no have found schemas by %s in %s", tablePattern, backupName)
	}
	log.Debugf("found %d tables with data in backup", len(tablesForRestore))
	if b.isEmbedded {
		err = b.restoreDataEmbedded(ctx, backupName, tablesForRestore, partitionsNameList)
	} else {
		err = b.restoreDataRegular(ctx, backupName, tablePattern, tablesForRestore, diskMap, diskTypes, disks, log)
	}
	if err != nil {
		return err
	}
	log.WithField("duration", utils.HumanizeDuration(time.Since(startRestore))).Info("done")
	return nil
}

func (b *Backuper) restoreDataEmbedded(ctx context.Context, backupName string, tablesForRestore ListOfTables, partitionsNameList map[metadata.TableTitle][]string) error {
	return b.restoreEmbedded(ctx, backupName, false, tablesForRestore, partitionsNameList)
}

func (b *Backuper) restoreDataRegular(ctx context.Context, backupName string, tablePattern string, tablesForRestore ListOfTables, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, log *apexLog.Entry) error {
	if len(b.cfg.General.RestoreDatabaseMapping) > 0 {
		tablePattern = b.changeTablePatternFromRestoreDatabaseMapping(tablePattern)
	}
	chTables, err := b.ch.GetTables(ctx, tablePattern)
	if err != nil {
		return err
	}
	disks = b.adjustDisksFromTablesWithSystemDisks(tablesForRestore, diskMap, diskTypes, disks, log)
	dstTablesMap := b.prepareDstTablesMap(chTables)

	missingTables := b.checkMissingTables(tablesForRestore, chTables)
	if len(missingTables) > 0 {
		return fmt.Errorf("%s is not created. Restore schema first or create missing tables manually", strings.Join(missingTables, ", "))
	}

	for i, table := range tablesForRestore {
		// need mapped database path and original table.Database for HardlinkBackupPartsToStorage
		dstDatabase := table.Database
		if len(b.cfg.General.RestoreDatabaseMapping) > 0 {
			if targetDB, isMapped := b.cfg.General.RestoreDatabaseMapping[table.Database]; isMapped {
				dstDatabase = targetDB
				tablesForRestore[i].Database = targetDB
			}
		}
		log := log.WithField("table", fmt.Sprintf("%s.%s", dstDatabase, table.Table))
		dstTable, ok := dstTablesMap[metadata.TableTitle{
			Database: dstDatabase,
			Table:    table.Table}]
		if !ok {
			return fmt.Errorf("can't find '%s.%s' in current system.tables", dstDatabase, table.Table)
		}
		// https://github.com/Altinity/clickhouse-backup/issues/529
		if b.cfg.ClickHouse.RestoreAsAttach {
			if err = b.restoreDataRegularByAttach(ctx, backupName, table, diskMap, diskTypes, disks, dstTable, log, tablesForRestore, i); err != nil {
				return err
			}
		} else {
			if err = b.restoreDataRegularByParts(ctx, backupName, table, diskMap, diskTypes, disks, dstTable, log, tablesForRestore, i); err != nil {
				return err
			}
		}
		// https://github.com/Altinity/clickhouse-backup/issues/529
		for _, mutation := range table.Mutations {
			if err := b.ch.ApplyMutation(ctx, tablesForRestore[i], mutation); err != nil {
				log.Warnf("can't apply mutation %s for table `%s`.`%s`	: %v", mutation.Command, tablesForRestore[i].Database, tablesForRestore[i].Table, err)
			}
		}
		log.Info("done")
	}
	return nil
}

func (b *Backuper) restoreDataRegularByAttach(ctx context.Context, backupName string, table metadata.TableMetadata, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, dstTable clickhouse.Table, log *apexLog.Entry, tablesForRestore ListOfTables, i int) error {
	if err := filesystemhelper.HardlinkBackupPartsToStorage(backupName, table, disks, dstTable.DataPaths, b.ch, false); err != nil {
		return fmt.Errorf("can't copy data to storage '%s.%s': %v", table.Database, table.Table, err)
	}
	log.Debug("data to 'storage' copied")
	if err := b.downloadObjectDiskParts(ctx, backupName, table, diskMap, diskTypes); err != nil {
		return fmt.Errorf("can't restore object_disk server-side copy data parts '%s.%s': %v", table.Database, table.Table, err)
	}

	if err := b.ch.AttachTable(ctx, tablesForRestore[i]); err != nil {
		return fmt.Errorf("can't attach table '%s.%s': %v", tablesForRestore[i].Database, tablesForRestore[i].Table, err)
	}
	return nil
}

func (b *Backuper) restoreDataRegularByParts(ctx context.Context, backupName string, table metadata.TableMetadata, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, dstTable clickhouse.Table, log *apexLog.Entry, tablesForRestore ListOfTables, i int) error {
	if err := filesystemhelper.HardlinkBackupPartsToStorage(backupName, table, disks, dstTable.DataPaths, b.ch, true); err != nil {
		return fmt.Errorf("can't copy data to datached '%s.%s': %v", table.Database, table.Table, err)
	}
	log.Debug("data to 'detached' copied")
	if err := b.downloadObjectDiskParts(ctx, backupName, table, diskMap, diskTypes); err != nil {
		return fmt.Errorf("can't restore object_disk server-side copy data parts '%s.%s': %v", table.Database, table.Table, err)
	}
	if err := b.ch.AttachDataParts(tablesForRestore[i], disks); err != nil {
		return fmt.Errorf("can't attach data parts for table '%s.%s': %v", tablesForRestore[i].Database, tablesForRestore[i].Table, err)
	}
	return nil
}

func (b *Backuper) downloadObjectDiskParts(ctx context.Context, backupName string, backupTable metadata.TableMetadata, diskMap, diskTypes map[string]string) error {
	log := apexLog.WithFields(apexLog.Fields{"operation": "downloadObjectDiskParts"})
	start := time.Now()
	dbAndTableDir := path.Join(common.TablePathEncode(backupTable.Database), common.TablePathEncode(backupTable.Table))
	var err error
	needToDownloadObjectDisk := false
	for diskName := range backupTable.Parts {
		diskType, exists := diskTypes[diskName]
		if !exists {
			return fmt.Errorf("%s disk doesn't present in diskTypes: %v", diskName, diskTypes)
		}
		if diskType == "s3" || diskType == "azure_blob_storage" {
			needToDownloadObjectDisk = true
			break
		}
	}
	if !needToDownloadObjectDisk {
		return nil
	}
	b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, false, backupName)
	if err != nil {
		return err
	}
	if err = b.dst.Connect(ctx); err != nil {
		return fmt.Errorf("can't connect to %s: %v", b.dst.Kind(), err)
	}
	defer func() {
		if err := b.dst.Close(ctx); err != nil {
			b.log.Warnf("downloadObjectDiskParts: can't close BackupDestination error: %v", err)
		}
	}()

	for diskName, parts := range backupTable.Parts {
		diskType, exists := diskTypes[diskName]
		if !exists {
			return fmt.Errorf("%s disk doesn't present in diskTypes: %v", diskName, diskTypes)
		}
		if diskType == "s3" || diskType == "azure_blob_storage" {
			if err = config.ValidateObjectDiskConfig(b.cfg); err != nil {
				return err
			}
			if err = object_disk.InitCredentialsAndConnections(ctx, b.ch, b.cfg, diskName); err != nil {
				return err
			}
			for _, part := range parts {
				partPath := path.Join(diskMap[diskName], "backup", backupName, "shadow", dbAndTableDir, diskName, part.Name)
				if err := filepath.Walk(partPath, func(fPath string, fInfo fs.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if fInfo.IsDir() {
						return nil
					}
					objMeta, err := object_disk.ReadMetadataFromFile(fPath)
					if err != nil {
						return err
					}
					if objMeta.StorageObjectCount < 1 {
						return fmt.Errorf("%s: invalid object_dist.Metadata: %#v", fPath, objMeta)
					}
					var srcBucket, srcKey string
					for _, storageObject := range objMeta.StorageObjects {
						if b.cfg.General.RemoteStorage == "s3" && diskType == "s3" {
							srcBucket = b.cfg.S3.Bucket
							srcKey = path.Join(b.cfg.S3.ObjectDiskPath, backupName, diskName, storageObject.ObjectRelativePath)
						} else if b.cfg.General.RemoteStorage == "gcs" && diskType == "s3" {
							srcBucket = b.cfg.GCS.Bucket
							srcKey = path.Join(b.cfg.GCS.ObjectDiskPath, backupName, diskName, storageObject.ObjectRelativePath)
						} else if b.cfg.General.RemoteStorage == "azblob" && diskType == "azure_blob_storage" {
							srcBucket = b.cfg.AzureBlob.Container
							srcKey = path.Join(b.cfg.AzureBlob.ObjectDiskPath, backupName, diskName, storageObject.ObjectRelativePath)
						} else {
							return fmt.Errorf("incompatible object_disk[%s].Type=%s amd remote_storage: %s", diskName, diskType, b.cfg.General.RemoteStorage)
						}
						if err = object_disk.CopyObject(ctx, b.ch, b.cfg, diskName, srcBucket, srcKey, storageObject.ObjectRelativePath); err != nil {
							return fmt.Errorf("object_disk.CopyObject error: %v", err)
						}
					}
					return nil
				}); err != nil {
					return err
				}
			}
		}
	}
	log.WithField("duration", utils.HumanizeDuration(time.Since(start))).Debugf("done")
	return nil
}

func (b *Backuper) checkMissingTables(tablesForRestore ListOfTables, chTables []clickhouse.Table) []string {
	var missingTables []string
	for _, table := range tablesForRestore {
		dstDatabase := table.Database
		if len(b.cfg.General.RestoreDatabaseMapping) > 0 {
			if targetDB, isMapped := b.cfg.General.RestoreDatabaseMapping[table.Database]; isMapped {
				dstDatabase = targetDB
			}
		}
		found := false
		for _, chTable := range chTables {
			if (dstDatabase == chTable.Database) && (table.Table == chTable.Name) {
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

func (b *Backuper) adjustDisksFromTablesWithSystemDisks(tablesForRestore ListOfTables, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, log *apexLog.Entry) []clickhouse.Disk {
	for _, t := range tablesForRestore {
		for disk := range t.Parts {
			if _, diskExists := diskMap[disk]; !diskExists {
				if diskTypes[disk] != diskTypes["default"] {
					log.Fatalf("table '%s.%s' require disk '%s' that not found in clickhouse table system.disks, and have different diskType %s than `default` disk %s", t.Database, t.Table, disk, diskTypes[disk], diskTypes["default"])
				}
				log.Warnf("table '%s.%s' require disk '%s' that not found in clickhouse table system.disks, you can add nonexistent disks to `disk_mapping` in  `clickhouse` config section, data will restored to %s", t.Database, t.Table, disk, diskMap["default"])
				found := false
				for _, d := range disks {
					if d.Name == disk {
						found = true
						break
					}
				}
				if !found {
					newDisk := clickhouse.Disk{
						Name: disk,
						Path: diskMap["default"],
						Type: "local",
					}
					disks = append(disks, newDisk)
				}
			}
		}
	}
	return disks
}

func (b *Backuper) changeTablePatternFromRestoreDatabaseMapping(tablePattern string) string {
	for sourceDb, targetDb := range b.cfg.General.RestoreDatabaseMapping {
		if tablePattern != "" {
			sourceDbRE := regexp.MustCompile(fmt.Sprintf("(^%s.*)|(,%s.*)", sourceDb, sourceDb))
			if sourceDbRE.MatchString(tablePattern) {
				matches := sourceDbRE.FindAllStringSubmatch(tablePattern, -1)
				substitution := targetDb + ".*"
				if strings.HasPrefix(matches[0][1], ",") {
					substitution = "," + substitution
				}
				tablePattern = sourceDbRE.ReplaceAllString(tablePattern, substitution)
			} else {
				tablePattern += "," + targetDb + ".*"
			}
		} else {
			tablePattern += targetDb + ".*"
		}
	}
	return tablePattern
}

func (b *Backuper) restoreEmbedded(ctx context.Context, backupName string, restoreOnlySchema bool, tablesForRestore ListOfTables, partitionsNameList map[metadata.TableTitle][]string) error {
	restoreSQL := "Disk(?,?)"
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
					partitionsSQL := fmt.Sprintf("'%s'", strings.Join(tablePartitions, "','"))
					if strings.HasPrefix(partitionsSQL, "'(") {
						partitionsSQL = strings.Join(tablePartitions, ",")
					}
					tablesSQL += fmt.Sprintf(" PARTITIONS %s", partitionsSQL)
				}
			}
			if i < l-1 {
				tablesSQL += ", "
			}
		}
	}
	settings := ""
	if restoreOnlySchema {
		settings = "SETTINGS structure_only=1"
	}
	restoreSQL = fmt.Sprintf("RESTORE %s FROM %s %s", tablesSQL, restoreSQL, settings)
	restoreResults := make([]clickhouse.SystemBackups, 0)
	if err := b.ch.SelectContext(ctx, &restoreResults, restoreSQL, b.cfg.ClickHouse.EmbeddedBackupDisk, backupName); err != nil {
		return fmt.Errorf("restore error: %v", err)
	}
	if len(restoreResults) == 0 || restoreResults[0].Status != "RESTORED" {
		return fmt.Errorf("restore wrong result: %v", restoreResults)
	}
	return nil
}
