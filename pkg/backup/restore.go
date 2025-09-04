package backup

import (
	"bufio"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
	"github.com/Altinity/clickhouse-backup/v2/pkg/resumable"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

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

// PartInfo holds information about a data part for comparison
type PartInfo struct {
	Name string
	Disk string
}

// PartComparison holds the result of comparing backup parts vs current database parts
type PartComparison struct {
	PartsToRemove   []PartInfo
	PartsToDownload []PartInfo
	PartsToKeep     []PartInfo
}

// RestoreInPlace - restore tables in-place by comparing backup parts with current database parts
func (b *Backuper) RestoreInPlace(backupName, tablePattern string, commandId int) error {
	if pidCheckErr := pidlock.CheckAndCreatePidFile(backupName, "restore-in-place"); pidCheckErr != nil {
		return pidCheckErr
	}
	defer pidlock.RemovePidFile(backupName)

	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	startRestore := time.Now()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")

	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	if backupName == "" {
		localBackups := b.CollectLocalBackups(ctx, "all")
		_ = b.PrintBackup(localBackups, "all", "text")
		return fmt.Errorf("select backup for restore")
	}

	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		return err
	}
	b.DefaultDataPath, err = b.ch.GetDefaultPath(disks)
	if err != nil {
		log.Warn().Msgf("%v", err)
		return ErrUnknownClickhouseDataPath
	}

	// Load backup metadata
	backupMetafileLocalPaths := []string{path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")}
	var backupMetadataBody []byte
	b.EmbeddedBackupDataPath, err = b.ch.GetEmbeddedBackupPath(disks)
	if err == nil && b.EmbeddedBackupDataPath != "" {
		backupMetafileLocalPaths = append(backupMetafileLocalPaths, path.Join(b.EmbeddedBackupDataPath, backupName, "metadata.json"))
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

	if tablePattern == "" {
		tablePattern = "*"
	}

	metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
	b.isEmbedded = strings.Contains(backupMetadata.Tags, "embedded")
	if b.isEmbedded && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		metadataPath = path.Join(b.EmbeddedBackupDataPath, backupName, "metadata")
	}

	tablesForRestore, _, err := b.getTablesForRestoreLocal(ctx, backupName, metadataPath, tablePattern, false, nil)
	if err != nil {
		return err
	}

	if len(tablesForRestore) == 0 {
		if !b.cfg.General.AllowEmptyBackups {
			return fmt.Errorf("no tables found for restore by pattern %s in %s", tablePattern, backupName)
		}
		log.Warn().Msgf("no tables found for restore by pattern %s in %s", tablePattern, backupName)
		return nil
	}

	// Get current tables in database
	currentTables, err := b.ch.GetTables(ctx, tablePattern)
	if err != nil {
		return err
	}

	// Process each table in parallel
	restoreWorkingGroup, restoreCtx := errgroup.WithContext(ctx)
	restoreWorkingGroup.SetLimit(max(b.cfg.ClickHouse.MaxConnections, 1))

	for i := range tablesForRestore {
		table := *tablesForRestore[i]
		idx := i
		restoreWorkingGroup.Go(func() error {
			return b.processTableInPlace(restoreCtx, table, currentTables, backupMetadata, idx+1, len(tablesForRestore))
		})
	}

	if err := restoreWorkingGroup.Wait(); err != nil {
		return fmt.Errorf("in-place restore failed: %v", err)
	}

	log.Info().Fields(map[string]interface{}{
		"operation": "restore_in_place",
		"duration":  utils.HumanizeDuration(time.Since(startRestore)),
	}).Msg("done")
	return nil
}

// RestoreInPlaceFromRemote - restore tables in-place from remote backup by comparing parts and downloading only what's needed
func (b *Backuper) RestoreInPlaceFromRemote(backupName, tablePattern string, commandId int, dropIfSchemaChanged bool) error {
	if pidCheckErr := pidlock.CheckAndCreatePidFile(backupName, "restore-in-place-remote"); pidCheckErr != nil {
		return pidCheckErr
	}
	defer pidlock.RemovePidFile(backupName)

	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	startRestore := time.Now()
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")

	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()

	if backupName == "" {
		return fmt.Errorf("backup name is required")
	}

	// Initialize remote storage connection
	if err = b.CalculateMaxSize(ctx); err != nil {
		return err
	}
	b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, backupName)
	if err != nil {
		return err
	}
	if err = b.dst.Connect(ctx); err != nil {
		return fmt.Errorf("can't connect to %s: %v", b.dst.Kind(), err)
	}
	defer func() {
		if err := b.dst.Close(ctx); err != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", err)
		}
	}()

	// Read backup metadata from remote storage
	backupList, err := b.dst.BackupList(ctx, true, backupName)
	if err != nil {
		return fmt.Errorf("failed to get backup list: %v", err)
	}

	var backupMetadata *metadata.BackupMetadata
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			backupMetadata = &backup.BackupMetadata
			break
		}
	}
	if backupMetadata == nil {
		return fmt.Errorf("backup %s not found in remote storage", backupName)
	}

	if tablePattern == "" {
		tablePattern = "*"
	}

	// Get tables from remote backup metadata
	tablesForRestore, err := getTableListByPatternRemote(ctx, b, backupMetadata, tablePattern, false)
	if err != nil {
		return err
	}

	if len(tablesForRestore) == 0 {
		if !b.cfg.General.AllowEmptyBackups {
			return fmt.Errorf("no tables found for restore by pattern %s in %s", tablePattern, backupName)
		}
		log.Warn().Msgf("no tables found for restore by pattern %s in %s", tablePattern, backupName)
		return nil
	}

	// Get current tables in database
	currentTables, err := b.ch.GetTables(ctx, tablePattern)
	if err != nil {
		return err
	}

	// Process each table in parallel
	restoreWorkingGroup, restoreCtx := errgroup.WithContext(ctx)
	restoreWorkingGroup.SetLimit(max(b.cfg.ClickHouse.MaxConnections, 1))

	for i := range tablesForRestore {
		table := *tablesForRestore[i]
		idx := i
		restoreWorkingGroup.Go(func() error {
			return b.processTableInPlaceFromRemote(restoreCtx, table, currentTables, *backupMetadata, idx+1, len(tablesForRestore), dropIfSchemaChanged)
		})
	}

	if err := restoreWorkingGroup.Wait(); err != nil {
		return fmt.Errorf("in-place restore from remote failed: %v", err)
	}

	log.Info().Fields(map[string]interface{}{
		"operation": "restore_in_place_remote",
		"duration":  utils.HumanizeDuration(time.Since(startRestore)),
	}).Msg("done")
	return nil
}

// processTableInPlaceFromRemote processes a single table for remote metadata-driven in-place restore
func (b *Backuper) processTableInPlaceFromRemote(ctx context.Context, backupTable metadata.TableMetadata, currentTables []clickhouse.Table, backupMetadata metadata.BackupMetadata, idx, total int, dropIfSchemaChanged bool) error {
	logger := log.With().Str("table", fmt.Sprintf("%s.%s", backupTable.Database, backupTable.Table)).Logger()
	logger.Info().Msgf("processing table %d/%d for remote metadata-driven in-place restore", idx, total)

	// Check if table exists in current database
	currentTable := b.findCurrentTable(currentTables, backupTable.Database, backupTable.Table)

	// Schema comparison logic when --drop-if-schema-changed flag is provided and table exists
	if currentTable != nil && dropIfSchemaChanged {
		schemaChanged, err := b.checkSchemaChanges(ctx, backupTable, *currentTable)
		if err != nil {
			return fmt.Errorf("failed to check schema changes for %s.%s: %w", backupTable.Database, backupTable.Table, err)
		}

		if schemaChanged {
			logger.Info().Fields(map[string]interface{}{
				"operation":      "schema_change_detected",
				"database":       backupTable.Database,
				"table":          backupTable.Table,
				"action":         "drop_and_recreate",
				"reason":         "schema_mismatch",
				"drop_triggered": true,
			}).Msg("schema changed for table, dropping and recreating from backup")

			// Drop the existing table
			if err := b.ch.DropOrDetachTable(clickhouse.Table{
				Database: currentTable.Database,
				Name:     currentTable.Name,
			}, currentTable.CreateTableQuery, b.cfg.General.RestoreSchemaOnCluster, false, 0, b.DefaultDataPath, false, ""); err != nil {
				return fmt.Errorf("failed to drop table %s.%s: %w", backupTable.Database, backupTable.Table, err)
			}
			logger.Info().Fields(map[string]interface{}{
				"operation":     "table_dropped",
				"database":      backupTable.Database,
				"table":         backupTable.Table,
				"reason":        "schema_change",
				"table_dropped": true,
			}).Msg("table dropped successfully due to schema changes")

			// Recreate table from backup
			if err := b.createTableFromBackup(ctx, backupTable); err != nil {
				return fmt.Errorf("failed to recreate table %s.%s from backup: %w", backupTable.Database, backupTable.Table, err)
			}
			logger.Info().Fields(map[string]interface{}{
				"operation":       "table_recreated",
				"database":        backupTable.Database,
				"table":           backupTable.Table,
				"table_recreated": true,
			}).Msg("table recreated from backup schema")

			// Download all parts for the recreated table
			var partsToDownload []PartInfo
			for diskName, parts := range backupTable.Parts {
				for _, part := range parts {
					partsToDownload = append(partsToDownload, PartInfo{
						Name: part.Name,
						Disk: diskName,
					})
				}
			}
			if len(partsToDownload) > 0 {
				logger.Info().Fields(map[string]interface{}{
					"operation":   "download_all_parts",
					"database":    backupTable.Database,
					"table":       backupTable.Table,
					"parts_count": len(partsToDownload),
					"reason":      "table_recreated",
				}).Msgf("downloading all %d parts for recreated table", len(partsToDownload))

				// Download table metadata first
				tableMetadata, err := b.downloadTableMetadataIfNotExists(ctx, backupMetadata.BackupName, metadata.TableTitle{Database: backupTable.Database, Table: backupTable.Table})
				if err != nil {
					return fmt.Errorf("failed to download table metadata: %v", err)
				}
				// Get current table info after recreation
				updatedTables, err := b.ch.GetTables(ctx, fmt.Sprintf("%s.%s", backupTable.Database, backupTable.Table))
				if err != nil {
					return fmt.Errorf("failed to get updated table info: %v", err)
				}
				if len(updatedTables) == 0 {
					return fmt.Errorf("table not found after recreation")
				}
				return b.downloadAndAttachPartsToDatabase(ctx, backupMetadata.BackupName, backupTable.Database, backupTable.Table, partsToDownload, *tableMetadata, backupMetadata, updatedTables[0])
			}
			return nil
		}
	}

	if currentTable == nil {
		// Table doesn't exist in DB but exists in backup -> CREATE TABLE first, then download data
		logger.Info().Msg("table not found in database, creating table schema from remote backup")
		if err := b.createTableFromBackup(ctx, backupTable); err != nil {
			return err
		}
		// After creating table, we need to download all parts (no comparison needed)
		// Convert all backup parts to PartsToDownload format
		var partsToDownload []PartInfo
		for diskName, parts := range backupTable.Parts {
			for _, part := range parts {
				partsToDownload = append(partsToDownload, PartInfo{
					Name: part.Name,
					Disk: diskName,
				})
			}
		}
		if len(partsToDownload) > 0 {
			logger.Info().Fields(map[string]interface{}{
				"operation":   "download_all_parts",
				"database":    backupTable.Database,
				"table":       backupTable.Table,
				"parts_count": len(partsToDownload),
				"reason":      "newly_created_table",
			}).Msgf("downloading all %d parts for newly created table", len(partsToDownload))

			// Download table metadata first
			tableMetadata, err := b.downloadTableMetadataIfNotExists(ctx, backupMetadata.BackupName, metadata.TableTitle{Database: backupTable.Database, Table: backupTable.Table})
			if err != nil {
				return fmt.Errorf("failed to download table metadata: %v", err)
			}
			// Get current table info after creation
			updatedTables, err := b.ch.GetTables(ctx, fmt.Sprintf("%s.%s", backupTable.Database, backupTable.Table))
			if err != nil {
				return fmt.Errorf("failed to get updated table info: %v", err)
			}
			if len(updatedTables) == 0 {
				return fmt.Errorf("table not found after creation")
			}
			return b.downloadAndAttachPartsToDatabase(ctx, backupMetadata.BackupName, backupTable.Database, backupTable.Table, partsToDownload, *tableMetadata, backupMetadata, updatedTables[0])
		}
		return nil
	}

	// Use metadata-driven approach: Compare backup parts vs actual current parts
	actualCurrentParts, err := b.getCurrentPartsFromDatabase(ctx, backupTable.Database, backupTable.Table)
	if err != nil {
		return fmt.Errorf("failed to get actual current parts for table %s.%s: %v", backupTable.Database, backupTable.Table, err)
	}

	// Perform metadata-driven comparison using stored current parts from backup creation
	comparison := b.comparePartsMetadataDriven(backupTable.Parts, backupTable.Parts, actualCurrentParts)

	// Log detailed part comparison results
	logger.Info().Fields(map[string]interface{}{
		"operation":         "part_comparison_completed",
		"database":          backupTable.Database,
		"table":             backupTable.Table,
		"parts_to_remove":   len(comparison.PartsToRemove),
		"parts_to_download": len(comparison.PartsToDownload),
		"parts_to_keep":     len(comparison.PartsToKeep),
		"table_dropped":     false,
	}).Msg("remote metadata-driven part comparison completed")

	// Log specific parts that will be removed
	if len(comparison.PartsToRemove) > 0 {
		for _, part := range comparison.PartsToRemove {
			logger.Info().Fields(map[string]interface{}{
				"operation": "part_scheduled_for_removal",
				"database":  backupTable.Database,
				"table":     backupTable.Table,
				"part_name": part.Name,
				"disk":      part.Disk,
			}).Msgf("part %s on disk %s will be removed", part.Name, part.Disk)
		}
	}

	// Log specific parts that will be downloaded
	if len(comparison.PartsToDownload) > 0 {
		for _, part := range comparison.PartsToDownload {
			logger.Info().Fields(map[string]interface{}{
				"operation": "part_scheduled_for_download",
				"database":  backupTable.Database,
				"table":     backupTable.Table,
				"part_name": part.Name,
				"disk":      part.Disk,
			}).Msgf("part %s on disk %s will be downloaded", part.Name, part.Disk)
		}
	}

	// CRITICAL: Remove parts first to avoid disk space issues
	if len(comparison.PartsToRemove) > 0 {
		logger.Info().Fields(map[string]interface{}{
			"operation":   "removing_unwanted_parts",
			"database":    backupTable.Database,
			"table":       backupTable.Table,
			"parts_count": len(comparison.PartsToRemove),
			"reason":      "free_disk_space",
		}).Msgf("removing %d unwanted parts first to free disk space", len(comparison.PartsToRemove))

		if err := b.removePartsFromDatabase(ctx, backupTable.Database, backupTable.Table, comparison.PartsToRemove); err != nil {
			return fmt.Errorf("failed to remove unwanted parts: %v", err)
		}

		logger.Info().Fields(map[string]interface{}{
			"operation":           "parts_removed_successfully",
			"database":            backupTable.Database,
			"table":               backupTable.Table,
			"removed_parts_count": len(comparison.PartsToRemove),
			"table_dropped":       false,
		}).Msgf("successfully removed %d unwanted parts", len(comparison.PartsToRemove))
	}

	// Then download and attach missing parts from remote
	if len(comparison.PartsToDownload) > 0 {
		logger.Info().Fields(map[string]interface{}{
			"operation":   "downloading_missing_parts",
			"database":    backupTable.Database,
			"table":       backupTable.Table,
			"parts_count": len(comparison.PartsToDownload),
			"source":      "remote_backup",
		}).Msgf("downloading and attaching %d missing parts from remote backup", len(comparison.PartsToDownload))

		// Download table metadata if needed
		tableMetadata, err := b.downloadTableMetadataIfNotExists(ctx, backupMetadata.BackupName, metadata.TableTitle{Database: backupTable.Database, Table: backupTable.Table})
		if err != nil {
			return fmt.Errorf("failed to download table metadata: %v", err)
		}
		if err := b.downloadAndAttachPartsToDatabase(ctx, backupMetadata.BackupName, backupTable.Database, backupTable.Table, comparison.PartsToDownload, *tableMetadata, backupMetadata, *currentTable); err != nil {
			return fmt.Errorf("failed to download and attach missing parts: %v", err)
		}

		logger.Info().Fields(map[string]interface{}{
			"operation":              "parts_downloaded_successfully",
			"database":               backupTable.Database,
			"table":                  backupTable.Table,
			"downloaded_parts_count": len(comparison.PartsToDownload),
			"table_dropped":          false,
		}).Msgf("successfully downloaded and attached %d missing parts", len(comparison.PartsToDownload))
	}

	// Parts to keep require no action
	if len(comparison.PartsToKeep) > 0 {
		logger.Info().Fields(map[string]interface{}{
			"operation":        "keeping_common_parts",
			"database":         backupTable.Database,
			"table":            backupTable.Table,
			"kept_parts_count": len(comparison.PartsToKeep),
		}).Msgf("keeping %d common parts unchanged", len(comparison.PartsToKeep))
	}

	logger.Info().Fields(map[string]interface{}{
		"operation":     "restore_completed",
		"database":      backupTable.Database,
		"table":         backupTable.Table,
		"table_dropped": false,
		"restore_type":  "in_place_remote",
	}).Msg("remote metadata-driven in-place restore completed for table")
	return nil
}

// processTableInPlace processes a single table for metadata-driven in-place restore
func (b *Backuper) processTableInPlace(ctx context.Context, backupTable metadata.TableMetadata, currentTables []clickhouse.Table, backupMetadata metadata.BackupMetadata, idx, total int) error {
	logger := log.With().Str("table", fmt.Sprintf("%s.%s", backupTable.Database, backupTable.Table)).Logger()
	logger.Info().Msgf("processing table %d/%d for metadata-driven in-place restore", idx, total)

	// Check if table exists in current database
	currentTable := b.findCurrentTable(currentTables, backupTable.Database, backupTable.Table)

	if currentTable == nil {
		// Table doesn't exist in DB but exists in backup -> CREATE TABLE
		logger.Info().Msg("table not found in database, creating table schema")
		return b.createTableFromBackup(ctx, backupTable)
	}

	// Use metadata-driven approach: Compare backup parts vs stored current parts vs actual current parts
	// This avoids querying the live database for planning and uses the stored state from backup creation
	actualCurrentParts, err := b.getCurrentPartsFromDatabase(ctx, backupTable.Database, backupTable.Table)
	if err != nil {
		return fmt.Errorf("failed to get actual current parts for table %s.%s: %v", backupTable.Database, backupTable.Table, err)
	}

	// Perform metadata-driven comparison using stored current parts from backup creation
	comparison := b.comparePartsMetadataDriven(backupTable.Parts, backupTable.Parts, actualCurrentParts)

	logger.Info().Fields(map[string]interface{}{
		"parts_to_remove":   len(comparison.PartsToRemove),
		"parts_to_download": len(comparison.PartsToDownload),
		"parts_to_keep":     len(comparison.PartsToKeep),
	}).Msg("metadata-driven part comparison completed")

	// CRITICAL: Remove parts first to avoid disk space issues
	if len(comparison.PartsToRemove) > 0 {
		logger.Info().Msgf("removing %d unwanted parts first to free disk space", len(comparison.PartsToRemove))
		if err := b.removePartsFromDatabase(ctx, backupTable.Database, backupTable.Table, comparison.PartsToRemove); err != nil {
			return fmt.Errorf("failed to remove unwanted parts: %v", err)
		}
		logger.Info().Msgf("successfully removed %d unwanted parts", len(comparison.PartsToRemove))
	}

	// Then download and attach missing parts
	if len(comparison.PartsToDownload) > 0 {
		logger.Info().Msgf("downloading and attaching %d missing parts", len(comparison.PartsToDownload))
		if err := b.downloadAndAttachPartsToDatabase(ctx, backupMetadata.BackupName, backupTable.Database, backupTable.Table, comparison.PartsToDownload, backupTable, backupMetadata, *currentTable); err != nil {
			return fmt.Errorf("failed to download and attach missing parts: %v", err)
		}
		logger.Info().Msgf("successfully downloaded and attached %d missing parts", len(comparison.PartsToDownload))
	}

	// Parts to keep require no action
	if len(comparison.PartsToKeep) > 0 {
		logger.Info().Msgf("keeping %d common parts unchanged", len(comparison.PartsToKeep))
	}

	logger.Info().Msg("metadata-driven in-place restore completed for table")
	return nil
}

// findCurrentTable finds a table in the current database tables list
func (b *Backuper) findCurrentTable(currentTables []clickhouse.Table, database, table string) *clickhouse.Table {
	for i := range currentTables {
		if currentTables[i].Database == database && currentTables[i].Name == table {
			return &currentTables[i]
		}
	}
	return nil
}

// getCurrentParts gets current parts from the database for a specific table
func (b *Backuper) getCurrentParts(ctx context.Context, database, table string) (map[string][]string, error) {
	query := "SELECT disk_name, name FROM system.parts WHERE active AND database=? AND table=?"
	rows := make([]struct {
		DiskName string `ch:"disk_name"`
		Name     string `ch:"name"`
	}, 0)

	if err := b.ch.SelectContext(ctx, &rows, query, database, table); err != nil {
		return nil, err
	}

	parts := make(map[string][]string)
	for _, row := range rows {
		if _, exists := parts[row.DiskName]; !exists {
			parts[row.DiskName] = make([]string, 0)
		}
		parts[row.DiskName] = append(parts[row.DiskName], row.Name)
	}

	return parts, nil
}

// compareParts compares backup parts vs current database parts
func (b *Backuper) compareParts(backupParts map[string][]metadata.Part, currentParts map[string][]string) PartComparison {
	var comparison PartComparison

	// Create maps for fast lookup
	backupPartMap := make(map[string]string)  // partName -> diskName
	currentPartMap := make(map[string]string) // partName -> diskName

	for diskName, parts := range backupParts {
		for _, part := range parts {
			backupPartMap[part.Name] = diskName
		}
	}

	for diskName, parts := range currentParts {
		for _, partName := range parts {
			currentPartMap[partName] = diskName
		}
	}

	// Find parts to remove (in current but not in backup)
	for partName, diskName := range currentPartMap {
		if _, exists := backupPartMap[partName]; !exists {
			comparison.PartsToRemove = append(comparison.PartsToRemove, PartInfo{
				Name: partName,
				Disk: diskName,
			})
		}
	}

	// Find parts to download (in backup but not in current)
	for partName, diskName := range backupPartMap {
		if _, exists := currentPartMap[partName]; !exists {
			comparison.PartsToDownload = append(comparison.PartsToDownload, PartInfo{
				Name: partName,
				Disk: diskName,
			})
		} else {
			// Part exists in both, keep it
			comparison.PartsToKeep = append(comparison.PartsToKeep, PartInfo{
				Name: partName,
				Disk: diskName,
			})
		}
	}

	return comparison
}

// createTableFromBackup creates a table from backup metadata
func (b *Backuper) createTableFromBackup(ctx context.Context, table metadata.TableMetadata) error {
	// Create database if not exists
	if err := b.ch.CreateDatabase(table.Database, ""); err != nil {
		return fmt.Errorf("failed to create database %s: %v", table.Database, err)
	}

	// Create the table
	if err := b.ch.CreateTable(clickhouse.Table{
		Database: table.Database,
		Name:     table.Table,
	}, table.Query, false, false, "", 0, b.DefaultDataPath, false, ""); err != nil {
		return fmt.Errorf("failed to create table %s.%s: %v", table.Database, table.Table, err)
	}

	return nil
}

// removeUnwantedParts removes parts that exist in the database but not in backup
func (b *Backuper) removeUnwantedParts(ctx context.Context, table metadata.TableMetadata, partsToRemove []PartInfo) error {
	for _, part := range partsToRemove {
		query := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PART '%s'", table.Database, table.Table, part.Name)
		if err := b.ch.QueryContext(ctx, query); err != nil {
			log.Warn().Msgf("failed to drop part %s from table %s.%s: %v", part.Name, table.Database, table.Table, err)
			// Continue with other parts even if one fails
		} else {
			log.Debug().Msgf("dropped part %s from table %s.%s", part.Name, table.Database, table.Table)
		}
	}
	return nil
}

// downloadAndAttachMissingParts downloads and attaches parts that exist in backup but not in database
func (b *Backuper) downloadAndAttachMissingParts(ctx context.Context, backupTable metadata.TableMetadata, backupMetadata metadata.BackupMetadata, partsToDownload []PartInfo, currentTable clickhouse.Table) error {
	// Filter the backup table to only include parts we need to download
	filteredTable := backupTable
	filteredTable.Parts = make(map[string][]metadata.Part)

	// Create a map of parts to download for fast lookup
	partsToDownloadMap := make(map[string]bool)
	for _, part := range partsToDownload {
		partsToDownloadMap[part.Name] = true
	}

	// Filter backup parts to only include the ones we need
	for diskName, parts := range backupTable.Parts {
		filteredParts := make([]metadata.Part, 0)
		for _, part := range parts {
			if partsToDownloadMap[part.Name] {
				filteredParts = append(filteredParts, part)
			}
		}
		if len(filteredParts) > 0 {
			filteredTable.Parts[diskName] = filteredParts
		}
	}

	if len(filteredTable.Parts) == 0 {
		return nil // No parts to download
	}

	// Use existing restore logic to download and attach the filtered parts
	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		return err
	}

	diskMap := make(map[string]string, len(disks))
	diskTypes := make(map[string]string, len(disks))
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
		diskTypes[disk.Name] = disk.Type
	}

	logger := log.With().Str("table", fmt.Sprintf("%s.%s", backupTable.Database, backupTable.Table)).Logger()

	// Use the regular restore logic but only for the filtered parts
	return b.restoreDataRegularByParts(ctx, backupMetadata.BackupName, backupMetadata, filteredTable, diskMap, diskTypes, disks, currentTable, nil, logger, false)
}

// getCurrentPartsFromDatabase gets actual current parts from the database for metadata-driven comparison
func (b *Backuper) getCurrentPartsFromDatabase(ctx context.Context, database, table string) (map[string][]metadata.Part, error) {
	query := "SELECT disk_name, name FROM system.parts WHERE active AND database=? AND table=?"
	rows := make([]struct {
		DiskName string `ch:"disk_name"`
		Name     string `ch:"name"`
	}, 0)

	if err := b.ch.SelectContext(ctx, &rows, query, database, table); err != nil {
		return nil, err
	}

	parts := make(map[string][]metadata.Part)
	for _, row := range rows {
		if _, exists := parts[row.DiskName]; !exists {
			parts[row.DiskName] = make([]metadata.Part, 0)
		}
		parts[row.DiskName] = append(parts[row.DiskName], metadata.Part{Name: row.Name})
	}

	return parts, nil
}

// comparePartsMetadataDriven performs metadata-driven comparison of parts
func (b *Backuper) comparePartsMetadataDriven(backupParts, storedCurrentParts, actualCurrentParts map[string][]metadata.Part) PartComparison {
	var comparison PartComparison

	// Create maps for fast lookup
	backupPartMap := make(map[string]string)        // partName -> diskName
	storedCurrentPartMap := make(map[string]string) // partName -> diskName
	actualCurrentPartMap := make(map[string]string) // partName -> diskName

	// Build backup parts map
	for diskName, parts := range backupParts {
		for _, part := range parts {
			backupPartMap[part.Name] = diskName
		}
	}

	// Build stored current parts map (from backup creation time)
	for diskName, parts := range storedCurrentParts {
		for _, part := range parts {
			storedCurrentPartMap[part.Name] = diskName
		}
	}

	// Build actual current parts map (current database state)
	for diskName, parts := range actualCurrentParts {
		for _, part := range parts {
			actualCurrentPartMap[part.Name] = diskName
		}
	}

	// Find parts to remove: Parts that exist in actual current DB but not in backup
	for partName, diskName := range actualCurrentPartMap {
		if _, existsInBackup := backupPartMap[partName]; !existsInBackup {
			comparison.PartsToRemove = append(comparison.PartsToRemove, PartInfo{
				Name: partName,
				Disk: diskName,
			})
		}
	}

	// Find parts to download: Parts that exist in backup but not in actual current DB
	for partName, diskName := range backupPartMap {
		if _, existsInActualCurrent := actualCurrentPartMap[partName]; !existsInActualCurrent {
			comparison.PartsToDownload = append(comparison.PartsToDownload, PartInfo{
				Name: partName,
				Disk: diskName,
			})
		} else {
			// Part exists in both backup and actual current, keep it
			comparison.PartsToKeep = append(comparison.PartsToKeep, PartInfo{
				Name: partName,
				Disk: diskName,
			})
		}
	}

	return comparison
}

// checkSchemaChanges compares the schema of a table in backup with current database table
func (b *Backuper) checkSchemaChanges(ctx context.Context, backupTable metadata.TableMetadata, currentTable clickhouse.Table) (bool, error) {
	// Get current table's CREATE statement from database
	var currentCreateQuery struct {
		Statement string `ch:"statement"`
	}

	query := "SHOW CREATE TABLE `" + currentTable.Database + "`.`" + currentTable.Name + "`"
	if err := b.ch.SelectContext(ctx, &currentCreateQuery, query); err != nil {
		return false, fmt.Errorf("failed to get current table schema: %v", err)
	}

	// Normalize both queries for comparison (remove extra whitespace, etc.)
	backupSchema := b.normalizeCreateTableQuery(backupTable.Query)
	currentSchema := b.normalizeCreateTableQuery(currentCreateQuery.Statement)

	// Compare normalized schemas
	return backupSchema != currentSchema, nil
}

// normalizeCreateTableQuery normalizes a CREATE TABLE query for schema comparison
func (b *Backuper) normalizeCreateTableQuery(query string) string {
	// Remove leading/trailing whitespace and normalize internal whitespace
	normalized := strings.TrimSpace(query)

	// Replace multiple whitespace with single space
	re := regexp.MustCompile(`\s+`)
	normalized = re.ReplaceAllString(normalized, " ")

	// Convert to uppercase for case-insensitive comparison
	normalized = strings.ToUpper(normalized)

	// Remove potential UUID differences for comparison (since UUIDs will be different)
	uuidRE := regexp.MustCompile(`UUID\s+'[^']+'`)
	normalized = uuidRE.ReplaceAllString(normalized, "UUID 'PLACEHOLDER'")

	return normalized
}

// removePartsFromDatabase removes specific parts from the database
func (b *Backuper) removePartsFromDatabase(ctx context.Context, database, table string, partsToRemove []PartInfo) error {
	for _, part := range partsToRemove {
		// Log detailed part information for all operations (both local and remote)
		log.Info().Fields(map[string]interface{}{
			"operation":   "removing_part",
			"database":    database,
			"table":       table,
			"part_name":   part.Name,
			"disk":        part.Disk,
			"part_status": "starting_removal",
		}).Msgf("removing part %s from table %s.%s on disk %s", part.Name, database, table, part.Disk)

		query := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PART '%s'", database, table, part.Name)
		if err := b.ch.QueryContext(ctx, query); err != nil {
			log.Warn().Fields(map[string]interface{}{
				"operation":   "part_removal_failed",
				"database":    database,
				"table":       table,
				"part_name":   part.Name,
				"disk":        part.Disk,
				"part_status": "removal_failed",
				"error":       err.Error(),
			}).Msgf("failed to drop part %s from table %s.%s: %v", part.Name, database, table, err)
			// Continue with other parts even if one fails
		} else {
			log.Info().Fields(map[string]interface{}{
				"operation":   "part_removed_successfully",
				"database":    database,
				"table":       table,
				"part_name":   part.Name,
				"disk":        part.Disk,
				"part_status": "removed",
			}).Msgf("successfully removed part %s from table %s.%s on disk %s", part.Name, database, table, part.Disk)
		}
	}
	return nil
}

// downloadAndAttachPartsToDatabase downloads and attaches specific parts to the database
func (b *Backuper) downloadAndAttachPartsToDatabase(ctx context.Context, backupName, database, table string, partsToDownload []PartInfo, backupTable metadata.TableMetadata, backupMetadata metadata.BackupMetadata, currentTable clickhouse.Table) error {
	if len(partsToDownload) == 0 {
		return nil
	}

	// Log detailed part information for all operations (both local and remote)
	for _, part := range partsToDownload {
		log.Info().Fields(map[string]interface{}{
			"operation":   "starting_part_download",
			"database":    database,
			"table":       table,
			"part_name":   part.Name,
			"disk":        part.Disk,
			"part_status": "queued_for_download",
			"backup_name": backupName,
		}).Msgf("preparing to download and attach part %s to table %s.%s on disk %s", part.Name, database, table, part.Disk)
	}

	// Filter the backup table to only include parts we need to download
	filteredTable := backupTable
	filteredTable.Parts = make(map[string][]metadata.Part)

	// Create a map of parts to download for fast lookup
	partsToDownloadMap := make(map[string]bool)
	for _, part := range partsToDownload {
		partsToDownloadMap[part.Name] = true
	}

	// Filter backup parts to only include the ones we need
	for diskName, parts := range backupTable.Parts {
		filteredParts := make([]metadata.Part, 0)
		for _, part := range parts {
			if partsToDownloadMap[part.Name] {
				filteredParts = append(filteredParts, part)
			}
		}
		if len(filteredParts) > 0 {
			filteredTable.Parts[diskName] = filteredParts
		}
	}

	if len(filteredTable.Parts) == 0 {
		return nil // No parts to download
	}

	// Use existing restore logic to download and attach the filtered parts
	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		return err
	}

	diskMap := make(map[string]string, len(disks))
	diskTypes := make(map[string]string, len(disks))
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
		diskTypes[disk.Name] = disk.Type
	}

	logger := log.With().Str("table", fmt.Sprintf("%s.%s", database, table)).Logger()

	// Use the regular restore logic but only for the filtered parts
	err = b.restoreDataRegularByParts(ctx, backupName, backupMetadata, filteredTable, diskMap, diskTypes, disks, currentTable, nil, logger, false)

	if err != nil {
		// Log failure for each part that was supposed to be downloaded
		for _, part := range partsToDownload {
			log.Error().Fields(map[string]interface{}{
				"operation":   "part_download_failed",
				"database":    database,
				"table":       table,
				"part_name":   part.Name,
				"disk":        part.Disk,
				"part_status": "download_failed",
				"backup_name": backupName,
				"error":       err.Error(),
			}).Msgf("failed to download and attach part %s to table %s.%s on disk %s: %v", part.Name, database, table, part.Disk, err)
		}
		return err
	}

	// Log successful completion for each part
	for _, part := range partsToDownload {
		log.Info().Fields(map[string]interface{}{
			"operation":   "part_downloaded_successfully",
			"database":    database,
			"table":       table,
			"part_name":   part.Name,
			"disk":        part.Disk,
			"part_status": "downloaded_and_attached",
			"backup_name": backupName,
		}).Msgf("successfully downloaded and attached part %s to table %s.%s on disk %s", part.Name, database, table, part.Disk)
	}

	return nil
}

// Restore - restore tables matched by tablePattern from backupName
func (b *Backuper) Restore(backupName, tablePattern string, databaseMapping, tableMapping, partitions, skipProjections []string, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, schemaAsAttach, replicatedCopyToDetached bool, backupVersion string, commandId int) error {
	// Check if in-place restore is enabled and we're doing data-only restore
	if b.cfg.General.RestoreInPlace && dataOnly && !schemaOnly && !rbacOnly && !configsOnly && !namedCollectionsOnly && !dropExists {
		log.Info().Msg("using in-place restore mode")
		return b.RestoreInPlace(backupName, tablePattern, commandId)
	}

	if pidCheckErr := pidlock.CheckAndCreatePidFile(backupName, "restore"); pidCheckErr != nil {
		return pidCheckErr
	}
	defer pidlock.RemovePidFile(backupName)
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

	version, versionErr := b.ch.GetVersion(ctx)
	if versionErr != nil {
		return versionErr
	}
	if version < 24003000 && skipProjections != nil && len(skipProjections) > 0 {
		return fmt.Errorf("backup with skip-projections can restore only in 24.3+")
	}
	// https://github.com/Altinity/clickhouse-backup/issues/868
	if schemaAsAttach && b.cfg.General.RestoreSchemaOnCluster != "" {
		return fmt.Errorf("can't apply `--restore-schema-as-attach` and config `retore_schema_on_cluster` together")
	}

	if backupName == "" {
		localBackups := b.CollectLocalBackups(ctx, "all")
		_ = b.PrintBackup(localBackups, "all", "text")
		return fmt.Errorf("select backup for restore")
	}
	disks, err := b.ch.GetDisks(ctx, true)
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
		if !restoreRBAC && !rbacOnly && !restoreConfigs && !configsOnly && !restoreNamedCollections && !namedCollectionsOnly {
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
	if namedCollectionsOnly || restoreNamedCollections {
		if err := b.restoreNamedCollections(backupName); err != nil {
			return err
		}
		log.Info().Msgf("NAMED COLLECTIONS successfully restored")
	}

	if needRestart {
		if err := b.restartClickHouse(ctx, backupName); err != nil {
			return err
		}
	}
	if rbacOnly || configsOnly || namedCollectionsOnly {
		return nil
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

	tablesForRestore, partitionsNames, err = b.getTablesForRestoreLocal(ctx, backupName, metadataPath, tablePattern, dropExists, partitions)
	if err != nil {
		return err
	}
	if schemaOnly || dropExists || (schemaOnly == dataOnly) {
		if err = b.RestoreSchema(ctx, backupName, backupMetadata, disks, tablesForRestore, ignoreDependencies, version, schemaAsAttach); err != nil {
			return err
		}
	}
	// https://github.com/Altinity/clickhouse-backup/issues/756
	if dataOnly && !schemaOnly && len(partitions) > 0 {
		if err = b.dropExistPartitions(ctx, tablesForRestore, partitionsNames, partitions, version); err != nil {
			return err
		}

	}
	if dataOnly || (schemaOnly == dataOnly) {
		if err := b.RestoreData(ctx, backupName, backupMetadata, dataOnly, metadataPath, tablePattern, partitions, skipProjections, disks, version, replicatedCopyToDetached); err != nil {
			return err
		}
	}
	// do not create UDF when use --data, --rbac-only, --configs-only flags, https://github.com/Altinity/clickhouse-backup/issues/697
	if schemaOnly || (schemaOnly == dataOnly) {
		if funcErr := b.restoreFunctions(ctx, backupMetadata); funcErr != nil {
			return funcErr
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

func (b *Backuper) restoreFunctions(ctx context.Context, backupMetadata metadata.BackupMetadata) error {
	// https://github.com/Altinity/clickhouse-backup/issues/1123
	onCluster := b.cfg.General.RestoreSchemaOnCluster
	if onCluster != "" && len(backupMetadata.Functions) > 0 {
		configFile, doc, configErr := b.ch.ParseXML(ctx, "config.xml")
		if configErr != nil {
			return errors.Wrapf(configErr, "can't parse %s", configFile)
		}
		userDefinedKeeperPathNode := doc.SelectElement("//user_defined_zookeeper_path")
		if userDefinedKeeperPathNode != nil {
			userDefinedKeeperPath := strings.Trim(userDefinedKeeperPathNode.InnerText(), " \t\r\n")
			if userDefinedKeeperPath != "" {
				log.Warn().Msgf("%s contains <user_defined_zookeeper_path>%s</user_defined_zookeeper_path> ON CLUSTER '%s' will ignored during functions restore", configFile, userDefinedKeeperPath, onCluster)
				onCluster = ""
			}
		}
	}

	for _, function := range backupMetadata.Functions {
		if funcErr := b.ch.CreateUserDefinedFunction(function.Name, function.CreateQuery, onCluster); funcErr != nil {
			return funcErr
		}
	}
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
	log.Debug().Msg(string(out))
	if err != nil {
		log.Warn().Msgf("restart exec: %s, error: %v", cmd, err)
	}
	return nil
}

var CreateDatabaseRE = regexp.MustCompile(`(?m)^CREATE DATABASE (\s*)(\S+)(\s*)(.*)`)

func (b *Backuper) restoreEmptyDatabase(ctx context.Context, targetDB, tablePattern string, database metadata.DatabasesMeta, dropTable, schemaOnly, ignoreDependencies bool, version int) error {
	// https://github.com/Altinity/clickhouse-backup/issues/583
	// https://github.com/Altinity/clickhouse-backup/issues/663
	if ShallSkipDatabase(b.cfg, targetDB, tablePattern) {
		return nil
	}

	isMapped := false
	if targetDB, isMapped = b.cfg.General.RestoreDatabaseMapping[database.Name]; !isMapped {
		targetDB = database.Name
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
		sync := ""
		if version > 20011000 {
			sync = "SYNC"
		}
		if _, err := os.Create(path.Join(b.DefaultDataPath, "/flags/force_drop_table")); err != nil {
			return err
		}
		if err := b.ch.QueryContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s` %s %s %s", targetDB, onCluster, sync, settings)); err != nil {
			return err
		}

	}
	matches := CreateDatabaseRE.FindAllStringSubmatch(database.Query, -1)
	databaseEngine := ""
	if len(matches) == 1 && len(matches[0]) == 5 {
		databaseEngine = strings.Replace(matches[0][4], database.Name, targetDB, -1)
	}
	substitution := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS ${1}`%s`${3}%s", targetDB, databaseEngine)
	createSQL := CreateDatabaseRE.ReplaceAllString(database.Query, substitution)
	if err := b.ch.CreateDatabaseFromQuery(ctx, createSQL, b.cfg.General.RestoreSchemaOnCluster); err != nil {
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
					if resolveErr := b.resolveRBACConflictIfExist(ctx, string(data.Value), accessPath, version, k, replicatedUserDirectories, dropExists); resolveErr != nil {
						return resolveErr
					}
					log.Debug().Msgf("%s:%s b.resolveRBACConflictIfExist(%s) no error", fPath, data.Path, string(data.Value))
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
	if version > 22004000 {
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
		var existsRBACObjectIds []string
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
				if len(node.Value) == 0 {
					return false, nil
				}
				if checkRBACExists(string(node.Value)) {
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
	// rbacType contains name of keeper user directory
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
			if string(node.Value) == rbacObjectId {
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

// restoreNamedCollections - restore named collections from backup
func (b *Backuper) restoreNamedCollections(backupName string) error {
	ctx := context.Background()

	// Parse named_collections_storage configuration
	namedCollectionsSettings := map[string]string{
		"type":      "//named_collections_storage/type",
		"path":      "//named_collections_storage/path",
		"key_hex":   "//named_collections_storage/key_hex",
		"algorithm": "//named_collections_storage/algorithm",
	}
	settings, err := b.ch.GetPreprocessedXMLSettings(ctx, namedCollectionsSettings, "config.xml")
	if err != nil {
		return fmt.Errorf("failed to get named_collections_storage settings: %v", err)
	}

	storageType := "local"
	if typeSetting, exists := settings["type"]; exists {
		storageType = typeSetting
	}

	namedCollectionsBackup := path.Join(b.DefaultDataPath, "backup", backupName, "named_collections")
	info, err := os.Stat(namedCollectionsBackup)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to stat named_collections path: %v", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("named_collections path is not a directory: %s", namedCollectionsBackup)
	}

	settingsFile := path.Join(namedCollectionsBackup, "settings.json")
	backupSettingsJSON, openErr := os.ReadFile(settingsFile)
	if openErr != nil {
		return openErr
	}
	var backupSettings map[string]string
	if unmarshalErr := json.Unmarshal(backupSettingsJSON, &backupSettings); unmarshalErr != nil {
		return unmarshalErr
	}

	// Check compatibility - only 'local' and 'keeper' are supported
	if !strings.Contains(storageType, "local") && !strings.Contains(storageType, "keeper") {
		return fmt.Errorf("incompatible named_collections_storage type: %s, shall contains 'local' or 'keeper'", storageType)
	}

	// Restore based on storage type
	jsonlFiles, err := filepath.Glob(path.Join(namedCollectionsBackup, "*.jsonl"))
	if err != nil {
		return fmt.Errorf("failed to glob jsonl files: %v", err)
	}

	sqlFiles, err := filepath.Glob(path.Join(namedCollectionsBackup, "*.sql"))
	if err != nil {
		return fmt.Errorf("failed to glob sql files: %v", err)
	}

	// Check if storage is encrypted
	isEncrypted := false
	keyHex := ""
	if key, exists := settings["key_hex"]; exists && key != "" {
		isEncrypted = true
		keyHex = key
	}

	// Handle JSONL files
	for _, jsonlFile := range jsonlFiles {
		file, openErr := os.Open(jsonlFile)
		if openErr != nil {
			return openErr
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Bytes()
			var node keeper.DumpNode
			if unmarshalErr := json.Unmarshal(line, &node); unmarshalErr != nil {
				return fmt.Errorf("failed to unmarshal from %s: %v", jsonlFile, unmarshalErr)
			}
			var sqlQuery string
			if len(node.Value) == 0 {
				continue
			}
			if isEncrypted {
				decryptedNode, decryptErr := b.decryptNamedCollectionKeeperJSON(node, keyHex)
				if decryptErr != nil {
					return decryptErr
				}
				sqlQuery = string(decryptedNode.Value)
			} else {
				sqlQuery = string(node.Value)
			}
			sqlQuery = strings.TrimSpace(sqlQuery)
			if sqlQuery == "" {
				log.Warn().Msgf("Empty SQL content in line from: %s", jsonlFile)
				continue
			}

			re := regexp.MustCompile(`(?i)CREATE\s+NAMED\s+COLLECTION\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)`)
			matches := re.FindStringSubmatch(sqlQuery)
			if len(matches) < 2 {
				return fmt.Errorf("could not extract collection name from: %s, %s skipping", jsonlFile, sqlQuery)
			}
			collectionName := matches[1]

			// Drop existing collection first
			dropQuery := fmt.Sprintf("DROP NAMED COLLECTION IF EXISTS %s", collectionName)
			if b.cfg.General.RestoreSchemaOnCluster != "" {
				dropQuery += fmt.Sprintf(" ON CLUSTER '%s'", b.cfg.General.RestoreSchemaOnCluster)
			}
			if err := b.ch.QueryContext(ctx, dropQuery); err != nil {
				return fmt.Errorf("failed to drop named collection %s: %v", collectionName, err)
			}

			// Create new collection
			if b.cfg.General.RestoreSchemaOnCluster != "" {
				sqlQuery = strings.Replace(sqlQuery, " AS ", fmt.Sprintf(" ON CLUSTER '%s' AS ", b.cfg.General.RestoreSchemaOnCluster), 1)
			}
			if err := b.ch.QueryContext(ctx, sqlQuery); err != nil {
				return fmt.Errorf("failed to create named collection %s: %v", collectionName, err)
			}

			log.Info().Msgf("Restored SQL named collection from jsonl: %s", collectionName)
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scanner error on %s: %v", jsonlFile, err)
		}
		if err := file.Close(); err != nil {
			log.Warn().Msgf("can't close %s error: %v", jsonlFile, err)
		}
	}

	for _, sqlFile := range sqlFiles {
		// Handle SQL files - execute DROP/CREATE commands
		var sqlContent []byte
		var err error

		if isEncrypted {
			// For encrypted storage, decrypt the SQL file content
			sqlContent, err = b.decryptNamedCollectionFile(sqlFile, keyHex)
			if err != nil {
				return fmt.Errorf("failed to decrypt SQL file %s: %v", sqlFile, err)
			}
		} else {
			// For non-encrypted storage, read directly
			sqlContent, err = os.ReadFile(sqlFile)
			if err != nil {
				return fmt.Errorf("failed to read SQL file %s: %v", sqlFile, err)
			}
		}

		sqlQuery := strings.TrimSpace(string(sqlContent))
		if sqlQuery == "" {
			log.Warn().Msgf("Empty SQL content in: %s", sqlFile)
			continue
		}

		// Extract collection name from CREATE NAMED COLLECTION statement
		// Expected format: CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster_name] AS ...
		re := regexp.MustCompile(`(?i)CREATE\s+NAMED\s+COLLECTION\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)`)
		matches := re.FindStringSubmatch(sqlQuery)
		if len(matches) < 2 {
			return fmt.Errorf("could not extract collection name from: %s, %s", sqlFile, sqlQuery)
		}
		collectionName := matches[1]

		// Drop existing collection first
		dropQuery := fmt.Sprintf("DROP NAMED COLLECTION IF EXISTS %s", collectionName)
		if b.cfg.General.RestoreSchemaOnCluster != "" {
			dropQuery += fmt.Sprintf(" ON CLUSTER '%s'", b.cfg.General.RestoreSchemaOnCluster)
		}
		if err := b.ch.QueryContext(ctx, dropQuery); err != nil {
			return fmt.Errorf("failed to drop named collection %s: %v", collectionName, err)
		}

		// Create new collection
		if b.cfg.General.RestoreSchemaOnCluster != "" {
			sqlQuery = strings.Replace(sqlQuery, " AS ", fmt.Sprintf(" ON CLUSTER '%s' AS ", b.cfg.General.RestoreSchemaOnCluster), 1)
		}
		if err := b.ch.QueryContext(ctx, sqlQuery); err != nil {
			return fmt.Errorf("failed to create named collection %s: %v", collectionName, err)
		}

		log.Info().Msgf("Restored SQL named collection: %s", collectionName)
	}

	return nil
}

// decryptNamedCollectionFile decrypts an encrypted named collection SQL file
func (b *Backuper) decryptNamedCollectionFile(filePath, keyHex string) ([]byte, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read encrypted file: %v", err)
	}
	decryptedData, err := b.decryptNamedCollectionData(data, keyHex)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", filePath, err)
	}
	return decryptedData, nil
}

// decryptNamedCollectionKeeperJSON decrypts an encrypted named collection keeper value
func (b *Backuper) decryptNamedCollectionKeeperJSON(node keeper.DumpNode, keyHex string) (keeper.DumpNode, error) {
	if len(node.Value) == 0 || len(node.Value) < 3 || !strings.HasPrefix(string(node.Value), "ENC") {
		return node, fmt.Errorf("does not have ENC encrypted header")
	}
	decryptedValue, err := b.decryptNamedCollectionData(node.Value, keyHex)
	if err != nil {
		return node, fmt.Errorf("path %s: %v", node.Path, err)
	}
	node.Value = decryptedValue
	return node, nil
}

// decryptNamedCollectionData decrypts an encrypted named collection data
func (b *Backuper) decryptNamedCollectionData(data []byte, keyHex string) ([]byte, error) {
	// 2. Check header signature
	if len(data) < 3 || string(data[:3]) != "ENC" {
		return nil, fmt.Errorf("does not have ENC encrypted header")
	}
	// Data is encrypted; proceed to parse header.
	// Header format (version 2):
	// [0..2]: "ENC"
	// [3..4]: version (2 bytes, little-endian)
	// [5..6]: algorithm ID (UInt16, little-endian: 0=AES-128-CTR, 1=AES-192-CTR, 2=AES-256-CTR)
	// [7..22]: key fingerprint (16 bytes)
	// [23..38]: IV (16 bytes)
	// [39..63]: reserved padding (zeros)

	if len(data) < 64 {
		return nil, fmt.Errorf("encrypted data is too short, missing header")
	}
	// 3. Read version
	version := binary.LittleEndian.Uint16(data[3:5])
	// 4. Read algorithm and determine key length
	algID := binary.LittleEndian.Uint16(data[5:7])
	var keyLen int
	switch algID {
	case 0:
		keyLen = 16 // AES-128
	case 1:
		keyLen = 24 // AES-192
	case 2:
		keyLen = 32 // AES-256
	default:
		return nil, fmt.Errorf("unknown algorithm ID: %d, expected 0,1,2", algID)
	}

	// 5. Extract IV from header
	iv := data[23:39] // 16 bytes

	// 6. Get the key from config (hex string)
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid key hex: %v", err)
	}
	if len(key) != keyLen {
		return nil, fmt.Errorf("provided key does not match expected length for algorithm")
	}

	// 7. Verify key fingerprint according to header version
	headerFingerprint := data[7:23] // 16 bytes
	switch version {
	case 2:
		// v2: fingerprint = sipHash128Keyed(seed0, seed1, UNHEX(key_hex))
		// seeds are literal: "ChEncryp" and "tedDiskF" in little-endian UInt64
		const seed0 uint64 = 0x4368456E63727970 // 'ChEncryp'
		const seed1 uint64 = 0x7465644469736B46 // 'tedDiskF'
		lo, hi := sipHash128Keyed(seed0, seed1, key)
		var expected [16]byte
		binary.LittleEndian.PutUint64(expected[0:8], lo)
		binary.LittleEndian.PutUint64(expected[8:16], hi)
		if subtle.ConstantTimeCompare(expected[:], headerFingerprint) != 1 {
			return nil, fmt.Errorf("key fingerprint (v2) mismatch: expected=%X actual=%X", expected, headerFingerprint)
		}
	case 1:
		// v1: header stores { key_id (low 64), very_small_hash(key) (high 64, low nibble) }
		// very_small_hash(key) = sipHash64(UNHEX(key_hex)) & 0x0F
		low := binary.LittleEndian.Uint64(headerFingerprint[0:8])   // key_id (not used for verification)
		high := binary.LittleEndian.Uint64(headerFingerprint[8:16]) // small hash in low nibble
		_ = low
		computedSmall := sipHash64(0, 0, key) & 0x0F
		if byte(high&0x0F) != byte(computedSmall) {
			return nil, fmt.Errorf("key fingerprint (v1) mismatch: expected small_hash=%X actual_high64=%016X", computedSmall, high)
		}
	default:
		return nil, fmt.Errorf("unsupported header version: %d", version)
	}

	// 8. Decrypt the ciphertext
	ciphertext := data[64:] // everything after the 64-byte header
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}
	// Use AES in CTR mode with the extracted IV:
	stream := cipher.NewCTR(block, iv)
	plaintext := make([]byte, len(ciphertext))
	stream.XORKeyStream(plaintext, ciphertext)
	return plaintext, nil
}

// ---- SipHash implementations compatible with ClickHouse (2-4 rounds) ----

// sipHash128Keyed computes 128-bit SipHash (2-4) for the given data and 128-bit key (k0,k1).
// It returns (lo, hi) where ClickHouse serializes them as little-endian UInt64 (lo first, hi second).
func sipHash128Keyed(k0, k1 uint64, data []byte) (uint64, uint64) {
	// Initialization as in SipHash spec
	v0 := uint64(0x736f6d6570736575) ^ k0
	v1 := uint64(0x646f72616e646f6d) ^ k1
	v2 := uint64(0x6c7967656e657261) ^ k0
	v3 := uint64(0x7465646279746573) ^ k1

	rotl := func(x uint64, b uint) uint64 { return (x << b) | (x >> (64 - b)) }
	sipRound := func() {
		v0 += v1
		v1 = rotl(v1, 13)
		v1 ^= v0
		v0 = rotl(v0, 32)

		v2 += v3
		v3 = rotl(v3, 16)
		v3 ^= v2

		v0 += v3
		v3 = rotl(v3, 21)
		v3 ^= v0

		v2 += v1
		v1 = rotl(v1, 17)
		v1 ^= v2
		v2 = rotl(v2, 32)
	}

	n := len(data)
	i := 0
	// process 8-byte blocks (little-endian)
	for ; i+8 <= n; i += 8 {
		m := binary.LittleEndian.Uint64(data[i : i+8])
		v3 ^= m
		sipRound()
		sipRound()
		v0 ^= m
	}

	// final block with remaining bytes and length in the top byte (per SipHash)
	var tail [8]byte
	copy(tail[:], data[i:])
	tail[7] = byte(n)
	m := binary.LittleEndian.Uint64(tail[:])
	v3 ^= m
	sipRound()
	sipRound()
	v0 ^= m

	v2 ^= 0xff
	// finalization: 4 rounds
	sipRound()
	sipRound()
	sipRound()
	sipRound()

	lo := v0 ^ v1
	hi := v2 ^ v3
	return lo, hi
}

// sipHash64 computes 64-bit SipHash(2-4) with the given 128-bit key (k0,k1).
// ClickHouse's sipHash64() uses k0=k1=0 by default.
func sipHash64(k0, k1 uint64, data []byte) uint64 {
	v0 := uint64(0x736f6d6570736575) ^ k0
	v1 := uint64(0x646f72616e646f6d) ^ k1
	v2 := uint64(0x6c7967656e657261) ^ k0
	v3 := uint64(0x7465646279746573) ^ k1

	rotl := func(x uint64, b uint) uint64 { return (x << b) | (x >> (64 - b)) }
	sipRound := func() {
		v0 += v1
		v1 = rotl(v1, 13)
		v1 ^= v0
		v0 = rotl(v0, 32)

		v2 += v3
		v3 = rotl(v3, 16)
		v3 ^= v2

		v0 += v3
		v3 = rotl(v3, 21)
		v3 ^= v0

		v2 += v1
		v1 = rotl(v1, 17)
		v1 ^= v2
		v2 = rotl(v2, 32)
	}

	n := len(data)
	i := 0
	for ; i+8 <= n; i += 8 {
		m := binary.LittleEndian.Uint64(data[i : i+8])
		v3 ^= m
		sipRound()
		sipRound()
		v0 ^= m
	}

	var tail [8]byte
	copy(tail[:], data[i:])
	tail[7] = byte(n)
	m := binary.LittleEndian.Uint64(tail[:])
	v3 ^= m
	sipRound()
	sipRound()
	v0 ^= m

	v2 ^= 0xff
	sipRound()
	sipRound()
	sipRound()
	sipRound()
	return v0 ^ v1 ^ v2 ^ v3
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
func (b *Backuper) RestoreSchema(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, disks []clickhouse.Disk, tablesForRestore ListOfTables, ignoreDependencies bool, version int, schemaAsAttach bool) error {
	startRestoreSchema := time.Now()
	databaseEnginesForRestore := b.prepareDatabaseEnginesMap(backupMetadata.Databases)
	if dropErr := b.dropExistsTables(tablesForRestore, databaseEnginesForRestore, ignoreDependencies, version, schemaAsAttach); dropErr != nil {
		return dropErr
	}
	var restoreErr error
	if b.isEmbedded {
		restoreErr = b.restoreSchemaEmbedded(ctx, backupName, backupMetadata, disks, tablesForRestore, version)
	} else {
		restoreErr = b.restoreSchemaRegular(ctx, tablesForRestore, databaseEnginesForRestore, version, schemaAsAttach)
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

//var emptyReplicatedMergeTreeRE = regexp.MustCompile(`(?m)Replicated(MergeTree|ReplacingMergeTree|SummingMergeTree|AggregatingMergeTree|CollapsingMergeTree|VersionedCollapsingMergeTree|GraphiteMergeTree)\s*\(([^']*)\)(.*)`)

var emptyReplicatedMergeTreeRE = regexp.MustCompile(`(?m)Replicated(MergeTree|ReplacingMergeTree|SummingMergeTree|AggregatingMergeTree|CollapsingMergeTree|VersionedCollapsingMergeTree|GraphiteMergeTree)\s*(\(([^']*)\)|(\w+))(.*)`)

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
			err = b.dst.PutFileAbsolute(ctx, remoteFilePath, io.NopCloser(strings.NewReader(sqlQuery)), 0)
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
	filePathParts := strings.Split(filePath, "/")
	if strings.Contains(sqlQuery, "{uuid}") {
		if UUIDWithMergeTreeRE.Match(sqlBytes) && version < 23009000 {
			sqlQuery = UUIDWithMergeTreeRE.ReplaceAllString(sqlQuery, "$1$2$3'$4'$5$4$7")
		} else {
			log.Warn().Msgf("%s contains `{uuid}` macro, will replace to `{database}/{table}` see https://github.com/ClickHouse/ClickHouse/issues/42709 for details", filePath)
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
		database, err := url.QueryUnescape(filePathParts[len(filePathParts)-3])
		if err != nil {
			return "", false, err
		}
		table, err := url.QueryUnescape(filePathParts[len(filePathParts)-2])
		if err != nil {
			return "", false, err
		}
		if len(settings) != 2 {
			settings["default_replica_path"] = "/clickhouse/tables/{database}/{table}/{shard}"
			settings["default_replica_name"] = "{replica}"
			log.Warn().Msgf("can't get %#v from preprocessed_configs/config.xml, will use %#v", replicaXMLSettings, settings)
		}
		settings["default_replica_path"] = strings.Replace(settings["default_replica_path"], "{uuid}", "{database}/{table}", -1)
		log.Warn().Msgf("%s contains `ReplicatedMergeTree()` without parameters, will replace to '%s` and `%s` see https://github.com/ClickHouse/ClickHouse/issues/42709 for details", filePath, settings["default_replica_path"], settings["default_replica_name"])
		matches := emptyReplicatedMergeTreeRE.FindStringSubmatch(sqlQuery)
		substitution := fmt.Sprintf("Replicated$1('%s','%s')$4$5", settings["default_replica_path"], settings["default_replica_name"])
		if matches[3] != "" && matches[4] == "" {
			substitution = fmt.Sprintf("Replicated$1('%s','%s',$3) $5", settings["default_replica_path"], settings["default_replica_name"])
		}
		sqlQuery = emptyReplicatedMergeTreeRE.ReplaceAllString(sqlQuery, substitution)
		sqlQuery = strings.NewReplacer("{database}", database, "{table}", table).Replace(sqlQuery)
		sqlMetadataChanged = true
	}
	return sqlQuery, sqlMetadataChanged, nil
}

func (b *Backuper) restoreSchemaRegular(ctx context.Context, tablesForRestore ListOfTables, databaseEnginesForRestore map[string]string, version int, schemaAsAttach bool) error {
	totalRetries := len(tablesForRestore)
	restoreRetries := 0
	isDatabaseCreated := common.EmptyMap{}
	var restoreErr error
	for restoreRetries < totalRetries {
		var notRestoredTables ListOfTables
		for _, schema := range tablesForRestore {
			if _, isCreated := isDatabaseCreated[schema.Database]; !isCreated {
				var createDbErr error
				if version > 20008000 && databaseEnginesForRestore[schema.Database] != "" {
					createDbErr = b.ch.CreateDatabaseWithEngine(schema.Database, databaseEnginesForRestore[schema.Database], b.cfg.General.RestoreSchemaOnCluster, version)
				} else {
					createDbErr = b.ch.CreateDatabase(schema.Database, b.cfg.General.RestoreSchemaOnCluster)
				}
				if createDbErr != nil {
					return fmt.Errorf("can't create database '%s': %v", schema.Database, createDbErr)
				}
				isDatabaseCreated[schema.Database] = struct{}{}
			}
			//materialized and window views should restore via ATTACH
			b.replaceCreateToAttachForView(schema)
			// https://github.com/Altinity/clickhouse-backup/issues/849
			b.checkReplicaAlreadyExistsAndChangeReplicationPath(ctx, schema, version)

			// https://github.com/Altinity/clickhouse-backup/issues/466
			b.replaceUUIDMacroValue(schema)
			restoreErr = b.ch.CreateTable(clickhouse.Table{
				Database: schema.Database,
				Name:     schema.Table,
			}, schema.Query, false, false, b.cfg.General.RestoreSchemaOnCluster, version, b.DefaultDataPath, schemaAsAttach, databaseEnginesForRestore[schema.Database])

			if restoreErr != nil {
				restoreRetries++
				if restoreRetries >= totalRetries {
					return fmt.Errorf(
						"can't create table `%s`.`%s`: %v after %d times, please check your schema dependencies",
						schema.Database, schema.Table, restoreErr, restoreRetries,
					)
				} else {
					log.Warn().Msgf(
						"can't create table `%s`.`%s`: %v, will try again", schema.Database, schema.Table, restoreErr,
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

func (b *Backuper) checkReplicaAlreadyExistsAndChangeReplicationPath(ctx context.Context, schema *metadata.TableMetadata, version int) {
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
		if version < 19017000 {
			resolvedReplicaPath = strings.NewReplacer("{database}", schema.Database, "{table}", schema.Table).Replace(resolvedReplicaPath)
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
		if version < 19017000 {
			schema.Query = strings.NewReplacer("{database}", schema.Database, "{table}", schema.Table).Replace(schema.Query)
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

func (b *Backuper) dropExistsTables(tablesForDrop ListOfTables, databaseEngines map[string]string, ignoreDependencies bool, version int, schemaAsAttach bool) error {
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
					dropErr = b.ch.DropOrDetachTable(clickhouse.Table{
						Database: schema.Database,
						Name:     schema.Table,
					}, query, b.cfg.General.RestoreSchemaOnCluster, ignoreDependencies, version, b.DefaultDataPath, schemaAsAttach, databaseEngines[schema.Database])
					if dropErr == nil {
						tablesForDrop[i].Query = query
						break
					}
				}
			} else {
				dropErr = b.ch.DropOrDetachTable(clickhouse.Table{
					Database: schema.Database,
					Name:     schema.Table,
				}, schema.Query, b.cfg.General.RestoreSchemaOnCluster, ignoreDependencies, version, b.DefaultDataPath, schemaAsAttach, databaseEngines[schema.Database])
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
func (b *Backuper) RestoreData(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, dataOnly bool, metadataPath, tablePattern string, partitions, skipProjections []string, disks []clickhouse.Disk, version int, replicatedCopyToDetached bool) error {
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
		err = b.restoreDataRegular(ctx, backupName, backupMetadata, tablePattern, tablesForRestore, diskMap, diskTypes, disks, skipProjections, replicatedCopyToDetached)
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

func (b *Backuper) restoreDataRegular(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, tablePattern string, tablesForRestore ListOfTables, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, skipProjections []string, replicatedCopyToDetached bool) error {
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

	b.filterPartsAndFilesByDisk(tablesForRestore, disks)

	restoreBackupWorkingGroup, restoreCtx := errgroup.WithContext(ctx)
	// Use unified download concurrency for restore operations
	optimalConcurrency := b.cfg.GetOptimalDownloadConcurrency()
	restoreBackupWorkingGroup.SetLimit(max(optimalConcurrency, 1))

	for i := range tablesForRestore {
		tableRestoreStartTime := time.Now()
		table := *tablesForRestore[i]
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
			return fmt.Errorf("can't find '%s.%s' in current system.tables", dstDatabase, dstTableName)
		}
		idx := i
		restoreBackupWorkingGroup.Go(func() error {
			// https://github.com/Altinity/clickhouse-backup/issues/529
			if b.cfg.ClickHouse.RestoreAsAttach {
				if restoreErr := b.restoreDataRegularByAttach(restoreCtx, backupName, backupMetadata, table, diskMap, diskTypes, disks, dstTable, skipProjections, logger, replicatedCopyToDetached); restoreErr != nil {
					return restoreErr
				}
			} else {
				if restoreErr := b.restoreDataRegularByParts(restoreCtx, backupName, backupMetadata, table, diskMap, diskTypes, disks, dstTable, skipProjections, logger, replicatedCopyToDetached); restoreErr != nil {
					return restoreErr
				}
			}
			// https://github.com/Altinity/clickhouse-backup/issues/529
			for _, mutation := range table.Mutations {
				if err := b.ch.ApplyMutation(restoreCtx, *tablesForRestore[idx], mutation); err != nil {
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

func (b *Backuper) restoreDataRegularByAttach(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, table metadata.TableMetadata, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, dstTable clickhouse.Table, skipProjections []string, logger zerolog.Logger, replicatedCopyToDetached bool) error {
	if err := filesystemhelper.HardlinkBackupPartsToStorage(backupName, table, disks, diskMap, dstTable.DataPaths, skipProjections, b.ch, false); err != nil {
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
		logger.Info().Str("duration", utils.HumanizeDuration(time.Since(start))).Str("size", utils.FormatBytes(uint64(size))).Str("database", table.Database).Str("table", table.Table).Msg("download object_disks finish")
	}
	// Skip ATTACH TABLE for Replicated*MergeTree tables if replicatedCopyToDetached is true
	if !replicatedCopyToDetached || !strings.Contains(dstTable.Engine, "Replicated") {
		if err := b.ch.AttachTable(ctx, table, dstTable); err != nil {
			return fmt.Errorf("can't attach table '%s.%s': %v", table.Database, table.Table, err)
		}
	} else {
		logger.Info().Msg("skipping ATTACH TABLE for Replicated*MergeTree table due to --replicated-copy-to-detached flag")
	}
	return nil
}

func (b *Backuper) restoreDataRegularByParts(ctx context.Context, backupName string, backupMetadata metadata.BackupMetadata, table metadata.TableMetadata, diskMap, diskTypes map[string]string, disks []clickhouse.Disk, dstTable clickhouse.Table, skipProjections []string, logger zerolog.Logger, replicatedCopyToDetached bool) error {
	if err := filesystemhelper.HardlinkBackupPartsToStorage(backupName, table, disks, diskMap, dstTable.DataPaths, skipProjections, b.ch, true); err != nil {
		return fmt.Errorf("can't copy data to detached `%s`.`%s`: %v", table.Database, table.Table, err)
	}
	logger.Debug().Msg("data to 'detached' copied")
	logger.Info().Msg("download object_disks start")
	var size int64
	var err error
	start := time.Now()
	if size, err = b.downloadObjectDiskParts(ctx, backupName, backupMetadata, table, diskMap, diskTypes, disks); err != nil {
		return fmt.Errorf("can't restore object_disk server-side copy data parts '%s.%s': %v", table.Database, table.Table, err)
	}
	log.Info().Str("duration", utils.HumanizeDuration(time.Since(start))).Str("size", utils.FormatBytes(uint64(size))).Str("database", table.Database).Str("table", table.Table).Msg("download object_disks finish")
	// Skip ATTACH PART for Replicated*MergeTree tables if replicatedCopyToDetached is true
	if !replicatedCopyToDetached || !strings.Contains(dstTable.Engine, "Replicated") {
		if err := b.ch.AttachDataParts(table, dstTable); err != nil {
			return fmt.Errorf("can't attach data parts for table '%s.%s': %v", table.Database, table.Table, err)
		}
	} else {
		logger.Info().Msg("skipping ATTACH PART for Replicated*MergeTree table due to --replicated-copy-to-detached flag")
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

	// Initialize performance monitor for object disk downloads
	initialConcurrency := b.cfg.GetOptimalObjectDiskConcurrency()
	maxConcurrency := int(b.cfg.GetOptimalDownloadConcurrency())
	if maxConcurrency < initialConcurrency {
		maxConcurrency = initialConcurrency * 2
	}
	performanceMonitor := NewPerformanceMonitor(initialConcurrency, maxConcurrency)

	// Add performance callback to log significant changes
	performanceMonitor.AddCallback(func(monitor *PerformanceMonitor, event PerformanceEvent) {
		metrics := monitor.GetMetrics()
		switch event {
		case EventPerformanceDegradation:
			log.Warn().
				Float64("current_speed_mbps", metrics.CurrentSpeed/(1024*1024)).
				Float64("average_speed_mbps", metrics.AverageSpeed/(1024*1024)).
				Float64("peak_speed_mbps", metrics.PeakSpeed/(1024*1024)).
				Int32("concurrency", metrics.CurrentConcurrency).
				Str("table", fmt.Sprintf("%s.%s", backupTable.Database, backupTable.Table)).
				Msg("performance degradation detected")
		case EventConcurrencyAdjusted:
			log.Info().
				Int32("concurrency", metrics.CurrentConcurrency).
				Float64("speed_mbps", metrics.CurrentSpeed/(1024*1024)).
				Str("table", fmt.Sprintf("%s.%s", backupTable.Database, backupTable.Table)).
				Msg("concurrency adjusted for better performance")
		}
	})

	// Start performance monitoring
	monitorCtx, monitorCancel := context.WithCancel(ctx)
	defer monitorCancel()
	go performanceMonitor.StartMonitoring(monitorCtx)

	var err error
	for diskName, parts := range backupTable.Parts {
		if b.shouldDiskNameSkipByNameOrType(diskName, disks) {
			log.Warn().Str("database", backupTable.Database).Str("table", backupTable.Table).Str("disk.Name", diskName).Msg("skipped")
			continue
		}
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
			// Start with optimal concurrency and make it adaptive
			objectDiskConcurrency := performanceMonitor.GetCurrentConcurrency()
			downloadObjectDiskPartsWorkingGroup.SetLimit(int(objectDiskConcurrency))
			var isCopyFailed atomic.Bool
			isCopyFailed.Store(false)

			// Adaptive concurrency adjustment during download
			adjustmentTicker := time.NewTicker(30 * time.Second)
			adjustmentDone := make(chan struct{})
			go func() {
				defer close(adjustmentDone)
				defer adjustmentTicker.Stop()
				for {
					select {
					case <-downloadCtx.Done():
						return
					case <-adjustmentDone:
						return
					case <-adjustmentTicker.C:
						newConcurrency := performanceMonitor.AdjustConcurrency()
						if newConcurrency != objectDiskConcurrency {
							objectDiskConcurrency = newConcurrency
							// Note: errgroup doesn't support runtime limit changes,
							// but this will affect future batches
							log.Debug().
								Int32("new_concurrency", newConcurrency).
								Str("table", fmt.Sprintf("%s.%s", backupTable.Database, backupTable.Table)).
								Msg("adjusted download concurrency")
						}
					}
				}
			}()
			defer func() {
				adjustmentDone <- struct{}{}
			}()
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
					if strings.Contains(fInfo.Name(), "frozen_metadata.txt") {
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
								retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
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
									retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure, common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
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
							// Track performance for adaptive concurrency
							if copiedSize > 0 {
								performanceMonitor.AddBytes(copiedSize)
								performanceMonitor.UpdateSpeed()
							}
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
			// Log final performance metrics
			finalMetrics := performanceMonitor.GetMetrics()
			logger.Info().
				Str("disk", diskName).
				Str("duration", utils.HumanizeDuration(time.Since(start))).
				Str("size", utils.FormatBytes(uint64(size))).
				Float64("avg_speed_mbps", finalMetrics.AverageSpeed/(1024*1024)).
				Float64("peak_speed_mbps", finalMetrics.PeakSpeed/(1024*1024)).
				Int32("final_concurrency", finalMetrics.CurrentConcurrency).
				Bool("degradation_detected", finalMetrics.DegradationDetected).
				Msg("object_disk data downloaded with performance metrics")
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
	if err != nil {
		return "", "", err
	}
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
