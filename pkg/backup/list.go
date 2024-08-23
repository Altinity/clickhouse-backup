package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ricochet2200/go-disk-usage/du"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/custom"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

	"github.com/rs/zerolog/log"
)

// List - list backups to stdout from command line
func (b *Backuper) List(what, format string) error {
	ctx, cancel, _ := status.Current.GetContextWithCancel(status.NotFromAPI)
	defer cancel()
	switch what {
	case "local":
		return b.PrintLocalBackups(ctx, format)
	case "remote":
		return b.PrintRemoteBackups(ctx, format)
	case "all", "":
		return b.PrintAllBackups(ctx, format)
	}
	return nil
}
func printBackupsRemote(w io.Writer, backupList []storage.Backup, format string) error {
	switch format {
	case "latest", "last", "l":
		if len(backupList) < 1 {
			return fmt.Errorf("no backups found")
		}
		fmt.Println(backupList[len(backupList)-1].BackupName)
	case "penult", "prev", "previous", "p":
		if len(backupList) < 2 {
			return fmt.Errorf("no previous backup is found")
		}
		fmt.Println(backupList[len(backupList)-2].BackupName)
	case "all", "":
		// if len(backupList) == 0 {
		// 	fmt.Println("no backups found")
		// }
		for _, backup := range backupList {
			size := utils.FormatBytes(backup.GetFullSize())
			description := backup.DataFormat
			uploadDate := backup.UploadDate.Format("02/01/2006 15:04:05")
			if backup.Tags != "" {
				description += ", " + backup.Tags
			}
			required := ""
			if backup.RequiredBackup != "" {
				required = "+" + backup.RequiredBackup
			}
			if backup.Broken != "" {
				description = backup.Broken
				size = "???"
			}
			if bytes, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", backup.BackupName, size, uploadDate, "remote", required, description); err != nil {
				log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
			}
		}
	default:
		return fmt.Errorf("'%s' undefined", format)
	}
	return nil
}

func printBackupsLocal(ctx context.Context, w io.Writer, backupList []LocalBackup, format string) error {
	switch format {
	case "latest", "last", "l":
		if len(backupList) < 1 {
			return fmt.Errorf("no backups found")
		}
		fmt.Println(backupList[len(backupList)-1].BackupName)
	case "penult", "prev", "previous", "p":
		if len(backupList) < 2 {
			return fmt.Errorf("no previous backup is found")
		}
		fmt.Println(backupList[len(backupList)-2].BackupName)
	case "all", "":
		for _, backup := range backupList {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				size := utils.FormatBytes(backup.GetFullSize())
				description := backup.DataFormat
				if backup.Tags != "" {
					if description != "" {
						description += ", "
					}
					description += backup.Tags
				}
				creationDate := backup.CreationDate.Format("02/01/2006 15:04:05")
				required := ""
				if backup.RequiredBackup != "" {
					required = "+" + backup.RequiredBackup
				}
				if backup.Broken != "" {
					description = backup.Broken
					size = "???"
				}
				if bytes, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", backup.BackupName, size, creationDate, "local", required, description); err != nil {
					log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
				}
			}
		}
	default:
		return fmt.Errorf("'%s' undefined", format)
	}
	return nil
}

// PrintLocalBackups - print all backups stored locally
func (b *Backuper) PrintLocalBackups(ctx context.Context, format string) error {
	if !b.ch.IsOpen {
		if err := b.ch.Connect(); err != nil {
			return fmt.Errorf("can't connect to clickhouse: %v", err)
		}
		defer b.ch.Close()
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.DiscardEmptyColumns)
	defer func() {
		if err := w.Flush(); err != nil {
			log.Error().Msgf("can't flush tabular writer error: %v", err)
		}
	}()
	backupList, _, err := b.GetLocalBackups(ctx, nil)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return printBackupsLocal(ctx, w, backupList, format)
}

// GetLocalBackups - return slice of all backups stored locally
func (b *Backuper) GetLocalBackups(ctx context.Context, disks []clickhouse.Disk) ([]LocalBackup, []clickhouse.Disk, error) {
	var err error
	if !b.ch.IsOpen {
		if err = b.ch.Connect(); err != nil {
			return nil, nil, err
		}
		defer b.ch.Close()
	}
	if disks == nil {
		disks, err = b.ch.GetDisks(ctx, true)
		if err != nil {
			return nil, nil, err
		}
	}
	if disks == nil {
		disks = []clickhouse.Disk{
			{
				Name:            "default",
				Path:            "/var/lib/clickhouse",
				Type:            "local",
				FreeSpace:       du.NewDiskUsage("/var/lib/clickhouse").Free(),
				StoragePolicies: []string{"default"},
			},
		}
	}
	var result []LocalBackup
	allBackupPaths := []string{}
	for _, disk := range disks {
		if disk.IsBackup || disk.Name == b.cfg.ClickHouse.EmbeddedBackupDisk {
			allBackupPaths = append(allBackupPaths, disk.Path)
		} else {
			allBackupPaths = append(allBackupPaths, path.Join(disk.Path, "backup"))
		}
	}
	addBrokenBackupIfNotExists := func(result []LocalBackup, name string, info os.FileInfo, broken string) []LocalBackup {
		backupAlreadyExists := false
		for _, backup := range result {
			if backup.BackupName == name {
				backupAlreadyExists = true
				break
			}
		}
		// add broken backup if not exists
		if !backupAlreadyExists {
			result = append(result, LocalBackup{
				BackupMetadata: metadata.BackupMetadata{
					BackupName:   name,
					CreationDate: info.ModTime(),
				},
				Broken: broken,
			})
		}
		return result
	}
	l := len(allBackupPaths)
	for i, backupPath := range allBackupPaths {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
			d, openErr := os.Open(backupPath)
			if openErr != nil {
				if i < l-1 {
					continue
				}
				if os.IsNotExist(openErr) {
					return result, disks, nil
				}
				return nil, nil, openErr
			}
			names, err := d.Readdirnames(-1)
			if err != nil {
				return nil, nil, err
			}
			for _, name := range names {
				info, err := os.Stat(path.Join(backupPath, name))
				if err != nil {
					continue
				}
				if !info.IsDir() {
					continue
				}
				backupMetafilePath := path.Join(backupPath, name, "metadata.json")
				backupMetadataBody, err := os.ReadFile(backupMetafilePath)
				if err != nil {
					if !os.IsNotExist(err) {
						log.Warn().Msgf("list can't read %s error: %s", backupMetafilePath, err)
					}
					result = addBrokenBackupIfNotExists(result, name, info, "broken metadata.json not found")
					continue
				}
				var backupMetadata metadata.BackupMetadata
				if parseErr := json.Unmarshal(backupMetadataBody, &backupMetadata); parseErr != nil {
					result = addBrokenBackupIfNotExists(result, name, info, fmt.Sprintf("parse metadata.json error: %v", parseErr))
					continue
				}
				brokenBackupIsAlreadyExists := false
				for i, backup := range result {
					if backup.BackupName == backupMetadata.BackupName {
						brokenBackupIsAlreadyExists = true
						result[i].BackupMetadata = backupMetadata
						result[i].Broken = ""
						break
					}
				}
				if !brokenBackupIsAlreadyExists {
					result = append(result, LocalBackup{
						BackupMetadata: backupMetadata,
					})
				}

			}
			if closeErr := d.Close(); closeErr != nil {
				log.Error().Msgf("can't close %s error: %v", backupPath, closeErr)
			}
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].CreationDate.Before(result[j].CreationDate)
	})
	return result, disks, nil
}

func (b *Backuper) PrintAllBackups(ctx context.Context, format string) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.DiscardEmptyColumns)
	if !b.ch.IsOpen {
		if err := b.ch.Connect(); err != nil {
			return fmt.Errorf("can't connect to clickhouse: %v", err)
		}
		defer b.ch.Close()
	}
	defer func() {
		if err := w.Flush(); err != nil {
			log.Error().Msgf("can't flush tabular writer error: %v", err)
		}
	}()
	localBackups, _, err := b.GetLocalBackups(ctx, nil)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err = printBackupsLocal(ctx, w, localBackups, format); err != nil {
		log.Warn().Msgf("printBackupsLocal return error: %v", err)
	}

	if b.cfg.General.RemoteStorage != "none" {
		remoteBackups, err := b.GetRemoteBackups(ctx, true)
		if err != nil {
			return err
		}
		if err = printBackupsRemote(w, remoteBackups, format); err != nil {
			log.Warn().Msgf("printBackupsRemote return error: %v", err)
		}
	}
	return nil
}

// PrintRemoteBackups - print all backups stored on remote storage
func (b *Backuper) PrintRemoteBackups(ctx context.Context, format string) error {
	if !b.ch.IsOpen {
		if err := b.ch.Connect(); err != nil {
			return fmt.Errorf("can't connect to clickhouse: %v", err)
		}
		defer b.ch.Close()
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.DiscardEmptyColumns)
	defer func() {
		if err := w.Flush(); err != nil {
			log.Error().Msgf("can't flush tabular writer error: %v", err)
		}
	}()
	backupList, err := b.GetRemoteBackups(ctx, true)
	if err != nil {
		return err
	}
	return printBackupsRemote(w, backupList, format)
}

func (b *Backuper) getLocalBackup(ctx context.Context, backupName string, disks []clickhouse.Disk) (*LocalBackup, []clickhouse.Disk, error) {
	if backupName == "" {
		return nil, disks, fmt.Errorf("backup name is required")
	}
	backupList, disks, err := b.GetLocalBackups(ctx, disks)
	if err != nil {
		return nil, disks, err
	}
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			return &backup, disks, nil
		}
	}
	return nil, disks, fmt.Errorf("backup '%s' is not found", backupName)
}

// GetRemoteBackups - get all backups stored on remote storage
func (b *Backuper) GetRemoteBackups(ctx context.Context, parseMetadata bool) ([]storage.Backup, error) {
	if !b.ch.IsOpen {
		if err := b.ch.Connect(); err != nil {
			return nil, err
		}
		defer b.ch.Close()
	}

	if b.cfg.General.RemoteStorage == "none" {
		return nil, fmt.Errorf("remote_storage is 'none'")
	}
	if b.cfg.General.RemoteStorage == "custom" {
		return custom.List(ctx, b.cfg)
	}
	bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, false, "")
	if err != nil {
		return []storage.Backup{}, err
	}
	if err := bd.Connect(ctx); err != nil {
		return []storage.Backup{}, err
	}
	defer func() {
		if err := bd.Close(ctx); err != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", err)
		}
	}()
	backupList, err := bd.BackupList(ctx, parseMetadata, "")
	if err != nil {
		return []storage.Backup{}, err
	}
	// ugly hack to fix https://github.com/Altinity/clickhouse-backup/issues/309
	if parseMetadata == false && len(backupList) > 0 {
		lastBackup := backupList[len(backupList)-1]
		backupList, err = bd.BackupList(ctx, true, lastBackup.BackupName)
		if err != nil {
			return []storage.Backup{}, err
		}
	}
	return backupList, err
}

// GetTables - get all tables for use by CreateBackup, PrintTables, and API
func (b *Backuper) GetTables(ctx context.Context, tablePattern string) ([]clickhouse.Table, error) {
	if !b.ch.IsOpen {
		if err := b.ch.Connect(); err != nil {
			return []clickhouse.Table{}, fmt.Errorf("can't connect to clickhouse: %v", err)
		}
		defer b.ch.Close()
	}

	allTables, err := b.ch.GetTables(ctx, tablePattern)
	if err != nil {
		return []clickhouse.Table{}, fmt.Errorf("can't get tables: %v", err)
	}
	if err := b.populateBackupShardField(ctx, allTables); err != nil {
		return nil, err
	}
	return allTables, nil
}

// PrintTables - print all tables suitable for backup
func (b *Backuper) PrintTables(printAll bool, tablePattern, remoteBackup string) error {
	var err error
	ctx, cancel, _ := status.Current.GetContextWithCancel(status.NotFromAPI)
	defer cancel()
	if err = b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.DiscardEmptyColumns)
	if remoteBackup == "" {
		if err = b.printTablesLocal(ctx, tablePattern, printAll, w); err != nil {
			return err
		}
	} else {
		if err = b.printTablesRemote(ctx, remoteBackup, tablePattern, printAll, w); err != nil {
			return err
		}

	}
	if err := w.Flush(); err != nil {
		log.Error().Msgf("can't flush tabular writer error: %v", err)
	}
	return nil
}

func (b *Backuper) printTablesLocal(ctx context.Context, tablePattern string, printAll bool, w *tabwriter.Writer) error {
	logger := log.With().Str("logger", "PrintTablesLocal").Logger()
	allTables, err := b.GetTables(ctx, tablePattern)
	if err != nil {
		return err
	}
	disks, err := b.ch.GetDisks(ctx, false)
	if err != nil {
		return err
	}
	for _, table := range allTables {
		if table.Skip && !printAll {
			continue
		}
		var tableDisks []string
		for disk := range clickhouse.GetDisksByPaths(disks, table.DataPaths) {
			tableDisks = append(tableDisks, disk)
		}
		if table.Skip {
			if bytes, err := fmt.Fprintf(w, "%s.%s\t%s\t%v\tskip\n", table.Database, table.Name, utils.FormatBytes(table.TotalBytes), strings.Join(tableDisks, ",")); err != nil {
				logger.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
			}
			continue
		}
		if bytes, err := fmt.Fprintf(w, "%s.%s\t%s\t%v\t%v\n", table.Database, table.Name, utils.FormatBytes(table.TotalBytes), strings.Join(tableDisks, ","), table.BackupType); err != nil {
			logger.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
		}
	}
	return nil
}

func (b *Backuper) GetTablesRemote(ctx context.Context, backupName string, tablePattern string) ([]clickhouse.Table, error) {
	if !b.ch.IsOpen {
		if err := b.ch.Connect(); err != nil {
			return []clickhouse.Table{}, fmt.Errorf("can't connect to clickhouse: %v", err)
		}
		defer b.ch.Close()
	}
	if b.cfg.General.RemoteStorage == "none" || b.cfg.General.RemoteStorage == "custom" {
		return nil, fmt.Errorf("GetTablesRemote does not support `none` and `custom` remote storage")
	}
	if b.dst == nil {
		bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, false, "")
		if err != nil {
			return nil, err
		}
		err = bd.Connect(ctx)
		if err != nil {
			return nil, fmt.Errorf("can't connect to remote storage: %v", err)
		}
		defer func() {
			if err := bd.Close(ctx); err != nil {
				log.Warn().Msgf("can't close BackupDestination error: %v", err)
			}
		}()

		b.dst = bd
	}
	backupList, err := b.dst.BackupList(ctx, true, backupName)
	if err != nil {
		return nil, err
	}

	var tables []clickhouse.Table
	tablePatterns := []string{"*"}

	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}

	for _, remoteBackup := range backupList {
		if remoteBackup.BackupName == backupName {
			for _, t := range remoteBackup.Tables {
				isInformationSchema := IsInformationSchema(t.Database)
				tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
				shallSkipped := b.shouldSkipByTableName(tableName)
				matched := false
				for _, p := range tablePatterns {
					if matched, _ = filepath.Match(strings.Trim(p, " \t\r\n"), tableName); matched {
						break
					}
				}
				tables = append(tables, clickhouse.Table{
					Database: t.Database,
					Name:     t.Table,
					Skip:     !matched || (isInformationSchema || shallSkipped),
				})
			}
		}
	}

	return tables, nil
}

// printTablesRemote https://github.com/Altinity/clickhouse-backup/issues/778
func (b *Backuper) printTablesRemote(ctx context.Context, backupName string, tablePattern string, printAll bool, w *tabwriter.Writer) error {
	tables, err := b.GetTablesRemote(ctx, backupName, tablePattern)
	if err != nil {
		return err
	}

	for _, t := range tables {
		if t.Skip && !printAll {
			continue
		}
		if bytes, err := fmt.Fprintf(w, "%s.%s\tskip=%v\n", t.Database, t.Name, t.Skip); err != nil {
			log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
		}
	}

	return nil
}
