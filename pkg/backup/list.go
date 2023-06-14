package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/custom"
	"github.com/Altinity/clickhouse-backup/pkg/status"
	apexLog "github.com/apex/log"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/pkg/storage"
	"github.com/Altinity/clickhouse-backup/pkg/utils"
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
	log := apexLog.WithField("logger", "printBackupsRemote")
	switch format {
	case "latest", "last", "l":
		if len(backupList) < 1 {
			return fmt.Errorf("no backups found")
		}
		fmt.Println(backupList[len(backupList)-1].BackupName)
	case "penult", "prev", "previous", "p":
		if len(backupList) < 2 {
			return fmt.Errorf("no penult backup is found")
		}
		fmt.Println(backupList[len(backupList)-2].BackupName)
	case "all", "":
		// if len(backupList) == 0 {
		// 	fmt.Println("no backups found")
		// }
		for _, backup := range backupList {
			size := utils.FormatBytes(backup.DataSize + backup.MetadataSize)
			if backup.CompressedSize > 0 {
				size = utils.FormatBytes(backup.CompressedSize + backup.MetadataSize)
			}
			description := backup.DataFormat
			uploadDate := backup.UploadDate.Format("02/01/2006 15:04:05")
			if backup.Legacy {
				description += ", old-format"
			}
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
				log.Errorf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
			}
		}
	default:
		return fmt.Errorf("'%s' undefined", format)
	}
	return nil
}

func printBackupsLocal(ctx context.Context, w io.Writer, backupList []LocalBackup, format string) error {
	log := apexLog.WithField("logger", "printBackupsLocal")
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
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				size := utils.FormatBytes(backup.DataSize + backup.MetadataSize)
				if backup.CompressedSize > 0 {
					size = utils.FormatBytes(backup.CompressedSize + backup.MetadataSize)
				}
				description := backup.DataFormat
				creationDate := backup.CreationDate.Format("02/01/2006 15:04:05")
				if backup.Legacy {
					size = "???"
				}
				required := ""
				if backup.RequiredBackup != "" {
					required = "+" + backup.RequiredBackup
				}
				if backup.Broken != "" {
					description = backup.Broken
					size = "???"
				}
				if bytes, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", backup.BackupName, size, creationDate, "local", required, description); err != nil {
					log.Errorf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
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
	log := apexLog.WithField("logger", "PrintLocalBackups")
	if !b.ch.IsOpen {
		if err := b.ch.Connect(); err != nil {
			return fmt.Errorf("can't connect to clickhouse: %v", err)
		}
		defer b.ch.Close()
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.DiscardEmptyColumns)
	defer func() {
		if err := w.Flush(); err != nil {
			log.Errorf("can't flush tabular writer error: %v", err)
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
	log := b.log.WithField("logger", "GetLocalBackups")
	if disks == nil {
		disks, err = b.ch.GetDisks(ctx)
		if err != nil {
			return nil, nil, err
		}
	}
	if disks == nil {
		disks = []clickhouse.Disk{
			{Name: "default", Path: "/var/lib/clickhouse"},
		}
	}
	defaultDataPath, err := b.ch.GetDefaultPath(disks)
	if err != nil {
		return nil, nil, err
	}
	var result []LocalBackup
	allBackupPaths := []string{path.Join(defaultDataPath, "backup")}
	if b.cfg.ClickHouse.UseEmbeddedBackupRestore {
		for _, disk := range disks {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			default:
				if disk.IsBackup || disk.Name == b.cfg.ClickHouse.EmbeddedBackupDisk {
					allBackupPaths = append(allBackupPaths, disk.Path)
				}
			}
		}
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
				if os.IsNotExist(err) {
					// Legacy backup
					result = append(result, LocalBackup{
						BackupMetadata: metadata.BackupMetadata{
							BackupName:   name,
							CreationDate: info.ModTime(),
						},
						Legacy: true,
					})
					continue
				}
				var backupMetadata metadata.BackupMetadata
				if err := json.Unmarshal(backupMetadataBody, &backupMetadata); err != nil {
					return nil, disks, err
				}
				result = append(result, LocalBackup{
					BackupMetadata: backupMetadata,
					Legacy:         false,
				})
			}
			if closeErr := d.Close(); closeErr != nil {
				log.Errorf("can't close %s openError: %v", backupPath, closeErr)
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
	log := b.log.WithField("logger", "PrintAllBackups")
	defer func() {
		if err := w.Flush(); err != nil {
			log.Errorf("can't flush tabular writer error: %v", err)
		}
	}()
	localBackups, _, err := b.GetLocalBackups(ctx, nil)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err = printBackupsLocal(ctx, w, localBackups, format); err != nil {
		log.Warnf("printBackupsLocal return error: %v", err)
	}

	if b.cfg.General.RemoteStorage != "none" {
		remoteBackups, err := b.GetRemoteBackups(ctx, true)
		if err != nil {
			return err
		}
		if err = printBackupsRemote(w, remoteBackups, format); err != nil {
			log.Warnf("printBackupsRemote return error: %v", err)
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
	log := b.log.WithField("logger", "PrintRemoteBackups")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.DiscardEmptyColumns)
	defer func() {
		if err := w.Flush(); err != nil {
			log.Errorf("can't flush tabular writer error: %v", err)
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
			b.log.Warnf("can't close BackupDestination error: %v", err)
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

// GetTables - get all tables for use by PrintTables and API
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
	return allTables, nil
}

// PrintTables - print all tables suitable for backup
func (b *Backuper) PrintTables(printAll bool, tablePattern string) error {
	ctx, cancel, _ := status.Current.GetContextWithCancel(status.NotFromAPI)
	defer cancel()
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()
	log := b.log.WithField("logger", "PrintTables")
	allTables, err := b.GetTables(ctx, tablePattern)
	if err != nil {
		return err
	}
	disks, err := b.ch.GetDisks(ctx)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.DiscardEmptyColumns)
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
				log.Errorf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
			}
			continue
		}
		if bytes, err := fmt.Fprintf(w, "%s.%s\t%s\t%v\t\n", table.Database, table.Name, utils.FormatBytes(table.TotalBytes), strings.Join(tableDisks, ",")); err != nil {
			log.Errorf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
		}
	}
	if err := w.Flush(); err != nil {
		log.Errorf("can't flush tabular writer error: %v", err)
	}
	return nil
}
