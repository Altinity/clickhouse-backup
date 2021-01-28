package backup

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/storage"
	"github.com/AlexAkulov/clickhouse-backup/utils"
)

func printBackups(backupList []storage.Backup, format string, printSize bool) error {
	switch format {
	case "latest", "last", "l":
		if len(backupList) < 1 {
			return fmt.Errorf("no backups found")
		}
		fmt.Println(backupList[len(backupList)-1].Name)
	case "penult", "prev", "previous", "p":
		if len(backupList) < 2 {
			return fmt.Errorf("no penult backup is found")
		}
		fmt.Println(backupList[len(backupList)-2].Name)
	case "all", "":
		if len(backupList) == 0 {
			fmt.Println("no backups found")
		}
		for _, backup := range backupList {
			if printSize {
				fmt.Printf("- '%s'\t%s\t(created at %s)\n", backup.Name, utils.FormatBytes(backup.Size), backup.Date.Format("02-01-2006 15:04:05"))
			} else {
				fmt.Printf("- '%s'\t(created at %s)\n", backup.Name, backup.Date.Format("02-01-2006 15:04:05"))
			}
		}
	default:
		return fmt.Errorf("'%s' undefined", format)
	}
	return nil
}

// PrintLocalBackups - print all backups stored locally
func PrintLocalBackups(cfg config.Config, format string) error {
	backupList, err := ListLocalBackups(cfg)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return printBackups(backupList, format, false)
}

// ListLocalBackups - return slice of all backups stored locally
func ListLocalBackups(cfg config.Config) ([]storage.Backup, error) {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return nil, fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	dataPath, err := ch.GetDefaultPath()
	if err != nil {
		return nil, err
	}

	backupsPath := path.Join(dataPath, "backup")
	d, err := os.Open(backupsPath)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	result := []storage.Backup{}
	names, err := d.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		info, err := os.Stat(path.Join(backupsPath, name))
		if err != nil {
			continue
		}
		if !info.IsDir() {
			continue
		}
		result = append(result, storage.Backup{
			Name: name,
			Date: info.ModTime(),
		})
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Date.Before(result[j].Date)
	})
	return result, nil
}

// PrintRemoteBackups - print all backups stored on remote storage
func PrintRemoteBackups(cfg config.Config, format string) error {
	backupList, err := GetRemoteBackups(cfg)
	if err != nil {
		return err
	}
	return printBackups(backupList, format, true)
}

func GetLocalBackup(cfg config.Config, backupName string) error {
	if backupName == "" {
		return fmt.Errorf("backup name is required")
	}
	backupList, err := ListLocalBackups(cfg)
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.Name == backupName {
			return nil
		}
	}
	return fmt.Errorf("backup '%s' not found", backupName)
}

// GetRemoteBackups - get all backups stored on remote storage
func GetRemoteBackups(cfg config.Config) ([]storage.Backup, error) {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("PrintRemoteBackups aborted: RemoteStorage set to \"none\"")
		return []storage.Backup{}, nil
	}
	bd, err := storage.NewBackupDestination(cfg)
	if err != nil {
		return []storage.Backup{}, err
	}
	err = bd.Connect()
	if err != nil {
		return []storage.Backup{}, err
	}

	backupList, err := bd.BackupList()
	if err != nil {
		return []storage.Backup{}, err
	}
	return backupList, err
}

// getTables - get all tables for use by PrintTables and API
func GetTables(cfg config.Config) ([]clickhouse.Table, error) {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}

	if err := ch.Connect(); err != nil {
		return []clickhouse.Table{}, fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	allTables, err := ch.GetTables()
	if err != nil {
		return []clickhouse.Table{}, fmt.Errorf("can't get tables: %v", err)
	}
	return allTables, nil
}

// PrintTables - print all tables suitable for backup
func PrintTables(cfg config.Config, printAll bool) error {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return err
	}
	defer ch.Close()

	allTables, err := GetTables(cfg)
	if err != nil {
		return err
	}
	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	for _, table := range allTables {
		if table.Skip && !printAll {
			continue
		}
		tableDisks := []string{}
		for disk := range clickhouse.GetDisksByPaths(disks, table.DataPaths) {
			tableDisks = append(tableDisks, disk)
		}
		if table.Skip {
			fmt.Printf("skip\t%s.%s\t%s\t%v\n", table.Database, table.Name, utils.FormatBytes(table.TotalBytes), strings.Join(tableDisks, ","))
			continue
		}
		fmt.Printf("%s.%s\t%s\t%v\n", table.Database, table.Name, utils.FormatBytes(table.TotalBytes), strings.Join(tableDisks, ","))
	}
	return nil
}
