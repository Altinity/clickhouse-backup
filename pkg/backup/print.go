package backup

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/pkg/new_storage"
	"github.com/AlexAkulov/clickhouse-backup/utils"
)

func printBackups(backupList []new_storage.Backup, format, location string) error {
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
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.DiscardEmptyColumns)
		for _, backup := range backupList {
			size := utils.FormatBytes(backup.Size)
			oldFormatLabel := ""
			if backup.Legacy {
				if location == "local" {
					size = ""
				}
				oldFormatLabel = "old-format"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", backup.BackupName, size, backup.CreationDate.Format("02/01/2006 15:04:05"), location, oldFormatLabel)
		}
		w.Flush()
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
	return printBackups(backupList, format, "local")
}

// ListLocalBackups - return slice of all backups stored locally
func ListLocalBackups(cfg config.Config) ([]new_storage.Backup, error) {
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
	result := []new_storage.Backup{}
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
		backupMetafilePath := path.Join(backupsPath, name, "metadata.json")
		backupMetadataBody, err := ioutil.ReadFile(backupMetafilePath)
		if os.IsNotExist(err) {
			// Legacy backup
			result = append(result, new_storage.Backup{
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
			return nil, err
		}
		result = append(result, new_storage.Backup{
			BackupMetadata: backupMetadata,
			Legacy:         false,
		})
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].CreationDate.Before(result[j].CreationDate)
	})
	return result, nil
}

// PrintRemoteBackups - print all backups stored on remote storage
func PrintRemoteBackups(cfg config.Config, format string) error {
	backupList, err := GetRemoteBackups(cfg)
	if err != nil {
		return err
	}
	return printBackups(backupList, format, "remote")
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
		if backup.BackupName == backupName {
			return nil
		}
	}
	return fmt.Errorf("backup '%s' not found", backupName)
}

// GetRemoteBackups - get all backups stored on remote storage
func GetRemoteBackups(cfg config.Config) ([]new_storage.Backup, error) {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("PrintRemoteBackups aborted: RemoteStorage set to \"none\"")
		return []new_storage.Backup{}, nil
	}
	bd, err := new_storage.NewBackupDestination(cfg)
	if err != nil {
		return []new_storage.Backup{}, err
	}
	if err := bd.Connect(); err != nil {
		return []new_storage.Backup{}, err
	}

	backupList, err := bd.BackupList()
	if err != nil {
		return []new_storage.Backup{}, err
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
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.DiscardEmptyColumns)
	for _, table := range allTables {
		if table.Skip && !printAll {
			continue
		}
		tableDisks := []string{}
		for disk := range clickhouse.GetDisksByPaths(disks, table.DataPaths) {
			tableDisks = append(tableDisks, disk)
		}
		if table.Skip {
			fmt.Fprintf(w, "%s.%s\t%s\t%v\tskip\n", table.Database, table.Name, utils.FormatBytes(table.TotalBytes.Int64), strings.Join(tableDisks, ","))
			continue
		}
		fmt.Fprintf(w, "%s.%s\t%s\t%v\t\n", table.Database, table.Name, utils.FormatBytes(table.TotalBytes.Int64), strings.Join(tableDisks, ","))
	}
	w.Flush()
	return nil
}
