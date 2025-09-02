package backup

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/custom"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/gocarina/gocsv"
	"github.com/ricochet2200/go-disk-usage/du"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type BackupInfo struct {
	BackupName     string
	CreationDate   time.Time
	Size           string
	Description    string
	RequiredBackup string // for incremental backup, this is the base full backup
	Type           string // local or remote
}

func getBackupSizeString(backup interface{}) string {
	var full, data, compressed, obj, meta, rbac, conf, nc uint64
	validType := true
	switch b := backup.(type) {
	case storage.Backup:
		full, data, compressed, obj, meta, rbac, conf, nc = b.GetFullSize(), b.DataSize, b.CompressedSize, b.ObjectDiskSize, b.MetadataSize, b.RBACSize, b.ConfigSize, b.NamedCollectionsSize
	case LocalBackup:
		full, data, compressed, obj, meta, rbac, conf, nc = b.GetFullSize(), b.DataSize, b.CompressedSize, b.ObjectDiskSize, b.MetadataSize, b.RBACSize, b.ConfigSize, b.NamedCollectionsSize
	default:
		validType = false
		log.Warn().Msgf("getBackupSizeString: unknown backup type %T", backup)
	}
	if !validType {
		return "???"
	}
	return fmt.Sprintf("all:%s,data:%s,arch:%s,obj:%s,meta:%s,rbac:%s,conf:%s,nc:%s",
		utils.FormatBytes(full),
		utils.FormatBytes(data),
		utils.FormatBytes(compressed),
		utils.FormatBytes(obj),
		utils.FormatBytes(meta),
		utils.FormatBytes(rbac),
		utils.FormatBytes(conf),
		utils.FormatBytes(nc),
	)
}

// List - list backups to stdout from command line
func (b *Backuper) List(what, ptype, format string) error {
	ctx, cancel, _ := status.Current.GetContextWithCancel(status.NotFromAPI)
	defer cancel()
	backupInfos := make([]BackupInfo, 0, 10)
	switch what {
	case "local":
		backupInfos = append(backupInfos, b.CollectLocalBackups(ctx, ptype)...)
	case "remote":
		backupInfos = append(backupInfos, b.CollectRemoteBackups(ctx, ptype)...)
	case "all", "":
		backupInfos = append(backupInfos, b.CollectAllBackups(ctx, ptype)...)
	}
	return b.PrintBackup(backupInfos, ptype, format)
}

func (b *Backuper) PrintBackup(backupInfos []BackupInfo, ptype, format string) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.DiscardEmptyColumns)
	switch format {
	case "json":
		bytes, err := json.Marshal(backupInfos)
		if err != nil {
			log.Error().Msgf("json.Marshal return error: %v", err)
			return err
		}
		if _, err := fmt.Fprintln(w, string(bytes)); err != nil {
			log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
			return err
		}
		return nil
	case "yaml":
		bytes, err := yaml.Marshal(backupInfos)
		if err != nil {
			log.Error().Msgf("yaml.Marshal return error: %v", err)
			return err
		}
		if _, err := fmt.Fprintln(w, string(bytes)); err != nil {
			log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
			return err
		}
		return nil
	case "csv":
		csvString, err := gocsv.MarshalString(backupInfos)
		if err != nil {
			log.Error().Msgf("gocsv.MarshalString return error: %v", err)
			return err
		}
		if _, err := fmt.Fprintln(w, csvString); err != nil {
			log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", len(csvString), err)
			return err
		}
		return nil
	case "tsv":
		gocsv.SetCSVWriter(func(out io.Writer) *gocsv.SafeCSVWriter {
			writer := gocsv.NewSafeCSVWriter(csv.NewWriter(out))
			writer.Comma = '\t' // Change delimiter to tab
			return writer
		})
		csvString, err := gocsv.MarshalString(backupInfos)
		if err != nil {
			log.Error().Msgf("gocsv.MarshalString return error: %v", err)
			return err
		}
		if _, err := fmt.Fprintln(w, csvString); err != nil {
			log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", len(csvString), err)
			return err
		}
		return nil
	case "text", "":
		for _, backup := range backupInfos {
			creationDate := backup.CreationDate.In(time.Local).Format("2006-01-02 15:04:05")
			if bytes, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", backup.BackupName, creationDate, backup.Type, backup.RequiredBackup, backup.Size, backup.Description); err != nil {
				log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
			}
		}
		return w.Flush()
	}
	return nil
}

func (b *Backuper) CollectAllBackups(ctx context.Context, ptype string) []BackupInfo {
	backupInfos := append(b.CollectLocalBackups(ctx, ptype), b.CollectRemoteBackups(ctx, ptype)...)
	return backupInfos
}

func (b *Backuper) CollectRemoteBackups(ctx context.Context, ptype string) []BackupInfo {
	backupInfos := make([]BackupInfo, 0, 10)
	if b.cfg.General.RemoteStorage != "none" {
		backupList, err := b.GetRemoteBackups(ctx, true)
		if err != nil {
			return backupInfos
		}
		// if err = printBackupsRemote( remoteBackups, format); err != nil {
		// 	log.Warn().Msgf("printBackupsRemote return error: %v", err)
		// }
		switch ptype {
		case "latest", "last", "l":
			if len(backupList) < 1 {
				return backupInfos
			}
			// fmt.Println(backupList[len(backupList)-1].BackupName)
			backupInfos = append(backupInfos, BackupInfo{
				BackupName:   backupList[len(backupList)-1].BackupName,
				CreationDate: backupList[len(backupList)-1].UploadDate,
				Size:         getBackupSizeString(backupList[len(backupList)-1]),
				Description:  backupList[len(backupList)-1].DataFormat,
				Type:         "remote",
				RequiredBackup: func() string {
					if backupList[len(backupList)-1].RequiredBackup != "" {
						return "+" + backupList[len(backupList)-1].RequiredBackup
					}
					return ""
				}(),
			})
			return backupInfos
		case "penult", "prev", "previous", "p":
			if len(backupList) < 2 {
				return backupInfos
			}
			// fmt.Println(backupList[len(backupList)-2].BackupName)
			backupInfos = append(backupInfos, BackupInfo{
				BackupName:   backupList[len(backupList)-2].BackupName,
				CreationDate: backupList[len(backupList)-2].UploadDate,
				Size:         getBackupSizeString(backupList[len(backupList)-2]),
				Description:  backupList[len(backupList)-2].DataFormat,
				Type:         "remote",
				RequiredBackup: func() string {
					if backupList[len(backupList)-2].RequiredBackup != "" {
						return "+" + backupList[len(backupList)-2].RequiredBackup
					}
					return ""
				}(),
			})
			return backupInfos
		case "all", "":
			for _, backup := range backupList {
				size := getBackupSizeString(backup)
				description := backup.DataFormat
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
				backupInfos = append(backupInfos, BackupInfo{
					BackupName:     backup.BackupName,
					CreationDate:   backup.UploadDate,
					Size:           size,
					Description:    description,
					RequiredBackup: required,
					Type:           "remote",
				})

			}
		default:
			return backupInfos
		}
	}
	return backupInfos
}

func (b *Backuper) CollectLocalBackups(ctx context.Context, ptype string) []BackupInfo {
	backupInfos := make([]BackupInfo, 0, 10)
	if !b.ch.IsOpen {
		if err := b.ch.Connect(); err != nil {
			return backupInfos
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
		return backupInfos
	}
	switch ptype {
	case "latest", "last", "l":
		if len(backupList) < 1 {
			return backupInfos
		}
		// fmt.Println(backupList[len(backupList)-1].BackupName)
		backupInfos = append(backupInfos, BackupInfo{
			BackupName:   backupList[len(backupList)-1].BackupName,
			CreationDate: backupList[len(backupList)-1].CreationDate,
			Size:         getBackupSizeString(backupList[len(backupList)-1]),
			Description:  backupList[len(backupList)-1].DataFormat,
			RequiredBackup: func() string {
				if backupList[len(backupList)-1].RequiredBackup != "" {
					return "+" + backupList[len(backupList)-1].RequiredBackup
				}
				return ""
			}(),
			Type: "local",
		})
		return backupInfos
	case "penult", "prev", "previous", "p":
		if len(backupList) < 2 {
			return backupInfos
		}
		// fmt.Println(backupList[len(backupList)-2].BackupName)
		backupInfos = append(backupInfos, BackupInfo{
			BackupName:   backupList[len(backupList)-2].BackupName,
			CreationDate: backupList[len(backupList)-2].CreationDate,
			Size:         getBackupSizeString(backupList[len(backupList)-2]),
			Description:  backupList[len(backupList)-2].DataFormat,
			RequiredBackup: func() string {
				if backupList[len(backupList)-2].RequiredBackup != "" {
					return "+" + backupList[len(backupList)-2].RequiredBackup
				}
				return ""
			}(),
			Type: "local",
		})
		return backupInfos
	case "all", "":
		for _, backup := range backupList {
			select {
			case <-ctx.Done():
				return backupInfos
			default:
				size := getBackupSizeString(backup)
				description := backup.DataFormat
				if backup.Tags != "" {
					if description != "" {
						description += ", "
					}
					description += backup.Tags
				}
				required := ""
				if backup.RequiredBackup != "" {
					required = "+" + backup.RequiredBackup
				}
				if backup.Broken != "" {
					description = backup.Broken
					size = "???"
				}
				backupInfos = append(backupInfos, BackupInfo{
					BackupName:     backup.BackupName,
					CreationDate:   backup.CreationDate,
					Size:           size,
					Description:    description,
					RequiredBackup: required,
					Type:           "local",
				})
			}
		}
	default:
		return backupInfos
	}
	return backupInfos
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
	allBackupPaths := make([]string, 0)
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
	bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, "")
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
		bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, "")
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
			// https://github.com/Altinity/clickhouse-backup/issues/1091
			replacer := strings.NewReplacer("/", "_", `\`, "_")

			for _, t := range remoteBackup.Tables {
				isInformationSchema := IsInformationSchema(t.Database)
				tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
				shallSkipped := b.shouldSkipByTableName(tableName)
				matched := false
				for _, pattern := range tablePatterns {
					// https://github.com/Altinity/clickhouse-backup/issues/1091
					if pattern == "*" {
						matched = true
						break
					}
					// https://github.com/Altinity/clickhouse-backup/issues/1091
					if matched, _ = filepath.Match(replacer.Replace(strings.Trim(pattern, " \t\r\n")), replacer.Replace(tableName)); matched {
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
