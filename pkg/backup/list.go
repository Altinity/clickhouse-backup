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
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/custom"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/gocarina/gocsv"
	"github.com/pkg/errors"
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
	return b.PrintBackup(backupInfos, format)
}

func (b *Backuper) PrintBackup(backupInfos []BackupInfo, format string) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.DiscardEmptyColumns)
	switch format {
	case "json":
		bytes, err := json.Marshal(backupInfos)
		if err != nil {
			log.Error().Msgf("json.Marshal return error: %v", err)
			return errors.Wrap(err, "PrintBackup json.Marshal")
		}
		if _, err := fmt.Fprintln(w, string(bytes)); err != nil {
			log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
			return errors.Wrap(err, "PrintBackup json Fprintln")
		}
		return nil
	case "yaml":
		bytes, err := yaml.Marshal(backupInfos)
		if err != nil {
			log.Error().Msgf("yaml.Marshal return error: %v", err)
			return errors.Wrap(err, "PrintBackup yaml.Marshal")
		}
		if _, err := fmt.Fprintln(w, string(bytes)); err != nil {
			log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", bytes, err)
			return errors.Wrap(err, "PrintBackup yaml Fprintln")
		}
		return nil
	case "csv":
		csvString, err := gocsv.MarshalString(backupInfos)
		if err != nil {
			log.Error().Msgf("gocsv.MarshalString return error: %v", err)
			return errors.Wrap(err, "PrintBackup csv MarshalString")
		}
		if _, err := fmt.Fprintln(w, csvString); err != nil {
			log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", len(csvString), err)
			return errors.Wrap(err, "PrintBackup csv Fprintln")
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
			return errors.Wrap(err, "PrintBackup tsv MarshalString")
		}
		if _, err := fmt.Fprintln(w, csvString); err != nil {
			log.Error().Msgf("fmt.Fprintf write %d bytes return error: %v", len(csvString), err)
			return errors.Wrap(err, "PrintBackup tsv Fprintln")
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
			return nil, nil, errors.WithStack(err)
		}
		defer b.ch.Close()
	}
	if disks == nil {
		disks, err = b.ch.GetDisks(ctx, true)
		if err != nil {
			return nil, nil, errors.WithStack(err)
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
				return nil, nil, errors.WithStack(openErr)
			}
			names, err := d.Readdirnames(-1)
			if err != nil {
				return nil, nil, errors.WithStack(err)
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
		return nil, disks, errors.New("backup name is required")
	}
	backupList, disks, err := b.GetLocalBackups(ctx, disks)
	if err != nil {
		return nil, disks, errors.Wrap(err, "getLocalBackup GetLocalBackups")
	}
	for _, backup := range backupList {
		if backup.BackupName == backupName {
			return &backup, disks, nil
		}
	}
	return nil, disks, errors.Errorf("backup '%s' is not found", backupName)
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
		return nil, errors.New("remote_storage is 'none'")
	}
	if b.cfg.General.RemoteStorage == "custom" {
		return custom.List(ctx, b.cfg)
	}
	bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, "")
	if err != nil {
		return []storage.Backup{}, errors.Wrap(err, "GetRemoteBackups NewBackupDestination")
	}
	if err := bd.Connect(ctx); err != nil {
		return []storage.Backup{}, errors.Wrap(err, "GetRemoteBackups bd.Connect")
	}
	defer func() {
		if err := bd.Close(ctx); err != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", err)
		}
	}()
	backupList, err := bd.BackupList(ctx, parseMetadata, "")
	if err != nil {
		return []storage.Backup{}, errors.Wrap(err, "GetRemoteBackups BackupList")
	}
	// ugly hack to fix https://github.com/Altinity/clickhouse-backup/issues/309
	if parseMetadata == false && len(backupList) > 0 {
		lastBackup := backupList[len(backupList)-1]
		backupList, err = bd.BackupList(ctx, true, lastBackup.BackupName)
		if err != nil {
			return []storage.Backup{}, errors.Wrap(err, "GetRemoteBackups BackupList last")
		}
	}
	return backupList, nil
}

// GetTables - get all tables for use by CreateBackup, PrintTables, and API
func (b *Backuper) GetTables(ctx context.Context, tablePattern string) ([]clickhouse.Table, error) {
	if !b.ch.IsOpen {
		if err := b.ch.Connect(); err != nil {
			return []clickhouse.Table{}, errors.Wrap(err, "can't connect to clickhouse")
		}
		defer b.ch.Close()
	}

	allTables, err := b.ch.GetTables(ctx, tablePattern)
	if err != nil {
		return []clickhouse.Table{}, errors.Wrap(err, "can't get tables")
	}
	if err := b.populateBackupShardField(ctx, allTables); err != nil {
		return nil, errors.Wrap(err, "GetTables populateBackupShardField")
	}
	return allTables, nil
}

// TableRow is the output projection used by the `tables` command for non-text formats.
// Disks is exposed as a structured list in JSON/YAML and as a comma-joined string in CSV/TSV.
type TableRow struct {
	Database   string         `json:"database" yaml:"database" csv:"database"`
	Table      string         `json:"table" yaml:"table" csv:"table"`
	TotalBytes uint64         `json:"total_bytes" yaml:"total_bytes" csv:"total_bytes"`
	Size       string         `json:"size" yaml:"size" csv:"size"`
	Parts      int            `json:"parts" yaml:"parts" csv:"parts"`
	Disks      []string       `json:"disks" yaml:"disks" csv:"-"`
	DisksStr   string         `json:"-" yaml:"-" csv:"disks"`
	Skip       bool           `json:"skip" yaml:"skip" csv:"skip"`
	BackupType string         `json:"backup_type,omitempty" yaml:"backup_type,omitempty" csv:"backup_type"`
	PartsList  []PartRow      `json:"parts_list,omitempty" yaml:"parts_list,omitempty" csv:"-"`
	Partitions []PartitionRow `json:"partitions,omitempty" yaml:"partitions,omitempty" csv:"-"`
}

// PartRow describes one physical data part of a table, as returned by `tables --list-parts`
// (alias `--parts`). Against the live server it comes straight from `system.parts`; against
// `--local-backup`/`--remote-backup` only Name and PartitionID are available (from backup
// metadata), PartitionID being the `_`-delimited prefix of Name, same convention as
// filesystemhelper.IsPartInPartition.
type PartRow struct {
	Name        string `json:"name" yaml:"name" csv:"name"`
	PartitionID string `json:"partition_id" yaml:"partition_id" csv:"partition_id"`
	TotalBytes  uint64 `json:"total_bytes,omitempty" yaml:"total_bytes,omitempty" csv:"total_bytes"`
	Size        string `json:"size,omitempty" yaml:"size,omitempty" csv:"size"`
}

// PartitionRow describes one distinct partition of a table (parts grouped by partition_id), as
// returned by `tables --partitions` (alias `--list-partitions`). Against the live server it
// comes from `system.parts`, including the human-readable Partition value and total size.
// Against `--local-backup`/`--remote-backup` it's aggregated from part names (same convention as
// PartRow.PartitionID), so Partition/TotalBytes/Size stay empty/zero there.
type PartitionRow struct {
	PartitionID string `json:"partition_id" yaml:"partition_id" csv:"partition_id"`
	Partition   string `json:"partition,omitempty" yaml:"partition,omitempty" csv:"partition"`
	Parts       int    `json:"parts" yaml:"parts" csv:"parts"`
	TotalBytes  uint64 `json:"total_bytes,omitempty" yaml:"total_bytes,omitempty" csv:"total_bytes"`
	Size        string `json:"size,omitempty" yaml:"size,omitempty" csv:"size"`
}

// InfoResult wraps a per-backup result with aggregate fields for JSON/YAML output of
// `tables --local-backup` / `tables --remote-backup`.
type InfoResult struct {
	BackupName   string     `json:"backup_name" yaml:"backup_name"`
	BackupType   string     `json:"backup_type" yaml:"backup_type"`
	TablePattern string     `json:"table_pattern,omitempty" yaml:"table_pattern,omitempty"`
	TableCount   int        `json:"table_count" yaml:"table_count"`
	TotalBytes   uint64     `json:"total_bytes" yaml:"total_bytes"`
	TotalSize    string     `json:"total_size" yaml:"total_size"`
	TotalParts   int        `json:"total_parts" yaml:"total_parts"`
	Tables       []TableRow `json:"tables" yaml:"tables"`
}

// tableSection groups rows of a single backup location for layered text/JSON output.
type tableSection struct {
	BackupName   string
	BackupType   string
	TablePattern string
	Rows         []TableRow
}

// PrintTables - print all tables suitable for backup.
// When localBackup or remoteBackup is set, list tables from the corresponding backup
// (per-table size and parts count are read from `metadata.TableMetadata`); both flags may
// be set simultaneously to render `local` and `remote` sections in one go.
// Otherwise tables are read from the live ClickHouse server.
// `format` controls output: text (default), json, yaml, csv, tsv.
// `listParts` and `listPartitions` are independent toggles, usable alone or together, against
// the live server or `--local-backup`/`--remote-backup` alike:
//   - listParts attaches every physical part (name, partition_id, and against the live server
//     also size), read from `system.parts` live or from backup metadata otherwise.
//   - listPartitions attaches the distinct partitions (parts grouped by partition_id), with
//     the human-readable partition value and size only available against the live server.
//
// Against `--local-backup`/`--remote-backup`, partition_id is derived from each part's name
// (the `_`-delimited prefix, same convention as filesystemhelper.IsPartInPartition).
func (b *Backuper) PrintTables(printAll bool, tablePattern, remoteBackup, localBackup, format string, listParts, listPartitions bool) error {
	ctx, cancel, _ := status.Current.GetContextWithCancel(status.NotFromAPI)
	defer cancel()
	if err := b.ch.Connect(); err != nil {
		return errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()

	if localBackup == "" && remoteBackup == "" {
		rows, err := b.collectTablesFromLive(ctx, tablePattern, listParts, listPartitions)
		if err != nil {
			return err
		}
		if !printAll {
			rows = filterSkippedRows(rows)
		}
		return printLiveTableRows(rows, format)
	}

	var sections []tableSection
	if localBackup != "" {
		rows, err := b.collectTablesFromLocalBackup(ctx, localBackup, tablePattern, listParts, listPartitions)
		if err != nil {
			return err
		}
		if !printAll {
			rows = filterSkippedRows(rows)
		}
		sections = append(sections, tableSection{
			BackupName:   localBackup,
			BackupType:   "local",
			TablePattern: tablePattern,
			Rows:         rows,
		})
	}
	if remoteBackup != "" {
		rows, err := b.collectTablesFromRemoteBackup(ctx, remoteBackup, tablePattern, listParts, listPartitions)
		if err != nil {
			return err
		}
		if !printAll {
			rows = filterSkippedRows(rows)
		}
		sections = append(sections, tableSection{
			BackupName:   remoteBackup,
			BackupType:   "remote",
			TablePattern: tablePattern,
			Rows:         rows,
		})
	}
	return printBackupSections(sections, format)
}

// GetTableRowsForLocalBackup returns per-table rows (db, table, size, parts, disks, skip)
// for a local backup, reading metadata from disk; intended for callers like the REST API.
// When printAll is false, tables matching skip_tables are filtered out.
func (b *Backuper) GetTableRowsForLocalBackup(ctx context.Context, backupName, tablePattern string, printAll, listParts, listPartitions bool) ([]TableRow, error) {
	if err := b.ch.Connect(); err != nil {
		return nil, errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()
	rows, err := b.collectTablesFromLocalBackup(ctx, backupName, tablePattern, listParts, listPartitions)
	if err != nil {
		return nil, err
	}
	if printAll {
		return rows, nil
	}
	return filterSkippedRows(rows), nil
}

// GetTableRowsForLive returns per-table rows for the live ClickHouse server, optionally
// including the parts/partitions breakdown from `system.parts` (see collectTablesFromLive);
// intended for callers like the REST API. When printAll is false, tables matching skip_tables
// are filtered out.
func (b *Backuper) GetTableRowsForLive(ctx context.Context, tablePattern string, printAll, listParts, listPartitions bool) ([]TableRow, error) {
	if err := b.ch.Connect(); err != nil {
		return nil, errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()
	rows, err := b.collectTablesFromLive(ctx, tablePattern, listParts, listPartitions)
	if err != nil {
		return nil, err
	}
	if printAll {
		return rows, nil
	}
	return filterSkippedRows(rows), nil
}

// GetTableRowsForRemoteBackup returns per-table rows (db, table, size, parts, disks, skip)
// for a remote backup, downloading per-table metadata; intended for callers like the REST API.
// When printAll is false, tables matching skip_tables are filtered out.
func (b *Backuper) GetTableRowsForRemoteBackup(ctx context.Context, backupName, tablePattern string, printAll, listParts, listPartitions bool) ([]TableRow, error) {
	if err := b.ch.Connect(); err != nil {
		return nil, errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()
	rows, err := b.collectTablesFromRemoteBackup(ctx, backupName, tablePattern, listParts, listPartitions)
	if err != nil {
		return nil, err
	}
	if printAll {
		return rows, nil
	}
	return filterSkippedRows(rows), nil
}

func filterSkippedRows(rows []TableRow) []TableRow {
	out := rows[:0]
	for _, r := range rows {
		if !r.Skip {
			out = append(out, r)
		}
	}
	return out
}

func (b *Backuper) collectTablesFromLive(ctx context.Context, tablePattern string, listParts, listPartitions bool) ([]TableRow, error) {
	allTables, err := b.GetTables(ctx, tablePattern)
	if err != nil {
		return nil, errors.Wrap(err, "collectTablesFromLive GetTables")
	}
	disks, err := b.ch.GetDisks(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "collectTablesFromLive GetDisks")
	}
	rows := make([]TableRow, 0, len(allTables))
	for _, table := range allTables {
		var tableDisks []string
		for disk := range clickhouse.GetDisksByPaths(disks, table.DataPaths) {
			tableDisks = append(tableDisks, disk)
		}
		sort.Strings(tableDisks)
		row := TableRow{
			Database:   table.Database,
			Table:      table.Name,
			TotalBytes: table.TotalBytes,
			Size:       utils.FormatBytes(table.TotalBytes),
			Disks:      tableDisks,
			DisksStr:   strings.Join(tableDisks, ","),
			Skip:       table.Skip,
			BackupType: string(table.BackupType),
		}
		if listParts {
			partRows, err := b.collectPartsForTable(ctx, table.Database, table.Name)
			if err != nil {
				log.Warn().Str("table", fmt.Sprintf("%s.%s", table.Database, table.Name)).Err(err).Msg("can't list parts")
			} else {
				row.PartsList = partRows
			}
		}
		if listPartitions {
			partitionRows, err := b.collectPartitionsForTable(ctx, table.Database, table.Name)
			if err != nil {
				log.Warn().Str("table", fmt.Sprintf("%s.%s", table.Database, table.Name)).Err(err).Msg("can't list partitions")
			} else {
				row.Partitions = partitionRows
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// collectPartsForTable returns every active physical part of a live table from `system.parts`.
func (b *Backuper) collectPartsForTable(ctx context.Context, database, table string) ([]PartRow, error) {
	var result []struct {
		Name        string `ch:"name"`
		PartitionID string `ch:"partition_id"`
		TotalBytes  uint64 `ch:"total_bytes"`
	}
	query := fmt.Sprintf(
		"SELECT name, partition_id, bytes_on_disk AS total_bytes FROM `system`.`parts` WHERE active AND database='%s' AND table='%s' ORDER BY name",
		database, table,
	)
	if err := b.ch.SelectContext(ctx, &result, query); err != nil {
		return nil, errors.Wrapf(err, "collectPartsForTable %s.%s", database, table)
	}
	rows := make([]PartRow, 0, len(result))
	for _, r := range result {
		rows = append(rows, PartRow{
			Name:        r.Name,
			PartitionID: r.PartitionID,
			TotalBytes:  r.TotalBytes,
			Size:        utils.FormatBytes(r.TotalBytes),
		})
	}
	return rows, nil
}

// collectPartitionsForTable returns the distinct partitions of a live table from `system.parts`
// (active parts only), grouped by partition_id.
func (b *Backuper) collectPartitionsForTable(ctx context.Context, database, table string) ([]PartitionRow, error) {
	var result []struct {
		PartitionID string `ch:"partition_id"`
		Partition   string `ch:"partition"`
		Parts       uint64 `ch:"parts"`
		TotalBytes  uint64 `ch:"total_bytes"`
	}
	query := fmt.Sprintf(
		"SELECT partition_id, partition, count() AS parts, sum(bytes_on_disk) AS total_bytes FROM `system`.`parts` WHERE active AND database='%s' AND table='%s' GROUP BY partition_id, partition ORDER BY partition_id",
		database, table,
	)
	if err := b.ch.SelectContext(ctx, &result, query); err != nil {
		return nil, errors.Wrapf(err, "collectPartitionsForTable %s.%s", database, table)
	}
	rows := make([]PartitionRow, 0, len(result))
	for _, r := range result {
		rows = append(rows, PartitionRow{
			PartitionID: r.PartitionID,
			Partition:   r.Partition,
			Parts:       int(r.Parts),
			TotalBytes:  r.TotalBytes,
			Size:        utils.FormatBytes(r.TotalBytes),
		})
	}
	return rows, nil
}

func (b *Backuper) collectTablesFromLocalBackup(ctx context.Context, backupName, tablePattern string, listParts, listPartitions bool) ([]TableRow, error) {
	localBackup, disks, err := b.getLocalBackup(ctx, backupName, nil)
	if err != nil {
		return nil, errors.Wrap(err, "collectTablesFromLocalBackup getLocalBackup")
	}
	if b.DefaultDataPath == "" {
		if b.DefaultDataPath, err = b.ch.GetDefaultPath(disks); err != nil {
			return nil, errors.Wrap(err, "collectTablesFromLocalBackup GetDefaultPath")
		}
	}
	filtered := filterBackupTablesByPattern(localBackup.Tables, tablePattern)
	if len(filtered) == 0 && tablePattern != "" {
		log.Warn().Msgf("no tables matching pattern '%s' found in local backup '%s'", tablePattern, backupName)
	}
	rows := make([]TableRow, 0, len(filtered))
	metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
	for _, t := range filtered {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
		tmFile := path.Join(metadataPath, common.TablePathEncode(t.Database), fmt.Sprintf("%s.json", common.TablePathEncode(t.Table)))
		var tm metadata.TableMetadata
		if _, err := tm.Load(tmFile); err != nil {
			log.Warn().Str("table", tableName).Err(err).Msg("can't load table metadata, skipping size/parts")
			rows = append(rows, TableRow{Database: t.Database, Table: t.Table, Disks: []string{}, Skip: b.shouldSkipByTableName(tableName) || IsInformationSchema(t.Database)})
			continue
		}
		rows = append(rows, b.tableRowFromMetadata(t, &tm, listParts, listPartitions))
	}
	return rows, nil
}

func (b *Backuper) collectTablesFromRemoteBackup(ctx context.Context, backupName, tablePattern string, listParts, listPartitions bool) ([]TableRow, error) {
	if b.cfg.General.RemoteStorage == "none" || b.cfg.General.RemoteStorage == "custom" {
		return nil, errors.New("`tables --remote-backup` does not support `none` and `custom` remote storage")
	}
	ownDst := false
	if b.dst == nil {
		bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, "")
		if err != nil {
			return nil, errors.Wrap(err, "collectTablesFromRemoteBackup NewBackupDestination")
		}
		if err := bd.Connect(ctx); err != nil {
			return nil, errors.Wrap(err, "can't connect to remote storage")
		}
		b.dst = bd
		ownDst = true
	}
	defer func() {
		if ownDst {
			if err := b.dst.Close(ctx); err != nil {
				log.Warn().Msgf("can't close BackupDestination error: %v", err)
			}
			b.dst = nil
		}
	}()

	backupList, err := b.dst.BackupList(ctx, true, backupName)
	if err != nil {
		return nil, errors.Wrap(err, "collectTablesFromRemoteBackup BackupList")
	}
	var remoteBackupMeta *storage.Backup
	for i := range backupList {
		if backupList[i].BackupName == backupName {
			remoteBackupMeta = &backupList[i]
			break
		}
	}
	if remoteBackupMeta == nil {
		return nil, errors.Errorf("backup '%s' not found on remote storage", backupName)
	}

	filtered := filterBackupTablesByPattern(remoteBackupMeta.Tables, tablePattern)
	if len(filtered) == 0 && tablePattern != "" {
		log.Warn().Msgf("no tables matching pattern '%s' found in remote backup '%s'", tablePattern, backupName)
	}
	rows := make([]TableRow, 0, len(filtered))
	for _, t := range filtered {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
		tmPath := path.Join(backupName, "metadata", common.TablePathEncode(t.Database), fmt.Sprintf("%s.json", common.TablePathEncode(t.Table)))
		tmReader, err := b.dst.GetFileReader(ctx, tmPath)
		if err != nil {
			log.Warn().Str("table", tableName).Err(err).Msg("can't read remote table metadata, skipping size/parts")
			rows = append(rows, TableRow{Database: t.Database, Table: t.Table, Disks: []string{}, Skip: b.shouldSkipByTableName(tableName) || IsInformationSchema(t.Database)})
			continue
		}
		data, readErr := io.ReadAll(tmReader)
		if closeErr := tmReader.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Str("path", tmPath).Msg("can't close reader")
		}
		if readErr != nil {
			return nil, errors.Wrapf(readErr, "io.ReadAll(%s)", tmPath)
		}
		var tm metadata.TableMetadata
		if jsonErr := json.Unmarshal(data, &tm); jsonErr != nil {
			log.Warn().Str("table", tableName).Err(jsonErr).Msg("can't unmarshal remote table metadata, skipping size/parts")
			rows = append(rows, TableRow{Database: t.Database, Table: t.Table, Disks: []string{}, Skip: b.shouldSkipByTableName(tableName) || IsInformationSchema(t.Database)})
			continue
		}
		rows = append(rows, b.tableRowFromMetadata(t, &tm, listParts, listPartitions))
	}
	return rows, nil
}

// tableRowFromMetadata builds a TableRow from a backup's TableMetadata. listParts/listPartitions
// derive their breakdown from each part's name (the `_`-delimited prefix is the partition_id,
// same convention as filesystemhelper.IsPartInPartition) - the human-readable partition value
// and per-partition/per-part size aren't stored in backup metadata, so those fields stay
// empty/zero.
func (b *Backuper) tableRowFromMetadata(t metadata.TableTitle, tm *metadata.TableMetadata, listParts, listPartitions bool) TableRow {
	tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
	disks := []string{}
	for disk := range tm.Size {
		disks = append(disks, disk)
	}
	sort.Strings(disks)
	partCount := 0
	for _, parts := range tm.Parts {
		partCount += len(parts)
	}
	row := TableRow{
		Database:   t.Database,
		Table:      t.Table,
		TotalBytes: tm.TotalBytes,
		Size:       utils.FormatBytes(tm.TotalBytes),
		Parts:      partCount,
		Disks:      disks,
		DisksStr:   strings.Join(disks, ","),
		Skip:       IsInformationSchema(t.Database) || b.shouldSkipByTableName(tableName),
	}
	if listParts {
		row.PartsList = partsListFromMetadata(tm)
	}
	if listPartitions {
		row.Partitions = partitionsFromMetadata(tm)
	}
	return row
}

// partsListFromMetadata lists every part stored in a backup's TableMetadata, deriving each
// part's partition_id from the `_`-delimited prefix of its name.
func partsListFromMetadata(tm *metadata.TableMetadata) []PartRow {
	var rows []PartRow
	for _, parts := range tm.Parts {
		for _, part := range parts {
			partitionId, _, _ := strings.Cut(part.Name, "_")
			rows = append(rows, PartRow{Name: part.Name, PartitionID: partitionId})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows
}

// partitionsFromMetadata derives a partition_id -> parts-count breakdown from a backup's
// TableMetadata, by splitting each part's name on its first '_' (the partition_id prefix).
func partitionsFromMetadata(tm *metadata.TableMetadata) []PartitionRow {
	counts := map[string]int{}
	for _, parts := range tm.Parts {
		for _, part := range parts {
			partitionId, _, _ := strings.Cut(part.Name, "_")
			counts[partitionId]++
		}
	}
	ids := make([]string, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	rows := make([]PartitionRow, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, PartitionRow{PartitionID: id, Parts: counts[id]})
	}
	return rows
}

// filterBackupTablesByPattern keeps only tables matching any comma-separated glob pattern.
// Empty pattern returns the input unchanged.
func filterBackupTablesByPattern(tables []metadata.TableTitle, tablePattern string) []metadata.TableTitle {
	if tablePattern == "" {
		return tables
	}
	patterns := strings.Split(tablePattern, ",")
	// https://github.com/Altinity/clickhouse-backup/issues/1091
	replacer := strings.NewReplacer("/", "_", `\`, "_")
	result := make([]metadata.TableTitle, 0, len(tables))
	for _, t := range tables {
		tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
		for _, p := range patterns {
			p = strings.Trim(p, " \t\r\n")
			if p == "*" {
				result = append(result, t)
				break
			}
			if matched, _ := filepath.Match(replacer.Replace(p), replacer.Replace(tableName)); matched {
				result = append(result, t)
				break
			}
		}
	}
	return result
}

// sortTableRows sorts rows by database.table for deterministic, human-friendly output.
func sortTableRows(rows []TableRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Database != rows[j].Database {
			return rows[i].Database < rows[j].Database
		}
		return rows[i].Table < rows[j].Table
	})
}

// printLiveTableRows renders rows from the live ClickHouse server (no backup header / TOTAL).
func printLiveTableRows(rows []TableRow, format string) error {
	switch format {
	case "json":
		data, err := json.Marshal(rows)
		if err != nil {
			return errors.Wrap(err, "printLiveTableRows json.Marshal")
		}
		fmt.Println(string(data))
		return nil
	case "yaml":
		data, err := yaml.Marshal(rows)
		if err != nil {
			return errors.Wrap(err, "printLiveTableRows yaml.Marshal")
		}
		fmt.Print(string(data))
		return nil
	case "csv":
		s, err := gocsv.MarshalString(rows)
		if err != nil {
			return errors.Wrap(err, "printLiveTableRows csv MarshalString")
		}
		fmt.Print(s)
		return nil
	case "tsv":
		gocsv.SetCSVWriter(func(out io.Writer) *gocsv.SafeCSVWriter {
			writer := gocsv.NewSafeCSVWriter(csv.NewWriter(out))
			writer.Comma = '\t'
			return writer
		})
		s, err := gocsv.MarshalString(rows)
		if err != nil {
			return errors.Wrap(err, "printLiveTableRows tsv MarshalString")
		}
		fmt.Print(s)
		return nil
	case "text", "":
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.DiscardEmptyColumns)
		for _, r := range rows {
			marker := r.BackupType
			if r.Skip {
				marker = "skip"
			}
			if _, err := fmt.Fprintf(w, "%s.%s\t%s\t%d\t%s\t%s\n", r.Database, r.Table, r.Size, r.Parts, r.DisksStr, marker); err != nil {
				log.Error().Msgf("printLiveTableRows Fprintf error: %v", err)
			}
			for _, p := range r.Partitions {
				if _, err := fmt.Fprintf(w, "  partition_id=%s\tpartition=%s\t%d parts\t%s\t\n", p.PartitionID, p.Partition, p.Parts, p.Size); err != nil {
					log.Error().Msgf("printLiveTableRows Fprintf error: %v", err)
				}
			}
			for _, part := range r.PartsList {
				if _, err := fmt.Fprintf(w, "  part=%s\tpartition_id=%s\t%s\t\t\n", part.Name, part.PartitionID, part.Size); err != nil {
					log.Error().Msgf("printLiveTableRows Fprintf error: %v", err)
				}
			}
		}
		return w.Flush()
	}
	return errors.Errorf("unknown format '%s', use one of: text, json, yaml, csv, tsv", format)
}

// buildInfoResults transforms tableSections into the InfoResult projection (with totals
// computed from rendered rows).
func buildInfoResults(sections []tableSection) []InfoResult {
	results := make([]InfoResult, 0, len(sections))
	for _, s := range sections {
		var totalBytes uint64
		var totalParts int
		for _, r := range s.Rows {
			totalBytes += r.TotalBytes
			totalParts += r.Parts
		}
		results = append(results, InfoResult{
			BackupName:   s.BackupName,
			BackupType:   s.BackupType,
			TablePattern: s.TablePattern,
			TableCount:   len(s.Rows),
			TotalBytes:   totalBytes,
			TotalSize:    utils.FormatBytes(totalBytes),
			TotalParts:   totalParts,
			Tables:       s.Rows,
		})
	}
	return results
}

// printBackupSections renders one or more tableSection in the requested format.
// JSON/YAML get an InfoResult wrapper (single object for one section, array for several).
// CSV/TSV emit per-section blocks separated by a blank line.
// Text gets `Backup: <name> (type)` header, optional `Filter:` line, sorted rows, and a TOTAL line.
func printBackupSections(sections []tableSection, format string) error {
	for i := range sections {
		sortTableRows(sections[i].Rows)
	}
	switch format {
	case "json":
		results := buildInfoResults(sections)
		var data []byte
		var err error
		if len(results) == 1 {
			data, err = json.MarshalIndent(results[0], "", "  ")
		} else {
			data, err = json.MarshalIndent(results, "", "  ")
		}
		if err != nil {
			return errors.Wrap(err, "printBackupSections json.Marshal")
		}
		fmt.Println(string(data))
		return nil
	case "yaml":
		results := buildInfoResults(sections)
		var data []byte
		var err error
		if len(results) == 1 {
			data, err = yaml.Marshal(results[0])
		} else {
			data, err = yaml.Marshal(results)
		}
		if err != nil {
			return errors.Wrap(err, "printBackupSections yaml.Marshal")
		}
		fmt.Print(string(data))
		return nil
	case "csv":
		for i, s := range sections {
			if i > 0 {
				fmt.Println()
			}
			csvString, err := gocsv.MarshalString(s.Rows)
			if err != nil {
				return errors.Wrap(err, "printBackupSections csv MarshalString")
			}
			fmt.Print(csvString)
		}
		return nil
	case "tsv":
		gocsv.SetCSVWriter(func(out io.Writer) *gocsv.SafeCSVWriter {
			writer := gocsv.NewSafeCSVWriter(csv.NewWriter(out))
			writer.Comma = '\t'
			return writer
		})
		for i, s := range sections {
			if i > 0 {
				fmt.Println()
			}
			csvString, err := gocsv.MarshalString(s.Rows)
			if err != nil {
				return errors.Wrap(err, "printBackupSections tsv MarshalString")
			}
			fmt.Print(csvString)
		}
		return nil
	case "text", "":
		for i, s := range sections {
			if i > 0 {
				fmt.Println()
			}
			if err := renderTextSection(s); err != nil {
				return err
			}
		}
		return nil
	}
	return errors.Errorf("unknown format '%s', use one of: text, json, yaml, csv, tsv", format)
}

func renderTextSection(s tableSection) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.DiscardEmptyColumns)
	if _, err := fmt.Fprintf(w, "Backup:\t%s (%s)\n", s.BackupName, s.BackupType); err != nil {
		return err
	}
	if s.TablePattern != "" {
		if _, err := fmt.Fprintf(w, "Filter:\t%s\n", s.TablePattern); err != nil {
			return err
		}
	}
	if len(s.Rows) == 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "(no tables)"); err != nil {
			return err
		}
		return w.Flush()
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "TABLE\tSIZE\tPARTS\tDISKS\tFLAGS"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "-----\t----\t-----\t-----\t-----"); err != nil {
		return err
	}
	var totalBytes uint64
	var totalParts int
	for _, r := range s.Rows {
		marker := r.BackupType
		if r.Skip {
			marker = "skip"
		}
		if _, err := fmt.Fprintf(w, "%s.%s\t%s\t%d\t%s\t%s\n", r.Database, r.Table, r.Size, r.Parts, r.DisksStr, marker); err != nil {
			return err
		}
		totalBytes += r.TotalBytes
		totalParts += r.Parts
	}
	if _, err := fmt.Fprintln(w, "-----\t----\t-----\t-----\t-----"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "TOTAL (%d tables)\t%s\t%d\t\t\n", len(s.Rows), utils.FormatBytes(totalBytes), totalParts); err != nil {
		return err
	}
	return w.Flush()
}

func (b *Backuper) GetTablesRemote(ctx context.Context, backupName string, tablePattern string) ([]clickhouse.Table, error) {
	if !b.ch.IsOpen {
		if err := b.ch.Connect(); err != nil {
			return []clickhouse.Table{}, errors.Wrap(err, "can't connect to clickhouse")
		}
		defer b.ch.Close()
	}
	if b.cfg.General.RemoteStorage == "none" || b.cfg.General.RemoteStorage == "custom" {
		return nil, errors.New("GetTablesRemote does not support `none` and `custom` remote storage")
	}
	if b.dst == nil {
		bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, "")
		if err != nil {
			return nil, errors.Wrap(err, "GetTablesRemote NewBackupDestination")
		}
		err = bd.Connect(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "can't connect to remote storage")
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
		return nil, errors.Wrap(err, "GetTablesRemote BackupList")
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
