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

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/gocarina/gocsv"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// TableInfo holds per-table size information for the info command.
type TableInfo struct {
	Database   string   `json:"database" csv:"database" yaml:"database"`
	Table      string   `json:"table" csv:"table" yaml:"table"`
	TotalBytes uint64   `json:"total_bytes" csv:"total_bytes" yaml:"total_bytes"`
	Size       string   `json:"size" csv:"size" yaml:"size"`
	Parts      int      `json:"parts" csv:"parts" yaml:"parts"`
	Disks      []string `json:"disks" csv:"-" yaml:"disks"`
	DisksStr   string   `json:"-" csv:"disks" yaml:"-"`
}

// InfoResult is the top-level structure for machine-readable output formats.
type InfoResult struct {
	BackupName   string      `json:"backup_name" yaml:"backup_name"`
	BackupType   string      `json:"backup_type" yaml:"backup_type"`
	TablePattern string      `json:"table_pattern,omitempty" yaml:"table_pattern,omitempty"`
	TableCount   int         `json:"table_count" yaml:"table_count"`
	TotalBytes   uint64      `json:"total_bytes" yaml:"total_bytes"`
	TotalSize    string      `json:"total_size" yaml:"total_size"`
	TotalParts   int         `json:"total_parts" yaml:"total_parts"`
	Tables       []TableInfo `json:"tables" yaml:"tables"`
}

// Info displays per-table size breakdown for a backup, with optional table pattern filtering.
// The 'what' parameter selects local, remote, or both (like the list command).
// When tablePattern is set, only matching tables are shown and a total sum is printed.
// The 'format' parameter controls output: text (default), json, yaml, csv, tsv.
func (b *Backuper) Info(what, backupName, tablePattern, format string) error {
	if backupName == "" {
		return errors.New("backup name is required")
	}
	ctx, cancel, _ := status.Current.GetContextWithCancel(status.NotFromAPI)
	defer cancel()

	switch what {
	case "local":
		tables, err := b.infoLocal(ctx, backupName, tablePattern)
		if err != nil {
			return err
		}
		return printInfo(backupName, "local", tablePattern, format, tables)
	case "remote":
		tables, err := b.infoRemote(ctx, backupName, tablePattern)
		if err != nil {
			return err
		}
		return printInfo(backupName, "remote", tablePattern, format, tables)
	case "all", "":
		// Try local first
		localTables, localErr := b.infoLocal(ctx, backupName, tablePattern)
		if localErr == nil && len(localTables) > 0 {
			if err := printInfo(backupName, "local", tablePattern, format, localTables); err != nil {
				return err
			}
		}
		// Then try remote
		remoteTables, remoteErr := b.infoRemote(ctx, backupName, tablePattern)
		if remoteErr == nil && len(remoteTables) > 0 {
			if localTables != nil && len(localTables) > 0 {
				fmt.Println() // separator between local and remote output
			}
			if err := printInfo(backupName, "remote", tablePattern, format, remoteTables); err != nil {
				return err
			}
		}
		// If both failed, return the most relevant error
		if localErr != nil && remoteErr != nil {
			return errors.Errorf("backup '%s' not found locally (%v) or remotely (%v)", backupName, localErr, remoteErr)
		}
		if localTables == nil && remoteTables == nil {
			return errors.Errorf("backup '%s' has no tables", backupName)
		}
		return nil
	default:
		return errors.Errorf("unknown info type '%s', use 'local', 'remote', or 'all'", what)
	}
}

// infoLocal reads per-table metadata from a local backup directory.
func (b *Backuper) infoLocal(ctx context.Context, backupName, tablePattern string) ([]TableInfo, error) {
	if err := b.ch.Connect(); err != nil {
		return nil, errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()

	// Find the backup
	localBackup, _, err := b.getLocalBackup(ctx, backupName, nil)
	if err != nil {
		return nil, errors.WithMessage(err, "Info getLocalBackup")
	}

	// Filter tables by pattern
	filteredTables := filterTablesByPattern(localBackup.Tables, tablePattern)
	if len(filteredTables) == 0 {
		if tablePattern != "" {
			log.Warn().Msgf("no tables matching pattern '%s' found in backup '%s'", tablePattern, backupName)
		}
		return nil, nil
	}

	// Read per-table metadata
	var result []TableInfo
	metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
	for _, t := range filteredTables {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		tmFile := path.Join(metadataPath, common.TablePathEncode(t.Database), fmt.Sprintf("%s.json", common.TablePathEncode(t.Table)))
		var tm metadata.TableMetadata
		if _, err := tm.Load(tmFile); err != nil {
			log.Warn().Str("table", fmt.Sprintf("%s.%s", t.Database, t.Table)).Err(err).Msg("can't load table metadata, skipping")
			continue
		}
		var disks []string
		for disk := range tm.Size {
			disks = append(disks, disk)
		}
		sort.Strings(disks)
		partCount := 0
		for _, parts := range tm.Parts {
			partCount += len(parts)
		}
		result = append(result, TableInfo{
			Database:   t.Database,
			Table:      t.Table,
			TotalBytes: tm.TotalBytes,
			Size:       utils.FormatBytes(tm.TotalBytes),
			Parts:      partCount,
			Disks:      disks,
			DisksStr:   strings.Join(disks, ","),
		})
	}
	return result, nil
}

// infoRemote fetches per-table metadata from remote storage.
func (b *Backuper) infoRemote(ctx context.Context, backupName, tablePattern string) ([]TableInfo, error) {
	if err := b.ch.Connect(); err != nil {
		return nil, errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()

	if b.cfg.General.RemoteStorage == "none" {
		return nil, errors.New("remote_storage is 'none'")
	}
	if b.cfg.General.RemoteStorage == "custom" {
		return nil, errors.New("info command does not support 'custom' remote storage")
	}

	bd, err := storage.NewBackupDestination(ctx, b.cfg, b.ch, "")
	if err != nil {
		return nil, errors.WithMessage(err, "Info NewBackupDestination")
	}
	if err := bd.Connect(ctx); err != nil {
		return nil, errors.WithMessage(err, "Info bd.Connect")
	}
	defer func() {
		if err := bd.Close(ctx); err != nil {
			log.Warn().Msgf("can't close BackupDestination error: %v", err)
		}
	}()

	// Get backup metadata (with table list)
	backupList, err := bd.BackupList(ctx, true, backupName)
	if err != nil {
		return nil, errors.WithMessage(err, "Info BackupList")
	}

	var backupMeta *storage.Backup
	for i := range backupList {
		if backupList[i].BackupName == backupName {
			backupMeta = &backupList[i]
			break
		}
	}
	if backupMeta == nil {
		return nil, errors.Errorf("backup '%s' not found on remote storage", backupName)
	}

	// Filter tables by pattern
	filteredTables := filterTablesByPattern(backupMeta.Tables, tablePattern)
	if len(filteredTables) == 0 {
		if tablePattern != "" {
			log.Warn().Msgf("no tables matching pattern '%s' found in backup '%s'", tablePattern, backupName)
		}
		return nil, nil
	}

	// Download per-table metadata to read TotalBytes
	var result []TableInfo
	for _, t := range filteredTables {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		tableMetadataPath := path.Join(backupName, "metadata", common.TablePathEncode(t.Database), fmt.Sprintf("%s.json", common.TablePathEncode(t.Table)))
		tmReader, err := bd.GetFileReader(ctx, tableMetadataPath)
		if err != nil {
			if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "key not found") || strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "StatusCode 404") {
				log.Warn().Str("table", fmt.Sprintf("%s.%s", t.Database, t.Table)).Msg("table metadata not found on remote, skipping")
				continue
			}
			return nil, errors.Wrapf(err, "GetFileReader(%s)", tableMetadataPath)
		}
		data, err := io.ReadAll(tmReader)
		if err != nil {
			return nil, errors.Wrapf(err, "io.ReadAll(%s)", tableMetadataPath)
		}
		if err := tmReader.Close(); err != nil {
			log.Warn().Err(err).Str("path", tableMetadataPath).Msg("can't close reader")
		}

		var tm metadata.TableMetadata
		if err := json.Unmarshal(data, &tm); err != nil {
			log.Warn().Str("table", fmt.Sprintf("%s.%s", t.Database, t.Table)).Err(err).Msg("can't unmarshal table metadata, skipping")
			continue
		}

		var disks []string
		for disk := range tm.Size {
			disks = append(disks, disk)
		}
		sort.Strings(disks)
		partCount := 0
		for _, parts := range tm.Parts {
			partCount += len(parts)
		}
		result = append(result, TableInfo{
			Database:   t.Database,
			Table:      t.Table,
			TotalBytes: tm.TotalBytes,
			Size:       utils.FormatBytes(tm.TotalBytes),
			Parts:      partCount,
			Disks:      disks,
			DisksStr:   strings.Join(disks, ","),
		})
	}
	return result, nil
}

// filterTablesByPattern filters a list of TableTitle by a comma-separated glob pattern.
func filterTablesByPattern(tables []metadata.TableTitle, tablePattern string) []metadata.TableTitle {
	if tablePattern == "" {
		return tables
	}
	tablePatterns := strings.Split(tablePattern, ",")
	// https://github.com/Altinity/clickhouse-backup/issues/1091
	replacer := strings.NewReplacer("/", "_", `\`, "_")

	var result []metadata.TableTitle
	for _, t := range tables {
		tableName := fmt.Sprintf("%s.%s", t.Database, t.Table)
		for _, pattern := range tablePatterns {
			if matched, _ := filepath.Match(replacer.Replace(strings.Trim(pattern, " \t\r\n")), replacer.Replace(tableName)); matched {
				result = append(result, t)
				break
			}
		}
	}
	return result
}

// printInfo renders the table info output to stdout in the specified format.
func printInfo(backupName, backupType, tablePattern, format string, tables []TableInfo) error {
	// Sort by database.table
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Database != tables[j].Database {
			return tables[i].Database < tables[j].Database
		}
		return tables[i].Table < tables[j].Table
	})

	// Compute totals
	var totalBytes uint64
	var totalParts int
	for _, t := range tables {
		totalBytes += t.TotalBytes
		totalParts += t.Parts
	}

	switch format {
	case "json":
		result := InfoResult{
			BackupName:   backupName,
			BackupType:   backupType,
			TablePattern: tablePattern,
			TableCount:   len(tables),
			TotalBytes:   totalBytes,
			TotalSize:    utils.FormatBytes(totalBytes),
			TotalParts:   totalParts,
			Tables:       tables,
		}
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return errors.WithMessage(err, "printInfo json.Marshal")
		}
		fmt.Println(string(data))
		return nil

	case "yaml":
		result := InfoResult{
			BackupName:   backupName,
			BackupType:   backupType,
			TablePattern: tablePattern,
			TableCount:   len(tables),
			TotalBytes:   totalBytes,
			TotalSize:    utils.FormatBytes(totalBytes),
			TotalParts:   totalParts,
			Tables:       tables,
		}
		data, err := yaml.Marshal(result)
		if err != nil {
			return errors.WithMessage(err, "printInfo yaml.Marshal")
		}
		fmt.Print(string(data))
		return nil

	case "csv":
		csvString, err := gocsv.MarshalString(tables)
		if err != nil {
			return errors.WithMessage(err, "printInfo csv MarshalString")
		}
		fmt.Print(csvString)
		return nil

	case "tsv":
		gocsv.SetCSVWriter(func(out io.Writer) *gocsv.SafeCSVWriter {
			writer := gocsv.NewSafeCSVWriter(csv.NewWriter(out))
			writer.Comma = '\t'
			return writer
		})
		csvString, err := gocsv.MarshalString(tables)
		if err != nil {
			return errors.WithMessage(err, "printInfo tsv MarshalString")
		}
		fmt.Print(csvString)
		return nil

	case "text", "":
		if len(tables) == 0 {
			fmt.Printf("No tables found in %s backup '%s'", backupType, backupName)
			if tablePattern != "" {
				fmt.Printf(" matching pattern '%s'", tablePattern)
			}
			fmt.Println()
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.DiscardEmptyColumns)
		fmt.Fprintf(w, "Backup:\t%s (%s)\n", backupName, backupType)
		if tablePattern != "" {
			fmt.Fprintf(w, "Filter:\t%s\n", tablePattern)
		}
		fmt.Fprintln(w)
		fmt.Fprintf(w, "TABLE\tSIZE\tPARTS\tDISKS\n")
		fmt.Fprintf(w, "-----\t----\t-----\t-----\n")

		for _, t := range tables {
			fmt.Fprintf(w, "%s.%s\t%s\t%d\t%s\n",
				t.Database, t.Table,
				t.Size,
				t.Parts,
				t.DisksStr,
			)
		}

		fmt.Fprintf(w, "-----\t----\t-----\t-----\n")
		fmt.Fprintf(w, "TOTAL (%d tables)\t%s\t%d\t\n", len(tables), utils.FormatBytes(totalBytes), totalParts)
		return w.Flush()
	}
	return nil
}
