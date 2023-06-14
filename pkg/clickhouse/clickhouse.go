package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/pkg/common"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/metadata"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog/log"
)

// ClickHouse - provide
type ClickHouse struct {
	Config               *config.ClickHouseConfig
	conn                 driver.Conn
	disks                []Disk
	version              int
	isPartsColumnPresent int8
	IsOpen               bool
}

// Connect - establish connection to ClickHouse
func (ch *ClickHouse) Connect() error {
	if ch.IsOpen {
		if err := ch.conn.Close(); err != nil {
			log.Error().Msgf("close previous connection error: %v", err)
		}
	}
	ch.IsOpen = false
	timeout, err := time.ParseDuration(ch.Config.Timeout)
	if err != nil {
		return err
	}

	//timeoutSeconds := fmt.Sprintf("%d", int(timeout.Seconds()))
	opt := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", ch.Config.Host, ch.Config.Port)},
		Auth: clickhouse.Auth{
			Username: ch.Config.Username,
			Password: ch.Config.Password,
		},
		Settings: clickhouse.Settings{
			//	"connect_timeout": int(timeout.Seconds()),
			//	"receive_timeout": int(timeout.Seconds()),
			//	"send_timeout":    int(timeout.Seconds()),
		},
		MaxOpenConns:    1,
		ConnMaxLifetime: 0,
		MaxIdleConns:    0,
		DialTimeout:     timeout,
		ReadTimeout:     timeout,
	}

	if ch.Config.Debug {
		opt.Debug = true
	}

	if ch.Config.Secure {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: ch.Config.SkipVerify,
		}
		if ch.Config.TLSKey != "" || ch.Config.TLSCert != "" || ch.Config.TLSCa != "" {
			if ch.Config.TLSCert != "" || ch.Config.TLSKey != "" {
				cert, err := tls.LoadX509KeyPair(ch.Config.TLSCert, ch.Config.TLSKey)
				if err != nil {
					log.Error().Msgf("tls.LoadX509KeyPair error: %v", err)
					return err
				}
				tlsConfig.Certificates = []tls.Certificate{cert}
			}
			if ch.Config.TLSCa != "" {
				caCert, err := os.ReadFile(ch.Config.TLSCa)
				if err != nil {
					log.Error().Msgf("read `tls_ca` file %s return error: %v ", ch.Config.TLSCa, err)
					return err
				}
				caCertPool := x509.NewCertPool()
				if caCertPool.AppendCertsFromPEM(caCert) != true {
					log.Error().Msgf("AppendCertsFromPEM %s return false", ch.Config.TLSCa)
					return fmt.Errorf("AppendCertsFromPEM %s return false", ch.Config.TLSCa)
				}
				tlsConfig.RootCAs = caCertPool
			}
		}
		opt.TLS = tlsConfig
	}
	if !ch.Config.LogSQLQueries {
		opt.Settings["log_queries"] = 0
	}

	if ch.conn, err = clickhouse.Open(opt); err != nil {
		log.Error().Msgf("clickhouse connection: %s, clickhouse.Open return error: %v", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port), err)
		return err
	}

	logFunc := log.Info()
	if !ch.Config.LogSQLQueries {
		logFunc = log.Debug()
	}
	logFunc.Stack().Msgf("clickhouse connection prepared: %s run ping", fmt.Sprintf("tcp://%v:%v?timeout=%v", ch.Config.Host, ch.Config.Port, ch.Config.Timeout))
	err = ch.conn.Ping(context.Background())
	if err != nil {
		log.Error().Msgf("clickhouse connection ping: %s return error: %v", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port), err)
		return err
	} else {
		ch.IsOpen = true
	}
	logFunc.Stack().Msgf("clickhouse connection open: %s", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port))
	return err
}

// GetDisks - return data from system.disks table
func (ch *ClickHouse) GetDisks(ctx context.Context) ([]Disk, error) {
	version, err := ch.GetVersion(ctx)
	if err != nil {
		return nil, err
	}
	var disks []Disk
	if version < 19015000 {
		disks, err = ch.getDisksFromSystemSettings(ctx)
	} else {
		disks, err = ch.getDisksFromSystemDisks(ctx)
	}
	if err != nil {
		return nil, err
	}
	for i := range disks {
		if disks[i].Name == ch.Config.EmbeddedBackupDisk {
			disks[i].IsBackup = true
		}
	}
	if len(ch.Config.DiskMapping) == 0 {
		return disks, nil
	}
	dm := map[string]string{}
	for k, v := range ch.Config.DiskMapping {
		dm[k] = v
	}
	for i := range disks {
		if p, ok := dm[disks[i].Name]; ok {
			disks[i].Path = p
			delete(dm, disks[i].Name)
		}
	}
	for k, v := range dm {
		disks = append(disks, Disk{
			Name: k,
			Path: v,
			Type: "local",
		})
	}
	return disks, nil
}

func (ch *ClickHouse) GetEmbeddedBackupPath(disks []Disk) (string, error) {
	if !ch.Config.UseEmbeddedBackupRestore {
		return "", nil
	}
	if ch.Config.EmbeddedBackupDisk == "" {
		return "", fmt.Errorf("please setup `clickhouse->embedded_backup_disk` in config or CLICKHOUSE_EMBEDDED_BACKUP_DISK environment variable")
	}
	for _, d := range disks {
		if d.Name == ch.Config.EmbeddedBackupDisk {
			return d.Path, nil
		}
	}
	return "", fmt.Errorf("%s not found in system.disks %v", ch.Config.EmbeddedBackupDisk, disks)
}

func (ch *ClickHouse) GetDefaultPath(disks []Disk) (string, error) {
	defaultPath := "/var/lib/clickhouse"
	for _, d := range disks {
		if d.Name == "default" {
			defaultPath = d.Path
			break
		}
	}
	return defaultPath, nil
}

func (ch *ClickHouse) getDisksFromSystemSettings(ctx context.Context) ([]Disk, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		metadataPath, err := ch.getMetadataPath(ctx)
		if err != nil {
			return nil, err
		}
		dataPathArray := strings.Split(metadataPath, "/")
		clickhouseData := path.Join(dataPathArray[:len(dataPathArray)-1]...)
		return []Disk{{
			Name: "default",
			Path: path.Join("/", clickhouseData),
			Type: "local",
		}}, nil
	}
}

func (ch *ClickHouse) getMetadataPath(ctx context.Context) (string, error) {
	var result []struct {
		MetadataPath string `ch:"metadata_path"`
	}
	query := "SELECT metadata_path FROM system.tables WHERE database = 'system' AND metadata_path!='' LIMIT 1;"
	if err := ch.SelectContext(ctx, &result, query); err != nil {
		return "", err
	}
	if len(result) == 0 {
		return "", fmt.Errorf("can't get metadata_path from system.tables")
	}
	metadataPath := strings.Split(result[0].MetadataPath, "/")
	if strings.Contains(result[0].MetadataPath, "/store/") {
		result[0].MetadataPath = path.Join(metadataPath[:len(metadataPath)-4]...)
		result[0].MetadataPath = path.Join(result[0].MetadataPath, "metadata")
	} else {
		result[0].MetadataPath = path.Join(metadataPath[:len(metadataPath)-2]...)
	}
	return path.Join("/", result[0].MetadataPath), nil
}

func (ch *ClickHouse) getDisksFromSystemDisks(ctx context.Context) ([]Disk, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		isDiskType := make([]struct {
			Present uint64 `ch:"is_disk_type_present"`
		}, 0)
		if err := ch.SelectContext(ctx, &isDiskType, "SELECT count() is_disk_type_present FROM system.columns WHERE database='system' AND table='disks' AND name='type'"); err != nil {
			return nil, err
		}
		diskTypeSQL := "'local'"
		if len(isDiskType) > 0 && isDiskType[0].Present > 0 {
			diskTypeSQL = "any(type)"
		}
		var result []Disk
		query := fmt.Sprintf("SELECT path, any(name) AS name, %s AS type FROM system.disks GROUP BY path", diskTypeSQL)
		err := ch.SelectContext(ctx, &result, query)
		return result, err
	}
}

// Close - closing connection to ClickHouse
func (ch *ClickHouse) Close() {
	if ch.IsOpen {
		if err := ch.conn.Close(); err != nil {
			log.Warn().Msgf("can't close clickhouse connection: %v", err)
		}
	}
	if ch.Config.LogSQLQueries {
		log.Info().Msg("clickhouse connection closed")
	} else {
		log.Debug().Msg("clickhouse connection closed")
	}
	ch.IsOpen = false
}

// GetTables - return slice of all tables suitable for backup, MySQL and PostgresSQL database engine shall be skipped
func (ch *ClickHouse) GetTables(ctx context.Context, tablePattern string) ([]Table, error) {
	var err error
	settings := map[string]bool{
		"show_table_uuid_in_table_create_query_if_not_nil": false,
		"display_secrets_in_show_and_select":               false,
	}
	if settings, err = ch.CheckSettingsExists(ctx, settings); err != nil {
		return nil, err
	}
	skipDatabases := make([]struct {
		Name string `ch:"name"`
	}, 0)
	// MaterializedPostgreSQL doesn't support FREEZE look https://github.com/Altinity/clickhouse-backup/issues/550
	if err = ch.SelectContext(ctx, &skipDatabases, "SELECT name FROM system.databases WHERE engine IN ('MySQL','PostgreSQL','MaterializedPostgreSQL')"); err != nil {
		return nil, err
	}
	skipDatabaseNames := make([]string, len(skipDatabases))
	for i, s := range skipDatabases {
		skipDatabaseNames[i] = s.Name
	}
	allTablesSQL, err := ch.prepareAllTablesSQL(ctx, tablePattern, err, skipDatabaseNames, settings)
	if err != nil {
		return nil, err
	}
	tables := make([]Table, 0)
	if err = ch.SelectContext(ctx, &tables, allTablesSQL); err != nil {
		return nil, err
	}
	metadataPath, err := ch.getMetadataPath(ctx)
	if err != nil {
		return nil, err
	}
	for i, t := range tables {
		for _, filter := range ch.Config.SkipTables {
			if matched, _ := filepath.Match(strings.Trim(filter, " \t\r\n"), fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				t.Skip = true
				break
			}
		}
		if ch.Config.UseEmbeddedBackupRestore && (strings.HasPrefix(t.Name, ".inner_id.") /*|| strings.HasPrefix(t.Name, ".inner.")*/) {
			t.Skip = true
		}
		if t.Skip {
			tables[i] = t
			continue
		}
		tables[i] = ch.fixVariousVersions(ctx, t, metadataPath)
	}
	if len(tables) == 0 {
		return tables, nil
	}
	for i, table := range tables {
		if table.TotalBytes == 0 && !table.Skip && strings.HasSuffix(table.Engine, "Tree") {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				tables[i].TotalBytes = ch.getTableSizeFromParts(ctx, tables[i])
			}
		}
	}
	return tables, nil
}

func (ch *ClickHouse) prepareAllTablesSQL(ctx context.Context, tablePattern string, err error, skipDatabases []string, settings map[string]bool) (string, error) {
	isSystemTablesFieldPresent := make([]IsSystemTablesFieldPresent, 0)
	isFieldPresentSQL := `
		SELECT 
			countIf(name='data_path') is_data_path_present, 
			countIf(name='data_paths') is_data_paths_present, 
			countIf(name='uuid') is_uuid_present, 
			countIf(name='create_table_query') is_create_table_query_present, 
			countIf(name='total_bytes') is_total_bytes_present 
		FROM system.columns WHERE database='system' AND table='tables'
	`
	if err = ch.SelectContext(ctx, &isSystemTablesFieldPresent, isFieldPresentSQL); err != nil {
		return "", err
	}

	allTablesSQL := "SELECT database, name, engine "
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsDataPathPresent > 0 {
		allTablesSQL += ", data_path "
	}
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsDataPathsPresent > 0 {
		allTablesSQL += ", data_paths "
	}
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsUUIDPresent > 0 {
		allTablesSQL += ", uuid "
	}
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsCreateTableQueryPresent > 0 {
		allTablesSQL += ", create_table_query "
	}
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsTotalBytesPresent > 0 {
		allTablesSQL += ", coalesce(total_bytes, 0) AS total_bytes "
	}

	allTablesSQL += "  FROM system.tables WHERE is_temporary = 0"
	if tablePattern != "" {
		replacer := strings.NewReplacer(".", "\\.", ",", "|", "*", ".*", "?", ".", " ", "", "'", "")
		allTablesSQL += fmt.Sprintf(" AND match(concat(database,'.',name),'%s') ", replacer.Replace(tablePattern))
	}
	if len(skipDatabases) > 0 {
		allTablesSQL += fmt.Sprintf(" AND database NOT IN ('%s')", strings.Join(skipDatabases, "','"))
	}
	// try to upload big tables first
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsTotalBytesPresent > 0 {
		allTablesSQL += " ORDER BY total_bytes DESC"
	}
	allTablesSQL = ch.addSettingsSQL(allTablesSQL, settings)
	return allTablesSQL, nil
}

func (ch *ClickHouse) addSettingsSQL(inputSQL string, settings map[string]bool) string {
	maxI := 0
	for _, v := range settings {
		if v {
			maxI += 1
		}
	}
	i := 0
	for k, v := range settings {
		if v {
			if i == 0 {
				inputSQL += " SETTINGS "
			}
			inputSQL += k + "=1"
			if i < maxI-1 {
				inputSQL += ", "
			}
			i++
		}
	}
	return inputSQL
}

// GetDatabases - return slice of all non system databases for backup
func (ch *ClickHouse) GetDatabases(ctx context.Context, cfg *config.Config, tablePattern string) ([]Database, error) {
	allDatabases := make([]Database, 0)
	skipDatabases := []string{"system", "INFORMATION_SCHEMA", "information_schema", "_temporary_and_external_tables"}
	bypassDatabases := make([]string, 0)
	var skipTablesPatterns, bypassTablesPatterns []string
	skipTablesPatterns = append(skipTablesPatterns, cfg.ClickHouse.SkipTables...)
	bypassTablesPatterns = append(bypassTablesPatterns, strings.Split(tablePattern, ",")...)
	for _, pattern := range skipTablesPatterns {
		pattern = strings.Trim(pattern, " \r\t\n")
		if strings.HasSuffix(pattern, ".*") {
			skipDatabases = append(skipDatabases, strings.TrimSuffix(pattern, ".*"))
		}
	}
	for _, pattern := range bypassTablesPatterns {
		pattern = strings.Trim(pattern, " \r\t\n")
		if strings.HasSuffix(pattern, ".*") {
			bypassDatabases = append(bypassDatabases, strings.TrimSuffix(pattern, ".*"))
		}
	}
	metadataPath, err := ch.getMetadataPath(ctx)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if len(bypassDatabases) > 0 {
			allDatabasesSQL := fmt.Sprintf(
				"SELECT name, engine FROM system.databases WHERE name NOT IN ('%s') AND name IN ('%s')",
				strings.Join(skipDatabases, "','"), strings.Join(bypassDatabases, "','"),
			)
			if err := ch.StructSelect(&allDatabases, allDatabasesSQL); err != nil {
				return nil, err
			}
		} else {
			allDatabasesSQL := fmt.Sprintf(
				"SELECT name, engine FROM system.databases WHERE name NOT IN ('%s')",
				strings.Join(skipDatabases, "','"),
			)
			if err := ch.StructSelect(&allDatabases, allDatabasesSQL); err != nil {
				return nil, err
			}
		}
	}
	for i, db := range allDatabases {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			showDatabaseSQL := fmt.Sprintf("SHOW CREATE DATABASE `%s`", db.Name)
			var result string
			// 19.4 doesn't have /var/lib/clickhouse/metadata/default.sql
			if err := ch.SelectSingleRow(ctx, &result, showDatabaseSQL); err != nil {
				log.Warn().Msgf("can't get create database query: %v", err)
				allDatabases[i].Query = fmt.Sprintf("CREATE DATABASE `%s` ENGINE = %s", db.Name, db.Engine)
			} else {
				// 23.3+ masked secrets https://github.com/Altinity/clickhouse-backup/issues/640
				if strings.Contains(result, "'[HIDDEN]'") {
					if attachSQL, err := os.ReadFile(path.Join(metadataPath, common.TablePathEncode(db.Name)+".sql")); err != nil {
						return nil, err
					} else {
						result = strings.Replace(string(attachSQL), "ATTACH", "CREATE", 1)
						result = strings.Replace(result, " _ ", " `"+db.Name+"` ", 1)
					}
				}
				allDatabases[i].Query = result
			}
		}
	}
	return allDatabases, nil
}

func (ch *ClickHouse) getTableSizeFromParts(ctx context.Context, table Table) uint64 {
	var tablesSize []struct {
		Size uint64 `ch:"size"`
	}
	query := fmt.Sprintf("SELECT sum(bytes_on_disk) as size FROM system.parts WHERE active AND database='%s' AND table='%s' GROUP BY database, table", table.Database, table.Name)
	if err := ch.SelectContext(ctx, &tablesSize, query); err != nil {
		log.Warn().Msgf("error parsing tablesSize: %v", err)
	}
	if len(tablesSize) > 0 {
		return tablesSize[0].Size
	}
	return 0
}

func (ch *ClickHouse) fixVariousVersions(ctx context.Context, t Table, metadataPath string) Table {
	// versions before 19.15 contain data_path in a different column
	if t.DataPath != "" {
		t.DataPaths = []string{t.DataPath}
	}
	// version 20.6.3.28 has zero UUID
	if t.UUID == "00000000-0000-0000-0000-000000000000" {
		t.UUID = ""
	}
	// version 1.1.54394 no has query column
	if strings.TrimSpace(t.CreateTableQuery) == "" {
		t.CreateTableQuery = ch.ShowCreateTable(ctx, t.Database, t.Name)
	}
	// materialized views should properly restore via attach
	if t.Engine == "MaterializedView" {
		t.CreateTableQuery = strings.Replace(
			t.CreateTableQuery, "CREATE MATERIALIZED VIEW", "ATTACH MATERIALIZED VIEW", 1,
		)
	}
	// 23.3+ masked secrets https://github.com/Altinity/clickhouse-backup/issues/640
	if strings.Contains(t.CreateTableQuery, "'[HIDDEN]'") {
		tableSQLPath := path.Join(metadataPath, common.TablePathEncode(t.Database), common.TablePathEncode(t.Name)+".sql")
		if attachSQL, err := os.ReadFile(tableSQLPath); err != nil {
			log.Warn().Msgf("can't read %s: %v", tableSQLPath, err)
		} else {
			t.CreateTableQuery = strings.Replace(string(attachSQL), "ATTACH", "CREATE", 1)
			t.CreateTableQuery = strings.Replace(t.CreateTableQuery, " _ ", " `"+t.Database+"`.`"+t.Name+"` ", 1)
		}
	}
	return t
}

// GetVersion - returned ClickHouse version in number format
// Example value: 19001005
func (ch *ClickHouse) GetVersion(ctx context.Context) (int, error) {
	if ch.version != 0 {
		return ch.version, nil
	}
	var result string
	var err error
	query := "SELECT value FROM `system`.`build_options` where name='VERSION_INTEGER'"
	if err = ch.SelectSingleRow(ctx, &result, query); err != nil {
		log.Warn().Msgf("can't get ClickHouse version: %v", err)
		return 0, nil
	}
	ch.version, err = strconv.Atoi(result)
	return ch.version, err
}

func (ch *ClickHouse) GetVersionDescribe(ctx context.Context) string {
	var result string
	query := "SELECT value FROM `system`.`build_options` where name='VERSION_DESCRIBE'"
	if err := ch.SelectSingleRow(ctx, &result, query); err != nil {
		return ""
	}
	return result
}

// FreezeTableOldWay - freeze all partitions in table one by one
// This way using for ClickHouse below v19.1
func (ch *ClickHouse) FreezeTableOldWay(ctx context.Context, table *Table, name string) error {
	var partitions []struct {
		PartitionID string `ch:"partition_id"`
	}
	q := fmt.Sprintf("SELECT DISTINCT partition_id FROM `system`.`parts` WHERE database='%s' AND table='%s' %s", table.Database, table.Name, ch.Config.FreezeByPartWhere)
	if err := ch.conn.Select(ctx, &partitions, q); err != nil {
		return fmt.Errorf("can't get partitions for '%s.%s': %w", table.Database, table.Name, err)
	}
	withNameQuery := ""
	if name != "" {
		withNameQuery = fmt.Sprintf("WITH NAME '%s'", name)
	}
	for _, item := range partitions {
		log.Debug().Msgf("  partition '%v'", item.PartitionID)
		query := fmt.Sprintf(
			"ALTER TABLE `%v`.`%v` FREEZE PARTITION ID '%v' %s;",
			table.Database,
			table.Name,
			item.PartitionID,
			withNameQuery,
		)
		if item.PartitionID == "all" {
			query = fmt.Sprintf(
				"ALTER TABLE `%v`.`%v` FREEZE PARTITION tuple() %s;",
				table.Database,
				table.Name,
				withNameQuery,
			)
		}
		if err := ch.QueryContext(ctx, query); err != nil {
			if (strings.Contains(err.Error(), "code: 60") || strings.Contains(err.Error(), "code: 81")) && ch.Config.IgnoreNotExistsErrorDuringFreeze {
				log.Warn().Msgf("can't freeze partition: %v", err)
			} else {
				return fmt.Errorf("can't freeze partition '%s': %w", item.PartitionID, err)
			}
		}
	}
	return nil
}

// FreezeTable - freeze all partitions for table
// This way available for ClickHouse since v19.1
func (ch *ClickHouse) FreezeTable(ctx context.Context, table *Table, name string) error {
	version, err := ch.GetVersion(ctx)
	if err != nil {
		return err
	}
	if strings.HasPrefix(table.Engine, "Replicated") && ch.Config.SyncReplicatedTables {
		query := fmt.Sprintf("SYSTEM SYNC REPLICA `%s`.`%s`;", table.Database, table.Name)
		if err := ch.QueryContext(ctx, query); err != nil {
			log.Warn().Msgf("can't sync replica: %v", err)
		} else {
			log.Debug().Str("table", fmt.Sprintf("%s.%s", table.Database, table.Name)).Msg("replica synced")
		}
	}
	if version < 19001005 || ch.Config.FreezeByPart {
		return ch.FreezeTableOldWay(ctx, table, name)
	}
	withNameQuery := ""
	if name != "" {
		withNameQuery = fmt.Sprintf("WITH NAME '%s'", name)
	}
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE %s;", table.Database, table.Name, withNameQuery)
	if err := ch.QueryContext(ctx, query); err != nil {
		if (strings.Contains(err.Error(), "code: 60") || strings.Contains(err.Error(), "code: 81")) && ch.Config.IgnoreNotExistsErrorDuringFreeze {
			log.Warn().Msgf("can't freeze table: %v", err)
			return nil
		}
		return fmt.Errorf("can't freeze table: %v", err)
	}
	return nil
}

// AttachDataParts - execute ALTER TABLE ... ATTACH PART command for specific table
func (ch *ClickHouse) AttachDataParts(table metadata.TableMetadata, disks []Disk) error {
	canContinue, err := ch.CheckReplicationInProgress(table)
	if err != nil {
		return err
	}
	if !canContinue {
		return nil
	}
	for _, disk := range disks {
		for _, part := range table.Parts[disk.Name] {
			if !strings.HasSuffix(part.Name, ".proj") {
				query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PART '%s'", table.Database, table.Table, part.Name)
				if err := ch.Query(query); err != nil {
					return err
				}
				log.Debug().Str("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).Str("disk", disk.Name).Str("part", part.Name).Msg("attached")
			}
		}
	}
	return nil
}

var replicatedMergeTreeRE = regexp.MustCompile(`Replicated[\w_]*MergeTree\s*\(((\s*'[^']+'\s*),(\s*'[^']+')(\s*,\s*|.*?)([^)]*)|)\)`)
var uuidRE = regexp.MustCompile(`UUID '([^']+)'`)

// AttachTable - execute ATTACH TABLE  command for specific table
func (ch *ClickHouse) AttachTable(ctx context.Context, table metadata.TableMetadata) error {
	if len(table.Parts) == 0 {
		log.Warn().Msgf("no data parts for restore for `%s`.`%s`", table.Database, table.Table)
		return nil
	}
	canContinue, err := ch.CheckReplicationInProgress(table)
	if err != nil {
		return err
	}
	if !canContinue {
		return nil
	}

	query := fmt.Sprintf("DETACH TABLE `%s`.`%s`", table.Database, table.Table)
	if err := ch.Query(query); err != nil {
		return err
	}
	if matches := replicatedMergeTreeRE.FindStringSubmatch(table.Query); len(matches) > 0 {
		zkPath := strings.Trim(matches[2], "' \r\n\t")
		replicaName := strings.Trim(matches[3], "' \r\n\t")
		if strings.Contains(zkPath, "{uuid}") {
			if uuidMatches := uuidRE.FindStringSubmatch(table.Query); len(uuidMatches) > 0 {
				zkPath = strings.Replace(zkPath, "{uuid}", uuidMatches[1], 1)
			}
		}
		zkPath = strings.NewReplacer("{database}", table.Database, "{table}", table.Table).Replace(zkPath)
		zkPath, err = ch.ApplyMacros(ctx, zkPath)
		if err != nil {
			return err
		}
		replicaName, err = ch.ApplyMacros(ctx, replicaName)
		if err != nil {
			return err
		}
		query = fmt.Sprintf("SYSTEM DROP REPLICA '%s' FROM ZKPATH '%s'", replicaName, zkPath)
		if err := ch.Query(query); err != nil {
			return err
		}
	}
	query = fmt.Sprintf("ATTACH TABLE `%s`.`%s`", table.Database, table.Table)
	if err := ch.Query(query); err != nil {
		return err
	}

	query = fmt.Sprintf("SYSTEM RESTORE REPLICA `%s`.`%s`", table.Database, table.Table)
	if err := ch.Query(query); err != nil {
		return err
	}

	log.Debug().Str("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).Msg("attached")
	return nil
}
func (ch *ClickHouse) ShowCreateTable(ctx context.Context, database, name string) string {
	var result []struct {
		Statement string `ch:"statement"`
	}
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`;", database, name)
	if err := ch.SelectContext(ctx, &result, query); err != nil {
		return ""
	}
	return result[0].Statement
}

// CreateDatabase - create ClickHouse database
func (ch *ClickHouse) CreateDatabase(database string, cluster string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
	if cluster != "" {
		query += fmt.Sprintf(" ON CLUSTER '%s'", cluster)
	}
	return ch.Query(query)
}

func (ch *ClickHouse) CreateDatabaseWithEngine(database, engine, cluster string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ENGINE=%s", database, engine)
	query = ch.addOnClusterToCreateDatabase(cluster, query)
	return ch.Query(query)
}

func (ch *ClickHouse) CreateDatabaseFromQuery(ctx context.Context, query, cluster string, args ...interface{}) error {
	if !strings.HasPrefix(query, "CREATE DATABASE IF NOT EXISTS") {
		query = strings.Replace(query, "CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS", 1)
	}
	query = ch.addOnClusterToCreateDatabase(cluster, query)
	return ch.QueryContext(ctx, query, args)
}

func (ch *ClickHouse) addOnClusterToCreateDatabase(cluster string, query string) string {
	if cluster != "" && !strings.Contains(query, " ON CLUSTER ") {
		if !strings.Contains(query, "ENGINE") {
			query += fmt.Sprintf(" ON CLUSTER '%s'", cluster)
		} else {
			query = strings.Replace(query, "ENGINE", fmt.Sprintf(" ON CLUSTER '%s' ENGINE", cluster), 1)
		}
	}
	return query
}

// DropTable - drop ClickHouse table
func (ch *ClickHouse) DropTable(table Table, query string, onCluster string, ignoreDependencies bool, version int) error {
	var isAtomic bool
	var err error
	if isAtomic, err = ch.IsAtomic(table.Database); err != nil {
		return err
	}
	kind := "TABLE"
	if strings.HasPrefix(query, "CREATE DICTIONARY") {
		kind = "DICTIONARY"
	}
	dropQuery := fmt.Sprintf("DROP %s IF EXISTS `%s`.`%s`", kind, table.Database, table.Name)
	if version > 19000000 && onCluster != "" {
		dropQuery += " ON CLUSTER '" + onCluster + "' "
	}
	if isAtomic {
		dropQuery += " NO DELAY"
	}
	if ignoreDependencies {
		dropQuery += " SETTINGS check_table_dependencies=0"
	}
	if err = ch.Query(dropQuery); err != nil {
		return err
	}
	return nil
}

var createViewToClauseRe = regexp.MustCompile(`(?im)^(CREATE[\s\w]+VIEW[^(]+)(\s+TO\s+.+)`)
var createViewSelectRe = regexp.MustCompile(`(?im)^(CREATE[\s\w]+VIEW[^(]+)(\s+AS\s+SELECT.+)`)
var attachViewToClauseRe = regexp.MustCompile(`(?im)^(ATTACH[\s\w]+VIEW[^(]+)(\s+TO\s+.+)`)
var attachViewSelectRe = regexp.MustCompile(`(?im)^(ATTACH[\s\w]+VIEW[^(]+)(\s+AS\s+SELECT.+)`)
var createObjRe = regexp.MustCompile(`(?is)^(CREATE [^(]+)(\(.+)`)
var onClusterRe = regexp.MustCompile(`(?im)\s+ON\s+CLUSTER\s+`)
var distributedRE = regexp.MustCompile(`(Distributed)\(([^,]+),([^)]+)\)`)

// CreateTable - create ClickHouse table
func (ch *ClickHouse) CreateTable(table Table, query string, dropTable, ignoreDependencies bool, onCluster string, version int) error {
	var err error
	if dropTable {
		if err = ch.DropTable(table, query, onCluster, ignoreDependencies, version); err != nil {
			return err
		}
	}

	if version > 19000000 && onCluster != "" && !onClusterRe.MatchString(query) {
		tryMatchReList := []*regexp.Regexp{attachViewToClauseRe, attachViewSelectRe, createViewToClauseRe, createViewSelectRe, createObjRe}
		for _, tryMatchRe := range tryMatchReList {
			if tryMatchRe.MatchString(query) {
				query = tryMatchRe.ReplaceAllString(query, "$1 ON CLUSTER '"+onCluster+"' $2")
				break
			}
		}
	}

	if !strings.Contains(query, table.Name) {
		return errors.New(fmt.Sprintf("schema query ```%s``` doesn't contains table name `%s`", query, table.Name))
	}

	// fix restore schema for legacy backup
	// see https://github.com/Altinity/clickhouse-backup/issues/268
	// https://github.com/Altinity/clickhouse-backup/issues/297
	// https://github.com/Altinity/clickhouse-backup/issues/331
	isOnlyTableWithQuotesPresent, err := regexp.Match(fmt.Sprintf("^CREATE [^(\\.]+ `%s`", table.Name), []byte(query))
	if err != nil {
		return err
	}
	isOnlyTableWithQuotesPresent = isOnlyTableWithQuotesPresent && !strings.Contains(query, fmt.Sprintf("`%s`.`%s`", table.Database, table.Name))

	isOnlyTablePresent, err := regexp.Match(fmt.Sprintf("^CREATE [^(\\.]+ %s", table.Name), []byte(query))
	if err != nil {
		return err
	}
	isOnlyTablePresent = isOnlyTablePresent && !strings.Contains(query, fmt.Sprintf("%s.%s", table.Database, table.Name))
	if isOnlyTableWithQuotesPresent && table.Database != "" {
		query = strings.Replace(query, fmt.Sprintf("`%s`", table.Name), fmt.Sprintf("`%s`.`%s`", table.Database, table.Name), 1)
	} else if isOnlyTablePresent && table.Database != "" {
		query = strings.Replace(query, fmt.Sprintf("%s", table.Name), fmt.Sprintf("%s.%s", table.Database, table.Name), 1)
	}

	// https://github.com/Altinity/clickhouse-backup/issues/574, replace ENGINE=Distributed to new cluster name
	if onCluster != "" && distributedRE.MatchString(query) {
		matches := distributedRE.FindAllStringSubmatch(query, -1)
		if onCluster != strings.Trim(matches[0][2], "'\" ") {
			log.Warn().Msgf("Will replace cluster ENGINE=Distributed %s -> %s", matches[0][2], onCluster)
			query = distributedRE.ReplaceAllString(query, fmt.Sprintf("${1}(%s,${3})", onCluster))
		}
	}

	if err := ch.Query(query); err != nil {
		return err
	}
	return nil
}

// GetConn - return current connection
func (ch *ClickHouse) GetConn() driver.Conn {
	return ch.conn
}

func (ch *ClickHouse) IsClickhouseShadow(path string) bool {
	d, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() {
		if err := d.Close(); err != nil {
			log.Warn().Msgf("can't close directory %v", err)
		}
	}()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return false
	}
	for _, name := range names {
		if name == "increment.txt" {
			continue
		}
		if _, err := strconv.Atoi(name); err != nil {
			return false
		}
	}
	return true
}

func (ch *ClickHouse) StructSelect(dest interface{}, query string, args ...interface{}) error {
	return ch.SelectContext(context.Background(), dest, query, args...)
}

func (ch *ClickHouse) QueryContext(ctx context.Context, query string, args ...interface{}) error {
	return ch.conn.Exec(ctx, ch.LogQuery(query, args...), args...)
}

func (ch *ClickHouse) Query(query string, args ...interface{}) error {
	return ch.conn.Exec(context.Background(), ch.LogQuery(query, args...), args...)
}

func (ch *ClickHouse) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return ch.conn.Select(ctx, dest, ch.LogQuery(query, args...), args...)
}

func (ch *ClickHouse) Select(dest interface{}, query string, args ...interface{}) error {
	return ch.conn.Select(context.Background(), dest, ch.LogQuery(query, args...), args...)
}

func (ch *ClickHouse) SelectSingleRow(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return ch.conn.QueryRow(ctx, ch.LogQuery(query, args...), args...).Scan(dest)
}

func (ch *ClickHouse) SelectSingleRowNoCtx(dest interface{}, query string, args ...interface{}) error {
	err := ch.conn.QueryRow(context.Background(), ch.LogQuery(query, args...), args...).Scan(dest)
	if err != nil && err == sql.ErrNoRows {
		return nil
	}
	return err
}

func (ch *ClickHouse) LogQuery(query string, args ...interface{}) string {
	var logF *zerolog.Event
	if !ch.Config.LogSQLQueries {
		logF = log.Debug()
	} else {
		logF = log.Info()
	}
	if len(args) > 0 {
		logF.Msg(strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(fmt.Sprintf("%s with args %v", query, args)))
	} else {
		logF.Msg(strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(query))
	}
	return query
}

func (ch *ClickHouse) IsAtomic(database string) (bool, error) {
	var isDatabaseAtomic string
	if err := ch.SelectSingleRowNoCtx(&isDatabaseAtomic, fmt.Sprintf("SELECT engine FROM system.databases WHERE name = '%s'", database)); err != nil {
		return false, err
	}
	return isDatabaseAtomic == "Atomic", nil
}

// GetAccessManagementPath @todo think about how to properly extract access_management_path from /etc/clickhouse-server/
func (ch *ClickHouse) GetAccessManagementPath(ctx context.Context, disks []Disk) (string, error) {
	accessPath := "/var/lib/clickhouse/access"
	rows := make([]struct {
		AccessPath string `ch:"access_path"`
	}, 0)
	if err := ch.SelectContext(ctx, &rows, "SELECT JSONExtractString(params,'path') AS access_path FROM system.user_directories WHERE type='local directory'"); err != nil || len(rows) == 0 {
		if disks == nil {
			disks, err = ch.GetDisks(ctx)
			if err != nil {
				return "", err
			}
		}
		for _, disk := range disks {
			if _, err := os.Stat(path.Join(disk.Path, "access")); !os.IsNotExist(err) {
				accessPath = path.Join(disk.Path, "access")
				break
			}
		}
	} else {
		accessPath = rows[0].AccessPath
	}
	return accessPath, nil
}

func (ch *ClickHouse) GetUserDefinedFunctions(ctx context.Context) ([]Function, error) {
	allFunctions := make([]Function, 0)
	allFunctionsSQL := "SELECT name, create_query FROM system.functions WHERE create_query!=''"
	var detectUDF uint64
	detectUDFSQL := "SELECT count() as cnt FROM system.columns WHERE database='system' AND table='functions' AND name='create_query' SETTINGS empty_result_for_aggregation_by_empty_set=0"
	if err := ch.SelectSingleRow(ctx, &detectUDF, detectUDFSQL); err != nil {
		return nil, err
	}
	if detectUDF == 0 {
		return allFunctions, nil
	}

	if err := ch.SelectContext(ctx, &allFunctions, allFunctionsSQL); err != nil {
		return nil, err
	}
	return allFunctions, nil
}

func (ch *ClickHouse) CreateUserDefinedFunction(name string, query string, cluster string) error {
	dropQuery := fmt.Sprintf("DROP FUNCTION IF EXISTS `%s`", name)
	if cluster != "" {
		dropQuery += fmt.Sprintf(" ON CLUSTER '%s'", cluster)
		query = strings.Replace(query, " AS ", fmt.Sprintf(" ON CLUSTER '%s' AS ", cluster), 1)
	}
	if err := ch.Query(dropQuery); err != nil {
		return err
	}
	return ch.Query(query)
}

func (ch *ClickHouse) CalculateMaxFileSize(ctx context.Context, cfg *config.Config) (int64, error) {
	var rows int64
	maxSizeQuery := "SELECT max(toInt64(bytes_on_disk * 1.02)) AS max_file_size FROM system.parts"
	if !cfg.General.UploadByPart {
		maxSizeQuery = "SELECT toInt64(max(data_by_disk) * 1.02) AS max_file_size FROM (SELECT disk_name, max(toInt64(bytes_on_disk)) data_by_disk FROM system.parts GROUP BY disk_name)"
	}
	if err := ch.SelectSingleRow(ctx, &rows, maxSizeQuery); err != nil {
		return 0, fmt.Errorf("can't calculate max(bytes_on_disk): %v", err)
	}
	return rows, nil
}

func (ch *ClickHouse) GetInProgressMutations(ctx context.Context, database string, table string) ([]metadata.MutationMetadata, error) {
	inProgressMutations := make([]metadata.MutationMetadata, 0)
	getInProgressMutationsQuery := "SELECT mutation_id, command FROM system.mutations WHERE is_done=0 AND database=? AND table=?"
	if err := ch.SelectContext(ctx, &inProgressMutations, getInProgressMutationsQuery, database, table); err != nil {
		return nil, fmt.Errorf("can't get in progress mutations: %v", err)
	}
	return inProgressMutations, nil
}

func (ch *ClickHouse) ApplyMacros(ctx context.Context, s string) (string, error) {
	var macrosExists uint64
	err := ch.SelectSingleRow(ctx, &macrosExists, "SELECT count() AS is_macros_exists FROM system.tables WHERE database='system' AND name='macros'  SETTINGS empty_result_for_aggregation_by_empty_set=0")
	if err != nil || macrosExists == 0 {
		return s, err
	}

	macros := make([]Macro, 0)
	err = ch.SelectContext(ctx, &macros, "SELECT macro, substitution FROM system.macros")
	if err != nil || len(macros) == 0 {
		return s, err
	}

	replaces := make([]string, len(macros)*2)
	for i, macro := range macros {
		replaces[i*2] = fmt.Sprintf("{%s}", macro.Macro)
		replaces[i*2+1] = fmt.Sprintf("%s", macro.Substitution)
	}
	s = strings.NewReplacer(replaces...).Replace(s)
	return s, nil
}

func (ch *ClickHouse) ApplyMutation(ctx context.Context, tableMetadata metadata.TableMetadata, mutation metadata.MutationMetadata) error {
	applyMutatoinSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` %s", tableMetadata.Database, tableMetadata.Table, mutation.Command)
	if err := ch.QueryContext(ctx, applyMutatoinSQL); err != nil {
		return err
	}
	return nil
}

// CheckReplicationInProgress allow to avoid concurrent ATTACH PART https://github.com/Altinity/clickhouse-backup/issues/474
func (ch *ClickHouse) CheckReplicationInProgress(table metadata.TableMetadata) (bool, error) {
	if ch.Config.CheckReplicasBeforeAttach && strings.Contains(table.Query, "Replicated") {
		existsReplicas := make([]struct {
			InProgress uint64 `ch:"replication_in_progress"`
		}, 0)
		if err := ch.Select(&existsReplicas, "SELECT sum(log_pointer + log_max_index + absolute_delay + queue_size)  AS replication_in_progress FROM system.replicas WHERE database=? and table=? SETTINGS empty_result_for_aggregation_by_empty_set=0", table.Database, table.Table); err != nil {
			return false, err
		}
		if len(existsReplicas) != 1 {
			return false, fmt.Errorf("invalid result for check exists replicas: %+v", existsReplicas)
		}
		if existsReplicas[0].InProgress > 0 {
			log.Warn().Msgf("%s.%s skipped cause system.replicas entry already exists and replication in progress from another replica", table.Database, table.Table)
			return false, nil
		} else {
			log.Info().Msgf("replication_in_progress status = %+v", existsReplicas)
		}
	}
	return true, nil
}

// CheckSystemPartsColumns check data parts types consistency https://github.com/Altinity/clickhouse-backup/issues/529#issuecomment-1554460504
func (ch *ClickHouse) CheckSystemPartsColumns(ctx context.Context, table *Table) error {
	if ch.isPartsColumnPresent == -1 {
		return nil
	}
	if ch.isPartsColumnPresent == 0 {
		isPartsColumn := make([]struct {
			Present uint64 `ch:"is_parts_column_present"`
		}, 0)
		if err := ch.SelectContext(ctx, &isPartsColumn, "SELECT count() is_parts_column_present FROM system.tables WHERE database='system' AND name='parts_columns'"); err != nil {
			return err
		}
		if len(isPartsColumn) != 1 || isPartsColumn[0].Present != 1 {
			ch.isPartsColumnPresent = -1
			return nil
		}
	}
	ch.isPartsColumnPresent = 1
	isPartsColumnsInconsistent := make([]struct {
		Column string   `ch:"column"`
		Types  []string `ch:"uniq_types"`
	}, 0)
	partsColumnsSQL := "SELECT column, groupUniqArray(type) AS uniq_types " +
		"FROM system.parts_columns " +
		"WHERE active AND database=? AND table=? " +
		"GROUP BY column HAVING length(uniq_types) > 1"
	if err := ch.SelectContext(ctx, &isPartsColumnsInconsistent, partsColumnsSQL, table.Database, table.Name); err != nil {
		return err
	}
	if len(isPartsColumnsInconsistent) > 0 {
		for i := range isPartsColumnsInconsistent {
			log.Error().Msgf("`%s`.`%s` have inconsistent data types %#v for \"%s\" column", table.Database, table.Name, isPartsColumnsInconsistent[i].Types, isPartsColumnsInconsistent[i].Column)
		}
		return fmt.Errorf("`%s`.`%s` have inconsistent data types for active data part in system.parts_columns", table.Database, table.Name)
	}
	return nil
}

func (ch *ClickHouse) CheckSettingsExists(ctx context.Context, settings map[string]bool) (map[string]bool, error) {
	isSettingsPresent := make([]struct {
		Name      string `ch:"name"`
		IsPresent uint64 `ch:"is_present"`
	}, 0)
	queryStr := "SELECT name, count(*) as is_present FROM system.settings WHERE name IN ("
	args := make([]interface{}, 0, len(settings))
	for k := range settings {
		queryStr += "?, "
		args = append(args, k)
	}
	queryStr = queryStr[:len(queryStr)-2]
	queryStr += ") GROUP BY name"
	if err := ch.SelectContext(ctx, &isSettingsPresent, queryStr, args...); err != nil {
		return nil, err
	}
	for _, item := range isSettingsPresent {
		settings[item.Name] = item.IsPresent > 0
	}
	return settings, nil
}
