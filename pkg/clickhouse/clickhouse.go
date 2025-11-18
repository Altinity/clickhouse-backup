package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/antchfx/xmlquery"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/pkg/errors"
	"github.com/ricochet2200/go-disk-usage/du"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ClickHouse - provide
type ClickHouse struct {
	Config              *config.ClickHouseConfig
	conn                driver.Conn
	version             int
	IsOpen              bool
	BreakConnectOnError bool
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
			"connect_timeout":      int(timeout.Seconds()),
			"receive_timeout":      int(timeout.Seconds()),
			"send_timeout":         int(timeout.Seconds()),
			"http_send_timeout":    300,
			"http_receive_timeout": 300,
		},
		MaxOpenConns:    ch.Config.MaxConnections,
		ConnMaxLifetime: 0, // don't change it, it related to SYSTEM SHUTDOWN behavior for properly rebuild RBAC lists on 20.4-22.3
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
	} else {
		opt.Settings["log_queries"] = 1
	}

	logLevel := zerolog.InfoLevel
	if !ch.Config.LogSQLQueries {
		logLevel = zerolog.DebugLevel
	}
	// infinite reconnect until success, fix https://github.com/Altinity/clickhouse-backup/issues/857
	for {
		for {
			ch.conn, err = clickhouse.Open(opt)
			if err == nil {
				break
			}
			if ch.BreakConnectOnError {
				return err
			}
			log.Warn().Msgf("clickhouse connection: %s, sql.Open return error: %v, will wait 5 second to reconnect", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port), err)
			time.Sleep(5 * time.Second)
		}
		log.WithLevel(logLevel).Msgf("clickhouse connection prepared: %s run ping", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port))
		err = ch.conn.Ping(context.Background())
		if err == nil {
			log.WithLevel(logLevel).Msgf("clickhouse connection success: %s", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port))
			ch.IsOpen = true
			break
		}
		if ch.BreakConnectOnError {
			return err
		}
		log.Warn().Msgf("clickhouse connection ping: %s return error: %v, will wait 5 second to reconnect", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port), err)
		time.Sleep(5 * time.Second)
	}

	return nil
}

// GetDisks - return data from system.disks table
func (ch *ClickHouse) GetDisks(ctx context.Context, enrich bool) ([]Disk, error) {
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
		// s3_plain disk could contain relative remote disks path, need transform it to `/var/lib/clickhouse/disks/disk_name`
		if disks[i].Path != "" && !strings.HasPrefix(disks[i].Path, "/") {
			for _, d := range disks {
				if d.Name == "default" {
					disks[i].Path = path.Join(d.Path, "disks", disks[i].Name) + "/"
					break
				}
			}
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
	// https://github.com/Altinity/clickhouse-backup/issues/676#issuecomment-1606547960
	if enrich {
		for k, v := range dm {
			disks = append(disks, Disk{
				Name: k,
				Path: v,
				Type: "local",
			})
		}
	}
	return disks, nil
}

func (ch *ClickHouse) GetEmbeddedBackupPath(disks []Disk) (string, error) {
	if !ch.Config.UseEmbeddedBackupRestore || ch.Config.EmbeddedBackupDisk == "" {
		return "", nil
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
			Name:            "default",
			Path:            path.Join("/", clickhouseData),
			Type:            "local",
			FreeSpace:       du.NewDiskUsage(path.Join("/", clickhouseData)).Free(),
			StoragePolicies: []string{"default"},
		}}, nil
	}
}

func (ch *ClickHouse) getMetadataPath(ctx context.Context) (string, error) {
	var result []struct {
		MetadataPath string `ch:"metadata_path"`
	}
	query := "SELECT metadata_path FROM system.tables WHERE database = 'system' AND metadata_path!='' LIMIT 1"
	// https://github.com/ClickHouse/ClickHouse/issues/76546
	if ch.version >= 25000000 {
		query = "SELECT data_path AS metadata_path FROM system.databases WHERE name = 'system' LIMIT 1"
	}
	if err := ch.SelectContext(ctx, &result, query); err != nil {
		return "", err
	}
	if len(result) == 0 {
		return "", fmt.Errorf("can't get metadata_path from system.tables or system.databases")
	}
	metadataPath := strings.Split(result[0].MetadataPath, "/")
	// https://github.com/ClickHouse/ClickHouse/issues/76546
	if ch.version >= 25000000 && strings.HasSuffix(result[0].MetadataPath, "/store/") {
		result[0].MetadataPath = path.Join(metadataPath[:len(metadataPath)-2]...)
		result[0].MetadataPath = path.Join(result[0].MetadataPath, "metadata")
	} else if strings.Contains(result[0].MetadataPath, "/store/") {
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
		type DiskFields struct {
			DiskTypePresent          uint64 `ch:"is_disk_type_present"`
			ObjectStorageTypePresent uint64 `ch:"is_object_storage_type_present"`
			FreeSpacePresent         uint64 `ch:"is_free_space_present"`
			StoragePolicyPresent     uint64 `ch:"is_storage_policy_present"`
		}
		diskFields := make([]DiskFields, 0)
		if err := ch.SelectContext(ctx, &diskFields,
			"SELECT countIf(name='type') AS is_disk_type_present, "+
				"countIf(name='object_storage_type') AS is_object_storage_type_present, "+
				"countIf(name='free_space') AS is_free_space_present, "+
				"countIf(name='disks') AS is_storage_policy_present "+
				"FROM system.columns WHERE database='system' AND table IN ('disks','storage_policies') ",
		); err != nil {
			return nil, err
		}
		diskTypeSQL := "'local'"
		if len(diskFields) > 0 && diskFields[0].DiskTypePresent > 0 {
			diskTypeSQL = "any(d.type)"
		}
		if len(diskFields) > 0 && diskFields[0].ObjectStorageTypePresent > 0 {
			diskTypeSQL = "any(lower(if(d.type='ObjectStorage',d.object_storage_type,d.type)))"
		}

		diskFreeSpaceSQL := "toUInt64(0)"
		if len(diskFields) > 0 && diskFields[0].FreeSpacePresent > 0 {
			diskFreeSpaceSQL = "min(d.free_space)"
		}
		storagePoliciesSQL := "['default']"
		joinStoragePoliciesSQL := ""
		if len(diskFields) > 0 && diskFields[0].StoragePolicyPresent > 0 {
			storagePoliciesSQL = "groupUniqArray(s.policy_name)"
			// LEFT JOIN to allow disks which not have policy, https://github.com/Altinity/clickhouse-backup/issues/845
			joinStoragePoliciesSQL = " LEFT JOIN "
			joinStoragePoliciesSQL += "(SELECT policy_name, arrayJoin(disks) AS disk FROM system.storage_policies) AS s ON s.disk = d.name"
		}
		var result []Disk
		query := fmt.Sprintf(
			"SELECT d.path AS path, any(d.name) AS name, %s AS type, %s AS free_space, %s AS storage_policies "+
				"FROM system.disks AS d %s GROUP BY d.path",
			diskTypeSQL, diskFreeSpaceSQL, storagePoliciesSQL, joinStoragePoliciesSQL,
		)
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
	// MaterializedPostgreSQL doesn't support FREEZE look https://github.com/Altinity/clickhouse-backup/issues/550 and https://github.com/ClickHouse/ClickHouse/issues/32902
	if err = ch.SelectContext(ctx, &skipDatabases, "SELECT name FROM system.databases WHERE engine IN ('MySQL','PostgreSQL','MaterializedPostgreSQL')"); err != nil {
		return nil, err
	}
	skipDatabaseNames := make([]string, len(skipDatabases))
	for i, s := range skipDatabases {
		skipDatabaseNames[i] = s.Name
	}
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
		return nil, err
	}

	allTablesSQL := ch.prepareGetTablesSQL(tablePattern, skipDatabaseNames, ch.Config.SkipTableEngines, settings, isSystemTablesFieldPresent)
	tables := make([]Table, 0)
	if err = ch.SelectContext(ctx, &tables, allTablesSQL); err != nil {
		return nil, err
	}
	for i := range tables {
		// https://github.com/Altinity/clickhouse-backup/issues/1091, https://github.com/Altinity/clickhouse-backup/issues/1151
		escapeReplacer := strings.NewReplacer(`\`, `\\`)
		escapeDb := escapeReplacer.Replace(tables[i].Database)
		escapeTable := escapeReplacer.Replace(tables[i].Name)
		tables[i].CreateTableQuery = strings.Replace(tables[i].CreateTableQuery, escapeDb, tables[i].Database, 1)
		tables[i].CreateTableQuery = strings.Replace(tables[i].CreateTableQuery, escapeTable, tables[i].Name, 1)
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
		for _, engine := range ch.Config.SkipTableEngines {
			if t.Engine == engine {
				t.Skip = true
				break
			}
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
			tables[i].TotalBytes = ch.getTableSizeFromParts(ctx, tables[i])
		}
	}
	// https://github.com/Altinity/clickhouse-backup/issues/613
	if !ch.Config.UseEmbeddedBackupRestore {
		if tables, err = ch.enrichTablesByInnerDependencies(ctx, tables, metadataPath, settings, isSystemTablesFieldPresent); err != nil {
			return nil, err
		}
	}
	return tables, nil
}

// https://github.com/Altinity/clickhouse-backup/issues/613
func (ch *ClickHouse) enrichTablesByInnerDependencies(ctx context.Context, tables []Table, metadataPath string, settings map[string]bool, isSystemTablesFieldPresent []IsSystemTablesFieldPresent) ([]Table, error) {
	innerTablesMissed := make([]string, 0)
	for _, t := range tables {
		if !t.Skip && (strings.HasPrefix(t.CreateTableQuery, "ATTACH MATERIALIZED") || strings.HasPrefix(t.CreateTableQuery, "CREATE MATERIALIZED")) {
			if strings.Contains(t.CreateTableQuery, " TO ") && !strings.Contains(t.CreateTableQuery, " TO INNER UUID") {
				continue
			}
			found := false
			for j := range tables {
				if tables[j].Database == t.Database && (tables[j].Name == ".inner."+t.Name || tables[j].Name == ".inner_id."+t.UUID) {
					found = true
					break
				}
			}
			if !found {
				missedInnerTableName := ".inner." + t.Name
				if t.UUID != "" {
					missedInnerTableName = ".inner_id." + t.UUID
				}
				innerTablesMissed = append(innerTablesMissed, fmt.Sprintf("%s.%s", t.Database, missedInnerTableName))
			}
		}
	}
	if len(innerTablesMissed) == 0 {
		return tables, nil
	}

	missedTablesSQL := ch.prepareGetTablesSQL(strings.Join(innerTablesMissed, ","), nil, nil, settings, isSystemTablesFieldPresent)
	missedTables := make([]Table, 0)
	var err error
	if err = ch.SelectContext(ctx, &missedTables, missedTablesSQL); err != nil {
		return nil, err
	}
	if len(missedTables) == 0 {
		return tables, nil
	}
	for i, t := range missedTables {
		missedTables[i] = ch.fixVariousVersions(ctx, t, metadataPath)
		if t.TotalBytes == 0 && !t.Skip && strings.HasSuffix(t.Engine, "Tree") {
			missedTables[i].TotalBytes = ch.getTableSizeFromParts(ctx, missedTables[i])
		}
	}
	return append(missedTables, tables...), nil
}

func (ch *ClickHouse) prepareGetTablesSQL(tablePattern string, skipDatabases, skipTableEngines []string, settings map[string]bool, isSystemTablesFieldPresent []IsSystemTablesFieldPresent) string {
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
		// https://github.com/Altinity/clickhouse-backup/issues/1091
		replacer := strings.NewReplacer(`\`, `\\\`, ".", "\\.", "$", ".", ",", "$|^", "*", ".*", "?", ".", " ", "", "`", "", `"`, "", "-", "\\-")
		allTablesSQL += fmt.Sprintf(" AND match(concat(database,'.',name),'^%s$') ", replacer.Replace(tablePattern))
	}
	if len(skipDatabases) > 0 {
		allTablesSQL += fmt.Sprintf(" AND database NOT IN ('%s')", strings.Join(skipDatabases, "','"))
	}
	if len(skipTableEngines) > 0 {
		allTablesSQL += fmt.Sprintf(" AND NOT has(arrayMap(x->lower(x), ['%s']), lower(engine))", strings.Join(skipTableEngines, "','"))
	}
	// try to upload big tables first
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsTotalBytesPresent > 0 {
		allTablesSQL += " ORDER BY total_bytes DESC"
	}
	allTablesSQL = ch.addSettingsSQL(allTablesSQL, settings)
	return allTablesSQL
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

var tableNameSuffixRE = regexp.MustCompile("\\.\"[^\"]+\"$|\\.`[^`]+`$|\\.[a-zA-Z0-9\\-_]+$")

// GetDatabases - return slice of all non system databases for backup
func (ch *ClickHouse) GetDatabases(ctx context.Context, cfg *config.Config, tablePattern string) ([]Database, error) {
	allDatabases := make([]Database, 0)
	skipDatabases := []string{"system", "INFORMATION_SCHEMA", "information_schema", "_temporary_and_external_tables"}
	bypassDatabases := make([]string, 0)
	skipTablesPatterns := make([]string, 0)
	bypassTablesPatterns := make([]string, 0)
	if len(cfg.ClickHouse.SkipTables) > 0 {
		skipTablesPatterns = append(skipTablesPatterns, cfg.ClickHouse.SkipTables...)
	}
	if tablePattern != "" {
		bypassTablesPatterns = append(bypassTablesPatterns, strings.Split(tablePattern, ",")...)
	}
	for _, pattern := range skipTablesPatterns {
		pattern = strings.Trim(pattern, " \r\t\n")
		if strings.HasSuffix(pattern, ".*") {
			skipDatabases = common.AddStringToSliceIfNotExists(skipDatabases, strings.Trim(strings.TrimSuffix(pattern, ".*"), "\"` "))
		} else {
			skipDatabases = common.AddStringToSliceIfNotExists(skipDatabases, strings.Trim(tableNameSuffixRE.ReplaceAllString(pattern, ""), "\"` "))
		}
	}
	for _, pattern := range bypassTablesPatterns {
		pattern = strings.Trim(pattern, " \r\t\n")
		if strings.HasSuffix(pattern, ".*") {
			bypassDatabases = common.AddStringToSliceIfNotExists(bypassDatabases, strings.Trim(strings.TrimSuffix(pattern, ".*"), "\"` "))
		} else {
			bypassDatabases = common.AddStringToSliceIfNotExists(bypassDatabases, strings.Trim(tableNameSuffixRE.ReplaceAllString(pattern, ""), "\"` "))
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
		fileMatchToRE := strings.NewReplacer("*", ".*", "?", ".", "(", "\\(", ")", "\\)", "[", "\\[", "]", "\\]", "$", "\\$", "^", "\\^")
		if len(bypassDatabases) > 0 {
			allDatabasesSQL := fmt.Sprintf(
				"SELECT name, engine FROM system.databases WHERE NOT match(name,'^(%s)$') AND match(name,'^(%s)$')",
				fileMatchToRE.Replace(strings.Join(skipDatabases, "|")), fileMatchToRE.Replace(strings.Join(bypassDatabases, "|")),
			)
			if err := ch.StructSelect(&allDatabases, allDatabasesSQL); err != nil {
				return nil, err
			}
		} else {
			allDatabasesSQL := fmt.Sprintf(
				"SELECT name, engine FROM system.databases WHERE NOT match(name,'^(%s)$')",
				fileMatchToRE.Replace(strings.Join(skipDatabases, "|")),
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
	// version 1.1.54394 has no query column
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
	query := "SELECT value FROM `system`.`build_options` WHERE name='VERSION_DESCRIBE'"
	if err := ch.SelectSingleRow(ctx, &result, query); err != nil {
		return ""
	}
	return result
}

// FreezeTableByParts - freeze all partitions in table one by one
// also ally `freeze_by_part_where`
func (ch *ClickHouse) FreezeTableByParts(ctx context.Context, table *Table, name string) error {
	var partitions []struct {
		PartitionID string `ch:"partition_id"`
	}
	q := fmt.Sprintf("SELECT DISTINCT partition_id FROM `system`.`parts` WHERE database='%s' AND table='%s' %s", table.Database, table.Name, ch.Config.FreezeByPartWhere)
	if err := ch.SelectContext(ctx, &partitions, q); err != nil {
		return errors.Wrapf(err, "can't get partitions for '%s.%s'", table.Database, table.Name)
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
				return errors.Wrapf(err, "can't freeze partition '%s'", item.PartitionID)
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
		return ch.FreezeTableByParts(ctx, table, name)
	}
	withNameQuery := ""
	if name != "" {
		withNameQuery = fmt.Sprintf("WITH NAME '%s'", name)
	}
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE %s;", table.Database, table.Name, withNameQuery)
	if err := ch.QueryContext(ctx, query); err != nil {
		if (strings.Contains(err.Error(), "code: 60") || strings.Contains(err.Error(), "code: 81") || strings.Contains(err.Error(), "code: 218")) && ch.Config.IgnoreNotExistsErrorDuringFreeze {
			log.Warn().Msgf("can't freeze table: %v", err)
			return nil
		}
		return errors.Wrap(err, "can't freeze table")
	}
	return nil
}

// AttachDataParts - execute ALTER TABLE ... ATTACH PART command for specific table
func (ch *ClickHouse) AttachDataParts(table metadata.TableMetadata, dstTable Table) error {
	if dstTable.Database != "" && dstTable.Database != table.Database {
		table.Database = dstTable.Database
	}
	if dstTable.Name != "" && dstTable.Name != table.Table {
		table.Table = dstTable.Name
	}
	canContinue, err := ch.CheckReplicationInProgress(table)
	if err != nil {
		return err
	}
	if !canContinue {
		return nil
	}
	for disk := range table.Parts {
		// https://github.com/ClickHouse/ClickHouse/issues/71009
		metadata.SortPartsByMinBlock(table.Parts[disk])
		for _, part := range table.Parts[disk] {
			if !strings.HasSuffix(part.Name, ".proj") {
				query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PART '%s'", table.Database, table.Table, part.Name)
				if err := ch.Query(query); err != nil {
					return err
				}
				log.Debug().Str("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).Str("disk", disk).Str("part", part.Name).Msg("attached")
			}
		}
	}
	return nil
}

var replicatedMergeTreeRE = regexp.MustCompile(`Replicated[\w_]*MergeTree\s*\(((\s*'[^']+'\s*),(\s*'[^']+')(\s*,\s*|.*?)([^)]*)|)\)`)
var uuidRE = regexp.MustCompile(`UUID '([^']+)'`)

// AttachTable - execute ATTACH TABLE  command for specific table
func (ch *ClickHouse) AttachTable(ctx context.Context, table metadata.TableMetadata, dstTable Table) error {
	if len(table.Parts) == 0 {
		log.Warn().Msgf("no data parts for restore for `%s`.`%s`", table.Database, table.Table)
		return nil
	}
	if dstTable.Database != "" && dstTable.Database != table.Database {
		table.Database = dstTable.Database
	}
	if dstTable.Name != "" && dstTable.Name != table.Table {
		table.Table = dstTable.Name
	}
	canContinue, err := ch.CheckReplicationInProgress(table)
	if err != nil {
		return errors.Wrap(err, "ch.CheckReplicationInProgress error")
	}
	if !canContinue {
		return nil
	}

	if ch.version <= 21003000 {
		return fmt.Errorf("your clickhouse-server version doesn't support SYSTEM RESTORE REPLICA statement, use `restore_as_attach: false` in config")
	}
	query := fmt.Sprintf("DETACH TABLE `%s`.`%s` SYNC", table.Database, table.Table)
	if err := ch.Query(query); err != nil {
		return errors.Wrapf(err, "%s error", query)
	}
	replicatedMatches := replicatedMergeTreeRE.FindStringSubmatch(table.Query)
	if len(replicatedMatches) > 0 {
		zkPath := strings.Trim(replicatedMatches[2], "' \r\n\t")
		replicaName := strings.Trim(replicatedMatches[3], "' \r\n\t")
		if strings.Contains(zkPath, "{uuid}") {
			if uuidMatches := uuidRE.FindStringSubmatch(table.Query); len(uuidMatches) > 0 {
				zkPath = strings.Replace(zkPath, "{uuid}", uuidMatches[1], 1)
			}
		}
		zkPath = strings.NewReplacer("{database}", table.Database, "{table}", table.Table).Replace(zkPath)
		zkPath, err = ch.ApplyMacros(ctx, zkPath)
		if err != nil {
			return errors.Wrap(err, "ch.ApplyMacros(ctx, zkPath) error")
		}
		replicaName, err = ch.ApplyMacros(ctx, replicaName)
		if err != nil {
			return errors.Wrap(err, "ch.ApplyMacros(ctx, replicaName) error")
		}
		query = fmt.Sprintf("SYSTEM DROP REPLICA '%s' FROM ZKPATH '%s'", replicaName, zkPath)
		if err := ch.Query(query); err != nil {
			return errors.Wrapf(err, "%s error", query)
		}
	}
	query = fmt.Sprintf("ATTACH TABLE `%s`.`%s`", table.Database, table.Table)
	if err := ch.Query(query); err != nil {
		return errors.Wrapf(err, "%s error", query)
	}

	if len(replicatedMatches) > 0 {
		query = fmt.Sprintf("SYSTEM RESTORE REPLICA `%s`.`%s`", table.Database, table.Table)
		if err := ch.Query(query); err != nil {
			return errors.Wrapf(err, "%s error", query)
		}
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

func (ch *ClickHouse) CreateDatabaseWithEngine(database, engine, cluster string, version int) error {
	if version <= 24004000 {
		engine = strings.Replace(engine, "{database}", database, -1)
	}
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

// DropOrDetachTable - drop ClickHouse table
func (ch *ClickHouse) DropOrDetachTable(table Table, query, onCluster string, ignoreDependencies bool, version int, defaultDataPath string, useDetach bool, databaseEngine string) error {
	var isAtomicOrReplicated bool
	var err error
	if databaseEngine == "" {
		if databaseEngine, err = ch.GetDatabaseEngine(table.Database); err != nil {
			return err
		}
	}
	isAtomicOrReplicated = strings.HasPrefix(databaseEngine, "Atomic") || strings.HasPrefix(databaseEngine, "Replicated")
	kind := "TABLE"
	if strings.HasPrefix(query, "CREATE DICTIONARY") {
		kind = "DICTIONARY"
	}

	action := "DROP"
	if useDetach {
		action = "DETACH"
	}

	dropQuery := fmt.Sprintf("%s %s IF EXISTS `%s`.`%s`", action, kind, table.Database, table.Name)
	if version > 19000000 && onCluster != "" {
		// https://github.com/Altinity/clickhouse-backup/issues/1127
		if !strings.HasPrefix(databaseEngine, "Replicated") {
			dropQuery += " ON CLUSTER '" + onCluster + "' "
		} else {
			log.Warn().Msgf("can't drop or detach table `%s`.`%s` on cluster for database engine=Replicated, will drop only in current host", table.Database, table.Name)
		}
	}
	if isAtomicOrReplicated {
		dropQuery += " NO DELAY"
	}
	if ignoreDependencies {
		dropQuery += " SETTINGS check_table_dependencies=0"
	}
	if defaultDataPath != "" && !useDetach {
		if _, err = os.Create(path.Join(defaultDataPath, "/flags/force_drop_table")); err != nil {
			return err
		}
	}
	if err = ch.Query(dropQuery); err != nil {
		return err
	}
	return nil
}

var createViewRefreshRe = regexp.MustCompile(`(?im)^(CREATE[\s\w]+VIEW[^(]+)(\s+REFRESH\s+.+)`)
var createViewToClauseRe = regexp.MustCompile(`(?im)^(CREATE[\s\w]+VIEW[^(]+)(\s+TO\s+.+)`)
var createViewAsWithRe = regexp.MustCompile(`(?im)^(CREATE[\s\w]+VIEW[^(]+)(\s+AS\s+WITH\s+.+)`)
var createViewRe = regexp.MustCompile(`(?im)^(CREATE[\s\w]+VIEW[^(]+)(\s+AS\s+SELECT.+)`)
var attachViewRefreshRe = regexp.MustCompile(`(?im)^(ATTACH[\s\w]+VIEW[^(]+)(\s+REFRESH\s+.+)`)
var attachViewToClauseRe = regexp.MustCompile(`(?im)^(ATTACH[\s\w]+VIEW[^(]+)(\s+TO\s+.+)`)
var attachViewAsWithRe = regexp.MustCompile(`(?im)^(ATTACH[\s\w]+VIEW[^(]+)(\s+AS\s+WITH\s+.+)`)
var attachViewRe = regexp.MustCompile(`(?im)^(ATTACH[\s\w]+VIEW[^(]+)(\s+AS\s+.+)`)
var createObjRe = regexp.MustCompile(`(?is)^(CREATE [^(]+)(\(.+)`)
var onClusterRe = regexp.MustCompile(`(?im)\s+ON\s+CLUSTER\s+`)
var distributedRE = regexp.MustCompile(`(Distributed)\(([^,]+),([^)]+)\)`)
var macroRE = regexp.MustCompile(`(?i){[a-z0-9-_]+}`)

// CreateTable - create ClickHouse table
func (ch *ClickHouse) CreateTable(table Table, query string, dropTable, ignoreDependencies bool, onCluster string, version int, defaultDataPath string, asAttach bool, databaseEngine string) error {
	var err error
	// https://github.com/Altinity/clickhouse-backup/issues/868
	if asAttach && onCluster != "" {
		return fmt.Errorf("can't apply `--restore-schema-as-attach` and config `restore_schema_on_cluster` together")
	}
	if dropTable {
		if err = ch.DropOrDetachTable(table, query, onCluster, ignoreDependencies, version, defaultDataPath, asAttach, databaseEngine); err != nil {
			return err
		}
	}
	// https://github.com/Altinity/clickhouse-backup/issues/868
	// For asAttach mode, use ATTACH query directly and write metadata SQL file
	if asAttach {
		return ch.CreateTableAsAttach(query)
	}

	query = ch.enrichQueryWithOnCluster(query, onCluster, version, databaseEngine)

	if !strings.Contains(query, table.Name) {
		return errors.New(fmt.Sprintf("schema query ```%s``` doesn't contains table name `%s`", query, table.Name))
	}

	// fix schema for restore
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
	// https://github.com/Altinity/clickhouse-backup/issues/1252, if cluster don't exist, replace it to RESTORE_SCHEMA_ON_CLUSTER or CLICKHOUSE_RESTORE_DISTRIBUTED_CLUSTER
	if distributedRE.MatchString(query) {
		matches := distributedRE.FindAllStringSubmatch(query, -1)
		oldCluster := strings.Trim(matches[0][2], "'\" ")
		newCluster := onCluster
		var existCluster string
		if newCluster == "" && !macroRE.MatchString(oldCluster) {
			if err = ch.SelectSingleRowNoCtx(&existCluster, "SELECT cluster FROM system.clusters WHERE cluster=?", oldCluster); err != nil {
				return errors.Wrapf(err, "check system.clusters for %s return error", oldCluster)
			}
			if existCluster == "" {
				newCluster = strings.Trim(ch.Config.RestoreDistributedCluster, "'\" ")
			}
		}
		if newCluster != "" && existCluster == "" && newCluster != oldCluster && !macroRE.MatchString(oldCluster) {
			newCluster = "'" + strings.Trim(newCluster, "'\" ") + "'"
			log.Warn().Msgf("will replace cluster ENGINE=Distributed %s -> %s", matches[0][2], newCluster)
			query = distributedRE.ReplaceAllString(query, fmt.Sprintf("${1}(%s,${3})", newCluster))
		}
	}

	// https://github.com/Altinity/clickhouse-backup/issues/1127
	query, err = ch.cleanUUIDForReplicatedDatabase(table, query, databaseEngine)
	if err != nil {
		return errors.Wrap(err, "ch.cleanUUIDForReplicatedDatabase return error")
	}

	// WINDOW VIEW unavailable after 24.3
	allowExperimentalAnalyzer := ""
	if allowExperimentalAnalyzer, err = ch.TurnAnalyzerOffIfNecessary(version, query, allowExperimentalAnalyzer); err != nil {
		return err
	}
	// MATERIALIZED VIEW ... REFRESH shall be restored as EMPTY to avoid data inconsistency
	// https://github.com/Altinity/clickhouse-backup/issues/1237
	// https://github.com/Altinity/clickhouse-backup/issues/1271
	if (strings.HasPrefix(query, "CREATE MATERIALIZED VIEW") || strings.HasPrefix(query, "ATTACH MATERIALIZED VIEW")) && strings.Contains(query, " REFRESH ") && !strings.Contains(query, " EMPTY ") {
		query = strings.Replace(query, "DEFINER", "EMPTY DEFINER", 1)
	}
	// CREATE
	if err := ch.Query(query); err != nil {
		return err
	}

	// WINDOW VIEW unavailable after 24.3
	if err = ch.TurnAnalyzerOnIfNecessary(version, query, allowExperimentalAnalyzer); err != nil {
		return err
	}
	return nil
}

func (ch *ClickHouse) cleanUUIDForReplicatedDatabase(table Table, query string, databaseEngine string) (string, error) {
	if strings.HasPrefix(query, "ATTACH MATERIALIZED VIEW") {
		return query, nil
	}
	if strings.HasPrefix(databaseEngine, "Replicated") && uuidRE.MatchString(query) {
		uuidAllowExplicit := ""
		if settingsErr := ch.SelectSingleRowNoCtx(&uuidAllowExplicit, "SELECT value FROM system.settings WHERE name='database_replicated_allow_explicit_uuid'"); settingsErr != nil {
			return "", settingsErr
		}
		if uuidAllowExplicit == "0" || uuidAllowExplicit == "" {
			uuidReplaced := false
			query = uuidRE.ReplaceAllStringFunc(query, func(match string) string {
				if !uuidReplaced {
					uuidReplaced = true
					log.Warn().Msgf("database `%s` engine=%s doesn't allow use UUID explicitly, check `database_replicated_allow_explicit_uuid` in system.settings", table.Database, databaseEngine)
					return ""
				}
				return match
			})
		}
	}
	return query, nil
}

func (ch *ClickHouse) CreateTableAsAttach(query string) error {
	attachQuery := strings.Replace(query, "CREATE", "ATTACH", 1)
	if attachErr := ch.Query(attachQuery); attachErr != nil {
		return errors.Wrap(attachErr, "createTable attach query error")
	}
	return nil
}

func (ch *ClickHouse) enrichQueryWithOnCluster(query string, onCluster string, version int, databaseEngine string) string {
	if version > 19000000 && !strings.HasPrefix(databaseEngine, "Replicated") && onCluster != "" && !onClusterRe.MatchString(query) {
		tryMatchReList := []*regexp.Regexp{attachViewRefreshRe, attachViewToClauseRe, attachViewAsWithRe, attachViewRe, createViewRefreshRe, createViewToClauseRe, createViewAsWithRe, createViewRe, createObjRe}
		for _, tryMatchRe := range tryMatchReList {
			if tryMatchRe.MatchString(query) {
				query = tryMatchRe.ReplaceAllString(query, "$1 ON CLUSTER '"+onCluster+"' $2")
				break
			}
		}
	}
	return query
}

func (ch *ClickHouse) TurnAnalyzerOnIfNecessary(version int, query string, allowExperimentalAnalyzer string) error {
	if version > 24003000 && (strings.HasPrefix(query, "CREATE LIVE VIEW") || strings.HasPrefix(query, "ATTACH LIVE VIEW") || strings.HasPrefix(query, "CREATE WINDOW VIEW") || strings.HasPrefix(query, "ATTACH WINDOW VIEW")) && allowExperimentalAnalyzer == "1" {
		if err := ch.Query("SET allow_experimental_analyzer=1"); err != nil {
			return err
		}
	}
	return nil
}

func (ch *ClickHouse) TurnAnalyzerOffIfNecessary(version int, query string, allowExperimentalAnalyzer string) (string, error) {
	if version > 24003000 && (strings.HasPrefix(query, "CREATE LIVE VIEW") || strings.HasPrefix(query, "ATTACH LIVE VIEW") || strings.HasPrefix(query, "CREATE WINDOW VIEW") || strings.HasPrefix(query, "ATTACH WINDOW VIEW")) {
		if err := ch.SelectSingleRowNoCtx(&allowExperimentalAnalyzer, "SELECT value FROM system.settings WHERE name='allow_experimental_analyzer'"); err != nil {
			return "", err
		}
		if allowExperimentalAnalyzer == "1" {
			if err := ch.Query("SET allow_experimental_analyzer=0"); err != nil {
				return "", err
			}
		}
		return allowExperimentalAnalyzer, nil
	}
	return "", nil
}

// GetConn - return current connection
func (ch *ClickHouse) GetConn() driver.Conn {
	return ch.conn
}

func (ch *ClickHouse) StructSelect(dest interface{}, query string, args ...interface{}) error {
	return ch.SelectContext(context.Background(), dest, query, args...)
}

func (ch *ClickHouse) QueryContext(ctx context.Context, query string, args ...interface{}) error {
	return errors.WithStack(ch.conn.Exec(ctx, ch.LogQuery(query, args...), args...))
}

func (ch *ClickHouse) Query(query string, args ...interface{}) error {
	return errors.WithStack(ch.conn.Exec(context.Background(), ch.LogQuery(query, args...), args...))
}

func (ch *ClickHouse) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return errors.WithStack(ch.conn.Select(ctx, dest, ch.LogQuery(query, args...), args...))
}

func (ch *ClickHouse) Select(dest interface{}, query string, args ...interface{}) error {
	return errors.WithStack(ch.conn.Select(context.Background(), dest, ch.LogQuery(query, args...), args...))
}

func (ch *ClickHouse) SelectSingleRow(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	row := ch.conn.QueryRow(ctx, ch.LogQuery(query, args...), args...)
	if row.Err() != nil {
		return errors.WithStack(row.Err())
	}
	return errors.WithStack(row.Scan(dest))
}

func (ch *ClickHouse) SelectSingleRowNoCtx(dest interface{}, query string, args ...interface{}) error {
	row := ch.conn.QueryRow(context.Background(), ch.LogQuery(query, args...), args...)
	if row.Err() != nil {
		return errors.WithStack(row.Err())
	}
	err := row.Scan(dest)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return errors.WithStack(err)
}

func (ch *ClickHouse) LogQuery(query string, args ...interface{}) string {
	level := zerolog.InfoLevel
	if !ch.Config.LogSQLQueries {
		level = zerolog.DebugLevel
	}
	if len(args) > 0 {
		log.WithLevel(level).Msg(strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(fmt.Sprintf("%s with args %#v", query, args)))
	} else {
		log.WithLevel(level).Msg(strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(query))
	}
	return query
}

func (ch *ClickHouse) IsDbAtomicOrReplicated(database string) (bool, error) {
	dbEngine, err := ch.GetDatabaseEngine(database)
	if err != nil {
		return false, err
	}
	return dbEngine == "Atomic" || dbEngine == "Replicated", nil
}

func (ch *ClickHouse) GetDatabaseEngine(database string) (string, error) {
	var dbEngine string
	if err := ch.SelectSingleRowNoCtx(&dbEngine, fmt.Sprintf("SELECT engine FROM system.databases WHERE name = '%s'", database)); err != nil {
		return "", err
	}
	return dbEngine, nil
}

// GetAccessManagementPath extract path from following sources system.user_directories, access_control_path from /var/lib/clickhouse/preprocessed_configs/config.xml, system.disks
func (ch *ClickHouse) GetAccessManagementPath(ctx context.Context, disks []Disk) (string, error) {
	accessPath := "/var/lib/clickhouse/access"
	rows := make([]struct {
		AccessPath string `ch:"access_path"`
	}, 0)
	if err := ch.SelectContext(ctx, &rows, "SELECT JSONExtractString(params,'path') AS access_path FROM system.user_directories WHERE type in ('local_directory','local directory')"); err != nil || len(rows) == 0 {
		configFile, doc, err := ch.ParseXML(ctx, "config.xml")
		if err != nil {
			log.Warn().Msgf("can't parse config.xml from %s, error: %v", configFile, err)
		}
		if err == nil {
			accessControlPathNode := doc.SelectElement("access_control_path")
			if accessControlPathNode != nil {
				return accessControlPathNode.InnerText(), nil
			}
		}

		if disks == nil {
			disks, err = ch.GetDisks(ctx, false)
			if err != nil {
				return "", err
			}
		}
		for _, disk := range disks {
			if fInfo, err := os.Stat(path.Join(disk.Path, "access")); err == nil && fInfo.IsDir() {
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
	for i := range allFunctions {
		allFunctions[i].CreateQuery = strings.Replace(allFunctions[i].CreateQuery, "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
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
	maxSizeQuery := "SELECT max(toInt64(bytes_on_disk * 1.02)) AS max_file_size FROM system.parts WHERE active"
	if !cfg.General.UploadByPart {
		maxSizeQuery = "SELECT toInt64(max(data_by_disk) * 1.02) AS max_file_size FROM (SELECT disk_name, max(toInt64(bytes_on_disk)) data_by_disk FROM system.parts WHERE active GROUP BY disk_name)"
	}
	maxSizeQuery += " SETTINGS empty_result_for_aggregation_by_empty_set=0"
	if err := ch.SelectSingleRow(ctx, &rows, maxSizeQuery); err != nil {
		return 0, errors.Wrap(err, "can't calculate max(bytes_on_disk)")
	}
	return rows, nil
}

func (ch *ClickHouse) GetInProgressMutations(ctx context.Context, database string, table string) ([]metadata.MutationMetadata, error) {
	inProgressMutations := make([]metadata.MutationMetadata, 0)
	getInProgressMutationsQuery := "SELECT mutation_id, command FROM system.mutations WHERE is_done=0 AND database=? AND table=?"
	if err := ch.SelectContext(ctx, &inProgressMutations, getInProgressMutationsQuery, database, table); err != nil {
		return nil, errors.Wrap(err, "can't get in progress mutations")
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

// ApplyMacrosToObjectLabels https://github.com/Altinity/clickhouse-backup/issues/588
func (ch *ClickHouse) ApplyMacrosToObjectLabels(ctx context.Context, objectLabels map[string]string, backupName string) (map[string]string, error) {
	var err error
	for k, v := range objectLabels {
		v, err = ch.ApplyMacros(ctx, v)
		if err != nil {
			return nil, err
		}
		r := strings.NewReplacer("{backup}", backupName, "{backupName}", backupName, "{backup_name}", backupName, "{BACKUP_NAME}", backupName)
		objectLabels[k] = r.Replace(v)
	}
	return objectLabels, nil
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
			LogPointer    uint64 `ch:"log_pointer"`
			LogMaxIndex   uint64 `ch:"log_max_index"`
			AbsoluteDelay uint64 `ch:"absolute_delay"`
			QueueSize     uint32 `ch:"queue_size"`
		}, 0)
		if err := ch.Select(&existsReplicas, "SELECT log_pointer, log_max_index, absolute_delay, queue_size FROM system.replicas WHERE database=? and table=?", table.Database, table.Table); err != nil {
			return false, err
		}
		if len(existsReplicas) == 0 {
			return true, nil
		}
		if len(existsReplicas) > 1 {
			return false, fmt.Errorf("invalid result for check exists replicas: %+v", existsReplicas)
		}
		// https://github.com/Altinity/clickhouse-backup/issues/967
		if existsReplicas[0].LogPointer > 2 || existsReplicas[0].LogMaxIndex > 1 || existsReplicas[0].AbsoluteDelay > 0 || existsReplicas[0].QueueSize > 0 {
			return false, fmt.Errorf("%s.%s can't restore cause system.replicas entries already exists and replication in progress from another replica, log_pointer=%d, log_max_index=%d, absolute_delay=%d, queue_size=%d", table.Database, table.Table, existsReplicas[0].LogPointer, existsReplicas[0].LogMaxIndex, existsReplicas[0].AbsoluteDelay, existsReplicas[0].QueueSize)
		} else {
			log.Info().Msgf("replication_in_progress status = %+v", existsReplicas)
		}
	}
	return true, nil
}

// CheckSystemPartsColumns check data parts types consistency https://github.com/Altinity/clickhouse-backup/issues/529#issuecomment-1554460504
func (ch *ClickHouse) CheckSystemPartsColumns(ctx context.Context, table *Table) error {
	var err error
	partColumnsDataTypes := make([]ColumnDataTypes, 0)
	partsColumnsSQL := "SELECT column, groupUniqArray(type) AS uniq_types " +
		"FROM system.parts_columns " +
		"WHERE active AND database=? AND table=? AND type NOT LIKE 'Enum%(%' AND type NOT LIKE 'Tuple(%' AND type NOT LIKE 'Nullable(Enum%(%' AND type NOT LIKE 'Nullable(Tuple(%' AND type NOT LIKE 'Array(Tuple(%' AND type NOT LIKE 'Nullable(Array(Tuple(%' " +
		"GROUP BY column HAVING length(uniq_types) > 1"
	if err = ch.SelectContext(ctx, &partColumnsDataTypes, partsColumnsSQL, table.Database, table.Name); err != nil {
		return err
	}
	return ch.CheckTypesConsistency(table, partColumnsDataTypes)
}

var dateWithParams = regexp.MustCompile(`^(Date[^(]+)\([^)]+\)`)
var versioningAggregateRE = regexp.MustCompile(`^[0-9]+,\s*`)

func (ch *ClickHouse) CheckTypesConsistency(table *Table, partColumnsDataTypes []ColumnDataTypes) error {
	cleanType := func(dataType string) string {
		for _, compatiblePrefix := range []string{"LowCardinality(", "Nullable("} {
			if strings.HasPrefix(dataType, compatiblePrefix) {
				dataType = strings.TrimPrefix(dataType, compatiblePrefix)
				dataType = strings.TrimSuffix(dataType, ")")
			}
		}
		dataType = dateWithParams.ReplaceAllString(dataType, "$1")
		return dataType
	}
	for i := range partColumnsDataTypes {
		isAggregationPresent := false
		uniqTypes := common.EmptyMap{}
		for _, dataType := range partColumnsDataTypes[i].Types {
			isAggregationPresent = strings.Contains(dataType, "AggregateFunction(")
			if isAggregationPresent {
				dataType = strings.TrimPrefix(dataType, "SimpleAggregateFunction(")
				dataType = strings.TrimPrefix(dataType, "AggregateFunction(")
				dataType = strings.TrimSuffix(dataType, ")")
				dataType = versioningAggregateRE.ReplaceAllString(dataType, "")
			} else {
				dataType = cleanType(dataType)
			}
			uniqTypes[dataType] = struct{}{}
		}
		if len(uniqTypes) > 1 {
			log.Error().Msgf("`%s`.`%s` have incompatible data types %#v for \"%s\" column", table.Database, table.Name, partColumnsDataTypes[i].Types, partColumnsDataTypes[i].Column)
			return fmt.Errorf("`%s`.`%s` have inconsistent data types for active data part in system.parts_columns", table.Database, table.Name)
		}
	}
	return nil
}

func (ch *ClickHouse) GetSettingsValues(ctx context.Context, settings []interface{}) (map[string]string, error) {
	settingsValues := make([]struct {
		Name  string `ch:"name"`
		Value string `ch:"value"`
	}, 0)
	queryStr := "SELECT name, value FROM system.settings WHERE name IN (" + strings.Repeat("?, ", len(settings))
	queryStr = queryStr[:len(queryStr)-2]
	queryStr += ")"
	if err := ch.SelectContext(ctx, &settingsValues, queryStr, settings...); err != nil {
		return nil, err
	}
	settingsValuesMap := map[string]string{}
	for _, v := range settingsValues {
		settingsValuesMap[v.Name] = v.Value
	}
	return settingsValuesMap, nil
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

func (ch *ClickHouse) GetPreprocessedConfigPath(ctx context.Context) (string, error) {
	metadataPath, err := ch.getMetadataPath(ctx)
	if err != nil {
		return "/var/lib/clickhouse/preprocessed_configs", err
	}
	paths := strings.Split(metadataPath, "/")
	return path.Join("/", path.Join(paths[:len(paths)-1]...), "preprocessed_configs"), nil
}

func (ch *ClickHouse) ParseXML(ctx context.Context, configName string) (string, *xmlquery.Node, error) {
	preprocessedConfigPath, err := ch.GetPreprocessedConfigPath(ctx)
	if err != nil {
		return "", nil, errors.Wrap(err, "ch.GetPreprocessedConfigPath error")
	}
	var doc *xmlquery.Node
	configFile := path.Join(preprocessedConfigPath, configName)
	//to avoid race-condition, cause preprocessed_configs rewrites every second
	retry := retrier.New(retrier.ConstantBackoff(5, time.Millisecond*100), nil)
	retryErr := retry.RunCtx(ctx, func(ctx context.Context) error {
		f, openErr := os.Open(configFile)
		if openErr != nil {
			return openErr
		}
		defer func() {
			if closeErr := f.Close(); closeErr != nil {
				log.Error().Msgf("can't close %s error: %v", configFile, closeErr)
			}
		}()
		var parseErr error
		doc, parseErr = xmlquery.Parse(f)
		return parseErr
	})
	if retryErr != nil {
		xmlContent, readErr := os.ReadFile(configFile)
		if readErr != nil {
			log.Error().Err(readErr).Str("xmlContent", string(xmlContent)).Send()
			return configFile, nil, readErr
		}
		retryErr = errors.Wrapf(retryErr, "xmlquery.Parse(%s) error", configFile)
		return configFile, nil, retryErr
	}
	return configFile, doc, nil
}

var preprocessedXMLSettings = make(map[string]map[string]string)

// GetPreprocessedXMLSettings - @todo think about from_env and from_zookeeper corner cases
func (ch *ClickHouse) GetPreprocessedXMLSettings(ctx context.Context, settingsXPath map[string]string, fileName string) (map[string]string, error) {
	preprocessedPath, err := ch.GetPreprocessedConfigPath(ctx)
	if err != nil {
		return nil, err
	}
	resultSettings := make(map[string]string, len(settingsXPath))
	if _, exists := preprocessedXMLSettings[fileName]; !exists {
		preprocessedXMLSettings[fileName] = make(map[string]string)
	}
	var doc *xmlquery.Node
	for settingName, xpathExpr := range settingsXPath {
		if value, exists := preprocessedXMLSettings[fileName][settingName]; exists {
			resultSettings[settingName] = value
		} else {
			if doc == nil {
				configFile := path.Join(preprocessedPath, fileName)
				//to avoid race-condition, cause preprocessed_configs rewrites every second
				retry := retrier.New(retrier.ConstantBackoff(5, time.Millisecond*100), nil)
				retryErr := retry.RunCtx(ctx, func(ctx context.Context) error {
					f, openErr := os.Open(configFile)
					if openErr != nil {
						return openErr
					}
					defer func() {
						if closeErr := f.Close(); closeErr != nil {
							log.Error().Err(closeErr).Msgf("can't close %s", configFile)
						}
					}()
					var parseErr error
					doc, parseErr = xmlquery.Parse(f)
					return parseErr
				})
				if retryErr != nil {
					xmlContent, readErr := os.ReadFile(configFile)
					retryErr = errors.Wrapf(retryErr, "xmlquery.Parse(%s) error", configFile)
					log.Error().Err(readErr).Str("xmlContent", string(xmlContent)).Send()
					log.Error().Msg(retryErr.Error())
					return nil, retryErr
				}
			}
			for _, node := range xmlquery.Find(doc, xpathExpr) {
				resultSettings[settingName] = strings.Trim(node.InnerText(), " \t\r\n")
				preprocessedXMLSettings[fileName][settingName] = resultSettings[settingName]
			}
		}
	}
	return resultSettings, nil
}

var storagePolicyRE = regexp.MustCompile(`SETTINGS.+storage_policy[^=]*=[^']*'([^']+)'`)

func (ch *ClickHouse) ExtractStoragePolicy(query string) string {
	storagePolicy := "default"
	matches := storagePolicyRE.FindStringSubmatch(query)
	if len(matches) > 0 {
		storagePolicy = matches[1]
	}
	log.Debug().Msgf("extract storage_policy: %s, query: %s", storagePolicy, query)
	return storagePolicy
}
