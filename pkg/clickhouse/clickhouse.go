package clickhouse

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/apex/log"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

// ClickHouse - provide
type ClickHouse struct {
	Config  *config.ClickHouseConfig
	conn    *sqlx.DB
	uid     *int
	gid     *int
	disks   []Disk
	version int
}

func (ch *ClickHouse) GetUid() *int {
	return ch.uid
}

func (ch *ClickHouse) GetGid() *int {
	return ch.gid
}

func (ch *ClickHouse) SetUid(puid *int) {
	ch.uid = puid
}

func (ch *ClickHouse) SetGid(pgid *int) {
	ch.gid = pgid
}

// Connect - establish connection to ClickHouse
func (ch *ClickHouse) Connect() error {
	timeout, err := time.ParseDuration(ch.Config.Timeout)
	if err != nil {
		return err
	}

	timeoutSeconds := fmt.Sprintf("%d", int(timeout.Seconds()))
	params := url.Values{}
	params.Add("username", ch.Config.Username)
	params.Add("password", ch.Config.Password)
	params.Add("database", "system")
	params.Add("connect_timeout", timeoutSeconds)
	params.Add("receive_timeout", timeoutSeconds)
	params.Add("send_timeout", timeoutSeconds)
	params.Add("timeout", timeoutSeconds)
	params.Add("read_timeout", timeoutSeconds)
	params.Add("write_timeout", timeoutSeconds)

	if ch.Config.Debug {
		params.Add("debug", "true")
	}

	if ch.Config.Secure {
		params.Add("secure", "true")
		params.Add("skip_verify", strconv.FormatBool(ch.Config.SkipVerify))
	}
	if !ch.Config.LogSQLQueries {
		params.Add("log_queries", "0")
	}
	connectionString := fmt.Sprintf("tcp://%v:%v?%s", ch.Config.Host, ch.Config.Port, params.Encode())
	if ch.conn, err = sqlx.Open("clickhouse", connectionString); err != nil {
		return err
	}
	ch.conn.SetMaxOpenConns(1)
	ch.conn.SetConnMaxLifetime(0)
	ch.conn.SetMaxIdleConns(0)
	return ch.conn.Ping()
}

// GetDisks - return data from system.disks table
func (ch *ClickHouse) GetDisks() ([]Disk, error) {
	version, err := ch.GetVersion()
	if err != nil {
		return nil, err
	}
	var disks []Disk
	if version < 19015000 {
		disks, err = ch.getDataPathFromSystemSettings()
	} else {
		disks, err = ch.getDataPathFromSystemDisks()
	}
	if err != nil {
		return nil, err
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

func (ch *ClickHouse) GetDefaultPath() (string, error) {
	disks, err := ch.GetDisks()
	if err != nil {
		return "", err
	}
	defaultPath := "/var/lib/clickhouse"
	for _, d := range disks {
		if d.Name == "default" {
			defaultPath = d.Path
			break
		}
	}
	return defaultPath, nil
}

func (ch *ClickHouse) getDataPathFromSystemSettings() ([]Disk, error) {
	var result []struct {
		MetadataPath string `db:"metadata_path"`
	}
	query := "SELECT metadata_path FROM system.tables WHERE database = 'system' AND metadata_path!='' LIMIT 1;"
	if err := ch.Select(&result, query); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("can't get metadata_path from system.tables")
	}
	metadataPath := result[0].MetadataPath
	dataPathArray := strings.Split(metadataPath, "/")
	clickhouseData := path.Join(dataPathArray[:len(dataPathArray)-3]...)
	return []Disk{{
		Name: "default",
		Path: path.Join("/", clickhouseData),
		Type: "local",
	}}, nil
}

func (ch *ClickHouse) getDataPathFromSystemDisks() ([]Disk, error) {
	var result []Disk
	query := "SELECT * FROM system.disks;"
	err := ch.SoftSelect(&result, query)
	return result, err
}

// Close - closing connection to ClickHouse
func (ch *ClickHouse) Close() {
	if err := ch.conn.Close(); err != nil {
		log.Warnf("can't close clickhouse connection: %v", err)
	}
}

// GetTables - return slice of all tables suitable for backup, MySQL and PostgreSQL database engine shall be skipped
func (ch *ClickHouse) GetTables(tablePattern string) ([]Table, error) {
	var err error
	tables := make([]Table, 0)
	isUUIDPresent := make([]int, 0)
	if err = ch.Select(&isUUIDPresent, "SELECT count() FROM system.settings WHERE name = 'show_table_uuid_in_table_create_query_if_not_nil'"); err != nil {
		return nil, err
	}
	skipDatabases := make([]string, 0)
	if err = ch.Select(&skipDatabases, "SELECT name FROM system.databases WHERE engine IN ('MySQL','PostgreSQL')"); err != nil {
		return nil, err
	}
	allTablesSQL, err := ch.prepareAllTablesSQL(tablePattern, err, skipDatabases, isUUIDPresent)
	if err != nil {
		return nil, err
	}
	if err = ch.SoftSelect(&tables, allTablesSQL); err != nil {
		return nil, err
	}
	for i, t := range tables {
		for _, filter := range ch.Config.SkipTables {
			if matched, _ := filepath.Match(strings.Trim(filter, " \t\r\n"), fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				t.Skip = true
				break
			}
		}
		if t.Skip {
			tables[i] = t
			continue
		}
		tables[i] = ch.fixVariousVersions(t)
	}
	if len(tables) == 0 {
		return tables, nil
	}
	for i, table := range tables {
		if table.TotalBytes == 0 && !table.Skip && strings.HasSuffix(table.Engine, "Tree") {
			tables[i].TotalBytes = ch.getTableSizeFromParts(tables[i])
		}
	}
	return tables, nil
}

func (ch *ClickHouse) prepareAllTablesSQL(tablePattern string, err error, skipDatabases []string, isUUIDPresent []int) (string, error) {
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
	if err = ch.Select(&isSystemTablesFieldPresent, isFieldPresentSQL); err != nil {
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
		replacer := strings.NewReplacer(".", "\\.", ",", "|", "*", ".*", "?", ".", " ", "")
		allTablesSQL += fmt.Sprintf(" AND match(concat(database,'.',name),'%s') ", replacer.Replace(tablePattern))
	}
	if len(skipDatabases) > 0 {
		allTablesSQL += fmt.Sprintf(" AND database NOT IN ('%s')", strings.Join(skipDatabases, "','"))
	}
	if len(isUUIDPresent) > 0 && isUUIDPresent[0] > 0 {
		allTablesSQL += " SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
	}
	return allTablesSQL, nil
}

// GetDatabases - return slice of all non system databases for backup
func (ch *ClickHouse) GetDatabases() ([]Database, error) {
	allDatabases := make([]Database, 0)
	allDatabasesSQL := "SELECT name, engine FROM system.databases WHERE name != 'system'"
	if err := ch.SoftSelect(&allDatabases, allDatabasesSQL); err != nil {
		return nil, err
	}
	for i, db := range allDatabases {
		showDatabaseSQL := fmt.Sprintf("SHOW CREATE DATABASE `%s`", db.Name)
		var result []string
		// 19.4 doesn't have /var/lib/clickhouse/metadata/default.sql
		if err := ch.Select(&result, showDatabaseSQL); err != nil {
			log.Warnf("can't get create database query: %v", err)
			allDatabases[i].Query = fmt.Sprintf("CREATE DATABASE `%s` ENGINE = %s", db.Name, db.Engine)
		} else {
			allDatabases[i].Query = result[0]
		}
	}
	return allDatabases, nil
}

func (ch *ClickHouse) getTableSizeFromParts(table Table) uint64 {
	var tablesSize []struct {
		Size uint64 `db:"size"`
	}
	query := fmt.Sprintf("SELECT sum(bytes_on_disk) as size FROM system.parts WHERE database='%s' AND table='%s' GROUP BY database, table", table.Database, table.Name)
	if err := ch.SoftSelect(&tablesSize, query); err != nil {
		log.Warnf("error parsing tablesSize: %w", err)
	}
	if len(tablesSize) > 0 {
		return tablesSize[0].Size
	}
	return 0
}

func (ch *ClickHouse) fixVariousVersions(t Table) Table {
	// versions before 19.15 contain data_path in a different column
	if t.DataPath != "" {
		t.DataPaths = []string{t.DataPath}
	}
	// version 20.6.3.28 has zero UUID
	if t.UUID == "00000000-0000-0000-0000-000000000000" {
		t.UUID = ""
	}
	// version 1.1.54390 no has query column
	if strings.TrimSpace(t.CreateTableQuery) == "" {
		t.CreateTableQuery = ch.ShowCreateTable(t.Database, t.Name)
	}
	// materialized views should properly restore via attach
	if t.Engine == "MaterializedView" {
		t.CreateTableQuery = strings.Replace(
			t.CreateTableQuery, "CREATE MATERIALIZED VIEW", "ATTACH MATERIALIZED VIEW", 1,
		)
	}
	return t
}

// GetVersion - returned ClickHouse version in number format
// Example value: 19001005
func (ch *ClickHouse) GetVersion() (int, error) {
	if ch.version != 0 {
		return ch.version, nil
	}
	var result []string
	var err error
	query := "SELECT value FROM `system`.`build_options` where name='VERSION_INTEGER'"
	if err = ch.Select(&result, query); err != nil {
		return 0, fmt.Errorf("can't get сlickHouse version: %w", err)
	}
	if len(result) == 0 {
		return 0, nil
	}
	ch.version, err = strconv.Atoi(result[0])
	return ch.version, err
}

func (ch *ClickHouse) GetVersionDescribe() string {
	var result []string
	query := "SELECT value FROM `system`.`build_options` where name='VERSION_DESCRIBE'"
	if err := ch.Select(&result, query); err != nil {
		return ""
	}
	return result[0]
}

// FreezeTableOldWay - freeze all partitions in table one by one
// This way using for ClickHouse below v19.1
func (ch *ClickHouse) FreezeTableOldWay(table *Table, name string) error {
	var partitions []struct {
		PartitionID string `db:"partition_id"`
	}
	q := fmt.Sprintf("SELECT DISTINCT partition_id FROM `system`.`parts` WHERE database='%s' AND table='%s'", table.Database, table.Name)
	if err := ch.conn.Select(&partitions, q); err != nil {
		return fmt.Errorf("can't get partitions for '%s.%s': %w", table.Database, table.Name, err)
	}
	withNameQuery := ""
	if name != "" {
		withNameQuery = fmt.Sprintf("WITH NAME '%s'", name)
	}
	for _, item := range partitions {
		log.Debugf("  partition '%v'", item.PartitionID)
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
		if _, err := ch.Query(query); err != nil {
			if (strings.Contains(err.Error(), "code: 60") || strings.Contains(err.Error(), "code: 81")) && ch.Config.IgnoreNotExistsErrorDuringFreeze {
				log.Warnf("can't freeze partition: %v", err)
			} else {
				return fmt.Errorf("can't freeze partition '%s': %w", item.PartitionID, err)
			}
		}
	}
	return nil
}

// FreezeTable - freeze all partitions for table
// This way available for ClickHouse since v19.1
func (ch *ClickHouse) FreezeTable(table *Table, name string) error {
	version, err := ch.GetVersion()
	if err != nil {
		return err
	}
	if strings.HasPrefix(table.Engine, "Replicated") && ch.Config.SyncReplicatedTables {
		query := fmt.Sprintf("SYSTEM SYNC REPLICA `%s`.`%s`;", table.Database, table.Name)
		if _, err := ch.Query(query); err != nil {
			log.Warnf("can't sync replica: %v", err)
		} else {
			log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Name)).Debugf("replica synced")
		}
	}
	if version < 19001005 || ch.Config.FreezeByPart {
		return ch.FreezeTableOldWay(table, name)
	}
	withNameQuery := ""
	if name != "" {
		withNameQuery = fmt.Sprintf("WITH NAME '%s'", name)
	}
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE %s;", table.Database, table.Name, withNameQuery)
	if _, err := ch.Query(query); err != nil {
		if (strings.Contains(err.Error(), "code: 60") || strings.Contains(err.Error(), "code: 81")) && ch.Config.IgnoreNotExistsErrorDuringFreeze {
			log.Warnf("can't freeze table: %v", err)
			return nil
		}
		return fmt.Errorf("can't freeze table: %v", err)
	}
	return nil
}

//
// AttachPartitions - execute ATTACH command for specific table
func (ch *ClickHouse) AttachPartitions(table metadata.TableMetadata, disks []Disk) error {
	for _, disk := range disks {
		for _, partition := range table.Parts[disk.Name] {
			if !strings.HasSuffix(partition.Name, ".proj") {
				query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PART '%s'", table.Database, table.Table, partition.Name)
				if _, err := ch.Query(query); err != nil {
					return err
				}
				log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).WithField("disk", disk.Name).WithField("part", partition.Name).Debug("attached")
			}
		}
	}
	return nil
}

func (ch *ClickHouse) ShowCreateTable(database, name string) string {
	var result []struct {
		Statement string `db:"statement"`
	}
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`;", database, name)
	if err := ch.conn.Select(&result, query); err != nil {
		return ""
	}
	return result[0].Statement
}

// CreateDatabase - create ClickHouse database
func (ch *ClickHouse) CreateDatabase(database string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
	_, err := ch.Query(query)
	return err
}

func (ch *ClickHouse) CreateDatabaseWithEngine(database string, engine string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ENGINE=%s", database, engine)
	_, err := ch.Query(query)
	return err
}

func (ch *ClickHouse) CreateDatabaseFromQuery(query string) error {
	if !strings.HasPrefix(query, "CREATE DATABASE IF NOT EXISTS") {
		query = strings.Replace(query, "CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS", 1)
	}
	_, err := ch.Query(query)
	return err
}

// DropTable - drop ClickHouse table
func (ch *ClickHouse) DropTable(table Table, query string, onCluster string, version int) error {
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
	if _, err := ch.Query(dropQuery); err != nil {
		return err
	}
	return nil
}

var createViewRe = regexp.MustCompile(`(?im)(CREATE[\s\w]+VIEW[^(]+)(\s+AS\s+SELECT.+)`)
var createObjRe = regexp.MustCompile(`(?im)(CREATE[^(]+)(\(.+)`)
var onClusterRe = regexp.MustCompile(`(?im)\S+ON\S+CLUSTER\S+`)

// CreateTable - create ClickHouse table
func (ch *ClickHouse) CreateTable(table Table, query string, dropTable bool, onCluster string, version int) error {
	var err error
	if dropTable {
		if err = ch.DropTable(table, query, onCluster, version); err != nil {
			return err
		}
	}

	if version > 19000000 && onCluster != "" && !onClusterRe.MatchString(query) {
		if createViewRe.MatchString(query) {
			query = createViewRe.ReplaceAllString(query, "$1 ON CLUSTER '"+onCluster+"' $2")
		} else if createObjRe.MatchString(query) {
			query = createObjRe.ReplaceAllString(query, "$1 ON CLUSTER '"+onCluster+"' $2")
		}
	}

	if !strings.Contains(query, table.Name) {
		return errors.New(fmt.Sprintf("schema query ```%s``` doesn't contains table name `%s`", query, table.Name))
	}

	// fix restore schema for legacy backup
	// see https://github.com/AlexAkulov/clickhouse-backup/issues/268
	// https://github.com/AlexAkulov/clickhouse-backup/issues/297
	// https://github.com/AlexAkulov/clickhouse-backup/issues/331
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
	if isOnlyTableWithQuotesPresent {
		query = strings.Replace(query, fmt.Sprintf("`%s`", table.Name), fmt.Sprintf("`%s`.`%s`", table.Database, table.Name), 1)
	} else if isOnlyTablePresent {
		query = strings.Replace(query, fmt.Sprintf("%s", table.Name), fmt.Sprintf("%s.%s", table.Database, table.Name), 1)
	}

	if _, err := ch.Query(query); err != nil {
		return err
	}
	return nil
}

// GetConn - return current connection
func (ch *ClickHouse) GetConn() *sqlx.DB {
	return ch.conn
}

func IsClickhouseShadow(path string) bool {
	d, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() {
		if err := d.Close(); err != nil {
			log.Warnf("can't close directory %w", err)
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

func (ch *ClickHouse) SoftSelect(dest interface{}, query string) error {
	rows, err := ch.Queryx(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var v, vp reflect.Value

	value := reflect.ValueOf(dest)

	// json.Unmarshal returns errors for these
	if value.Kind() != reflect.Ptr {
		return fmt.Errorf("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return fmt.Errorf("nil pointer passed to StructScan destination")
	}
	direct := reflect.Indirect(value)

	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return err
	}

	isPtr := slice.Elem().Kind() == reflect.Ptr
	base := reflectx.Deref(slice.Elem())

	columns, err := rows.Columns()
	fields := rows.Mapper.TraversalsByName(base, columns)
	values := make([]interface{}, len(columns))

	if err != nil {
		return err
	}
	for rows.Next() {
		vp = reflect.New(base)
		v = reflect.Indirect(vp)

		err = fieldsByTraversal(v, fields, values, true)
		if err != nil {
			return err
		}

		// scan into the struct field pointers and append to our results
		err = rows.Scan(values...)
		if err != nil {
			return err
		}

		if isPtr {
			direct.Set(reflect.Append(direct, vp))
		} else {
			direct.Set(reflect.Append(direct, v))
		}

	}
	return rows.Err()
}

// GetPartitions - return slice of all partitions for a table
func (ch *ClickHouse) GetPartitions(database, table string) (map[string][]metadata.Part, error) {
	disks, err := ch.GetDisks()
	if err != nil {
		return nil, err
	}
	result := map[string][]metadata.Part{}
	for _, disk := range disks {
		partitions := make([]partition, 0)
		if len(disks) == 1 {
			if err := ch.SoftSelect(&partitions,
				fmt.Sprintf("select * from `system`.`parts` where database='%s' and table='%s' and active=1;", database, table)); err != nil {
				return nil, err
			}
		} else {
			if err := ch.SoftSelect(&partitions,
				fmt.Sprintf("select * from `system`.`parts` where database='%s' and table='%s' and disk_name='%s' and active=1;", database, table, disk.Name)); err != nil {
				return nil, err
			}
		}
		if len(partitions) > 0 {
			parts := make([]metadata.Part, len(partitions))
			for i := range partitions {
				parts[i] = metadata.Part{
					Partition:                         partitions[i].Partition,
					Name:                              partitions[i].Name,
					HashOfAllFiles:                    partitions[i].HashOfAllFiles,
					HashOfUncompressedFiles:           partitions[i].HashOfUncompressedFiles,
					UncompressedHashOfCompressedFiles: partitions[i].UncompressedHashOfCompressedFiles,
					PartitionID:                       partitions[i].PartitionID,
					ModificationTime:                  &partitions[i].ModificationTime,
					Size:                              partitions[i].DataUncompressedBytes,
				}
			}
			result[disk.Name] = parts
		}
	}
	return result, nil
}

func (ch *ClickHouse) Query(query string, args ...interface{}) (sql.Result, error) {
	return ch.conn.Exec(ch.LogQuery(query), args...)
}

func (ch *ClickHouse) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	return ch.conn.Queryx(ch.LogQuery(query), args...)
}

func (ch *ClickHouse) Select(dest interface{}, query string, args ...interface{}) error {
	return ch.conn.Select(dest, ch.LogQuery(query), args...)
}

func (ch *ClickHouse) LogQuery(query string) string {
	if !ch.Config.LogSQLQueries {
		log.Debug(query)
	} else {
		log.Info(query)
	}
	return query
}

func (ch *ClickHouse) IsAtomic(database string) (bool, error) {
	var isDatabaseAtomic []string
	if err := ch.Select(&isDatabaseAtomic, fmt.Sprintf("SELECT engine FROM system.databases WHERE name = '%s'", database)); err != nil {
		return false, err
	}
	return len(isDatabaseAtomic) > 0 && isDatabaseAtomic[0] == "Atomic", nil
}

// GetAccessManagementPath @todo think about how to properly extract access_management_path from /etc/clickhouse-server/
func (ch *ClickHouse) GetAccessManagementPath(disks []Disk) (string, error) {
	accessPath := "/var/lib/clickhouse/access"
	var rows []string
	if err := ch.Select(&rows, "SELECT JSONExtractString(params,'path') AS access_path FROM system.user_directories WHERE type='local directory'"); err != nil || len(rows) == 0 {
		if disks == nil {
			disks, err = ch.GetDisks()
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
		accessPath = rows[0]
	}
	return accessPath, nil
}
