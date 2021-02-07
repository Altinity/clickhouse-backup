package clickhouse

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/apex/log"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
)

// ClickHouse - provide
type ClickHouse struct {
	Config *config.ClickHouseConfig
	conn   *sqlx.DB
	uid    *int
	gid    *int
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
	params.Add("receive_timeout", timeoutSeconds)
	params.Add("send_timeout", timeoutSeconds)
	if ch.Config.Secure {
		params.Add("secure", "true")
		params.Add("skip_verify", strconv.FormatBool(ch.Config.SkipVerify))
	}
	connectionString := fmt.Sprintf("tcp://%v:%v?%s", ch.Config.Host, ch.Config.Port, params.Encode())
	if ch.conn, err = sqlx.Open("clickhouse", connectionString); err != nil {
		return err
	}
	return ch.conn.Ping()
}

// GetDisks - return data from system.disks table
func (ch *ClickHouse) GetDisks() ([]Disk, error) {
	version, err := ch.GetVersion()
	if err != nil {
		return nil, err
	}
	if version < 19015000 {
		return ch.getDataPathFromSystemSettings()
	}
	return ch.getDataPathFromSystemDisks()
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
	query := "SELECT metadata_path FROM system.tables WHERE database == 'system' LIMIT 1;"
	if err := ch.conn.Select(&result, query); err != nil {
		return nil, err
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
	err := ch.softSelect(&result, query)
	return result, err
}

// Close - closing connection to ClickHouse
func (ch *ClickHouse) Close() error {
	return ch.conn.Close()
}

// GetTables - return slice of all tables suitable for backup
func (ch *ClickHouse) GetTables() ([]Table, error) {
	tables := make([]Table, 0)
	if err := ch.softSelect(&tables, "SELECT * FROM system.tables WHERE is_temporary = 0;"); err != nil {
		return nil, err
	}
	for i, t := range tables {
		for _, filter := range ch.Config.SkipTables {
			if matched, _ := filepath.Match(filter, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				t.Skip = true
				break
			}
		}
		tables[i] = ch.fixVariousVersions(t)
	}
	if len(tables) == 0 {
		return tables, nil
	}
	if !tables[0].TotalBytes.Valid {
		tables = ch.getTableSizeFromParts(tables)
	}
	return tables, nil
}

func (ch *ClickHouse) getTableSizeFromParts(tables []Table) []Table {
	var tablesSize []struct {
		Database string `db:"database"`
		Table    string `db:"table"`
		Size     int64  `db:"size"`
	}
	query := "SELECT database, table, sum(bytes_on_disk) as size FROM system.parts GROUP BY (database, table);"
	ch.softSelect(&tablesSize, query)
	tableMap := map[metadata.TableTitle]int64{}
	for i := range tablesSize {
		tableMap[metadata.TableTitle{
			Database: tablesSize[i].Database,
			Table:    tablesSize[i].Table,
		}] = tablesSize[i].Size
	}
	for i, t := range tables {
		if t.TotalBytes.Valid {
			continue
		}
		t.TotalBytes = sql.NullInt64{
			Int64: tableMap[metadata.TableTitle{
				Database: t.Database,
				Table:    t.Name}],
			Valid: true,
		}
		tables[i] = t
	}
	return tables
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
	return t
}

// GetVersion - returned ClickHouse version in number format
// Example value: 19001005
func (ch *ClickHouse) GetVersion() (int, error) {
	var result []string
	query := "SELECT value FROM `system`.`build_options` where name='VERSION_INTEGER'"
	if err := ch.conn.Select(&result, query); err != nil {
		return 0, fmt.Errorf("can't get сlickHouse version: %v", err)
	}
	if len(result) == 0 {
		return 0, nil
	}
	return strconv.Atoi(result[0])
}

// FreezeTableOldWay - freeze all partitions in table one by one
// This way using for ClickHouse below v19.1
func (ch *ClickHouse) FreezeTableOldWay(table *Table) error {
	var partitions []struct {
		PartitionID string `db:"partition_id"`
	}
	q := fmt.Sprintf("SELECT DISTINCT partition_id FROM `system`.`parts` WHERE database='%s' AND table='%s'", table.Database, table.Name)
	if err := ch.conn.Select(&partitions, q); err != nil {
		return fmt.Errorf("can't get partitions for '%s.%s': %v", table.Database, table.Name, err)
	}
	for _, item := range partitions {
		log.Debugf("  partition '%v'", item.PartitionID)
		query := fmt.Sprintf(
			"ALTER TABLE `%v`.`%v` FREEZE PARTITION ID '%v';",
			table.Database,
			table.Name,
			item.PartitionID)
		if item.PartitionID == "all" {
			query = fmt.Sprintf(
				"ALTER TABLE `%v`.`%v` FREEZE PARTITION tuple();",
				table.Database,
				table.Name)
		}
		if _, err := ch.conn.Exec(query); err != nil {
			return fmt.Errorf("can't freeze partition '%s' on '%s.%s': %v", item.PartitionID, table.Database, table.Name, err)
		}
	}
	return nil
}

// FreezeTable - freeze all partitions for table
// This way available for ClickHouse sience v19.1
func (ch *ClickHouse) FreezeTable(table *Table) error {
	version, err := ch.GetVersion()
	if err != nil {
		return err
	}
	if strings.HasPrefix(table.Engine, "Replicated") && ch.Config.SyncReplicatedTables {
		log.Debugf("Sync '%s.%s'", table.Database, table.Name)
		query := fmt.Sprintf("SYSTEM SYNC REPLICA `%s`.`%s`;", table.Database, table.Name)
		if _, err := ch.conn.Exec(query); err != nil {
			return fmt.Errorf("can't sync '%s.%s': %v", table.Database, table.Name, err)
		}
	}
	if version < 19001005 || ch.Config.FreezeByPart {
		return ch.FreezeTableOldWay(table)
	}
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE;", table.Database, table.Name)
	if _, err := ch.conn.Exec(query); err != nil {
		return fmt.Errorf("can't freeze '%s.%s': %v", table.Database, table.Name, err)
	}
	return nil
}

func (ch *ClickHouse) CleanShadow() error {
	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	for _, disk := range disks {
		shadowDir := path.Join(disk.Path, "shadow")
		if err := cleanDir(shadowDir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("can't clean '%s': %v", shadowDir, err)
		}
	}
	return nil
}

// Chown - set permission on file to clickhouse user
// This is necessary that the ClickHouse will be able to read parts files on restore
func (ch *ClickHouse) Chown(filename string) error {
	var (
		dataPath string
		err      error
	)
	if ch.uid == nil || ch.gid == nil {
		if dataPath, err = ch.GetDefaultPath(); err != nil {
			return err
		}
		info, err := os.Stat(dataPath)
		if err != nil {
			return err
		}
		stat := info.Sys().(*syscall.Stat_t)
		uid := int(stat.Uid)
		gid := int(stat.Gid)
		ch.uid = &uid
		ch.gid = &gid
	}
	return os.Chown(filename, *ch.uid, *ch.gid)
}

func (ch *ClickHouse) Mkdir(name string) error {
	if err := os.Mkdir(name, 0750); err != nil && !os.IsExist(err) {
		return err
	}
	if err := ch.Chown(name); err != nil {
		return err
	}
	return nil
}

// CopyData - copy partitions for specific table to detached folder
func (ch *ClickHouse) CopyData(backupName string, backupTable metadata.TableMetadata, disks []Disk, tableDataPaths []string) error {
	// TODO: проверить если диск есть в бэкапе но нет в кликхаусе
	dstDataPaths := GetDisksByPaths(disks, tableDataPaths)
	for _, backupDisk := range disks {
		if len(backupTable.Parts[backupDisk.Name]) == 0 {
			continue
		}
		detachedParentDir := filepath.Join(dstDataPaths[backupDisk.Name], "detached")
		// os.MkdirAll(detachedParentDir, 0750)
		// ch.Chown(detachedParentDir)
		for _, partition := range backupTable.Parts[backupDisk.Name] {
			detachedPath := filepath.Join(detachedParentDir, partition.Name)
			info, err := os.Stat(detachedPath)
			if err != nil {
				if os.IsNotExist(err) {
					os.MkdirAll(detachedPath, 0750)
				} else {
					return err
				}
			} else if !info.IsDir() {
				return fmt.Errorf("'%s' should be directory or absent", detachedPath)
			}
			ch.Chown(detachedPath)
			uuid := path.Join(TablePathEncode(backupTable.Database), TablePathEncode(backupTable.Table))
			if backupTable.UUID != "" {
				uuid = path.Join(backupTable.UUID[0:3], backupTable.UUID)
			}
			partitionPath := path.Join(backupDisk.Path, "backup", backupName, "shadow", uuid, partition.Name)
			if err := filepath.Walk(partitionPath, func(filePath string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				filename := strings.Trim(strings.TrimPrefix(filePath, partitionPath), "/")
				dstFilePath := filepath.Join(detachedPath, filename)
				if info.IsDir() {
					return ch.Mkdir(dstFilePath)
				}
				if !info.Mode().IsRegular() {
					log.Debugf("'%s' is not a regular file, skipping.", filePath)
					return nil
				}
				if err := os.Link(filePath, dstFilePath); err != nil {
					return fmt.Errorf("failed to crete hard link '%s' -> '%s': %v", filePath, dstFilePath, err)
				}
				return ch.Chown(dstFilePath)
			}); err != nil {
				return fmt.Errorf("error during filepath.Walk for partition '%s': %v", partition.Path, err)
			}
		}
	}
	return nil
}

// AttachPartitions - execute ATTACH command for specific table
func (ch *ClickHouse) AttachPartitions(table metadata.TableMetadata, disks []Disk) error {
	for _, disk := range disks {
		for _, partition := range table.Parts[disk.Name] {
			query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PART '%s'", table.Database, table.Table, partition.Name)
			log.Debug(query)
			if _, err := ch.conn.Exec(query); err != nil {
				return err
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
	_, err := ch.conn.Exec(query)
	return err
}

// CreateTable - create ClickHouse table
func (ch *ClickHouse) CreateTable(table Table, query string, dropTable bool) error {
	if _, err := ch.conn.Exec(fmt.Sprintf("USE `%s`", table.Database)); err != nil {
		return err
	}
	if dropTable {
		query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", table.Database, table.Name)
		log.Debugf(query)
		if _, err := ch.conn.Exec(query); err != nil {
			return err
		}
	}
	log.Infof("Create table '%s.%s'", table.Database, table.Name)
	if _, err := ch.conn.Exec(query); err != nil {
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
	defer d.Close()
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

func TablePathEncode(str string) string {
	return strings.ReplaceAll(
		strings.ReplaceAll(url.PathEscape(str), ".", "%2E"), "-", "%2D")
}

// GetPartitions - return slice of all partitions for a table
func (ch *ClickHouse) GetPartitions(table Table) (map[string][]metadata.Part, error) {
	disks, err := ch.GetDisks()
	if err != nil {
		return nil, err
	}
	result := map[string][]metadata.Part{}
	for _, disk := range disks {
		partitions := make([]partition, 0)
		if len(disks) == 1 {
			if err := ch.softSelect(&partitions,
				fmt.Sprintf("select * from `system`.`parts` where database='%s' and table='%s' and active=1;", table.Database, table.Name)); err != nil {
				return nil, err
			}
		} else {
			if err := ch.softSelect(&partitions,
				fmt.Sprintf("select * from `system`.`parts` where database='%s' and table='%s' and disk_name='%s' and active=1;", table.Database, table.Name, disk.Name)); err != nil {
				return nil, err
			}
		}
		if len(partitions) > 0 {
			parts := make([]metadata.Part, len(partitions))
			for i := range partitions {
				parts[i] = metadata.Part{
					Partition:                         partitions[i].Partition,
					Name:                              partitions[i].Name,
					Path:                              partitions[i].Path, // TODO: ???
					HashOfAllFiles:                    partitions[i].HashOfAllFiles,
					HashOfUncompressedFiles:           partitions[i].HashOfUncompressedFiles,
					UncompressedHashOfCompressedFiles: partitions[i].UncompressedHashOfCompressedFiles,
					PartitionID:                       partitions[i].PartitionID,
					ModificationTime:                  partitions[i].ModificationTime,
					Size:                              partitions[i].DataUncompressedBytes,
				}
			}
			result[disk.Name] = parts
		}
	}
	return result, nil
}
