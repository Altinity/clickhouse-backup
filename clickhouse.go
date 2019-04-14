package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
)

// ClickHouse - provide info and freeze tables
type ClickHouse struct {
	DryRun bool
	Config *ClickHouseConfig
	conn   *sqlx.DB
	uid    *int
	gid    *int
}

// Table - Clickhouse table struct
type Table struct {
	Database     string `db:"database"`
	Name         string `db:"name"`
	DataPath     string `db:"data_path"`
	MetadataPath string `db:"metadata_path"`
	IsTemporary  bool   `db:"is_temporary"`
	Skip         bool
}

// BackupPartition - struct representing Clickhouse partition
type BackupPartition struct {
	Name string
	Path string
}

// BackupTable - struct to store additional information on partitions
type BackupTable struct {
	Database   string
	Name       string
	Partitions []BackupPartition
	Path       string
}

// RestoreTable - struct to store information needed during restore
type RestoreTable struct {
	Database string
	Table    string
	Query    string
}

// Connect - connect to clickhouse
func (ch *ClickHouse) Connect() error {
	connectionString := fmt.Sprintf("tcp://%v:%v?username=%v&password=%v&database=system&compress=true",
		ch.Config.Host, ch.Config.Port, ch.Config.Username, ch.Config.Password)
	var err error
	if ch.conn, err = sqlx.Open("clickhouse", connectionString); err != nil {
		return err
	}
	return ch.conn.Ping()
}

// ConnectDatabase - connect to clickhouse to specified database
func (ch *ClickHouse) ConnectDatabase(database string) error {
	if database == "" {
		database = "default"
	}
	connectionString := fmt.Sprintf("tcp://%v:%v?username=%v&password=%v&database=%v&compress=true",
		ch.Config.Host, ch.Config.Port, ch.Config.Username, ch.Config.Password, database)
	var err error
	if ch.conn, err = sqlx.Open("clickhouse", connectionString); err != nil {
		return err
	}
	return ch.conn.Ping()
}

// GetDataPath - return clickhouse data_path
func (ch *ClickHouse) GetDataPath() (string, error) {
	if ch.Config.DataPath != "" {
		return ch.Config.DataPath, nil
	}
	var result []struct {
		MetadataPath string `db:"metadata_path"`
	}
	if err := ch.conn.Select(&result, "SELECT metadata_path FROM system.tables WHERE database == 'system' LIMIT 1;"); err != nil {
		return "/var/lib/clickhouse", err
	}
	metadataPath := result[0].MetadataPath
	dataPathArray := strings.Split(metadataPath, "/")
	clickhouseData := path.Join(dataPathArray[:len(dataPathArray)-3]...)
	return path.Join("/", clickhouseData), nil
}

// Close - close connection to clickhouse
func (ch *ClickHouse) Close() error {
	return ch.conn.Close()
}

// GetTables - get all tables info
func (ch *ClickHouse) GetTables() ([]Table, error) {
	var tables []Table
	if err := ch.conn.Select(&tables, "SELECT database, name, is_temporary, data_path, metadata_path FROM system.tables WHERE data_path != '' AND is_temporary = 0;"); err != nil {
		return nil, err
	}
	for i, t := range tables {
		for _, filter := range ch.Config.SkipTables {
			if matched, _ := filepath.Match(filter, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				t.Skip = true
				tables[i] = t
				break
			}
		}
	}
	return tables, nil
}

// FreezeTable - freeze all partitions for table
func (ch *ClickHouse) FreezeTable(table Table) error {
	var partitions []struct {
		PartitionID string `db:"partition_id"`
	}
	q := fmt.Sprintf("SELECT DISTINCT partition_id FROM system.parts WHERE database='%v' AND table='%v'", table.Database, table.Name)
	if err := ch.conn.Select(&partitions, q); err != nil {
		return fmt.Errorf("can't get partitions for \"%s.%s\" with %v", table.Database, table.Name, err)
	}
	log.Printf("Freeze '%v.%v'", table.Database, table.Name)
	for _, item := range partitions {
		if ch.DryRun {
			log.Printf("  partition '%v'   ...skip because dry-run", item.PartitionID)
			continue
		}
		log.Printf("  partition '%v'", item.PartitionID)
		query := fmt.Sprintf(
			"ALTER TABLE %v.%v FREEZE PARTITION ID '%v';",
			table.Database,
			table.Name,
			item.PartitionID)
		if item.PartitionID == "all" {
			query = fmt.Sprintf(
				"ALTER TABLE %v.%v FREEZE PARTITION tuple();",
				table.Database,
				table.Name)
		}
		if _, err := ch.conn.Exec(query); err != nil {
			return fmt.Errorf("can't freeze partition '%s' on '%s.%s' with: %v", item.PartitionID, table.Database, table.Name, err)
		}
	}
	return nil
}

// GetBackupTables - return list of backups of tables that can be restored
func (ch *ClickHouse) GetBackupTables(backupName string) (map[string]BackupTable, error) {
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return nil, err
	}
	backupShadowPath := filepath.Join(dataPath, "backup", backupName, "shadow")
	dbNum := 0
	tableNum := 1
	partNum := 2
	totalNum := 3
	if isClickhouseShadow(backupShadowPath) {
		dbNum = 2
		tableNum = 3
		partNum = 4
		totalNum = 5
	}
	fi, err := os.Stat(backupShadowPath)
	if err != nil {
		return nil, fmt.Errorf("can't get tables, %v", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("can't get tables, %s is not a dir", backupShadowPath)
	}

	result := make(map[string]BackupTable)
	if err := filepath.Walk(backupShadowPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			filePath = filepath.ToSlash(filePath) // fix fucking Windows slashes
			relativePath := strings.Trim(strings.TrimPrefix(filePath, backupShadowPath), "/")
			parts := strings.Split(relativePath, "/")
			if len(parts) != totalNum {
				return nil
			}
			partition := BackupPartition{
				Name: parts[partNum],
				Path: filePath,
			}
			table := BackupTable{
				Database:   parts[dbNum],
				Name:       parts[tableNum],
				Partitions: []BackupPartition{partition},
			}
			fullTableName := fmt.Sprintf("%s.%s", table.Database, table.Name)
			if t, ok := result[fullTableName]; ok {
				t.Partitions = append(t.Partitions, partition)
				result[fullTableName] = t
				return nil
			}
			result[fullTableName] = table
			return nil
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// Chown - set owner and group to clickhouse user
func (ch *ClickHouse) Chown(name string) error {
	var (
		dataPath string
		err      error
	)
	if ch.uid == nil || ch.gid == nil {
		if dataPath, err = ch.GetDataPath(); err != nil {
			return err
		}
		info, err := os.Stat(path.Join(dataPath, "data"))
		if err != nil {
			return err
		}
		stat := info.Sys().(*syscall.Stat_t)
		uid := int(stat.Uid)
		gid := int(stat.Gid)
		ch.uid = &uid
		ch.gid = &gid
	}
	return os.Chown(name, *ch.uid, *ch.gid)
}

// CopyData - copy partitions for specific table to detached folder
func (ch *ClickHouse) CopyData(table BackupTable) error {
	// TODO: we need to support legacy backup format here
	if ch.DryRun {
		log.Printf("Prepare data for restoring '%s.%s'  ...skip dry-run", table.Database, table.Name)
		return nil
	}
	log.Printf("Prepare data for restoring '%s.%s'", table.Database, table.Name)
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return err
	}

	detachedParentDir := filepath.Join(dataPath, "data", table.Database, table.Name, "detached")
	os.MkdirAll(detachedParentDir, 0750)
	ch.Chown(detachedParentDir)

	for _, partition := range table.Partitions {
		detachedPath := filepath.Join(detachedParentDir, partition.Name)
		info, err := os.Stat(detachedPath)
		if err != nil {
			if os.IsNotExist(err) {
				// partition dir does not exist, creating
				os.MkdirAll(detachedPath, 0750)
			} else {
				return err
			}
		} else if !info.IsDir() {
			return fmt.Errorf("'%s' should be directory or absent", detachedPath)
		}
		ch.Chown(detachedPath)

		if err := filepath.Walk(partition.Path, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			filePath = filepath.ToSlash(filePath) // fix Windows slashes
			filename := strings.Trim(strings.TrimPrefix(filePath, partition.Path), "/")
			dstFilePath := filepath.Join(detachedPath, filename)
			if info.IsDir() {
				os.MkdirAll(dstFilePath, 0750)
				return ch.Chown(dstFilePath)
			}
			if !info.Mode().IsRegular() {
				log.Printf("'%s' is not a regular file, skipping.", filePath)
				return nil
			}
			if err := os.Link(filePath, dstFilePath); err != nil {
				return fmt.Errorf("Failed to crete hard link %s -> %s with %v", filePath, dstFilePath, err)
			}
			return ch.Chown(dstFilePath)
		}); err != nil {
			return fmt.Errorf("Error during filepath.Walk for partition %s: %v", partition.Path, err)
		}
	}
	return nil
}

func convertPartition(detachedTableFolder string) string {
	parts := strings.Split(detachedTableFolder, "_")
	if parts[0] == "all" {
		// table is not partitioned at all
		// ENGINE = MergeTree ORDER BY id
		return "tuple()"
	}
	if len(parts) == 5 {
		// legacy partitioning based on month: toYYYYMM(date_column)
		// in this case we return YYYYMM
		// ENGINE = MergeTree(Date, (TimeStamp, Log), 8192)
		return parts[0][:6]
	}
	// in case a custom partitioning key is used this is a partition name
	// same as in system.parts table, it may be used in ALTER TABLE queries
	// https://clickhouse.yandex/docs/en/operations/table_engines/custom_partitioning_key/
	return fmt.Sprintf("ID '%s'", parts[0])
}

// AttachPatritions - execute ATTACH command for specific table
func (ch *ClickHouse) AttachPatritions(table BackupTable) error {
	attachedParts := make(map[string]struct{})
	for _, partition := range table.Partitions {
		partName := convertPartition(partition.Name)
		if _, ok := attachedParts[partName]; ok {
			continue
		}
		query := fmt.Sprintf("ALTER TABLE %v.%v ATTACH PARTITION %s", table.Database, table.Name, partName)
		attachedParts[partName] = struct{}{}
		log.Printf(query)
		if ch.DryRun {
			continue
		}
		if _, err := ch.conn.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

// CreateDatabase - create specific database from metadata in backup folder
func (ch *ClickHouse) CreateDatabase(database string) error {
	createQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)
	if ch.DryRun {
		log.Printf("DRY-RUN: %s", createQuery)
		return nil
	}
	if _, err := ch.conn.Exec(createQuery); err != nil {
		return err
	}
	return nil
}

// CreateTable - create specific table from metadata in backup folder
func (ch *ClickHouse) CreateTable(table RestoreTable) error {
	if ch.DryRun {
		log.Printf("DRY-RUN: Create table '%s.%s'", table.Database, table.Table)
		return nil
	}
	if _, err := ch.conn.Exec(fmt.Sprintf("USE %s", table.Database)); err != nil {
		return err
	}
	log.Printf("Create table '%s.%s'", table.Database, table.Table)
	if _, err := ch.conn.Exec(table.Query); err != nil {
		return err
	}
	return nil
}
