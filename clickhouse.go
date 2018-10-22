package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
)

// ClickHouse - provide info and freeze tables
type ClickHouse struct {
	DryRun bool
	Config *ClickHouseConfig
	conn   *sqlx.DB
}

// Table - table struct
type Table struct {
	Database     string `db:"database"`
	Name         string `db:"name"`
	DataPath     string `db:"data_path"`
	MetadataPath string `db:"metadata_path"`
	IsTemporary  bool   `db:"is_temporary"`
}

type BackupPartition struct {
	Name string
	Path string
}

type BackupTable struct {
	Increment  string
	Database   string
	Name       string
	Partitions []BackupPartition
	Path       string
}

// Connect - connect to clickhouse
func (ch *ClickHouse) Connect() error {
	connectionString := fmt.Sprintf("tcp://%v:%v?username=%v&password=%v&compress=true",
		ch.Config.Host, ch.Config.Port, ch.Config.Username, ch.Config.Password)
	var err error
	if ch.conn, err = sqlx.Open("clickhouse", connectionString); err != nil {
		return err
	}

	return ch.conn.Ping()
}

// GetDataPath - return clickhouse data_path
func (ch *ClickHouse) GetDataPath() (string, error) {
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
	if err := ch.conn.Select(&tables, "SELECT database, name, is_temporary, data_path, metadata_path FROM system.tables WHERE database != 'system';"); err != nil {
		return nil, err
	}
	return tables, nil
}

// FreezeTable - freze all partitions for table
func (ch *ClickHouse) FreezeTable(table Table) error {
	var partitions []struct {
		Partition string `db:"partition"`
	}
	q := fmt.Sprintf("select DISTINCT partition FROM system.parts WHERE database='%v' AND table='%v'", table.Database, table.Name)
	if err := ch.conn.Select(&partitions, q); err != nil {
		return fmt.Errorf("can't get partitions for \"%s.%s\" with %v", table.Database, table.Name, err)
	}

	log.Printf("Freeze '%v.%v'", table.Database, table.Name)

	for _, item := range partitions {
		if ch.DryRun {
			log.Printf("  partition '%v'   ...skip becouse dry-run", item.Partition)
			continue
		}
		log.Printf("  partition '%v'", item.Partition)
		if _, err := ch.conn.Exec(
			fmt.Sprintf(
				"ALTER TABLE %v.%v FREEZE PARTITION %v;",
				table.Database,
				table.Name,
				item.Partition,
			)); err != nil {
			return fmt.Errorf("can't freze partiotion '%s' on '%s.%s' with: %v", item.Partition, table.Database, table.Name, err)
		}
	}
	return nil
}

func (ch *ClickHouse) GetBackupTables() (map[string]BackupTable, error) {
	// /var/lib/clickhouse/shadow/[N]/[db]/[table]/[part]
	// /var/lib/clickhouse/backup/shadow/[N]/[db]/[table]/[part]
	// /var/lib/clickhouse/data/[bd]/[table]/detached/[part]
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return nil, err
	}
	backupShadowPath := filepath.Join(dataPath, "backup", "shadow")
	result := make(map[string]BackupTable)
	err = filepath.Walk(backupShadowPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			filePath = filepath.ToSlash(filePath) // fix fucking Windows slashes
			relativePath := strings.TrimPrefix(filePath, backupShadowPath)
			parts := filepath.SplitList(relativePath)
			if len(parts) < 4 {
				return fmt.Errorf("Unknown path '%s'", filePath)
			}
			partition := BackupPartition{
				Name: parts[3],
				Path: filePath,
			}
			table := BackupTable{
				Increment:  parts[0],
				Database:   parts[1],
				Name:       parts[2],
				Partitions: []BackupPartition{partition},
			}
			fullTableName := fmt.Sprintf("%s.%s-%s", table.Database, table.Name, table.Increment)
			if t, ok := result[fullTableName]; ok {
				t.Partitions = append(t.Partitions, partition)
				result[fullTableName] = t
				return nil
			}
			result[fullTableName] = table
			return nil
		}
		return nil
	})
	return result, nil
}

func (ch *ClickHouse) CopyData(table BackupTable) error {
	// /var/lib/clickhouse/shadow/[N]/[db]/[table]/[part]
	// /var/lib/clickhouse/backup/shadow/[N]/[db]/[table]/[part]
	// /var/lib/clickhouse/data/[bd]/[table]/detached/[part]
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return err
	}
	for _, partition := range table.Partitions {
		detachedPath := filepath.Join(dataPath, table.Database, table.Name, "detached", partition.Name)
		info, err := os.Stat(detachedPath)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("%v", err)
			}
		}
		if !info.IsDir() {
			return fmt.Errorf("'%s' must be not exists", detachedPath)
		}
		if err := os.MkdirAll(detachedPath, 0750); err != nil {
			return err
		}
		if err := filepath.Walk(partition.Path, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			filePath = filepath.ToSlash(filePath) // fix fucking Windows slashes
			srcFileStat, err := os.Stat(filePath)
			if err != nil {
				return err
			}
			if !srcFileStat.Mode().IsRegular() {
				return fmt.Errorf("'%s' is not a regular file", filePath)
			}
			srcFile, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer srcFile.Close()
			_, filename := filepath.Split(filePath)
			dstFilePath := filepath.Join(detachedPath, filename)
			dstFile, err := os.Create(dstFilePath)
			if err != nil {
				return err
			}
			defer dstFile.Close()
			_, err = io.Copy(dstFile, srcFile)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

func (ch *ClickHouse) AttachPatritions(table BackupTable) error {

	return nil
}

func (ch *ClickHouse) GetClickHouseUser() (string, string, error) {
	return "", "", nil
}

func (ch *ClickHouse) CreateDatabase(database string) error {
	return nil
}

func (ch *ClickHouse) CreateTable(query string) error {
	return nil
}
