package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
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
	Increment  int
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
	if err := filepath.Walk(backupShadowPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			filePath = filepath.ToSlash(filePath) // fix fucking Windows slashes
			relativePath := strings.Trim(strings.TrimPrefix(filePath, backupShadowPath), "/")
			parts := strings.Split(relativePath, "/")
			if len(parts) != 5 {
				// /var/lib/clickhouse/backup/shadow/6/data/testdb/test2/all_1_3_1
				// fmt.Printf("Skip path '%v'\n", parts)
				return nil
			}
			partition := BackupPartition{
				Name: parts[4],
				Path: filePath,
			}
			increment, err := strconv.Atoi(parts[0])
			if err != nil {
				// fmt.Printf("Skip path '%v'\n", parts)
				return nil
			}
			table := BackupTable{
				Increment:  increment,
				Database:   parts[2],
				Name:       parts[3],
				Partitions: []BackupPartition{partition},
			}
			fullTableName := fmt.Sprintf("%s.%s-%d", table.Database, table.Name, table.Increment)
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

func (ch *ClickHouse) Chown(name string) error {
	// TODO: fix this
	uid := 984
	gid := 979
	return os.Chown(name, uid, gid)
}

func (ch *ClickHouse) CopyData(table BackupTable) error {
	// /var/lib/clickhouse/shadow/[N]/[db]/[table]/[part]
	// /var/lib/clickhouse/backup/shadow/[N]/[db]/[table]/[part]
	// /var/lib/clickhouse/data/[bd]/[table]/detached/[part]
	fmt.Printf("copy %s.%s inscrement %d\n", table.Database, table.Name, table.Increment)
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return err
	}
	for _, partition := range table.Partitions {
		detachedPath := filepath.Join(dataPath, "data", table.Database, table.Name, "detached", partition.Name)
		info, err := os.Stat(detachedPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("%v", err)
			}
		} else if !info.IsDir() {
			return fmt.Errorf("'%s' must be not exists", detachedPath)
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
			_, filename := filepath.Split(filePath)
			dstFilePath := filepath.Join(detachedPath, filename)
			if srcFileStat.IsDir() {
				os.MkdirAll(dstFilePath, 0750)
				return ch.Chown(dstFilePath)
			}
			if !srcFileStat.Mode().IsRegular() {
				return fmt.Errorf("'%s' is not a regular file", filePath)
			}
			srcFile, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer srcFile.Close()
			dstFile, err := os.Create(dstFilePath)
			if err != nil {
				return err
			}
			defer dstFile.Close()
			if _, err = io.Copy(dstFile, srcFile); err != nil {
				return err
			}
			return ch.Chown(dstFilePath)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (ch *ClickHouse) AttachPatritions(table BackupTable) error {
	for _, p := range table.Partitions {
		log.Printf("  ATTACH partition %s for %s.%s increment %d", p.Name, table.Database, table.Name, table.Increment)
	}
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
