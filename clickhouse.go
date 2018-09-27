package main

import (
	"fmt"
	"log"

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

func (ch *ClickHouse) GetDataPath() (string, error) {
	return "/var/lib/clickhouse", nil
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
			log.Printf("  partition '%v'   ...skip becose dry-run", item.Partition)
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
