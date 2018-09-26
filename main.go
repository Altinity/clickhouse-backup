package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func help() {
	fmt.Println("clickhouse-backup backup [db[.table]]")
	fmt.Println("TODO: clickhouse-backup restore [db[.table]]")
	fmt.Println("TODO: clickhouse-backup upload [db[.table]]")
	fmt.Println("TODO: clickhouse-backup download [db[.table]]")

	fmt.Println("--dry-run")
}

func main() {

	args := os.Args
	if len(args) < 1 {
		help()
		os.Exit(1)
	}

	config, err := LoadConfig("config.yml")
	if err != nil {
		log.Fatal(err)
	}

	if args[1] != "backup" {
		help()
		os.Exit(1)
	}
	if err := backup(*config, args[2:]); err != nil {
		log.Fatal(err)
	}

}

func parseArgs(tables []Table, args []string) ([]Table, error) {
	// empty or * - all tables of all databases
	// db or db.*- all tables in specific database
	// db.table - specific table
	var result []Table
	for _, arg := range args {
		for _, t := range tables {
			if matched, _ := filepath.Match(arg, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				result = append(result, t)
				continue
			}
			return nil, fmt.Errorf("\"%s\" not found", arg)
		}
	}
	return result, nil
}

func backup(config Config, args []string) error {
	ch := &ClickHouse{
		DryRun: false,
		Config: &config.ClickHouse,
	}

	if err := ch.Connect(); err != nil {
		return err
	}

	allTables, err := ch.GetTables()
	if err != nil {
		return err
	}
	backupTables := allTables
	if len(args) > 0 {
		if backupTables, err = parseArgs(allTables, args); err != nil {
			return err
		}
	}
	if len(backupTables) == 0 {
		return fmt.Errorf("no have tables for backup")
	}

	for _, table := range backupTables {
		err := ch.FreezeTable(table)
		if err != nil {
			return err
		}
	}
	return nil
}

type S3 struct {
	Config *S3Config
}

func (s3 *S3) Connect() error {
	return nil
}

func (s3 *S3) Upload(localPath string, s3Path string) error {
	return nil
}

func (s3 *S3) Download(s3Path string, localPath string) error {
	return nil
}
