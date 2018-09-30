package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/urfave/cli"
)

var config *Config

func main() {

	cliapp := cli.NewApp()
	cliapp.Name = "clickhouse-backup"
	cliapp.Usage = "Backup ClickHouse to s3"
	cliapp.Version = "0.0.1"
	cliapp.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "config.yml",
			Usage: "Config `FILE` name.",
		},
		cli.BoolFlag{
			Name:  "dry-run",
			Usage: "Only show what should be uploaded or downloaded but don't actually do it. May still perform S3 requests to get bucket listings and other information though (only for file transfer commands)",
		},
	}
	cliapp.CommandNotFound = func(c *cli.Context, command string) {
		fmt.Printf("Error. Unknown command: '%s'\n\n", command)
		cli.ShowAppHelpAndExit(c, 1)
	}

	cliapp.Before = func(c *cli.Context) error {
		var err error
		config, err = LoadConfig(c.String("config"))
		if err != nil {
			log.Fatal(err)
		}
		return nil
	}

	cliapp.Commands = []cli.Command{
		{
			Name:      "backup",
			Usage:     "Freeze tables",
			UsageText: "You can set specific tables like db*.tables[1-2]",
			Action: func(c *cli.Context) error {
				return backup(*config, c.Args(), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "upload",
			Usage: "Upload freezed tables to s3",
			Action: func(c *cli.Context) error {
				return upload(*config, c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:   "download",
			Usage:  "NOT IMPLEMENTED! Download tables from s3 to rigth path",
			Action: func(c *cli.Context) error {
				s3 := &S3{
					DryRun: c.Bool("dry-run") || c.GlobalBool("dry-run"),
					Config: &config.S3,
				}
				if err := s3.Connect(); err != nil {
					return fmt.Errorf("can't connect to s3 with: %v", err)
				}
				return s3.Download("metadata", path.Join(config.ClickHouse.DataPath, "metadata"))
			},
			Flags:  cliapp.Flags,
		},
		{
			Name:   "restore",
			Usage:  "NOT IMPLEMENTED! Restore downloaded data",
			Action: CmdNotImplemented,
			Flags:  cliapp.Flags,
		},
		{
			Name:  "default-config",
			Usage: "Print default config and exit",
			Action: func(*cli.Context) {
				PrintDefaultConfig()
			},
			Flags: cliapp.Flags,
		},
	}
	if err := cliapp.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func parseArgs(tables []Table, args []string) ([]Table, error) {
	var result []Table
	for _, arg := range args {
		for _, t := range tables {
			if matched, _ := filepath.Match(arg, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				result = append(result, t)
				continue
			}
			return nil, fmt.Errorf("table '%s' not found", arg)
		}
	}
	return result, nil
}

func CmdNotImplemented(*cli.Context) error {
	return fmt.Errorf("Command not implemented")
}

func backup(config Config, args []string, dryRun bool) error {
	ch := &ClickHouse{
		DryRun: dryRun,
		Config: &config.ClickHouse,
	}

	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with: %v", err)
	}
	defer ch.Close()

	allTables, err := ch.GetTables()
	if err != nil {
		return fmt.Errorf("can't get tables with: %v", err)
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

func upload(config Config, dryRun bool) error {
	dataPath := config.ClickHouse.DataPath
	if dataPath == "" {
		ch := &ClickHouse{
			DryRun: dryRun,
			Config: &config.ClickHouse,
		}
		if err := ch.Connect(); err != nil {
			return fmt.Errorf("can't connect to clickouse for get data path with: %v\nyou can set clickhouse.data_path in config", err)
		}
		defer ch.Close()
		var err error
		if dataPath, err = ch.GetDataPath(); err != nil || dataPath == "" {
			return fmt.Errorf("can't get data path from clickhouse with: %v\nyou can set data_path in config file", err)
		}
	}
	s3 := &S3{
		DryRun: dryRun,
		Config: &config.S3,
	}
	if err := s3.Connect(); err != nil {
		return fmt.Errorf("can't connect to s3 with: %v", err)
	}
	log.Printf("upload metadata")
	if err := s3.Upload(path.Join(dataPath, "metadata"), "metadata"); err != nil {
		return fmt.Errorf("can't upload metadata to s3 with: %v", err)
	}
	log.Printf("upload data")
	if err := s3.Upload(path.Join(dataPath, "shadow"), "shadow"); err != nil {
		return fmt.Errorf("can't upload metadata to s3 with: %v", err)
	}
	return nil
}
