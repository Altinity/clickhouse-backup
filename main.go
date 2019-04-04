package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/urfave/cli"
)

const BackupTimeFormat = "2006-01-02T15-04-05"

var (
	config    *Config
	version   = "unknown"
	gitCommit = "unknown"
	buildDate = "unknown"

	ErrUnknownClickhouseDataPath = errors.New("clickhouse data path is unknown, you can set data_path in config file")
)

func main() {
	log.SetOutput(os.Stdout)
	cliapp := cli.NewApp()
	cliapp.Name = "clickhouse-backup"
	cliapp.Usage = "Tool for easy backup of ClickHouse with S3 support"
	cliapp.UsageText = "clickhouse-backup <command> [--dry-run] [--table=<db>.<table>] <backup_name>"
	cliapp.Description = "Run as root or clickhouse user"
	cliapp.Version = version

	cliapp.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "/etc/clickhouse-backup/config.yml",
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

	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Println("Version:\t", c.App.Version)
		fmt.Println("Git Commit:\t", gitCommit)
		fmt.Println("Build Date:\t", buildDate)
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
			Name:      "tables",
			Usage:     "Print list of tables and exit",
			UsageText: "clickhouse-backup tables",
			Action: func(c *cli.Context) error {
				return getTables(*config)
			},
			Flags: cliapp.Flags,
		},
		{
			Name:      "list",
			Usage:     "Print list of backups and exit",
			UsageText: "clickhouse-backup list",
			Action: func(c *cli.Context) error {
				fmt.Println("Local backups:")
				printLocalBackups(*config)
				fmt.Println("Backups on S3:")
				printS3Backups(*config)
				return nil
			},
			Flags: cliapp.Flags,
		},
		{
			Name:        "freeze",
			Usage:       "Freeze all or specific tables",
			UsageText:   "clickhouse-backup freeze [--dry-run] [--table=<db>.<table>] <backup_name>",
			Description: "Freeze tables",
			Action: func(c *cli.Context) error {
				return freeze(*config, c.String("t"), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "tables, t",
					Hidden: false,
				},
			),
		},
		{
			Name:        "create",
			Usage:       "Create new backup of all or specific tables",
			UsageText:   "clickhouse-backup create [--dry-run] [--table=<db>.<table>] <backup_name>",
			Description: "Create new backup",
			Action: func(c *cli.Context) error {
				return createBackup(*config, c.Args().Get(0), c.String("t"), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "tables, t",
					Hidden: false,
				},
			),
		},
		{
			Name:      "upload",
			Usage:     "Upload backup to s3",
			UsageText: "clickhouse-backup upload [--dry-run] <backup_name>",
			Action: func(c *cli.Context) error {
				return upload(*config, c.Args().First(), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:      "download",
			Usage:     "Download backup from s3 to backup folder",
			UsageText: "clickhouse-backup download [--dry-run] <backup_name>",
			Action: func(c *cli.Context) error {
				return download(*config, c.Args().First(), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:      "restore-schema",
			Usage:     "Create databases and tables from backup metadata",
			UsageText: "clickhouse-backup restore-schema [--dry-run] [--table=<db>.<table>] <backup_name>",
			Action: func(c *cli.Context) error {
				return restoreSchema(*config, c.Args().First(), c.String("t"), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "tables, t",
					Hidden: false,
				},
			),
		},
		{
			Name:      "restore-data",
			Usage:     "Copy data to 'detached' folder and execute ATTACH",
			UsageText: "clickhouse-backup restore-data [--dry-run] [--table=<db>.<table>] <backup_name>",
			Action: func(c *cli.Context) error {
				return restoreData(*config, c.Args().First(), c.String("t"), c.Bool("dry-run") || c.GlobalBool("dry-run"), c.IntSlice("i"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "tables, t",
					Hidden: false,
				},
				cli.IntSliceFlag{
					Name:   "increments, i",
					Hidden: false,
				},
			),
		},
		{
			Name:  "default-config",
			Usage: "Print default config and exit",
			Action: func(*cli.Context) {
				PrintDefaultConfig()
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "clean",
			Usage: "Clean backup data from shadow folder",
			Action: func(c *cli.Context) error {
				return clean(*config, c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: cliapp.Flags,
		},
	}
	if err := cliapp.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func parseTablePatternForFreeze(tables []Table, tablePattern string) ([]Table, error) {
	if tablePattern == "" {
		return tables, nil
	}
	var result []Table
	for _, t := range tables {
		if matched, _ := filepath.Match(tablePattern, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
			result = append(result, t)
		}
	}
	return result, nil
}

func parseTablePatternForRestoreData(tables map[string]BackupTable, tablePattern string, increments []int) ([]BackupTable, error) {
	if tablePattern == "" {
		tablePattern = "*"
	}
	result := []BackupTable{}
	for _, t := range tables {
		tableName := fmt.Sprintf("%s.%s", t.Database, t.Name)
		if matched, _ := filepath.Match(tablePattern, tableName); matched {
			if len(increments) == 0 {
				result = append(result, t)
				continue
			}
			for _, n := range increments {
				if n == t.Increment {
					result = append(result, t)
					break
				}
			}
		}
	}
	return result, nil
}

func parseTablePatternForRestoreSchema(metadataPath, tablePattern string) ([]RestoreTable, error) {
	regularTables := []RestoreTable{}
	distributedTables := []RestoreTable{}
	filepath.Walk(metadataPath, func(filePath string, info os.FileInfo, err error) error {
		if !strings.HasSuffix(filePath, ".sql") || !info.Mode().IsRegular() {
			return nil
		}
		p := filepath.ToSlash(filePath)
		p = strings.Trim(strings.TrimPrefix(strings.TrimSuffix(p, ".sql"), metadataPath), "/")
		parts := strings.Split(p, "/")
		if len(parts) != 2 {
			return nil
		}
		database, table := parts[0], parts[1]
		if database == "system" {
			return nil
		}
		tableName := fmt.Sprintf("%s.%s", database, table)
		if matched, _ := filepath.Match(tablePattern, tableName); !matched {
			return nil
		}
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}
		restoreTable := RestoreTable{
			Database: database,
			Table:    table,
			Query:    strings.Replace(string(data), "ATTACH", "CREATE", 1),
		}
		if strings.Contains(restoreTable.Query, "ENGINE = Distributed") {
			distributedTables = append(distributedTables, restoreTable)
			return nil
		}
		regularTables = append(regularTables, restoreTable)
		return nil
	})
	return append(regularTables, distributedTables...), nil
}

func getTables(config Config) error {
	ch := &ClickHouse{
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
	for _, table := range allTables {
		if table.Skip {
			fmt.Printf("%s.%s\t(ignored)\n", table.Database, table.Name)
		} else {
			fmt.Printf("%s.%s\n", table.Database, table.Name)
		}
	}
	return nil
}

func restoreSchema(config Config, backupName string, tablePattern string, dryRun bool) error {
	if backupName == "" {
		fmt.Println("Select backup for restore:")
		printLocalBackups(config)
		os.Exit(1)
	}
	if strings.HasSuffix(backupName, ".tar") {
		return fmt.Errorf("extract archive before")
	}

	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	if tablePattern == "" {
		tablePattern = "*"
	}
	metadataPath := path.Join(dataPath, "backup", backupName, "metadata")
	info, err := os.Stat(metadataPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", metadataPath)
	}

	tablesForRestore, err := parseTablePatternForRestoreSchema(metadataPath, tablePattern)
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("No have found schemas by %s in %s", tablePattern, backupName)
	}

	ch := &ClickHouse{
		DryRun: dryRun,
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with: %v", err)
	}
	defer ch.Close()

	for _, schema := range tablesForRestore {
		ch.CreateDatabase(schema.Database)
		if err := ch.CreateTable(schema); err != nil {
			return fmt.Errorf("can't create table '%s.%s' %v", schema.Database, schema.Table, err)
		}
	}
	return nil
}

func printLocalBackups(config Config) error {
	names, err := listLocalBackups(config)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(names) == 0 {
		fmt.Println("No backups found")
	}
	for _, name := range names {
		fmt.Println(name)
	}
	return nil
}

func listLocalBackups(config Config) ([]string, error) {
	dataPath := getDataPath(config)
	if dataPath == "" {
		return nil, ErrUnknownClickhouseDataPath
	}
	backupsPath := path.Join(dataPath, "backup")
	d, err := os.Open(backupsPath)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	return d.Readdirnames(-1)
}

func printS3Backups(config Config) error {
	s3 := &S3{Config: &config.S3}
	if err := s3.Connect(); err != nil {
		return fmt.Errorf("can't connect to s3 with %v", err)
	}

	names, err := s3.BackupList()
	if err != nil {
		return err
	}
	if len(names) == 0 {
		fmt.Println("No backups found")
	}
	for _, name := range names {
		fmt.Println(name)
	}
	return nil
}

func freeze(config Config, tablePattern string, dryRun bool) error {
	ch := &ClickHouse{
		DryRun: dryRun,
		Config: &config.ClickHouse,
	}

	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with: %v", err)
	}
	defer ch.Close()

	dataPath, err := ch.GetDataPath()
	if err != nil || dataPath == "" {
		return fmt.Errorf("can't get data path from clickhouse with: %v\nyou can set data_path in config file", err)
	}

	shadowPath := filepath.Join(dataPath, "shadow")
	files, err := ioutil.ReadDir(shadowPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("can't read %s directory: %v", shadowPath, err)
		}
	} else if len(files) > 0 {
		return fmt.Errorf("%s is not empty, won't execute freeze", shadowPath)
	}

	allTables, err := ch.GetTables()
	if err != nil {
		return fmt.Errorf("can't get Clickhouse tables with: %v", err)
	}
	backupTables, err := parseTablePatternForFreeze(allTables, tablePattern)
	if err != nil {
		return err
	}
	if len(backupTables) == 0 {
		log.Printf("There are no tables in Clickhouse, create something to freeze.")
		return nil
	}
	for _, table := range backupTables {
		if table.Skip {
			fmt.Printf("%s.%s\t(ignored)\n", table.Database, table.Name)
			continue
		}
		if err := ch.FreezeTable(table); err != nil {
			return err
		}
	}
	return nil
}

func NewBackupName() string {
	return time.Now().UTC().Format(BackupTimeFormat)
}

func createBackup(config Config, backupName, tablePattern string, dryRun bool) error {
	if backupName == "" {
		backupName = NewBackupName()
	}
	log.Printf("Create backup '%s'", backupName)
	if err := freeze(config, tablePattern, dryRun); err != nil {
		return err
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupPath := path.Join(dataPath, "backup", backupName)
	if !dryRun {
		if err := os.MkdirAll(backupPath, os.ModePerm); err != nil {
			return err
		}
	}
	log.Println("Copy metadata")
	if err := copyPath(path.Join(dataPath, "metadata"), path.Join(backupPath, "metadata"), dryRun); err != nil {
		return fmt.Errorf("can't backup metadata with %v", err)
	}
	log.Println("  done")

	log.Println("Move shadow")
	backupShadowDir := path.Join(backupPath, "shadow")
	if !dryRun {
		if err := os.MkdirAll(backupShadowDir, os.ModePerm); err != nil {
			return err
		}
	}
	shadowDir := path.Join(dataPath, "shadow")
	d, err := os.Open(shadowDir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		if dryRun {
			continue
		}
		err := os.Rename(filepath.Join(shadowDir, name), filepath.Join(backupShadowDir, name))
		if err != nil {
			return err
		}
	}
	if err := removeOldBackupsLocal(config, dryRun); err != nil {
		return err
	}
	log.Println("  done")
	return nil
}

func restoreData(config Config, backupName string, tablePattern string, dryRun bool, increments []int) error {
	if backupName == "" {
		fmt.Println("Select backup for restore:")
		printLocalBackups(config)
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	ch := &ClickHouse{
		DryRun: dryRun,
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with: %v", err)
	}
	defer ch.Close()

	allTables, err := ch.GetBackupTables(backupName)
	if err != nil {
		return err
	}
	restoreTables, err := parseTablePatternForRestoreData(allTables, tablePattern, increments)
	if err != nil {
		return err
	}
	if len(restoreTables) == 0 {
		log.Printf("Backup doesn't have tables to restore, nothing to do.")
		return nil
	}
	for _, table := range restoreTables {
		if err := ch.CopyData(table); err != nil {
			return fmt.Errorf("can't restore %s.%s increment %d with %v", table.Database, table.Name, table.Increment, err)
		}
		if err := ch.AttachPatritions(table); err != nil {
			return fmt.Errorf("can't attach partitions for table %s.%s with %v", table.Database, table.Name, err)
		}
	}
	return nil
}

func getDataPath(config Config) string {
	if config.ClickHouse.DataPath != "" {
		return config.ClickHouse.DataPath
	}
	ch := &ClickHouse{Config: &config.ClickHouse}
	if err := ch.Connect(); err != nil {
		return ""
	}
	defer ch.Close()
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return ""
	}
	return dataPath
}

func getLocalBackup(config Config, backupName string) error {
	if backupName == "" {
		return fmt.Errorf("backup name is required")
	}
	backupList, err := listLocalBackups(config)
	if err != nil {
		return err
	}
	for _, name := range backupList {
		if name == backupName {
			return nil
		}
	}
	return fmt.Errorf("backup '%s' is not found", backupName)
}

func upload(config Config, backupName string, dryRun bool) error {
	if backupName == "" {
		fmt.Println("Select backup for upload:")
		printLocalBackups(config)
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	s3 := &S3{
		DryRun: dryRun,
		Config: &config.S3,
	}
	if err := s3.Connect(); err != nil {
		return fmt.Errorf("can't connect to s3 with: %v", err)
	}
	if err := getLocalBackup(config, backupName); err != nil {
		return fmt.Errorf("can't upload with %s", err)
	}
	log.Printf("Upload backup '%s'", backupName)
	if config.S3.Strategy == "archive" {
		if err := s3.CompressedStreamUpload(path.Join(dataPath, "backup", backupName), backupName); err != nil {
			return fmt.Errorf("can't upload with %v", err)
		}
	} else {
		if err := s3.UploadDirectory(path.Join(dataPath, "backup", backupName), backupName); err != nil {
			return fmt.Errorf("can't upload with %v", err)
		}
	}
	if err := s3.RemoveOldBackups(config.S3.BackupsToKeepS3); err != nil {
		return fmt.Errorf("can't remove old backups: %v", err)
	}
	log.Println("  done")
	return nil
}

func download(config Config, backupName string, dryRun bool) error {
	if backupName == "" {
		fmt.Println("Select backup for download:")
		printS3Backups(config)
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	s3 := &S3{
		DryRun: dryRun,
		Config: &config.S3,
	}
	if err := s3.Connect(); err != nil {
		return fmt.Errorf("can't connect to s3 with: %v", err)
	}
	if config.S3.Strategy == "archive" {
		return s3.CompressedStreamDownload(backupName, path.Join(dataPath, "backup", backupName))
	}
	return s3.DownloadTree(backupName, path.Join(dataPath, "backup", backupName))
}

func clean(config Config, dryRun bool) error {
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	shadowDir := path.Join(dataPath, "shadow")
	if _, err := os.Stat(shadowDir); os.IsNotExist(err) {
		log.Printf("%s directory does not exist, nothing to do", shadowDir)
		return nil
	}
	log.Printf("Clean %s", shadowDir)
	if !dryRun {
		if err := cleanDir(shadowDir); err != nil {
			return fmt.Errorf("can't remove contents from directory %v: %v", shadowDir, err)
		}
	}
	return nil
}

func removeOldBackupsLocal(config Config, dryRun bool) error {
	if config.S3.BackupsToKeepLocal < 1 {
		return nil
	}
	backupList, err := listLocalBackups(config)
	if err != nil {
		return err
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupsToDelete := GetBackupsToDelete(backupList, config.S3.BackupsToKeepLocal)
	for _, backupName := range backupsToDelete {
		backupPath := path.Join(dataPath, "backup", backupName)
		if dryRun {
			log.Println("Remove ", backupPath)
			continue
		}
		os.RemoveAll(backupPath)
	}
	return nil
}
