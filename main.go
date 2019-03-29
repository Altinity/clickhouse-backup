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

const (
	BackupTimeFormat = "2006-01-02T15-04-05"
)

var (
	config    *Config
	version   = "unknown"
	gitCommit = "unknown"
	buildDate = "unknown"

	ErrUnknownClickhouseDataPath = errors.New("clickhouse data path is unknown, you can set data_path in config file")
)

func main() {
	cliapp := cli.NewApp()
	cliapp.Name = "clickhouse-backup"
	cliapp.Usage = "Backup ClickHouse to s3"
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
			Name:  "tables",
			Usage: "Print all tables and exit",
			Action: func(c *cli.Context) error {
				return getTables(*config)
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "list",
			Usage: "Print backups list and exit",
			Action: func(c *cli.Context) error {
				fmt.Println("Local backups:")
				printLocalBackups(*config, "any")
				fmt.Println("Backups on S3:")
				printS3Backups(*config)
				return nil
			},
			Flags: cliapp.Flags,
		},
		{
			Name:        "freeze",
			Usage:       "Freeze all or specific tables. You can specify tables via flag -t db.[table]",
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
			Usage:       "Create new backup of all or specific tables. You can specify tables via flag -t [db].[table]",
			Description: "Create new backup",
			Action: func(c *cli.Context) error {
				return createBackup(*config, c.String("t"), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "tables, t",
					Hidden: false,
				},
			),
		},
		{
			Name:  "upload",
			Usage: "Upload backup to s3.",
			Action: func(c *cli.Context) error {
				return upload(*config, c.Args().First(), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "download",
			Usage: "Download backup from s3 to backup folder",
			Action: func(c *cli.Context) error {
				return download(*config, c.Args().First(), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "restore-schema",
			Usage: "Create databases and tables from backup metadata",
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
			Name:  "restore-data",
			Usage: "Copy data from 'backup' to 'detached' folder and execute ATTACH. You can specify tables like 'db.[table]' via flag -t and increments via -i flag",
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
		{
			Name:  "extract",
			Usage: "Extract archive",
			Action: func(c *cli.Context) error {
				return extract(*config, c.Args().First(), c.Bool("dry-run") || c.GlobalBool("dry-run"))
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
		fmt.Printf("%s.%s\n", table.Database, table.Name)
	}
	return nil
}

func restoreSchema(config Config, backupName string, tablePatten string, dryRun bool) error {
	if strings.HasSuffix(backupName, ".tar") {
		return fmt.Errorf("extract archive before")
	}
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

	metadataPath := path.Join(dataPath, "backup", backupName, "metadata")
	log.Printf("Will analyze restored metadata from here: %s", metadataPath)

	// for each dir in metadataPath (database name)
	// except system execute scripts
	files, err := ioutil.ReadDir(metadataPath)
	if err != nil {
		return fmt.Errorf("can't read metadata directory for creating tables: %v", err)
	}

	var distributedTables []RestoreTable
	for _, file := range files {
		if file.IsDir() {
			databaseName := file.Name()
			if databaseName == "system" {
				// do not touch system database
				continue
			}
			log.Printf("Found metadata files for database: %s", databaseName)
			ch.CreateDatabase(databaseName)
			databaseDir := path.Join(metadataPath, databaseName)
			log.Printf("Will analyze table information from here: %s", databaseDir)
			tableFiles, err := ioutil.ReadDir(databaseDir)
			if err != nil {
				return fmt.Errorf("can't read database directory in metadata dir: %v", err)
			}
			for _, table := range tableFiles {
				if strings.HasSuffix(table.Name(), "sql") {
					tablePath := path.Join(databaseDir, table.Name())
					log.Printf("Found table: %s", tablePath)
					dat, err := ioutil.ReadFile(tablePath)
					if err != nil {
						return fmt.Errorf("can't read file %s: %v", tablePath, err)
					}
					tableCreateQuery := strings.Replace(string(dat), "ATTACH", "CREATE", 1)

					if strings.Contains(tableCreateQuery, "ENGINE = Distributed") {
						// distributed engine tables should be created last
						// because they are based on real tables
						log.Printf("This is a distributed table, saving for later")
						distributedTables = append(distributedTables, RestoreTable{
							Database: databaseName,
							Query:    tableCreateQuery,
						})
					} else {
						if err := ch.CreateTable(RestoreTable{
							Database: databaseName,
							Query:    tableCreateQuery,
						}); err != nil {
							log.Printf("ERROR Table creation failed: %v", err)
							// continue to other tables
						}
					}
				}
			}
		}
	}
	log.Printf("Creating distributed tables")
	for _, table := range distributedTables {
		if err := ch.CreateTable(table); err != nil {
			log.Printf("ERROR Table creation failed: %v", err) // continue to other tables
		}
	}
	return nil
}

func printLocalBackups(config Config, backupType string) error {
	names, err := listLocalBackups(config)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(names) == 0 {
		fmt.Println("No backups found")
	}
	for _, name := range names {
		switch backupType {
		case "tree":
			if !strings.HasSuffix(name, ".tar") {
				fmt.Println(name)
			}
		case "archive":
			if strings.HasSuffix(name, ".tar") {
				fmt.Println(name)
			}
		default:
			fmt.Println(name)
		}
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
		if err := ch.FreezeTable(table); err != nil {
			return err
		}
	}
	return nil
}

func NewBackupName() string {
	return time.Now().UTC().Format(BackupTimeFormat)
}

func createBackup(config Config, tablePattern string, dryRun bool) error {
	backupName := NewBackupName()
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
	log.Println("  done")

	if config.Backup.Strategy == "archive" {
		log.Printf("Create '%s.tar'", backupName)
		file, err := os.Create(fmt.Sprintf("%s.tar", backupPath))
		if err != nil {
			return err
		}
		defer file.Close()
		if err := TarDirs(file, backupPath); err != nil {
			return fmt.Errorf("error achiving data with: %v", err)
		}
		if err := os.RemoveAll(backupPath); err != nil {
			return err
		}
	}
	return removeOldBackupsLocal(config, dryRun)
}

func extract(config Config, backupName string, dryRun bool) error {
	if backupName == "" {
		fmt.Println("Select backup for extract:")
		printLocalBackups(config, "archive")
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupArchivePath := path.Join(dataPath, "backup", backupName)
	backupName = strings.TrimSuffix(backupName, ".tar")
	archiveFile, err := os.Open(backupArchivePath)
	if err != nil {
		return fmt.Errorf("error opening archive: %v", err)
	}
	defer archiveFile.Close()
	if err := Untar(archiveFile, path.Join(dataPath, "backup")); err != nil {
		return fmt.Errorf("error unarchiving: %v", err)
	}
	return nil
}

func restoreData(config Config, backupName string, tablePattern string, dryRun bool, increments []int) error {
	if backupName == "" {
		fmt.Println("Select backup for restore:")
		printLocalBackups(config, "tree")
		os.Exit(1)
	}
	if strings.HasSuffix(backupName, ".tar") {
		return fmt.Errorf("extract archive before")
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
		printLocalBackups(config, "any")
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
	if err := s3.UploadDirectory(path.Join(dataPath, "backup", backupName), backupName); err != nil {
		return fmt.Errorf("can't upload with %v", err)
	}

	if err := s3.RemoveOldBackups(config.Backup.BackupsToKeepS3); err != nil {
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
	if strings.HasSuffix(backupName, ".tar") {
		return s3.DownloadArchive(backupName, path.Join(dataPath, "backup"))
	}
	if err := s3.DownloadTree(backupName, path.Join(dataPath, "backup", backupName)); err != nil {
		return fmt.Errorf("cat't download '%s' from s3 with %v", backupName, err)
	}
	return nil
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
	if config.Backup.BackupsToKeepLocal < 1 {
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
	backupsToDelete := GetBackupsToDelete(backupList, config.Backup.BackupsToKeepLocal)
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
