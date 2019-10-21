package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/urfave/cli"
)

const (
	// BackupTimeFormat - default backup name
	BackupTimeFormat  = "2006-01-02T15-04-05"
	defaultConfigPath = "/etc/clickhouse-backup/config.yml"
)

var (
	version   = "unknown"
	gitCommit = "unknown"
	buildDate = "unknown"
	// ErrUnknownClickhouseDataPath -
	ErrUnknownClickhouseDataPath = errors.New("clickhouse data path is unknown, you can set data_path in config file")
)

func main() {
	log.SetOutput(os.Stdout)
	cliapp := cli.NewApp()
	cliapp.Name = "clickhouse-backup"
	cliapp.Usage = "Tool for easy backup of ClickHouse with S3 support"
	cliapp.UsageText = "clickhouse-backup <command> [-t, --tables=<db>.<table>] <backup_name>"
	cliapp.Description = "Run as root or clickhouse user"
	cliapp.Version = version

	cliapp.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: defaultConfigPath,
			Usage: "Config `FILE` name.",
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

	cliapp.Commands = []cli.Command{
		{
			Name:      "tables",
			Usage:     "Print list of tables and exit",
			UsageText: "clickhouse-backup tables",
			Action: func(c *cli.Context) error {
				return getTables(*getConfig(c))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:      "list",
			Usage:     "Print list of backups and exit",
			UsageText: "clickhouse-backup list [all|local|s3] [latest|penult]",
			Action: func(c *cli.Context) error {
				config := getConfig(c)
				switch c.Args().Get(0) {
				case "local":
					return printLocalBackups(*config, c.Args().Get(1))
				case "s3":
					return printS3Backups(*config, c.Args().Get(1))
				case "all", "":
					fmt.Println("Local backups:")
					if err := printLocalBackups(*config, c.Args().Get(1)); err != nil {
						return err
					}
					fmt.Println("Backups on S3:")
					if err := printS3Backups(*config, c.Args().Get(1)); err != nil {
						return err
					}
				default:
					fmt.Fprintf(os.Stderr, "Unknown command '%s'\n", c.Args().Get(0))
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				return nil
			},
			Flags: cliapp.Flags,
		},
		{
			Name:      "delete",
			Usage:     "Delete specific backup",
			UsageText: "clickhouse-backup delete <local|s3> <backup_name>",
			Action: func(c *cli.Context) error {
				config := getConfig(c)
				if c.Args().Get(1) == "" {
					fmt.Fprintln(os.Stderr, "Backup name must be defined")
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				switch c.Args().Get(0) {
				case "local":
					return removeBackupLocal(*config, c.Args().Get(1))
				case "s3":
					return removeBackupS3(*config, c.Args().Get(1))
				default:
					fmt.Fprintf(os.Stderr, "Unknown command '%s'\n", c.Args().Get(0))
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				return nil
			},
			Flags: cliapp.Flags,
		},
		{
			Name:        "freeze",
			Usage:       "Freeze all or specific tables",
			UsageText:   "clickhouse-backup freeze [-t, --tables=<db>.<table>] <backup_name>",
			Description: "Freeze tables",
			Action: func(c *cli.Context) error {
				return freeze(*getConfig(c), c.String("t"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
				},
			),
		},
		{
			Name:        "create",
			Usage:       "Create new backup of all or specific tables",
			UsageText:   "clickhouse-backup create [-t, --tables=<db>.<table>] <backup_name>",
			Description: "Create new backup",
			Action: func(c *cli.Context) error {
				return createBackup(*getConfig(c), c.Args().First(), c.String("t"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
				},
			),
		},
		{
			Name:      "upload",
			Usage:     "Upload backup to s3",
			UsageText: "clickhouse-backup upload [--diff-from=<backup_name>] <backup_name>",
			Action: func(c *cli.Context) error {
				return upload(*getConfig(c), c.Args().First(), c.String("diff-from"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "diff-from",
					Hidden: false,
				},
			),
		},
		{
			Name:      "download",
			Usage:     "Download backup from s3 to backup folder",
			UsageText: "clickhouse-backup download <backup_name>",
			Action: func(c *cli.Context) error {
				return download(*getConfig(c), c.Args().First())
			},
			Flags: cliapp.Flags,
		},
		{
			Name:      "restore-schema",
			Usage:     "Create databases and tables from backup metadata",
			UsageText: "clickhouse-backup restore-schema [-t, --tables=<db>.<table>] <backup_name>",
			Action: func(c *cli.Context) error {
				return restoreSchema(*getConfig(c), c.Args().First(), c.String("t"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
				},
			),
		},
		{
			Name:      "restore-data",
			Usage:     "Copy data to 'detached' folder and execute ATTACH",
			UsageText: "clickhouse-backup restore-data [-t, --tables=<db>.<table>] <backup_name>",
			Action: func(c *cli.Context) error {
				return restoreData(*getConfig(c), c.Args().First(), c.String("t"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
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
				return clean(*getConfig(c))
			},
			Flags: cliapp.Flags,
		},
	}
	if err := cliapp.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func addTable(tables []Table, table Table) []Table {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Name == table.Name) {
			return tables
		}
	}
	return append(tables, table)
}

func addBackupTable(tables []BackupTable, table BackupTable) []BackupTable {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Name == table.Name) {
			return tables
		}
	}
	return append(tables, table)
}

func addRestoreTable(tables []RestoreTable, table RestoreTable) []RestoreTable {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Table == table.Table) {
			return tables
		}
	}
	return append(tables, table)
}

func parseTablePatternForFreeze(tables []Table, tablePattern string) ([]Table, error) {
	if tablePattern == "" {
		return tables, nil
	}
	tablePatterns := strings.Split(tablePattern, ",")
	var result []Table
	for _, t := range tables {
		for _, pattern := range tablePatterns {
			if matched, _ := filepath.Match(pattern, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				result = addTable(result, t)
			}
		}
	}
	return result, nil
}

func parseTablePatternForRestoreData(tables map[string]BackupTable, tablePattern string) ([]BackupTable, error) {
	tablePatterns := []string{"*"}
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	result := []BackupTable{}
	for _, t := range tables {
		for _, pattern := range tablePatterns {
			tableName := fmt.Sprintf("%s.%s", t.Database, t.Name)
			if matched, _ := filepath.Match(pattern, tableName); matched {
				result = addBackupTable(result, t)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return (result[i].Database < result[j].Database) || (result[i].Database == result[j].Database && result[i].Name < result[j].Name)
	})
	return result, nil
}

func parseSchemaPattern(metadataPath string, tablePattern string) ([]RestoreTable, error) {
	regularTables := []RestoreTable{}
	distributedTables := []RestoreTable{}
	tablePatterns := []string{"*"}
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
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
		database, _ := url.PathUnescape(parts[0])
		table, _ := url.PathUnescape(parts[1])
		tableName := fmt.Sprintf("%s.%s", database, table)
		for _, p := range tablePatterns {
			if matched, _ := filepath.Match(p, tableName); matched {
				data, err := ioutil.ReadFile(filePath)
				if err != nil {
					return err
				}
				restoreTable := RestoreTable{
					Database: database,
					Table:    table,
					Query:    strings.Replace(string(data), "ATTACH", "CREATE", 1),
					Path:     filePath,
				}
				if strings.Contains(restoreTable.Query, "ENGINE = Distributed") {
					distributedTables = addRestoreTable(distributedTables, restoreTable)
					return nil
				}
				regularTables = addRestoreTable(regularTables, restoreTable)
				return nil
			}
		}
		return nil
	})
	result := append(regularTables, distributedTables...)
	sort.Slice(result, func(i, j int) bool {
		return (result[i].Database < result[j].Database) || (result[i].Database == result[j].Database && result[i].Table < result[j].Table)
	})
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
		if table.Skip {
			fmt.Printf("%s.%s\t(ignored)\n", table.Database, table.Name)
		} else {
			fmt.Printf("%s.%s\n", table.Database, table.Name)
		}
	}
	return nil
}

func restoreSchema(config Config, backupName string, tablePattern string) error {
	if backupName == "" {
		fmt.Println("Select backup for restore:")
		printLocalBackups(config, "all")
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	metadataPath := path.Join(dataPath, "backup", backupName, "metadata")
	info, err := os.Stat(metadataPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", metadataPath)
	}
	tablesForRestore, err := parseSchemaPattern(metadataPath, tablePattern)
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("No have found schemas by %s in %s", tablePattern, backupName)
	}
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with %v", err)
	}
	defer ch.Close()

	for _, schema := range tablesForRestore {
		ch.CreateDatabase(schema.Database)
		if err := ch.CreateTable(schema); err != nil {
			return fmt.Errorf("can't create table `%s`.`%s` %v", schema.Database, schema.Table, err)
		}
	}
	return nil
}

func printBackups(backupList []Backup, format string, printSize bool) error {
	switch format {
	case "latest", "last", "l":
		if len(backupList) < 1 {
			return fmt.Errorf("No backups found")
		}
		fmt.Println(backupList[len(backupList)-1].Name)
	case "penult", "prev", "previous", "p":
		if len(backupList) < 2 {
			return fmt.Errorf("No penult backup is found")
		}
		fmt.Println(backupList[len(backupList)-2].Name)
	case "all", "":
		if len(backupList) == 0 {
			fmt.Println("No backups found")
		}
		for _, backup := range backupList {
			if printSize {
				fmt.Printf("- '%s'\t%s\t(created at %s)\n", backup.Name, FormatBytes(backup.Size), backup.Date.Format("02-01-2006 15:04:05"))
			} else {
				fmt.Printf("- '%s'\t(created at %s)\n", backup.Name, backup.Date.Format("02-01-2006 15:04:05"))
			}
		}
	default:
		return fmt.Errorf("'%s' undefined", format)
	}
	return nil
}

func printLocalBackups(config Config, format string) error {
	backupList, err := listLocalBackups(config)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return printBackups(backupList, format, false)
}

func listLocalBackups(config Config) ([]Backup, error) {
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
	result := []Backup{}
	names, err := d.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		info, err := os.Stat(path.Join(backupsPath, name))
		if err != nil {
			continue
		}
		if !info.IsDir() {
			continue
		}
		result = append(result, Backup{
			Name: name,
			Date: info.ModTime(),
		})
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Date.Before(result[j].Date)
	})
	return result, nil
}

func printS3Backups(config Config, format string) error {
	s3 := &S3{Config: &config.S3}
	if err := s3.Connect(); err != nil {
		return fmt.Errorf("can't connect to s3 with %v", err)
	}
	backupList, err := s3.BackupList()
	if err != nil {
		return err
	}
	return printBackups(backupList, format, true)
}

func freeze(config Config, tablePattern string) error {
	ch := &ClickHouse{
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
		return fmt.Errorf("There are no tables in Clickhouse, create something to freeze")
	}
	for _, table := range backupTables {
		if table.Skip {
			log.Printf("Skip `%s`.`%s`", table.Database, table.Name)
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

func createBackup(config Config, backupName, tablePattern string) error {
	if backupName == "" {
		backupName = NewBackupName()
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupPath := path.Join(dataPath, "backup", backupName)
	if _, err := os.Stat(backupPath); err == nil || !os.IsNotExist(err) {
		return fmt.Errorf("can't create backup with '%s' already exists", backupPath)
	}
	if err := os.MkdirAll(backupPath, os.ModePerm); err != nil {
		return fmt.Errorf("can't create backup with %v", err)
	}
	log.Printf("Create backup '%s'", backupName)
	if err := freeze(config, tablePattern); err != nil {
		return err
	}
	log.Println("Copy metadata")
	schemaList, err := parseSchemaPattern(path.Join(dataPath, "metadata"), tablePattern)
	if err != nil {
		return err
	}
	for _, schema := range schemaList {
		skip := false
		for _, filter := range config.ClickHouse.SkipTables {
			if matched, _ := filepath.Match(filter, fmt.Sprintf("%s.%s", schema.Database, schema.Table)); matched {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		relativePath := strings.Trim(strings.TrimPrefix(schema.Path, path.Join(dataPath, "metadata")), "/")
		newPath := path.Join(backupPath, "metadata", relativePath)
		if err := copyFile(schema.Path, newPath); err != nil {
			return fmt.Errorf("can't backup metadata with %v", err)
		}
	}
	log.Println("  Done.")

	log.Println("Move shadow")
	backupShadowDir := path.Join(backupPath, "shadow")
	if err := os.MkdirAll(backupShadowDir, os.ModePerm); err != nil {
		return err
	}
	shadowDir := path.Join(dataPath, "shadow")
	if err := moveShadow(shadowDir, backupShadowDir); err != nil {
		return err
	}
	if err := removeOldBackupsLocal(config); err != nil {
		return err
	}
	log.Println("  Done.")
	return nil
}

func restoreData(config Config, backupName string, tablePattern string) error {
	if backupName == "" {
		fmt.Println("Select backup for restore:")
		printLocalBackups(config, "all")
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with: %v", err)
	}
	defer ch.Close()

	allBackupTables, err := ch.GetBackupTables(backupName)
	if err != nil {
		return err
	}
	restoreTables, err := parseTablePatternForRestoreData(allBackupTables, tablePattern)
	if err != nil {
		return err
	}
	chTables, err := ch.GetTables()
	if err != nil {
		return err
	}
	if len(restoreTables) == 0 {
		return fmt.Errorf("Backup doesn't have tables to restore")
	}
	allTablesCreated := true
	for _, restoreTable := range restoreTables {
		found := false
		for _, chTable := range chTables {
			if (restoreTable.Database == chTable.Database) && (restoreTable.Name == chTable.Name) {
				found = true
				break
			}
		}
		if !found {
			log.Printf("`%s`.`%s` is not created", restoreTable.Database, restoreTable.Name)
			allTablesCreated = false
		}
	}
	if !allTablesCreated {
		return fmt.Errorf("run 'restore-schema' first")
	}
	for _, table := range restoreTables {
		if err := ch.CopyData(table); err != nil {
			return fmt.Errorf("can't restore `%s`.`%s` with %v", table.Database, table.Name, err)
		}
		if err := ch.AttachPatritions(table); err != nil {
			return fmt.Errorf("can't attach partitions for table '%s.%s' with %v", table.Database, table.Name, err)
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
	for _, backup := range backupList {
		if backup.Name == backupName {
			return nil
		}
	}
	return fmt.Errorf("backup '%s' not found", backupName)
}

func upload(config Config, backupName string, diffFrom string) error {
	if backupName == "" {
		fmt.Println("Select backup for upload:")
		printLocalBackups(config, "all")
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	s3 := &S3{
		Config: &config.S3,
	}
	if err := s3.Connect(); err != nil {
		return fmt.Errorf("can't connect to s3 with: %v", err)
	}
	if err := getLocalBackup(config, backupName); err != nil {
		return fmt.Errorf("can't upload with %s", err)
	}
	backupPath := path.Join(dataPath, "backup", backupName)
	log.Printf("Upload backup '%s'", backupName)
	diffFromPath := ""
	if diffFrom != "" {
		diffFromPath = path.Join(dataPath, "backup", diffFrom)
	}
	if err := s3.CompressedStreamUpload(backupPath, backupName, diffFromPath); err != nil {
		return fmt.Errorf("can't upload with %v", err)
	}
	if err := s3.RemoveOldBackups(config.S3.BackupsToKeepS3); err != nil {
		return fmt.Errorf("can't remove old backups: %v", err)
	}
	log.Println("  Done.")
	return nil
}

func download(config Config, backupName string) error {
	if backupName == "" {
		fmt.Println("Select backup for download:")
		printS3Backups(config, "all")
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	s3 := &S3{
		Config: &config.S3,
	}
	if err := s3.Connect(); err != nil {
		return fmt.Errorf("can't connect to s3 with: %v", err)
	}
	err := s3.CompressedStreamDownload(backupName, path.Join(dataPath, "backup", backupName))
	if err != nil {
		return err
	}
	log.Println("  Done.")
	return nil
}

func clean(config Config) error {
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
	if err := cleanDir(shadowDir); err != nil {
		return fmt.Errorf("can't remove contents from directory %v: %v", shadowDir, err)
	}
	return nil
}

func removeOldBackupsLocal(config Config) error {
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
	for _, backup := range backupsToDelete {
		backupPath := path.Join(dataPath, "backup", backup.Name)
		os.RemoveAll(backupPath)
	}
	return nil
}

func removeBackupLocal(config Config, backupName string) error {
	backupList, err := listLocalBackups(config)
	if err != nil {
		return err
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	for _, backup := range backupList {
		if backup.Name == backupName {
			return os.RemoveAll(path.Join(dataPath, "backup", backupName))
		}
	}
	return fmt.Errorf("backup '%s' not found", backupName)
}

func removeBackupS3(config Config, backupName string) error {
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	s3 := &S3{Config: &config.S3}
	if err := s3.Connect(); err != nil {
		return fmt.Errorf("can't connect to s3 with: %v", err)
	}
	backupList, err := s3.BackupList()
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.Name == backupName {
			return s3.RemoveBackup(backupName)
		}
	}
	return fmt.Errorf("backup '%s' not found on s3", backupName)
}

func getConfig(ctx *cli.Context) *Config {
	configPath := ctx.String("config")
	if configPath == defaultConfigPath {
		configPath = ctx.GlobalString("config")
	}

	config, err := LoadConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}

	return config
}
