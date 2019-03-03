package main

import (
	"fmt"
	"github.com/urfave/cli"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
)

var config *Config

func main() {
	cliapp := cli.NewApp()
	cliapp.Name = "clickhouse-backup"
	cliapp.Usage = "Backup ClickHouse to s3"
	cliapp.Version = "0.0.2"
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
		fmt.Println(c.App.Version)
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
				return getTables(*config, c.Args())
			},
			Flags: cliapp.Flags,
		},
		{
			Name:        "freeze",
			Usage:       "Freeze all or specific tables. You may use this syntax for specify tables [db].[table]",
			Description: "Freeze tables",
			Action: func(c *cli.Context) error {
				return freeze(*config, c.Args(), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "upload",
			Usage: "Upload 'metadata' and 'shadows' directories to s3. Extra files on s3 will be deleted",
			Action: func(c *cli.Context) error {
				return upload(*config, c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "download",
			Usage: "Download 'metadata' and 'shadows' from s3 to backup folder",
			Action: func(c *cli.Context) error {
				return download(*config, c.Args(), c.Bool("dry-run") || c.GlobalBool("dry-run"))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "create-tables",
			Usage: "NOT IMPLEMENTED! Create tables from backup metadata",
			Action: func(c *cli.Context) error {
				return fmt.Errorf("NOT IMPLEMENTED!")
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "restore",
			Usage: "Copy data from 'backup' to 'detached' folder and execute ATTACH. You can specify tables [db].[table] and increments via -i flag",
			Action: func(c *cli.Context) error {
				return restore(*config, c.Args(), c.Bool("dry-run") || c.GlobalBool("dry-run"), c.IntSlice("i"), c.Bool("d"))
			},
			Flags: append(cliapp.Flags,
				cli.IntSliceFlag{
					Name:   "increments, i",
					Hidden: false,
				},
				cli.BoolFlag{
					Name:   "deprecated, d",
					Hidden: false,
					Usage:  "Set this flag if Table was created of deprecated method: ENGINE = MergeTree(Date, (TimeStamp, Log), 8192)",
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

func parseArgsForFreeze(tables []Table, args []string) ([]Table, error) {
	if len(args) == 0 {
		return tables, nil
	}
	var result []Table
	for _, arg := range args {
		for _, t := range tables {
			if matched, _ := filepath.Match(arg, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				result = append(result, t)
			}
		}
	}
	return result, nil
}

func parseArgsForRestore(tables map[string]BackupTable, args []string, increments []int) ([]BackupTable, error) {
	if len(args) == 0 {
		args = []string{"*"}
	}
	result := []BackupTable{}
	for _, arg := range args {
		for _, t := range tables {
			tableName := fmt.Sprintf("%s.%s", t.Database, t.Name)
			if matched, _ := filepath.Match(arg, tableName); matched {
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
	}
	return result, nil
}

func parseArgsForDownload(args []string) (filename string) {
	if len(args) == 1 {
		filename = args[0]
	}
	return
}

func getTables(config Config, args []string) error {
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

func freeze(config Config, args []string, dryRun bool) error {
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
	backupTables, err := parseArgsForFreeze(allTables, args)
	if err != nil {
		return err
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

func restore(config Config, args []string, dryRun bool, increments []int, deprecatedCreation bool) error {
	ch := &ClickHouse{
		DryRun: dryRun,
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with: %v", err)
	}
	defer ch.Close()
	allTables, err := ch.GetBackupTables()
	if err != nil {
		return err
	}
	restoreTables, err := parseArgsForRestore(allTables, args, increments)
	if err != nil {
		return err
	}
	if len(restoreTables) == 0 {
		return fmt.Errorf("didn't find tables to restore")
	}
	for _, table := range restoreTables {
		// TODO: Use move instead copy
		if err := ch.CopyData(table); err != nil {
			return fmt.Errorf("can't restore %s.%s increment %d with %v", table.Database, table.Name, table.Increment, err)
		}
		if err := ch.AttachPatritions(table, deprecatedCreation); err != nil {
			return fmt.Errorf("can't attach partitions for table %s.%s with %v", table.Database, table.Name, err)
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
			return fmt.Errorf("can't connect to clickhouse to get data path with: %v\nyou can set clickhouse.data_path in config", err)
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
	backupStrategy := config.Backup.Strategy
	switch backupStrategy {
	case "tree":
		err := uploadTree(s3, dataPath)
		if err != nil {
			return err
		}
	case "archive":
		err := uploadArchive(s3, dataPath)
		if err != nil {
			return err
		}
		if err := removeOldBackups(config, s3); err != nil {
			return fmt.Errorf("can't remove old backups: %v", err)
		}
	default:
		return fmt.Errorf("unsupported backup strategy")
	}
	return nil
}

func uploadTree(s3 *S3, dataPath string) error {
	log.Printf("upload metadata")
	if err := s3.UploadDirectory(path.Join(dataPath, "metadata"), "metadata"); err != nil {
		return fmt.Errorf("can't upload metadata: %v", err)
	}
	log.Printf("upload data")
	if err := s3.UploadDirectory(path.Join(dataPath, "shadow"), "shadow"); err != nil {
		return fmt.Errorf("can't upload data: %v", err)
	}
	return nil
}

func uploadArchive(s3 *S3, dataPath string) error {
	file, err := ioutil.TempFile("", "*.tar")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	log.Printf("archive data")
	if err = TarDirs(file, path.Join(dataPath, "shadow"), path.Join(dataPath, "metadata")); err != nil {
		return fmt.Errorf("error achiving data with: %v", err)
	}
	log.Printf("upload data")
	if err := s3.UploadFile(file.Name(), filepath.Base(file.Name())); err != nil {
		return fmt.Errorf("can't upload archive to s3 with: %v", err)
	}
	return nil
}

func download(config Config, args []string, dryRun bool) error {
	dataPath := config.ClickHouse.DataPath
	if dataPath == "" {
		ch := &ClickHouse{
			DryRun: dryRun,
			Config: &config.ClickHouse,
		}
		if err := ch.Connect(); err != nil {
			return fmt.Errorf("can't connect to clickhouse for get data path with: %v\nyou can set clickhouse.data_path in config", err)
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
	backupStrategy := config.Backup.Strategy
	switch backupStrategy {
	case "tree":
		err := downloadTree(s3, dataPath)
		if err != nil {
			return err
		}
	case "archive":
		filename := parseArgsForDownload(args)
		if filename == "" {
			return fmt.Errorf("an argument needs to be passed to download with archive strategy")
		}
		err := downloadArchive(s3, dataPath, filename)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported backup strategy")
	}
	return nil
}

func downloadTree(s3 *S3, dataPath string) error {
	if err := s3.DownloadTree("metadata", path.Join(dataPath, "backup", "metadata")); err != nil {
		return fmt.Errorf("cat't download metadata from s3 with %v", err)
	}
	if err := s3.DownloadTree("shadow", path.Join(dataPath, "backup", "shadow")); err != nil {
		return fmt.Errorf("can't download shadow from s3 with %v", err)
	}
	return nil
}

func downloadArchive(s3 *S3, dataPath string, filename string) error {
	if err := s3.DownloadTree("metadata", path.Join(dataPath, "backup", "metadata")); err != nil {
		return fmt.Errorf("cat't download metadata from s3 with %v", err)
	}
	dstPath := path.Join(dataPath, "backup")
	err := s3.DownloadArchive(filename, dstPath)
	if err != nil {
		return fmt.Errorf("error downloading shadow from s3 with %v", err)
	}
	archivePath := filepath.Join(dstPath, filepath.Base(filename))
	defer os.Remove(archivePath)
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("error opening archive: %v", err)
	}
	if err := Untar(archiveFile, dstPath); err != nil {
		return fmt.Errorf("error unarchiving: %v", err)
	}
	return nil
}

func clean(config Config, dryRun bool) error {
	dataPath := config.ClickHouse.DataPath
	if dataPath == "" {
		ch := &ClickHouse{
			DryRun: dryRun,
			Config: &config.ClickHouse,
		}
		if err := ch.Connect(); err != nil {
			return fmt.Errorf("can't connect to clickhouse to get data path with: %v\nyou can set clickhouse.data_path in config", err)
		}
		defer ch.Close()
		var err error
		if dataPath, err = ch.GetDataPath(); err != nil || dataPath == "" {
			return fmt.Errorf("can't get data path from clickhouse with: %v\nyou can set data_path in config file", err)
		}
	}
	shadowDir := path.Join(dataPath, "shadow")
	log.Printf("remove contents from directory %v", shadowDir)
	if !dryRun {
		if err := cleanDir(shadowDir); err != nil {
			return fmt.Errorf("can't remove contents from directory %v: %v", shadowDir, err)
		}
	}
	return nil
}

func cleanDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

func removeOldBackups(config Config, s3 *S3) error {
	objects, err := s3.ListObjects(config.S3.Path)
	if err != nil {
		return err
	}
	backupsToDelete := len(objects) - config.Backup.BackupsToKeep
	if backupsToDelete > 0 {
		sort.Slice(objects, func(i, j int) bool {
			return objects[i].LastModified.Sub(*objects[j].LastModified) < 0
		})
		log.Printf("Delete %d objects from s3\n", backupsToDelete)
		if err := s3.DeleteObjects(objects[:backupsToDelete]); err != nil {
			return err
		}
	}
	return nil
}
