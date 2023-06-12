package main

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"time"

	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/status"

	"github.com/Altinity/clickhouse-backup/pkg/backup"
	"github.com/Altinity/clickhouse-backup/pkg/server"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/urfave/cli"
)

var (
	version   = "unknown"
	gitCommit = "unknown"
	buildDate = "unknown"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true, TimeFormat: time.StampMilli})
	stdlog.SetOutput(log.Logger)
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	cliapp := cli.NewApp()
	cliapp.Name = "clickhouse-backup"
	cliapp.Usage = "Tool for easy backup of ClickHouse with cloud support"
	cliapp.UsageText = "clickhouse-backup <command> [-t, --tables=<db>.<table>] <backup_name>"
	cliapp.Description = "Run as 'root' or 'clickhouse' user"
	cliapp.Version = version

	cliapp.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "config, c",
			Value:  config.DefaultConfigPath,
			Usage:  "Config 'FILE' name.",
			EnvVar: "CLICKHOUSE_BACKUP_CONFIG",
		},
		cli.IntFlag{
			Name:     "command-id",
			Hidden:   true,
			Value:    -1,
			Required: false,
			Usage:    "internal parameter for API call",
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
			Usage:     "List of tables, exclude skip_tables",
			UsageText: "clickhouse-backup tables [-t, --tables=<db>.<table>]] [--all]",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.PrintTables(c.Bool("all"), c.String("table"))
			},
			Flags: append(cliapp.Flags,
				cli.BoolFlag{
					Name:   "all, a",
					Hidden: false,
					Usage:  "print table even when match with skip_tables pattern",
				},
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
					Usage:  "list tables only match with table name patterns, separated by comma, allow ? and * as wildcard",
				},
			),
		},
		{
			Name:        "create",
			Usage:       "Create new backup",
			UsageText:   "clickhouse-backup create [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--rbac] [--configs] [--skip-check-parts-columns] <backup_name>",
			Description: "Create new backup",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CreateBackup(c.Args().First(), c.String("t"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("rbac"), c.Bool("configs"), c.Bool("skip-check-parts-columns"), version, c.Int("command-id"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
					Usage:  "create backup only matched with table name patterns, separated by comma, allow ? and * as wildcard",
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "create backup only for selected partition names, separated by comma\n" +
						"if PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"if PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"if PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"values depends on field types in your table, use single quote for String and Date/DateTime related types\n" +
						"look to system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Backup schemas only",
				},
				cli.BoolFlag{
					Name:   "rbac, backup-rbac, do-backup-rbac",
					Hidden: false,
					Usage:  "Backup RBAC related objects only",
				},
				cli.BoolFlag{
					Name:   "configs, backup-configs, do-backup-configs",
					Hidden: false,
					Usage:  "Backup 'clickhouse-server' configuration files only",
				},
				cli.BoolFlag{
					Name:   "skip-check-parts-columns",
					Hidden: false,
					Usage:  "skip check system.parts_columns to disallow backup inconsistent column types for data parts",
				},
			),
		},
		{
			Name:        "create_remote",
			Usage:       "Create and upload new backup",
			UsageText:   "clickhouse-backup create_remote [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [--diff-from=<local_backup_name>] [--diff-from-remote=<local_backup_name>] [--schema] [--rbac] [--configs] [--resumable] [--skip-check-parts-columns] <backup_name>",
			Description: "Create and upload",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CreateToRemote(c.Args().First(), c.String("diff-from"), c.String("diff-from-remote"), c.String("t"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("rbac"), c.Bool("configs"), c.Bool("resume"), c.Bool("skip-check-parts-columns"), version, c.Int("command-id"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
					Usage:  "create and upload backup only matched with table name patterns, separated by comma, allow ? and * as wildcard",
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "create and upload backup only for selected partition names, separated by comma\n" +
						"if PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"if PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"if PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"values depends on field types in your table, use single quote for String and Date/DateTime related types\n" +
						"look to system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.StringFlag{
					Name:   "diff-from",
					Hidden: false,
					Usage:  "local backup name which used to upload current backup as incremental",
				},
				cli.StringFlag{
					Name:   "diff-from-remote",
					Hidden: false,
					Usage:  "remote backup name which used to upload current backup as incremental",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Backup and upload metadata schema only",
				},
				cli.BoolFlag{
					Name:   "rbac, backup-rbac, do-backup-rbac",
					Hidden: false,
					Usage:  "Backup and upload RBAC related objects only",
				},
				cli.BoolFlag{
					Name:   "configs, backup-configs, do-backup-configs",
					Hidden: false,
					Usage:  "Backup 'clickhouse-server' configuration files only",
				},
				cli.BoolFlag{
					Name:   "resume, resumable",
					Hidden: false,
					Usage:  "Save intermediate upload state and resume upload if backup exists on remote storage, ignore when 'remote_storage: custom' or 'use_embedded_backup_restore: true'",
				},
				cli.BoolFlag{
					Name:   "skip-check-parts-columns",
					Hidden: false,
					Usage:  "skip check system.parts_columns to disallow backup inconsistent column types for data parts",
				},
			),
		},
		{
			Name:      "upload",
			Usage:     "Upload backup to remote storage",
			UsageText: "clickhouse-backup upload [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--diff-from=<local_backup_name>] [--diff-from-remote=<remote_backup_name>] [--resumable] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.Upload(c.Args().First(), c.String("diff-from"), c.String("diff-from-remote"), c.String("t"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("resume"), c.Int("command-id"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "diff-from",
					Hidden: false,
					Usage:  "local backup name which used to upload current backup as incremental",
				},
				cli.StringFlag{
					Name:   "diff-from-remote",
					Hidden: false,
					Usage:  "remote backup name which used to upload current backup as incremental",
				},
				cli.StringFlag{
					Name:   "table, tables, t",
					Usage:  "Upload data only for matched table name patterns, separated by comma, allow ? and * as wildcard",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "Upload backup only for selected partition names, separated by comma\n" +
						"if PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"if PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"if PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"values depends on field types in your table, use single quote for String and Date/DateTime related types\n" +
						"look to system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Upload schemas only",
				},
				cli.BoolFlag{
					Name:   "resume, resumable",
					Hidden: false,
					Usage:  "Save intermediate upload state and resume upload if backup exists on remote storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'",
				},
			),
		},
		{
			Name:      "list",
			Usage:     "List of backups",
			UsageText: "clickhouse-backup list [all|local|remote] [latest|previous]",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.List(c.Args().Get(0), c.Args().Get(1))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:      "download",
			Usage:     "Download backup from remote storage",
			UsageText: "clickhouse-backup download [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--resumable] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.Download(c.Args().First(), c.String("t"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("resume"), c.Int("command-id"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Usage:  "Download objects which matched with table name patterns, separated by comma, allow ? and * as wildcard",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "Download backup data only for selected partition names, separated by comma\n" +
						"if PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"if PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"if PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"values depends on field types in your table, use single quote for String and Date/DateTime related types\n" +
						"look to system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Download schema only",
				},
				cli.BoolFlag{
					Name:   "resume, resumable",
					Hidden: false,
					Usage:  "Save intermediate download state and resume download if backup exists on local storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'",
				},
			),
		},
		{
			Name:      "restore",
			Usage:     "Create schema and restore data from backup",
			UsageText: "clickhouse-backup restore  [-t, --tables=<db>.<table>] [-m, --restore-database-mapping=<originDB>:<targetDB>[,<...>]] [--partitions=<partitions_names>] [-s, --schema] [-d, --data] [--rm, --drop] [-i, --ignore-dependencies] [--rbac] [--configs] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.Restore(c.Args().First(), c.String("t"), c.StringSlice("restore-database-mapping"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("d"), c.Bool("rm"), c.Bool("ignore-dependencies"), c.Bool("rbac"), c.Bool("configs"), c.Int("command-id"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Usage:  "Restore only database and objects which matched with table name patterns, separated by comma, allow ? and * as wildcard",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "restore-database-mapping, m",
					Usage:  "Define the rule to restore data. For the database not defined in this struct, the program will not deal with it.",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "Restore backup only for selected partition names, separated by comma\n" +
						"if PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"if PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"if PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"values depends on field types in your table, use single quote for String and Date/DateTime related types\n" +
						"look to system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Restore schema only",
				},
				cli.BoolFlag{
					Name:   "data, d",
					Hidden: false,
					Usage:  "Restore data only",
				},
				cli.BoolFlag{
					Name:   "rm, drop",
					Hidden: false,
					Usage:  "Drop exists schema objects before restore",
				},
				cli.BoolFlag{
					Name:   "i, ignore-dependencies",
					Hidden: false,
					Usage:  "Ignore dependencies when drop exists schema objects",
				},
				cli.BoolFlag{
					Name:   "rbac, restore-rbac, do-restore-rbac",
					Hidden: false,
					Usage:  "Restore RBAC related objects only",
				},
				cli.BoolFlag{
					Name:   "configs, restore-configs, do-restore-configs",
					Hidden: false,
					Usage:  "Restore 'clickhouse-server' CONFIG related files only",
				},
			),
		},
		{
			Name:      "restore_remote",
			Usage:     "Download and restore",
			UsageText: "clickhouse-backup restore_remote [--schema] [--data] [-t, --tables=<db>.<table>] [-m, --restore-database-mapping=<originDB>:<targetDB>[,<...>]] [--partitions=<partitions_names>] [--rm, --drop] [-i, --ignore-dependencies] [--rbac] [--configs] [--skip-rbac] [--skip-configs] [--resumable] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.RestoreFromRemote(c.Args().First(), c.String("t"), c.StringSlice("restore-database-mapping"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("d"), c.Bool("rm"), c.Bool("i"), c.Bool("rbac"), c.Bool("configs"), c.Bool("resume"), c.Int("command-id"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Usage:  "Download and restore objects which matched with table name patterns, separated by comma, allow ? and * as wildcard",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "restore-database-mapping, m",
					Usage:  "Define the rule to restore data. For the database not defined in this struct, the program will not deal with it.",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "Download and restore backup only for selected partition names, separated by comma\n" +
						"if PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"if PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"if PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"values depends on field types in your table, use single quote for String and Date/DateTime related types\n" +
						"look to system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Download and Restore schema only",
				},
				cli.BoolFlag{
					Name:   "data, d",
					Hidden: false,
					Usage:  "Download and Restore data only",
				},
				cli.BoolFlag{
					Name:   "rm, drop",
					Hidden: false,
					Usage:  "Drop schema objects before restore",
				},
				cli.BoolFlag{
					Name:   "i, ignore-dependencies",
					Hidden: false,
					Usage:  "Ignore dependencies when drop exists schema objects",
				},
				cli.BoolFlag{
					Name:   "rbac, restore-rbac, do-restore-rbac",
					Hidden: false,
					Usage:  "Download and Restore RBAC related objects only",
				},
				cli.BoolFlag{
					Name:   "configs, restore-configs, do-restore-configs",
					Hidden: false,
					Usage:  "Download and Restore 'clickhouse-server' CONFIG related files only",
				},
				cli.BoolFlag{
					Name:   "resume, resumable",
					Hidden: false,
					Usage:  "Save intermediate upload state and resume upload if backup exists on remote storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'",
				},
			),
		},
		{
			Name:      "delete",
			Usage:     "Delete specific backup",
			UsageText: "clickhouse-backup delete <local|remote> <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				if c.Args().Get(1) == "" {
					log.Err(fmt.Errorf("Backup name must be defined")).Send()
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				if c.Args().Get(0) != "local" && c.Args().Get(0) != "remote" {
					log.Err(fmt.Errorf("Unknown command '%s'\n", c.Args().Get(0))).Send()
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				return b.Delete(c.Args().Get(0), c.Args().Get(1), c.Int("command-id"))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "default-config",
			Usage: "Print default config",
			Action: func(*cli.Context) error {
				return config.PrintConfig(nil)
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "print-config",
			Usage: "Print current config merged with environment variables",
			Action: func(c *cli.Context) error {
				return config.PrintConfig(c)
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "clean",
			Usage: "Remove data in 'shadow' folder from all 'path' folders available from 'system.disks'",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.Clean(context.Background())
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "clean_remote_broken",
			Usage: "Remove all broken remote backups",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CleanRemoteBroken(status.NotFromAPI)
			},
			Flags: cliapp.Flags,
		},

		{
			Name:        "watch",
			Usage:       "Run infinite loop which create full + incremental backup sequence to allow efficient backup sequences",
			UsageText:   "clickhouse-backup watch [--watch-interval=1h] [--full-interval=24h] [--watch-backup-name-template=shard{shard}-{type}-{time:20060102150405}] [-t, --tables=<db>.<table>] [--partitions=<partitions_names>] [--schema] [--rbac] [--configs] [--skip-check-parts-columns]",
			Description: "Execute create_remote + delete local, create full backup every `--full-interval`, create and upload incremental backup every `--watch-interval` use previous backup as base with `--diff-from-remote` option, use `backups_to_keep_remote` config option for properly deletion remote backups, will delete old backups which not have references from other backups",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.Watch(c.String("watch-interval"), c.String("full-interval"), c.String("watch-backup-name-template"), c.String("tables"), c.StringSlice("partitions"), c.Bool("schema"), c.Bool("rbac"), c.Bool("configs"), c.Bool("skip-check-parts-columns"), version, c.Int("command-id"), nil, c)
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "watch-interval",
					Usage:  "Interval for run 'create_remote' + 'delete local' for incremental backup, look format https://pkg.go.dev/time#ParseDuration",
					Hidden: false,
				},
				cli.StringFlag{
					Name:   "full-interval",
					Usage:  "Interval for run 'create_remote'+'delete local' when stop create incremental backup sequence and create full backup, look format https://pkg.go.dev/time#ParseDuration",
					Hidden: false,
				},
				cli.StringFlag{
					Name:   "watch-backup-name-template",
					Usage:  "Template for new backup name, could contain names from system.macros, {type} - full or incremental and {time:LAYOUT}, look to https://go.dev/src/time/format.go for layout examples",
					Hidden: false,
				},
				cli.StringFlag{
					Name:   "table, tables, t",
					Usage:  "Create and upload only objects which matched with table name patterns, separated by comma, allow ? and * as wildcard",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "partition names, separated by comma\n" +
						"if PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"if PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"if PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"values depends on field types in your table, use single quote for String and Date/DateTime related types\n" +
						"look to system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Schemas only",
				},
				cli.BoolFlag{
					Name:   "rbac, backup-rbac, do-backup-rbac",
					Hidden: false,
					Usage:  "Backup RBAC related objects only",
				},
				cli.BoolFlag{
					Name:   "configs, backup-configs, do-backup-configs",
					Hidden: false,
					Usage:  "Backup `clickhouse-server' configuration files only",
				},
				cli.BoolFlag{
					Name:   "skip-check-parts-columns",
					Hidden: false,
					Usage:  "skip check system.parts_columns to disallow backup inconsistent column types for data parts",
				},
			),
		},
		{
			Name:  "server",
			Usage: "Run API server",
			Action: func(c *cli.Context) error {
				return server.Run(c, cliapp, config.GetConfigPath(c), version)
			},
			Flags: append(cliapp.Flags,
				cli.BoolFlag{
					Name:   "watch",
					Usage:  "run watch go-routine for 'create_remote' + 'delete local', after API server startup",
					Hidden: false,
				},
				cli.StringFlag{
					Name:   "watch-interval",
					Usage:  "Interval for run 'create_remote' + 'delete local' for incremental backup, look format https://pkg.go.dev/time#ParseDuration",
					Hidden: false,
				},
				cli.StringFlag{
					Name:   "full-interval",
					Usage:  "Interval for run 'create_remote'+'delete local' when stop create incremental backup sequence and create full backup, look format https://pkg.go.dev/time#ParseDuration",
					Hidden: false,
				},
				cli.StringFlag{
					Name:   "watch-backup-name-template",
					Usage:  "Template for new backup name, could contain names from system.macros, {type} - full or incremental and {time:LAYOUT}, look to https://go.dev/src/time/format.go for layout examples",
					Hidden: false,
				},
			),
		},
	}
	if err := cliapp.Run(os.Args); err != nil {
		log.Fatal().Err(err).Send()
	}
}
