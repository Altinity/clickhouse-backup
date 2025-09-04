package main

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/urfave/cli"

	"github.com/Altinity/clickhouse-backup/v2/pkg/backup"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/server"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
)

var (
	version   = "unknown"
	gitCommit = "unknown"
	buildDate = "unknown"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	// Customize the caller format to remove the prefix
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return strings.TrimPrefix(file, "github.com/Altinity/clickhouse-backup/v2/") + ":" + strconv.Itoa(line)
	}
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, NoColor: true, TimeFormat: "2006-01-02 15:04:05.000"}
	//diodeWriter := diode.NewWriter(consoleWriter, 4096, 10*time.Millisecond, func(missed int) {
	//	fmt.Printf("Logger Dropped %d messages", missed)
	//})
	log.Logger = zerolog.New(zerolog.SyncWriter(consoleWriter)).With().Timestamp().Caller().Logger()
	//zerolog.SetGlobalLevel(zerolog.Disabled)
	//log.Logger = zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	stdlog.SetOutput(log.Logger)
	cliapp := cli.NewApp()
	cliapp.Name = "clickhouse-backup"
	cliapp.Usage = "Tool for easy backup of ClickHouse with cloud support"
	cliapp.UsageText = "clickhouse-backup <command> [-t, --tables=<db>.<table>] <backup_name>"
	cliapp.Description = "Run as 'root' or 'clickhouse' user"
	cliapp.Version = version
	// @todo add GCS and Azure support when resolve https://github.com/googleapis/google-cloud-go/issues/8169 and https://github.com/Azure/azure-sdk-for-go/issues/21047
	if strings.HasSuffix(version, "fips") {
		_ = os.Setenv("AWS_USE_FIPS_ENDPOINT", "true")
	}
	cliapp.Flags = []cli.Flag{
		cli.StringFlag{
			Name:     "config, c",
			Value:    config.DefaultConfigPath,
			Usage:    "Config 'FILE' name.",
			EnvVar:   "CLICKHOUSE_BACKUP_CONFIG",
			Required: false,
		},
		cli.StringSliceFlag{
			Name:     "environment-override, env",
			Usage:    "override any environment variable via CLI parameter",
			Required: false,
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
			UsageText: "clickhouse-backup tables [--tables=<db>.<table>] [--remote-backup=<backup-name>] [--all]",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.PrintTables(c.Bool("all"), c.String("table"), c.String("remote-backup"))
			},
			Flags: append(cliapp.Flags,
				cli.BoolFlag{
					Name:   "all, a",
					Hidden: false,
					Usage:  "Print table even when match with skip_tables pattern",
				},
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
					Usage:  "List tables only match with table name patterns, separated by comma, allow ? and * as wildcard",
				},
				cli.StringFlag{
					Name:   "remote-backup",
					Hidden: false,
					Usage:  "List tables from remote backup",
				},
			),
		},
		{
			Name:        "create",
			Usage:       "Create new backup",
			UsageText:   "clickhouse-backup create [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [--diff-from-remote=<backup-name>] [-s, --schema] [--rbac] [--configs] [--named-collections] [--skip-check-parts-columns] [--resume] <backup_name>",
			Description: "Create new backup",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CreateBackup(c.Args().First(), c.String("diff-from-remote"), c.String("t"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("rbac"), c.Bool("rbac-only"), c.Bool("configs"), c.Bool("configs-only"), c.Bool("named-collections"), c.Bool("named-collections-only"), c.Bool("skip-check-parts-columns"), c.StringSlice("skip-projections"), c.Bool("resume"), version, c.Int("command-id"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
					Usage:  "Create backup only matched with table name patterns, separated by comma, allow ? and * as wildcard",
				},
				cli.StringFlag{
					Name:   "diff-from-remote",
					Hidden: false,
					Usage:  "Create incremental embedded backup or upload incremental object disk data based on other remote backup name",
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "Create backup only for selected partition names, separated by comma\n" +
						"If PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*\n" +
						"Values depends on field types in your table, use single quotes for String and Date/DateTime related types\n" +
						"Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Backup schemas only, will skip data",
				},
				cli.BoolFlag{
					Name:   "rbac, backup-rbac, do-backup-rbac",
					Hidden: false,
					Usage:  "Backup RBAC related objects",
				},
				cli.BoolFlag{
					Name:   "configs, backup-configs, do-backup-configs",
					Hidden: false,
					Usage:  "Backup 'clickhouse-server' configuration files",
				},
				cli.BoolFlag{
					Name:   "named-collections, backup-named-collections, do-backup-named-collections",
					Hidden: false,
					Usage:  "Backup named collections",
				},
				cli.BoolFlag{
					Name:   "rbac-only",
					Hidden: false,
					Usage:  "Backup RBAC related objects only, will skip backup data, will backup schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "configs-only",
					Hidden: false,
					Usage:  "Backup 'clickhouse-server' configuration files only, will skip backup data, will backup schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "named-collections-only",
					Hidden: false,
					Usage:  "Backup named collections only, will skip backup data, will backup schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "skip-check-parts-columns",
					Hidden: false,
					Usage:  "Skip check system.parts_columns to allow backup inconsistent column types for data parts",
				},
				cli.StringSliceFlag{
					Name:   "skip-projections",
					Hidden: false,
					Usage:  "Skip make hardlinks to *.proj/* files during backup creation, format `db_pattern.table_pattern:projections_pattern`, use https://pkg.go.dev/path/filepath#Match syntax",
				},
				cli.BoolFlag{
					Name:   "resume, resumable",
					Hidden: false,
					Usage:  "Will resume upload for object disk data, hard links on local disk still continue to recreate, not work when `use_embedded_backup_restore: true`",
				},
			),
		},
		{
			Name:        "create_remote",
			Usage:       "Create and upload new backup",
			UsageText:   "clickhouse-backup create_remote [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [--diff-from=<local_backup_name>] [--diff-from-remote=<local_backup_name>] [--schema] [--rbac] [--configs] [--named-collections] [--resumable] [--skip-check-parts-columns] <backup_name>",
			Description: "Create and upload",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CreateToRemote(c.Args().First(), c.Bool("delete-source"), c.String("diff-from"), c.String("diff-from-remote"), c.String("tables"), c.StringSlice("partitions"), c.StringSlice("skip-projections"), c.Bool("schema"), c.Bool("rbac"), c.Bool("rbac-only"), c.Bool("configs"), c.Bool("configs-only"), c.Bool("named-collections"), c.Bool("named-collections-only"), c.Bool("skip-check-parts-columns"), c.Bool("resume"), version, c.Int("command-id"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
					Usage:  "Create and upload backup only matched with table name patterns, separated by comma, allow ? and * as wildcard",
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "Create and upload backup only for selected partition names, separated by comma\n" +
						"If PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*\n" +
						"Values depends on field types in your table, use single quotes for String and Date/DateTime related types\n" +
						"Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.StringFlag{
					Name:   "diff-from",
					Hidden: false,
					Usage:  "Local backup name which used to upload current backup as incremental",
				},
				cli.StringFlag{
					Name:   "diff-from-remote",
					Hidden: false,
					Usage:  "Remote backup name which used to upload current backup as incremental",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Backup and upload metadata schema only, will skip data backup",
				},
				cli.BoolFlag{
					Name:   "rbac, backup-rbac, do-backup-rbac",
					Hidden: false,
					Usage:  "Backup and upload RBAC related objects",
				},
				cli.BoolFlag{
					Name:   "configs, backup-configs, do-backup-configs",
					Hidden: false,
					Usage:  "Backup and upload 'clickhouse-server' configuration files",
				},
				cli.BoolFlag{
					Name:   "named-collections, backup-named-collections, do-backup-named-collections",
					Hidden: false,
					Usage:  "Backup and upload named collections and settings",
				},
				cli.BoolFlag{
					Name:   "rbac-only",
					Hidden: false,
					Usage:  "Backup RBAC related objects only, will skip backup data, will backup schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "configs-only",
					Hidden: false,
					Usage:  "Backup 'clickhouse-server' configuration files only, will skip backup data, will backup schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "named-collections-only",
					Hidden: false,
					Usage:  "Backup named collections only, will skip backup data, will backup schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "resume, resumable",
					Hidden: false,
					Usage:  "Save intermediate upload state and resume upload if backup exists on remote storage, ignore when 'remote_storage: custom' or 'use_embedded_backup_restore: true'",
				},
				cli.BoolFlag{
					Name:   "skip-check-parts-columns",
					Hidden: false,
					Usage:  "Skip check system.parts_columns to allow backup inconsistent column types for data parts",
				},
				cli.StringSliceFlag{
					Name:   "skip-projections",
					Hidden: false,
					Usage:  "Skip make and upload hardlinks to *.proj/* files during backup creation, format `db_pattern.table_pattern:projections_pattern`, use https://pkg.go.dev/path/filepath#Match syntax",
				},
				cli.BoolFlag{
					Name:   "delete, delete-source, delete-local",
					Hidden: false,
					Usage:  "explicitly delete local backup during upload",
				},
			),
		},
		{
			Name:      "upload",
			Usage:     "Upload backup to remote storage",
			UsageText: "clickhouse-backup upload [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--diff-from=<local_backup_name>] [--diff-from-remote=<remote_backup_name>] [--resumable] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.Upload(c.Args().First(), c.Bool("delete-source"), c.String("diff-from"), c.String("diff-from-remote"), c.String("t"), c.StringSlice("partitions"), c.StringSlice("skip-projections"), c.Bool("schema"), c.Bool("rbac-only"), c.Bool("configs-only"), c.Bool("named-collections-only"), c.Bool("resume"), version, c.Int("command-id"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "diff-from",
					Hidden: false,
					Usage:  "Local backup name which used to upload current backup as incremental",
				},
				cli.StringFlag{
					Name:   "diff-from-remote",
					Hidden: false,
					Usage:  "Remote backup name which used to upload current backup as incremental",
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
						"If PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*\n" +
						"Values depends on field types in your table, use single quotes for String and Date/DateTime related types\n" +
						"Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Upload schemas only",
				},
				cli.BoolFlag{
					Name:   "rbac-only, rbac",
					Hidden: false,
					Usage:  "Upload RBAC related objects only, will skip upload data, will backup schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "configs-only, configs",
					Hidden: false,
					Usage:  "Upload 'clickhouse-server' configuration files only, will skip upload data, will backup schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "named-collections-only, named-collections",
					Hidden: false,
					Usage:  "Upload named collections and settings only, will skip upload data, will backup schema only if --schema added",
				},
				cli.StringSliceFlag{
					Name:   "skip-projections",
					Hidden: false,
					Usage:  "Skip make and upload hardlinks to *.proj/* files during backup creation, format `db_pattern.table_pattern:projections_pattern`, use https://pkg.go.dev/path/filepath#Match syntax",
				},
				cli.BoolFlag{
					Name:   "resume, resumable",
					Hidden: false,
					Usage:  "Save intermediate upload state and resume upload if backup exists on remote storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'",
				},
				cli.BoolFlag{
					Name:   "delete, delete-source, delete-local",
					Hidden: false,
					Usage:  "explicitly delete local backup during upload",
				},
			),
		},
		{
			Name:      "list",
			Usage:     "List of backups",
			UsageText: "clickhouse-backup list [all|local|remote] [latest|previous]",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				err := b.List(c.Args().Get(0), c.Args().Get(1), c.String("format"))
				return err
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "format, f",
					Usage:  "Output format (text|json|yaml|csv|tsv)",
					Hidden: false,
				},
			),
		},
		{
			Name:      "download",
			Usage:     "Download backup from remote storage",
			UsageText: "clickhouse-backup download [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--resumable] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.Download(c.Args().First(), c.String("t"), c.StringSlice("partitions"), c.Bool("schema"), c.Bool("rbac-only"), c.Bool("configs-only"), c.Bool("named-collections-only"), c.Bool("resume"), c.Bool("hardlink-exists-files"), version, c.Int("command-id"))
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
						"If PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*\n" +
						"Values depends on field types in your table, use single quotes for String and Date/DateTime related types\n" +
						"Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.BoolFlag{
					Name:   "schema, schema-only, s",
					Hidden: false,
					Usage:  "Download schema only",
				},
				cli.BoolFlag{
					Name:   "rbac-only, rbac",
					Hidden: false,
					Usage:  "Download RBAC related objects only, will skip download data, will download schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "configs-only, configs",
					Hidden: false,
					Usage:  "Download 'clickhouse-server' configuration files only, will skip download data, will download schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "named-collections-only, named-collections",
					Hidden: false,
					Usage:  "Download named collections and settings only, will skip download data, will download schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "resume, resumable",
					Hidden: false,
					Usage:  "Save intermediate download state and resume download if backup exists on local storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'",
				},
				cli.BoolFlag{
					Name:   "hardlink-exists-files",
					Hidden: false,
					Usage:  "Create hardlinks for existing files instead of downloading",
				},
			),
		},
		{
			Name:      "restore",
			Usage:     "Create schema and restore data from backup",
			UsageText: "clickhouse-backup restore  [-t, --tables=<db>.<table>] [-m, --restore-database-mapping=<originDB>:<targetDB>[,<...>]] [--tm, --restore-table-mapping=<originTable>:<targetTable>[,<...>]] [--partitions=<partitions_names>] [-s, --schema] [-d, --data] [--rm, --drop] [-i, --ignore-dependencies] [--rbac] [--configs] [--named-collections] [--resume] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				// Override config with CLI flag if provided
				if c.Bool("restore-in-place") {
					b.SetRestoreInPlace(true)
				}
				return b.Restore(c.Args().First(), c.String("tables"), c.StringSlice("restore-database-mapping"), c.StringSlice("restore-table-mapping"), c.StringSlice("partitions"), c.StringSlice("skip-projections"), c.Bool("schema"), c.Bool("data"), c.Bool("drop"), c.Bool("ignore-dependencies"), c.Bool("rbac"), c.Bool("rbac-only"), c.Bool("configs"), c.Bool("configs-only"), c.Bool("named-collections"), c.Bool("named-collections-only"), c.Bool("resume"), c.Bool("restore-schema-as-attach"), c.Bool("replicated-copy-to-detached"), version, c.Int("command-id"))
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
					Name:   "restore-table-mapping, tm",
					Usage:  "Define the rule to restore data. For the table not defined in this struct, the program will not deal with it.",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "Restore backup only for selected partition names, separated by comma\n" +
						"If PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*\n" +
						"Values depends on field types in your table, use single quotes for String and Date/DateTime related types\n" +
						"Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
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
					Usage:  "Restore RBAC related objects",
				},
				cli.BoolFlag{
					Name:   "configs, restore-configs, do-restore-configs",
					Hidden: false,
					Usage:  "Restore 'clickhouse-server' CONFIG related files",
				},
				cli.BoolFlag{
					Name:   "named-collections, restore-named-collections, do-restore-named-collections",
					Hidden: false,
					Usage:  "Restore named collections and settings",
				},
				cli.BoolFlag{
					Name:   "rbac-only",
					Hidden: false,
					Usage:  "Restore RBAC related objects only, will skip restore data, will restore schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "configs-only",
					Hidden: false,
					Usage:  "Restore 'clickhouse-server' configuration files only, will skip restore data, will restore schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "named-collections-only",
					Hidden: false,
					Usage:  "Restore named collections only, will skip restore data, will restore schema only if --schema added",
				},
				cli.StringSliceFlag{
					Name:   "skip-projections",
					Hidden: false,
					Usage:  "Skip make hardlinks to *.proj/* files during backup restoring, format `db_pattern.table_pattern:projections_pattern`, use https://pkg.go.dev/path/filepath#Match syntax",
				},
				cli.BoolFlag{
					Name:   "resume, resumable",
					Hidden: false,
					Usage:  "Will resume download for object disk data",
				},
				cli.BoolFlag{
					Name:   "restore-schema-as-attach",
					Hidden: false,
					Usage:  "Use DETACH/ATTACH instead of DROP/CREATE for schema restoration",
				},
				cli.BoolFlag{
					Name:   "replicated-copy-to-detached",
					Hidden: false,
					Usage:  "Copy data to detached folder for Replicated*MergeTree tables but skip ATTACH PART step",
				},
				cli.BoolFlag{
					Name:   "restore-in-place",
					Hidden: false,
					Usage:  "Perform in-place restore by comparing backup parts with current database parts. Only downloads differential parts instead of full restore. Requires --data flag.",
				},
			),
		},
		{
			Name:      "restore_remote",
			Usage:     "Download and restore",
			UsageText: "clickhouse-backup restore_remote [--schema] [--data] [-t, --tables=<db>.<table>] [-m, --restore-database-mapping=<originDB>:<targetDB>[,<...>]] [--tm, --restore-table-mapping=<originTable>:<targetTable>[,<...>]] [--partitions=<partitions_names>] [--rm, --drop] [-i, --ignore-dependencies] [--rbac] [--configs] [--named-collections] [--resumable] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				// Override config with CLI flag if provided
				if c.Bool("restore-in-place") {
					b.SetRestoreInPlace(true)
				}
				// CI/CD Diagnostic: Log function signature validation for Go 1.25 + ClickHouse 23.8 compatibility
				log.Debug().Fields(map[string]interface{}{
					"operation":               "restore_remote_cli_validation",
					"go_version":             runtime.Version(),
					"hardlink_exists_files":  c.Bool("hardlink-exists-files"),
					"drop_if_schema_changed": c.Bool("drop-if-schema-changed"),
					"function_signature":     "RestoreFromRemote",
					"parameter_count":        "expected_22_parameters",
					"issue_diagnosis":        "hardcoded_false_should_be_cli_flag",
				}).Msg("diagnosing CI Build/Test (1.25, 23.8) function signature mismatch")
				return b.RestoreFromRemote(c.Args().First(), c.String("tables"), c.StringSlice("restore-database-mapping"), c.StringSlice("restore-table-mapping"), c.StringSlice("partitions"), c.StringSlice("skip-projections"), c.Bool("schema"), c.Bool("d"), c.Bool("rm"), c.Bool("i"), c.Bool("rbac"), c.Bool("rbac-only"), c.Bool("configs"), c.Bool("configs-only"), c.Bool("named-collections"), c.Bool("named-collections-only"), c.Bool("resume"), c.Bool("restore-schema-as-attach"), c.Bool("replicated-copy-to-detached"), c.Bool("hardlink-exists-files"), c.Bool("drop-if-schema-changed"), version, c.Int("command-id"))
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
					Name:   "restore-table-mapping, tm",
					Usage:  "Define the rule to restore data. For the database not defined in this struct, the program will not deal with it.",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage: "Download and restore backup only for selected partition names, separated by comma\n" +
						"If PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*\n" +
						"Values depends on field types in your table, use single quotes for String and Date/DateTime related types\n" +
						"Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
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
					Usage:  "Download and Restore RBAC related objects",
				},
				cli.BoolFlag{
					Name:   "configs, restore-configs, do-restore-configs",
					Hidden: false,
					Usage:  "Download and Restore 'clickhouse-server' CONFIG related files",
				},
				cli.BoolFlag{
					Name:   "named-collections, restore-named-collections, do-restore-named-collections",
					Hidden: false,
					Usage:  "Download and Restore named collections and settings",
				},
				cli.BoolFlag{
					Name:   "rbac-only",
					Hidden: false,
					Usage:  "Restore RBAC related objects only, will skip backup data, will backup schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "configs-only",
					Hidden: false,
					Usage:  "Restore 'clickhouse-server' configuration files only, will skip backup data, will backup schema only if --schema added",
				},
				cli.BoolFlag{
					Name:   "named-collections-only",
					Hidden: false,
					Usage:  "Restore named collections only, will skip restore data, will restore schema only if --schema added",
				},
				cli.StringSliceFlag{
					Name:   "skip-projections",
					Hidden: false,
					Usage:  "Skip make hardlinks to *.proj/* files during backup restoring, format `db_pattern.table_pattern:projections_pattern`, use https://pkg.go.dev/path/filepath#Match syntax",
				},
				cli.BoolFlag{
					Name:   "resume, resumable",
					Hidden: false,
					Usage:  "Save intermediate download state and resume download if backup exists on remote storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'",
				},
				cli.BoolFlag{
					Name:   "restore-schema-as-attach",
					Hidden: false,
					Usage:  "Use DETACH/ATTACH instead of DROP/CREATE for schema restoration",
				},
				cli.BoolFlag{
					Name:   "hardlink-exists-files",
					Hidden: false,
					Usage:  "Create hardlinks for existing files instead of downloading",
				},
				cli.BoolFlag{
					Name:   "restore-in-place",
					Hidden: false,
					Usage:  "Perform in-place restore by comparing backup parts with current database parts. Only downloads differential parts instead of full restore. Requires --data flag.",
				},
				cli.BoolFlag{
					Name:   "drop-if-schema-changed",
					Hidden: false,
					Usage:  "Drop and recreate tables when schema changes are detected during in-place restore. Only available with --restore-in-place flag.",
				},
			),
		},
		{
			Name:      "delete",
			Usage:     "Delete specific backup",
			UsageText: "clickhouse-backup delete <local|remote> <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				if c.Args().Get(0) != "local" && c.Args().Get(0) != "remote" {
					log.Err(fmt.Errorf("Unknown sub-command '%s', use 'local' or 'remote'\n", c.Args().Get(0))).Send()
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				if c.Args().Get(1) == "" {
					log.Err(fmt.Errorf("backup name must be defined")).Send()
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
			Name:  "clean_local_broken",
			Usage: "Remove all broken local backups",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CleanLocalBroken(status.NotFromAPI)
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
				return b.Watch(c.String("watch-interval"), c.String("full-interval"), c.String("watch-backup-name-template"), c.String("tables"), c.StringSlice("partitions"), c.StringSlice("skip-projections"), c.Bool("schema"), c.Bool("rbac"), c.Bool("configs"), c.Bool("named-collections"), c.Bool("skip-check-parts-columns"), c.Bool("delete-source"), version, c.Int("command-id"), nil, c)
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
					Usage: "Partitions names, separated by comma\n" +
						"If PARTITION BY clause returns numeric not hashed values for `partition_id` field in system.parts table, then use --partitions=partition_id1,partition_id2 format\n" +
						"If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format\n" +
						"If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format\n" +
						"If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*\n" +
						"Values depends on field types in your table, use single quotes for String and Date/DateTime related types\n" +
						"Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Schemas only",
				},
				cli.BoolFlag{
					Name:   "rbac, backup-rbac, do-backup-rbac",
					Hidden: false,
					Usage:  "Backup RBAC related objects",
				},
				cli.BoolFlag{
					Name:   "configs, backup-configs, do-backup-configs",
					Hidden: false,
					Usage:  "Backup `clickhouse-server' configuration files",
				},
				cli.BoolFlag{
					Name:   "named-collections, backup-named-collections, do-backup-named-collections",
					Hidden: false,
					Usage:  "Backup named collections and settings",
				},
				cli.BoolFlag{
					Name:   "skip-check-parts-columns",
					Hidden: false,
					Usage:  "Skip check system.parts_columns to allow backup inconsistent column types for data parts",
				},
				cli.StringSliceFlag{
					Name:   "skip-projections",
					Hidden: false,
					Usage:  "Skip make and upload hardlinks to *.proj/* files during backup creation, format `db_pattern.table_pattern:projections_pattern`, use https://pkg.go.dev/path/filepath#Match syntax",
				},
				cli.BoolFlag{
					Name:   "delete, delete-source, delete-local",
					Hidden: false,
					Usage:  "explicitly delete local backup during upload",
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
					Usage:  "Run watch go-routine for 'create_remote' + 'delete local', after API server startup",
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
				cli.BoolFlag{
					Name:   "watch-delete-source, watch-delete-local",
					Hidden: false,
					Usage:  "explicitly delete local backup during upload in watch",
				}),
		},
	}
	if err := cliapp.Run(os.Args); err != nil {
		log.Fatal().Err(err).Send()
	}
}
