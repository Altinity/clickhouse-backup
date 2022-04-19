package main

import (
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/logcli"
	"os"

	"github.com/AlexAkulov/clickhouse-backup/pkg/backup"
	"github.com/AlexAkulov/clickhouse-backup/pkg/server"

	"github.com/apex/log"
	"github.com/urfave/cli"
)

var (
	version   = "unknown"
	gitCommit = "unknown"
	buildDate = "unknown"
)

func main() {
	log.SetHandler(logcli.New(os.Stdout))
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
			Usage:  "Config `FILE` name.",
			EnvVar: "CLICKHOUSE_BACKUP_CONFIG",
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
			Usage:     "Print list of tables",
			UsageText: "clickhouse-backup tables",
			Action: func(c *cli.Context) error {
				return backup.PrintTables(config.GetConfig(c), c.Bool("a"))
			},
			Flags: append(cliapp.Flags,
				cli.BoolFlag{
					Name:   "all, a",
					Hidden: false,
				},
			),
		},
		{
			Name:        "create",
			Usage:       "Create new backup",
			UsageText:   "clickhouse-backup create [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--rbac] [--configs] <backup_name>",
			Description: "Create new backup",
			Action: func(c *cli.Context) error {
				return backup.CreateBackup(config.GetConfig(c), c.Args().First(), c.String("t"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("rbac"), c.Bool("configs"), version)
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
					Usage:  "table name patterns, separated by comma, allow ? and * as wildcard",
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage:  "partition names, separated by comma",
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
					Usage:  "Backup ClickHouse server configuration files only",
				},
			),
		},
		{
			Name:        "create_remote",
			Usage:       "Create and upload",
			UsageText:   "clickhouse-backup create_remote [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [--diff-from=<local_backup_name>] [--diff-from-remote=<local_backup_name>] [--schema] [--rbac] [--configs] <backup_name>",
			Description: "Create and upload",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfig(c))
				return b.CreateToRemote(c.Args().First(), c.String("diff-from"), c.String("diff-from-remote"), c.String("t"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("rbac"), c.Bool("configs"), version)
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Usage:  "table name patterns, separated by comma, allow ? and * as wildcard",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage:  "partition names, separated by comma",
				},
				cli.StringFlag{
					Name:   "diff-from",
					Hidden: false,
					Usage:  "local backup name which used to upload current backup as differential",
				},
				cli.StringFlag{
					Name:   "diff-from-remote",
					Hidden: false,
					Usage:  "remote backup name which used to upload current backup as differential",
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
					Usage:  "Backup ClickHouse server configuration files only",
				},
			),
		},
		{
			Name:      "upload",
			Usage:     "Upload backup to remote storage",
			UsageText: "clickhouse-backup upload [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--diff-from=<local_backup_name>] [--diff-from-remote=<remote_backup_name>] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfig(c))
				return b.Upload(c.Args().First(), c.String("diff-from"), c.String("diff-from-remote"), c.String("t"), c.StringSlice("partitions"), c.Bool("s"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "diff-from",
					Hidden: false,
					Usage:  "local backup name which used to upload current backup as differential",
				},
				cli.StringFlag{
					Name:   "diff-from-remote",
					Hidden: false,
					Usage:  "remote backup name which used to upload current backup as differential",
				},
				cli.StringFlag{
					Name:   "table, tables, t",
					Usage:  "table name patterns, separated by comma, allow ? and * as wildcard",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage:  "partition names, separated by comma",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Upload schemas only",
				},
			),
		},
		{
			Name:      "list",
			Usage:     "Print list of backups",
			UsageText: "clickhouse-backup list [all|local|remote] [latest|penult]",
			Action: func(c *cli.Context) error {
				cfg := config.GetConfig(c)
				switch c.Args().Get(0) {
				case "local":
					return backup.PrintLocalBackups(cfg, c.Args().Get(1))
				case "remote":
					return backup.PrintRemoteBackups(cfg, c.Args().Get(1))
				case "all", "":
					return backup.PrintAllBackups(cfg, c.Args().Get(1))
				default:
					log.Errorf("Unknown command '%s'\n", c.Args().Get(0))
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				return nil
			},
			Flags: cliapp.Flags,
		},
		{
			Name:      "download",
			Usage:     "Download backup from remote storage",
			UsageText: "clickhouse-backup download [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfig(c))
				return b.Download(c.Args().First(), c.String("t"), c.StringSlice("partitions"), c.Bool("s"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Usage:  "table name patterns, separated by comma, allow ? and * as wildcard",
					Hidden: false,
				},
				cli.StringFlag{
					Name:   "partitions",
					Hidden: false,
					Usage:  "partition names, separated by comma",
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Download schema only",
				},
			),
		},
		{
			Name:      "restore",
			Usage:     "Create schema and restore data from backup",
			UsageText: "clickhouse-backup restore  [-t, --tables=<db>.<table>] [--partitions=<partitions_names>] [-s, --schema] [-d, --data] [--rm, --drop] [--rbac] [--configs] <backup_name>",
			Action: func(c *cli.Context) error {
				return backup.Restore(config.GetConfig(c), c.Args().First(), c.String("t"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("d"), c.Bool("rm"), c.Bool("rbac"), c.Bool("configs"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Usage:  "table name patterns, separated by comma, allow ? and * as wildcard",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage:  "partition names, separated by comma",
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
					Usage:  "Drop table before restore",
				},
				cli.BoolFlag{
					Name:   "rbac, restore-rbac, do-restore-rbac",
					Hidden: false,
					Usage:  "Restore RBAC related objects only",
				},
				cli.BoolFlag{
					Name:   "configs, restore-configs, do-restore-configs",
					Hidden: false,
					Usage:  "Restore CONFIG related files only",
				},
			),
		},
		{
			Name:      "restore_remote",
			Usage:     "Download and restore",
			UsageText: "clickhouse-backup restore_remote [--schema] [--data] [-t, --tables=<db>.<table>] [--partitions=<partitions_names>] [--rm, --drop] [--rbac] [--configs] [--skip-rbac] [--skip-configs] <backup_name>",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfig(c))
				return b.RestoreFromRemote(c.Args().First(), c.String("t"), c.StringSlice("partitions"), c.Bool("s"), c.Bool("d"), c.Bool("rm"), c.Bool("rbac"), c.Bool("configs"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Usage:  "table name patterns, separated by comma, allow ? and * as wildcard",
					Hidden: false,
				},
				cli.StringSliceFlag{
					Name:   "partitions",
					Hidden: false,
					Usage:  "partition names, separated by comma",
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
					Usage:  "Drop table before restore",
				},
				cli.BoolFlag{
					Name:   "rbac, restore-rbac, do-restore-rbac",
					Hidden: false,
					Usage:  "Restore RBAC related objects only",
				},
				cli.BoolFlag{
					Name:   "configs, restore-configs, do-restore-configs",
					Hidden: false,
					Usage:  "Restore CONFIG related files only",
				},
			),
		},
		{
			Name:      "delete",
			Usage:     "Delete specific backup",
			UsageText: "clickhouse-backup delete <local|remote> <backup_name>",
			Action: func(c *cli.Context) error {
				cfg := config.GetConfig(c)
				if c.Args().Get(1) == "" {
					log.Errorf("Backup name must be defined")
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				switch c.Args().Get(0) {
				case "local":
					return backup.RemoveBackupLocal(cfg, c.Args().Get(1), nil)
				case "remote":
					return backup.RemoveBackupRemote(cfg, c.Args().Get(1))
				default:
					log.Errorf("Unknown command '%s'\n", c.Args().Get(0))
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				return nil
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
			Usage: "Print current config",
			Action: func(c *cli.Context) error {
				return config.PrintConfig(c)
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "clean",
			Usage: "Remove data in 'shadow' folder from all `path` folders available from `system.disks`",
			Action: func(c *cli.Context) error {
				return backup.Clean(config.GetConfig(c))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "server",
			Usage: "Run API server",
			Action: func(c *cli.Context) error {
				return server.Server(cliapp, config.GetConfigPath(c), version)
			},
			Flags: cliapp.Flags,
		},
	}
	if err := cliapp.Run(os.Args); err != nil {
		log.Fatal(err.Error())
	}
}
