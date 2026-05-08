package main

import (
	"fmt"
	"time"

	"github.com/urfave/cli"

	"github.com/Altinity/clickhouse-backup/v2/pkg/backup"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
)

// resolveWaitForPrune returns the --wait-for-prune CLI value if set, otherwise
// falls back to the configured cas.wait_for_prune value.
func resolveWaitForPrune(c *cli.Context, cfg *config.Config) (time.Duration, error) {
	if v := c.String("wait-for-prune"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("--wait-for-prune: %w", err)
		}
		return d, nil
	}
	return cfg.CAS.WaitForPruneDuration(), nil
}

// casCommands returns the seven cas-* CLI subcommands (six implemented + the
// cas-prune Phase-2 stub). rootFlags is the slice of global flags from main.go
// (passed via the same append-pattern as the existing v1 commands).
func casCommands(rootFlags []cli.Flag) []cli.Command {
	return []cli.Command{
		{
			Name:        "cas-upload",
			Usage:       "Upload a local backup using the content-addressable layout (see docs/cas-design.md)",
			UsageText:   "clickhouse-backup cas-upload [--skip-object-disks] [--dry-run] [--unlock] <backup_name>",
			Description: "Upload a backup created by 'clickhouse-backup create' using the CAS layout. Blobs are content-keyed via per-part checksums.txt; small files are packed into per-table tar.zstd archives. CAS dedupes across mutations and across backups; every backup is independently restorable. Requires cas.enabled=true and cas.cluster_id configured.\n\n   --unlock removes a stranded inprogress marker for <backup_name> (left behind by SIGKILL/OOM) and exits immediately without uploading. Incompatible with --dry-run and --skip-object-disks.",
			Action: func(c *cli.Context) error {
				cfg := config.GetConfigFromCli(c)
				wait, err := resolveWaitForPrune(c, cfg)
				if err != nil {
					return err
				}
				b := backup.NewBackuper(cfg)
				return b.CASUpload(c.Args().First(), c.Bool("skip-object-disks"), c.Bool("dry-run"), c.Bool("unlock"), version, c.Int("command-id"), wait)
			},
			Flags: append(rootFlags,
				cli.BoolFlag{
					Name:  "skip-object-disks",
					Usage: "Exclude tables on object disks (s3/azure/hdfs/web) instead of refusing the upload",
				},
				cli.BoolFlag{
					Name:  "dry-run",
					Usage: "Plan the upload without writing anything to remote storage",
				},
				cli.StringFlag{
					Name:  "wait-for-prune",
					Usage: `If a prune is in progress, wait up to this duration (Go duration string, e.g. "5m") before giving up. Overrides cas.wait_for_prune. Empty = use config; "0s" = don't wait.`,
				},
				cli.BoolFlag{
					Name:  "unlock",
					Usage: "Remove a stranded inprogress marker for <backup_name> (self-service recovery after SIGKILL/OOM). Incompatible with --dry-run and --skip-object-disks. Does NOT perform an upload.",
				},
			),
		},
		{
			Name:        "cas-download",
			Usage:       "Materialize a CAS backup into the local data directory (does not load into ClickHouse)",
			UsageText:   "clickhouse-backup cas-download [-t, --tables=<db>.<table>] [--partitions=<part_names>] [-s, --schema] <backup_name>",
			Description: "Download a CAS-layout backup into <DefaultDataPath>/backup/<name>/. Use cas-restore (or v1 restore) to load tables into ClickHouse from the materialized directory.",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CASDownload(c.Args().First(), c.String("tables"), c.StringSlice("partitions"), c.Bool("schema"), c.Bool("data"), version, c.Int("command-id"))
			},
			Flags: append(rootFlags,
				cli.StringFlag{
					Name:  "table, tables, t",
					Usage: "Restrict to tables matching db.table (comma-separated, exact match in CAS v1)",
				},
				cli.StringSliceFlag{
					Name:  "partitions",
					Usage: "Restrict to part names (comma-separated)",
				},
				cli.BoolFlag{
					Name:  "schema, schema-only, s",
					Usage: "Schema-only: write JSON metadata locally and skip part archives + blobs",
				},
				cli.BoolFlag{
					Name:   "data, d",
					Hidden: true,
					Usage:  "Reserved (currently a no-op); will gate data-only download in a future version",
				},
			),
		},
		{
			Name:        "cas-restore",
			Usage:       "Download a CAS backup and restore tables into ClickHouse",
			UsageText:   "clickhouse-backup cas-restore [-t, --tables=<db>.<table>] [-m, --restore-database-mapping=<src>:<dst>[,...]] [--tm, --restore-table-mapping=<src>:<dst>[,...]] [--partitions=<part_names>] [-s, --schema] [-d, --data] [--rm, --drop] [--restore-schema-as-attach] [--replicated-copy-to-detached] [--skip-empty-tables] [--resume] <backup_name>",
			Description: "Pulls the named CAS backup into the local backup directory and runs the v1 restore flow against it. --ignore-dependencies is rejected: CAS backups have no dependency chain. RBAC/configs/named-collections are out of scope for CAS v1.",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CASRestore(
					c.Args().First(),
					c.String("tables"),
					c.StringSlice("restore-database-mapping"),
					c.StringSlice("restore-table-mapping"),
					c.StringSlice("partitions"),
					c.StringSlice("skip-projections"),
					c.Bool("schema"),
					c.Bool("data"),
					c.Bool("drop"),
					c.Bool("ignore-dependencies"),
					c.Bool("restore-schema-as-attach"),
					c.Bool("replicated-copy-to-detached"),
					c.Bool("skip-empty-tables"),
					c.Bool("resume"),
					version,
					c.Int("command-id"),
				)
			},
			Flags: append(rootFlags,
				cli.StringFlag{
					Name:  "table, tables, t",
					Usage: "Restrict to tables matching db.table (comma-separated, exact match in CAS v1)",
				},
				cli.StringSliceFlag{
					Name:  "restore-database-mapping, m",
					Usage: "Database rename rules at restore time, format <src>:<dst> (repeatable or comma-separated)",
				},
				cli.StringSliceFlag{
					Name:  "restore-table-mapping, tm",
					Usage: "Table rename rules at restore time, format <src>:<dst> (repeatable or comma-separated)",
				},
				cli.StringSliceFlag{
					Name:  "partitions",
					Usage: "Restrict to part names (comma-separated)",
				},
				cli.StringSliceFlag{
					Name:  "skip-projections",
					Usage: "Skip listed projections during restore, format `db_pattern.table_pattern:projections_pattern`",
				},
				cli.BoolFlag{
					Name:  "schema, s",
					Usage: "Restore schema only",
				},
				cli.BoolFlag{
					Name:  "data, d",
					Usage: "Restore data only",
				},
				cli.BoolFlag{
					Name:  "rm, drop",
					Usage: "Drop existing schema objects before restore",
				},
				cli.BoolFlag{
					Name:   "i, ignore-dependencies",
					Usage:  "(rejected for CAS backups; accepted for CLI parity with 'restore')",
					Hidden: true,
				},
				cli.BoolFlag{
					Name:  "restore-schema-as-attach",
					Usage: "Use DETACH/ATTACH instead of DROP/CREATE for schema restoration",
				},
				cli.BoolFlag{
					Name:  "replicated-copy-to-detached",
					Usage: "Copy data to detached folder for Replicated*MergeTree tables but skip ATTACH PART step",
				},
				cli.BoolFlag{
					Name:  "skip-empty-tables",
					Usage: "Skip restoring tables that have no data (empty tables with only schema)",
				},
				cli.BoolFlag{
					Name:  "resume, resumable",
					Usage: "Save intermediate state and resume restore on retry",
				},
			),
		},
		{
			Name:        "cas-delete",
			Usage:       "Delete a CAS backup's metadata subtree (Phase 1: blobs are NOT reclaimed)",
			UsageText:   "clickhouse-backup cas-delete <backup_name>",
			Description: "Removes the named backup atomically by deleting metadata.json first, then the rest of the metadata subtree. Blob bytes are NOT reclaimed in Phase 1 — that ships with cas-prune in Phase 2; until then, deleted-backup blobs accumulate in remote storage.",
			Action: func(c *cli.Context) error {
				cfg := config.GetConfigFromCli(c)
				wait, err := resolveWaitForPrune(c, cfg)
				if err != nil {
					return err
				}
				b := backup.NewBackuper(cfg)
				return b.CASDelete(c.Args().First(), c.Int("command-id"), wait)
			},
			Flags: append(rootFlags,
				cli.StringFlag{
					Name:  "wait-for-prune",
					Usage: `If a prune is in progress, wait up to this duration (Go duration string, e.g. "5m") before giving up. Overrides cas.wait_for_prune. Empty = use config; "0s" = don't wait.`,
				},
			),
		},
		{
			Name:        "cas-verify",
			Usage:       "HEAD-check every blob referenced by a CAS backup",
			UsageText:   "clickhouse-backup cas-verify [--json] <backup_name>",
			Description: "Walks the per-table archives, parses every checksums.txt, and HEAD-checks each referenced blob's existence and size. Exits non-zero if any failures are detected.",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CASVerify(c.Args().First(), c.Bool("json"), c.Int("command-id"))
			},
			Flags: append(rootFlags,
				cli.BoolFlag{
					Name:  "json",
					Usage: "Emit one JSON object per failure instead of human-readable lines",
				},
			),
		},
		{
			Name:        "cas-status",
			Usage:       "Print a LIST-only health summary for the configured CAS cluster",
			UsageText:   "clickhouse-backup cas-status",
			Description: "Counts backups and blobs, reports the prune marker (if any), and lists fresh / abandoned in-progress upload markers. No object bodies are fetched.",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CASStatus(c.Int("command-id"))
			},
			Flags: rootFlags,
		},
		{
			Name:        "cas-prune",
			Usage:       "Garbage-collect orphan blobs (mark-and-sweep) for the configured CAS cluster",
			UsageText:   "clickhouse-backup cas-prune [--dry-run] [--grace-blob=<duration>] [--abandon-threshold=<duration>] [--unlock]",
			Description: "Mark-and-sweep GC: walks every live backup's per-table archives, builds a sorted on-disk reference set, then lists the blob store and deletes orphans older than cas.grace_blob. Holds an advisory cas/<cluster>/prune.marker — concurrent cas-upload and cas-delete refuse while it's held. See docs/cas-design.md §6.7 and docs/cas-operator-runbook.md.",
			Action: func(c *cli.Context) error {
				b := backup.NewBackuper(config.GetConfigFromCli(c))
				return b.CASPrune(c.Bool("dry-run"), c.String("grace-blob"), c.String("abandon-threshold"), c.Bool("unlock"), c.Int("command-id"))
			},
			Flags: append(rootFlags,
				cli.BoolFlag{
					Name:  "dry-run",
					Usage: "Print orphan candidates without deleting anything (no marker is written)",
				},
				cli.StringFlag{
					Name:  "grace-blob",
					Value: "",
					Usage: "Override cas.grace_blob — Go duration string (e.g. \"24h\", \"30m\", \"0s\"). Empty (default) uses the configured value.",
				},
				cli.StringFlag{
					Name:  "abandon-threshold",
					Value: "",
					Usage: "Override cas.abandon_threshold — Go duration string (e.g. \"168h\", \"0s\"). Empty (default) uses the configured value.",
				},
				cli.BoolFlag{
					Name:  "unlock",
					Usage: "Delete a stranded cas/<cluster>/prune.marker (escape hatch when SIGKILL/OOM left it behind). Refuses if no marker is present.",
				},
			),
		},
	}
}
