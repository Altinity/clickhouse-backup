package backup

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
)

// Embedded BACKUP/RESTORE ... ON CLUSTER orchestration, https://github.com/Altinity/clickhouse-backup/issues/928
//
// ClickHouse executes BACKUP/RESTORE ON CLUSTER on every node of the cluster, each node writes/reads its own
// `shards/N/replicas/M/{metadata,data}` part of the same shared destination and only the initiator writes `.backup`.
// clickhouse-backup metadata (`metadata.json`, per table `.json`) and the `.sql` fixes before RESTORE are node local,
// so the node which runs the command (initiator) drives every other node (worker) through its `system.backup_actions`
// table with the same command plus `--embedded-on-cluster-worker`, which skips the BACKUP/RESTORE SQL and touches only
// the own `shards/N/replicas/M` prefix.

// embeddedOnClusterWorkerFlag is the CLI flag which turns create/upload/download/restore into the per node part of an
// embedded BACKUP/RESTORE ON CLUSTER
const embeddedOnClusterWorkerFlag = "embedded-on-cluster-worker"

// embeddedClusterPollInterval is the delay between two system.backup_actions polls of the worker nodes
const embeddedClusterPollInterval = 5 * time.Second

// embeddedClusterHost is one non-local replica of `use_embedded_backup_restore_cluster`
type embeddedClusterHost struct {
	HostName   string `ch:"host_name"`
	Port       uint16 `ch:"port"`
	ShardNum   uint32 `ch:"shard_num"`
	ReplicaNum uint32 `ch:"replica_num"`
}

func (h embeddedClusterHost) String() string {
	return fmt.Sprintf("%s:%d (shard %d, replica %d)", h.HostName, h.Port, h.ShardNum, h.ReplicaNum)
}

// remoteBackupActions renders the remote()/remoteSecure() table function which reads system.backup_actions of the host,
// system.clusters has no `secure` column, so every node is expected to use the same transport as `clickhouse.secure`
func (h embeddedClusterHost) remoteBackupActions(user, password string, secure bool) string {
	tableFunction := "remote"
	if secure {
		tableFunction = "remoteSecure"
	}
	return fmt.Sprintf("%s(%s, 'system', 'backup_actions', %s, %s)", tableFunction, chQuote(fmt.Sprintf("%s:%d", h.HostName, h.Port)), chQuote(user), chQuote(password))
}

// chQuote renders s as a ClickHouse single quoted string literal
func chQuote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// shlexQuote wraps v in double quotes, so github.com/google/shlex in the API server returns it as one argument
func shlexQuote(v string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(v) + `"`
}

// workerCommand accumulates the CLI arguments of the command a worker node executes via system.backup_actions
type workerCommand struct {
	args []string
}

func newWorkerCommand(command string) *workerCommand {
	return &workerCommand{args: []string{command}}
}

func (w *workerCommand) flag(name string, set bool) *workerCommand {
	if set {
		w.args = append(w.args, "--"+name)
	}
	return w
}

func (w *workerCommand) value(name, v string) *workerCommand {
	if v != "" {
		w.args = append(w.args, fmt.Sprintf("--%s=%s", name, shlexQuote(v)))
	}
	return w
}

func (w *workerCommand) values(name string, vs []string) *workerCommand {
	for _, v := range vs {
		w.value(name, v)
	}
	return w
}

// String terminates the command with the worker flag and the backup name
func (w *workerCommand) String(backupName string) string {
	args := append(append([]string{}, w.args...), "--"+embeddedOnClusterWorkerFlag, shlexQuote(backupName))
	return strings.Join(args, " ")
}

// embeddedClusterCreateWorkerCommand renders `create`/`create_remote` for a worker node, rbac/configs/named collections
// stay on the initiator
func embeddedClusterCreateWorkerCommand(command, backupName, diffFromRemote, tablePattern string, partitions []string, schemaOnly, skipCheckPartsColumns, deleteSource bool) string {
	return newWorkerCommand(command).
		value("tables", tablePattern).
		values("partitions", partitions).
		value("diff-from-remote", diffFromRemote).
		flag("schema", schemaOnly).
		flag("skip-check-parts-columns", skipCheckPartsColumns).
		flag("delete-source", deleteSource).
		String(backupName)
}

// embeddedClusterRestoreWorkerCommand renders `restore`/`restore_remote` for a worker node, rbac/configs/named
// collections stay on the initiator
func embeddedClusterRestoreWorkerCommand(command, backupName, tablePattern string, databaseMapping, tableMapping, partitions []string, schemaOnly, dataOnly, dropExists, ignoreDependencies, schemaAsAttach, skipEmptyTables bool) string {
	return newWorkerCommand(command).
		value("tables", tablePattern).
		values("restore-database-mapping", databaseMapping).
		values("restore-table-mapping", tableMapping).
		values("partitions", partitions).
		flag("schema", schemaOnly).
		flag("data", dataOnly).
		flag("rm", dropExists).
		flag("ignore-dependencies", ignoreDependencies).
		flag("restore-schema-as-attach", schemaAsAttach).
		flag("skip-empty-tables", skipEmptyTables).
		String(backupName)
}

// embeddedMetadataDir returns <backupDir>/<clusterPrefix>/metadata when the cluster prefixed layout exists,
// otherwise the legacy flat <backupDir>/metadata, so backups made before ON CLUSTER support still work
func embeddedMetadataDir(backupDir, clusterPrefix string, exists func(string) bool) string {
	if clusterPrefix != "" && exists(path.Join(backupDir, clusterPrefix, "metadata")) {
		return path.Join(backupDir, clusterPrefix, "metadata")
	}
	return path.Join(backupDir, "metadata")
}

// remoteTableMetadataJSON is the remote path of a per table .json, under the cluster prefix for embedded ON CLUSTER
// backups, the legacy flat path when clusterPrefix is empty
func remoteTableMetadataJSON(backupName, clusterPrefix, database, table string) string {
	return path.Join(backupName, clusterPrefix, "metadata", common.TablePathEncode(database), fmt.Sprintf("%s.json", common.TablePathEncode(table)))
}

func dirExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}

// localMetadataDir is the per table metadata directory of a local backup, the cluster prefixed layout wins when present
func (b *Backuper) localMetadataDir(backupDir string) string {
	return embeddedMetadataDir(backupDir, b.embeddedClusterPrefix, dirExists)
}

// validateEmbeddedOnClusterWorker rejects --embedded-on-cluster-worker outside of embedded ON CLUSTER mode
func (b *Backuper) validateEmbeddedOnClusterWorker() error {
	if b.EmbeddedOnClusterWorker && (!b.cfg.ClickHouse.UseEmbeddedBackupRestore || b.cfg.ClickHouse.UseEmbeddedBackupRestoreCluster == "") {
		return errors.Errorf("--%s requires `use_embedded_backup_restore: true` and non-empty `use_embedded_backup_restore_cluster` in the `clickhouse` config section", embeddedOnClusterWorkerFlag)
	}
	return nil
}

// embeddedClusterInitiator reports whether this run drives the worker nodes: embedded ON CLUSTER mode, not a worker
// itself and not a dry-run
func (b *Backuper) embeddedClusterInitiator() bool {
	return b.cfg.ClickHouse.UseEmbeddedBackupRestore && b.cfg.ClickHouse.UseEmbeddedBackupRestoreCluster != "" && !b.EmbeddedOnClusterWorker && !b.DryRun
}

// embeddedClusterWorkerSkipsDDL reports whether the node local DDL (CREATE DATABASE, DROP TABLE, UDF) is left to the
// initiator, which executes it ON CLUSTER for every node when `restore_schema_on_cluster` is set
func (b *Backuper) embeddedClusterWorkerSkipsDDL() bool {
	return b.EmbeddedOnClusterWorker && b.cfg.General.RestoreSchemaOnCluster != ""
}

// getEmbeddedClusterRemoteHosts lists the other nodes of `use_embedded_backup_restore_cluster`, empty for a single node cluster
func (b *Backuper) getEmbeddedClusterRemoteHosts(ctx context.Context) ([]embeddedClusterHost, error) {
	clusterName, err := b.ch.ApplyMacros(ctx, b.cfg.ClickHouse.UseEmbeddedBackupRestoreCluster)
	if err != nil {
		return nil, errors.Wrap(err, "ApplyMacros for use_embedded_backup_restore_cluster")
	}
	hosts := make([]embeddedClusterHost, 0)
	query := fmt.Sprintf("SELECT host_name, port, shard_num, replica_num FROM system.clusters WHERE cluster=%s AND NOT is_local ORDER BY shard_num, replica_num", chQuote(clusterName))
	if err = b.ch.SelectContext(ctx, &hosts, query); err != nil {
		return nil, errors.Wrapf(err, "can't list remote hosts of cluster '%s'", clusterName)
	}
	return hosts, nil
}

// ensureClickHouseConnected connects b.ch when the previous command already closed it, the returned function undoes it
func (b *Backuper) ensureClickHouseConnected() (func(), error) {
	if b.ch.IsOpen {
		return func() {}, nil
	}
	if err := b.ch.Connect(); err != nil {
		return nil, errors.Wrap(err, "can't connect to clickhouse")
	}
	return b.ch.Close, nil
}

// startEmbeddedClusterWorkers enqueues command on every remote node of the cluster through its system.backup_actions
// table, each node executes it with its own `clickhouse-backup server`, returns the nodes for waitEmbeddedClusterWorkers
func (b *Backuper) startEmbeddedClusterWorkers(ctx context.Context, command string) ([]embeddedClusterHost, error) {
	closeConnection, err := b.ensureClickHouseConnected()
	if err != nil {
		return nil, err
	}
	defer closeConnection()
	hosts, err := b.getEmbeddedClusterRemoteHosts(ctx)
	if err != nil {
		return nil, err
	}
	for _, host := range hosts {
		insertSQL := fmt.Sprintf("INSERT INTO FUNCTION %s (command) VALUES (%s)", host.remoteBackupActions(b.cfg.ClickHouse.Username, b.cfg.ClickHouse.Password, b.cfg.ClickHouse.Secure), chQuote(command))
		if err = b.ch.QueryContext(ctx, insertSQL); err != nil {
			return nil, errors.Wrapf(err, "can't start `%s` on %s, every node of cluster '%s' needs a running `clickhouse-backup server` with `api.create_integration_tables: true`", command, host, b.cfg.ClickHouse.UseEmbeddedBackupRestoreCluster)
		}
		log.Info().Msgf("embedded ON CLUSTER worker started `%s` on %s", command, host)
	}
	return hosts, nil
}

// waitEmbeddedClusterWorkers polls system.backup_actions of every host until command leaves the "in progress" status,
// fails when any worker ends with an error
func (b *Backuper) waitEmbeddedClusterWorkers(ctx context.Context, hosts []embeddedClusterHost, command string) error {
	if len(hosts) == 0 {
		return nil
	}
	closeConnection, err := b.ensureClickHouseConnected()
	if err != nil {
		return err
	}
	defer closeConnection()
	type actionStatus struct {
		Status string `ch:"status"`
		Error  string `ch:"error"`
	}
	pending := hosts
	pollErrors := 0
	ticker := time.NewTicker(embeddedClusterPollInterval)
	defer ticker.Stop()
	for len(pending) > 0 {
		stillPending := make([]embeddedClusterHost, 0, len(pending))
		for _, host := range pending {
			rows := make([]actionStatus, 0)
			query := fmt.Sprintf("SELECT status, error FROM %s WHERE command = %s ORDER BY start DESC LIMIT 1", host.remoteBackupActions(b.cfg.ClickHouse.Username, b.cfg.ClickHouse.Password, b.cfg.ClickHouse.Secure), chQuote(command))
			if err = b.ch.SelectContext(ctx, &rows, query); err != nil {
				pollErrors++
				if pollErrors > b.cfg.General.RetriesOnFailure {
					return errors.Wrapf(err, "can't read status of `%s` from %s", command, host)
				}
				log.Warn().Msgf("can't read status of `%s` from %s, will retry: %v", command, host, err)
				stillPending = append(stillPending, host)
				continue
			}
			pollErrors = 0
			if len(rows) == 0 {
				return errors.Errorf("`%s` is not registered in system.backup_actions on %s", command, host)
			}
			switch rows[0].Status {
			case status.InProgressStatus:
				stillPending = append(stillPending, host)
			case status.SuccessStatus:
				log.Info().Msgf("embedded ON CLUSTER worker finished `%s` on %s", command, host)
			default:
				return errors.Errorf("`%s` on %s finished with status '%s': %s", command, host, rows[0].Status, rows[0].Error)
			}
		}
		pending = stillPending
		if len(pending) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

// runEmbeddedClusterWorkers starts command on every remote node of the cluster and waits until all of them finish
func (b *Backuper) runEmbeddedClusterWorkers(ctx context.Context, command string) error {
	hosts, err := b.startEmbeddedClusterWorkers(ctx, command)
	if err != nil {
		return err
	}
	return b.waitEmbeddedClusterWorkers(ctx, hosts, command)
}

// applyEmbeddedClusterLayout picks the remote layout of this node's clickhouse-backup files from the backup tags:
// backups tagged `cluster=<name>` keep per table .json and shadow data under shards/N/replicas/M, older ones use the flat layout
func (b *Backuper) applyEmbeddedClusterLayout(backupMetadata *metadata.BackupMetadata) {
	b.embeddedClusterFilesPrefix = ""
	if backupMetadata.EmbeddedCluster() != "" {
		b.embeddedClusterFilesPrefix = b.embeddedClusterPrefix
	}
}

// embeddedBackupTags renders the Tags of a new embedded backup, `cluster=<name>` marks the ON CLUSTER layout
func (b *Backuper) embeddedBackupTags() string {
	if b.embeddedClusterName == "" {
		return "embedded"
	}
	return "embedded,cluster=" + b.embeddedClusterName
}

// deleteLocalOnEmbeddedClusterWorkers runs `delete local <backupName>` on every remote node through system.backup_actions,
// the API delete handler is synchronous, so the INSERT returns after the node finished, a node without this backup is fine
func (b *Backuper) deleteLocalOnEmbeddedClusterWorkers(ctx context.Context, backupName string) error {
	closeConnection, err := b.ensureClickHouseConnected()
	if err != nil {
		return err
	}
	defer closeConnection()
	hosts, err := b.getEmbeddedClusterRemoteHosts(ctx)
	if err != nil {
		return err
	}
	command := "delete local --" + embeddedOnClusterWorkerFlag + " " + shlexQuote(backupName)
	for _, host := range hosts {
		insertSQL := fmt.Sprintf("INSERT INTO FUNCTION %s (command) VALUES (%s)", host.remoteBackupActions(b.cfg.ClickHouse.Username, b.cfg.ClickHouse.Password, b.cfg.ClickHouse.Secure), chQuote(command))
		if err = b.ch.QueryContext(ctx, insertSQL); err != nil {
			if strings.Contains(err.Error(), "is not found on local storage") {
				log.Info().Msgf("embedded ON CLUSTER `%s` on %s: nothing to delete", command, host)
				continue
			}
			return errors.Wrapf(err, "can't run `%s` on %s", command, host)
		}
		log.Info().Msgf("embedded ON CLUSTER `%s` done on %s", command, host)
	}
	return nil
}
