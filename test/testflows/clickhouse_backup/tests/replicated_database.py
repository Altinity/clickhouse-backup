import os

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.embedded_on_cluster import MINIO_URL, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from clickhouse_backup.tests.steps import *

CLUSTER = "replicated_cluster"
RMT_TABLES = 20
RMT_ROWS = 100
MT_ROWS = {"clickhouse1": 10, "clickhouse2": 25}


def rmt_tables():
    return [f"rmt_{i}" for i in range(RMT_TABLES)]


def expected_tables():
    return sorted(rmt_tables() + ["dist", "mt", "mv", "mv_target", "v"])


def count(node, database, table):
    return int(node.query(f"SELECT count() FROM {database}.{table}").output.strip())


@TestStep(Given)
def create_replicated_database(self, database, database_settings=""):
    """Create ENGINE=Replicated database on both replicas with ReplicatedMergeTree, MergeTree, MV, View and Distributed
    tables, the same table set what ACM customers usually have in a Replicated database."""
    ch1, ch2 = self.context.nodes
    ch1.query(f"CREATE DATABASE {database} ON CLUSTER {CLUSTER} "
              f"ENGINE=Replicated('/clickhouse/databases/{database}', '{{shard}}', '{{replica}}') {database_settings}")
    # DDL inside a Replicated database executes only on the initiator and propagates via the database DDL log,
    # ON CLUSTER is not allowed here
    for table in rmt_tables():
        ch1.query(f"CREATE TABLE {database}.{table} (id UInt64, v String) ENGINE=ReplicatedMergeTree ORDER BY id")
    ch1.query(f"CREATE TABLE {database}.mt (id UInt64, v String) ENGINE=MergeTree ORDER BY id")
    ch1.query(f"CREATE TABLE {database}.mv_target (id UInt64, v String) ENGINE=ReplicatedMergeTree ORDER BY id")
    ch1.query(f"CREATE MATERIALIZED VIEW {database}.mv TO {database}.mv_target AS SELECT id, v FROM {database}.rmt_0")
    ch1.query(f"CREATE VIEW {database}.v AS SELECT count() AS c FROM {database}.rmt_0")
    ch1.query(f"CREATE TABLE {database}.dist AS {database}.rmt_0 ENGINE=Distributed('{CLUSTER}', '{database}', 'rmt_0')")

    for table in rmt_tables():
        ch1.query(f"INSERT INTO {database}.{table} SELECT number, toString(number) FROM numbers({RMT_ROWS})")
    # MergeTree data isn't replicated, every replica has own rows
    for node in (ch1, ch2):
        node.query(f"INSERT INTO {database}.mt SELECT number, toString(number) FROM numbers({MT_ROWS[node.name]})")
    sync_replicas(database=database)


@TestStep(When)
def sync_replicas(self, database):
    """Wait until both replicas applied the whole database DDL log and fetched all ReplicatedMergeTree parts."""
    for node in self.context.nodes:
        node.query(f"SYSTEM SYNC DATABASE REPLICA {database}", query_settings=[("receive_timeout", 600)])
        tables = node.query(f"SELECT name FROM system.tables WHERE database='{database}' "
                            f"AND engine LIKE 'Replicated%MergeTree' FORMAT TSVRaw").output.split()
        for table in tables:
            node.query(f"SYSTEM SYNC REPLICA {database}.{table}", query_settings=[("receive_timeout", 600)])


@TestStep(When)
def diverge_data(self, database):
    """Make the live data differ from the backup, so a successful restore --rm is visible."""
    ch1, ch2 = self.context.nodes
    for table in rmt_tables():
        ch1.query(f"INSERT INTO {database}.{table} SELECT number, 'diverged' FROM numbers({RMT_ROWS}, 7)")
    for node in (ch1, ch2):
        node.query(f"TRUNCATE TABLE {database}.mt")
    sync_replicas(database=database)


@TestStep(Then)
def check_restored(self, database):
    """Both replicas have the same table set and data as at backup time, and no tables were moved to
    <db>_broken_tables / <db>_broken_replicated_tables by Replicated database recovery."""
    ch1, ch2 = self.context.nodes
    sync_replicas(database=database)
    for node in (ch1, ch2):
        with By(f"check {node.name}"):
            tables = node.query(f"SELECT name FROM system.tables WHERE database='{database}' ORDER BY name FORMAT TSVRaw").output.split()
            assert tables == expected_tables(), error()
            engine = node.query(f"SELECT engine FROM system.databases WHERE name='{database}' FORMAT TSVRaw").output.strip()
            assert engine == "Replicated", error()
            broken = node.query(f"SELECT name FROM system.databases WHERE name LIKE '{database}_broken%' FORMAT TSVRaw").output.strip()
            assert broken == "", error()
            for table in rmt_tables():
                assert count(node, database, table) == RMT_ROWS, error(table)
            # rows were inserted into rmt_0 after mv was created, so mv_target has the same rows at backup time
            assert count(node, database, "mv_target") == RMT_ROWS, error()
            assert int(node.query(f"SELECT c FROM {database}.v").output.strip()) == RMT_ROWS, error()
            assert count(node, database, "dist") == RMT_ROWS, error()
    with By("MergeTree data restored only from clickhouse1 backup on the replica where restore --data was executed"):
        assert count(ch1, database, "mt") == MT_ROWS[ch1.name], error()


@TestStep(When)
def backup_command(self, node, command):
    """Execute clickhouse-backup command in the backup container bound to node. ACM sends the same commands via
    POST /backup/actions (system.backup_actions), the server executes the same code, CLI keeps full logs in the test output."""
    backup = self.context.backup if node.name == "clickhouse1" else self.context.backup2
    backup.cmd(f"clickhouse-backup {command}")


@TestStep(When)
def acm_restore(self, backup_name, schema_order):
    """Emulate ACM BackupManager::restoreBackup:
    download on every replica, restore --schema --rm sequentially on every replica (only shard1/replica1 with
    RESTORE_SCHEMA_ON_CLUSTER), then restore --data only on the first replica of the shard."""
    ch1, ch2 = self.context.nodes
    for node in (ch1, ch2):
        backup_command(node=node, command=f"download {backup_name}")
    for node in schema_order:
        on_cluster = f" --env=RESTORE_SCHEMA_ON_CLUSTER={CLUSTER}" if node.name == ch1.name else ""
        backup_command(node=node, command=f"restore --schema --rm{on_cluster} {backup_name}")
    backup_command(node=ch1, command=f"restore --data {backup_name}")


@TestStep(Given)
def create_backup(self, database, backup_name):
    """Backup is created on the first replica only, like ACM and Examples.md How to back up / restore a sharded cluster."""
    ch1 = self.context.nodes[0]
    backup_command(node=ch1, command=f"create_remote --tables={database}.* {backup_name}")


@TestStep(Finally)
def drop_restored_database(self, database, backup_name):
    ch1 = self.context.nodes[0]
    self.context.backup.cmd(f"clickhouse-backup delete remote {backup_name}", exitcode=None)
    for node in (self.context.backup, self.context.backup2):
        node.cmd(f"clickhouse-backup delete local {backup_name}", exitcode=None)
    ch1.query(f"DROP DATABASE IF EXISTS {database} ON CLUSTER {CLUSTER} SYNC", no_checks=True)
    for node in self.context.nodes:
        node.query(f"DROP DATABASE IF EXISTS {database}_broken_tables SYNC", no_checks=True)
        node.query(f"DROP DATABASE IF EXISTS {database}_broken_replicated_tables SYNC", no_checks=True)


@TestOutline
def acm_workflow(self, database, schema_order_names, drop_database_before_restore=False, database_settings=""):
    backup_name = f"shard01-{database}"
    nodes = {n.name: n for n in self.context.nodes}
    try:
        create_replicated_database(database=database, database_settings=database_settings)
        create_backup(database=database, backup_name=backup_name)
        if drop_database_before_restore:
            with When("I drop the database on every replica, like a restore into a new cluster"):
                self.context.nodes[0].query(f"DROP DATABASE {database} ON CLUSTER {CLUSTER} SYNC")
        else:
            diverge_data(database=database)
        acm_restore(backup_name=backup_name, schema_order=[nodes[n] for n in schema_order_names])
        check_restored(database=database)
    finally:
        drop_restored_database(database=database, backup_name=backup_name)


@TestScenario
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedDatabase("1.0"))
def acm_restore_existing_database(self):
    """ACM restore over existing Replicated database, first replica with ON CLUSTER goes first, #1611."""
    acm_workflow(database="repl_db_acm", schema_order_names=["clickhouse1", "clickhouse2"])


@TestScenario
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedDatabase("1.0"))
def acm_restore_existing_database_reverse_order(self):
    """ACM doesn't sort nodes, the replica without ON CLUSTER can restore schema before the first replica."""
    acm_workflow(database="repl_db_acm_rev", schema_order_names=["clickhouse2", "clickhouse1"])


@TestScenario
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedDatabase("1.0"))
def acm_restore_dropped_database(self):
    """ACM restore into a cluster where the Replicated database doesn't exist."""
    acm_workflow(database="repl_db_acm_new", schema_order_names=["clickhouse1", "clickhouse2"],
                 drop_database_before_restore=True)


@TestScenario
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedDatabase("1.0"))
def acm_restore_replication_lag(self):
    """Replica which restores schema can lag behind the database DDL log, ClickHouse rejects DDL with code 529
    `Cannot enqueue query on this replica, because it has replication lag`, issue #1611 has 678 tables with default
    max_replication_lag_to_enqueue=50, low value reproduces the same with a few tables."""
    acm_workflow(database="repl_db_acm_lag", schema_order_names=["clickhouse1", "clickhouse2"],
                 database_settings="SETTINGS max_replication_lag_to_enqueue=1")


@TestScenario
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedDatabase("1.0"))
def examples_restore_sharded_cluster(self):
    """Examples.md How to back up / restore a sharded cluster: restore_remote --rm --schema on all replicas,
    then restore_remote --rm on the first replica of each shard."""
    database = "repl_db_examples"
    backup_name = f"shard01-{database}"
    backup, backup2 = self.context.backup, self.context.backup2
    try:
        create_replicated_database(database=database)
        create_backup(database=database, backup_name=backup_name)
        diverge_data(database=database)
        with When("I restore schema on all replicas"):
            for node in (backup, backup2):
                node.cmd(f"clickhouse-backup restore_remote --rm --schema {backup_name}")
                node.cmd(f"clickhouse-backup delete local {backup_name}")
        with And("I restore schema and data on the first replica"):
            backup.cmd(f"clickhouse-backup restore_remote --rm {backup_name}")
            backup.cmd(f"clickhouse-backup delete local {backup_name}")
        check_restored(database=database)
    finally:
        drop_restored_database(database=database, backup_name=backup_name)


@TestFeature
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedDatabase("1.0"))
def replicated_database(self):
    """Restore ENGINE=Replicated database on several replicas, https://github.com/Altinity/clickhouse-backup/issues/1611."""
    # SYSTEM SYNC DATABASE REPLICA, used to wait for the database DDL log, exists since 22.5
    if os.environ.get("CLICKHOUSE_VERSION", "26.8") < "22.5":
        skip("SYSTEM SYNC DATABASE REPLICA needs ClickHouse 22.5+")
    try:
        # default_replica_path in configs/clickhouse/config.d is the same as ACM uses,
        # ReplicatedMergeTree without arguments inside Replicated database keeps it unexpanded in metadata
        with Given("I configure both clickhouse-backup containers with the same minio remote storage"):
            config_modifier(fields={
                # ACM keeps no local backups, create_remote deletes local copy after upload
                "general": {"remote_storage": "s3", "backups_to_keep_local": -1},
                "s3": {
                    "access_key": MINIO_ACCESS_KEY,
                    "secret_key": MINIO_SECRET_KEY,
                    "endpoint": MINIO_URL,
                    "bucket": MINIO_BUCKET,
                    "disable_ssl": True,
                    "force_path_style": True,
                    "region": "us-west-2",
                },
            })

        for scenario in loads(current_module(), Scenario, Suite):
            Scenario(run=scenario, flags=TE)

    finally:
        with Finally("I restore the original config"):
            config_modifier()
