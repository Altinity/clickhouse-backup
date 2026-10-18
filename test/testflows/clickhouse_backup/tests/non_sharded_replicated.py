from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.steps import *

# shard 1 = clickhouse1, shard 2 = clickhouse2, the first replica of the first shard restores the data of non-sharded tables
CLUSTER = "sharded_cluster"
DATABASE = "non_sharded_1181"
BACKUP_NAME = "non_sharded_1181"
GLOBAL_ROWS = 100
# MergeTree and sharded ReplicatedMergeTree data isn't shared between shards, every shard has own rows
SHARD_ROWS = {"clickhouse1": 10, "clickhouse2": 25}
SKIP_ENV = (f"--env=CLICKHOUSE_SKIP_RESTORE_NON_SHARDED_REPLICATED_TABLES=true "
            f"--env=CLICKHOUSE_SKIP_CHECK_NON_SHARDED_REPLICATED_TABLES_CLUSTER={CLUSTER}")


def count(node, table):
    return int(node.query(f"SELECT count() FROM {DATABASE}.{table}").output.strip())


def backup_container(node):
    """clickhouse_backup is bound to clickhouse1, clickhouse_backup2 to clickhouse2."""
    return current().context.backup if node.name == "clickhouse1" else current().context.backup2


# zookeeper paths of non-sharded tables, their value is the same on both shards:
# no macros, {uuid} of the table created ON CLUSTER, {shard} which is 01 on both nodes of sharded_cluster
NON_SHARDED_PATHS = {
    "global": "/clickhouse/tables/non_sharded_1181/{database}/{table}",
    "global_uuid": "/clickhouse/tables/non_sharded_1181/{uuid}",
    "global_shard_macro": "/clickhouse/tables/non_sharded_1181/{shard}/{database}/{table}",
}


@TestStep(Given)
def create_tables(self):
    """Create the same tables on both shards:
    NON_SHARDED_PATHS - both shards are replicas of one replication queue,
    `sharded` - ZooKeeper path with {shard2} macro which differs between the shards,
    `local` - MergeTree."""
    ch1, ch2 = self.context.nodes
    ch1.query(f"CREATE DATABASE IF NOT EXISTS {DATABASE} ON CLUSTER {CLUSTER}")
    for table, zk_path in NON_SHARDED_PATHS.items():
        ch1.query(f"CREATE TABLE {DATABASE}.{table} ON CLUSTER {CLUSTER} (id UInt64) "
                  f"ENGINE=ReplicatedMergeTree('{zk_path}', '{{replica}}') ORDER BY id")
    ch1.query(f"CREATE TABLE {DATABASE}.sharded ON CLUSTER {CLUSTER} (id UInt64) "
              f"ENGINE=ReplicatedMergeTree('/clickhouse/tables/non_sharded_1181/{{shard2}}/{{database}}/{{table}}', '{{replica}}') ORDER BY id")
    ch1.query(f"CREATE TABLE {DATABASE}.local ON CLUSTER {CLUSTER} (id UInt64) ENGINE=MergeTree ORDER BY id")
    for table in NON_SHARDED_PATHS:
        ch1.query(f"INSERT INTO {DATABASE}.{table} SELECT number FROM numbers({GLOBAL_ROWS})")
    for node in (ch1, ch2):
        node.query(f"INSERT INTO {DATABASE}.sharded SELECT number FROM numbers({SHARD_ROWS[node.name]})")
        node.query(f"INSERT INTO {DATABASE}.local SELECT number FROM numbers({SHARD_ROWS[node.name]})")
    sync_non_sharded()
    for node in (ch1, ch2):
        for table in NON_SHARDED_PATHS:
            assert count(node, table) == GLOBAL_ROWS, error(table)


@TestStep(When)
def sync_non_sharded(self):
    for node in self.context.nodes:
        for table in NON_SHARDED_PATHS:
            node.query(f"SYSTEM SYNC REPLICA {DATABASE}.{table}", query_settings=[("receive_timeout", 600)])


@TestStep(Given)
def create_backups(self):
    """Every shard creates own local backup, both backups contain the whole data of `global`."""
    for node in self.context.nodes:
        backup_container(node).cmd(f"clickhouse-backup create --tables={DATABASE}.* {BACKUP_NAME}")


@TestStep(When)
def drop_database(self):
    self.context.nodes[0].query(f"DROP DATABASE IF EXISTS {DATABASE} ON CLUSTER {CLUSTER} SYNC")


@TestStep(When)
def restore(self, node):
    """Restore with the same skip_restore_non_sharded_replicated_tables settings on every shard."""
    r = backup_container(node).cmd(f"clickhouse-backup {SKIP_ENV} restore --rm --tables={DATABASE}.* {BACKUP_NAME}")
    n = len(NON_SHARDED_PATHS)
    with Then("{shard} from always_sharded_macros has the same value on both shards"):
        assert "{shard} listed in always_sharded_macros have the same value on all 2 shards" in r.output, error(r.output)
    if node.name == "clickhouse1":
        summary = f"restore data of {n} non-sharded replicated tables"
    else:
        summary = f"skip data restore of {n} non-sharded replicated tables"
    assert summary in r.output, error(r.output)
    summary_line = r.output.split(summary)[1].split("\n")[0]
    for table in NON_SHARDED_PATHS:
        assert f"`{DATABASE}`.`{table}`" in summary_line, error(summary_line)
    with Then("{shard2} differs between the shards, the table with it is sharded"):
        assert f"`{DATABASE}`.`sharded`" not in summary_line, error(summary_line)


@TestStep(Then)
def check_restored(self):
    """non-sharded tables have the data of one backup on both shards, not the doubled one,
    the sharded data is restored on every shard, the zookeeper paths are kept."""
    ch1, ch2 = self.context.nodes
    sync_non_sharded()
    for node in (ch1, ch2):
        with By(f"check {node.name}"):
            for table in NON_SHARDED_PATHS:
                assert count(node, table) == GLOBAL_ROWS, error(table)
            assert count(node, "sharded") == SHARD_ROWS[node.name], error()
            assert count(node, "local") == SHARD_ROWS[node.name], error()
            engine = node.query(f"SELECT engine_full FROM system.tables WHERE database='{DATABASE}' AND name='global' FORMAT TSVRaw").output
            assert "/clickhouse/tables/non_sharded_1181/" in engine and "{cluster}" not in engine, error(engine)


@TestStep(Finally)
def cleanup(self):
    for node in self.context.nodes:
        backup_container(node).cmd(f"clickhouse-backup delete local {BACKUP_NAME}", exitcode=None)
    drop_database()


@TestOutline
def restore_shards(self, order):
    nodes = {n.name: n for n in self.context.nodes}
    try:
        create_tables()
        create_backups()
        with When("I drop the database on every shard, like a restore into a new cluster"):
            drop_database()
        with When("I check that dry-run on the second shard reports the skipped data"):
            r = self.context.backup2.cmd(f"clickhouse-backup {SKIP_ENV} restore --rm --dry-run {BACKUP_NAME}")
            assert "skipped_data_tables" in r.output, error(r.output)
            for table in NON_SHARDED_PATHS:
                assert f"{DATABASE}.{table}" in r.output, error(table)
        for name in order:
            with When(f"I restore on {name}"):
                restore(node=nodes[name])
                if name == "clickhouse2" and order[0] == "clickhouse2":
                    with Then("the second shard restores only the schema of the non-sharded tables"):
                        for table in NON_SHARDED_PATHS:
                            assert count(nodes[name], table) == 0, error(table)
        check_restored()
    finally:
        cleanup()


@TestScenario
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedTable("1.0"))
def second_shard_first(self):
    """The second shard restores the schema of the non-sharded table without data, then the first shard restores the data,
    it's replicated to the second shard, https://github.com/Altinity/clickhouse-backup/issues/1181."""
    restore_shards(order=["clickhouse2", "clickhouse1"])


@TestScenario
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedTable("1.0"))
def first_shard_first(self):
    """The first shard restores the data of the non-sharded table, then `restore --rm` on the second shard drops and creates
    only its own replica and fetches the data instead of attaching it again, https://github.com/Altinity/clickhouse-backup/issues/1181."""
    restore_shards(order=["clickhouse1", "clickhouse2"])


@TestFeature
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedTable("1.0"))
def non_sharded_replicated(self):
    """skip_restore_non_sharded_replicated_tables: replicated tables without {shard} in the ZooKeeper path are restored with data
    only on the first replica of the first shard, https://github.com/Altinity/clickhouse-backup/issues/1181."""
    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario, flags=TE)
