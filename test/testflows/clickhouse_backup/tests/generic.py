import time
import random
import itertools

from testflows.core import *
from testflows.asserts import *

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.steps import *


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_SpecificTables_ManyColumns("1.0")
)
def many_columns(self):
    """Test that a table with many columns can be backed up and restored.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    name_prefix = "backup_manycols"
    columns = {}

    col_types = list(self.context.columns.values())
    ntypes = len(col_types)
    for i in range(500):
        columns[f"c{i}"] = col_types[i % ntypes]

    try:
        with Given("I create table"):
            create_and_populate_table(node=clickhouse, table_name=f"{name_prefix}", columns=columns)
            table_data = clickhouse.query(f"SELECT * FROM {name_prefix}").output

        with When(f"I create a backup for {name_prefix}"):
            backup.cmd(f"clickhouse-backup create --tables=default.{name_prefix} {name_prefix}")

        with And("I drop table"):
            drop_table(node=clickhouse, table_name=f"{name_prefix}")

        with And("I restore table"):
            backup.cmd(f"clickhouse-backup restore --tables=default.{name_prefix} {name_prefix}")

        with Then("I expect table restored"):
            r = clickhouse.query("SHOW TABLES").output
            with By(f"expect {name_prefix} exists"):
                assert f"{name_prefix}" in r, error()
            with And(f"expect {name_prefix} data restored"):
                assert clickhouse.query(f"SELECT * FROM {name_prefix}").output == table_data, error()

    finally:
        with Finally("I remove created backup"):
            backup.cmd(f"clickhouse-backup delete local {name_prefix}")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_AllDataTypes("1.0")
)
def backup_all_datatypes(self):
    """Test that table containing any of supported data types can be backed up and restored.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    name_prefix = "bp_all_dt"
    aggr_funcs = ["any", "anyLast","min", "max", "sum", "sumWithOverflow"]
    multiword_types = {"dprec": "DOUBLE PRECISION", "clo": "CHAR LARGE OBJECT", "chv": "CHAR VARYING",
                       "crlo": "CHARACTER LARGE OBJECT", "ctrv": "CHARACTER VARYING", "nclo": "NCHAR LARGE OBJECT",
                       "ncv": "NCHAR VARYING", "natclo": "NATIONAL CHARACTER LARGE OBJECT",
                       "natchrv": "NATIONAL CHARACTER VARYING", "natchv": "NATIONAL CHAR VARYING",
                       "nchar": "NATIONAL CHARACTER", "nch": "NATIONAL CHAR",
                       "blo": "BINARY LARGE OBJECT", "bivar": "BINARY VARYING"}

    all_columns = {}
    simple_columns = {}
    for v in self.context.all_columns.values():
        simple_columns.update(v)

    all_columns.update(simple_columns)

    with Given("I create a table schema"):
        for col_name, col_type in simple_columns.items():
            if col_name in ("OrderBy", "Sign", "Version", "Path", "Time", "Value"):
                continue

            all_columns[f"Tup{col_name}"] = f"Tuple({col_type})"
            all_columns[f"Arr{col_name}"] = f"Array({col_type})"
            all_columns[f"ArrTup{col_name}"] = f"Array(Tuple({col_type}))"
            all_columns[f"TupArr{col_name}"] = f"Tuple(Array({col_type}))"

        for k, v in itertools.product(["String", "Integer"], ["String", "Integer"]):
            all_columns[f"Map{k}{v}"] = f"Map({k}, {v})"

        for t in ["String", "Date", "DateTime"]:
            all_columns[f"LC{t}"] = f"LowCardinality({t})"

        nested_str = ", ".join([f"{i} {j}" for i, j in simple_columns.items()])
        all_columns["nested"] = f"Nested({nested_str})"

        for ag_func in aggr_funcs:
            all_columns[f"saf_{ag_func}"] = f"SimpleAggregateFunction({ag_func}, Double)"
            all_columns[f"af_{ag_func}"] = f"AggregateFunction({ag_func}, Double)"

        for k, v in multiword_types.items():
            all_columns[k] = v

    try:
        with Given("I create table"):
            create_and_populate_table(node=clickhouse, table_name=name_prefix, columns=all_columns, native=True)
            table_data = clickhouse.query(f"SELECT * FROM {name_prefix}").output
            debug(clickhouse.query(f"DESCRIBE {name_prefix}").output)

        with When("I create backup"):
            backup.cmd(f"clickhouse-backup create --tables=default.{name_prefix} {name_prefix}")

        with And("I drop created table"):
            drop_table(node=clickhouse, table_name=name_prefix)

        with Then("I restore tables"):
            backup.cmd(f"clickhouse-backup restore --tables=default.{name_prefix} {name_prefix}")

        with And("I expect table restored"):
            r = clickhouse.query("SHOW TABLES").output

            with By(f"expect {name_prefix} exists"):
                assert f"{name_prefix}" in r, error()

            with And(f"expect {name_prefix} data restored"):
                assert clickhouse.query(f"SELECT * FROM default.{name_prefix}").output == table_data, error()

    finally:
        with Finally("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local {name_prefix}")

        with And("I drop table"):
            drop_table(node=clickhouse, table_name=name_prefix)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_RestorePartiallyDropped("1.0")
)
def restore_partially_dropped(self):
    """Test that table can be restored once part of it was lost.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    name_prefix = "bp_part"

    try:
        with Given("I create table"):
            create_and_populate_table(node=clickhouse, table_name=name_prefix, columns=self.context.columns, native=True)
            table_data = clickhouse.query(f"SELECT * FROM {name_prefix}").output

        with When("I create backup"):
            backup.cmd(f"clickhouse-backup create --tables=default.{name_prefix} {name_prefix}")

        with And("I drop some data in created table"):
            clickhouse.query(f"ALTER TABLE default.{name_prefix} DELETE WHERE Sign=1")

        with Then("I restore tables"):
            backup.cmd(f"clickhouse-backup restore --tables=default.{name_prefix} {name_prefix}")

        with And("I expect table restored"):
            r = clickhouse.query("SHOW TABLES").output

            with By(f"expect {name_prefix} exists"):
                assert f"{name_prefix}" in r, error()

            with And(f"expect {name_prefix} data restored"):
                assert clickhouse.query(f"SELECT * FROM default.{name_prefix}").output == table_data, error()

    finally:
        with Finally("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local {name_prefix}")

        with And("I drop table"):
            drop_table(node=clickhouse, table_name=name_prefix)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedTable("1.0")
)
def restore_one_replica(self):
    """Test that replicas remain consistent if one replica is restored from a backup.
    """
    clickhouse1, clickhouse2 = self.context.nodes
    backup = self.context.backup
    name_prefix = "bp_one_repl"

    try:
        with Given("I create replicated table"):
            with By("I create and populate table"):
                create_and_populate_table(node=clickhouse1, table_name=name_prefix,
                                          engine="ReplicatedMergeTree", columns=self.context.columns)
                table_data = clickhouse1.query(f"SELECT * FROM {name_prefix}").output

            with And("I create a replica for created table"):
                create_table(node=clickhouse2, table_name=name_prefix,
                             columns=self.context.columns, engine="ReplicatedMergeTree")
                time.sleep(1)

        with When("I create backup"):
            backup.cmd(f"clickhouse-backup create --tables=default.{name_prefix} {name_prefix}")

        with And("I drop some data in table"):
            clickhouse1.query(f"ALTER TABLE default.{name_prefix} DELETE WHERE Sign=1")
            time.sleep(10)

        with Then("I restore table"):
            backup.cmd(f"clickhouse-backup restore --tables=default.{name_prefix} {name_prefix}")
            time.sleep(10)

        with And("I expect data restored on both replicas"):
            query = f"SELECT * FROM default.{name_prefix}"
            tables_data = (clickhouse1.query(query).output.split('\n'), clickhouse2.query(query).output.split('\n'))

            with By("I compare data in both tables"):
                assert set(tables_data[0]) <= set(tables_data[1]) <= set(tables_data[0]), error()

            with And("I check data restored"):
                assert set(tables_data[0]) <= set(table_data.split('\n')) <= set(tables_data[0]), error()

    finally:
        with Finally("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local {name_prefix}")

        with And("I drop table"):
            drop_table(node=clickhouse1, table_name=name_prefix)
            drop_table(node=clickhouse2, table_name=name_prefix)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_AllTables("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Create("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Restore("1.0"),
)
def backup_and_restore_all_tables(self):
    """Test that ALL existing tables can be backed up and restored.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    table_engines = self.context.table_engines
    name_prefix = "bp_all"
    database_names = list(self.context.database_engines_names.values())

    for dbe, dbn in self.context.database_engines_names.items():
        with Given(f"I create {dbe} database"):
            clickhouse.query(f"CREATE DATABASE {dbn} ENGINE = {dbe}")

    try:
        table_data = {dbn: {} for dbn in database_names}
        with Given("I create tables"):
            for dbn in database_names:
                for e in table_engines:
                    create_and_populate_table(node=clickhouse, database=dbn, table_name=f"{name_prefix}_{e}")

                    with By(f"save data from {e} table"):
                        table_data[dbn][f"{name_prefix}_{e}"] = clickhouse.query(f"SELECT * FROM {dbn}.{name_prefix}_{e}").output

        with When("I create backup for all tables"):
            backup.cmd(f"clickhouse-backup create {name_prefix}")

        with And("I drop created tables"):
            for dbn in table_data.keys():
                for table_name in table_data[dbn].keys():
                    drop_table(node=clickhouse, database=dbn, table_name=table_name)

        with Then("I restore tables"):
            backup.cmd(f"clickhouse-backup restore {name_prefix}")

        with And("I expect tables restored"):
            for dbn in table_data.keys():
                r = clickhouse.query(f"SHOW TABLES FROM {dbn}").output

                for table_name in table_data[dbn].keys():
                    with By(f"expect {table_name} exists"):
                        assert f"{table_name}" in r, error()
                    with And(f"expect {table_name} data restored"):
                        assert clickhouse.query(f"SELECT * FROM {dbn}.{table_name}").output == table_data[dbn][table_name], error()

    finally:
        with Finally("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local {name_prefix}")

        with And("I drop tables"):
            for dbn in table_data.keys():
                for table_name in table_data[dbn].keys():
                    drop_table(node=clickhouse, database=dbn, table_name=table_name)

        with And("I drop databases"):
            for dbn in database_names:
                clickhouse.query(f"DROP DATABASE {dbn}")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_SpecificTables("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedTable("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Create("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Restore("1.0"),
)
def backup_and_restore_specific_table(self):
    """Test that a specific table can be backed up and restored.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    table_engines = self.context.table_engines + [f"Replicated{e}" for e in self.context.table_engines]
    name_prefix = "backup_specific"
    table_data = {}

    try:
        with Given("creating tables to be backed up"):
            for e in table_engines:
                create_and_populate_table(node=clickhouse, table_name=f"{name_prefix}_{e}")

                with By(f"save data from {e} table"):
                    table_data[f"{name_prefix}_{e}"] = clickhouse.query(f"SELECT * FROM {name_prefix}_{e}").output

        for table_name in table_data.keys():
            with When(f"create a backup for {table_name}"):
                backup.cmd(f"clickhouse-backup create --tables=default.{table_name} {table_name}")

            with And("drop table"):
                drop_table(node=clickhouse, table_name=f"{table_name}")

            with And("restore table"):
                backup.cmd(f"clickhouse-backup restore --tables=default.{table_name} {table_name}")

            with Then("expect table restored"):
                r = clickhouse.query("SHOW TABLES").output
                with By(f"expect {table_name} exists"):
                    assert f"{table_name}" in r, error()
                with And(f"expect {table_name} data restored"):
                    assert clickhouse.query(f"SELECT * FROM {table_name}").output == table_data[table_name], error()

    finally:
        with Finally("remove all created backups"):
            for table_name in table_data.keys():
                with When(f"delete backup for {table_name}"):
                    backup.cmd(f"clickhouse-backup delete local {table_name}")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_MultipleBackups_EfficientStorageByUsingHardLinks("1.0")
)
def efficient_hardlinks(self):
    """Test that backups are created by manipulating hardlinks and storage is consumed efficiently.
    """

    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    table_name = "bp_hlns"

    try:
        with Given("I create and populate table"):
            create_and_populate_table(node=clickhouse, table_name=table_name, size=10)

        with When("I create backup for the table"):
            backup.cmd(f"clickhouse-backup create {table_name}")

        with And("I collect all backup files inodes"):
            bp_files = backup.cmd(f"ls -lih --color=none /var/lib/clickhouse/backup/{table_name}/shadow/default/"
                                  f"{table_name}/default/all_1_1_0").output.split('\n')[1:]
            bp_file_inode = {entry.split()[9]: entry.split()[0] for entry in bp_files}

        with And("I collect all CH files inodes"):
            ch_files = backup.cmd(f"ls -lih --color=none /var/lib/clickhouse/data/default/"
                                  f"{table_name}/all_1_1_0").output.split('\n')[1:]
            ch_file_inode = {entry.split()[9]: entry.split()[0] for entry in ch_files}

        with Then("I expect corresponding inodes to match"):
            for filename in bp_file_inode.keys():
                if filename in ch_file_inode.keys():
                    with By(f"check {filename}"):
                        assert bp_file_inode[filename] == ch_file_inode[filename], error()

    finally:
        with Finally("I remove table"):
            drop_table(node=clickhouse, table_name=f"{table_name}")

        with And("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local {table_name}")


@TestOutline
def storage_types_outline(self, policy_name):
    """Test different storage types.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    table_name = policy_name

    try:
        with Given("I create and populate table"):
            create_and_populate_table(node=clickhouse, table_name=table_name, size=1000,
                                      settings=f"storage_policy = '{policy_name}'")

        with When("create a backup"):
            backup.cmd(f"clickhouse-backup create --tables=default.{table_name} {table_name}")

        with And("drop created table"):
            drop_table(node=clickhouse, table_name=table_name)

        with Then("restore table"):
            backup.cmd(f"clickhouse-backup restore --tables=default.{table_name} {table_name}")

        with And("expect table restored"):
            r = clickhouse.query("SHOW TABLES").output
            assert table_name in r, error()

    finally:
        with Finally("I remove created backup"):
            backup.cmd(f"clickhouse-backup delete local {table_name}")
        with And("I drop created table"):
            drop_table(node=clickhouse, table_name=table_name)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_TieredStorage("1.0")
)
def tiered_storage(self):
    """Test clickhouse-backup working with tiered storage.
    """
    storage_types_outline(policy_name="tiered_policy")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_EncryptedStorage("1.0")
)
def encrypted_storage(self):
    """Test clickhouse-backup working with encrypted storage.
    """
    storage_types_outline(policy_name="encrypted_policy")


@TestFeature
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_TableEngines_MergeTree("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_DatabaseEngines_Atomic("1.0"),
)
def generic(self):
    """Check the basic clickhouse-backup features.
    """
    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario)
