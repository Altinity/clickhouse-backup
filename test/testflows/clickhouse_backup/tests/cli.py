from testflows.core import *
from testflows.asserts import values, error, snapshot

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.steps import *

import yaml


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Usage("1.0")
)
def usage(self):
    """Test that CLI usage scenario remains the same.
    """
    backup = self.context.backup

    with When("I run clickhouse-backup with no arguments"):
        r = backup.cmd(f"clickhouse-backup")

        snap = "".join([s for s in r.output.split('\n\n') if s != "" and "VERSION" not in s])

        with Then("I expect snapshot to match"):
            with values() as that:
                assert that(snapshot(snap, "cli", name="cli_usage")), error()


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_DefaultConfig("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_Configuration("1.0")
)
def default_config(self):
    """Test that default config remains the same.
    """
    backup = self.context.backup

    with When("I run clickhouse-backup with no arguments"):
        r = backup.cmd(f"clickhouse-backup default-config").output

        with Then("I expect snapshot to match"):
            with values() as that:
                assert that(snapshot(str([e for e in r.split('\n') if not ("concurrency" in e or "max" in e)]), "cli", name="default_config")), error()


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Tables("1.0")
)
def tables(self):
    """Test that CLI `tables` command works.
    """
    backup = self.context.backup

    with When("I run clickhouse-backup tables"):
        r = backup.cmd(f"clickhouse-backup tables")

        with Then("I check my table is present"):
            assert "cli_table" in r.output, error()
            assert "cli_table" in r.output, error()


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Create("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Delete("1.0"),
)
def create_delete(self):
    """Test that CLI `create` and `delete` commands work.
    """
    backup = self.context.backup

    with Step("I check CLI create command works"):
        with When("I run clickhouse-backup create"):
            backup.cmd(f"clickhouse-backup create bp_cli_crt")

            with Then("I check backup appears in filesystem"):
                assert "bp_cli_crt" in backup.cmd("ls /var/lib/clickhouse/backup").output, error()

    with Step("I check CLI delete command works"):
        with When("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local bp_cli_crt")

            with Then("I check backup disappears from filesystem"):
                assert "bp_cli_crt" not in backup.cmd("ls /var/lib/clickhouse/backup").output, error()


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Upload("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Download("1.0"),
)
def upload_download(self):
    """Test that CLI `upload` and `download` commands work.
    """
    backup = self.context.backup

    try:
        with Given("I create a local backup"):
            backup.cmd(f"clickhouse-backup create bp_cli_ud")

        with And("I modify config file to use ftp"):
            config_modifier(fields={"general": {"remote_storage": "ftp"}})

        with When("I run clickhouse-backup upload"):
            backup.cmd(f"clickhouse-backup upload bp_cli_ud")

        with And("I delete local backup"):
            backup.cmd(f"clickhouse-backup delete local bp_cli_ud")

        with When("I download remote backup again"):
            backup.cmd(f"clickhouse-backup download bp_cli_ud")

        with Then("I check backup appears in filesystem"):
            assert "bp_cli_ud" in backup.cmd("ls /var/lib/clickhouse/backup").output, error()

    finally:
        with Finally("I remove created backups"):
            backup.cmd(f"clickhouse-backup delete local bp_cli_ud")
            backup.cmd(f"clickhouse-backup delete remote bp_cli_ud")

        with And("I set remote_storage to none"):
            config_modifier(fields={"general": {"remote_storage": "none"}})


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Help("1.0")
)
def help_flag(self):
    """Test that CLI help option exists.
    """
    backup = self.context.backup

    with When("I run clickhouse-backup with no arguments"):
        r = backup.cmd(f"clickhouse-backup --help")

        snap = "".join([s for s in r.output.split('\n\n') if s != "" and "VERSION" not in s])


        with Then("I expect snapshot to match"):
            with values() as that:
                assert that(snapshot(snap, "cli", name="help_flag")), error()


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_List("1.0")
)
def list_local_remote(self):
    """Test that list [all|local|remote] only shows corresponding existing backups.
    """
    backup = self.context.backup
    name_prefix = "cli_list"

    try:
        with Given("create a local backup"):
            backup.cmd(f"clickhouse-backup create {name_prefix}_0")

        with And("create a remote backup"):
            with By("set remote storage to ftp"):
                config_modifier(fields={"general": {"remote_storage": "ftp"}})
            with And("create a backup"):
                backup.cmd(f"clickhouse-backup create_remote {name_prefix}_1")

        with When("check the list query return only appropriate results"):
            with By("check all backups"):
                total_backups = 0
                data = [[entry for entry in line.split(' ') if entry]
                        for line in backup.cmd("clickhouse-backup list").output.replace('\t', ' ').split('\n') if name_prefix in line]
                for line in data:
                    debug(line)
                    total_backups += 1
                    assert line[4] in ("local", "remote") , error()

            with By("check local backups"):
                local_backups = 0
                data = [[entry for entry in line.split(' ') if entry]
                        for line in backup.cmd("clickhouse-backup list local").output.replace('\t', ' ').split('\n') if name_prefix in line]
                for line in data:
                    debug(line)
                    local_backups += 1
                    assert line[4] == "local", error()

            with By("check remote backups"):
                remote_backups = 0
                data = [[entry for entry in line.split(' ') if entry]
                        for line in backup.cmd("clickhouse-backup list remote").output.replace('\t', ' ').split('\n') if name_prefix in line]
                for line in data:
                    debug(line)
                    remote_backups += 1
                    assert line[4] == "remote", error()

            with By("I expect total number to match"):
                assert total_backups == local_backups + remote_backups, error()

    finally:
        with Finally("I undo changes"):
            backup.cmd(f"clickhouse-backup delete local {name_prefix}_1")
            backup.cmd(f"clickhouse-backup delete remote {name_prefix}_1")
            backup.cmd(f"clickhouse-backup delete local {name_prefix}_0")

        with And("I set remote_storage to none"):
            config_modifier(fields={"general": {"remote_storage": "none"}})


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_MatchExpression("1.0")
)
def match_expression(self):
    """Test that CLI supports pattern matching in --table flag.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup

    expect = {'*': [f"cli_re_{i}" for i in range(20)],
              '?': [f"cli_re_{i}" for i in range(10)],
              }

    with Given("I create a backup for many tables"):
        with By("I create a number of tables"):
            for i in range(20):
                create_and_populate_table(node=clickhouse, table_name=f"cli_re_{i}")

        with And("I create backup for created tables"):
            backup.cmd(f"clickhouse-backup create bp_cli_re")

        with And("I drop all tables"):
            for i in range(20):
                drop_table(node=clickhouse, table_name=f"cli_re_{i}")

    for sign, match in expect.items():
        with Step(f"I check ({sign})"):
            try:
                with When("I restore using re"):
                    backup.cmd(f"clickhouse-backup restore --tables default.cli_re_{sign} bp_cli_re")

                with Then("I check only matching tables restored"):
                    restored_tables = clickhouse.query("SHOW TABLES").output
                    for m in match:
                        assert m in restored_tables, error()
            finally:
                with Finally("I drop all tables"):
                    for i in range(20):
                        drop_table(node=clickhouse, table_name=f"cli_re_{i}")


@TestFeature
def cli(self):
    """Check the CLI works as intended.
    """
    clickhouse = self.context.nodes[0]

    create_and_populate_table(node=clickhouse, table_name="cli_table")

    try:
        for scenario in loads(current_module(), Scenario, Suite):
            Scenario(run=scenario)

    finally:
        with Finally("remove created table"):
            drop_table(node=clickhouse, table_name="cli_table")
