import os
import posixpath
from pathlib import Path

from testflows.asserts import error

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.steps import *


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Configs_Backup("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_Configs_Restore("1.0")
)
def configs_backup_restore(self):
    """Test that configs can be backed up and restored.
    """
    ch_node = self.context.nodes[0]
    backup = self.context.backup
    table_name = "config_bp"
    files_contents = {}

    try:
        with Given("I create and populate table"):
            create_and_populate_table(node=ch_node, table_name=table_name)

        with And("I make sure some configuration files/directories exist"):
            for dirname in ("clickhouse/config.d", "clickhouse/users.d", "clickhouse1/config.d"):
                dirname = os.path.abspath(os.path.join((posixpath.dirname(__file__)), '../configs', dirname))
                if not Path(dirname).is_dir():
                    fail(f"{dirname} not found, why you change file structure?")

            for filename in (
                "clickhouse1/config.d/macros.xml", "clickhouse1/config.d/rabbitmq.xml", "clickhouse/users.d/default.xml"
            ):
                filename = os.path.abspath(os.path.join((posixpath.dirname(__file__)), '../configs', filename))
                if not Path(filename).is_file():
                    fail(f"{filename} not found, why you change file structure?")

        with When(f"I create backup"):
            r = backup.cmd(f"clickhouse-backup create --env=LOG_LEVEL=debug --configs {table_name}")
            with Then("I expect clickhouse-backup to attempt create configs"):
                assert "done createBackupConfigs" in r.output, error()

        with When("I remove existing configuration to restore it later"):
            for local_config_dir in ("configs/clickhouse", "configs/clickhouse1"):
                local_config_dir = os.path.abspath(os.path.join((posixpath.dirname(__file__)), '..', local_config_dir))
                for root, dirs, files in os.walk(local_config_dir, topdown=False):
                    for file in files:
                        filename = f"{root[len(local_config_dir)+1:]}/{file}"
                        if "storage_configuration.sh" not in filename:
                            files_contents[filename] = ch_node.cmd(f"cat /etc/clickhouse-server/{filename}").output
                            ch_node.cmd(f"truncate -s 0 /etc/clickhouse-server/{filename}")

        with Then("I restore from the backup and restart"):
            r = backup.cmd(f"clickhouse-backup restore --env=LOG_LEVEL=debug --configs {table_name}", exitcode=None)

            with Then("I expect clickhouse-backup to attempt restart clickhouse-server"):
                assert "restart clickhouse-server" in r.output, error()

            with And("I restart clickhouse, cause clickhouse-backup doesn't have access to systemd"):
                ch_node.restart(safe=False, wait_healthy=True)

        with And("I check files restored correctly"):
            for local_config_dir in ("configs/clickhouse", "configs/clickhouse1"):
                local_config_dir = os.path.abspath(os.path.join((posixpath.dirname(__file__)), '..', local_config_dir))
                for root, dirs, files in os.walk(local_config_dir, topdown=False):
                    for f in files:
                        filename = f"{root[len(local_config_dir) + 1:]}/{f}"
                        if "storage_configuration.sh" not in filename:
                            output = ch_node.cmd(f"cat /etc/clickhouse-server/{filename}").output
                            assert files_contents[filename] == output, error()

    finally:
        with Finally("removing created backup"):
            backup.cmd(f"clickhouse-backup delete local {table_name}")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_RBAC_Backup("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_RBAC_Restore("1.0")
)
def rbac_backup_restore(self):
    """Test that RBAC objects can be backed up and restored.
    """
    ch_node = self.context.nodes[0]
    backup = self.context.backup
    table_name = "rbac_bp"

    try:
        with Given("I create and populate table"):
            create_and_populate_table(node=ch_node, table_name=table_name)

        with And("I create RBAC objects to be backed up"):
            for o, n in (("USER", "test_user"), ("ROLE", "test_role"), ("ROW POLICY", "test_policy ON rbac_bp"),
                         ("SETTINGS PROFILE", "test_profile"), ("QUOTA", "test_quota")):
                ch_node.query(f"CREATE {o} {n}")

        with When(f"I create backup"):
            backup.cmd(f"clickhouse-backup create --rbac {table_name}")

        with And("I delete objects to be backed up"):
            for o, n in (("USER", "test_user"), ("ROLE", "test_role"), ("ROW POLICY", "test_policy ON rbac_bp"),
                         ("SETTINGS PROFILE", "test_profile"), ("QUOTA", "test_quota")):
                ch_node.query(f"DROP {o} IF EXISTS {n}")
            ch_node.restart()

        with Then("I restore from the backup and restart"):
            backup.cmd(f"clickhouse-backup restore --rbac {table_name}", exitcode=None)
            ch_node.restart()

        with And("I check objects restored correctly"):
            with By("expect user restored"):
                output = ch_node.query("SHOW USERS").output
                assert "test_user" in output, error()

            with And("expect role restored"):
                output = ch_node.query("SHOW ROLES").output
                assert "test_role" in output, error()

            with And("expect row policy restored"):
                output = ch_node.query("SHOW ROW POLICIES").output
                assert "test_policy" in output, error()

            with And("expect settings profile restored"):
                output = ch_node.query("SHOW SETTINGS PROFILES").output
                assert "test_profile" in output, error()

            with And("expect quota restored"):
                output = ch_node.query("SHOW QUOTAS").output
                assert "test_quota" in output, error()

            for o, n in (("ROLE", "test_role"), ("ROW POLICY", "test_policy ON rbac_bp"),
                         ("SETTINGS PROFILE", "test_profile"), ("QUOTA", "test_quota"), ("USER", "test_user")):
                ch_node.query(f"DROP {o} IF EXISTS {n}")

    finally:
        with Finally("removing created backup"):
            backup.cmd(f"clickhouse-backup delete local {table_name}")


@TestFeature
def config_rbac(self):
    """Check that configs and RBAC backups can be created and restored.
    """
    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario)
