import os
import yaml

from testflows.core import *
from testflows.asserts import *

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.steps import *


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_IncrementalBackups("1.0")
)
def incremental_remote_storage(self):
    """Test creating and uploading incremental backups.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    table_name = "incr_remote"

    columns = self.context.columns
    columns["ToDrop"] = "Int32"
    columns["ToUpdate"] = "Int32"
    columns["ToAdd"] = "Int32"
    columns["ToDelete"] = "Int32"

    try:
        with Given("I create and populate table"):
            create_and_populate_table(node=clickhouse, table_name=f"{table_name}", size=100)

        with And("I set remote_storage to ftp"):
            config_modifier(fields={"general": {"remote_storage": "ftp"}})

        with When("I create a base backup"):
            backup.cmd(f"clickhouse-backup create_remote {table_name}_1")

        with And("I modify the table and save its contents"):
            populate_table(node=clickhouse, table_name=f"{table_name}", columns=self.context.columns, size=100)

            with By("I perform `ALTER` requests"):
                clickhouse.query(f"ALTER TABLE {table_name} DELETE WHERE ToDelete=0 SETTINGS mutations_sync=2")
                clickhouse.query(f"ALTER TABLE {table_name} UPDATE ToUpdate=OrderBy WHERE ToUpdate=0 SETTINGS mutations_sync=2")
                clickhouse.query(f"ALTER TABLE {table_name} ADD COLUMN ToClear Int32")
                clickhouse.query(f"ALTER TABLE {table_name} CLEAR COLUMN ToClear")
                clickhouse.query(f"ALTER TABLE {table_name} DROP COLUMN ToDrop")
                time.sleep(10)

            with And("I save table contents"):
                contents_before = clickhouse.query(f"SELECT * FROM {table_name}").output.split('\n')

        with And("I do create_remote --diff-from"):
            backup.cmd(f"clickhouse-backup create_remote --diff-from={table_name}_1 {table_name}_2")
            time.sleep(10)

        with Then("I drop created table and local backups"):
            drop_table(node=clickhouse, table_name=table_name)
            backup.cmd(f"clickhouse-backup delete local {table_name}_2")
            backup.cmd(f"clickhouse-backup delete local {table_name}_1")

        with And("I restore table"):
            backup.cmd(f"clickhouse-backup restore_remote {table_name}_2")
            time.sleep(10)

        with And("I expect table restored"):
            with By("I check table exists"):
                r = clickhouse.query("SHOW TABLES").output
                assert table_name in r, error()

            with And("I check table contents are restored"):
                contents_after = clickhouse.query(f"SELECT * FROM {table_name}").output.split('\n')

                for line in contents_after:
                    assert line in contents_before, error()
                for line in contents_before:
                    assert line in contents_after, error()

    finally:
        with Finally("I remove created backups"):
            backup.cmd(f"clickhouse-backup delete local {table_name}_1", exitcode=None)
            backup.cmd(f"clickhouse-backup delete remote {table_name}_1", exitcode=None)
            backup.cmd(f"clickhouse-backup delete local {table_name}_2", exitcode=None)
            backup.cmd(f"clickhouse-backup delete remote {table_name}_2", exitcode=None)

        with And("I set remote_storage to none"):
            config_modifier(fields={"general": {"remote_storage": "none"}})


@TestOutline
def test_storage_outline(self, storage_type, fields_to_modify=None):
    """Test that an existing backup can be uploaded to a remote server of a given type.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    name_prefix = f"{storage_type}_backup_{time.time_ns()}"

    if not fields_to_modify:
        fields_to_modify = {"general": {"remote_storage": storage_type}}

    try:
        with Given(f"modifying config file to use {storage_type}"):
            config_modifier(fields=fields_to_modify)

        with And("creating MergeTree table to be backed up"):
            create_and_populate_table(node=clickhouse, table_name=f"{name_prefix}")
            with And(f"save data from the table"):
                table_data = clickhouse.query(f"SELECT * FROM {name_prefix}").output

        with When("creating a backup for the table"):
            backup.cmd(f"clickhouse-backup create_remote --tables=default.{name_prefix} {name_prefix}")

        with And("dropping the table"):
            drop_table(node=clickhouse, table_name=f"{name_prefix}")

        with And("remove backup locally"):
            backup.cmd(f"clickhouse-backup delete local {name_prefix}")

        with And("restore table"):
            backup.cmd(f"clickhouse-backup restore_remote {name_prefix}")

        with Then("expect table restored"):
            r = clickhouse.query("SHOW TABLES").output
            with By(f"expect {name_prefix} exists"):
                assert f"{name_prefix}" in r, error()
            with And(f"expect {name_prefix} data restored"):
                assert clickhouse.query(f"SELECT * FROM {name_prefix}").output == table_data, error()

    finally:
        with Finally(f"delete backup for {name_prefix}"):
            backup.cmd(f"clickhouse-backup delete local {name_prefix}")
            backup.cmd(f"clickhouse-backup delete remote {name_prefix}")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_FTP("1.0")
)
def test_ftp(self):
    """Test that an existing backup can be uploaded to a FTP server.
    """
    test_storage_outline(storage_type="ftp")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_SFTP("1.0")
)
def sftp(self):
    """Test that an existing backup can be uploaded to a SFTP server.
    """
    test_storage_outline(storage_type="sftp")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_S3_Minio("1.0")
)
def s3_minio(self):
    """Test that an existing backup can be uploaded to a S3 server.
    """
    test_storage_outline(storage_type="s3", fields_to_modify={"general": {"remote_storage": "s3"},
                                                              "s3": {"access_key": "access-key",
                                                                     "secret_key": "it-is-my-super-secret-key",
                                                                     "endpoint": "http://minio:9000", "disable_ssl": True,
                                                                     "region": "us-west-2", "bucket": "clickhouse"}})


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_S3_AWS("1.0")
)
def s3_aws(self):
    """Test that an existing backup can be uploaded to a S3 server.
    """
    if not os.environ.get('QA_AWS_ACCESS_KEY'):
        skip("QA_AWS_ACCESS_KEY environment not found, test skipped")
    access_key = os.environ.get('QA_AWS_ACCESS_KEY')
    secret_key = os.environ.get('QA_AWS_SECRET_KEY')
    endpoint = os.environ.get('QA_AWS_ENDPOINT')
    region = os.environ.get('QA_AWS_REGION')
    bucket = os.environ.get('QA_AWS_BUCKET')

    test_storage_outline(storage_type="aws", fields_to_modify={"general": {"remote_storage": "s3"},
                                                               "s3": {"access_key": access_key, "secret_key": secret_key,
                                                                      "endpoint": endpoint, "disable_ssl": False,
                                                                      "region": region, "bucket": bucket}})


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_GCS("1.0")
)
def gcs(self):
    """Test that an existing backup can be uploaded to GCS bucket.
    """
    if not os.environ.get('QA_GCS_CRED_JSON'):
        skip("QA_GCS_CRED_JSON environment not found, test skipped")

    test_storage_outline(storage_type="gcs")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_CompressionFormats("1.0")
)
def compression_formats(self):
    """Test that backup can be stored on a remote server in different formats.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    name_prefix = "bp_compr"

    with Given("I save original config.yml contents"):
        with open(self.context.backup_config_file) as f:
            initial_config_state = yaml.safe_load(f)

    for compr_mode in ("tar", "bzip2", "gzip", "sz", "xz", "br", "zstd"):
        try:
            with Given(f"I modify config file to use ftp and {compr_mode}"):
                config_modifier(fields={"general": {"remote_storage": "ftp"}, "ftp": {"compression_format": compr_mode}})

            with And("creating MergeTree table to be backed up"):
                create_and_populate_table(node=clickhouse, table_name=f"{name_prefix}_{compr_mode}")
                with And(f"save data from the table"):
                    table_data = clickhouse.query(f"SELECT * FROM {name_prefix}_{compr_mode}").output

            with When("creating a backup for the table"):
                backup.cmd(f"clickhouse-backup create_remote --tables=default.{name_prefix}_{compr_mode} {name_prefix}_{compr_mode}")

            with And("dropping the table"):
                drop_table(node=clickhouse, table_name=f"{name_prefix}_{compr_mode}")

            with And("remove backup locally"):
                backup.cmd(f"clickhouse-backup delete local {name_prefix}_{compr_mode}")

            with And("restore table"):
                backup.cmd(f"clickhouse-backup restore_remote {name_prefix}_{compr_mode}")

            with Then("expect table restored"):
                r = clickhouse.query("SHOW TABLES").output
                with By(f"expect {name_prefix}_{compr_mode} exists"):
                    assert f"{name_prefix}_{compr_mode}" in r, error()
                with And(f"expect {name_prefix}_{compr_mode} data restored"):
                    assert clickhouse.query(f"SELECT * FROM {name_prefix}_{compr_mode}").output == table_data, error()

        finally:
            with Finally(f"delete backup for {name_prefix}_{compr_mode}"):
                backup.cmd(f"clickhouse-backup delete local {name_prefix}_{compr_mode}")
                backup.cmd(f"clickhouse-backup delete remote {name_prefix}_{compr_mode}")

            with And("I delete table"):
                drop_table(node=clickhouse, table_name=f"{name_prefix}_{compr_mode}")

            with And("I restore original config.yml contents"):
                with open(self.context.backup_config_file, "w") as f:
                    yaml.dump(initial_config_state, f)


@TestFeature
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_CreateRemote("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_RestoreRemote("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_CompressionLevel("1.0"),
)
def cloud_storage(self):
    """Check integrations with cloud storage services and (S)FTP
    """
    initial_config_state = None

    try:
        with Given("I save original config.yml contents"):
            with open(self.context.backup_config_file) as f:
                initial_config_state = yaml.safe_load(f)

        for scenario in loads(current_module(), Scenario, Suite):
            Scenario(run=scenario)

    finally:
        if initial_config_state:
            with Finally("I restore original config.yml contents"):
                with open(self.context.backup_config_file, "w") as f:
                    yaml.dump(initial_config_state, f)
