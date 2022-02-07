import json

from testflows.core import *
from testflows.asserts import *

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.steps import *
from clickhouse_backup.tests.common import *


@TestStep
def validate_JsonEachRow(self, r):
    try:
        return [json.loads(s) for s in r.text.split('\n')[:-1]]
    except ValueError as e:
        return False


@TestStep
def post_api_actions(self, url, command: str, code=None):
    data = {'command': command}
    r = api_request(endpoint=f"{url}/backup/actions", type="post", payload=data)

    if code:
        with Then(f"check response code is {code}"):
            assert r.status_code == code, error()


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetStatus("1.0")
)
def get_status(self):
    """Test that GET /backup/status works properly.
    Return format is the following: {'command': 'create', 'status': 'in progress', 'start': '2021-10-26 12:11:34'}
    """
    url = self.context.url
    name_prefix = "api_status"

    try:
        with Given("make 2 create backup requests"):
            with By("create first backup and wait for it to finish"):
                api_request(endpoint=f"{url}/backup/create?name={name_prefix}_0", type="post")
                wait_request_finalized(url)
            with By("create a second one"):
                api_request(endpoint=f"{url}/backup/create?name={name_prefix}_1", type="post")

        with When("check the return concerns only the last query"):
            r = api_request(endpoint=f"{url}/backup/status")
            assert validate_JsonEachRow(r=r), error()
            assert r.json()["command"] == f"create {name_prefix}_1", error()
            wait_request_finalized(url)

    finally:
        with Finally("remove created backups"):
            for i in range(2):
                api_request(endpoint=f"{url}/backup/delete/local/{name_prefix}_{i}", type="post")
                wait_request_finalized(url)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_AsyncPostQuery("1.0")
)
def async_post_query(self):
    """Test that POST queries are async. If previous query is not finished yet, the new one shall return 423 without being executed.
    """
    url = self.context.url
    name_prefix = "api_async"

    try:
        with When("I send first create backup request"):
            r1 = api_request(endpoint=f"{url}/backup/create?name={name_prefix}_0", type="post")

        with And("I send second create backup request"):
            r2 = api_request(endpoint=f"{url}/backup/create?name={name_prefix}_1", type="post")
            validate_JsonEachRow(r=r1)
            validate_JsonEachRow(r=r2)
            with Then("I expect code 423"):
                assert r2.status_code == 423, error()

    finally:
        with Finally("remove created backups"):
            for i in range(2):
                api_request(endpoint=f"{url}/backup/delete/local/{name_prefix}_{i}", type="post")
                wait_request_finalized(url)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetTables("1.0")
)
def get_tables(self):
    """Test that GET /tables works in different cases.
    """
    url = self.context.url
    backup = self.context.backup
    api_base_table_name = self.context.api_base_table_name

    with Check("I check GET tables"):
        with When("I list tables (via CLI)"):
            r_cli = backup.cmd(f"clickhouse-backup tables").output
            with Then("I expect table to be listed"):
                assert api_base_table_name in r_cli, error()

        with And("I GET tables (via API)"):
            r_api = api_request(endpoint=f"{url}/backup/tables", type="get").text
            debug(r_api)
            with Then("I expect table to be listed"):
                assert api_base_table_name in r_api, error()

    with Check("I check GET tables all"):
        with When("I list tables (via CLI)"):
            r_cli = backup.cmd(f"clickhouse-backup tables --all").output
            with Then("I expect table to be listed"):
                assert api_base_table_name in r_cli, error()

        with And("I GET tables (via API)"):
            r_api = api_request(endpoint=f"{url}/backup/tables/all", type="get")
            with Then("I expect table to be listed"):
                assert api_base_table_name in r_api.text, error()

            with Then("I check all tables are listed"):
                tables_data = validate_JsonEachRow(r=r_api)
                for t in tables_data:
                    assert f"{t['Database']}.{t['Name']}" in r_cli, error()


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostCreate("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostRestore("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostDelete_Local("1.0")
)
def create_restore_delete_local(self):
    """Test that POST /backup/create and POST /backup/delete/local works properly.
    """
    url = self.context.url
    backup_name = "api_crd_l"
    backup = self.context.backup
    clickhouse = self.context.nodes[0]
    api_base_table_name = self.context.api_base_table_name

    with Step("I check POST /create command works"):
        with When("I create local backup"):
            api_request(endpoint=f"{url}/backup/create?name={backup_name}", type="post")
            wait_request_finalized(url)

            with Then("I check backup appears in filesystem"):
                assert backup_name in backup.cmd("ls /var/lib/clickhouse/backup").output, error()

    with Step("I check POST /restore command works"):
        with When(f"I delete table {api_base_table_name}"):
            drop_table(node=clickhouse, table_name=api_base_table_name)

        with And("I perform a POST restore"):
            api_request(endpoint=f"{url}/backup/restore/{backup_name}", type="post")
            wait_request_finalized(url)

        with Then("I expect table is restored"):
            assert api_base_table_name in clickhouse.query("SHOW TABLES").output, error()

    with Step("I check API /delete/local command works"):
        with When("I remove backup"):
            api_request(endpoint=f"{url}/backup/delete/local/{backup_name}", type="post")
            wait_request_finalized(url)

            with Then("I check backup disappears from filesystem"):
                assert backup_name not in backup.cmd("ls /var/lib/clickhouse/backup").output, error()


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostCreate_Remote("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostRestore_Remote("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostDelete_Remote("1.0")
)
def create_restore_delete_remote(self):
    """Test that remote backup can be created via API and POST /backup/delete/remote works properly.
    """
    url = self.context.url
    backup_name = "api_crd_r"
    backup = self.context.backup
    clickhouse = self.context.nodes[0]
    api_base_table_name = self.context.api_base_table_name

    with Given("I configure ch-backup to use remote storage"):
        config_modifier(fields= {"general": {"remote_storage": "ftp"}})

    try:
        with Step("I check API /create command works"):
            with When("I create remote backup"):
                post_api_actions(url=url, command=f"create_remote {backup_name}")
                wait_request_finalized(url)

                with Then("I check backup appears in storage"):
                    assert backup_name in backup.cmd("clickhouse-backup list remote").output, error()

        with Step("I check POST /restore command works"):
            with When(f"I delete table {api_base_table_name} and local backup"):
                drop_table(node=clickhouse, table_name=api_base_table_name)
                backup.cmd(f"clickhouse-backup delete local {backup_name}")

            with And("I perform a POST restore"):
                post_api_actions(url=url, command=f"restore_remote {backup_name}")
                wait_request_finalized(url)

            with Then("I expect table is restored"):
                assert api_base_table_name in clickhouse.query("SHOW TABLES").output, error()

        with Step("I check API /delete/remote command works"):
            with When("I remove backup"):
                api_request(endpoint=f"{url}/backup/delete/remote/{backup_name}", type="post")
                wait_request_finalized(url)

                with Then("I check backup is not in storage"):
                    assert backup_name not in backup.cmd("clickhouse-backup list remote").output, error()

    finally:
        with Finally("I set remote_storage to none and remove local backup"):
            config_modifier(fields={"general": {"remote_storage": "none"}})
            backup.cmd(f"clickhouse-backup delete local {backup_name}")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetList("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetList_Local("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetList_Remote("1.0")
)
def get_list_local_remote(self):
    """Test that GET /backup/list/local and GET /backup/list/remote only show corresponding existing backups.
    """
    url = self.context.url
    name_prefix = "api_list"

    try:
        with Given("create a local backup"):
            api_request(endpoint=f"{url}/backup/create?name={name_prefix}_0", type="post")
            wait_request_finalized(url)

        with And("create a remote backup"):
            with By("set remote storage to ftp"):
                config_modifier(fields={"general": {"remote_storage": "ftp"}})
            with And("create a backup"):
                post_api_actions(url=url, command=f"create_remote {name_prefix}_1")
                wait_request_finalized(url)

        with When("check the list query return only appropriate results"):
            with By("check all backups"):
                total_backups = 0
                data = validate_JsonEachRow(r=api_request(endpoint=f"{url}/backup/list", type="get"))
                for entry in data:
                    total_backups += 1
                    assert entry["location"] in ("local", "remote") , error()

            with By("check local backups"):
                local_backups = 0
                data = validate_JsonEachRow(r=api_request(endpoint=f"{url}/backup/list/local", type="get"))
                for entry in data:
                    local_backups += 1
                    assert entry["location"] == "local", error()

            with By("check remote backups"):
                remote_backups = 0
                data = [json.loads(s) for s in api_request(endpoint=f"{url}/backup/list/remote", type="get").text.split('\n')[:-1]]
                for entry in data:
                    remote_backups += 1
                    assert entry["location"] == "remote", error()

            with By("I expect total number to match"):
                assert total_backups == local_backups + remote_backups, error()

    finally:
        with Finally("undo changes"):
            with By("remove remote backup and set remote_storage none"):
                api_request(endpoint=f"{url}/backup/delete/remote/{name_prefix}_1", type="post")
                wait_request_finalized(url)
                config_modifier(fields={"general": {"remote_storage": "none"}})

            with And("remove local backup"):
                api_request(endpoint=f"{url}/backup/delete/local/{name_prefix}_0", type="post")
                wait_request_finalized(url)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostUpload("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostDownload("1.0")
)
def post_upload_download(self):
    """Test that API calls POST /upload and /download work properly.
    """
    url = self.context.url
    backup_name = "api_pud"
    backup = self.context.backup

    with Given("I configure ch-backup to use remote storage"):
        config_modifier(fields={"general": {"remote_storage": "ftp"}})

    with And("I create a local backup"):
        backup.cmd(f"clickhouse-backup create {backup_name}")

    try:
        with When("I perform POST /upload"):
            api_request(endpoint=f"{url}/backup/upload/{backup_name}", type="post")
            wait_request_finalized(url)

            with Then("I check backup appears in storage"):
                assert backup_name in backup.cmd("clickhouse-backup list remote").output, error()

        with And(f"I delete local copy"):
            backup.cmd(f"clickhouse-backup delete local {backup_name}")

        with And("I perform POST /download"):
            api_request(endpoint=f"{url}/backup/download/{backup_name}", type="post")
            wait_request_finalized(url)

            with Then("I check backup exists locally"):
                assert backup_name in backup.cmd("clickhouse-backup list local").output, error()

    finally:
        with Finally("I set remote_storage to none"):
            config_modifier(fields={"general": {"remote_storage": "none"}})

        with And("I remove backups"):
            backup.cmd(f"clickhouse-backup delete local {backup_name}")
            backup.cmd(f"clickhouse-backup delete remote {backup_name}")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostActions("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetActions("1.0")
)
def post_get_actions(self):
    """Test that API calls POST/GET /actions work properly.
    """
    url = self.context.url
    backup_name = "api_gpa"
    backup = self.context.backup

    with Given("I configure ch-backup to use remote storage"):
        config_modifier(fields= {"general": {"remote_storage": "ftp"}})

    try:
        with When("I start remote backup creations via POST actions"):
            r1 = api_request(endpoint=f"{url}/backup/actions", type="post", payload={'command': f"create_remote {backup_name}"})

        with And("I perform GET /actions"):
            r2 = api_request(endpoint=f"{url}/backup/actions", type="get")

        with Then("I check both responses have correct data"):
            assert r1.json()["operation"] == f"create_remote {backup_name}" and r1.json()["status"] == "acknowledged", error()
            last_op = validate_JsonEachRow(r=r2)[-1]
            assert last_op["command"] == f"create_remote {backup_name}" and last_op["status"] == "in progress", error()

        with And("I wait for request to finish"):
            wait_request_finalized(url)

    finally:
        with Finally("I set remote_storage to none"):
            config_modifier(fields={"general": {"remote_storage": "none"}})

        with And("I remove backups"):
            backup.cmd(f"clickhouse-backup delete local {backup_name}")
            backup.cmd(f"clickhouse-backup delete remote {backup_name}")



@TestFeature
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_CLI_Server("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_OutputFormat_Default("1.0"),
)
def api(self):
    """Test API server
    """
    self.context.url = "http://localhost:7171"
    clickhouse = self.context.nodes[0]
    self.context.api_base_table_name = "api_base"

    create_and_populate_table(node=clickhouse, table_name=self.context.api_base_table_name)

    try:
        for scenario in loads(current_module(), Scenario, Suite):
            Scenario(run=scenario)

    finally:
        with Finally("remove created table"):
            drop_table(node=clickhouse, table_name=self.context.api_base_table_name)
