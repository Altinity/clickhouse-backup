import json
import os
import time

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.steps import *

CLUSTER = "sharded_cluster"
TABLE = "embedded_oc"
MINIO_URL = "http://minio:9000"
MINIO_BUCKET = "clickhouse"
MINIO_ACCESS_KEY = "access_key"
MINIO_SECRET_KEY = "it_is_my_super_secret_key"
WORKER_FLAG = "--embedded-on-cluster-worker"


@TestStep(When)
def post_action(self, url, command):
    """POST a command to /backup/actions and expect it to be acknowledged."""
    r = api_request(endpoint=f"{url}/backup/actions", request_type="post", payload={"command": command})
    with Then("the command is acknowledged"):
        assert r.status_code in (200, 201), error(r.text)
        # async commands (create_remote, restore_remote) are acknowledged, `delete` is executed synchronously
        assert r.json()["status"] in ("acknowledged", "success"), error(r.text)


@TestStep(Then)
def wait_action(self, url, command, timeout=600):
    """Poll /backup/actions until the last row for ``command`` leaves ``in progress`` and return it."""
    deadline = time.time() + timeout
    row = None
    while time.time() < deadline:
        rows = [json.loads(s) for s in api_request(endpoint=f"{url}/backup/actions").text.splitlines() if s]
        matching = [r for r in rows if r["command"] == command]
        if matching:
            row = matching[-1]
            if row["status"] != "in progress":
                return row
        time.sleep(2)
    fail(f"`{command}` did not finish in {timeout}s, last row: {row}")


@TestStep(Given)
def wait_integration_tables(self, node, timeout=120):
    """Wait until clickhouse-backup server created system.backup_actions on ``node``."""
    for _ in range(timeout):
        if node.query("EXISTS TABLE system.backup_actions", no_checks=True).output.strip() == "1":
            return
        time.sleep(1)
    fail(f"system.backup_actions not created on {node.name} in {timeout}s")


@TestStep(When)
def remote_objects(self, node, backup_name):
    """List object keys of ``backup_name`` in the minio bucket under both s3.path and s3.object_disk_path.

    Returns ``(keys, path, object_disk_path)``: ClickHouse writes the native backup (``.backup``,
    ``shards/N/replicas/M/{metadata,data}``) under ``object_disk_path``, clickhouse-backup uploads its own
    ``metadata.json`` and per-node ``shards/N/replicas/M/metadata/<db>/<table>.json`` under ``path``.
    """
    with open(self.context.backup_config_file) as f:
        s3 = yaml.safe_load(f)["s3"]
    prefixes = ",".join(sorted({s3["path"], s3["object_disk_path"]}))
    r = node.query(
        f"SELECT DISTINCT _path FROM s3('{MINIO_URL}/{MINIO_BUCKET}/{{{prefixes}}}/{backup_name}/**', "
        # LineAsString instead of One: format One exists only since 23.8, the backup is tiny
        f"'{MINIO_ACCESS_KEY}', '{MINIO_SECRET_KEY}', 'LineAsString') ORDER BY _path FORMAT TSVRaw"
    ).output
    return [line for line in r.splitlines() if line], s3["path"], s3["object_disk_path"]


def row_count(node, table=TABLE):
    return int(node.query(f"SELECT count() FROM default.{table}").output.strip())


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_EmbeddedBackup_OnCluster("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_EmbeddedBackup_OnCluster_Worker("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_EmbeddedBackup_OnCluster_MetadataLayout("1.0"),
)
def create_remote_on_cluster(self):
    """create_remote issued on one node backs up every shard of the cluster to the shared S3 bucket."""
    ch1, ch2 = self.context.nodes
    url = self.context.url
    backup_name = self.context.backup_name

    with Given(f"I create a MergeTree table ON CLUSTER {CLUSTER} and insert different rows on each shard"):
        ch1.query(f"CREATE TABLE default.{TABLE} ON CLUSTER {CLUSTER} (id UInt64, v String) "
                  f"ENGINE=MergeTree ORDER BY id")
        ch1.query(f"INSERT INTO default.{TABLE} SELECT number, toString(number) FROM numbers(10)")
        ch2.query(f"INSERT INTO default.{TABLE} SELECT number, toString(number) FROM numbers(100, 25)")
        assert {ch1.name: row_count(ch1), ch2.name: row_count(ch2)} == self.context.expected_rows, error()

    with When("I issue create_remote on the initiator"):
        post_action(url=url, command=f"create_remote {backup_name}")

    with Then("the operation finishes successfully"):
        row = wait_action(url=url, command=f"create_remote {backup_name}")
        assert row["status"] == "success", error(row)

    with And("the backup is listed as remote via system.backup_list on the initiator node"):
        r = ch1.query(f"SELECT location FROM system.backup_list WHERE name='{backup_name}' FORMAT TSVRaw").output
        assert "remote" in r, error(r)

    with And("the worker on clickhouse2 reports a finished create_remote worker command"):
        r = ch2.query(f"SELECT status, error FROM system.backup_actions "
                      f"WHERE command LIKE 'create_remote {WORKER_FLAG} %{backup_name}%' ORDER BY start DESC LIMIT 1 "
                      f"FORMAT TSVRaw").output
        assert r.startswith("success"), error(r)

    with And("the backup metadata is tagged as embedded for the cluster"):
        metadata = json.loads(self.context.backup.cmd(f"cat /var/lib/clickhouse/backup/{backup_name}/metadata.json").output)
        assert f"embedded,cluster={CLUSTER}" in metadata.get("tags", ""), error(metadata.get("tags"))

    with And("the bucket contains the ON CLUSTER layout for both shards"):
        keys, path, disk_path = remote_objects(node=ch1, backup_name=backup_name)
        joined = "\n".join(keys)
        for expected in (
            f"{disk_path}/{backup_name}/.backup",
            f"{path}/{backup_name}/metadata.json",
            f"{disk_path}/{backup_name}/shards/1/replicas/1/data/default/{TABLE}/",
            f"{disk_path}/{backup_name}/shards/2/replicas/1/data/default/{TABLE}/",
            # the CREATE query is identical on both shards, ClickHouse `deduplicate_files=1` stores one physical
            # copy under the first host prefix and references it from `.backup`, so only shard 1 has the `.sql`
            f"{disk_path}/{backup_name}/shards/1/replicas/1/metadata/default/{TABLE}.sql",
            f"{path}/{backup_name}/shards/1/replicas/1/metadata/default/{TABLE}.json",
            f"{path}/{backup_name}/shards/2/replicas/1/metadata/default/{TABLE}.json",
        ):
            with By(f"expect an object matching {expected}"):
                assert expected in joined, error(joined)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_EmbeddedBackup_OnCluster("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_EmbeddedBackup_OnCluster_RestoreOrder("1.0"),
)
def restore_remote_on_cluster(self):
    """restore_remote issued on one node restores every shard of the cluster."""
    ch1, ch2 = self.context.nodes
    url = self.context.url
    backup_name = self.context.backup_name

    with Given("I drop the table on every shard"):
        ch1.query(f"DROP TABLE IF EXISTS default.{TABLE} ON CLUSTER {CLUSTER} SYNC")
        for node in (ch1, ch2):
            assert node.query(f"EXISTS TABLE default.{TABLE}").output.strip() == "0", error()

    with When("I issue restore_remote on the initiator"):
        post_action(url=url, command=f"restore_remote {backup_name}")

    with Then("the operation finishes successfully"):
        row = wait_action(url=url, command=f"restore_remote {backup_name}")
        assert row["status"] == "success", error(row)

    with And("the worker on clickhouse2 reports a finished restore_remote worker command"):
        r = ch2.query(f"SELECT status, error FROM system.backup_actions "
                      f"WHERE command LIKE 'restore_remote {WORKER_FLAG} %{backup_name}%' ORDER BY start DESC LIMIT 1 "
                      f"FORMAT TSVRaw").output
        assert r.startswith("success"), error(r)

    with And("row counts on every shard match the originals"):
        for node in (ch1, ch2):
            with By(f"check {node.name}"):
                assert row_count(node) == self.context.expected_rows[node.name], error()


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_EmbeddedBackup_OnCluster_DeleteLocal("1.0"),
)
def delete_local_on_cluster(self):
    """delete local issued on one node removes the local backup on every node of the cluster."""
    url = self.context.url
    backup_name = self.context.backup_name
    backup_nodes = (self.context.backup, self.context.backup2)
    backup_dir = f"/var/lib/clickhouse/backup/{backup_name}"

    with Given("the local backup directory exists on every node"):
        for node in backup_nodes:
            with By(f"check {node.name}"):
                node.cmd(f"test -d {backup_dir}", exitcode=0)

    with When("I issue delete local on the initiator"):
        post_action(url=url, command=f"delete local {backup_name}")

    with Then("the operation finishes successfully"):
        row = wait_action(url=url, command=f"delete local {backup_name}")
        assert row["status"] == "success", error(row)

    with And("the local backup directory is gone on every node"):
        for node in backup_nodes:
            with By(f"check {node.name}"):
                node.cmd(f"test -d {backup_dir}", exitcode=1)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_EmbeddedBackup_OnCluster_Worker("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_EmbeddedBackup_OnCluster_Requirements("1.0"),
)
def worker_unavailable(self):
    """create_remote fails when the clickhouse-backup server of another node is not reachable."""
    url = self.context.url
    backup2 = self.context.backup2
    backup_name = f"{self.context.backup_name}_noworker"

    try:
        with Given("I stop the clickhouse-backup server of clickhouse2"):
            backup2.stop_server()

        with When("I issue create_remote on the initiator"):
            post_action(url=url, command=f"create_remote {backup_name}")

        with Then("the operation fails and the error points at the unreachable worker"):
            row = wait_action(url=url, command=f"create_remote {backup_name}")
            assert row["status"] == "error", error(row)
            err = row.get("error", "").lower()
            assert any(s in err for s in ("clickhouse2", "backup2", "backup_actions")), error(row)

    finally:
        with Finally("I start the clickhouse-backup server of clickhouse2 again"):
            backup2.start_server(timeout=120)

        with And("I remove leftovers of the failed backup"):
            self.context.backup.cmd(f"clickhouse-backup delete local {backup_name}", exitcode=None)
            self.context.backup.cmd(f"clickhouse-backup delete remote {backup_name}", exitcode=None)
            backup2.cmd(f"clickhouse-backup delete local {backup_name}", exitcode=None)


@TestFeature
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_EmbeddedBackup_OnCluster("1.0"),
    RQ_SRS_013_ClickHouse_BackupUtility_EmbeddedBackup_OnCluster_Requirements("1.0"),
)
def embedded_on_cluster(self):
    """Embedded BACKUP/RESTORE ON CLUSTER orchestration between two clickhouse-backup servers (#928)."""
    # BACKUP ... ON CLUSTER TO S3(...) needs the S3 backup engine and production ready BACKUP/RESTORE,
    # same gate as TestEmbeddedS3 in test/integration, https://github.com/ClickHouse/ClickHouse/issues/39416
    if os.environ.get("CLICKHOUSE_VERSION", "26.8") < "23.3":
        skip("embedded BACKUP/RESTORE ON CLUSTER needs ClickHouse 23.3+")
    ch1, ch2 = self.context.nodes
    backup = self.context.backup
    backup2 = self.context.backup2
    self.context.url = f"http://localhost:{self.context.backup_api_port}"
    self.context.backup_name = f"embedded_oc_{time.time_ns()}"
    # scenario contexts are not shared between sibling scenarios, so the expected row counts live on the feature
    self.context.expected_rows = {ch1.name: 10, ch2.name: 25}

    with open(self.context.backup_config_origin) as f:
        s3_path = yaml.safe_load(f)["s3"]["path"]

    try:
        with Given("I configure both clickhouse-backup servers for embedded ON CLUSTER backups to minio"):
            config_modifier(fields={
                "general": {"remote_storage": "s3"},
                "s3": {
                    "access_key": MINIO_ACCESS_KEY,
                    "secret_key": MINIO_SECRET_KEY,
                    "endpoint": MINIO_URL,
                    "bucket": MINIO_BUCKET,
                    "disable_ssl": True,
                    "force_path_style": True,
                    "region": "us-west-2",
                    "object_disk_path": f"{s3_path}_embedded_disk",
                },
                "clickhouse": {
                    # ValidateConfig requires clickhouse.timeout >= 240m with use_embedded_backup_restore,
                    # the base config has 5m and the server log.Fatal()s on it inside the actions handler
                    "timeout": "4h",
                    "use_embedded_backup_restore": True,
                    "use_embedded_backup_restore_cluster": CLUSTER,
                    "embedded_backup_disk": "",
                },
            })

        with And("both clickhouse-backup servers are running with integration tables"):
            if not backup2.is_server_running():
                backup2.start_server(timeout=120)
            for node in (ch1, ch2):
                wait_integration_tables(node=node)

        for scenario in loads(current_module(), Scenario, Suite):
            Scenario(run=scenario)

    finally:
        with Finally("I remove the backup and the table"):
            backup.cmd(f"clickhouse-backup delete remote {self.context.backup_name}", exitcode=None)
            backup.cmd(f"clickhouse-backup delete local {self.context.backup_name}", exitcode=None)
            backup2.cmd(f"clickhouse-backup delete local {self.context.backup_name}", exitcode=None)
            ch1.query(f"DROP TABLE IF EXISTS default.{TABLE} ON CLUSTER {CLUSTER} SYNC", no_checks=True)

        with And("I restore the original config"):
            config_modifier()
