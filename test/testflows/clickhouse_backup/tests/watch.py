import json

import yaml

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.steps import *

DATABASE = "watch_1535"
STALE_BACKUPS = ["stale_1535_1", "stale_1535_2"]
# backups_to_keep_remote=2 needs watch_interval * 2 >= full_interval, watch is killed by `timeout` between the
# increment (~30s) and the next full backup (~60s)
WATCH_TIMEOUT = 50
# old remote backups tagged `embedded` are cleaned through `embedded_backup_disk`, a disk which doesn't exist on the
# server makes every backups_to_keep_remote retention fail after the new backup is completely uploaded,
# regular (non embedded) backups never use embedded_backup_disk
WATCH_ENV = ("--env=REMOTE_STORAGE=ftp --env=BACKUPS_TO_KEEP_LOCAL=0 --env=BACKUPS_TO_KEEP_REMOTE=2 "
             "--env=CLICKHOUSE_EMBEDDED_BACKUP_DISK=missing_disk_1535")
FTP_ENV = "--env=REMOTE_STORAGE=ftp"


def remote_backups(backup, prefix):
    """[(name, required_backup)] of remote backups with the name prefix, ordered by upload date."""
    r = backup.cmd(f"clickhouse-backup {FTP_ENV} list remote")
    rows = []
    for line in r.output.splitlines():
        fields = line.split()
        if fields and fields[0].startswith(prefix):
            required = fields[4] if len(fields) > 4 and fields[4].startswith("+") else ""
            rows.append((fields[0], required.lstrip("+")))
    return rows


@TestStep(Given)
def upload_stale_embedded_backups(self):
    """Put two old remote backups tagged `embedded` directly to FTP, retention tries to delete them first."""
    backup = self.context.backup
    with open(self.context.backup_config_file) as f:
        ftp = yaml.safe_load(f)["ftp"]
    for name in STALE_BACKUPS:
        backup_metadata = {
            "backup_name": name, "disks": {}, "disk_types": {}, "version": "test",
            "creation_date": "2020-01-01T00:00:00Z", "tags": "embedded", "metadata_size": 0,
            "tables": [], "functions": [], "data_format": "tar",
        }
        backup.write_file(f"/tmp/{name}.json", json.dumps(backup_metadata))
        backup.cmd(f"curl -sS --ftp-create-dirs -T /tmp/{name}.json "
                   f"ftp://{ftp['username']}:{ftp['password']}@{ftp['address']}/{ftp['path']}/{name}/metadata.json")
    names = [name for name, _ in remote_backups(backup, "stale_1535_")]
    assert names == STALE_BACKUPS, error(names)


@TestStep(Finally)
def cleanup(self):
    backup = self.context.backup
    with By("delete remote backups newest first, a parent is never deleted before its increment"):
        for prefix in ("watch_1535_", "stale_1535_"):
            for name, _ in reversed(remote_backups(backup, prefix)):
                backup.cmd(f"clickhouse-backup {FTP_ENV} delete remote {name}", exitcode=None)
    with By("delete local backups left by the killed watch"):
        r = backup.cmd("clickhouse-backup list local", exitcode=None)
        for line in r.output.splitlines():
            fields = line.split()
            if fields and fields[0].startswith("watch_1535_"):
                backup.cmd(f"clickhouse-backup delete local {fields[0]}", exitcode=None)
    self.context.nodes[0].query(f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
    with Then("no backups of this test remain on remote storage"):
        rows = remote_backups(backup, "watch_1535_") + remote_backups(backup, "stale_1535_")
        assert rows == [], error(rows)


@TestScenario
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_IncrementalBackups("1.0"))
def retention_failure_keeps_chain(self):
    """A failed backups_to_keep_remote retention after a completely uploaded backup doesn't abort watch,
    the next increment uses the uploaded backup as its base, https://github.com/Altinity/clickhouse-backup/issues/1535."""
    ch = self.context.nodes[0]
    backup = self.context.backup
    try:
        with Given("a table to back up"):
            ch.query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
            ch.query(f"CREATE TABLE {DATABASE}.t (id UInt64) ENGINE=MergeTree ORDER BY id")
            ch.query(f"INSERT INTO {DATABASE}.t SELECT number FROM numbers(100)")
        upload_stale_embedded_backups()

        with When(f"I run watch for {WATCH_TIMEOUT}s while every remote retention fails"):
            r = backup.cmd(
                f"timeout {WATCH_TIMEOUT} clickhouse-backup {WATCH_ENV} watch --tables={DATABASE}.* "
                f"--watch-interval=30s --full-interval=60s "
                f"--watch-backup-name-template='watch_1535_{{type}}_{{time:20060102150405}}'",
                exitcode=124,
            )

        with Then("watch keeps running, the retention error is logged"):
            assert "too many errors" not in r.output, error(r.output)
            assert "can't remove old backups on remote storage" in r.output, error(r.output)

        with And("the increment is based on the full backup uploaded before"):
            rows = remote_backups(backup, "watch_1535_")
            assert len(rows) >= 2, error(rows)
            (full_name, full_required), (increment_name, increment_required) = rows[0], rows[1]
            assert "_full_" in full_name and full_required == "", error(rows)
            assert "_increment_" in increment_name and increment_required == full_name, error(rows)

        with And("retention still couldn't delete the stale backups"):
            names = [name for name, _ in remote_backups(backup, "stale_1535_")]
            assert names == STALE_BACKUPS, error(names)
    finally:
        cleanup()


@TestFeature
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_IncrementalBackups("1.0"))
def watch(self):
    """`watch` command."""
    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario, flags=TE)
