import json
import time

import yaml

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.steps import *

DATABASE = "watch_1535"
STALE_BACKUPS = ["stale_1535_1", "stale_1535_2"]
# old remote backups tagged `embedded` are cleaned through `embedded_backup_disk`, a disk which doesn't exist on the
# server makes every backups_to_keep_remote retention fail after the new backup is completely uploaded,
# regular (non embedded) backups never use embedded_backup_disk;
# the config file is modified instead of --env, watch reloads the config file every iteration without --env overrides;
# backups_to_keep_remote=2 needs watch_interval * 2 >= full_interval
WATCH_CONFIG = {
    "general": {"remote_storage": "ftp", "backups_to_keep_local": 0, "backups_to_keep_remote": 2},
    "clickhouse": {"embedded_backup_disk": "missing_disk_1535"},
}
WATCH_SCRIPT = "/tmp/watch_1535.sh"
WATCH_LOG = "/tmp/watch_1535.log"
WATCH_PID = "/tmp/watch_1535.pid"
WATCH_EXIT_MARKER = "watch exit code:"
# the increment starts ~30s after the full backup, the deadline only bounds a hung run
INCREMENT_DEADLINE = 300


def remote_backups(backup, prefix):
    """[(name, required_backup)] of remote backups with the name prefix, ordered by upload date,
    required_backup is empty for a full backup and for a backup without uploaded metadata.json."""
    r = backup.cmd("clickhouse-backup list --format=json remote 2>/dev/null", steps=False)
    rows = []
    for line in r.output.splitlines():
        if line.startswith("["):
            for item in json.loads(line):
                if item["BackupName"].startswith(prefix):
                    rows.append((item["BackupName"], item["RequiredBackup"].lstrip("+")))
    return rows


def ftp_config():
    with open(current().context.backup_config_file) as f:
        return yaml.safe_load(f)["ftp"]


def watch_exited(backup):
    return WATCH_EXIT_MARKER in backup.cmd(f"cat {WATCH_LOG}", exitcode=None, steps=False).output


@TestStep(Given)
def upload_stale_embedded_backups(self):
    """Put two old remote backups tagged `embedded` directly to FTP, retention tries to delete them first."""
    backup = self.context.backup
    ftp = ftp_config()
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


@TestStep(When)
def start_watch(self):
    """Start watch in the background, its PID and finally its exit code are written to files,
    so the test can tell whether watch stopped by itself."""
    backup = self.context.backup
    backup.write_file(WATCH_SCRIPT, "\n".join([
        "#!/bin/bash",
        f"clickhouse-backup watch --tables={DATABASE}.* --watch-interval=30s --full-interval=60s "
        f"--watch-backup-name-template='watch_1535_{{type}}_{{time:20060102150405}}' > {WATCH_LOG} 2>&1 &",
        f"echo $! > {WATCH_PID}",
        "wait $!",
        f'echo "{WATCH_EXIT_MARKER} $?" >> {WATCH_LOG}',
        "",
    ]))
    backup.cmd(f"rm -f {WATCH_LOG} {WATCH_PID}")
    # background job of a child bash like BackupNode.start_server, a job of the interactive shell
    # prints `[1]+ Done ...` into the output of a later command
    backup.cmd(f"bash -c 'nohup bash {WATCH_SCRIPT} > /dev/null 2>&1 < /dev/null &'")


@TestStep(Finally)
def stop_watch(self):
    """Kill the watch process by the PID from its PID file and wait until it exits."""
    backup = self.context.backup
    backup.cmd(f"if [[ -f {WATCH_PID} ]]; then kill $(cat {WATCH_PID}) 2>/dev/null; fi", exitcode=None)
    deadline = time.time() + 60
    while backup.cmd(f"[[ -f {WATCH_PID} ]] && kill -0 $(cat {WATCH_PID}) 2>/dev/null", exitcode=None, steps=False).exitcode == 0:
        assert time.time() < deadline, error("watch didn't stop after SIGTERM")
        time.sleep(1)


@TestStep(Finally)
def cleanup(self):
    backup = self.context.backup
    try:
        stop_watch()
        with By("delete remote backups newest first, a parent is never deleted before its increment"):
            for name, _ in reversed(remote_backups(backup, "watch_1535_")):
                backup.cmd(f"clickhouse-backup delete remote {name}", exitcode=None)
        with By("delete stale backups directly on FTP, `delete remote` of an embedded backup fails with missing embedded_backup_disk"):
            ftp = ftp_config()
            for name in STALE_BACKUPS:
                backup.cmd(f"curl -sS -o /dev/null ftp://{ftp['username']}:{ftp['password']}@{ftp['address']}/ "
                           f"-Q 'DELE {ftp['path']}/{name}/metadata.json' -Q 'RMD {ftp['path']}/{name}'", exitcode=None)
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
    finally:
        with By("I restore the original config"):
            config_modifier()


@TestScenario
@Requirements(RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_IncrementalBackups("1.0"))
def retention_failure_keeps_chain(self):
    """A failed backups_to_keep_remote retention after a completely uploaded backup doesn't abort watch,
    the next increment uses the uploaded backup as its base, https://github.com/Altinity/clickhouse-backup/issues/1535.
    Before the fix every failed iteration was retried at once as a full backup and watch stopped by itself with
    `too many errors` after the third one, before any increment was uploaded."""
    ch = self.context.nodes[0]
    backup = self.context.backup
    try:
        with Given("ftp remote storage, backups_to_keep_remote=2 and a missing embedded_backup_disk"):
            config_modifier(fields=WATCH_CONFIG)
        with And("a table to back up"):
            ch.query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
            ch.query(f"CREATE TABLE IF NOT EXISTS {DATABASE}.t (id UInt64) ENGINE=MergeTree ORDER BY id")
            ch.query(f"INSERT INTO {DATABASE}.t SELECT number FROM numbers(100)")
        upload_stale_embedded_backups()

        start_watch()
        with When("I wait until an increment with uploaded metadata.json appears on remote storage or watch stops"):
            deadline = time.time() + INCREMENT_DEADLINE
            rows = []
            while time.time() < deadline and not watch_exited(backup):
                rows = remote_backups(backup, "watch_1535_")
                if any("_increment_" in name and required for name, required in rows):
                    break
                time.sleep(2)

        # the exit code after our SIGTERM depends on the signal handler, so the check is that watch was still
        # running when the increment was uploaded, a watch which stopped by itself writes WATCH_EXIT_MARKER
        with Then("watch is still running"):
            log = backup.cmd(f"cat {WATCH_LOG}", exitcode=None).output
            assert WATCH_EXIT_MARKER not in log, error(log)
            assert "too many errors" not in log, error(log)

        with And("the retention error is logged"):
            assert "can't remove old backups on remote storage" in log, error(log)

        with And("the increment is based on the full backup uploaded before"):
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
