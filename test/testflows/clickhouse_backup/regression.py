#!/usr/bin/env python3
#  Copyright 2022, Altinity LTD. All Rights Reserved.
#
#  All information contained herein is, and remains the property
#  of Altinity LTD. Any dissemination of this information or
#  reproduction of this material is strictly forbidden unless
#  prior written permission is obtained from Altinity LTD.
import os
import shutil
import sys
import yaml
import testflows.settings as testflows_settings
from testflows.core import *

append_path(sys.path, "..")
from helpers.cluster import Cluster
from helpers.argparser import argparser

from clickhouse_backup.requirements.requirements import *

from clickhouse_backup.requirements.fips.requirements import *
from clickhouse_backup.tests.common import simple_data_types_columns

# `--fips-godebug` choices mapped to the `GODEBUG` value exported on
# the `clickhouse_backup_fips` container, so the whole FIPS suite runs in the
# selected mode. Default `only`:
#   * `unset` - leave `GODEBUG` unset; FIPS active by build-time default.
#   * `on`    - FIPS active without strict enforcement.
#   * `only`  - FIPS active with strict enforcement (default).
#   * `off`   - FIPS disabled at runtime.
FIPS_GODEBUG_VALUES = {
    "empty": "",
    "unset": None,
    "on": "fips140=on",
    "only": "fips140=only",
    "off": "fips140=off",
}

testflows_settings.show_skipped = True # used for debug if a check is unintentionally skipped


xfails = {
    "/clickhouse backup/other engines/materializedpostgresql/:": [
        (Fail, "FREEZE not supported by MaterializedPostgreSQL, https://github.com/ClickHouse/ClickHouse/issues/32902")
    ],
    "/clickhouse backup/other engines/materializedmysql/:": [
        (Fail, "DROP TABLE not supported by MaterializedMySQL, https://github.com/ClickHouse/ClickHouse/issues/57543")
    ],
}


@TestSuite
@Name("clickhouse backup")
@XFails(xfails)
@ArgumentParser(argparser)
@Specifications(
    QA_SRS013_ClickHouse_Backup_Utility,
    QA_SRS013_ClickHouse_Backup_Utility_FIPS_Compatibility,
)
def regression(self, local, stress=False, fips=True, fips_godebug="only"):
    """ClickHouse Backup utility test regression suite.

    :param fips_godebug: GODEBUG fips140 mode the whole FIPS suite runs in -
        one of `unset`, `on`, `only` (default) or `off`. It is exported on the
        `clickhouse_backup_fips` container so every `clickhouse-backup-fips`
        command inherits it.
    """
    nodes = {
        "clickhouse": ("clickhouse1", "clickhouse2"),
        "clickhouse_backup": ("clickhouse_backup",),
        "clickhouse_backup_fips": ("clickhouse_backup_fips",),
        "kafka": ("kafka",),
        "mysql": ("mysql",),
        "postgres": ("postgres",),
    }

    # Create per-process backup config dir to avoid races in parallel runs
    cwd = os.environ.get('CLICKHOUSE_TESTS_DIR') if os.environ.get('CLICKHOUSE_TESTS_DIR') else os.path.dirname(os.path.abspath(__file__))
    base_config_dir = f"{cwd}/configs/backup"
    config_dir = f"{cwd}/configs/backup_{os.getpid()}"
    shutil.copytree(base_config_dir, config_dir, dirs_exist_ok=True)

    storage_prefix = f"testflows_{os.getpid()}"
    origin_path = f"{config_dir}/config.yml.origin"
    config_path = f"{config_dir}/config.yml"
    with open(origin_path) as f:
        cfg = yaml.safe_load(f)
    for section in ("s3", "gcs", "azblob", "ftp", "sftp", "cos"):
        if section in cfg:
            cfg[section]["path"] = storage_prefix
    with open(origin_path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False)
    with open(config_path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False)

    # Resolve `--fips-godebug` choice to the `GODEBUG` value
    # exported on the `clickhouse_backup_fips` container. This is FIPS-scoped:
    # only affects the `fips_140_3` tests
    fips_godebug_value = FIPS_GODEBUG_VALUES.get(fips_godebug, "fips140=only")

    try:
        with Cluster(local, nodes=nodes, backup_config_dir=config_dir,
                     fips_godebug=fips_godebug_value) as cluster:
            self.context.backup_config_origin = origin_path
            self.context.backup_config_file = config_path
            self.context.cluster = cluster
            # `--stress` widens the FIPS cipher/suite coverage (see tests/fips_140_3.py).
            # Default runs keep the documented minimum so they stay fast.
            self.context.stress = stress
            self.context.nodes = [self.context.cluster.node(n) for n in ["clickhouse1", "clickhouse2"]]
            self.context.backup = self.context.cluster.node("clickhouse_backup")
            self.context.kafka = self.context.cluster.node("kafka")
            self.context.mysql = self.context.cluster.node("mysql")
            self.context.postgres = self.context.cluster.node("postgres")

            self.context.backup_api_port = cluster.get_mapped_port("clickhouse_backup", 7171)

            # FIPS backup container is optional: only present when the FIPS-compatible
            # binary was built (``make build-race-fips-docker``). FIPS scenarios skip
            # gracefully when this is None (see tests/fips_140_3.py).
            if cluster.has_node("clickhouse_backup_fips"):
                self.context.backup_fips = self.context.cluster.node("clickhouse_backup_fips")
                self.context.backup_fips_api_port = cluster.get_mapped_port("clickhouse_backup_fips", 7172)
            else:
                self.context.backup_fips = None
                self.context.backup_fips_api_port = None

            self.context.database_engines_names = {"Atomic": "atmc", "Ordinary": "ordn"}
            self.context.table_engines = ["MergeTree", "ReplacingMergeTree", "SummingMergeTree", "CollapsingMergeTree",
                                          "VersionedCollapsingMergeTree"]

            self.context.columns = simple_data_types_columns["misc"]

            self.context.all_columns = simple_data_types_columns

            Feature(run=load("clickhouse_backup.tests.fips_140_3", "fips_140_3"), flags=TE)
            Scenario(run=load("clickhouse_backup.tests.smoke", "smoke"), flags=TE)

            Scenario(run=load("clickhouse_backup.tests.cloud_storage", "cloud_storage"))
            Scenario(run=load("clickhouse_backup.tests.other_engines", "other_engines"))
            Scenario(run=load("clickhouse_backup.tests.api", "api"))
            Scenario(run=load("clickhouse_backup.tests.cli", "cli"))
            Scenario(run=load("clickhouse_backup.tests.generic", "generic"))
            Scenario(run=load("clickhouse_backup.tests.views", "views"))
            Scenario(run=load("clickhouse_backup.tests.config_rbac", "config_rbac"))
    finally:
        shutil.rmtree(config_dir, ignore_errors=True)


if main():
    regression()
