#!/usr/bin/env python3
#  Copyright 2021, Altinity LTD. All Rights Reserved.
#
#  All information contained herein is, and remains the property
#  of Altinity LTD. Any dissemination of this information or
#  reproduction of this material is strictly forbidden unless
#  prior written permission is obtained from Altinity LTD.
import os
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import simple_data_types_columns

xfails = {
    "/clickhouse backup/other engines/materializedpostgresql/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/32902")],
    "/clickhouse backup/other engines/materializedmysql/:": [(Fail, "DROP TABLE not supported by MaterializedMySQL, just attach will not help")],
}


@TestSuite
@Name("clickhouse backup")
@XFails(xfails)
@ArgumentParser(argparser)
@Specifications(
    QA_SRS013_ClickHouse_Backup_Utility
)
def regression(self, local):
    """ClickHouse Backup utility test regression suite.
    """
    nodes = {
        "clickhouse": ("clickhouse1", "clickhouse2"),
        "clickhouse_backup": ("clickhouse_backup",),
        "kafka": ("kafka",),
        "mysql": ("mysql",),
        "postgres": ("postgres",),
    }

    with Cluster(local, nodes=nodes) as cluster:
        cwd = os.environ.get('CLICKHOUSE_TESTS_DIR') if os.environ.get('CLICKHOUSE_TESTS_DIR') else os.getcwd()
        self.context.backup_config_file = f"{cwd}/configs/backup/config.yml"
        self.context.cluster = cluster
        self.context.nodes = [self.context.cluster.node(n) for n in ["clickhouse1", "clickhouse2"]]
        self.context.backup = self.context.cluster.node("clickhouse_backup")
        self.context.kafka = self.context.cluster.node("kafka")
        self.context.mysql = self.context.cluster.node("mysql")
        self.context.postgres = self.context.cluster.node("postgres")

        self.context.database_engines_names = {"Atomic": "atmc", "Ordinary": "ordn"}
        self.context.table_engines = ["MergeTree", "ReplacingMergeTree", "SummingMergeTree", "CollapsingMergeTree",
                                      "VersionedCollapsingMergeTree"]

        self.context.columns = simple_data_types_columns["misc"]

        self.context.all_columns = simple_data_types_columns

        Scenario(run=load("clickhouse_backup.tests.smoke", "smoke"), flags=TE)

        Scenario(run=load("clickhouse_backup.tests.generic", "generic"))
        Scenario(run=load("clickhouse_backup.tests.other_engines", "other_engines"))
        Scenario(run=load("clickhouse_backup.tests.config_rbac", "config_rbac"))
        Scenario(run=load("clickhouse_backup.tests.api", "api"))
        Scenario(run=load("clickhouse_backup.tests.cli", "cli"))
        Scenario(run=load("clickhouse_backup.tests.cloud_storage", "cloud_storage"))
        Scenario(run=load("clickhouse_backup.tests.views", "views"))


if main():
    regression()
