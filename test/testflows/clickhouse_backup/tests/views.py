from testflows.core import *
from testflows.asserts import *

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.steps import *


@TestOutline
def views_outline(self, view_name, view_create_query, view_contents_query):
    """Test that view is handled properly by clickhouse-backup.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    base_table_name = self.context.views_base_name

    try:
        with Given(f"I create {view_name}"):
            clickhouse.query(view_create_query)
            view_data = clickhouse.query(view_contents_query).output if view_contents_query else ""

        with And("I create backup"):
            backup.cmd(f"clickhouse-backup create {base_table_name}_{view_name}")

        with When("I drop view and original table"):
            clickhouse.query(f"DROP VIEW IF EXISTS {base_table_name}_{view_name} SYNC")
            drop_table(node=clickhouse, table_name=base_table_name)
            time.sleep(10)

        with Then("I restore"):
            backup.cmd(f"clickhouse-backup restore {base_table_name}_{view_name}")

        with And("I expect view restored"):
            r = clickhouse.query("SHOW TABLES").output

            with By(f"expect {base_table_name}_{view_name} exists"):
                assert f"{base_table_name}_{view_name}" in r, error()

            with By(f"expect {base_table_name}_{view_name} restored"):
                assert clickhouse.query(view_contents_query).output == view_data, error()

    finally:
        with Finally("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local {base_table_name}_{view_name}")

        with And("I drop view"):
            clickhouse.query(f"DROP VIEW IF EXISTS {base_table_name}_{view_name} SYNC")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Views_View("1.0")
)
def view(self):
    """Test that view is handled properly by clickhouse-backup.
    """
    base_table_name = self.context.views_base_name

    views_outline(view_name="sview", view_contents_query=f"SELECT * FROM {base_table_name}_sview",
                  view_create_query=f"CREATE VIEW {base_table_name}_sview AS "
                                    f"SELECT Version, Path, Time FROM default.{base_table_name}")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Views_MaterializedView("1.0"),
)
def materialized_view(self):
    """Test that materialized view is handled properly by clickhouse-backup.
    """
    base_table_name = self.context.views_base_name

    views_outline(view_name="mview", view_contents_query=f"SELECT * FROM {base_table_name}_mview",
                  view_create_query=f"CREATE MATERIALIZED VIEW {base_table_name}_mview ENGINE = MergeTree "
                                    f"ORDER BY Time POPULATE AS SELECT Version, Path, Time FROM default.{base_table_name}")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Views_LiveView("1.0")
)
def live_view(self):
    """Test that live view is handled properly by clickhouse-backup.
    """
    base_table_name = self.context.views_base_name

    views_outline(view_name="lview", view_contents_query=f"SELECT * FROM {base_table_name}_lview",
                  view_create_query=f"CREATE LIVE VIEW {base_table_name}_lview AS "
                                    f"SELECT Version, Path, Time FROM default.{base_table_name}")

@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Views_WindowView("1.0")
)
def window_view(self):
    """Test that window view is handled properly by clickhouse-backup.
    """
    base_table_name = self.context.views_base_name

    views_outline(view_name="wview", view_contents_query=f"DESCRIBE {base_table_name}_wview",
                  view_create_query=f"CREATE WINDOW VIEW {base_table_name}_wview AS SELECT count(Version), tumbleStart(w_id) "
                                    f"FROM default.{base_table_name} GROUP BY tumble(Time, INTERVAL '10' SECOND) as w_id")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Views_NestedViews("1.0")
)
def nested_views(self):
    """Test that nested views are handled properly by clickhouse-backup.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    base_table_name = self.context.views_base_name

    try:
        with Given("I create views"):
            with By("I create View"):
                clickhouse.query(f"CREATE VIEW {base_table_name}_sview AS SELECT Version, Path, Time FROM default.{base_table_name}")

            with And("I create nested View"):
                clickhouse.query(f"CREATE VIEW {base_table_name}_nview AS SELECT Version, Time FROM default.{base_table_name}_sview")
                view_data = clickhouse.query(f"SELECT * FROM {base_table_name}_nview").output

        with When("I create backup"):
            backup.cmd(f"clickhouse-backup create {base_table_name}")

        with And("I drop original table and views"):
            drop_table(node=clickhouse, table_name=f"{base_table_name}_nview")
            drop_table(node=clickhouse, table_name=f"{base_table_name}_sview")
            drop_table(node=clickhouse, table_name=base_table_name)

        with Then("I restore"):
            backup.cmd(f"clickhouse-backup restore {base_table_name}")

        with And("I expect view restored"):
            r = clickhouse.query("SHOW TABLES").output

            assert f"{base_table_name}_nview" in r, error()

            with By(f"expect {base_table_name}_nview restored"):
                assert clickhouse.query(f"SELECT * FROM default.{base_table_name}_nview").output == view_data, error()

    finally:
        with Finally("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local {base_table_name}")

        with And("I drop views"):
            drop_table(node=clickhouse, table_name=f"{base_table_name}_sview")
            drop_table(node=clickhouse, table_name=f"{base_table_name}_nview")


@TestFeature
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Views_NestedViews("1.0")
)
def views(self):
    """Check proper handling of views.
    """
    self.context.views_base_name = "bp_view"
    clickhouse = self.context.nodes[0]

    try:
        with Given("I create base table"):
            create_and_populate_table(node=clickhouse, table_name=self.context.views_base_name, columns=self.context.columns)

        for scenario in loads(current_module(), Scenario, Suite):
            Scenario(run=scenario)

    finally:
        with Finally("I drop table"):
            drop_table(node=clickhouse, table_name=self.context.views_base_name)
