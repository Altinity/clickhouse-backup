from testflows.core import *
from testflows.asserts import *

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.steps import *


@TestScenario
@Flags(MANUAL)
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_Generic_SpecificTables_Performance("1.0")
)
def create_backup_performance(self):
    """Test that a specific table backups are created with high performance.
    To perform this smoke test, you need to follow the steps below manually.
    Some helper scripts may be found in smoke_helpers
    """
    with Given("I create 1000 different tables in ClickHouse"):
        pass

    with And("I populate them with some random data, 1000 rows for each table"):
        pass

    with When("I create backup for one of the tables using clickhouse-backup==1.2.0 and above"):
        pass

    with And("I create backup for one of the tables using clickhouse-backup==1.1.1 and below"):
        pass

    with Then("I expect clickhouse-backup>=1.2.0 to outperform clickhouse-backup<=1.1.1"):
        pass



@TestFeature
def smoke(self):
    """Perform smoke tests fo ClickHouse backup.
    """
    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario, flags=TE)
