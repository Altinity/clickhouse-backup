package backup

import (
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/stretchr/testify/assert"
)

// TestSkipTablesByEngine reproduces https://github.com/Altinity/clickhouse-backup
// where exactly one table escaped SkipTableEngines filtering when two skippable
// tables ended up adjacent at the head of the list after Sort: removing result[0]
// shifted result[1] into index 0, but the `if i > 0` guard prevented re-checking it.
func TestSkipTablesByEngine(t *testing.T) {
	iceberg := func(db, table string) *metadata.TableMetadata {
		return &metadata.TableMetadata{
			Database: db,
			Table:    table,
			Query:    "CREATE TABLE " + db + ".`" + table + "` (`id` Nullable(Int32)) ENGINE = Iceberg('s3://bucket/path')",
		}
	}
	mergeTree := func(db, table string) *metadata.TableMetadata {
		return &metadata.TableMetadata{
			Database: db,
			Table:    table,
			Query:    "CREATE TABLE " + db + ".`" + table + "` (`id` Int32) ENGINE = MergeTree ORDER BY id",
		}
	}

	b := &Backuper{cfg: &config.Config{}}
	b.cfg.ClickHouse.SkipTableEngines = []string{"IceBerg"}

	// Two Iceberg tables adjacent at the start triggered the off-by-one: the second survived.
	result := ListOfTables{
		iceberg("provisioning_iceberg", "provisioning_dbo.dataeventtype"),
		iceberg("provisioning_iceberg", "provisioning_rptsched.reportschedulingeventtype"),
		mergeTree("default", "events"),
		iceberg("provisioning_iceberg", "provisioning_dbo.trackingtagtype"),
	}
	partitionNames := map[metadata.TableTitle][]string{
		{Database: "provisioning_iceberg", Table: "provisioning_dbo.dataeventtype"}:                  {"p1"},
		{Database: "provisioning_iceberg", Table: "provisioning_rptsched.reportschedulingeventtype"}: {"p2"},
	}

	result = b.skipTablesByEngine(result, partitionNames)

	for _, tbl := range result {
		assert.NotContains(t, tbl.Query, "ENGINE = Iceberg", "Iceberg table `%s`.`%s` was not skipped", tbl.Database, tbl.Table)
	}
	assert.Len(t, result, 1, "only the MergeTree table must remain")
	assert.Equal(t, "events", result[0].Table)
	assert.Empty(t, partitionNames, "partition names of skipped tables must be removed")
}
