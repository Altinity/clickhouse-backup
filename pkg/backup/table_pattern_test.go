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

// TestAddTableToListIfNotExistsOrEnrichQueryAndParts covers the O(1) index-based dedup/enrich:
// the .sql file (carrying Query) and the .json file (carrying Parts) of the same embedded table
// must merge into a single entry, non-empty fields must never be overwritten, and the tableIndex
// must keep pointing at the right slice position across multiple distinct tables.
func TestAddTableToListIfNotExistsOrEnrichQueryAndParts(t *testing.T) {
	result := ListOfTables{}
	tableIndex := make(map[metadata.TableTitle]int)

	// first file for db.t1 carries only Parts (json)
	result = addTableToListIfNotExistsOrEnrichQueryAndParts(result, tableIndex, metadata.TableMetadata{
		Database: "db", Table: "t1",
		Parts: map[string][]metadata.Part{"default": {{Name: "p1"}}},
	})
	// an unrelated table in between, to verify index correctness
	result = addTableToListIfNotExistsOrEnrichQueryAndParts(result, tableIndex, metadata.TableMetadata{
		Database: "db", Table: "t2", Query: "CREATE TABLE db.t2 ...",
	})
	// second file for db.t1 carries only Query (sql) -> must enrich the existing entry, not append
	result = addTableToListIfNotExistsOrEnrichQueryAndParts(result, tableIndex, metadata.TableMetadata{
		Database: "db", Table: "t1", Query: "CREATE TABLE db.t1 ...",
	})

	assert.Len(t, result, 2, "same db.table must not be duplicated")
	assert.Equal(t, 0, tableIndex[metadata.TableTitle{Database: "db", Table: "t1"}])
	assert.Equal(t, 1, tableIndex[metadata.TableTitle{Database: "db", Table: "t2"}])
	assert.Equal(t, "CREATE TABLE db.t1 ...", result[0].Query, "Query must be enriched from the .sql file")
	assert.Len(t, result[0].Parts, 1, "Parts from the .json file must be preserved")

	// a further file with a different Query/Parts must NOT overwrite already-populated fields
	result = addTableToListIfNotExistsOrEnrichQueryAndParts(result, tableIndex, metadata.TableMetadata{
		Database: "db", Table: "t1", Query: "SHOULD NOT WIN",
		Parts: map[string][]metadata.Part{"other": {{Name: "p2"}}},
	})
	assert.Len(t, result, 2)
	assert.Equal(t, "CREATE TABLE db.t1 ...", result[0].Query, "non-empty Query must not be overwritten")
	assert.Contains(t, result[0].Parts, "default", "non-empty Parts must not be overwritten")
	assert.NotContains(t, result[0].Parts, "other")
}
