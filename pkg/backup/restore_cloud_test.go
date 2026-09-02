package backup

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testCloudManifestXML = `<?xml version="1.0"?>
<config>
	<version>1</version>
	<deduplicate_files>1</deduplicate_files>
	<timestamp>2026-08-25 00:00:00</timestamp>
	<uuid>8b6c9b4e-0000-0000-0000-000000000000</uuid>
	<data_file_name_generator>Checksum</data_file_name_generator>
	<data_file_name_prefix_length>3</data_file_name_prefix_length>
	<contents>
		<file>
			<name>metadata/default.sql</name>
			<size>77</size>
			<checksum>abcdef0123456789abcdef0123456789</checksum>
		</file>
		<file>
			<name>metadata/default/hits.sql</name>
			<size>256</size>
			<checksum>0123456789abcdef0123456789abcdef</checksum>
		</file>
		<file>
			<name>data/default/hits/all_0_0_0/data.packed</name>
			<size>1048576</size>
			<checksum>fedcba9876543210fedcba9876543210</checksum>
			<use_base>true</use_base>
		</file>
		<file>
			<name>metadata/db%2Dwith%2Ddash.sql</name>
			<size>42</size>
			<data_file>metadata/other.sql</data_file>
		</file>
		<file>
			<name>metadata/default/empty.sql</name>
			<size>0</size>
		</file>
	</contents>
</config>`

func TestParseCloudManifest(t *testing.T) {
	m, err := parseCloudManifest(strings.NewReader(testCloudManifestXML))
	require.NoError(t, err)
	assert.Equal(t, "Checksum", m.DataFileNameGenerator)
	assert.Equal(t, 3, m.DataFileNamePrefixLength)
	require.Len(t, m.Files, 5)
	assert.Equal(t, "metadata/default.sql", m.Files[0].Name)
	assert.Equal(t, int64(77), m.Files[0].Size)
	assert.True(t, m.Files[2].UseBase)
	assert.False(t, m.Files[1].UseBase)

	// checksum generator splits the checksum by prefix_length
	assert.Equal(t, "abc/def0123456789abcdef0123456789", m.blobKey(&m.Files[0]))
	// data_file has priority over checksum
	assert.Equal(t, "metadata/other.sql", m.blobKey(&m.Files[3]))
}

func TestParseCloudManifestFirstFileNameDefaults(t *testing.T) {
	ossManifest := `<config><version>1</version><contents>
		<file><name>metadata/default/hits.sql</name><size>10</size><checksum>0123456789abcdef0123456789abcdef</checksum></file>
	</contents></config>`
	m, err := parseCloudManifest(strings.NewReader(ossManifest))
	require.NoError(t, err)
	assert.Equal(t, "FirstFileName", m.DataFileNameGenerator)
	assert.Equal(t, 3, m.DataFileNamePrefixLength)
	// FirstFileName generator resolves the blob by logical name
	assert.Equal(t, "metadata/default/hits.sql", m.blobKey(&m.Files[0]))
}

func TestCloudLogicalNames(t *testing.T) {
	db, table := cloudLogicalNames("metadata/default.sql")
	assert.Equal(t, "default", db)
	assert.Equal(t, "", table)

	db, table = cloudLogicalNames("metadata/default/hits.sql")
	assert.Equal(t, "default", db)
	assert.Equal(t, "hits", table)

	db, table = cloudLogicalNames("metadata/db%2Dwith%2Ddash/table%20name.sql")
	assert.Equal(t, "db-with-dash", db)
	assert.Equal(t, "table name", table)
}

func TestRewriteCloudSchemaTables(t *testing.T) {
	zkPath, replica := "'/clickhouse/tables/{uuid}/{shard}'", "'{replica}'"

	// SharedMergeTree without engine args gets Replicated + default args
	out := rewriteCloudSchema(
		"CREATE TABLE default.hits UUID 'aaaa' (`v` UInt64) ENGINE = SharedMergeTree ORDER BY v SETTINGS index_granularity = 8192",
		"table", zkPath, replica)
	assert.Contains(t, out, "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY v")
	assert.Contains(t, out, "CREATE TABLE IF NOT EXISTS default.hits")

	// SharedReplacingMergeTree with args keeps its args, longer engine names are not eaten by SharedMergeTree
	out = rewriteCloudSchema(
		"CREATE TABLE d.t (`v` UInt64) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY v",
		"table", zkPath, replica)
	assert.Contains(t, out, "ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY v")
	assert.NotContains(t, out, "Shared")

	for _, engine := range []string{"VersionedCollapsing", "Replacing", "Aggregating", "Summing", "Collapsing", "Graphite", "Coalescing"} {
		out = rewriteCloudSchema("CREATE TABLE d.t (`v` UInt64) ENGINE = Shared"+engine+"MergeTree ORDER BY v", "table", zkPath, replica)
		assert.Contains(t, out, "ENGINE = Replicated"+engine+"MergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')")
	}

	// plain MergeTree and already-Replicated DDL stay unchanged (except IF NOT EXISTS)
	out = rewriteCloudSchema("CREATE TABLE d.t (`v` UInt64) ENGINE = MergeTree ORDER BY v", "table", zkPath, replica)
	assert.Contains(t, out, "ENGINE = MergeTree ORDER BY v")
	out = rewriteCloudSchema("CREATE TABLE d.t (`v` UInt64) ENGINE = ReplicatedMergeTree('/zk/path', 'r1') ORDER BY v", "table", zkPath, replica)
	assert.Contains(t, out, "ENGINE = ReplicatedMergeTree('/zk/path', 'r1') ORDER BY v")

	// IF NOT EXISTS not duplicated
	out = rewriteCloudSchema("CREATE TABLE IF NOT EXISTS d.t (`v` UInt64) ENGINE = MergeTree ORDER BY v", "table", zkPath, replica)
	assert.Equal(t, 1, strings.Count(out, "IF NOT EXISTS"))

	// materialized view gets IF NOT EXISTS too
	out = rewriteCloudSchema("CREATE MATERIALIZED VIEW d.mv TO d.t (`v` UInt64) AS SELECT v FROM d.src", "table", zkPath, replica)
	assert.True(t, strings.HasPrefix(out, "CREATE MATERIALIZED VIEW IF NOT EXISTS"), out)
}

func TestRewriteCloudSchemaDatabases(t *testing.T) {
	out := rewriteCloudSchema("CREATE DATABASE default ENGINE = Shared", "database", "", "")
	assert.Equal(t, "CREATE DATABASE IF NOT EXISTS default ENGINE = Atomic", out)

	// Shared prefix of a longer engine name must not be rewritten
	out = rewriteCloudSchema("CREATE DATABASE d ENGINE = SharedCatalog", "database", "", "")
	assert.Contains(t, out, "ENGINE = SharedCatalog")

	out = rewriteCloudSchema("CREATE DATABASE d ENGINE = Atomic", "database", "", "")
	assert.Contains(t, out, "ENGINE = Atomic")
}

func TestCloudDropKind(t *testing.T) {
	assert.Equal(t, "DICTIONARY", cloudDropKind("CREATE DICTIONARY IF NOT EXISTS db.d (id UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"))
	assert.Equal(t, "TABLE", cloudDropKind("CREATE TABLE IF NOT EXISTS db.t (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"))
	assert.Equal(t, "TABLE", cloudDropKind("CREATE MATERIALIZED VIEW IF NOT EXISTS db.mv TO db.t AS SELECT * FROM db.src"))
	assert.Equal(t, "TABLE", cloudDropKind("CREATE VIEW IF NOT EXISTS db.v AS SELECT 1"))
}

func TestCloudApplyOrder(t *testing.T) {
	assert.Equal(t, 0, cloudApplyOrder("CREATE DATABASE d"))
	assert.Equal(t, 1, cloudApplyOrder("CREATE DICTIONARY d.dict (v UInt64)"))
	assert.Equal(t, 2, cloudApplyOrder("CREATE TABLE d.t (v UInt64) ENGINE = MergeTree"))
	assert.Equal(t, 3, cloudApplyOrder("CREATE VIEW d.v AS SELECT 1"))
	assert.Equal(t, 4, cloudApplyOrder("CREATE MATERIALIZED VIEW d.mv AS SELECT 1"))
	assert.Equal(t, 5, cloudApplyOrder("ATTACH TABLE d.t"))
}

func TestMatchCloudTablePattern(t *testing.T) {
	assert.True(t, matchCloudTablePattern("", "default", "hits"))
	assert.True(t, matchCloudTablePattern("default.*", "default", "hits"))
	assert.True(t, matchCloudTablePattern("default.h?ts", "default", "hits"))
	assert.True(t, matchCloudTablePattern("other.*, default.hits", "default", "hits"))
	assert.False(t, matchCloudTablePattern("other.*", "default", "hits"))
}

func TestRestoreCloudRedact(t *testing.T) {
	out := restoreCloudRedact("RESTORE TABLE t FROM S3('url', 'AKIAKEY', 'AKIAKEYLONGSECRET')", "AKIAKEY", "AKIAKEYLONGSECRET")
	assert.Equal(t, "RESTORE TABLE t FROM S3('url', '***', '***')", out)
}

func TestCloudRestorePartitionsSQL(t *testing.T) {
	b := &Backuper{}
	ctx := context.Background()
	tableSQL := "CREATE TABLE default.hits (id UInt64, d Date) ENGINE = ReplicatedMergeTree PARTITION BY toYYYYMM(d) ORDER BY id"

	// no --partitions - no clause, table kept
	sql, matched := b.cloudRestorePartitionsSQL(ctx, "default", "hits", tableSQL, nil)
	assert.True(t, matched)
	assert.Equal(t, "", sql)

	// plain partition ids
	sql, matched = b.cloudRestorePartitionsSQL(ctx, "default", "hits", tableSQL, []string{"202408,202409"})
	assert.True(t, matched)
	assert.Equal(t, " PARTITIONS ID '202408',ID '202409'", sql)

	// per-table pattern matches
	sql, matched = b.cloudRestorePartitionsSQL(ctx, "default", "hits", tableSQL, []string{"default.h?ts:202408"})
	assert.True(t, matched)
	assert.Equal(t, " PARTITIONS ID '202408'", sql)

	// per-table pattern does not match - table skipped
	_, matched = b.cloudRestorePartitionsSQL(ctx, "default", "hits", tableSQL, []string{"other.*:202408"})
	assert.False(t, matched)

	// `*` restores everything without a clause
	sql, matched = b.cloudRestorePartitionsSQL(ctx, "default", "hits", tableSQL, []string{"default.hits:*"})
	assert.True(t, matched)
	assert.Equal(t, "", sql)

	// views never get a PARTITIONS clause but are still filtered by per-table patterns
	viewSQL := "CREATE MATERIALIZED VIEW default.mv TO default.hits AS SELECT * FROM default.src"
	sql, matched = b.cloudRestorePartitionsSQL(ctx, "default", "mv", viewSQL, []string{"202408"})
	assert.True(t, matched)
	assert.Equal(t, "", sql)
	_, matched = b.cloudRestorePartitionsSQL(ctx, "default", "mv", viewSQL, []string{"default.hits:202408"})
	assert.False(t, matched)
}

func TestInjectCloudOnCluster(t *testing.T) {
	onCluster := " ON CLUSTER 'prod'"
	assert.Equal(t,
		"CREATE TABLE IF NOT EXISTS default.hits UUID 'aaaa-bb' ON CLUSTER 'prod' (`id` UInt64) ENGINE = ReplicatedMergeTree",
		injectCloudOnCluster("CREATE TABLE IF NOT EXISTS default.hits UUID 'aaaa-bb' (`id` UInt64) ENGINE = ReplicatedMergeTree", onCluster))
	assert.Equal(t,
		"CREATE DATABASE IF NOT EXISTS `my db` ON CLUSTER 'prod' ENGINE = Atomic",
		injectCloudOnCluster("CREATE DATABASE IF NOT EXISTS `my db` ENGINE = Atomic", onCluster))
	assert.Equal(t,
		"CREATE MATERIALIZED VIEW default.mv UUID 'aaaa-bb' ON CLUSTER 'prod' TO default.hits AS SELECT 1",
		injectCloudOnCluster("CREATE MATERIALIZED VIEW default.mv UUID 'aaaa-bb' TO default.hits AS SELECT 1", onCluster))
	assert.Equal(t,
		"CREATE DICTIONARY `db`.`dict` ON CLUSTER 'prod' (v UInt64) PRIMARY KEY v",
		injectCloudOnCluster("CREATE DICTIONARY `db`.`dict` (v UInt64) PRIMARY KEY v", onCluster))
	// no clause requested - unchanged
	sql := "CREATE TABLE default.hits (id UInt64) ENGINE = MergeTree"
	assert.Equal(t, sql, injectCloudOnCluster(sql, ""))
}

func TestCloudShardPrefix(t *testing.T) {
	m := cloudShardPrefixRE.FindStringSubmatch("shards/2/replicas/3/metadata/default/hits.sql")
	require.NotNil(t, m)
	assert.Equal(t, "2", m[1])
	assert.Equal(t, "metadata/default/hits.sql", strings.TrimPrefix("shards/2/replicas/3/metadata/default/hits.sql", m[0]))
	assert.Nil(t, cloudShardPrefixRE.FindStringSubmatch("metadata/default/hits.sql"))
	assert.Nil(t, cloudShardPrefixRE.FindStringSubmatch("data/default/hits/all_1_1_0/data.packed"))
}

func TestCloudLogicalNamesShardPrefix(t *testing.T) {
	db, table := cloudLogicalNames("shards/2/replicas/3/metadata/default/hits.sql")
	assert.Equal(t, "default", db)
	assert.Equal(t, "hits", table)
	db, table = cloudLogicalNames("shards/1/replicas/1/metadata/default.sql")
	assert.Equal(t, "default", db)
	assert.Equal(t, "", table)
}
