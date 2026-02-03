//go:build integration

package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

func TestRestoreMapping(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	testBackupName := "test_restore_database_mapping"
	databaseList := []string{"database-1", "database-2"}
	fullCleanup(t, r, env, []string{testBackupName}, []string{"local"}, databaseList, false, false, false, "config-database-mapping.yml")

	createSQL := "CREATE DATABASE `database-1`"
	// https://github.com/Altinity/clickhouse-backup/issues/1146
	expectedDbEngine := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.11") >= 0 {
		engineSQL := " ENGINE=Replicated('/clickhouse/{cluster}/{database}','{shard}','{replica}')"
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "24.3") <= 0 {
			engineSQL = " ENGINE=Replicated('/clickhouse/{cluster}/database-1','{shard}','{replica}')"
		}
		createSQL += engineSQL
		expectedDbEngine = "Replicated"
	}

	env.queryWithNoError(r, createSQL)
	env.queryWithNoError(r, "CREATE TABLE `database-1`.t1 (dt DateTime, v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/database-1/t1','{replica}') PARTITION BY v % 10 ORDER BY dt")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.3") < 0 {
		env.queryWithNoError(r, "CREATE TABLE `database-1`.t2 AS `database-1`.t1 ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/database-1/t2','{replica}') PARTITION BY toYYYYMM(dt) ORDER BY dt")
	} else {
		env.queryWithNoError(r, "CREATE TABLE `database-1`.t2 AS `database-1`.t1 ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}','{replica}') PARTITION BY toYYYYMM(dt) ORDER BY dt")
	}
	env.queryWithNoError(r, "CREATE TABLE `database-1`.`t-d1` AS `database-1`.t1 ENGINE=Distributed('{cluster}', 'database-1', 't1')")
	env.queryWithNoError(r, "CREATE MATERIALIZED VIEW `database-1`.mv1 TO `database-1`.t2 AS SELECT * FROM `database-1`.t1")
	env.queryWithNoError(r, "CREATE VIEW `database-1`.v1 AS SELECT * FROM `database-1`.t1")
	env.queryWithNoError(r, "INSERT INTO `database-1`.t1 SELECT '2022-01-01 00:00:00', number FROM numbers(10)")

	log.Debug().Msg("Create backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "create", testBackupName)

	log.Debug().Msg("Restore schema with --restore-database-mapping + --restore-table-mapping")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--schema", "--rm", "--restore-database-mapping", "database-1:database-2", "--restore-table-mapping", "t1:t3,t2:t4,t-d1:t-d2,mv1:mv2,v1:v2", "--tables", "database-1.*", testBackupName)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.11") >= 0 {
		env.checkCount(r, 1, 1, "SELECT count() FROM system.databases WHERE name='database-2' AND engine='"+expectedDbEngine+"'")
	}

	log.Debug().Msg("Check result database-1")
	env.queryWithNoError(r, "INSERT INTO `database-1`.t1 SELECT '2023-01-01 00:00:00', number FROM numbers(10)")
	env.checkCount(r, 1, 20, "SELECT count() FROM `database-1`.t1")
	env.checkCount(r, 1, 20, "SELECT count() FROM `database-1`.t2")
	env.checkCount(r, 1, 20, "SELECT count() FROM `database-1`.`t-d1`")
	env.checkCount(r, 1, 20, "SELECT count() FROM `database-1`.mv1")
	env.checkCount(r, 1, 20, "SELECT count() FROM `database-1`.v1")

	log.Debug().Msg("Drop database-1")
	r.NoError(env.dropDatabase("database-1", false))

	log.Debug().Msg("Restore data only --restore-database-mappings")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--rm", "--restore-database-mapping", "database-1:database-2", testBackupName)

	log.Debug().Msg("Check result database-2 without table mapping")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.11") >= 0 {
		env.checkCount(r, 1, 1, "SELECT count() FROM system.databases WHERE name='database-2' AND engine='"+expectedDbEngine+"'")
	}
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.t1")
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.t2")
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.`t-d1`")
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.mv1")
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.v1")

	log.Debug().Msg("Restore data --restore-table-mappings both with --restore-database-mappings")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--data", "--restore-database-mapping", "database-1:database-2", "--restore-table-mapping", "t1:t3,t2:t4,t-d1:t-d2,mv1:mv2,v1:v2", "--tables", "database-1.*", testBackupName)

	log.Debug().Msg("Check result database-2")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.11") >= 0 {
		env.checkCount(r, 1, 1, "SELECT count() FROM system.databases WHERE name='database-2' AND engine='"+expectedDbEngine+"'")
	}
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.t3")
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.t4")
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.`t-d2`")
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.mv2")
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.v2")

	log.Debug().Msg("Check database-1 not exists")
	env.checkCount(r, 1, 0, "SELECT count() FROM system.databases WHERE name='database-1' SETTINGS empty_result_for_aggregation_by_empty_set=0")

	log.Debug().Msg("Drop database2")
	r.NoError(env.dropDatabase("database-2", false))

	log.Debug().Msg("Restore data with partitions")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--restore-database-mapping", "database-1:database-2", "--restore-table-mapping", "t1:t3,t2:t4,t-d1:t-d2,mv1:mv2,v1:v2", "--partitions", "3", "--partitions", "database-1.t2:202201", "--tables", "database-1.*", testBackupName)

	log.Debug().Msg("Check result database-2 after restore with partitions")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.11") >= 0 {
		env.checkCount(r, 1, 1, "SELECT count() FROM system.databases WHERE name='database-2' AND engine='"+expectedDbEngine+"'")
	}
	// t1->t3 restored only 1 partition with name 3 partition with 1 rows
	env.checkCount(r, 1, 1, "SELECT count() FROM `database-2`.t3")
	// t2->t4 restored only 1 partition with name 3 partition with 10 rows
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.t4")
	env.checkCount(r, 1, 1, "SELECT count() FROM `database-2`.`t-d2`")
	env.checkCount(r, 1, 10, "SELECT count() FROM `database-2`.mv2")
	env.checkCount(r, 1, 1, "SELECT count() FROM `database-2`.v2")

	fullCleanup(t, r, env, []string{testBackupName}, []string{"local"}, databaseList, false, true, true, "config-database-mapping.yml")

	// Corner case 1: Table-only mapping without database mapping
	log.Debug().Msg("Corner case 1: Table-only mapping without database mapping")
	testBackupName2 := "test_table_only_mapping"
	databaseList2 := []string{"database-3"}
	fullCleanup(t, r, env, []string{testBackupName2}, []string{"local"}, databaseList2, false, false, false, "config-database-mapping.yml")

	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `database-3`")
	env.queryWithNoError(r, "CREATE TABLE `database-3`.src_table (dt DateTime, v UInt64) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY dt")
	env.queryWithNoError(r, "INSERT INTO `database-3`.src_table SELECT '2022-01-01 00:00:00', number FROM numbers(5)")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "create", testBackupName2)

	log.Debug().Msg("Restore with table-only mapping (src_table -> dst_table)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--schema", "--rm", "--restore-table-mapping", "src_table:dst_table", "--tables", "database-3.src_table", testBackupName2)

	env.checkCount(r, 1, 1, "SELECT count() FROM system.tables WHERE database='database-3' AND name='dst_table'")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--data", "--restore-table-mapping", "src_table:dst_table", "--tables", "database-3.src_table", testBackupName2)
	env.checkCount(r, 1, 5, "SELECT count() FROM `database-3`.dst_table")

	fullCleanup(t, r, env, []string{testBackupName2}, []string{"local"}, databaseList2, false, true, true, "config-database-mapping.yml")

	// Corner case 2: Multiple databases with comma-separated patterns
	log.Debug().Msg("Corner case 2: Multiple databases with comma-separated table patterns")
	testBackupName3 := "test_multi_db_mapping"
	databaseList3 := []string{"db_a", "db_b", "db_c", "db_d"}
	fullCleanup(t, r, env, []string{testBackupName3}, []string{"local"}, databaseList3, false, false, false, "config-database-mapping.yml")

	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `db_a`")
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `db_b`")
	env.queryWithNoError(r, "CREATE TABLE `db_a`.t1 (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "CREATE TABLE `db_b`.t1 (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "INSERT INTO `db_a`.t1 SELECT number FROM numbers(3)")
	env.queryWithNoError(r, "INSERT INTO `db_b`.t1 SELECT number FROM numbers(7)")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "create", testBackupName3)

	log.Debug().Msg("Drop source databases before restore to simulate migration")
	r.NoError(env.dropDatabase("db_a", false))
	r.NoError(env.dropDatabase("db_b", false))

	log.Debug().Msg("Restore with multiple database mappings (db_a->db_c, db_b->db_d)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--rm", "--restore-database-mapping", "db_a:db_c,db_b:db_d", "--tables", "db_a.*,db_b.*", testBackupName3)

	env.checkCount(r, 1, 3, "SELECT count() FROM `db_c`.t1")
	env.checkCount(r, 1, 7, "SELECT count() FROM `db_d`.t1")
	env.checkCount(r, 1, 0, "SELECT count() FROM system.databases WHERE name IN ('db_a','db_b') SETTINGS empty_result_for_aggregation_by_empty_set=0")

	fullCleanup(t, r, env, []string{testBackupName3}, []string{"local"}, databaseList3, false, true, true, "config-database-mapping.yml")

	// Corner case 3: Combined database and table mapping with comma-separated patterns
	log.Debug().Msg("Corner case 3: Combined database and table mapping with comma-separated patterns")
	testBackupName4 := "test_combined_mapping"
	databaseList4 := []string{"src_db1", "src_db2", "dst_db1", "dst_db2"}
	fullCleanup(t, r, env, []string{testBackupName4}, []string{"local"}, databaseList4, false, false, false, "config-database-mapping.yml")

	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `src_db1`")
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `src_db2`")
	env.queryWithNoError(r, "CREATE TABLE `src_db1`.old_name (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "CREATE TABLE `src_db2`.old_name (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "INSERT INTO `src_db1`.old_name SELECT number FROM numbers(4)")
	env.queryWithNoError(r, "INSERT INTO `src_db2`.old_name SELECT number + 100 FROM numbers(6)")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "create", testBackupName4)

	log.Debug().Msg("Restore with database mapping and table mapping on comma-separated patterns")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--rm", "--restore-database-mapping", "src_db1:dst_db1,src_db2:dst_db2", "--restore-table-mapping", "old_name:new_name", "--tables", "src_db1.old_name,src_db2.old_name", testBackupName4)

	env.checkCount(r, 1, 4, "SELECT count() FROM `dst_db1`.new_name")
	env.checkCount(r, 1, 6, "SELECT count() FROM `dst_db2`.new_name")
	// Verify data integrity - dst_db2 should have values starting from 100
	env.checkCount(r, 1, 6, "SELECT count() FROM `dst_db2`.new_name WHERE id >= 100")

	fullCleanup(t, r, env, []string{testBackupName4}, []string{"local"}, databaseList4, false, true, true, "config-database-mapping.yml")

	// Corner case 4: Table mapping with same table name in different databases
	log.Debug().Msg("Corner case 4: Table mapping for same table name across databases")
	testBackupName5 := "test_same_table_diff_db"
	databaseList5 := []string{"alpha", "beta"}
	fullCleanup(t, r, env, []string{testBackupName5}, []string{"local"}, databaseList5, false, false, false, "config-database-mapping.yml")

	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `alpha`")
	env.queryWithNoError(r, "CREATE TABLE `alpha`.common (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "CREATE TABLE `alpha`.unique_a (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "INSERT INTO `alpha`.common SELECT number FROM numbers(2)")
	env.queryWithNoError(r, "INSERT INTO `alpha`.unique_a SELECT number FROM numbers(3)")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "create", testBackupName5)

	log.Debug().Msg("Drop source database before restore to simulate migration")
	r.NoError(env.dropDatabase("alpha", false))

	log.Debug().Msg("Restore to different database with selective table mapping")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--rm", "--restore-database-mapping", "alpha:beta", "--restore-table-mapping", "common:shared,unique_a:unique_b", "--tables", "alpha.*", testBackupName5)

	env.checkCount(r, 1, 2, "SELECT count() FROM `beta`.shared")
	env.checkCount(r, 1, 3, "SELECT count() FROM `beta`.unique_b")
	env.checkCount(r, 1, 0, "SELECT count() FROM system.databases WHERE name='alpha' SETTINGS empty_result_for_aggregation_by_empty_set=0")

	fullCleanup(t, r, env, []string{testBackupName5}, []string{"local"}, databaseList5, false, true, true, "config-database-mapping.yml")

	// Corner case 5: Full qualified table mapping (db.table:db.table_v2, src_db.table:dst_db.table_v2) - verify DROP uses target table name
	// https://github.com/Altinity/clickhouse-backup/issues/1302
	log.Debug().Msg("Corner case 5: Full qualified table mapping with --schema restore")
	testBackupName6 := "test_fq_table_mapping"
	databaseList6 := []string{"db-5", "source_db", "target_db"}
	fullCleanup(t, r, env, []string{testBackupName6}, []string{"local"}, databaseList6, false, false, false, "config-database-mapping.yml")

	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `source_db`")
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `target_db`")
	env.queryWithNoError(r, "CREATE TABLE `source_db`.original_table (dt DateTime, v UInt64) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY dt")
	env.queryWithNoError(r, "INSERT INTO `source_db`.original_table SELECT '2022-01-01 00:00:00', number FROM numbers(5)")
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `db-5`")
	env.queryWithNoError(r, "CREATE TABLE `db-5`.table (dt DateTime, v UInt64) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY dt")
	env.queryWithNoError(r, "INSERT INTO `db-5`.table SELECT '2022-01-01 00:00:00', number FROM numbers(5)")
	// Create target table to verify DROP operates on correct table
	env.queryWithNoError(r, "CREATE TABLE `target_db`.renamed_table_v2 (dt DateTime, v UInt64) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY dt")
	env.queryWithNoError(r, "INSERT INTO `target_db`.renamed_table_v2 SELECT '2023-01-01 00:00:00', number FROM numbers(3)")
	env.queryWithNoError(r, "CREATE TABLE `db-5`.table_v2 (dt DateTime, v UInt64) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY dt")
	env.queryWithNoError(r, "INSERT INTO `db-5`.table_v2 SELECT '2023-01-01 00:00:00', number FROM numbers(3)")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "create", testBackupName6)

	restoreFqMappingCases := []struct {
		srcDb    string
		srcTable string
		dstDb    string
		dstTable string
	}{
		{srcDb: "db-5", srcTable: "table", dstDb: "db-5", dstTable: "table_v2"},
		{srcDb: "source_db", srcTable: "original_table", dstDb: "target_db", dstTable: "renamed_table_v2"},
	}

	for _, tc := range restoreFqMappingCases {
		tableMapping := fmt.Sprintf("%s.%s:%s.%s", tc.srcDb, tc.srcTable, tc.dstDb, tc.dstTable)
		log.Debug().Msgf("Restore with full qualified table mapping %s", tableMapping)
		out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--schema", "--rm", "--restore-table-mapping", tableMapping, "--tables", tc.srcDb+"."+tc.srcTable, testBackupName6)
		log.Debug().Msg(out)
		r.NoError(err)

		// Verify DROP used target table name, not source table name
		r.Contains(out, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", tc.dstDb, tc.dstTable), "DROP should use target table name from mapping")
		r.NotContains(out, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", tc.srcDb, tc.srcTable), "DROP should NOT use source table name")

		// Verify table was created in target location
		env.checkCount(r, 1, 1, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s' AND name='%s'", tc.dstDb, tc.dstTable))

		// Restore data and verify
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--data", "--restore-table-mapping", tableMapping, "--tables", tc.srcDb+"."+tc.srcTable, testBackupName6)
		env.checkCount(r, 1, 5, fmt.Sprintf("SELECT count() FROM `%s`.`%s`", tc.dstDb, tc.dstTable))
	}

	fullCleanup(t, r, env, []string{testBackupName6}, []string{"local"}, databaseList6, false, true, true, "config-database-mapping.yml")

	// Corner case 6: Object disk tables with mapping - verify key rewriting
	// https://github.com/Altinity/clickhouse-backup/issues/1265
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 && (os.Getenv("COMPOSE_FILE") != "docker-compose.yml") {
		log.Debug().Msg("Corner case 6: Object disk tables with restore mapping - verify object keys are rewritten")
		testBackupName7 := "test_object_disk_mapping"
		databaseList7 := []string{"db_object_src", "db_object_dst"}
		fullCleanup(t, r, env, []string{testBackupName7}, []string{"local", "remote"}, databaseList7, false, false, false, "config-s3.yml")

		env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `db_object_src`")
		env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS `db_object_dst`")

		// Create table with object disk (S3)
		env.queryWithNoError(r, "CREATE TABLE `db_object_src`.table_s3 (id UInt64, dt DateTime) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id SETTINGS storage_policy='s3_only'")
		env.queryWithNoError(r, "INSERT INTO `db_object_src`.table_s3 SELECT number, '2024-01-01 00:00:00' FROM numbers(100)")

		// Create target table that already exists (simulating the conflict scenario)
		env.queryWithNoError(r, "CREATE TABLE `db_object_dst`.table_s3_mapped (id UInt64, dt DateTime) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id SETTINGS storage_policy='s3_only'")
		env.queryWithNoError(r, "INSERT INTO `db_object_dst`.table_s3_mapped SELECT number+1000, '2024-02-01 00:00:00' FROM numbers(50)")

		// Create backup
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", testBackupName7)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "upload", testBackupName7)

		// Restore with mapping - should trigger key rewriting
		out, restoreErr := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm",
			"--restore-database-mapping", "db_object_src:db_object_dst",
			"--restore-table-mapping", "table_s3:table_s3_mapped",
			"--tables", "db_object_src.table_s3", testBackupName7)
		log.Debug().Msg(out)
		r.NoError(restoreErr, out)

		// Verify warning message about key rewriting
		r.Contains(out, "we found existing table `db_object_dst`.`table_s3_mapped` with object disk data")
		r.Contains(out, "so restoring object disk data keys for `db_object_src`.`table_s3` will be changed to avoid data corruption")

		// Verify both tables exist and have correct data
		var dstCount uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&dstCount, "SELECT count() FROM `db_object_dst`.table_s3_mapped"))
		r.Equal(uint64(100), dstCount, "Restored table should have 100 rows from backup")

		// Verify original target table data is preserved (no corruption)
		// Drop the restored table and check if we can still read the original data
		env.queryWithNoError(r, "DROP TABLE `db_object_dst`.table_s3_mapped SYNC")

		// Recreate original target table and verify its object keys are intact
		env.queryWithNoError(r, "CREATE TABLE `db_object_dst`.table_s3_original (id UInt64, dt DateTime) ENGINE=MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id SETTINGS storage_policy='s3_only'")
		env.queryWithNoError(r, "INSERT INTO `db_object_dst`.table_s3_original SELECT number+2000, '2024-03-01 00:00:00' FROM numbers(75)")

		// This insert should work - verifying object storage is not corrupted
		var srcCount uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&srcCount, "SELECT count() FROM `db_object_dst`.table_s3_original"))
		r.Equal(uint64(75), srcCount, "New table should work with object disk after previous operations")

		// Clean up
		fullCleanup(t, r, env, []string{testBackupName7}, []string{"local", "remote"}, databaseList7, false, true, true, "config-s3.yml")
	}

	env.Cleanup(t, r)
}
