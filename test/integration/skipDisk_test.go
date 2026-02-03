//go:build integration

package main

import (
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestSkipDisk(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "remote")
	// Skip test if running in simple environment without storage policies
	if os.Getenv("COMPOSE_FILE") == "docker-compose.yml" {
		t.Skip("Skipping test in simple environment without storage policies")
	}

	// Setup test environment
	setupTestSkipDisks(r, env)

	// Test skipping disk by name during create
	testSkipDiskByNameCreate(r, env)

	// Test skipping disk by type during create
	testSkipDiskByTypeCreate(r, env)

	// Test skipping disk by name during upload
	testSkipDiskByNameUpload(r, env)

	// Test skipping disk by type during upload
	testSkipDiskByTypeUpload(r, env)

	// Test full upload without skipping
	testFullUpload(r, env)

	// Test skipping disks during download
	testSkipDiskDownload(r, env)

	// Test skipping disks during restore
	testRestoreSkipDisk(t, r, env)

	// Clean up
	r.NoError(env.dropDatabase("test_skip_disks", false))
	env.Cleanup(t, r)
}

// setupTestSkipDisks creates test database and tables on different disks
func setupTestSkipDisks(r *require.Assertions, env *TestEnvironment) {
	// Create test database and tables on different disks
	env.queryWithNoError(r, "CREATE DATABASE test_skip_disks")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_skip_disks.table_default (id UInt64) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_skip_disks.table_hdd1 (id UInt64) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy = 'hdd1_only'")

	// Create tables on S3 disk if available in this version
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_skip_disks.table_s3 (id UInt64) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy = 's3_only'")
	}

	// Insert some data
	env.queryWithNoError(r, "INSERT INTO test_skip_disks.table_default SELECT number FROM numbers(10)")
	env.queryWithNoError(r, "INSERT INTO test_skip_disks.table_hdd1 SELECT number FROM numbers(10)")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		env.queryWithNoError(r, "INSERT INTO test_skip_disks.table_s3 SELECT number FROM numbers(10)")
	}
}

// testSkipDiskByNameCreate tests skipping disk by name during create
func testSkipDiskByNameCreate(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Testing skip disk by name during create")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_DISKS=hdd1 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create skip_disk_by_name")

	// Metadata exists for all tables
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_disk_by_name/metadata/test_skip_disks/table_default.json")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_disk_by_name/metadata/test_skip_disks/table_hdd1.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_disk_by_name/metadata/test_skip_disks/table_s3.json")
	}

	//data exists for default and s3 disk
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_disk_by_name/shadow/test_skip_disks/table_default/")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/disks/disk_s3/backup/skip_disk_by_name/shadow/test_skip_disks/table_s3/")
	}

	// Check that tables on hdd1 disk are not backed up
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/hdd1_data/backup/skip_disk_by_name/shadow/test_skip_disks/table_hdd1/"))

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_disk_by_name")
}

// testSkipDiskByTypeCreate tests skipping disk by type during create
func testSkipDiskByTypeCreate(r *require.Assertions, env *TestEnvironment) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		log.Debug().Msg("Testing skip disk by type during create")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_DISK_TYPES=s3 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create skip_disk_by_type")

		// Check data tables on s3 disk are not backed up
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_disk_by_type/metadata/test_skip_disks/table_default.json")
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_disk_by_type/metadata/test_skip_disks/table_hdd1.json")
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_disk_by_type/metadata/test_skip_disks/table_s3.json")
		r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/disks/disk_s3/backup/skip_disk_by_type/shadow/test_skip_disks/table_s3/"))

		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_disk_by_type")
	}
}

// testSkipDiskByNameUpload tests skipping disk by name during upload
func testSkipDiskByNameUpload(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Testing skip disk by name during upload")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create skip_disk_upload_test")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_DISKS=hdd1 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload skip_disk_upload_test")

	// Check that tables on hdd1 disk are not uploaded to minio
	out, err := env.DockerExecOut("minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/skip_disk_upload_test/shadow/test_skip_disks/table_hdd1/hdd1*")
	r.Error(err, out)
	env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/skip_disk_upload_test/shadow/test_skip_disks/table_default/")

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/skip_disk_upload_test/shadow/test_skip_disks/table_s3/")
	}

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_disk_upload_test")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote skip_disk_upload_test")
}

// testSkipDiskByTypeUpload tests skipping disk by type during upload
func testSkipDiskByTypeUpload(r *require.Assertions, env *TestEnvironment) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		log.Debug().Msg("Testing skip disk by type during upload")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create skip_disk_type_upload_test")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_DISK_TYPES=s3 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload skip_disk_type_upload_test")

		// Check that tables on s3 disk are not uploaded to minio
		r.Error(env.DockerExec("minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/skip_disk_type_upload_test/shadow/test_skip_disks/table_s3/"))
		env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/skip_disk_type_upload_test/shadow/test_skip_disks/table_default/")
		env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/skip_disk_type_upload_test/shadow/test_skip_disks/table_hdd1/")

		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_disk_type_upload_test")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote skip_disk_type_upload_test")
	}
}

// testFullUpload tests full upload without skipping any disks
func testFullUpload(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Testing full upload without skipping")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create_remote full_upload_test")

	// Check that all tables are uploaded to minio
	env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/full_upload_test/shadow/test_skip_disks/table_default/")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/full_upload_test/shadow/test_skip_disks/table_hdd1/")

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/full_upload_test/shadow/test_skip_disks/table_s3/")
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local full_upload_test")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote full_upload_test")
}

// testSkipDiskDownload tests skipping disks during download operations
func testSkipDiskDownload(r *require.Assertions, env *TestEnvironment) {
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create_remote --delete-source skip_download_test")
	// Test skipping disk by name during download
	log.Debug().Msg("Testing skip disk by name during download")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_DISKS=hdd1 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download skip_download_test")

	// Check that tables on hdd1 disk are not downloaded
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_download_test/shadow/test_skip_disks/table_default/")
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/hdd1_data/backup/skip_download_test/shadow/test_skip_disks/table_hdd1/"))

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/disks/disk_s3/backup/skip_download_test/shadow/test_skip_disks/table_s3/")

		// Test skipping disk by type during download
		log.Debug().Msg("Testing skip disk by type during download")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_download_test")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_DISK_TYPES=s3 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download skip_download_test")

		// Check that tables on s3 disk are not downloaded
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_download_test/shadow/test_skip_disks/table_default/")
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/hdd1_data/backup/skip_download_test/shadow/test_skip_disks/table_hdd1/")
		r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/disks/disk_s3/backup/skip_download_test/shadow/test_skip_disks/table_s3/"))
	}

	// Test full download without skipping
	log.Debug().Msg("Testing full download without skipping")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_download_test")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download skip_download_test")

	// Check that all tables are downloaded
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_download_test/shadow/test_skip_disks/table_default/")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/hdd1_data/backup/skip_download_test/shadow/test_skip_disks/table_hdd1/")

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/disks/disk_s3/backup/skip_download_test/shadow/test_skip_disks/table_s3/")
	}

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_download_test")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote skip_download_test")
}

// testRestoreSkipDisk tests skipping disks during restore operations
func testRestoreSkipDisk(t *testing.T, r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Testing skip disk during restore")

	// Create a backup with all tables
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create_remote --delete-source skip_restore_test")

	// Download the backup
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download skip_restore_test")

	// Drop the test database to prepare for restore
	r.NoError(env.dropDatabase("test_skip_disks", false))

	// Test skipping disk by name during restore
	log.Debug().Msg("Testing skip disk by name during restore")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_DISKS=hdd1 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore skip_restore_test")
	// Check that tables on default disk are restored
	var tableDefaultCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&tableDefaultCount, "SELECT count() FROM test_skip_disks.table_default"))
	r.Equal(uint64(10), tableDefaultCount, "table_default should have 10 rows")

	// Check that tables on hdd1 disk restored, but  have no data (should not exist in system.parts)
	var tableHdd1Exists uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&tableHdd1Exists, "SELECT count() FROM system.tables WHERE database='test_skip_disks' AND name='table_hdd1'"))
	r.Equal(uint64(1), tableHdd1Exists, "table_hdd1 shall exist in system.tables")
	tableHdd1Exists = 0
	r.NoError(env.ch.SelectSingleRowNoCtx(&tableHdd1Exists, "SELECT count() FROM system.parts WHERE active AND database='test_skip_disks' AND table='table_hdd1' AND disk_name='hdd1'"))
	if tableHdd1Exists != 0 {
		type hdd1Parts = struct {
			Name     string `ch:"name"`
			DiskName string `ch:"disk_name"`
		}
		parts := make([]hdd1Parts, 0)
		r.NoError(env.ch.SelectContext(t.Context(), &parts, "SELECT name, disk_name FROM system.parts WHERE active AND database='test_skip_disks' AND table='table_hdd1'"))
		t.Errorf("unexpected table_hdd1 in system.parts=%#v", parts)
	}
	r.Equal(uint64(0), tableHdd1Exists, "unexpected table_hdd1 in system.parts")

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		// Check that tables on s3 disk are restored
		var tableS3Count uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&tableS3Count, "SELECT count() FROM test_skip_disks.table_s3"))
		r.Equal(uint64(10), tableS3Count, "table_s3 should have 10 rows")

		// Drop the test database to prepare for next test
		r.NoError(env.dropDatabase("test_skip_disks", false))

		// Test skipping disk by type during restore
		log.Debug().Msg("Testing skip disk by type during restore")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_DISK_TYPES=s3 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore skip_restore_test")

		// Check that tables on default disk are restored
		r.NoError(env.ch.SelectSingleRowNoCtx(&tableDefaultCount, "SELECT count() FROM test_skip_disks.table_default"))
		r.Equal(uint64(10), tableDefaultCount, "table_default should have 10 rows")

		// Check that tables on hdd1 disk are restored
		var tableHdd1Count uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&tableHdd1Count, "SELECT count() FROM test_skip_disks.table_hdd1"))
		r.Equal(uint64(10), tableHdd1Count, "table_hdd1 should have 10 rows")

		// Check that tables on s3 disk restored but not contains data (should not exist in system.parts)
		var tableS3Exists uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&tableS3Exists, "SELECT count() FROM system.tables WHERE database='test_skip_disks' AND name='table_s3'"))
		r.Equal(uint64(1), tableS3Exists, "table_s3 shall exists in system.tables")
		r.NoError(env.ch.SelectSingleRowNoCtx(&tableS3Exists, "SELECT count() FROM system.parts WHERE active AND disk_name='disk_s3' AND database='test_skip_disks' AND name='table_s3'"))
		r.Equal(uint64(0), tableS3Exists, "table_s3 shall not exists in system.parts")
	}

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_restore_test")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote skip_restore_test")
}
