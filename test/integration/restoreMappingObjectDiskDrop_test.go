//go:build integration

package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestRestoreMappingObjectDiskDropDst reproduces https://github.com/Altinity/clickhouse-backup/issues/1568
// source table stays alive, backup is restored next to it with --restore-database-mapping,
// then the mapped copy is dropped with SYNC and the source must still be readable from object storage
func TestRestoreMappingObjectDiskDropDst(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.8") < 0 || !isAdvancedMode() {
		t.Skipf("requires ClickHouse >= 22.8 for `type: cache`, current %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	env.DockerExecNoError(r, "clickhouse", "bash", "-xc", `
cat > /etc/clickhouse-server/config.d/zz_issue_1568.xml <<'XML'
<clickhouse>
  <storage_configuration>
    <disks>
      <s3_1568>
        <type>s3</type>
        <endpoint>https://minio:9000/clickhouse/issue_1568/</endpoint>
        <access_key_id>access_key</access_key_id>
        <secret_access_key>it_is_my_super_secret_key</secret_access_key>
        <skip_access_check>true</skip_access_check>
      </s3_1568>
      <s3_1568_cache>
        <type>cache</type>
        <disk>s3_1568</disk>
        <path>/var/lib/clickhouse/filesystem_caches/s3_1568_cache/</path>
        <max_size>67108864</max_size>
      </s3_1568_cache>
    </disks>
    <policies>
      <hot_cold_1568>
        <volumes>
          <hot><disk>default</disk></hot>
          <cold><disk>s3_1568_cache</disk></cold>
        </volumes>
        <move_factor>0</move_factor>
      </hot_cold_1568>
    </policies>
  </storage_configuration>
</clickhouse>
XML
`)
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)

	scenarios := []struct {
		name           string
		policy         string
		moveToCold     bool
		expectDiskLike string
		backupName     string
		// restoreEnv overrides clickhouse-backup settings for the restore only, "" to keep the config
		restoreEnv string
		// expectRestoreErr - restore shall fail, but the mapped copy shall still not share object keys with the source
		expectRestoreErr bool
		// restoreTwice - restore the same local backup a second time into another database
		restoreTwice bool
	}{
		{name: "s3_only", policy: "s3_only", moveToCold: false, expectDiskLike: "disk_s3", backupName: "issue_1568_s3_only"},
		{name: "cache_tiered", policy: "hot_cold_1568", moveToCold: true, expectDiskLike: "s3_1568%", backupName: "issue_1568_cache_tiered", restoreTwice: true},
		// backup name is a substring of the object disk key prefix `issue_1568/`
		{name: "backup_name_in_key_prefix", policy: "hot_cold_1568", moveToCold: true, expectDiskLike: "s3_1568%", backupName: "issue_1568"},
		// object copy fails after the parts are already linked to `detached`, emulates an interrupted restore
		{name: "interrupted_restore", policy: "s3_only", moveToCold: false, expectDiskLike: "disk_s3", backupName: "issue_1568_interrupted", restoreEnv: "S3_OBJECT_DISK_PATH=object_disk/wrong_1568", expectRestoreErr: true},
	}
	for _, sc := range scenarios {
		log.Debug().Msgf("scenario %s", sc.name)
		func() {
			backupName := sc.backupName
			srcDb := "source_1568_" + sc.name
			dstDb := "completed_1568_" + sc.name
			fullCleanup(t, r, env, []string{backupName}, []string{"local", "remote"}, []string{srcDb, dstDb}, false, false, false, "config-s3.yml")

			env.queryWithNoError(t, r, "CREATE DATABASE "+srcDb)
			env.queryWithNoError(t, r, fmt.Sprintf("CREATE TABLE %s.canary (point_id UInt64, payload String) ENGINE=MergeTree ORDER BY point_id SETTINGS storage_policy='%s'", srcDb, sc.policy))
			env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.canary SELECT number, randomString(64) FROM numbers(10000)", srcDb))
			env.queryWithNoError(t, r, fmt.Sprintf("OPTIMIZE TABLE %s.canary FINAL", srcDb))
			if sc.moveToCold {
				env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.canary MOVE PARTITION tuple() TO VOLUME 'cold'", srcDb))
			}
			env.checkCount(r, 1, 1, fmt.Sprintf("SELECT count() FROM system.parts WHERE database='%s' AND table='canary' AND active AND disk_name LIKE '%s'", srcDb, sc.expectDiskLike))

			var baseline uint64
			checksumSQL := "SELECT sum(cityHash64(*)) FROM %s.canary SETTINGS enable_filesystem_cache=0"
			r.NoError(env.ch.SelectSingleRowNoCtx(&baseline, fmt.Sprintf(checksumSQL, srcDb)))

			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+srcDb+".canary", backupName)
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "upload", backupName)
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", backupName)

			restoreCmd := []string{"clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--tables=" + srcDb + ".canary", "--restore-database-mapping=" + srcDb + ":" + dstDb, backupName}
			if sc.restoreEnv != "" {
				restoreCmd = append([]string{"env", sc.restoreEnv}, restoreCmd...)
			}
			out, err := env.DockerExecOut("clickhouse-backup", restoreCmd...)
			log.Debug().Msg(out)
			var dstSum, srcSum uint64
			if sc.expectRestoreErr {
				r.Error(err, out)
				env.checkCount(r, 1, 1, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s' AND name='canary'", dstDb))
			} else {
				r.NoError(err, out)
				r.NoError(env.ch.SelectSingleRowNoCtx(&dstSum, fmt.Sprintf(checksumSQL, dstDb)))
				r.Equal(baseline, dstSum, "restored copy shall match baseline")
			}
			r.NoError(env.ch.SelectSingleRowNoCtx(&srcSum, fmt.Sprintf(checksumSQL, srcDb)))
			r.Equal(baseline, srcSum, "source shall match baseline before dropping the copy")

			if sc.restoreTwice {
				dstDb2 := dstDb + "_2"
				out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--tables="+srcDb+".canary", "--restore-database-mapping="+srcDb+":"+dstDb2, backupName)
				log.Debug().Msg(out)
				r.NoError(err, out)
				r.NoError(env.ch.SelectSingleRowNoCtx(&dstSum, fmt.Sprintf(checksumSQL, dstDb2)))
				r.Equal(baseline, dstSum, "second restored copy shall match baseline")
				env.queryWithNoError(t, r, "DROP DATABASE "+dstDb2+" SYNC")
				r.NoError(env.ch.SelectSingleRowNoCtx(&dstSum, fmt.Sprintf(checksumSQL, dstDb)))
				r.Equal(baseline, dstSum, "first restored copy shall stay readable after dropping the second one")
			}

			env.queryWithNoError(t, r, "DROP DATABASE "+dstDb+" SYNC")

			srcSum = 0
			r.NoError(env.ch.SelectSingleRowNoCtx(&srcSum, fmt.Sprintf(checksumSQL, srcDb)), "source shall stay readable after dropping the mapped copy")
			r.Equal(baseline, srcSum, "source shall match baseline after dropping the mapped copy")

			fullCleanup(t, r, env, []string{backupName}, []string{"local", "remote"}, []string{srcDb, dstDb}, false, true, true, "config-s3.yml")
		}()
	}

	env.DockerExecNoError(r, "clickhouse", "rm", "-f", "/etc/clickhouse-server/config.d/zz_issue_1568.xml")
	env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/issue_1568")
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
}
