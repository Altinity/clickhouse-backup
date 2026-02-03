//go:build integration

package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestConfigs - require direct access to `/etc/clickhouse-backup/`, so executed inside `clickhouse` container
// need clickhouse-server restart, no parallel
func TestConfigs(t *testing.T) {
	env, r := NewTestEnvironment(t)
	testConfigsScenario := func(config string) {
		env.connectWithWait(t, r, 0*time.Millisecond, 1*time.Second, 1*time.Minute)
		env.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_configs")
		env.queryWithNoError(r, "CREATE TABLE default.test_configs (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")

		env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "echo '<yandex><profiles><default><empty_result_for_aggregation_by_empty_set>1</empty_result_for_aggregation_by_empty_set></default></profiles></yandex>' > /etc/clickhouse-server/users.d/test_config.xml")

		env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", config, "create", "--configs", "--configs-only", "test_configs_backup")
		env.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_configs")
		compression := ""
		if !strings.Contains(config, "embedded") {
			compression = "--env AZBLOB_COMPRESSION_FORMAT=zstd --env S3_COMPRESSION_FORMAT=zstd"
		}
		env.DockerExecNoError(r, "clickhouse", "bash", "-xec", "clickhouse-backup upload "+compression+" --env CLICKHOUSE_BACKUP_CONFIG="+config+" --env S3_COMPRESSION_FORMAT=none --env ALLOW_EMPTY_BACKUPS=1 test_configs_backup")
		env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", config, "delete", "local", "test_configs_backup")

		env.queryWithNoError(r, "SYSTEM RELOAD CONFIG")
		env.ch.Close()
		env.connectWithWait(t, r, 1*time.Second, 1*time.Second, 1*time.Minute)
		selectEmptyResultForAggQuery := "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"
		var settings string
		r.NoError(env.ch.SelectSingleRowNoCtx(&settings, selectEmptyResultForAggQuery))
		if settings != "1" {
			env.DockerExecNoError(r, "clickhouse", "grep", "empty_result_for_aggregation_by_empty_set", "-r", "/var/lib/clickhouse/preprocessed_configs/")
		}
		r.Equal("1", settings, "expect empty_result_for_aggregation_by_empty_set=1")

		env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml")
		env.DockerExecNoError(r, "clickhouse", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG="+config+" ALLOW_EMPTY_BACKUPS=1 clickhouse-backup download test_configs_backup")

		r.NoError(env.ch.Query("SYSTEM RELOAD CONFIG"))
		env.ch.Close()
		env.connectWithWait(t, r, 1*time.Second, 1*time.Second, 1*time.Minute)

		settings = ""
		r.NoError(env.ch.SelectSingleRowNoCtx(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
		r.Equal("0", settings, "expect empty_result_for_aggregation_by_empty_set=0")

		env.DockerExecNoError(r, "clickhouse", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG="+config+" CLICKHOUSE_RESTART_COMMAND='sql:SYSTEM RELOAD CONFIG' clickhouse-backup restore --rm --configs --configs-only test_configs_backup")

		env.ch.Close()
		env.connectWithWait(t, r, 1*time.Second, 1*time.Second, 1*time.Second)

		settings = ""
		r.NoError(env.ch.SelectSingleRowNoCtx(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
		r.Equal("1", settings, "expect empty_result_for_aggregation_by_empty_set=1")

		isTestConfigsTablePresent := 0
		r.NoError(env.ch.SelectSingleRowNoCtx(&isTestConfigsTablePresent, "SELECT count() FROM system.tables WHERE database='default' AND name='test_configs' SETTINGS empty_result_for_aggregation_by_empty_set=1"))
		r.Equal(0, isTestConfigsTablePresent, "expect default.test_configs is not present")

		env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", config, "delete", "local", "test_configs_backup")
		env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", config, "delete", "remote", "test_configs_backup")
		env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml")

		env.ch.Close()
	}
	testConfigsScenario("/etc/clickhouse-backup/config-s3.yml")
	chVersion := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(chVersion, "24.1") >= 0 {
		testConfigsScenario("/etc/clickhouse-backup/config-s3-embedded.yml")
		testConfigsScenario("/etc/clickhouse-backup/config-s3-embedded-url.yml")
		testConfigsScenario("/etc/clickhouse-backup/config-azblob-embedded.yml")
	}
	if compareVersion(chVersion, "24.2") >= 0 {
		testConfigsScenario("/etc/clickhouse-backup/config-azblob-embedded-url.yml")
	}
	env.Cleanup(t, r)
}
