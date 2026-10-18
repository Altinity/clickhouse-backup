//go:build integration

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestAzureWorkloadIdentityDisk - every endpoint form of azure disks which clickhouse-server accepts
// (<storage_account_url>, <connection_string>, <endpoint>) is backed up and restored, and an azure disk
// without <account_key> (<use_workload_identity>) doesn't fail the backup, https://github.com/Altinity/clickhouse-backup/issues/1604
func TestAzureWorkloadIdentityDisk(t *testing.T) {
	if isTestShouldSkip("AZURE_TESTS") {
		t.Skip("Skipping Azure integration tests...")
		return
	}
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "24.8") < 0 {
		t.Skipf("Test requires ClickHouse >= 24.8 for `<type>object_storage</type>` azure disks, current version %s", version)
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	const configFile = "/etc/clickhouse-server/config.d/zz_azure_workload_identity_1604.xml"
	env.DockerExecNoError(r, "clickhouse", "bash", "-xc", `
cat > `+configFile+` <<'XML'
<clickhouse>
  <storage_configuration>
    <disks>
      <azure_account_key_1604>
        <type>object_storage</type>
        <object_storage_type>azure_blob_storage</object_storage_type>
        <metadata_type>local</metadata_type>
        <storage_account_url>http://azure:10000/devstoreaccount1</storage_account_url>
        <container_name>azure-account-key-1604</container_name>
        <account_name>devstoreaccount1</account_name>
        <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
      </azure_account_key_1604>
      <azure_workload_identity_1604>
        <type>object_storage</type>
        <object_storage_type>azure_blob_storage</object_storage_type>
        <metadata_type>local</metadata_type>
        <storage_account_url>http://azure:10000/devstoreaccount1</storage_account_url>
        <container_name>azure-workload-identity-1604</container_name>
        <account_name>devstoreaccount1</account_name>
        <use_workload_identity>true</use_workload_identity>
        <container_already_exists>true</container_already_exists>
        <skip_access_check>true</skip_access_check>
      </azure_workload_identity_1604>
      <azure_connection_string_1604>
        <type>object_storage</type>
        <object_storage_type>azure_blob_storage</object_storage_type>
        <metadata_type>local</metadata_type>
        <connection_string>DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azure:10000/devstoreaccount1;</connection_string>
        <container_name>azure-connection-string-1604</container_name>
      </azure_connection_string_1604>
      <azure_endpoint_1604>
        <type>object_storage</type>
        <object_storage_type>azure_blob_storage</object_storage_type>
        <metadata_type>local</metadata_type>
        <endpoint>http://azure:10000/devstoreaccount1/azure-endpoint-1604/prefix</endpoint>
        <endpoint_subpath>subpath</endpoint_subpath>
        <account_name>devstoreaccount1</account_name>
        <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
      </azure_endpoint_1604>
    </disks>
    <policies>
      <azure_tiered_1604>
        <volumes>
          <hot><disk>default</disk></hot>
          <account_key><disk>azure_account_key_1604</disk><prefer_not_to_merge>true</prefer_not_to_merge></account_key>
          <connection_string><disk>azure_connection_string_1604</disk><prefer_not_to_merge>true</prefer_not_to_merge></connection_string>
          <endpoint><disk>azure_endpoint_1604</disk><prefer_not_to_merge>true</prefer_not_to_merge></endpoint>
        </volumes>
      </azure_tiered_1604>
    </policies>
  </storage_configuration>
</clickhouse>
XML
`)
	// runs before env.Cleanup, the pooled environment must not keep the disks when the test fails
	dbName := "test_azure_workload_identity_1604"
	defer func() {
		// clickhouse-server doesn't start with a table on a removed storage policy
		env.queryWithNoError(t, r, "DROP DATABASE IF EXISTS "+dbName+" SYNC")
		env.DockerExecNoError(r, "clickhouse", "rm", "-fv", configFile)
		env.ch.Close()
		r.NoError(env.tc.RestartContainer(t, "clickhouse"))
		env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
	}()
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t, "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
	disks := []string{"azure_account_key_1604", "azure_connection_string_1604", "azure_endpoint_1604"}
	env.checkCount(r, 1, 4, "SELECT count() FROM system.disks WHERE name IN ('azure_workload_identity_1604','"+strings.Join(disks, "','")+"')")

	backupName := "test_azure_workload_identity_1604"
	env.queryWithNoError(t, r, "DROP DATABASE IF EXISTS "+dbName+" SYNC")
	env.queryWithNoError(t, r, "CREATE DATABASE "+dbName)
	env.queryWithNoError(t, r, "CREATE TABLE "+dbName+".t (d Date, x UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x SETTINGS storage_policy = 'azure_tiered_1604'")
	// one partition on each azure disk which holds parts, 202604 stays on default
	for month := 1; month <= 4; month++ {
		env.queryWithNoError(t, r, fmt.Sprintf("INSERT INTO %s.t SELECT toDate('2026-%02d-15'), number FROM numbers(1000)", dbName, month))
	}
	for i, disk := range disks {
		env.queryWithNoError(t, r, fmt.Sprintf("ALTER TABLE %s.t MOVE PARTITION 20260%d TO DISK '%s'", dbName, i+1, disk))
	}
	checkPartsOnDisks := func() {
		env.checkCount(r, 1, 4000, "SELECT count() FROM "+dbName+".t")
		for _, disk := range append(disks, "default") {
			env.checkCount(r, 1, 1000, "SELECT sum(rows) AS `count()` FROM system.parts WHERE active AND database='"+dbName+"' AND table='t' AND disk_name='"+disk+"' SETTINGS empty_result_for_aggregation_by_empty_set=0")
		}
	}
	checkPartsOnDisks()

	out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-azblob.yml", "create_remote", "--tables="+dbName+".*", backupName)
	r.NoError(err, "create_remote failed: %s", out)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-azblob.yml", "delete", "local", backupName)
	env.queryWithNoError(t, r, "DROP TABLE "+dbName+".t SYNC")
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-azblob.yml", "restore_remote", "--tables="+dbName+".*", backupName)
	r.NoError(err, "restore_remote failed: %s", out)
	checkPartsOnDisks()

	fullCleanup(t, r, env, []string{backupName}, []string{"remote", "local"}, []string{dbName}, false, true, true, "config-azblob.yml")
}
