//go:build integration

package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

const strictKeeperTLSXML = `<yandex>
  <openSSL>
    <client replace="replace">
      <caConfig>/etc/clickhouse-server/keeper.crt</caConfig>
      <certificateFile>/etc/clickhouse-server/keeper.crt</certificateFile>
      <privateKeyFile>/etc/clickhouse-server/keeper.key</privateKeyFile>
      <loadDefaultCAFile>true</loadDefaultCAFile>
      <cacheSessions>true</cacheSessions>
      <disableProtocols>sslv2,sslv3</disableProtocols>
      <preferServerCiphers>true</preferServerCiphers>
      <verificationMode replace="replace">relaxed</verificationMode>
      <extendedVerification replace="replace">false</extendedVerification>
      <invalidCertificateHandler replace="replace">
        <name replace="replace">RejectCertificateHandler</name>
      </invalidCertificateHandler>
    </client>
  </openSSL>
</yandex>
`

func TestKeeperTLS(t *testing.T) {
	if isTestShouldSkip("KEEPER_TLS_ENABLED") {
		t.Skip("KEEPER_TLS_ENABLED is not set or false, skipping TestKeeperTLS")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.9") < 0 {
		t.Skipf("ClickHouse version %s is too old for KEEPER_TLS_ENABLED tests", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	defer env.Cleanup(t, r)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 2*time.Minute)

	// Install strict <openSSL><client> so ClickHouse verifies Keeper TLS certs
	// (ssl.xml uses verificationMode=none — fine for most tests, but this test
	// specifically validates the strict path). Restart so Poco reloads SSL context.
	env.DockerExecNoError(r, "clickhouse", "bash", "-c",
		fmt.Sprintf("cat > /etc/clickhouse-server/config.d/zz_keeper_tls.xml <<'XML'\n%s\nXML", strictKeeperTLSXML))
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t.Context(), "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)

	// create table using ZooKeeper
	dbName := "test_keeper_tls"
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	tableName := "test_table_tls"
	zkPath := "/clickhouse/tables/{shard}/{database}/{table}"
	onCluster := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.3") >= 0 {
		zkPath = "/clickhouse/tables/{shard}/{database}/{table}/{uuid}"
		onCluster = "ON CLUSTER '{cluster}'"
	}
	createSQL := fmt.Sprintf("CREATE TABLE %s.%s %s (id UInt64) ENGINE=ReplicatedMergeTree('%s', '{replica}') ORDER BY id",
		dbName, tableName, onCluster, zkPath)
	env.queryWithNoError(r, createSQL)
	env.queryWithNoError(r, fmt.Sprintf("INSERT INTO %s.%s SELECT number FROM numbers(100)", dbName, tableName))

	// create RBAC User
	rbacUser := "test_keeper_tls_user"
	env.queryWithNoError(r, fmt.Sprintf("DROP USER IF EXISTS %s %s", rbacUser, onCluster))
	env.queryWithNoError(r, fmt.Sprintf("CREATE USER %s %s IDENTIFIED WITH sha256_password BY '123'", rbacUser, onCluster))
	defer env.queryWithNoError(r, fmt.Sprintf("DROP USER IF EXISTS %s %s", rbacUser, onCluster))

	// backup with RBAC
	backupName := "test_keeper_tls_backup"
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--rbac", backupName)
	out, err := env.DockerExecOut("clickhouse-backup", "ls", "-laR", fmt.Sprintf("/var/lib/clickhouse/backup/%s/", backupName))
	r.NoError(err, "unexpected ls -laR error %s", out)

	// clean and restore
	r.NoError(env.dropDatabase(dbName, false))
	env.queryWithNoError(r, fmt.Sprintf("DROP USER IF EXISTS %s %s", rbacUser, onCluster))
	env.ch.Close()
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rbac", backupName)

	// wait for restart after RBAC restore and check
	env.connectWithWait(t, r, 5*time.Second, 1*time.Second, 2*time.Minute)
	var rowCount, userCount uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCount, fmt.Sprintf("SELECT count() FROM %s.%s", dbName, tableName)))
	r.Equal(uint64(100), rowCount)
	r.NoError(env.ch.SelectSingleRowNoCtx(&userCount, fmt.Sprintf("SELECT count() FROM system.users WHERE name='%s'", rbacUser)))
	r.Equal(uint64(1), userCount)

	// cleanup
	r.NoError(env.dropDatabase(dbName, false))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)

	env.DockerExecNoError(r, "clickhouse", "rm", "-f", "/etc/clickhouse-server/config.d/zz_keeper_tls.xml")
	env.ch.Close()
	r.NoError(env.tc.RestartContainer(t.Context(), "clickhouse"))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
}
