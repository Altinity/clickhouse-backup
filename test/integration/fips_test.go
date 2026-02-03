//go:build integration

package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/rs/zerolog/log"
)

func TestFIPS(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") <= 0 {
		t.Skip("go 1.25 with boringcrypto stop works for 19.17, works only for 20.1+")
	}
	if os.Getenv("QA_AWS_ACCESS_KEY") == "" {
		t.Skip("QA_AWS_ACCESS_KEY is empty, TestFIPS will skip")
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 1*time.Second, 1*time.Second, 1*time.Minute)
	fipsBackupName := fmt.Sprintf("fips_backup_%d", rand.Int())
	env.DockerExecNoError(r, "clickhouse", "rm", "-fv", "/etc/apt/sources.list.d/clickhouse.list")
	env.InstallDebIfNotExists(r, "clickhouse", "ca-certificates", "curl", "gettext-base", "bsdmainutils", "dnsutils", "git")
	env.DockerExecNoError(r, "clickhouse", "update-ca-certificates")
	r.NoError(env.DockerCP("configs/config-s3-fips.yml", "clickhouse:/etc/clickhouse-backup/config.yml.fips-template"))
	env.DockerExecNoError(r, "clickhouse", "git", "clone", "--depth", "1", "--branch", "v3.2rc3", "https://github.com/drwetter/testssl.sh.git", "/opt/testssl")
	env.DockerExecNoError(r, "clickhouse", "chmod", "+x", "/opt/testssl/testssl.sh")

	generateCerts := func(certType, keyLength, curveType string) {
		env.DockerExecNoError(r, "clickhouse", "bash", "-xce", "openssl rand -out /root/.rnd 2048")
		switch certType {
		case "rsa":
			env.DockerExecNoError(r, "clickhouse", "bash", "-xce", fmt.Sprintf("openssl genrsa -out /etc/clickhouse-backup/ca-key.pem %s", keyLength))
			env.DockerExecNoError(r, "clickhouse", "bash", "-xce", fmt.Sprintf("openssl genrsa -out /etc/clickhouse-backup/server-key.pem %s", keyLength))
		case "ecdsa":
			env.DockerExecNoError(r, "clickhouse", "bash", "-xce", fmt.Sprintf("openssl ecparam -name %s -genkey -out /etc/clickhouse-backup/ca-key.pem", curveType))
			env.DockerExecNoError(r, "clickhouse", "bash", "-xce", fmt.Sprintf("openssl ecparam -name %s -genkey -out /etc/clickhouse-backup/server-key.pem", curveType))
		}
		env.DockerExecNoError(r, "clickhouse", "bash", "-xce", "openssl req -subj \"/O=altinity\" -x509 -new -nodes -key /etc/clickhouse-backup/ca-key.pem -sha256 -days 365000 -out /etc/clickhouse-backup/ca-cert.pem")
		env.DockerExecNoError(r, "clickhouse", "bash", "-xce", "openssl req -subj \"/CN=localhost\" -addext \"subjectAltName = DNS:localhost,DNS:*.cluster.local\" -new -key /etc/clickhouse-backup/server-key.pem -out /etc/clickhouse-backup/server-req.csr")
		env.DockerExecNoError(r, "clickhouse", "bash", "-xce", "openssl x509 -req -days 365000 -extensions SAN -extfile <(printf \"\\n[SAN]\\nsubjectAltName=DNS:localhost,DNS:*.cluster.local\") -in /etc/clickhouse-backup/server-req.csr -out /etc/clickhouse-backup/server-cert.pem -CA /etc/clickhouse-backup/ca-cert.pem -CAkey /etc/clickhouse-backup/ca-key.pem -CAcreateserial")
	}
	env.DockerExecNoError(r, "clickhouse", "bash", "-xec", "cat /etc/clickhouse-backup/config-s3-fips.yml.template | envsubst > /etc/clickhouse-backup/config-s3-fips.yml")

	generateCerts("rsa", "4096", "")
	env.queryWithNoError(r, "CREATE DATABASE "+t.Name())
	createSQL := "CREATE TABLE " + t.Name() + ".fips_table (v UInt64) ENGINE=MergeTree() ORDER BY tuple()"
	env.queryWithNoError(r, createSQL)
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".fips_table SELECT number FROM numbers(1000)")
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml create_remote --tables="+t.Name()+".fips_table "+fipsBackupName)
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml delete local "+fipsBackupName)
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml restore_remote --tables="+t.Name()+".fips_table "+fipsBackupName)
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml delete local "+fipsBackupName)
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml delete remote "+fipsBackupName)

	log.Debug().Msg("Run `clickhouse-backup-fips server` in background")
	env.DockerExecBackgroundNoError(r, "clickhouse", "bash", "-ce", "AWS_USE_FIPS_ENDPOINT=true clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml server &>>/tmp/clickhouse-backup-server-fips.log")
	time.Sleep(1 * time.Second)

	runClickHouseClientInsertSystemBackupActions(r, env, []string{fmt.Sprintf("create_remote --tables="+t.Name()+".fips_table %s", fipsBackupName)}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{fmt.Sprintf("delete local %s", fipsBackupName)}, false)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{fmt.Sprintf("restore_remote --tables="+t.Name()+".fips_table  %s", fipsBackupName)}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{fmt.Sprintf("delete local %s", fipsBackupName)}, false)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{fmt.Sprintf("delete remote %s", fipsBackupName)}, false)

	inProgressActions := make([]struct {
		Command string `ch:"command"`
		Status  string `ch:"status"`
	}, 0)
	r.NoError(env.ch.StructSelect(&inProgressActions,
		"SELECT command, status FROM system.backup_actions WHERE command LIKE ? AND status IN (?,?)",
		fmt.Sprintf("%%%s%%", fipsBackupName), status.InProgressStatus, status.ErrorStatus,
	))
	r.Equal(0, len(inProgressActions), "inProgressActions=%+v", inProgressActions)
	env.DockerExecNoError(r, "clickhouse", "pkill", "-n", "-f", "clickhouse-backup-fips")

	testTLSCerts := func(certType, keyLength, curveName string, cipherList ...string) {
		generateCerts(certType, keyLength, curveName)
		log.Debug().Msgf("Run `clickhouse-backup-fips server` in background for %s %s %s", certType, keyLength, curveName)
		env.DockerExecBackgroundNoError(r, "clickhouse", "bash", "-ce", "AWS_USE_FIPS_ENDPOINT=true clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml server &>>/tmp/clickhouse-backup-server-fips.log")
		time.Sleep(1 * time.Second)

		env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "rm -rf /tmp/testssl* && /opt/testssl/testssl.sh -e -s -oC /tmp/testssl.csv --color 0 --disable-rating --quiet -n min --mode parallel --add-ca /etc/clickhouse-backup/ca-cert.pem localhost:7172")
		env.DockerExecNoError(r, "clickhouse", "cat", "/tmp/testssl.csv")
		out, err := env.DockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("grep -o -E '%s' /tmp/testssl.csv | sort | uniq | wc -l", strings.Join(cipherList, "|")))
		r.NoError(err, "%s\nunexpected grep testssl.csv error: %v", out, err)
		r.Equal(strconv.Itoa(len(cipherList)), strings.Trim(out, " \t\r\n"))

		inProgressActions := make([]struct {
			Command string `ch:"command"`
			Status  string `ch:"status"`
		}, 0)
		r.NoError(env.ch.StructSelect(&inProgressActions,
			"SELECT command, status FROM system.backup_actions WHERE command LIKE ? AND status IN (?,?)",
			fmt.Sprintf("%%%s%%", fipsBackupName), status.InProgressStatus, status.ErrorStatus,
		))
		r.Equal(0, len(inProgressActions), "inProgressActions=%+v", inProgressActions)
		env.DockerExecNoError(r, "clickhouse", "pkill", "-n", "-f", "clickhouse-backup-fips")
	}
	// https://www.perplexity.ai/search/0920f1e8-59ec-4e14-b779-ba7b2e037196
	testTLSCerts("rsa", "4096", "", "ECDHE-RSA-AES128-GCM-SHA256", "ECDHE-RSA-AES256-GCM-SHA384", "AES_128_GCM_SHA256", "AES_256_GCM_SHA384")
	testTLSCerts("ecdsa", "", "prime256v1", "ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-ECDSA-AES256-GCM-SHA384")
	r.NoError(env.ch.DropOrDetachTable(clickhouse.Table{Database: t.Name(), Name: "fips_table"}, createSQL, "", false, 0, "", false, ""))
	r.NoError(env.dropDatabase(t.Name(), true))
	env.Cleanup(t, r)
}
