//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// TestCASAPIRoundtrip drives a full CAS upload→list→restore→delete→prune
// flow over the REST API, mirroring the v1 API roundtrip pattern in
// serverAPI_test.go.
func TestCASAPIRoundtrip(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "api_roundtrip")

	// Install curl + jq for HTTP probes inside the clickhouse-backup container.
	env.InstallDebIfNotExists(r, "clickhouse-backup", "curl", "jq")

	// Start the daemon.
	log.Debug().Msg("Run `clickhouse-backup server` in background")
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce",
		"clickhouse-backup -c "+casConfigPath+" server &>>/tmp/clickhouse-backup-cas-api-server.log")
	time.Sleep(5 * time.Second)
	defer func() {
		_, _ = env.DockerExecOut("clickhouse-backup", "pkill", "-n", "-f", "clickhouse-backup")
	}()

	const (
		dbName = "cas_api_db"
		tbl    = "t"
		bk     = "cas_api_bk"
	)

	// Prepare test data and local backup.
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf(
		"CREATE TABLE `%s`.`%s` (id UInt64, payload String) ENGINE=MergeTree ORDER BY id "+
			"SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0",
		dbName, tbl))
	// Use randomPrintableASCII to exceed the 1024-byte inline threshold.
	env.queryWithNoError(r, fmt.Sprintf(
		"INSERT INTO `%s`.`%s` SELECT number, randomPrintableASCII(64) FROM numbers(1000)",
		dbName, tbl))

	// Create local backup via CLI (CAS upload itself goes via HTTP).
	env.casBackupNoError(r, "create", "--tables", dbName+".*", bk)

	// POST /backup/cas-upload/<bk>
	opID := casAPIPostAndCaptureOpID(t, env, r, fmt.Sprintf("/backup/cas-upload/%s", bk))
	casAPIWaitForOperation(t, env, r, opID, 60*time.Second)

	// GET /backup/list/remote — assert the backup appears with kind="cas"
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		"curl -sfL 'http://localhost:7171/backup/list/remote'")
	r.NoError(err, "list/remote: %s", out)
	found := false
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry map[string]interface{}
		if json.Unmarshal([]byte(line), &entry) != nil {
			continue
		}
		if entry["name"] == bk && entry["kind"] == "cas" {
			found = true
		}
	}
	r.True(found, "cas backup must appear in /backup/list/remote with kind=cas; out=%s", out)

	// POST /backup/cas-restore/<bk>?rm — drop the table first so restore re-creates it.
	env.queryWithNoError(r, fmt.Sprintf("DROP TABLE `%s`.`%s` SYNC", dbName, tbl))
	opID = casAPIPostAndCaptureOpID(t, env, r, fmt.Sprintf("/backup/cas-restore/%s?rm", bk))
	casAPIWaitForOperation(t, env, r, opID, 120*time.Second)

	var rows uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&rows, fmt.Sprintf("SELECT count() FROM `%s`.`%s`", dbName, tbl)))
	r.Equal(uint64(1000), rows, "restored row count mismatch")

	// POST /backup/cas-delete/<bk> (async since wave-5 F13 — same pattern as upload/restore/prune).
	opID = casAPIPostAndCaptureOpID(t, env, r, fmt.Sprintf("/backup/cas-delete/%s", bk))
	casAPIWaitForOperation(t, env, r, opID, 60*time.Second)

	// POST /backup/cas-prune (async)
	opID = casAPIPostAndCaptureOpID(t, env, r, "/backup/cas-prune")
	casAPIWaitForOperation(t, env, r, opID, 60*time.Second)

	r.NoError(env.dropDatabase(dbName, true))
}

// casAPIPostAndCaptureOpID POSTs to the given path under the API server,
// expects an "acknowledged" response with an operation_id, and returns it.
func casAPIPostAndCaptureOpID(t *testing.T, env *TestEnvironment, r *require.Assertions, path string) string {
	t.Helper()
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
		fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171%s'", path))
	r.NoError(err, "POST %s: %s", path, out)
	out = strings.TrimSpace(out)
	// The response is a single JSON object (sendJSONEachRow with a non-slice value).
	var ack struct {
		Status      string `json:"status"`
		OperationId string `json:"operation_id"`
	}
	r.NoError(json.Unmarshal([]byte(out), &ack), "parse ack for POST %s: %s", path, out)
	r.Equal("acknowledged", ack.Status, "POST %s: expected acknowledged; out=%s", path, out)
	r.NotEmpty(ack.OperationId, "POST %s: empty operation_id; out=%s", path, out)
	return ack.OperationId
}

// casAPIWaitForOperation polls GET /backup/status?operationid=<id> until the
// operation completes (success) or fails (error). Uses the same approach as
// testAPIBackupCreateRemote in serverAPI_test.go.
func casAPIWaitForOperation(t *testing.T, env *TestEnvironment, r *require.Assertions, opID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// GET /backup/status?operationid=<id> returns line-delimited JSON
		// (one ActionRowStatus per line).
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce",
			fmt.Sprintf("curl -sfL 'http://localhost:7171/backup/status?operationid=%s'", opID))
		if err == nil {
			for _, line := range strings.Split(out, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				var action status.ActionRowStatus
				if json.Unmarshal([]byte(line), &action) != nil {
					continue
				}
				switch action.Status {
				case status.SuccessStatus:
					return
				case status.ErrorStatus:
					r.FailNow(fmt.Sprintf(
						"operation %s failed: %s (command=%s)",
						opID, action.Error, action.Command,
					))
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	// Print server log on timeout for diagnostics.
	logOut, _ := env.DockerExecOut("clickhouse-backup", "cat",
		"/tmp/clickhouse-backup-cas-api-server.log")
	r.FailNow(fmt.Sprintf(
		"operation %s did not complete within %s\nserver log:\n%s",
		opID, timeout, logOut,
	))
}

// TestCASAPI_ListMixedBackups — kind=cas presence is already covered by
// TestCASAPIRoundtrip; a full mixed (v1 + CAS) list flow is deferred.
func TestCASAPI_ListMixedBackups(t *testing.T) {
	t.Skip("kind=cas presence covered by TestCASAPIRoundtrip; full mixed-list flow deferred")
}
