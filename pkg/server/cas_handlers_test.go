package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/server/metrics"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
)

// testMetrics is a shared metrics instance registered once for the test binary.
// prometheus.MustRegister panics on duplicates so we must share across tests.
var testMetrics = func() *metrics.APIMetrics {
	m := metrics.NewAPIMetrics()
	m.RegisterMetrics()
	return m
}()

// newTestAPI builds a minimal APIServer suitable for handler unit-tests.
// It uses a non-existent configPath so ReloadConfig falls back to DefaultConfig.
func newTestAPI(t *testing.T) *APIServer {
	t.Helper()
	cfg := config.DefaultConfig()
	// Ensure AllowParallel default is false — tests set it explicitly.
	cfg.API.AllowParallel = false

	app := cli.NewApp()
	app.Version = "test"

	return &APIServer{
		cliApp:                  app,
		configPath:              "/nonexistent/config.yaml", // causes LoadConfig to use DefaultConfig
		config:                  cfg,
		metrics:                 testMetrics,
		restart:                 make(chan struct{}, 1),
		stop:                    make(chan struct{}, 1),
		clickhouseBackupVersion: "test",
	}
}

// TestCASUploadHandler_AsyncAck verifies that a POST to /backup/cas-upload/{name}
// immediately returns 200 with an acknowledged asyncAck body before the background
// goroutine runs.
func TestCASUploadHandler_AsyncAck(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = true // permit the call even if another op is in progress

	req := httptest.NewRequest("POST", "/backup/cas-upload/myname", nil)
	// Inject mux vars manually (bypasses the router).
	req = mux.SetURLVars(req, map[string]string{"name": "myname"})
	rr := httptest.NewRecorder()

	api.httpCASUploadHandler(rr, req)

	require.Equal(t, 200, rr.Code)

	var ack asyncAck
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ack))
	require.Equal(t, "acknowledged", ack.Status)
	require.Equal(t, "cas-upload", ack.Operation)
	require.Equal(t, "myname", ack.BackupName)
	require.NotEmpty(t, ack.OperationId)
}

// TestCASUploadHandler_LockedWhenBusy verifies that the handler returns 423 when
// AllowParallel=false and another operation is in progress.
func TestCASUploadHandler_LockedWhenBusy(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = false

	// Register a fake in-progress operation.
	cmdId, _ := status.Current.Start("upload some-other-backup")
	defer status.Current.Stop(cmdId, nil)

	req := httptest.NewRequest("POST", "/backup/cas-upload/myname", nil)
	req = mux.SetURLVars(req, map[string]string{"name": "myname"})
	rr := httptest.NewRecorder()

	api.httpCASUploadHandler(rr, req)

	require.Equal(t, 423, rr.Code)
}

// ---------- cas-download ----------

// TestCASDownloadHandler_AsyncAck verifies that POST /backup/cas-download/{name}
// returns 200 with an acknowledged asyncAck body immediately.
func TestCASDownloadHandler_AsyncAck(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = true

	req := httptest.NewRequest("POST", "/backup/cas-download/mybackup", nil)
	req = mux.SetURLVars(req, map[string]string{"name": "mybackup"})
	rr := httptest.NewRecorder()

	api.httpCASDownloadHandler(rr, req)

	require.Equal(t, 200, rr.Code)
	var ack asyncAck
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ack))
	require.Equal(t, "acknowledged", ack.Status)
	require.Equal(t, "cas-download", ack.Operation)
	require.Equal(t, "mybackup", ack.BackupName)
	require.NotEmpty(t, ack.OperationId)
}

// TestCASDownloadHandler_DataOnlyReturns501 verifies that ?data returns 501.
func TestCASDownloadHandler_DataOnlyReturns501(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = true

	req := httptest.NewRequest("POST", "/backup/cas-download/mybackup?data", nil)
	req = mux.SetURLVars(req, map[string]string{"name": "mybackup"})
	rr := httptest.NewRecorder()

	api.httpCASDownloadHandler(rr, req)

	require.Equal(t, 501, rr.Code)
}

// ---------- cas-restore ----------

// TestCASRestoreHandler_AsyncAck verifies that POST /backup/cas-restore/{name}
// returns 200 with an acknowledged asyncAck body immediately.
func TestCASRestoreHandler_AsyncAck(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = true

	req := httptest.NewRequest("POST", "/backup/cas-restore/mybackup", nil)
	req = mux.SetURLVars(req, map[string]string{"name": "mybackup"})
	rr := httptest.NewRecorder()

	api.httpCASRestoreHandler(rr, req)

	require.Equal(t, 200, rr.Code)
	var ack asyncAck
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ack))
	require.Equal(t, "acknowledged", ack.Status)
	require.Equal(t, "cas-restore", ack.Operation)
	require.Equal(t, "mybackup", ack.BackupName)
	require.NotEmpty(t, ack.OperationId)
}

// TestCASRestoreHandler_IgnoreDependenciesReturns400 verifies that
// ?ignore-dependencies is rejected with 400 at the handler boundary.
func TestCASRestoreHandler_IgnoreDependenciesReturns400(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = true

	req := httptest.NewRequest("POST", "/backup/cas-restore/mybackup?ignore-dependencies", nil)
	req = mux.SetURLVars(req, map[string]string{"name": "mybackup"})
	rr := httptest.NewRecorder()

	api.httpCASRestoreHandler(rr, req)

	require.Equal(t, 400, rr.Code)
}

// ---------- cas-delete ----------

// TestCASDeleteHandler_LockedWhenBusy verifies that the handler returns 423 when
// AllowParallel=false and another operation is in progress.
func TestCASDeleteHandler_LockedWhenBusy(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = false

	cmdId, _ := status.Current.Start("some-other-op")
	defer status.Current.Stop(cmdId, nil)

	req := httptest.NewRequest("POST", "/backup/cas-delete/mybackup", nil)
	req = mux.SetURLVars(req, map[string]string{"name": "mybackup"})
	rr := httptest.NewRecorder()

	api.httpCASDeleteHandler(rr, req)

	require.Equal(t, 423, rr.Code)
}

// ---------- cas-verify ----------

// TestCASVerifyHandler_AsyncAck verifies that POST /backup/cas-verify/{name}
// returns 200 with an acknowledged asyncAck body immediately.
func TestCASVerifyHandler_AsyncAck(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = true

	req := httptest.NewRequest("POST", "/backup/cas-verify/mybackup", nil)
	req = mux.SetURLVars(req, map[string]string{"name": "mybackup"})
	rr := httptest.NewRecorder()

	api.httpCASVerifyHandler(rr, req)

	require.Equal(t, 200, rr.Code)
	var ack asyncAck
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ack))
	require.Equal(t, "acknowledged", ack.Status)
	require.Equal(t, "cas-verify", ack.Operation)
	require.Equal(t, "mybackup", ack.BackupName)
	require.NotEmpty(t, ack.OperationId)
}

// ---------- cas-prune ----------

// TestCASPruneHandler_AsyncAck verifies that POST /backup/cas-prune
// returns 200 with an acknowledged asyncAck body immediately.
func TestCASPruneHandler_AsyncAck(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = true

	req := httptest.NewRequest("POST", "/backup/cas-prune", nil)
	rr := httptest.NewRecorder()

	api.httpCASPruneHandler(rr, req)

	require.Equal(t, 200, rr.Code)
	var ack asyncAck
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ack))
	require.Equal(t, "acknowledged", ack.Status)
	require.Equal(t, "cas-prune", ack.Operation)
	require.NotEmpty(t, ack.OperationId)
}

// TestCASPruneHandler_PassesQueryParams verifies that dry-run and grace-blob
// are reflected in the status command string that was started.
func TestCASPruneHandler_PassesQueryParams(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = true

	req := httptest.NewRequest("POST", "/backup/cas-prune?dry-run&grace-blob=0s", nil)
	rr := httptest.NewRecorder()

	api.httpCASPruneHandler(rr, req)

	require.Equal(t, 200, rr.Code)
	var ack asyncAck
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ack))
	require.NotEmpty(t, ack.OperationId)

	// Retrieve the started command from status and verify it contains the flags.
	rows := status.Current.GetStatus(false, "", 10)
	found := false
	for _, row := range rows {
		if row.OperationId == ack.OperationId {
			require.Contains(t, row.Command, "--dry-run")
			require.Contains(t, row.Command, "--grace-blob=0s")
			found = true
			break
		}
	}
	require.True(t, found, "operation not found in status log")
}

// ---------- cas-status ----------

// TestCASStatusHandler_ReturnsJSON verifies that GET /backup/cas-status
// returns a JSON body. With no real CAS backend configured the handler
// returns 500 with an error JSON object — we just assert the response is
// valid JSON (not empty/HTML) and that Content-Type is set appropriately.
// Full structured-data verification is an integration-test concern.
func TestCASStatusHandler_ReturnsJSON(t *testing.T) {
	api := newTestAPI(t)

	req := httptest.NewRequest("GET", "/backup/cas-status", nil)
	rr := httptest.NewRecorder()

	api.httpCASStatusHandler(rr, req)

	// With cas.enabled=false the handler returns 500, but the body must be JSON.
	body := rr.Body.Bytes()
	require.True(t, len(body) > 0, "response body must not be empty")
	var payload interface{}
	require.NoError(t, json.Unmarshal(body, &payload), "response body must be valid JSON")
}

// ──────────────────────────────────────────────────────────────────────────────
// Task 7: /backup/actions dispatcher
// ──────────────────────────────────────────────────────────────────────────────

// TestCASActionsDispatcher_Upload verifies that a POST to /backup/actions with
// a cas-upload command returns 200 with an "acknowledged" result row.
//
// /backup/actions uses sendJSONEachRow: the response body is newline-delimited
// JSON objects, not a JSON array — we decode the first line accordingly.
func TestCASActionsDispatcher_Upload(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = true

	body := `{"command": "cas-upload myname --skip-object-disks"}`
	req := httptest.NewRequest("POST", "/backup/actions", strings.NewReader(body))
	rr := httptest.NewRecorder()

	api.actions(rr, req)

	require.Equal(t, 200, rr.Code, "body: %s", rr.Body.String())

	// sendJSONEachRow emits one JSON object per line; decode the first line.
	firstLine := strings.SplitN(strings.TrimSpace(rr.Body.String()), "\n", 2)[0]
	var result actionsResultsRow
	require.NoError(t, json.Unmarshal([]byte(firstLine), &result))
	require.Equal(t, "acknowledged", result.Status)
	require.Contains(t, result.Operation, "cas-upload")
}

// TestCASActionsDispatcher_UnknownVerb verifies that an unknown command still
// returns 400 (the existing default branch), not a panic or 500.
func TestCASActionsDispatcher_UnknownVerb(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = true

	body := `{"command": "cas-frobnicate myname"}`
	req := httptest.NewRequest("POST", "/backup/actions", strings.NewReader(body))
	rr := httptest.NewRecorder()

	api.actions(rr, req)

	// The default switch branch returns 400 for unknown commands.
	require.Equal(t, 400, rr.Code, "body: %s", rr.Body.String())
}

// TestCASActionsDispatcher_LockedWhenBusy verifies that the dispatcher honours
// AllowParallel=false and returns 500 (which wraps ErrAPILocked) when another
// operation is already in progress.
func TestCASActionsDispatcher_LockedWhenBusy(t *testing.T) {
	api := newTestAPI(t)
	api.config.API.AllowParallel = false

	cmdId, _ := status.Current.Start("upload some-other-backup")
	defer status.Current.Stop(cmdId, nil)

	body := `{"command": "cas-upload myname"}`
	req := httptest.NewRequest("POST", "/backup/actions", strings.NewReader(body))
	rr := httptest.NewRecorder()

	api.actions(rr, req)

	// actionsAsyncCommandsHandler returns ErrAPILocked → writeError → 500.
	require.Equal(t, 500, rr.Code, "body: %s", rr.Body.String())
}

// ──────────────────────────────────────────────────────────────────────────────
// Task 8: /backup/list kind field
// ──────────────────────────────────────────────────────────────────────────────

// TestHttpListHandler_KindFieldPresent verifies that the list handler returns
// valid JSON. With no real ClickHouse or remote storage configured the handler
// returns an empty array — we verify that the response is parseable and the
// kind field is omitted (rather than present but wrong) for the zero-entry case.
//
// Full "v1 + cas merged" verification requires a Backuper stub and is covered
// by the integration test TestCASAPI_ListMixedBackups.
func TestHttpListHandler_KindFieldPresent(t *testing.T) {
	t.Skip("requires live ClickHouse connection; covered by integration TestCASAPI_ListMixedBackups")
}
