package server

import (
	"encoding/json"
	"net/http/httptest"
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
