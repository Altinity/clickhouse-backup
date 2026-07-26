package backup

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/stretchr/testify/require"
)

func TestWatch_CallbackDispatchedPerIteration(t *testing.T) {
	r := require.New(t)
	var (
		mu       sync.Mutex
		payloads []status.CallbackPayload
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var p status.CallbackPayload
		if err := json.Unmarshal(body, &p); err != nil {
			t.Errorf("unmarshal: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		payloads = append(payloads, p)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config.DefaultConfig()
	cfg.General.CallbackURL = srv.URL
	cfg.General.CallbackTimeoutDuration = 2 * time.Second
	b := NewBackuper(cfg)

	parentCtx := context.Background()
	seenOpIDs := map[string]struct{}{}
	for i := 0; i < 3; i++ {
		_, _, _, finish := b.startWatchIteration(parentCtx, "watch create_remote test")
		finish(nil)
	}

	mu.Lock()
	defer mu.Unlock()
	r.Len(payloads, 3)
	for _, p := range payloads {
		r.Equal(status.SuccessStatus, p.Status)
		r.Equal("watch create_remote test", p.Command)
		r.NotEmpty(p.OperationId)
		r.NotEmpty(p.Duration)
		_, dup := seenOpIDs[p.OperationId]
		r.False(dup, "operation_id must be unique per iteration")
		seenOpIDs[p.OperationId] = struct{}{}
	}
}

func TestWatch_CallbackDispatchedOnIterationFailure(t *testing.T) {
	r := require.New(t)
	var (
		mu       sync.Mutex
		payloads []status.CallbackPayload
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		var p status.CallbackPayload
		_ = json.Unmarshal(body, &p)
		mu.Lock()
		payloads = append(payloads, p)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config.DefaultConfig()
	cfg.General.CallbackURL = srv.URL
	cfg.General.CallbackTimeoutDuration = 2 * time.Second
	b := NewBackuper(cfg)

	parentCtx := context.Background()
	_, _, _, finish1 := b.startWatchIteration(parentCtx, "watch create_remote fail")
	finish1(errors.New("create_remote failed"))

	_, _, _, finish2 := b.startWatchIteration(parentCtx, "watch create_remote ok")
	finish2(nil)

	mu.Lock()
	defer mu.Unlock()
	r.Len(payloads, 2)
	r.Equal(status.ErrorStatus, payloads[0].Status)
	r.Equal("create_remote failed", payloads[0].Error)
	r.Equal(status.SuccessStatus, payloads[1].Status)
	r.Equal("", payloads[1].Error)
	r.NotEqual(payloads[0].OperationId, payloads[1].OperationId)
}

func TestWatch_IterationStopClearsInProgress(t *testing.T) {
	r := require.New(t)
	cfg := config.DefaultConfig()
	b := NewBackuper(cfg)

	iterId, _, _, finish := b.startWatchIteration(context.Background(), "watch create_remote dangling")
	rows := status.Current.GetStatus(true, "watch create_remote dangling", 1)
	r.NotEmpty(rows)
	r.Equal(status.InProgressStatus, rows[0].Status)

	finish(nil)
	rows = status.Current.GetStatus(false, "watch create_remote dangling", 0)
	found := false
	for _, row := range rows {
		if row.Command == "watch create_remote dangling" {
			found = true
			r.Equal(status.SuccessStatus, row.Status)
			r.NotEqual(status.InProgressStatus, row.Status)
		}
	}
	r.True(found)
	_ = iterId
}
