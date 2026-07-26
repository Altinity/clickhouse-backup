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

	seenOpIDs := map[string]struct{}{}
	for i := 0; i < 3; i++ {
		_, finish := b.startWatchIteration("watch create_remote test")
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

	_, finish1 := b.startWatchIteration("watch create_remote fail")
	finish1(errors.New("create_remote failed"))

	_, finish2 := b.startWatchIteration("watch create_remote ok")
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

// Verifies context cancellation mid-iteration triggers exactly one error callback,
// and that calling finish multiple times is safely idempotent.
func TestWatch_CanceledIterationFiresErrorCallbackOnce(t *testing.T) {
	r := require.New(t)
	var (
		mu       sync.Mutex
		payloads []status.CallbackPayload
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var p status.CallbackPayload
		_ = json.NewDecoder(req.Body).Decode(&p)
		mu.Lock()
		payloads = append(payloads, p)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := config.DefaultConfig()
	cfg.General.CallbackURL = srv.URL
	cfg.General.CallbackTimeoutDuration = 2 * time.Second
	b := NewBackuper(cfg)

	watchCtx, cancel := context.WithCancel(context.Background())
	_, finish := b.startWatchIteration("watch create_remote canceled")
	cancel() // simulate SIGTERM mid-iteration
	iterErr := watchCtx.Err()
	r.Error(iterErr)
	finish(iterErr)
	finish(iterErr) // duplicate finish must be a no-op

	mu.Lock()
	defer mu.Unlock()
	r.Len(payloads, 1, "exactly one callback per iteration, even if finish is called twice")
	r.Equal(status.ErrorStatus, payloads[0].Status)
	r.Equal(context.Canceled.Error(), payloads[0].Error)
	r.Equal("watch create_remote canceled", payloads[0].Command)
	r.NotEmpty(payloads[0].OperationId)
}

// Ensures watch iterations don't append to AsyncStatus.commands, preventing a memory leak
// in long-running watch processes.
func TestWatch_IterationDoesNotGrowStatusRegistry(t *testing.T) {
	r := require.New(t)
	cfg := config.DefaultConfig()
	b := NewBackuper(cfg)

	before := len(status.Current.GetStatus(false, "", 0))
	for i := 0; i < 5; i++ {
		_, finish := b.startWatchIteration("watch create_remote registry-growth")
		finish(nil)
	}
	after := len(status.Current.GetStatus(false, "", 0))
	r.Equal(before, after, "watch iterations must not append rows to the async-status registry")
}
