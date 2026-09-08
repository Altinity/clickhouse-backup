package status

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

	"github.com/stretchr/testify/require"
)

func TestSendCallback_Success(t *testing.T) {
	r := require.New(t)
	var (
		mu      sync.Mutex
		gotBody []byte
		gotCT   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotCT = req.Header.Get("Content-Type")
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		gotBody = body
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	payload := CallbackPayload{
		Status:      SuccessStatus,
		Error:       "",
		OperationId: "op-123",
		Command:     "create my_backup",
		Duration:    "1.5s",
	}
	err := SendCallback(context.Background(), srv.URL, payload)
	r.NoError(err)

	mu.Lock()
	defer mu.Unlock()
	r.Equal("application/json", gotCT)

	var got map[string]interface{}
	r.NoError(json.Unmarshal(gotBody, &got))
	r.Equal("success", got["status"])
	r.Equal("", got["error"])
	r.Equal("op-123", got["operation_id"])
	r.Equal("create my_backup", got["command"])
	r.Equal("1.5s", got["duration"])
}

func TestSendCallback_HTTPError_DoesNotPanic(t *testing.T) {
	r := require.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := SendCallback(context.Background(), srv.URL, CallbackPayload{
		Status:      ErrorStatus,
		Error:       "boom",
		OperationId: "op-err",
	})
	r.Error(err)
}

func TestSendCallback_Timeout(t *testing.T) {
	r := require.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := SendCallback(ctx, srv.URL, CallbackPayload{
		Status:      SuccessStatus,
		OperationId: "op-timeout",
	})
	r.Error(err)
}

func TestCallbackEligible(t *testing.T) {
	r := require.New(t)
	for _, command := range []string{"create", "create_remote my_backup", "restore_remote --tables=db.t my_backup", "clean_remote_broken"} {
		r.True(CallbackEligible(command), "command %q must notify", command)
	}
	for _, command := range []string{"", "list", "list remote", "tables", "status", "watch", "server", "create_lightweight"} {
		r.False(CallbackEligible(command), "command %q must not notify", command)
	}
}

func TestStop_SendsCallbackOnSuccess(t *testing.T) {
	r := require.New(t)
	payloads, srv := newCallbackReceiver(t)
	defer srv.Close()

	s := &AsyncStatus{}
	commandId, _ := s.StartWithCallback("create_remote my_backup", "op-1", &CallbackConfig{URLs: []string{srv.URL}, Timeout: 2 * time.Second})
	s.Stop(commandId, nil)

	got := awaitPayload(t, payloads)
	r.Equal(SuccessStatus, got.Status)
	r.Equal("", got.Error)
	r.Equal("op-1", got.OperationId)
	r.Equal("create_remote my_backup", got.Command)
	r.NotEmpty(got.Duration)
}

func TestStop_SendsCallbackOnError(t *testing.T) {
	r := require.New(t)
	payloads, srv := newCallbackReceiver(t)
	defer srv.Close()

	s := &AsyncStatus{}
	commandId, _ := s.StartWithCallback("create_remote my_backup", "op-2", &CallbackConfig{URLs: []string{srv.URL}, Timeout: 2 * time.Second})
	s.Stop(commandId, errors.New("disk is full"))

	got := awaitPayload(t, payloads)
	r.Equal(ErrorStatus, got.Status)
	r.Equal("disk is full", got.Error)
}

// Every configured URL is notified, the API accepts a repeated ?callback= param.
func TestStop_SendsCallbackToEveryURL(t *testing.T) {
	r := require.New(t)
	payloads1, srv1 := newCallbackReceiver(t)
	defer srv1.Close()
	payloads2, srv2 := newCallbackReceiver(t)
	defer srv2.Close()

	s := &AsyncStatus{}
	commandId, _ := s.StartWithCallback("upload my_backup", "op-3", &CallbackConfig{URLs: []string{srv1.URL, srv2.URL}, Timeout: 2 * time.Second})
	s.Stop(commandId, nil)

	r.Equal("op-3", awaitPayload(t, payloads1).OperationId)
	r.Equal("op-3", awaitPayload(t, payloads2).OperationId)
}

func TestStop_NoCallbackForReadOnlyCommand(t *testing.T) {
	r := require.New(t)
	payloads, srv := newCallbackReceiver(t)
	defer srv.Close()

	s := &AsyncStatus{}
	commandId, _ := s.StartWithCallback("list remote", "op-4", &CallbackConfig{URLs: []string{srv.URL}, Timeout: 2 * time.Second})
	s.Stop(commandId, nil)

	r.Nil(s.byId[commandId].callback)
	expectNoPayload(t, payloads)
}

// A broken receiver is logged and swallowed, the command result is unaffected
// and the caller is never blocked by callback IO.
func TestStop_BrokenReceiverDoesNotBlockCommand(t *testing.T) {
	r := require.New(t)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	defer close(release)

	s := &AsyncStatus{}
	commandId, _ := s.StartWithCallback("create my_backup", "op-5", &CallbackConfig{URLs: []string{srv.URL}, Timeout: 5 * time.Second})

	done := make(chan struct{})
	go func() {
		s.Stop(commandId, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		r.Fail("Stop blocked on a slow callback receiver")
	}
	r.Equal(SuccessStatus, s.GetStatusByOperationId("op-5")[0].Status)
}

// A killed command still owes its caller exactly one notification, sent when the
// command goroutine finally returns and calls Stop.
func TestStop_NotifiesOnceAfterCancel(t *testing.T) {
	r := require.New(t)
	payloads, srv := newCallbackReceiver(t)
	defer srv.Close()

	s := &AsyncStatus{}
	commandId, _ := s.StartWithCallback("restore my_backup", "op-6", &CallbackConfig{URLs: []string{srv.URL}, Timeout: 2 * time.Second})
	go s.Stop(commandId, nil)
	_, err := s.Cancel("restore my_backup", errors.New("canceled by user"))
	r.NoError(err)

	got := awaitPayload(t, payloads)
	r.Equal(CancelStatus, got.Status)
	r.Equal("canceled by user", got.Error)

	// second Stop must stay silent
	s.Stop(commandId, nil)
	expectNoPayload(t, payloads)
}

func newCallbackReceiver(t *testing.T) (chan CallbackPayload, *httptest.Server) {
	t.Helper()
	payloads := make(chan CallbackPayload, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var p CallbackPayload
		if err := json.Unmarshal(body, &p); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		payloads <- p
		w.WriteHeader(http.StatusOK)
	}))
	return payloads, srv
}

func awaitPayload(t *testing.T, payloads chan CallbackPayload) CallbackPayload {
	t.Helper()
	select {
	case p := <-payloads:
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
		return CallbackPayload{}
	}
}

func expectNoPayload(t *testing.T, payloads chan CallbackPayload) {
	t.Helper()
	select {
	case p := <-payloads:
		t.Fatalf("unexpected callback %+v", p)
	case <-time.After(300 * time.Millisecond):
	}
}
