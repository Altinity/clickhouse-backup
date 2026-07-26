package status

import (
	"context"
	"encoding/json"
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
