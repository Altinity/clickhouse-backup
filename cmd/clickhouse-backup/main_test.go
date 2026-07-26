package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
)

func TestCLI_CallbackDispatchedOnCommandSuccess(t *testing.T) {
	r := require.New(t)
	var (
		mu   sync.Mutex
		got  status.CallbackPayload
		hits int
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
		got = p
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config.DefaultConfig()
	cfg.General.CallbackURL = srv.URL
	cfg.General.CallbackTimeout = "2s"
	cfg.General.CallbackTimeoutDuration = 2 * time.Second

	err := wrapWithCLICallback("create", func(c *cli.Context) error {
		return nil
	})(newTestCLIContext(t, cfg, "create"))
	r.NoError(err)

	mu.Lock()
	defer mu.Unlock()
	r.Equal(1, hits)
	r.Equal(status.SuccessStatus, got.Status)
	r.Equal("", got.Error)
	r.Equal("create", got.Command)
	r.NotEmpty(got.Duration)
	r.NotEmpty(got.OperationId)
}

func TestCLI_CallbackDispatchedOnCommandFailure(t *testing.T) {
	r := require.New(t)
	var (
		mu  sync.Mutex
		got status.CallbackPayload
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		var p status.CallbackPayload
		_ = json.Unmarshal(body, &p)
		mu.Lock()
		got = p
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config.DefaultConfig()
	cfg.General.CallbackURL = srv.URL
	cfg.General.CallbackTimeoutDuration = 2 * time.Second

	actionErr := errors.New("invalid table pattern")
	err := wrapWithCLICallback("create", func(c *cli.Context) error {
		return actionErr
	})(newTestCLIContext(t, cfg, "create"))
	r.Equal(actionErr, err)

	mu.Lock()
	defer mu.Unlock()
	r.Equal(status.ErrorStatus, got.Status)
	r.Equal("invalid table pattern", got.Error)
	r.Equal("create", got.Command)
}

func TestCLI_CallbackFailureDoesNotAffectExitCode(t *testing.T) {
	r := require.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := config.DefaultConfig()
	cfg.General.CallbackURL = srv.URL
	cfg.General.CallbackTimeoutDuration = 2 * time.Second

	err := wrapWithCLICallback("create", func(c *cli.Context) error {
		return nil
	})(newTestCLIContext(t, cfg, "create"))
	r.NoError(err, "callback HTTP failure must not change the command result")
}

func newTestCLIContext(t *testing.T, cfg *config.Config, commandName string) *cli.Context {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yml")
	content := "general:\n  callback_url: \"" + cfg.General.CallbackURL + "\"\n  callback_timeout: \"2s\"\n"
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	app := cli.NewApp()
	app.Commands = []cli.Command{{Name: commandName}}
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.String("config", configPath, "")
	flagSet.Int("command-id", status.NotFromAPI, "")
	ctx := cli.NewContext(app, flagSet, nil)
	ctx.Command = app.Commands[0]
	return ctx
}
