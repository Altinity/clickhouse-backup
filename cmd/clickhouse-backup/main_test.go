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
	"sync/atomic"
	"testing"

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

	err := wrapWithCLICallback("create", func(c *cli.Context) error {
		return nil
	})(newTestCLIContext(t, cfg, "create"))
	r.NoError(err, "callback HTTP failure must not change the command result")
}

// Ensures API-spawned CLI runs (with --command-id) skip callbacks on both success
// and failure, leaving notification handling to pkg/server.
func TestCLI_CallbackSkippedWhenSpawnedFromAPI(t *testing.T) {
	r := require.New(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := config.DefaultConfig()
	cfg.General.CallbackURL = srv.URL

	actionRan := false
	err := wrapWithCLICallback("create", func(c *cli.Context) error {
		actionRan = true
		return nil
	})(newTestCLIContextWithCommandId(t, cfg, "create", 42))
	r.NoError(err)
	r.True(actionRan, "wrapped action must still run for API-spawned invocations")
	r.Equal(int32(0), hits.Load(), "API-spawned run must not fire the CLI callback (the API server already dispatches one)")

	// same guard applies when the command fails: the API errorCallback owns it
	actionErr := errors.New("create failed")
	err = wrapWithCLICallback("create", func(c *cli.Context) error {
		return actionErr
	})(newTestCLIContextWithCommandId(t, cfg, "create", 42))
	r.Equal(actionErr, err)
	r.Equal(int32(0), hits.Load(), "API-spawned failed run must not fire the CLI callback either")
}

// Ensures every cliCallbackCommands entry names a real top-level command from main.go,
// and that applyCLICallbacks can wrap each of them. Nested Subcommands are not supported.
func TestCLI_CallbackAllowlistMatchesTopLevelCommands(t *testing.T) {
	r := require.New(t)
	// Keep in sync with top-level Name fields in main.go's cliapp.Commands.
	topLevelNames := []string{
		"tables", "create", "create_remote", "upload", "list", "download",
		"rebase", "rebalance", "restore", "restore_remote", "delete",
		"default-config", "print-config", "clean", "clean_remote_broken",
		"clean_local_broken", "clean_broken_retention", "watch", "acvp", "server",
	}
	topLevel := make(map[string]struct{}, len(topLevelNames))
	commands := make([]cli.Command, 0, len(topLevelNames))
	for _, name := range topLevelNames {
		topLevel[name] = struct{}{}
		name := name
		commands = append(commands, cli.Command{
			Name: name,
			Action: func(_ *cli.Context) error {
				return nil
			},
		})
	}

	for name := range cliCallbackCommands {
		_, ok := topLevel[name]
		r.True(ok, "cliCallbackCommands entry %q is not a known top-level command in main.go", name)
	}
	for _, excluded := range []string{"watch", "server", "tables", "list", "default-config", "print-config", "acvp"} {
		_, ok := cliCallbackCommands[excluded]
		r.False(ok, "%q must not be in cliCallbackCommands", excluded)
	}

	applyCLICallbacks(commands)
	for _, cmd := range commands {
		if _, ok := cliCallbackCommands[cmd.Name]; !ok {
			continue
		}
		_, ok := cmd.Action.(func(*cli.Context) error)
		r.True(ok, "allowlisted command %q must keep a wrapable Action after applyCLICallbacks", cmd.Name)
		r.Nil(cmd.Subcommands, "allowlisted command %q must be top-level (no Subcommands); wrapping does not walk nested commands", cmd.Name)
	}
}

func newTestCLIContext(t *testing.T, cfg *config.Config, commandName string) *cli.Context {
	t.Helper()
	return newTestCLIContextWithCommandId(t, cfg, commandName, status.NotFromAPI)
}

func newTestCLIContextWithCommandId(t *testing.T, cfg *config.Config, commandName string, commandId int) *cli.Context {
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
	flagSet.Int("command-id", commandId, "")
	ctx := cli.NewContext(app, flagSet, nil)
	ctx.Command = app.Commands[0]
	return ctx
}
