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
	"reflect"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
)

func TestCLIStatus_CallbackDispatchedOnCommandSuccess(t *testing.T) {
	r := require.New(t)
	payloads, srv := callbackReceiver(t)
	defer srv.Close()

	err := runWithCLIStatus(newTestCLIContext(t, srv.URL, "create"), "create", func(c *cli.Context) error {
		return nil
	})
	r.NoError(err)

	got := awaitCallback(t, payloads)
	r.Equal(status.SuccessStatus, got.Status)
	r.Equal("", got.Error)
	r.Equal("create", got.Command)
	r.NotEmpty(got.Duration)
	r.NotEmpty(got.OperationId)
}

func TestCLIStatus_CallbackDispatchedOnCommandFailure(t *testing.T) {
	r := require.New(t)
	payloads, srv := callbackReceiver(t)
	defer srv.Close()

	actionErr := errors.New("invalid table pattern")
	err := runWithCLIStatus(newTestCLIContext(t, srv.URL, "create"), "create", func(c *cli.Context) error {
		return actionErr
	})
	r.ErrorIs(err, actionErr)

	got := awaitCallback(t, payloads)
	r.Equal(status.ErrorStatus, got.Status)
	r.Equal(actionErr.Error(), got.Error)
	r.Equal("create", got.Command)
}

// A broken or slow callback receiver must not change what the command returns.
func TestCLIStatus_CallbackFailureDoesNotAffectExitCode(t *testing.T) {
	r := require.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := runWithCLIStatus(newTestCLIContext(t, srv.URL, "create"), "create", func(c *cli.Context) error {
		return nil
	})
	r.NoError(err)
}

// Runs re-entered by the API server carry --command-id and are already tracked
// and notified by the handler which started them, so they must not notify twice.
func TestCLIStatus_SkippedWhenSpawnedFromAPI(t *testing.T) {
	r := require.New(t)
	payloads, srv := callbackReceiver(t)
	defer srv.Close()

	ctx := newTestCLIContextWithCommandId(t, srv.URL, "create", 7)
	err := runWithCLIStatus(ctx, "create", func(c *cli.Context) error { return nil })
	r.NoError(err)

	select {
	case p := <-payloads:
		r.Failf("unexpected callback", "API spawned run must not notify, got %+v", p)
	case <-time.After(300 * time.Millisecond):
	}
}

// The command line reported to the callback keeps the command name first, so
// status.CallbackEligible and /backup/status filters see the same shape as API runs.
func TestCLIStatus_FullCommandIncludesArguments(t *testing.T) {
	r := require.New(t)
	payloads, srv := callbackReceiver(t)
	defer srv.Close()

	ctx := newTestCLIContext(t, srv.URL, "create_remote")
	err := runWithCLIStatus(ctx, "create_remote", func(c *cli.Context) error { return nil })
	r.NoError(err)

	got := awaitCallback(t, payloads)
	r.Equal("create_remote backup-name", got.Command)
}

// registerCLIStatus must wrap eligible commands wherever they are declared and
// leave everything else untouched, without any command name list of its own.
func TestRegisterCLIStatus_WrapsEligibleCommandsRecursively(t *testing.T) {
	r := require.New(t)
	noop := func(c *cli.Context) error { return nil }
	commands := []cli.Command{
		{Name: "create", Action: noop},
		{Name: "list", Action: noop},
		{Name: "server", Action: noop, Subcommands: []cli.Command{{Name: "restore", Action: noop}}},
	}
	original := commands[1].Action

	registerCLIStatus(commands)

	r.NotNil(commands[0].Action)
	r.False(sameAction(commands[0].Action, noop), "eligible command `create` must be wrapped")
	r.True(sameAction(commands[1].Action, original), "read-only command `list` must stay untouched")
	r.True(sameAction(commands[2].Action, noop), "supervisor command `server` must stay untouched")
	r.False(sameAction(commands[2].Subcommands[0].Action, noop), "nested eligible command `restore` must be wrapped")
}

func sameAction(a, b interface{}) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

func callbackReceiver(t *testing.T) (chan status.CallbackPayload, *httptest.Server) {
	t.Helper()
	payloads := make(chan status.CallbackPayload, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var p status.CallbackPayload
		if err := json.Unmarshal(body, &p); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		payloads <- p
		w.WriteHeader(http.StatusOK)
	}))
	return payloads, srv
}

func awaitCallback(t *testing.T, payloads chan status.CallbackPayload) status.CallbackPayload {
	t.Helper()
	select {
	case p := <-payloads:
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
		return status.CallbackPayload{}
	}
}

func newTestCLIContext(t *testing.T, callbackURL, commandName string) *cli.Context {
	t.Helper()
	return newTestCLIContextWithCommandId(t, callbackURL, commandName, status.NotFromAPI)
}

func newTestCLIContextWithCommandId(t *testing.T, callbackURL, commandName string, commandId int) *cli.Context {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yml")
	content := "general:\n  callback_url: \"" + callbackURL + "\"\n  callback_timeout: \"2s\"\n"
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	app := cli.NewApp()
	app.Commands = []cli.Command{{Name: commandName}}

	// Mirror the real flag layout: main.go declares command-id at app level and
	// every command re-declares it via `Flags: append(cliapp.Flags, ...)`. The API
	// server passes --command-id *before* the command name, so it lands in the app
	// flag set while the command keeps its own default. A helper that puts it only
	// on the command flag set cannot catch a lookup reading the wrong one.
	appSet := flag.NewFlagSet("clickhouse-backup", flag.ContinueOnError)
	appSet.String("config", configPath, "")
	appSet.Int("command-id", commandId, "")
	parent := cli.NewContext(app, appSet, nil)

	cmdSet := flag.NewFlagSet(commandName, flag.ContinueOnError)
	cmdSet.String("config", configPath, "")
	cmdSet.Int("command-id", status.NotFromAPI, "")
	if commandName == "create_remote" {
		if err := cmdSet.Parse([]string{"backup-name"}); err != nil {
			t.Fatalf("parse args: %v", err)
		}
	}
	ctx := cli.NewContext(app, cmdSet, parent)
	ctx.Command = app.Commands[0]
	return ctx
}

// commandIdFromCli must find --command-id where the API server actually puts it:
// before the command name, i.e. in the app flag set, not the command's own copy.
func TestCommandIdFromCli(t *testing.T) {
	r := require.New(t)
	r.Equal(7, commandIdFromCli(newTestCLIContextWithCommandId(t, "", "create", 7)),
		"--command-id passed by the API server before the command name must be visible")
	r.Equal(status.NotFromAPI, commandIdFromCli(newTestCLIContext(t, "", "create")),
		"a plain CLI run must report NotFromAPI")
}

// In an API server process no CLI re-entry may register a status row, even when
// the handler deliberately passes NotFromAPI because the command is listed in
// api.backup_actions_skip_commands.
func TestCLIStatus_SkippedInAPIServerMode(t *testing.T) {
	r := require.New(t)
	payloads, srv := callbackReceiver(t)
	defer srv.Close()

	status.SetAPIServerMode()
	defer status.ResetAPIServerModeForTest()

	err := runWithCLIStatus(newTestCLIContext(t, srv.URL, "create"), "create", func(c *cli.Context) error { return nil })
	r.NoError(err)

	select {
	case p := <-payloads:
		r.Failf("unexpected callback", "API server mode must not register CLI rows, got %+v", p)
	case <-time.After(300 * time.Millisecond):
	}
}
