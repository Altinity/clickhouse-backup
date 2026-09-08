package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestCLIStatus_CallbackDispatchedOnCommandSuccess(t *testing.T) {
	r := require.New(t)
	payloads, srv := callbackReceiver(t)
	defer srv.Close()

	err := runWithCLIStatus(context.Background(), newTestCLIContext(t, srv.URL, "create"), "create", func(_ context.Context, c *cli.Command) error {
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
	err := runWithCLIStatus(context.Background(), newTestCLIContext(t, srv.URL, "create"), "create", func(_ context.Context, c *cli.Command) error {
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

	err := runWithCLIStatus(context.Background(), newTestCLIContext(t, srv.URL, "create"), "create", func(_ context.Context, c *cli.Command) error {
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

	cmd := newTestCLIContextWithCommandId(t, srv.URL, "create", 7)
	err := runWithCLIStatus(context.Background(), cmd, "create", func(_ context.Context, c *cli.Command) error { return nil })
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

	cmd := newTestCLIContext(t, srv.URL, "create_remote")
	err := runWithCLIStatus(context.Background(), cmd, "create_remote", func(_ context.Context, c *cli.Command) error { return nil })
	r.NoError(err)

	got := awaitCallback(t, payloads)
	r.Equal("create_remote backup-name", got.Command)
}

// registerCLIStatus must wrap eligible commands wherever they are declared and
// leave everything else untouched, without any command name list of its own.
func TestRegisterCLIStatus_WrapsEligibleCommandsRecursively(t *testing.T) {
	r := require.New(t)
	noop := func(_ context.Context, c *cli.Command) error { return nil }
	commands := []*cli.Command{
		{Name: "create", Action: noop},
		{Name: "list", Action: noop},
		{Name: "server", Action: noop, Commands: []*cli.Command{{Name: "restore", Action: noop}}},
	}
	original := commands[1].Action

	registerCLIStatus(commands)

	r.NotNil(commands[0].Action)
	r.False(sameAction(commands[0].Action, noop), "eligible command `create` must be wrapped")
	r.True(sameAction(commands[1].Action, original), "read-only command `list` must stay untouched")
	r.True(sameAction(commands[2].Action, noop), "supervisor command `server` must stay untouched")
	r.False(sameAction(commands[2].Commands[0].Action, noop), "nested eligible command `restore` must be wrapped")
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

func newTestCLIContext(t *testing.T, callbackURL, commandName string) *cli.Command {
	t.Helper()
	return newTestCLIContextWithCommandId(t, callbackURL, commandName, status.NotFromAPI)
}

func newTestCLIContextWithCommandId(t *testing.T, callbackURL, commandName string, commandId int) *cli.Command {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yml")
	content := "general:\n  callback_url: \"" + callbackURL + "\"\n  callback_timeout: \"2s\"\n"
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// Mirror the real flag layout: main.go declares config and command-id once, on
	// the root command, and both are persistent. The API server passes
	// --command-id *before* the command name, so it is parsed by the root and only
	// a lineage aware lookup can see it from the sub-command.
	args := []string{"clickhouse-backup", "-c", configPath}
	if commandId != status.NotFromAPI {
		args = append(args, "--command-id", strconv.Itoa(commandId))
	}
	args = append(args, commandName)
	if commandName == "create_remote" {
		args = append(args, "backup-name")
	}

	var parsed *cli.Command
	root := &cli.Command{
		Name: "clickhouse-backup",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config", Aliases: []string{"c"}, Value: config.DefaultConfigPath},
			&cli.IntFlag{Name: "command-id", Hidden: true, Value: status.NotFromAPI},
		},
		Commands: []*cli.Command{{
			Name: commandName,
			Action: func(_ context.Context, cmd *cli.Command) error {
				parsed = cmd
				return nil
			},
		}},
	}
	if err := root.Run(context.Background(), args); err != nil {
		t.Fatalf("parse %v: %v", args, err)
	}
	return parsed
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

	err := runWithCLIStatus(context.Background(), newTestCLIContext(t, srv.URL, "create"), "create", func(_ context.Context, c *cli.Command) error { return nil })
	r.NoError(err)

	select {
	case p := <-payloads:
		r.Failf("unexpected callback", "API server mode must not register CLI rows, got %+v", p)
	case <-time.After(300 * time.Millisecond):
	}
}
