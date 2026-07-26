package main

import (
	"context"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"
)

// cliCallbackCommands are one-shot commands that should fire general.callback_url
// on completion. watch/server are excluded (watch is per-iteration inside Watch).
var cliCallbackCommands = map[string]struct{}{
	"create":                 {},
	"create_remote":          {},
	"upload":                 {},
	"download":               {},
	"restore":                {},
	"restore_remote":         {},
	"delete":                 {},
	"rebase":                 {},
	"rebalance":              {},
	"clean":                  {},
	"clean_remote_broken":    {},
	"clean_local_broken":     {},
	"clean_broken_retention": {},
}

// applyCLICallbacks wraps top-level command Actions listed in cliCallbackCommands.
// Nested Subcommands are not walked — all allowlisted names must be top-level.
func applyCLICallbacks(commands []cli.Command) {
	for i := range commands {
		cmd := &commands[i]
		if _, ok := cliCallbackCommands[cmd.Name]; !ok || cmd.Action == nil {
			continue
		}
		original, ok := cmd.Action.(func(*cli.Context) error)
		if !ok {
			continue
		}
		name := cmd.Name
		cmd.Action = wrapWithCLICallback(name, original)
	}
}

func wrapWithCLICallback(commandName string, action func(*cli.Context) error) func(*cli.Context) error {
	return func(c *cli.Context) error {
		if _, ok := cliCallbackCommands[commandName]; !ok {
			return action(c)
		}
		start := time.Now()
		err := action(c)
		dispatchCLICallback(c, commandName, start, err)
		return err
	}
}

func dispatchCLICallback(c *cli.Context, commandName string, start time.Time, cmdErr error) {
	// "command-id" is set when spawned by the API server,
	// which already sends a callback via pkg/server.
	// Skip here to prevent double notifications.
	if c.Int("command-id") != status.NotFromAPI {
		return
	}
	cfg := config.GetConfigFromCli(c)
	if cfg == nil || cfg.General.CallbackURL == "" {
		return
	}
	payload := status.CallbackPayload{
		Command:     commandName,
		Duration:    time.Since(start).String(),
		OperationId: newCLIOperationId(),
	}
	if cmdErr != nil {
		payload.Status = status.ErrorStatus
		payload.Error = cmdErr.Error()
	} else {
		payload.Status = status.SuccessStatus
		payload.Error = ""
	}
	timeout := cfg.General.CallbackTimeoutDuration
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if cbErr := status.SendCallback(ctx, cfg.General.CallbackURL, payload); cbErr != nil {
		log.Error().Err(cbErr).Str("callback_url", cfg.General.CallbackURL).Msg("callback failed")
	}
}

func newCLIOperationId() string {
	return uuid.NewString()
}
