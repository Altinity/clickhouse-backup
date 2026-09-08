package main

import (
	"context"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"

	"github.com/google/uuid"
	"github.com/urfave/cli/v3"
)

// registerCLIStatus wraps every command Action so a one-shot CLI run registers
// itself in status.Current, exactly like an API request does. Completion
// callbacks are then emitted from the single place which owns them,
// status.AsyncStatus.Stop, instead of a CLI specific dispatcher.
//
// Which commands are worth notifying about is decided by status.CallbackEligible,
// this package deliberately holds no list of command names.
func registerCLIStatus(commands []*cli.Command) {
	for _, cmd := range commands {
		registerCLIStatus(cmd.Commands)
		if cmd.Action == nil || !status.CallbackEligible(cmd.Name) {
			continue
		}
		action := cmd.Action
		name := cmd.Name
		cmd.Action = func(ctx context.Context, c *cli.Command) error {
			return runWithCLIStatus(ctx, c, name, action)
		}
	}
}

func runWithCLIStatus(ctx context.Context, c *cli.Command, name string, action cli.ActionFunc) error {
	// The API server re-enters the CLI in process, see
	// APIServer.httpBackupActionsHandler. Such runs are already tracked and
	// notified by the handler which started them — or deliberately untracked, when
	// the command is listed in api.backup_actions_skip_commands, in which case
	// --command-id is status.NotFromAPI and only the server mode marker tells the
	// two apart.
	if status.APIServerMode() || commandIdFromCli(c) != status.NotFromAPI {
		return action(ctx, c)
	}
	cfg := config.GetConfigFromCli(c)
	commandId, _ := status.Current.StartWithCallback(cliFullCommand(c, name), uuid.NewString(), cliCallback(cfg))
	err := action(ctx, c)
	status.Current.Stop(commandId, err)
	return err
}

// cliFullCommand renders the command the way API handlers do, name first so
// status.CallbackEligible and /backup/status filters see the same shape.
func cliFullCommand(c *cli.Command, name string) string {
	if args := c.Args(); args.Present() {
		return name + " " + strings.Join(args.Slice(), " ")
	}
	return name
}

func cliCallback(cfg *config.Config) *status.CallbackConfig {
	if cfg == nil || cfg.General.CallbackURL == "" {
		return nil
	}
	return &status.CallbackConfig{
		URLs:    []string{cfg.General.CallbackURL},
		Timeout: cfg.General.CallbackTimeoutDuration,
	}
}
