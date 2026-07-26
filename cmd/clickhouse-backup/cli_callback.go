package main

import (
	"context"
	"strconv"
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
	"create":                {},
	"create_remote":         {},
	"upload":                {},
	"download":              {},
	"restore":               {},
	"restore_remote":        {},
	"delete":                {},
	"rebase":                {},
	"rebalance":             {},
	"clean":                 {},
	"clean_remote_broken":   {},
	"clean_local_broken":    {},
	"clean_broken_retention": {},
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
	cfg := config.GetConfigFromCli(c)
	if cfg == nil || cfg.General.CallbackURL == "" {
		return
	}
	payload := status.CallbackPayload{
		Command:     commandName,
		Duration:    time.Since(start).String(),
		OperationId: resolveCLIOperationId(c.Int("command-id")),
	}
	if cmdErr != nil {
		payload.Status = status.ErrorStatus
		payload.Error = cmdErr.Error()
	} else {
		payload.Status = status.SuccessStatus
		payload.Error = ""
	}
	timeout := cfg.General.CallbackTimeoutDuration
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if cbErr := status.SendCallback(ctx, cfg.General.CallbackURL, payload); cbErr != nil {
		log.Error().Err(cbErr).Str("callback_url", cfg.General.CallbackURL).Msg("callback failed")
	}
}

func resolveCLIOperationId(commandId int) string {
	if commandId != status.NotFromAPI {
		if opId := status.Current.GetOperationId(commandId); opId != "" {
			return opId
		}
		return strconv.Itoa(commandId)
	}
	id, err := uuid.NewUUID()
	if err != nil {
		return ""
	}
	return id.String()
}
