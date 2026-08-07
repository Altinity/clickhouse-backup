package status

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// CallbackPayload is the JSON body posted to callback URLs on command completion.
// Status, Error, and OperationId match the legacy API callback payload for
// backward compatibility (Error has no omitempty so success still sends "").
// Command and Duration are optional extras.
type CallbackPayload struct {
	Status      string `json:"status"`
	Error       string `json:"error"`
	OperationId string `json:"operation_id"`
	Command     string `json:"command,omitempty"`
	Duration    string `json:"duration,omitempty"`
}

// CallbackConfig describes where to notify when a command finishes.
// It is attached to a status row by the caller which starts the command
// (API handler, CLI wrapper or watch iteration).
type CallbackConfig struct {
	URLs    []string
	Timeout time.Duration
}

// DefaultCallbackTimeout is used when CallbackConfig.Timeout is not positive.
const DefaultCallbackTimeout = 5 * time.Second

// callbackCommands are the command names which produce a completion callback.
// Read-only commands (list, tables, status, ...) and long-running supervisors
// (server, watch) are deliberately absent, watch notifies per iteration instead.
var callbackCommands = map[string]struct{}{
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

// CallbackEligible reports whether a full command line ("create_remote --tables=x name")
// belongs to a command which produces a completion callback. Only the first token matters.
func CallbackEligible(fullCommand string) bool {
	name, _, _ := strings.Cut(strings.TrimSpace(fullCommand), " ")
	_, ok := callbackCommands[name]
	return ok
}

// notify posts the completion payload to every configured URL. It is invoked
// from Stop in a separate goroutine, so the status lock is never held here and
// a slow or broken receiver can not stall the command which just finished.
func notify(cb *CallbackConfig, payload CallbackPayload) {
	timeout := cb.Timeout
	if timeout <= 0 {
		timeout = DefaultCallbackTimeout
	}
	for _, callbackURL := range cb.URLs {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := SendCallback(ctx, callbackURL, payload)
		cancel()
		if err != nil {
			log.Error().Err(err).Str("callback_url", callbackURL).Str("command", payload.Command).Msg("callback failed")
		}
	}
}

// SendCallback POSTs payload as JSON to callbackURL. The caller owns timeouts via ctx.
func SendCallback(ctx context.Context, callbackURL string, payload CallbackPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrap(err, "error encoding callback payload")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		return errors.Wrapf(err, "error creating callback request to %q", callbackURL)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "error while posting callback to %q", callbackURL)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error while posting callback to %q: status code %d", callbackURL, resp.StatusCode)
	}
	return nil
}
