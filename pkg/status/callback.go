package status

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pkg/errors"
)

// CallbackPayload is the JSON body posted to callback URLs on command completion.
// Status, Error, and OperationId match the existing API CallbackResponse for
// backward compatibility (Error has no omitempty so success still sends "").
// Command and Duration are optional extras used by CLI/watch callers.
type CallbackPayload struct {
	Status      string `json:"status"`
	Error       string `json:"error"`
	OperationId string `json:"operation_id"`
	Command     string `json:"command,omitempty"`
	Duration    string `json:"duration,omitempty"`
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
