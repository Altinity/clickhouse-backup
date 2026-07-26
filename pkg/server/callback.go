package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/pkg/errors"
)

// callbackFn is a function which will post a callback when invoked
type callbackFn func(ctx context.Context, v interface{}) []error

// parseCallback parses callback URL(s) from query values, falling back to fallbackURL
// when the callback query param is absent or empty. The returned callback detaches
// caller cancellation while preserving context values, then applies callbackTimeout
// to each outgoing POST. Prefers status.SendCallback for CallbackResponse /
// status.CallbackPayload; other payload types use the legacy marshal path (tests).
func parseCallback(query url.Values, fallbackURL string, callbackTimeout time.Duration) (callbackFn, error) {
	decodedURLs, err := resolveCallbackURLs(query, fallbackURL)
	if err != nil {
		return nil, err
	}
	if len(decodedURLs) == 0 {
		return func(_ context.Context, _ interface{}) []error {
			return nil
		}, nil
	}

	client := &http.Client{}
	return func(ctx context.Context, v interface{}) []error {
		if ctx == nil {
			return []error{errors.New("callback context must not be nil")}
		}
		var errs []error
		for _, callBackURL := range decodedURLs {
			callbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), callbackTimeout)
			err := postCallback(callbackCtx, client, callBackURL, v)
			cancel()
			if err != nil {
				errs = append(errs, err)
			}
		}
		return errs
	}, nil
}

func resolveCallbackURLs(query url.Values, fallbackURL string) ([]string, error) {
	encodedURLs, exist := query["callback"]
	var nonEmpty []string
	if exist {
		for _, v := range encodedURLs {
			if strings.TrimSpace(v) == "" {
				continue
			}
			d, err := url.QueryUnescape(v)
			if err != nil {
				return nil, errors.Wrapf(err, "could not decode url %q", v)
			}
			if strings.TrimSpace(d) == "" {
				continue
			}
			nonEmpty = append(nonEmpty, d)
		}
	}
	if len(nonEmpty) > 0 {
		return nonEmpty, nil
	}
	if strings.TrimSpace(fallbackURL) != "" {
		return []string{fallbackURL}, nil
	}
	return nil, nil
}

func postCallback(ctx context.Context, client *http.Client, callBackURL string, v interface{}) error {
	switch p := v.(type) {
	case status.CallbackPayload:
		return status.SendCallback(ctx, callBackURL, p)
	case *status.CallbackPayload:
		return status.SendCallback(ctx, callBackURL, *p)
	case CallbackResponse:
		return status.SendCallback(ctx, callBackURL, status.CallbackPayload{
			Status:      p.Status,
			Error:       p.Error,
			OperationId: p.OperationId,
		})
	case *CallbackResponse:
		return status.SendCallback(ctx, callBackURL, status.CallbackPayload{
			Status:      p.Status,
			Error:       p.Error,
			OperationId: p.OperationId,
		})
	}

	payload, err := json.Marshal(v)
	if err != nil {
		return errors.Wrapf(err, "error encoding %v", v)
	}
	reader := bytes.NewReader(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callBackURL, reader)
	if err != nil {
		return errors.Wrapf(err, "error creating request to %q", callBackURL)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "error while posting callback to %q", callBackURL)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error while posting callback to %q: status code %d", callBackURL, resp.StatusCode)
	}
	return nil
}
