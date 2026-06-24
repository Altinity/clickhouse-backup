package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
)

// callbackFn is a function which will post a callback when invoked
type callbackFn func(ctx context.Context, v interface{}) []error

// parseCallback parses a callback URL from URL query values and returns a closure which can send
// a payload back to the specified URL when invoked.
func parseCallback(query url.Values) (callbackFn, error) {
	encodedURLs, exist := query["callback"]
	if !exist {
		noOpCallback := func(_ context.Context, _ interface{}) []error {
			return nil
		}
		return noOpCallback, nil
	}

	decodedURLs := make([]string, len(encodedURLs))
	for i, v := range encodedURLs {
		d, err := url.QueryUnescape(v)
		if err != nil {
			return nil, errors.Wrapf(err, "could not decode url %q", v)
		}
		decodedURLs[i] = d
	}

	client := &http.Client{}
	return func(ctx context.Context, v interface{}) []error {
		payload, err := json.Marshal(v)
		if err != nil {
			return []error{errors.Wrapf(err, "error encoding %v", v)}
		}

		var errs []error
		for _, callBackURL := range decodedURLs {
			reader := bytes.NewReader(payload)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, callBackURL, reader)
			if err != nil {
				errs = append(errs, errors.Wrapf(err, "error creating request to %q", callBackURL))
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				errs = append(
					errs,
					errors.Wrapf(err, "error while posting callback to %q", callBackURL),
				)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				errs = append(
					errs,
					fmt.Errorf("error while posting callback to %q: status code %d", callBackURL, resp.StatusCode),
				)
			}
		}
		return errs
	}, nil
}
