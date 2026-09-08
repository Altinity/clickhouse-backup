package server

import (
	"net/url"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/pkg/errors"
)

// parseCallback resolves the completion callback for a request. The `callback`
// query parameter may be repeated, and takes precedence over general.callback_url,
// which is used only when no non-empty query parameter is present.
// A nil result means "do not notify".
func parseCallback(query url.Values, fallbackURL string, timeout time.Duration) (*status.CallbackConfig, error) {
	var urls []string
	for _, v := range query["callback"] {
		if strings.TrimSpace(v) == "" {
			continue
		}
		decoded, err := url.QueryUnescape(v)
		if err != nil {
			return nil, errors.Wrapf(err, "could not decode url %q", v)
		}
		if strings.TrimSpace(decoded) == "" {
			continue
		}
		urls = append(urls, decoded)
	}
	if len(urls) == 0 && strings.TrimSpace(fallbackURL) != "" {
		urls = []string{fallbackURL}
	}
	if len(urls) == 0 {
		return nil, nil
	}
	return &status.CallbackConfig{URLs: urls, Timeout: timeout}, nil
}
