package server

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseCallback_NoCallbackConfigured(t *testing.T) {
	r := require.New(t)
	cb, err := parseCallback(url.Values{}, "", time.Second)
	r.NoError(err)
	r.Nil(cb)
}

func TestParseCallback_DecodesAndKeepsEveryQueryURL(t *testing.T) {
	r := require.New(t)
	cb, err := parseCallback(url.Values{"callback": []string{
		"http://localhost:1/good1",
		url.QueryEscape("http://localhost:1/good2?a=b"),
	}}, "", time.Second)
	r.NoError(err)
	r.NotNil(cb)
	r.Equal([]string{"http://localhost:1/good1", "http://localhost:1/good2?a=b"}, cb.URLs)
	r.Equal(time.Second, cb.Timeout)
}

func TestParseCallback_RejectsUndecodableURL(t *testing.T) {
	r := require.New(t)
	cb, err := parseCallback(url.Values{"callback": []string{"%zz"}}, "", time.Second)
	r.Error(err)
	r.Nil(cb)
}

// general.callback_url is a fallback, an explicit ?callback= always wins.
func TestParseCallback_QueryParamOverridesGlobalCallback(t *testing.T) {
	r := require.New(t)
	cb, err := parseCallback(url.Values{"callback": []string{"http://localhost:1/from-query"}}, "http://localhost:1/global", time.Second)
	r.NoError(err)
	r.NotNil(cb)
	r.Equal([]string{"http://localhost:1/from-query"}, cb.URLs)
}

func TestParseCallback_GlobalCallbackUsedWhenQueryParamMissing(t *testing.T) {
	r := require.New(t)
	cb, err := parseCallback(url.Values{}, "http://localhost:1/global", time.Second)
	r.NoError(err)
	r.NotNil(cb)
	r.Equal([]string{"http://localhost:1/global"}, cb.URLs)
}

// `?callback=` with an empty or blank value is treated as absent, not as a
// request to disable the globally configured callback.
func TestParseCallback_EmptyQueryParamFallsBackToGlobal(t *testing.T) {
	r := require.New(t)
	for _, raw := range []string{"", "   ", "%20"} {
		cb, err := parseCallback(url.Values{"callback": []string{raw}}, "http://localhost:1/global", time.Second)
		r.NoError(err, "raw=%q", raw)
		r.NotNil(cb, "raw=%q", raw)
		r.Equal([]string{"http://localhost:1/global"}, cb.URLs, "raw=%q", raw)
	}
}
