package common

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

func TestTablePathEncode(t *testing.T) {
	r := require.New(t)
	str := `!@#$^&*()+-=[]{}|;':\",./<>?~`
	expected := "%21%40%23%24%5E%26%2A%28%29%2B%2D%3D%5B%5D%7B%7D%7C%3B%27%3A%5C%22%2C%2E%2F%3C%3E%3F%7E"

	actual := TablePathEncode(str)
	r.Equal(expected, actual)
	decoded, err := url.PathUnescape(actual)
	r.NoError(err)
	r.Equal(str, decoded)
}

func TestTablePathDecodeRoundTrip(t *testing.T) {
	r := require.New(t)
	cases := []string{
		"plain_alphanum",
		"my-db",
		"my db with spaces",
		"with.dots",
		"weird(parens)",
		`!@#$^&*()+-=[]{}|;':\",./<>?~`,
		"unicode-привет",
		"", // empty
	}
	for _, in := range cases {
		got := TablePathDecode(TablePathEncode(in))
		r.Equal(in, got, "round-trip mismatch for %q", in)
	}
}

func TestTablePathDecode_PreservesUnencoded(t *testing.T) {
	// TablePathDecode of a plain (unencoded) string should be a no-op.
	r := require.New(t)
	r.Equal("foo_bar", TablePathDecode("foo_bar"))
}

func TestTablePathDecode_OnInvalidInputReturnsAsIs(t *testing.T) {
	// An invalid percent-escape (e.g. "%ZZ") should NOT panic; the function
	// returns the input verbatim. This guards against accidentally
	// double-decoding or feeding hostile data through.
	r := require.New(t)
	r.Equal("bad%ZZescape", TablePathDecode("bad%ZZescape"))
}

func TestCompareMaps(t *testing.T) {
	r := require.New(t)
	map1 := map[string]interface{}{
		"key1": "value1",
		"key2": 2,
	}
	map2 := map[string]interface{}{
		"key1": "value1",
		"key2": 2,
	}
	map3 := map[string]interface{}{
		"key1": "value1",
		"key2": "false",
	}

	r.True(CompareMaps(map1, map2))
	r.False(CompareMaps(map1, map3))
	oldParams := map[string]interface{}{}
	r.NoError(json.Unmarshal([]byte(`{
		"diffFrom":       "",
		"diffFromRemote": "",
		"partitions":     [],
		"schemaOnly":     false,
		"tablePattern":   ""
	}`), &oldParams))
	newParams := map[string]interface{}{
		"diffFrom": "", "diffFromRemote": "", "partitions": []string{}, "schemaOnly": false, "tablePattern": "",
	}
	r.True(CompareMaps(oldParams, newParams))

}
