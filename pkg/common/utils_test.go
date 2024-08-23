package common

import (
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
