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
