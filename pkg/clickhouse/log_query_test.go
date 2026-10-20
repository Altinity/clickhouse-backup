package clickhouse

import (
	"bytes"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestLogQueryMasksURLCredentials(t *testing.T) {
	const password = "s3cr&t'p@ss word"
	params := url.Values{}
	params.Add("user", "backup")
	params.Add("pass", password)
	encodedPassword := url.QueryEscape(password)

	testCases := []struct {
		Name     string
		Query    string
		Args     []interface{}
		Expected string
	}{
		{
			Name:     "integration table URL engine",
			Query:    "CREATE TABLE system.backup_actions (command String) ENGINE=URL('http://127.0.0.1:7171/backup/actions?" + params.Encode() + "', JSONEachRow) SETTINGS input_format_skip_unknown_fields=1",
			Expected: "CREATE TABLE system.backup_actions (command String) ENGINE=URL('http://127.0.0.1:7171/backup/actions?pass=[MASKED]&user=backup', JSONEachRow) SETTINGS input_format_skip_unknown_fields=1",
		},
		{
			Name:     "password parameter after another parameter",
			Query:    "CREATE TABLE db.t (x String) ENGINE=URL('https://host:8443/data?user=u&password=" + encodedPassword + "', JSONEachRow)",
			Expected: "CREATE TABLE db.t (x String) ENGINE=URL('https://host:8443/data?user=u&password=[MASKED]', JSONEachRow)",
		},
		{
			Name:     "password in URL userinfo",
			Query:    "CREATE TABLE db.t (x String) ENGINE=URL('https://u:" + encodedPassword + "@host:8443/data', JSONEachRow)",
			Expected: "CREATE TABLE db.t (x String) ENGINE=URL('https://u:[MASKED]@host:8443/data', JSONEachRow)",
		},
		{
			Name:     "query with args",
			Query:    "CREATE TABLE db.t (x String) ENGINE=URL('http://host/data?pass=" + encodedPassword + "', JSONEachRow) COMMENT ?",
			Args:     []interface{}{"c"},
			Expected: `CREATE TABLE db.t (x String) ENGINE=URL('http://host/data?pass=[MASKED]', JSONEachRow) COMMENT ? with args []interface {}{"c"}`,
		},
		{
			Name:     "query without credentials is unchanged",
			Query:    "SELECT name FROM system.tables WHERE database = 'default' AND engine_full LIKE '%://host:9000/%'",
			Expected: "SELECT name FROM system.tables WHERE database = 'default' AND engine_full LIKE '%://host:9000/%'",
		},
	}

	originalLogger := log.Logger
	originalGlobalLevel := zerolog.GlobalLevel()
	t.Cleanup(func() {
		log.Logger = originalLogger
		zerolog.SetGlobalLevel(originalGlobalLevel)
	})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	ch := ClickHouse{Config: &config.ClickHouseConfig{LogSQLQueries: true}}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			var buf bytes.Buffer
			log.Logger = zerolog.New(&buf)

			assert.Equal(t, tc.Query, ch.LogQuery(tc.Query, tc.Args...), "the executed query must keep the credentials")
			output := buf.String()
			assert.NotContains(t, output, password)
			assert.NotContains(t, output, encodedPassword)
			var entry struct {
				Message string `json:"message"`
			}
			assert.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
			assert.Equal(t, tc.Expected, entry.Message)
		})
	}
}
