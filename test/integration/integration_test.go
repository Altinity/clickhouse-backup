//go:build integration

package main

import (
	"context"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/log_helper"
	"github.com/Altinity/clickhouse-backup/pkg/partition"
	"github.com/Altinity/clickhouse-backup/pkg/status"
	"github.com/Altinity/clickhouse-backup/pkg/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	stdlog "log"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/rs/zerolog/log"
	"golang.org/x/mod/semver"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dbNameAtomic = "_test.ДБ_atomic_"
const dbNameOrdinary = "_test.ДБ_ordinary_"
const dbNameMySQL = "mysql_db"
const dbNamePostgreSQL = "pgsql_db"
const Issue331Atomic = "_issue331._atomic_"
const Issue331Ordinary = "_issue331.ordinary_"

type TestDataStruct struct {
	Database           string
	DatabaseEngine     string
	Name               string
	Schema             string
	Rows               []map[string]interface{}
	Fields             []string
	OrderBy            string
	IsMaterializedView bool
	IsView             bool
	IsDictionary       bool
	IsFunction         bool
	SkipInsert         bool
	CheckDatabaseOnly  bool
}

var testData = []TestDataStruct{
	{
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   ".inner.table1",
		Schema: "(Date Date, TimeStamp DateTime, Logger String) ENGINE = MergeTree(Date, (TimeStamp, Logger), 8192)",
		Rows: []map[string]interface{}{
			{"Date": toDate("2018-10-23"), "TimeStamp": toTS("2018-10-23 07:37:14"), "Logger": "One"},
			{"Date": toDate("2018-10-23"), "TimeStamp": toTS("2018-10-23 07:37:15"), "Logger": "Two"},
			{"Date": toDate("2018-10-24"), "TimeStamp": toTS("2018-10-24 07:37:16"), "Logger": "Three"},
			{"Date": toDate("2018-10-24"), "TimeStamp": toTS("2018-10-24 07:37:17"), "Logger": "Four"},
			{"Date": toDate("2019-10-25"), "TimeStamp": toTS("2019-01-25 07:37:18"), "Logger": "Five"},
			{"Date": toDate("2019-10-25"), "TimeStamp": toTS("2019-01-25 07:37:19"), "Logger": "Six"},
		},
		Fields:  []string{"Date", "TimeStamp", "Logger"},
		OrderBy: "TimeStamp",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   "2. Таблица №2",
		Schema: "(id UInt64, User String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"id": uint64(1), "User": "Alice"},
			{"id": uint64(2), "User": "Bob"},
			{"id": uint64(3), "User": "John"},
			{"id": uint64(4), "User": "Frank"},
			{"id": uint64(5), "User": "Nancy"},
			{"id": uint64(6), "User": "Brandon"},
		},
		Fields:  []string{"id", "User"},
		OrderBy: "id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   "-table-3-",
		Schema: "(TimeStamp DateTime, Item String, Date Date MATERIALIZED toDate(TimeStamp)) ENGINE = MergeTree() PARTITION BY Date ORDER BY TimeStamp SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"TimeStamp": toTS("2018-10-23 07:37:14"), "Item": "One"},
			{"TimeStamp": toTS("2018-10-23 07:37:15"), "Item": "Two"},
			{"TimeStamp": toTS("2018-10-24 07:37:16"), "Item": "Three"},
			{"TimeStamp": toTS("2018-10-24 07:37:17"), "Item": "Four"},
			{"TimeStamp": toTS("2019-01-25 07:37:18"), "Item": "Five"},
			{"TimeStamp": toTS("2019-01-25 07:37:19"), "Item": "Six"},
		},
		Fields:  []string{"TimeStamp", "Item"},
		OrderBy: "TimeStamp",
	}, {
		Database: Issue331Atomic, DatabaseEngine: "Atomic",
		Name:   Issue331Atomic, // need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		Schema: fmt.Sprintf("(`%s` UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) ENGINE = MergeTree PARTITION BY `%s` ORDER BY (`%s`, Col1, Col2, Col3, Col4, Col5) SETTINGS index_granularity = 8192", Issue331Atomic, Issue331Atomic, Issue331Atomic),
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{Issue331Atomic: uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{Issue331Atomic, "Col1", "Col2", "Col3", "Col4", "Col5"},
		OrderBy: Issue331Atomic,
	}, {
		Database: Issue331Ordinary, DatabaseEngine: "Ordinary",
		Name:   Issue331Ordinary, // need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		Schema: fmt.Sprintf("(`%s` String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMM(order_time) ORDER BY (order_time, `%s`)", Issue331Ordinary, Issue331Ordinary),
		Rows: []map[string]interface{}{
			{Issue331Ordinary: "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{Issue331Ordinary: "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{Issue331Ordinary, "order_time", "amount"},
		OrderBy: Issue331Ordinary,
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   "yuzhichang_table3",
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(order_time) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{"order_id": "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   "yuzhichang_table4",
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY (toYYYYMM(order_time), order_id) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{"order_id": "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   "jbod",
		Schema: "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'jbod'",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i)})
			}
			return result
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	}, {
		Database: dbNameAtomic, DatabaseEngine: "Atomic",
		Name:   "jbod",
		Schema: "(t DateTime, id UInt64) Engine=MergeTree PARTITION BY (toYYYYMM(t), id % 4) ORDER BY id SETTINGS storage_policy = 'jbod'",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"t": toTS("2022-01-01 00:00:00"), "id": uint64(i)})
			}
			return result
		}(),
		Fields:  []string{"t", "id"},
		OrderBy: "id",
	}, {
		Database: dbNameAtomic, DatabaseEngine: "Atomic",
		Name:   "mv_src_table",
		Schema: "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','replica1') ORDER BY id",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i)})
			}
			return result
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
	{
		Database:       dbNameAtomic,
		DatabaseEngine: "Atomic",
		Name:           "mv_dst_table",
		Schema:         "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}/{uuid}','replica1') ORDER BY id",
		SkipInsert:     true,
		Rows: func() []map[string]interface{} {
			return []map[string]interface{}{
				{"id": uint64(0)},
				{"id": uint64(99)},
			}
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
	{
		Database:           dbNameAtomic,
		DatabaseEngine:     "Atomic",
		IsMaterializedView: true,
		Name:               "mv_max_with_inner",
		Schema:             fmt.Sprintf("(id UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}/{uuid}','replica1') ORDER BY id AS SELECT max(id) AS id FROM `%s`.`mv_src_table`", dbNameAtomic),
		SkipInsert:         true,
		Rows: func() []map[string]interface{} {
			return []map[string]interface{}{
				{"id": uint64(99)},
			}
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
	{
		Database:       dbNameAtomic,
		DatabaseEngine: "Atomic",
		IsView:         true,
		Name:           "test_view",
		Schema:         fmt.Sprintf(" AS SELECT count() AS cnt FROM `%s`.`mv_src_table`", dbNameAtomic),
		SkipInsert:     true,
		Rows: func() []map[string]interface{} {
			return []map[string]interface{}{
				{"cnt": uint64(100)},
			}
		}(),
		Fields:  []string{"cnt"},
		OrderBy: "cnt",
	},
	{
		Database:           dbNameAtomic,
		DatabaseEngine:     "Atomic",
		IsMaterializedView: true,
		Name:               "mv_max_with_dst",
		Schema:             fmt.Sprintf(" TO `%s`.`mv_dst_table` AS SELECT max(id) AS id FROM `%s`.mv_src_table", dbNameAtomic, dbNameAtomic),
		SkipInsert:         true,
		Rows: func() []map[string]interface{} {
			return []map[string]interface{}{
				{"id": uint64(0)},
				{"id": uint64(99)},
			}
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
	{
		Database:           dbNameAtomic,
		DatabaseEngine:     "Atomic",
		IsMaterializedView: true,
		Name:               "mv_min_with_nested_depencency",
		Schema:             fmt.Sprintf(" TO `%s`.`mv_dst_table` AS SELECT min(id) * 2 AS id FROM `%s`.mv_src_table", dbNameAtomic, dbNameAtomic),
		SkipInsert:         true,
		Rows: func() []map[string]interface{} {
			return []map[string]interface{}{
				{"id": uint64(0)},
				{"id": uint64(99)},
			}
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
	{
		Database:       dbNameAtomic,
		DatabaseEngine: "Atomic",
		IsDictionary:   true,
		Name:           "dict_example",
		Schema: fmt.Sprintf(
			" (`%s` UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) PRIMARY KEY `%s` "+
				" SOURCE(CLICKHOUSE(host 'localhost' port 9000 db '%s' table '%s' user 'default' password ''))"+
				" LAYOUT(HASHED()) LIFETIME(60)",
			Issue331Atomic, Issue331Atomic, Issue331Atomic, Issue331Atomic), // same table and name need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		SkipInsert: true,
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{Issue331Atomic: uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{},
		OrderBy: Issue331Atomic,
	},
	{
		Database: dbNameMySQL, DatabaseEngine: "MySQL('mysql:3306','mysql','root','root')",
		CheckDatabaseOnly: true,
	},
	{
		Database: dbNamePostgreSQL, DatabaseEngine: "PostgreSQL('pgsql:5432','postgres','root','root')",
		CheckDatabaseOnly: true,
	},
	{
		IsFunction: true,
		Name:       "test_function",
		Schema:     fmt.Sprintf(" AS (a, b) -> a+b"),
		SkipInsert: true,
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 3; i++ {
				result = append(result, map[string]interface{}{"test_result": uint64(i + (i + 1))})
			}
			return result
		}(),
	},
}

var incrementData = []TestDataStruct{
	{
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   ".inner.table1",
		Schema: "(Date Date, TimeStamp DateTime, Logger String) ENGINE = MergeTree(Date, (TimeStamp, Logger), 8192)",
		Rows: []map[string]interface{}{
			{"Date": toDate("2019-10-26"), "TimeStamp": toTS("2019-01-26 07:37:19"), "Logger": "Seven"},
		},
		Fields:  []string{"Date", "TimeStamp", "Logger"},
		OrderBy: "TimeStamp",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   "2. Таблица №2",
		Schema: "(id UInt64, User String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"id": uint64(7), "User": "Alice"},
			{"id": uint64(8), "User": "Bob"},
			{"id": uint64(9), "User": "John"},
			{"id": uint64(10), "User": "Frank"},
		},
		Fields:  []string{"id", "User"},
		OrderBy: "id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   "-table-3-",
		Schema: "(TimeStamp DateTime, Item String, Date Date MATERIALIZED toDate(TimeStamp)) ENGINE = MergeTree() PARTITION BY Date ORDER BY TimeStamp SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"TimeStamp": toTS("2019-01-26 07:37:18"), "Item": "Seven"},
			{"TimeStamp": toTS("2019-01-27 07:37:19"), "Item": "Eight"},
		},
		Fields:  []string{"TimeStamp", "Item"},
		OrderBy: "TimeStamp",
	}, {
		Database: Issue331Atomic, DatabaseEngine: "Atomic",
		Name:   Issue331Atomic, // need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		Schema: fmt.Sprintf("(`%s` UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) ENGINE = MergeTree PARTITION BY `%s` ORDER BY (`%s`, Col1, Col2, Col3, Col4, Col5) SETTINGS index_granularity = 8192", Issue331Atomic, Issue331Atomic, Issue331Atomic),
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 200; i < 220; i++ {
				result = append(result, map[string]interface{}{Issue331Atomic: uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{Issue331Atomic, "Col1", "Col2", "Col3", "Col4", "Col5"},
		OrderBy: Issue331Atomic,
	}, {
		Database: Issue331Ordinary, DatabaseEngine: "Ordinary",
		Name:   Issue331Ordinary, // need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		Schema: fmt.Sprintf("(`%s` String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMM(order_time) ORDER BY (order_time, `%s`)", Issue331Ordinary, Issue331Ordinary),
		Rows: []map[string]interface{}{
			{Issue331Ordinary: "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{Issue331Ordinary: "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{Issue331Ordinary, "order_time", "amount"},
		OrderBy: Issue331Ordinary,
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   "yuzhichang_table3",
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(order_time) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{"order_id": "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Name:   "yuzhichang_table4",
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY (toYYYYMM(order_time), order_id) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{"order_id": "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameAtomic, DatabaseEngine: "Atomic",
		Name:   "jbod",
		Schema: "(t DateTime, id UInt64) Engine=MergeTree PARTITION BY (toYYYYMM(t), id % 4) ORDER BY id SETTINGS storage_policy = 'jbod'",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 100; i < 200; i++ {
				result = append(result, map[string]interface{}{"t": toTS("2022-02-01 00:00:00"), "id": uint64(i)})
			}
			return result
		}(),
		Fields:  []string{"t", "id"},
		OrderBy: "id",
	},
}

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true, TimeFormat: "2006-01-02 15:04:05.000"}
	log.Logger = zerolog.New(zerolog.SyncWriter(consoleWriter)).With().Timestamp().Logger()
	stdlog.SetOutput(log.Logger)
	logLevel := "info"
	if os.Getenv("LOG_LEVEL") != "" {
		logLevel = os.Getenv("LOG_LEVEL")
	}
	log_helper.SetLogLevelFromString(logLevel)
}

func TestDoRestoreRBAC(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.4") == -1 {
		t.Skipf("Test skipped, RBAC not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	ch := &TestClickHouse{}
	r := require.New(t)

	ch.connectWithWait(r, 1*time.Second, 10*time.Second)

	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_rbac")
	ch.queryWithNoError(r, "CREATE TABLE default.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")

	ch.queryWithNoError(r, "DROP SETTINGS PROFILE  IF EXISTS test_rbac")
	ch.queryWithNoError(r, "DROP QUOTA IF EXISTS test_rbac")
	ch.queryWithNoError(r, "DROP ROW POLICY IF EXISTS test_rbac ON default.test_rbac")
	ch.queryWithNoError(r, "DROP ROLE IF EXISTS test_rbac")
	ch.queryWithNoError(r, "DROP USER IF EXISTS test_rbac")

	log.Info().Msg("create RBAC related objects")
	ch.queryWithNoError(r, "CREATE SETTINGS PROFILE test_rbac SETTINGS max_execution_time=60")
	ch.queryWithNoError(r, "CREATE ROLE test_rbac SETTINGS PROFILE 'test_rbac'")
	ch.queryWithNoError(r, "CREATE USER test_rbac IDENTIFIED BY 'test_rbac' DEFAULT ROLE test_rbac")
	ch.queryWithNoError(r, "CREATE QUOTA test_rbac KEYED BY user_name FOR INTERVAL 1 hour NO LIMITS TO test_rbac")
	ch.queryWithNoError(r, "CREATE ROW POLICY test_rbac ON default.test_rbac USING 1=1 AS RESTRICTIVE TO test_rbac")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "--backup-rbac", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	log.Info().Msg("drop all RBAC related objects after backup")
	ch.queryWithNoError(r, "DROP SETTINGS PROFILE test_rbac")
	ch.queryWithNoError(r, "DROP QUOTA test_rbac")
	ch.queryWithNoError(r, "DROP ROW POLICY test_rbac ON default.test_rbac")
	ch.queryWithNoError(r, "DROP ROLE test_rbac")
	ch.queryWithNoError(r, "DROP USER test_rbac")

	ch.chbackend.Close()
	r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker-compose", "-f", os.Getenv("COMPOSE_FILE"), "restart", "clickhouse"))
	ch.connectWithWait(r, 2*time.Second, 10*time.Second)

	log.Info().Msg("download+restore RBAC")
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", "--rbac", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	// we can't restart clickhouse inside container, we need restart container
	ch.chbackend.Close()
	r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker-compose", "-f", os.Getenv("COMPOSE_FILE"), "restart", "clickhouse"))
	ch.connectWithWait(r, 2*time.Second, 10*time.Second)

	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	rbacTypes := map[string]string{
		"PROFILES": "test_rbac",
		"QUOTAS":   "test_rbac",
		"POLICIES": "test_rbac ON default.test_rbac",
		"ROLES":    "test_rbac",
		"USERS":    "test_rbac",
	}
	for rbacType, expectedValue := range rbacTypes {
		var rbacRows []struct {
			Name string `ch:"name"`
		}
		err := ch.chbackend.Select(&rbacRows, fmt.Sprintf("SHOW %s", rbacType))
		r.NoError(err)
		found := false
		for _, row := range rbacRows {
			if expectedValue == row.Name {
				found = true
				break
			}
		}
		if !found {
			r.NoError(dockerExec("clickhouse", "cat", "/var/log/clickhouse-server/clickhouse-server.log"))
			r.Failf("result for SHOW %s, %v doesn't contain %v", rbacType, rbacRows, expectedValue)
		}
	}
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "test_rbac_backup"))

	ch.queryWithNoError(r, "DROP SETTINGS PROFILE test_rbac")
	ch.queryWithNoError(r, "DROP QUOTA test_rbac")
	ch.queryWithNoError(r, "DROP ROW POLICY test_rbac ON default.test_rbac")
	ch.queryWithNoError(r, "DROP ROLE test_rbac")
	ch.queryWithNoError(r, "DROP USER test_rbac")

	ch.chbackend.Close()

}

func TestDoRestoreConfigs(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "1.1.54391") == -1 {
		t.Skipf("Test skipped, users.d is not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Millisecond, 1*time.Second)
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_rbac")
	ch.queryWithNoError(r, "CREATE TABLE default.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")

	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	r.NoError(dockerExec("clickhouse", "bash", "-ce", "echo '<yandex><profiles><default><empty_result_for_aggregation_by_empty_set>1</empty_result_for_aggregation_by_empty_set></default></profiles></yandex>' > /etc/clickhouse-server/users.d/test_config.xml"))

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "--configs", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_configs_backup"))

	ch.chbackend.Close()
	ch.connectWithWait(r, 2*time.Second, 10*time.Second)

	selectEmptyResultForAggQuery :=
		"SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"
	var settings string
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&settings, selectEmptyResultForAggQuery))
	r.Equal("1", settings, "expect empty_result_for_aggregation_by_empty_set=1")

	r.NoError(dockerExec("clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", "test_configs_backup"))

	ch.chbackend.Close()
	ch.connectWithWait(r, 2*time.Second, 10*time.Second)

	settings = ""
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
	r.Equal("0", settings, "expect empty_result_for_aggregation_by_empty_set=0")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", "--configs", "test_configs_backup"))
	err := ch.chbackend.Query("SYSTEM RELOAD CONFIG")
	r.NoError(err)
	ch.chbackend.Close()
	ch.connectWithWait(r, 2*time.Second, 1*time.Second)

	settings = ""
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
	r.Equal("1", settings, "expect empty_result_for_aggregation_by_empty_set=1")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml"))

	ch.chbackend.Close()
}

func TestIntegrationS3(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	runMainIntegrationScenario(t, "S3")
}

func TestIntegrationGCS(t *testing.T) {
	if isTestShouldSkip("GCS_TESTS") {
		t.Skip("Skipping GCS integration tests...")
		return
	}
	r := require.New(t)
	r.NoError(dockerCP("config-gcs.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	installDebIfNotExists(r, "clickhouse", "ca-certificates")
	runMainIntegrationScenario(t, "GCS")
}

func TestIntegrationAzure(t *testing.T) {
	if isTestShouldSkip("AZURE_TESTS") {
		t.Skip("Skipping Azure integration tests...")
		return
	}
	r := require.New(t)
	r.NoError(dockerCP("config-azblob.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	installDebIfNotExists(r, "clickhouse", "ca-certificates")
	runMainIntegrationScenario(t, "AZBLOB")
}

func TestIntegrationSFTPAuthPassword(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-sftp-auth-password.yaml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	runMainIntegrationScenario(t, "SFTP")
}

func TestIntegrationFTP(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-ftp.yaml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	runMainIntegrationScenario(t, "FTP")
}

func TestIntegrationSFTPAuthKey(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-sftp-auth-key.yaml", "clickhouse:/etc/clickhouse-backup/config.yml"))

	uploadSSHKeys(r)

	runMainIntegrationScenario(t, "SFTP")
}

func TestIntegrationCustom(t *testing.T) {
	r := require.New(t)

	for _, customType := range []string{"restic", "kopia", "rsync"} {
		if customType == "rsync" {
			uploadSSHKeys(r)
			installDebIfNotExists(r, "clickhouse", "openssh-client")
			installDebIfNotExists(r, "clickhouse", "rsync")
			installDebIfNotExists(r, "clickhouse", "jq")
		}
		if customType == "restic" {
			r.NoError(dockerExec("minio", "rm", "-rf", "/data/clickhouse/*"))
			installDebIfNotExists(r, "clickhouse", "curl")
			installDebIfNotExists(r, "clickhouse", "jq")
			installDebIfNotExists(r, "clickhouse", "bzip2")
			r.NoError(dockerExec("clickhouse", "bash", "-xec", "RELEASE_TAG=$(curl -H 'Accept: application/json' -sL https://github.com/restic/restic/releases/latest | jq -c -r -M '.tag_name'); RELEASE=$(echo ${RELEASE_TAG} | sed -e 's/v//'); curl -sfL \"https://github.com/restic/restic/releases/download/${RELEASE_TAG}/restic_${RELEASE}_linux_amd64.bz2\" | bzip2 -d > /bin/restic; chmod +x /bin/restic"))
		}
		if customType == "kopia" {
			r.NoError(dockerExec("minio", "bash", "-ce", "rm -rfv /data/clickhouse/*"))
			installDebIfNotExists(r, "clickhouse", "pgp")
			installDebIfNotExists(r, "clickhouse", "curl")
			r.NoError(dockerExec("clickhouse", "apt-get", "install", "-y", "ca-certificates"))
			r.NoError(dockerExec("clickhouse", "update-ca-certificates"))
			r.NoError(dockerExec("clickhouse", "bash", "-ce", "curl -sfL https://kopia.io/signing-key | gpg --dearmor -o /usr/share/keyrings/kopia-keyring.gpg"))
			r.NoError(dockerExec("clickhouse", "bash", "-ce", "echo 'deb [signed-by=/usr/share/keyrings/kopia-keyring.gpg] https://packages.kopia.io/apt/ stable main' > /etc/apt/sources.list.d/kopia.list"))
			installDebIfNotExists(r, "clickhouse", "kopia")
			installDebIfNotExists(r, "clickhouse", "jq")
		}
		r.NoError(dockerCP("config-custom-"+customType+".yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
		r.NoError(dockerExec("clickhouse", "mkdir", "-pv", "/custom/"+customType))
		r.NoError(dockerCP("./"+customType+"/", "clickhouse:/custom/"))
		runMainIntegrationScenario(t, "CUSTOM")
	}
}

func TestIntegrationEmbedded(t *testing.T) {
	t.Skipf("Test skipped, wait 23.01, RESTORE Ordinary table and RESTORE MATERIALIZED VIEW and {uuid} not works for %s version, look https://github.com/ClickHouse/ClickHouse/issues/43971 and https://github.com/ClickHouse/ClickHouse/issues/42709", os.Getenv("CLICKHOUSE_VERSION"))
	version := os.Getenv("CLICKHOUSE_VERSION")
	if version != "head" && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.7") < 0 {
		t.Skipf("Test skipped, BACKUP/RESTORE not available for %s version", version)
	}
	r := require.New(t)
	r.NoError(dockerCP("config-s3-embedded.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	runMainIntegrationScenario(t, "EMBEDDED")
}

func TestLongListRemote(t *testing.T) {
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second, 1*time.Second)
	defer ch.chbackend.Close()
	totalCacheCount := 20
	testBackupName := "test_list_remote"
	err := dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml")
	r.NoError(err)

	for i := 0; i < totalCacheCount; i++ {
		r.NoError(dockerExec("clickhouse", "bash", "-ce", fmt.Sprintf("ALLOW_EMPTY_BACKUPS=true clickhouse-backup create_remote %s_%d", testBackupName, i)))
	}

	r.NoError(dockerExec("clickhouse", "rm", "-rfv", "/tmp/.clickhouse-backup-metadata.cache.S3"))
	r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker-compose", "-f", os.Getenv("COMPOSE_FILE"), "restart", "minio"))
	time.Sleep(2 * time.Second)

	startFirst := time.Now()
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "list", "remote"))
	noCacheDuration := time.Since(startFirst)

	r.NoError(dockerExec("clickhouse", "chmod", "-Rv", "+r", "/tmp/.clickhouse-backup-metadata.cache.S3"))

	startCashed := time.Now()
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "list", "remote"))
	cashedDuration := time.Since(startCashed)

	r.Greater(noCacheDuration, cashedDuration)

	r.NoError(dockerExec("clickhouse", "rm", "-Rfv", "/tmp/.clickhouse-backup-metadata.cache.S3"))
	r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker-compose", "-f", os.Getenv("COMPOSE_FILE"), "restart", "minio"))
	time.Sleep(2 * time.Second)

	startCacheClear := time.Now()
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "list", "remote"))
	cacheClearDuration := time.Since(startCacheClear)

	r.Greater(cacheClearDuration, cashedDuration)
	log.Info().Msgf("noCacheDuration=%s cachedDuration=%s cacheClearDuration=%s", noCacheDuration.String(), cashedDuration.String(), cacheClearDuration.String())

	testListRemoteAllBackups := make([]string, totalCacheCount)
	for i := 0; i < totalCacheCount; i++ {
		testListRemoteAllBackups[i] = fmt.Sprintf("%s_%d", testBackupName, i)
	}
	fullCleanup(r, ch, testListRemoteAllBackups, []string{"remote", "local"}, []string{}, true)
}

func TestRestoreDatabaseMapping(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-database-mapping.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 1*time.Second)
	defer ch.chbackend.Close()
	checkRecordset := func(expectedRows int, expectedCount uint64, query string) {
		result := make([]struct {
			Count uint64 `ch:"count()"`
		}, 0)
		r.NoError(ch.chbackend.Select(&result, query))
		r.Equal(expectedRows, len(result), "expect %d row", expectedRows)
		r.Equal(expectedCount, result[0].Count, "expect count=%d", expectedCount)
	}

	testBackupName := "test_restore_database_mapping"
	databaseList := []string{"database1", "database2"}
	fullCleanup(r, ch, []string{testBackupName}, []string{"local"}, databaseList, false)

	ch.queryWithNoError(r, "CREATE DATABASE database1")
	ch.queryWithNoError(r, "CREATE TABLE database1.t1 (dt DateTime, v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/database1/t1','{replica}') PARTITION BY toYYYYMM(dt) ORDER BY dt")
	ch.queryWithNoError(r, "CREATE TABLE database1.d1 AS database1.t1 ENGINE=Distributed('{cluster}',database1, t1)")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.3") < 0 {
		ch.queryWithNoError(r, "CREATE TABLE database1.t2 AS database1.t1 ENGINE=ReplicatedMergeTree('/clickhouse/tables/database1/t2','{replica}') PARTITION BY toYYYYMM(dt) ORDER BY dt")
	} else {
		ch.queryWithNoError(r, "CREATE TABLE database1.t2 AS database1.t1 ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','{replica}') PARTITION BY toYYYYMM(dt) ORDER BY dt")
	}
	ch.queryWithNoError(r, "CREATE MATERIALIZED VIEW database1.mv1 TO database1.t2 AS SELECT * FROM database1.t1")
	ch.queryWithNoError(r, "CREATE VIEW database1.v1 AS SELECT * FROM database1.t1")
	ch.queryWithNoError(r, "INSERT INTO database1.t1 SELECT '2022-01-01 00:00:00', number FROM numbers(10)")

	log.Info().Msg("Create backup")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", testBackupName))

	log.Info().Msg("Restore schema")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--schema", "--rm", "--restore-database-mapping", "database1:database2", "--tables", "database1.*", testBackupName))

	log.Info().Msg("Check result database1")
	ch.queryWithNoError(r, "INSERT INTO database1.t1 SELECT '2023-01-01 00:00:00', number FROM numbers(10)")
	checkRecordset(1, 20, "SELECT count() FROM database1.t1")
	checkRecordset(1, 20, "SELECT count() FROM database1.d1")
	checkRecordset(1, 20, "SELECT count() FROM database1.mv1")
	checkRecordset(1, 20, "SELECT count() FROM database1.v1")

	log.Info().Msg("Drop database1")
	isAtomic, err := ch.chbackend.IsAtomic("database1")
	r.NoError(err)
	if isAtomic {
		ch.queryWithNoError(r, "DROP DATABASE database1 SYNC")
	} else {
		ch.queryWithNoError(r, "DROP DATABASE database1")
	}

	log.Info().Msg("Restore data")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--data", "--restore-database-mapping", "database1:database2", "--tables", "database1.*", testBackupName))

	log.Info().Msg("Check result database2")
	checkRecordset(1, 10, "SELECT count() FROM database2.t1")
	checkRecordset(1, 10, "SELECT count() FROM database2.d1")
	checkRecordset(1, 10, "SELECT count() FROM database2.mv1")
	checkRecordset(1, 10, "SELECT count() FROM database2.v1")

	log.Info().Msg("Check database1 not exists")
	checkRecordset(1, 0, "SELECT count() FROM system.databases WHERE name='database1'")

	fullCleanup(r, ch, []string{testBackupName}, []string{"local"}, databaseList, true)
}

func TestMySQLMaterialized(t *testing.T) {
	t.Skipf("Wait when fix DROP TABLE not supported by MaterializedMySQL, just attach will not help")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.12") == -1 {
		t.Skipf("MaterializedMySQL doens't support for clickhouse version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	r := require.New(t)
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	r.NoError(dockerExec("mysql", "mysql", "-u", "root", "--password=root", "-v", "-e", "CREATE DATABASE ch_mysql_repl"))
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 1*time.Second)
	defer ch.chbackend.Close()
	engine := "MaterializedMySQL"
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.9") == -1 {
		engine = "MaterializeMySQL"
	}
	ch.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE ch_mysql_repl ENGINE=%s('mysql:3306','ch_mysql_repl','root','root')", engine))
	r.NoError(dockerExec("mysql", "mysql", "-u", "root", "--password=root", "-v", "-e", "CREATE TABLE ch_mysql_repl.t1 (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, s VARCHAR(255)); INSERT INTO ch_mysql_repl.t1(s) VALUES('s1'),('s2'),('s3')"))
	time.Sleep(1 * time.Second)

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "test_mysql_materialized"))
	ch.queryWithNoError(r, "DROP DATABASE ch_mysql_repl")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "test_mysql_materialized"))

	result := 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT count() FROM ch_mysql_repl.t1"))
	r.Equal(3, result, "expect count=3")

	ch.queryWithNoError(r, "DROP DATABASE ch_mysql_repl")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_mysql_materialized"))
}

func TestPostgreSQLMaterialized(t *testing.T) {
	t.Skipf("Wait when fix https://github.com/ClickHouse/ClickHouse/issues/44250")

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.11") == -1 {
		t.Skipf("MaterializedPostgreSQL doens't support for clickhouse version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	r := require.New(t)
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	r.NoError(dockerExec("pgsql", "bash", "-ce", "echo 'CREATE DATABASE ch_pgsql_repl' | PGPASSWORD=root psql -v ON_ERROR_STOP=1 -U root"))
	r.NoError(dockerExec("pgsql", "bash", "-ce", "echo \"CREATE TABLE t1 (id BIGINT PRIMARY KEY, s VARCHAR(255)); INSERT INTO t1(id, s) VALUES(1,'s1'),(2,'s2'),(3,'s3')\" | PGPASSWORD=root psql -v ON_ERROR_STOP=1 -U root -d ch_pgsql_repl"))
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 1*time.Second)
	defer ch.chbackend.Close()

	ch.queryWithNoError(r,
		"CREATE DATABASE ch_pgsql_repl ENGINE=MaterializedPostgreSQL('pgsql:5432','ch_pgsql_repl','root','root') "+
			"SETTINGS materialized_postgresql_allow_automatic_update = 1, materialized_postgresql_schema = 'public'",
	)
	time.Sleep(1 * time.Second)

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "test_pgsql_materialized"))
	ch.queryWithNoError(r, "DROP DATABASE ch_pgsql_repl")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "test_pgsql_materialized"))

	result := 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT count() FROM ch_pgsql_repl.t1"))
	r.Equal(3, result, "expect count=3")

	ch.queryWithNoError(r, "DROP DATABASE ch_pgsql_repl")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_pgsql_materialized"))
}

func uploadSSHKeys(r *require.Assertions) {
	r.NoError(dockerCP("sftp/clickhouse-backup_rsa", "clickhouse:/id_rsa"))
	r.NoError(dockerExec("clickhouse", "cp", "-vf", "/id_rsa", "/tmp/id_rsa"))
	r.NoError(dockerExec("clickhouse", "chmod", "-v", "0600", "/tmp/id_rsa"))

	r.NoError(dockerCP("sftp/clickhouse-backup_rsa.pub", "sshd:/root/.ssh/authorized_keys"))
	r.NoError(dockerExec("sshd", "chown", "-v", "root:root", "/root/.ssh/authorized_keys"))
	r.NoError(dockerExec("sshd", "chmod", "-v", "0600", "/root/.ssh/authorized_keys"))
}

func runMainIntegrationScenario(t *testing.T, remoteStorageType string) {
	var out string
	var err error

	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 1*time.Minute)
	defer ch.chbackend.Close()

	// test for specified partitions backup
	testBackupSpecifiedPartitions(r, ch, remoteStorageType)

	// main test scenario
	testBackupName := fmt.Sprintf("test_backup_%d", rand.Int())
	incrementBackupName := fmt.Sprintf("increment_%d", rand.Int())
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameMySQL, dbNamePostgreSQL, Issue331Atomic, Issue331Ordinary}

	log.Info().Msg("Clean before start")
	fullCleanup(r, ch, []string{testBackupName, incrementBackupName}, []string{"remote", "local"}, databaseList, false)

	r.NoError(dockerExec("minio", "mc", "ls", "local/clickhouse/disk_s3"))
	generateTestData(ch, r)

	r.NoError(dockerExec("minio", "mc", "ls", "local/clickhouse/disk_s3"))
	log.Info().Msg("Create backup")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", testBackupName))

	generateIncrementTestData(ch, r)

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", incrementBackupName))

	log.Info().Msg("Upload")
	uploadCmd := fmt.Sprintf("%s_COMPRESSION_FORMAT=zstd clickhouse-backup upload --resume %s", remoteStorageType, testBackupName)
	checkResumeAlreadyProcessed(uploadCmd, testBackupName, "upload", r, remoteStorageType)

	//diffFrom := []string{"--diff-from", "--diff-from-remote"}[rand.Intn(2)]
	diffFrom := "--diff-from-remote"
	uploadCmd = fmt.Sprintf("clickhouse-backup upload %s %s %s --resume", incrementBackupName, diffFrom, testBackupName)
	checkResumeAlreadyProcessed(uploadCmd, incrementBackupName, "upload", r, remoteStorageType)

	backupDir := "/var/lib/clickhouse/backup"
	if remoteStorageType == "EMBEDDED" {
		backupDir = "/var/lib/clickhouse/disks/backups"
	}
	out, err = dockerExecOut("clickhouse", "ls", "-lha", backupDir)
	r.NoError(err)
	r.Equal(5, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect '2' backups exists in backup directory")
	log.Info().Msg("Delete backup")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", testBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", incrementBackupName))
	out, err = dockerExecOut("clickhouse", "ls", "-lha", backupDir)
	r.NoError(err)
	r.Equal(3, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect '0' backup exists in backup directory")

	dropDatabasesFromTestDataDataSet(r, ch, databaseList)

	log.Info().Msg("Download")
	downloadCmd := fmt.Sprintf("clickhouse-backup download --resume %s", testBackupName)
	checkResumeAlreadyProcessed(downloadCmd, testBackupName, "download", r, remoteStorageType)

	log.Info().Msg("Restore schema")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--schema", testBackupName))

	log.Info().Msg("Restore data")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--data", testBackupName))

	log.Info().Msg("Full restore with rm")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", testBackupName))

	log.Info().Msg("Check data")
	for i := range testData {
		if testData[i].CheckDatabaseOnly {
			r.NoError(ch.checkDatabaseEngine(t, testData[i]))
		} else {
			if isTableSkip(ch, testData[i], true) {
				continue
			}
			r.NoError(ch.checkData(t, testData[i], r))
		}
	}
	// test increment
	dropDatabasesFromTestDataDataSet(r, ch, databaseList)

	log.Info().Msg("Delete backup")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", testBackupName))

	log.Info().Msg("Download increment")
	downloadCmd = fmt.Sprintf("clickhouse-backup download --resume %s", incrementBackupName)
	checkResumeAlreadyProcessed(downloadCmd, incrementBackupName, "download", r, remoteStorageType)

	log.Info().Msg("Restore")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--schema", "--data", incrementBackupName))

	log.Info().Msg("Check increment data")
	for i := range testData {
		testDataItem := testData[i]
		if isTableSkip(ch, testDataItem, true) || testDataItem.IsDictionary {
			continue
		}
		for _, incrementDataItem := range incrementData {
			if testDataItem.Database == incrementDataItem.Database && testDataItem.Name == incrementDataItem.Name {
				testDataItem.Rows = append(testDataItem.Rows, incrementDataItem.Rows...)
			}
		}
		if testDataItem.CheckDatabaseOnly {
			r.NoError(ch.checkDatabaseEngine(t, testDataItem))
		} else {
			r.NoError(ch.checkData(t, testDataItem, r))
		}

	}

	// test end
	log.Info().Msg("Clean after finish")
	if remoteStorageType == "CUSTOM" {
		fullCleanup(r, ch, []string{}, []string{}, databaseList, true)
	} else {
		fullCleanup(r, ch, []string{testBackupName, incrementBackupName}, []string{"remote", "local"}, databaseList, true)
	}
}

func checkResumeAlreadyProcessed(backupCmd, testBackupName, resumeKind string, r *require.Assertions, remoteStorageType string) {
	// backupCmd = fmt.Sprintf("%s & PID=$!; sleep 0.7; kill -9 $PID; cat /var/lib/clickhouse/backup/%s/upload.state; sleep 0.3; %s", backupCmd, testBackupName, backupCmd)
	if remoteStorageType == "CUSTOM" || remoteStorageType == "EMBEDDED" {
		backupCmd = strings.Replace(backupCmd, "--resume", "", 1)
	} else {
		backupCmd = fmt.Sprintf("%s; cat /var/lib/clickhouse/backup/%s/%s.state; %s", backupCmd, testBackupName, resumeKind, backupCmd)
	}
	out, err := dockerExecOut("clickhouse", "bash", "-xce", backupCmd)
	log.Info().Msg(out)
	r.NoError(err)
	if strings.Contains(backupCmd, "--resume") {
		r.Contains(out, "already processed")
	}
}

func fullCleanup(r *require.Assertions, ch *TestClickHouse, backupNames, backupTypes, databaseList []string, checkDeleteErr bool) {
	for _, backupName := range backupNames {
		for _, backupType := range backupTypes {
			err := dockerExec("clickhouse", "clickhouse-backup", "delete", backupType, backupName)
			if checkDeleteErr {
				r.NoError(err)
			}
		}
	}
	otherBackupList, err := dockerExecOut("clickhouse", "ls", "-1", "/var/lib/clickhouse/backup")
	if err == nil {
		for _, backupName := range strings.Split(otherBackupList, "\n") {
			if backupName != "" {
				err := dockerExec("clickhouse", "clickhouse-backup", "delete", "local", backupName)
				if checkDeleteErr {
					r.NoError(err)
				}
			}
		}
	}

	dropDatabasesFromTestDataDataSet(r, ch, databaseList)
}

func generateTestData(ch *TestClickHouse, r *require.Assertions) {
	log.Info().Msg("Generate test data")
	generateTestDataWithDifferentStoragePolicy()
	for _, data := range testData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestSchema(data))
	}
	for _, data := range testData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestData(data))
	}
}

func generateTestDataWithDifferentStoragePolicy() {
	for databaseName, databaseEngine := range map[string]string{dbNameOrdinary: "Ordinary", dbNameAtomic: "Atomic"} {
		testDataEncrypted := TestDataStruct{
			Database: databaseName, DatabaseEngine: databaseEngine,
			Rows: func() []map[string]interface{} {
				var result []map[string]interface{}
				for i := 0; i < 100; i++ {
					result = append(result, map[string]interface{}{"id": uint64(i)})
				}
				return result
			}(),
			Fields:  []string{"id"},
			OrderBy: "id",
		}
		addTestDataIfNotExists := func() {
			found := false
			for _, data := range testData {
				if data.Name == testDataEncrypted.Name && data.Database == testDataEncrypted.Database {
					found = true
					break
				}
			}
			if !found {
				testData = append(testData, testDataEncrypted)
			}
		}
		//s3 disks support after 21.8
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
			testDataEncrypted.Name = "test_s3"
			testDataEncrypted.Schema = "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 's3_only'"
			addTestDataIfNotExists()
		}

		//encrypted disks support after 21.10
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.10") >= 0 {
			testDataEncrypted.Name = "test_hdd3_encrypted"
			testDataEncrypted.Schema = "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'hdd3_only_encrypted'"
			addTestDataIfNotExists()
		}
		//encrypted s3 disks support after 21.12
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
			testDataEncrypted.Name = "test_s3_encrypted"
			testDataEncrypted.Schema = "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 's3_only_encrypted'"
			addTestDataIfNotExists()
		}
	}
}

func generateIncrementTestData(ch *TestClickHouse, r *require.Assertions) {
	log.Info().Msg("Generate increment test data")
	for _, data := range incrementData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestData(data))
	}
}

func dropDatabasesFromTestDataDataSet(r *require.Assertions, ch *TestClickHouse, databaseList []string) {
	log.Info().Msg("Drop all databases")
	for _, db := range databaseList {
		r.NoError(ch.dropDatabase(db))
	}
}

func TestTablePatterns(t *testing.T) {
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 500*time.Millisecond, 5*time.Second)
	defer ch.chbackend.Close()

	testBackupName := "test_backup_patterns"
	databaseList := []string{dbNameOrdinary, dbNameAtomic}
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))

	for _, createPattern := range []bool{true, false} {
		for _, restorePattern := range []bool{true, false} {
			fullCleanup(r, ch, []string{testBackupName}, []string{"remote", "local"}, databaseList, false)
			generateTestData(ch, r)
			if createPattern {
				r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create_remote", "--tables", " "+dbNameOrdinary+".*", testBackupName))
			} else {
				r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create_remote", testBackupName))
			}

			r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", testBackupName))
			dropDatabasesFromTestDataDataSet(r, ch, databaseList)
			if restorePattern {
				r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore_remote", "--tables", " "+dbNameOrdinary+".*", testBackupName))
			} else {
				r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore_remote", testBackupName))
			}

			restored := uint64(0)
			r.NoError(ch.chbackend.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameOrdinary)))
			r.NotZero(restored)

			if createPattern || restorePattern {
				restored = 0
				r.NoError(ch.chbackend.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameAtomic)))
				// todo, old versions of clickhouse will return empty recordset
				r.Zero(restored)

				restored = 0
				r.NoError(ch.chbackend.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.databases WHERE name='%s'", dbNameAtomic)))
				// todo, old versions of clickhouse will return empty recordset
				r.Zero(restored)
			} else {
				restored = 0
				r.NoError(ch.chbackend.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameAtomic)))
				r.NotZero(restored)
			}

			fullCleanup(r, ch, []string{testBackupName}, []string{"remote", "local"}, databaseList, true)

		}
	}
}

func TestSkipNotExistsTable(t *testing.T) {
	t.Skip("TestSkipNotExistsTable is flaky now, need more precise algorithm for pause calculation")
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second, 1*time.Second)
	defer ch.chbackend.Close()

	log.Info().Msg("Check skip not exist errors")
	ifNotExistsCreateSQL := "CREATE TABLE IF NOT EXISTS default.if_not_exists (id UInt64) ENGINE=MergeTree() ORDER BY id"
	ifNotExistsInsertSQL := "INSERT INTO default.if_not_exists SELECT number FROM numbers(1000)"
	chVersion, err := ch.chbackend.GetVersion(context.Background())
	r.NoError(err)

	errorCaught := false
	pauseChannel := make(chan int64)
	resumeChannel := make(chan int64)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer func() {
			close(pauseChannel)
			wg.Done()
		}()
		pause := int64(0)
		pausePercent := int64(93)
		for i := 0; i < 100; i++ {
			testBackupName := fmt.Sprintf("not_exists_%d", i)
			err = ch.chbackend.Query(ifNotExistsCreateSQL)
			r.NoError(err)
			err = ch.chbackend.Query(ifNotExistsInsertSQL)
			r.NoError(err)
			pauseChannel <- pause
			startTime := time.Now()
			out, err := dockerExecOut("clickhouse", "bash", "-ce", "LOG_LEVEL=debug clickhouse-backup create --table default.if_not_exists "+testBackupName)
			pause = time.Since(startTime).Nanoseconds() * pausePercent / 100
			log.Info().Msg(out)
			if err != nil {
				if !strings.Contains(out, "no tables for backup") {
					assert.NoError(t, err)
				} else {
					pausePercent += 1
				}
			}

			if strings.Contains(out, "warn") && strings.Contains(out, "can't freeze") && strings.Contains(out, "code: 60") && err == nil {
				errorCaught = true
				<-resumeChannel
				r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", testBackupName))
				break
			}
			if err == nil {
				err = dockerExec("clickhouse", "clickhouse-backup", "delete", "local", testBackupName)
				assert.NoError(t, err)
			}
			<-resumeChannel
		}
	}()
	wg.Add(1)
	go func() {
		defer func() {
			close(resumeChannel)
			wg.Done()
		}()
		for pause := range pauseChannel {
			if pause > 0 {
				time.Sleep(time.Duration(pause) * time.Nanosecond)
				log.Info().Msgf("pause=%s", time.Duration(pause).String())
				err = ch.chbackend.DropTable(clickhouse.Table{Database: "default", Name: "if_not_exists"}, ifNotExistsCreateSQL, "", false, chVersion)
				r.NoError(err)
			}
			resumeChannel <- 1
		}
	}()
	wg.Wait()
	r.True(errorCaught)
}

func TestProjections(t *testing.T) {
	var err error
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") == -1 {
		t.Skipf("Test skipped, PROJECTION available only 21.8+, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}

	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second, 1*time.Second)
	defer ch.chbackend.Close()

	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	err = ch.chbackend.Query("CREATE TABLE default.table_with_projection(dt DateTime, v UInt64, PROJECTION x (SELECT toStartOfMonth(dt) m, sum(v) GROUP BY m)) ENGINE=MergeTree() ORDER BY dt")
	r.NoError(err)

	err = ch.chbackend.Query("INSERT INTO default.table_with_projection SELECT today() - INTERVAL number DAY, number FROM numbers(10)")
	r.NoError(err)

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "test_backup_projection"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", "test_backup_projection"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_backup_projection"))
	var counts uint64
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&counts, "SELECT count() FROM default.table_with_projection"))
	r.Equal(uint64(10), counts)
	err = ch.chbackend.Query("DROP TABLE default.table_with_projection NO DELAY")
	r.NoError(err)

}

func TestKeepBackupRemoteAndDiffFromRemote(t *testing.T) {
	if isTestShouldSkip("RUN_ADVANCED_TESTS") {
		t.Skip("Skipping Advanced integration tests...")
		return
	}
	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 2*time.Second)
	backupNames := make([]string, 5)
	for i := 0; i < 5; i++ {
		backupNames[i] = fmt.Sprintf("keep_remote_backup_%d", i)
	}
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameMySQL, dbNamePostgreSQL, Issue331Atomic, Issue331Ordinary}
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	fullCleanup(r, ch, backupNames, []string{"remote", "local"}, databaseList, false)
	generateTestData(ch, r)
	for i, backupName := range backupNames {
		generateIncrementTestData(ch, r)
		if i == 0 {
			r.NoError(dockerExec("clickhouse", "bash", "-ce", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 clickhouse-backup create_remote %s", backupName)))
		} else {
			r.NoError(dockerExec("clickhouse", "bash", "-ce", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 clickhouse-backup create_remote --diff-from-remote=%s %s", backupNames[i-1], backupName)))
		}
	}
	out, err := dockerExecOut("clickhouse", "bash", "-ce", "clickhouse-backup list local")
	r.NoError(err)
	// shall not delete any backup, cause all deleted backup have links as required in other backups
	for _, backupName := range backupNames {
		r.Contains(out, backupName)
		r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", backupName))
	}
	latestIncrementBackup := fmt.Sprintf("keep_remote_backup_%d", len(backupNames)-1)
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", latestIncrementBackup))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", latestIncrementBackup))
	var res uint64
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&res, fmt.Sprintf("SELECT count() FROM `%s`.`%s`", Issue331Atomic, Issue331Atomic)))
	r.Equal(uint64(200), res)
	fullCleanup(r, ch, backupNames, []string{"remote", "local"}, databaseList, true)
}

func TestS3NoDeletePermission(t *testing.T) {
	if isTestShouldSkip("RUN_ADVANCED_TESTS") {
		t.Skip("Skipping Advanced integration tests...")
		return
	}
	r := require.New(t)
	r.NoError(dockerExec("minio", "/bin/minio_nodelete.sh"))
	r.NoError(dockerCP("config-s3-nodelete.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))

	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 5*time.Second)
	defer ch.chbackend.Close()
	generateTestData(ch, r)
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create_remote", "no_delete_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "no_delete_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore_remote", "no_delete_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "no_delete_backup"))
	r.Error(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "no_delete_backup"))
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameMySQL, dbNamePostgreSQL, Issue331Atomic, Issue331Ordinary}
	dropDatabasesFromTestDataDataSet(r, ch, databaseList)
	r.NoError(dockerExec("minio", "bash", "-ce", "rm -rf /data/clickhouse/*"))
}

func TestSyncReplicaTimeout(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.11") == -1 {
		t.Skipf("Test skipped, SYNC REPLICA ignore receive_timeout for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Millisecond, 2*time.Second)
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))

	dropReplTables := func() {
		for _, table := range []string{"repl1", "repl2"} {
			query := "DROP TABLE IF EXISTS default." + table
			if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") == 1 {
				query += " NO DELAY"
			}
			ch.queryWithNoError(r, query)
		}
	}
	dropReplTables()
	ch.queryWithNoError(r, "CREATE TABLE default.repl1 (v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/default/repl','repl1') ORDER BY tuple()")
	ch.queryWithNoError(r, "CREATE TABLE default.repl2 (v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/default/repl','repl2') ORDER BY tuple()")

	ch.queryWithNoError(r, "INSERT INTO default.repl1 SELECT number FROM numbers(10)")

	ch.queryWithNoError(r, "SYSTEM STOP REPLICATED SENDS default.repl1")
	ch.queryWithNoError(r, "SYSTEM STOP FETCHES default.repl2")

	ch.queryWithNoError(r, "INSERT INTO default.repl1 SELECT number FROM numbers(100)")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "--tables=default.repl*", "test_not_synced_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "test_not_synced_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_not_synced_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "test_not_synced_backup"))

	ch.queryWithNoError(r, "SYSTEM START REPLICATED SENDS default.repl1")
	ch.queryWithNoError(r, "SYSTEM START FETCHES default.repl2")

	dropReplTables()
	ch.chbackend.Close()

}

func TestGetPartitionId(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") == -1 {
		t.Skipf("Test skipped, is_in_partition_key not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 1*time.Second)
	defer ch.chbackend.Close()

	type testData struct {
		CreateTableSQL string
		Database       string
		Table          string
		Partition      string
		ExpectedOutput string
	}
	testCases := []testData{
		{
			"CREATE TABLE default.test_part_id_1 UUID 'b45e751f-6c06-42a3-ab4a-f5bb9ac3716e' (dt Date, version DateTime, category String, name String) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}','{replica}',version) ORDER BY dt PARTITION BY (toYYYYMM(dt),category)",
			"default",
			"test_part_id_1",
			"('2023-01-01','category1')",
			"ad598a31d0ee285798bbe450f596c89c",
		},
		{
			"CREATE TABLE default.test_part_id_2 (dt Date, version DateTime, name String) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}','{replica}',version) ORDER BY dt PARTITION BY toYYYYMM(dt)",
			"default",
			"test_part_id_2",
			"'2023-01-01'",
			"202301",
		},
		{
			"CREATE TABLE default.test_part_id_3 ON CLUSTER '{cluster}' (i UInt32, name String) ENGINE = ReplicatedMergeTree() ORDER BY i PARTITION BY i",
			"default",
			"test_part_id_3",
			"202301",
			"202301",
		},
		{
			"CREATE TABLE default.test_part_id_4 (dt String, name String) ENGINE = MergeTree ORDER BY dt PARTITION BY dt",
			"default",
			"test_part_id_4",
			"'2023-01-01'",
			"c487903ebbb25a533634d6ec3485e3a9",
		},
		{
			"CREATE TABLE default.test_part_id_5 (dt String, name String) ENGINE = Memory",
			"default",
			"test_part_id_5",
			"'2023-01-01'",
			"",
		},
	}
	if isAtomic, _ := ch.chbackend.IsAtomic("default"); !isAtomic {
		testCases[0].CreateTableSQL = strings.Replace(testCases[0].CreateTableSQL, "UUID 'b45e751f-6c06-42a3-ab4a-f5bb9ac3716e'", "", 1)
	}
	for _, tc := range testCases {
		err, partitionId := partition.GetPartitionId(context.Background(), ch.chbackend, tc.Database, tc.Table, tc.CreateTableSQL, tc.Partition)
		assert.NoError(t, err)
		assert.Equal(t, tc.ExpectedOutput, partitionId)
	}
}

func TestRestoreMutationInProgress(t *testing.T) {
	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 0*time.Second, 5*time.Second)
	defer ch.chbackend.Close()
	version, err := ch.chbackend.GetVersion(context.Background())
	r.NoError(err)
	zkPath := "/clickhouse/tables/{shard}/default/test_restore_mutation_in_progress"
	onCluster := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		zkPath = "/clickhouse/tables/{shard}/{database}/{table}"
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.3") >= 0 {
		zkPath = "/clickhouse/tables/{shard}/{database}/{table}/{uuid}"
		onCluster = " ON CLUSTER '{cluster}'"
	}
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS default.test_restore_mutation_in_progress %s", onCluster)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") > 0 {
		dropSQL += " NO DELAY"
	}
	ch.queryWithNoError(r, dropSQL)

	createSQL := fmt.Sprintf("CREATE TABLE default.test_restore_mutation_in_progress %s (id UInt64, attr String) ENGINE=ReplicatedMergeTree('%s','{replica}') PARTITION BY id ORDER BY id", onCluster, zkPath)
	ch.queryWithNoError(r, createSQL)
	ch.queryWithNoError(r, "INSERT INTO default.test_restore_mutation_in_progress SELECT number, if(number>0,'a',toString(number)) FROM numbers(2)")

	mutationSQL := "ALTER TABLE default.test_restore_mutation_in_progress MODIFY COLUMN attr UInt64"
	err = ch.chbackend.QueryContext(context.Background(), mutationSQL)
	if err != nil {
		errStr := strings.ToLower(err.Error())
		r.True(strings.Contains(errStr, "code: 341") || strings.Contains(errStr, "code: 517") || strings.Contains(errStr, "timeout"), "UNKNOWN ERROR: %s", err.Error())
		t.Logf("%s RETURN EXPECTED ERROR=%#v", mutationSQL, err)
	}

	attrs := make([]struct {
		Attr uint64 `ch:"attr"`
	}, 0)
	err = ch.chbackend.Select(&attrs, "SELECT attr FROM default.test_restore_mutation_in_progress ORDER BY id")
	r.NotEqual(nil, err)
	errStr := strings.ToLower(err.Error())
	r.True(strings.Contains(errStr, "code: 53") || strings.Contains(errStr, "code: 6"))
	r.Zero(len(attrs))

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		mutationSQL = "ALTER TABLE default.test_restore_mutation_in_progress RENAME COLUMN attr TO attr_1"
		err = ch.chbackend.QueryContext(context.Background(), mutationSQL)
		r.NotEqual(nil, err)
		errStr = strings.ToLower(err.Error())
		r.True(strings.Contains(errStr, "code: 517") || strings.Contains(errStr, "timeout"))
		t.Logf("%s RETURN EXPECTED ERROR=%#v", mutationSQL, err)
	}
	r.NoError(dockerExec("clickhouse", "clickhouse", "client", "-q", "SELECT * FROM system.mutations WHERE is_done=0 FORMAT Vertical"))

	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	// backup with check consistency
	out, createErr := dockerExecOut("clickhouse", "clickhouse-backup", "create", "--tables=default.test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
	r.NotEqual(createErr, nil)
	r.Contains(out, "have inconsistent data types")
	t.Log(out)

	// backup without check consistency
	out, createErr = dockerExecOut("clickhouse", "clickhouse-backup", "create", "--skip-check-parts-columns", "--tables=default.test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
	t.Log(out)
	r.NoError(createErr)
	r.NotContains(out, "have inconsistent data types")

	r.NoError(ch.chbackend.DropTable(clickhouse.Table{Database: "default", Name: "test_restore_mutation_in_progress"}, "", "", false, version))
	var restoreErr error
	restoreErr = dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", "--tables=default.test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.8") < 0 {
		r.NotEqual(restoreErr, nil)
	} else {
		r.NoError(restoreErr)
	}

	attrs = make([]struct {
		Attr uint64 `ch:"attr"`
	}, 0)
	checkRestoredData := "attr"
	if restoreErr == nil {
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
			checkRestoredData = "attr_1 AS attr"
		}
	}
	selectSQL := fmt.Sprintf("SELECT %s FROM default.test_restore_mutation_in_progress ORDER BY id", checkRestoredData)
	selectErr := ch.chbackend.Select(&attrs, selectSQL)
	expectedSelectResults := make([]struct {
		Attr uint64 `ch:"attr"`
	}, 1)
	expectedSelectResults[0].Attr = 0

	expectedSelectError := "code: 517"

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") < 0 {
		expectedSelectResults = make([]struct {
			Attr uint64 `ch:"attr"`
		}, 2)
		expectedSelectError = ""
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.8") < 0 {
		expectedSelectError = ""
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.8") >= 0 {
		expectedSelectError = "code: 6"
		expectedSelectResults = make([]struct {
			Attr uint64 `ch:"attr"`
		}, 0)
	}
	r.Equal(expectedSelectResults, attrs)
	if expectedSelectError != "" {
		r.Error(selectErr)
		r.Contains(strings.ToLower(selectErr.Error()), expectedSelectError)
		t.Logf("%s RETURN EXPECTED ERROR=%#v", selectSQL, selectErr)
	} else {
		r.NoError(selectErr)
	}

	r.NoError(dockerExec("clickhouse", "clickhouse", "client", "-q", "SELECT * FROM system.mutations FORMAT Vertical"))

	r.NoError(ch.chbackend.DropTable(clickhouse.Table{Database: "default", Name: "test_restore_mutation_in_progress"}, "", "", false, version))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_restore_mutation_in_progress"))
}

const apiBackupNumber = 5

func TestServerAPI(t *testing.T) {
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second, 1*time.Second)
	defer func() {
		ch.chbackend.Close()
	}()
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	fieldTypes := []string{"UInt64", "String", "Int"}
	installDebIfNotExists(r, "clickhouse", "curl")
	maxTables := 10
	minFields := 10
	randFields := 10
	fillDatabaseForAPIServer(maxTables, minFields, randFields, ch, r, fieldTypes)

	log.Info().Msg("Run `clickhouse-backup server --watch` in background")
	r.NoError(dockerExec("-d", "clickhouse", "bash", "-ce", "clickhouse-backup server --watch &>>/tmp/clickhouse-backup-server.log"))
	time.Sleep(1 * time.Second)

	testAPIBackupCreate(r)

	testAPIBackupTables(r)

	log.Info().Msg("Check /backup/actions")
	ch.queryWithNoError(r, "SELECT count() FROM system.backup_actions")

	testAPIBackupUpload(r)

	testAPIBackupList(t, r)

	testAPIDeleteLocalDownloadRestore(r)

	testAPIMetrics(r, ch)

	testAPIWatchAndKill(r, ch)

	testAPIBackupActions(r, ch)

	testAPIRestart(r, ch)

	testAPIBackupDelete(r)

	r.NoError(dockerExec("clickhouse", "pkill", "-n", "-f", "clickhouse-backup"))
	r.NoError(ch.dropDatabase("long_schema"))
}

func testAPIRestart(r *require.Assertions, ch *TestClickHouse) {
	out, err := dockerExecOut("clickhouse", "bash", "-ce", "curl -sfL -XPOST 'http://localhost:7171/restart'")
	log.Debug().Msg(out)
	r.NoError(err)
	r.Contains(out, "acknowledged")

	//some actions need time for restart
	time.Sleep(6 * time.Second)

	var inProgressActions uint64
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&inProgressActions, "SELECT count() FROM system.backup_actions WHERE status!=?", status.CancelStatus))
	r.Equal(uint64(0), inProgressActions)
}

func testAPIBackupActions(r *require.Assertions, ch *TestClickHouse) {
	runClickHouseClientInsertSystemBackupActions := func(commands []string, needWait bool) {
		sql := "INSERT INTO system.backup_actions(command) " + "VALUES ('" + strings.Join(commands, "'),('") + "')"
		out, err := dockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("clickhouse client --echo -mn -q \"%s\"", sql))
		log.Debug().Msg(out)
		r.NoError(err)
		if needWait {
			for _, command := range commands {
				for {
					time.Sleep(500 * time.Millisecond)
					var commandStatus string
					r.NoError(ch.chbackend.SelectSingleRowNoCtx(&commandStatus, "SELECT status FROM system.backup_actions WHERE command=?", command))
					if commandStatus != status.InProgressStatus {
						break
					}
				}
			}
		}
	}
	runClickHouseClientInsertSystemBackupActions([]string{"create_remote actions_backup1"}, true)
	runClickHouseClientInsertSystemBackupActions([]string{"delete local actions_backup1", "restore_remote --rm actions_backup1"}, true)
	runClickHouseClientInsertSystemBackupActions([]string{"delete local actions_backup1", "delete remote actions_backup1"}, false)

	runClickHouseClientInsertSystemBackupActions([]string{"create actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions([]string{"upload actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions([]string{"delete local actions_backup2"}, false)
	runClickHouseClientInsertSystemBackupActions([]string{"download actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions([]string{"restore --rm actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions([]string{"delete local actions_backup2", "delete remote actions_backup2"}, false)

	inProgressActions := make([]struct {
		Command string `ch:"command"`
		Status  string `ch:"status"`
	}, 0)
	r.NoError(ch.chbackend.StructSelect(&inProgressActions, "SELECT command, status FROM system.backup_actions WHERE command LIKE '%actions%' AND status IN (?,?)", status.InProgressStatus, status.ErrorStatus))
	r.Equal(0, len(inProgressActions), "inProgressActions=%+v", inProgressActions)

	var actionsBackups uint64
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&actionsBackups, "SELECT count() FROM system.backup_list WHERE name LIKE 'backup_action%'"))
	r.Equal(uint64(0), actionsBackups)

	out, err := dockerExecOut("clickhouse", "curl", "http://localhost:7171/metrics")
	r.NoError(err)
	r.Contains(out, "clickhouse_backup_last_create_remote_status 1")
	r.Contains(out, "clickhouse_backup_last_create_status 1")
	r.Contains(out, "clickhouse_backup_last_upload_status 1")
	r.Contains(out, "clickhouse_backup_last_delete_status 1")
	r.Contains(out, "clickhouse_backup_last_download_status 1")
	r.Contains(out, "clickhouse_backup_last_restore_status 1")
}

func testAPIWatchAndKill(r *require.Assertions, ch *TestClickHouse) {
	log.Info().Msg("Check /backup/watch + /backup/kill")
	runKillCommand := func(command string) {
		out, err := dockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("curl -sfL 'http://localhost:7171/backup/kill?command=%s'", command))
		log.Debug().Msg(out)
		r.NoError(err)
	}
	checkWatchBackup := func(expectedCount uint64) {
		var watchBackups uint64
		r.NoError(ch.chbackend.SelectSingleRowNoCtx(&watchBackups, "SELECT count() FROM system.backup_list WHERE name LIKE 'shard%'"))
		r.Equal(expectedCount, watchBackups)
	}

	checkCanceledCommand := func(expectedCount int) {
		canceledCommands := make([]struct {
			Status  string `ch:"status"`
			Command string `ch:"command"`
		}, 0)
		r.NoError(ch.chbackend.StructSelect(&canceledCommands, "SELECT status, command FROM system.backup_actions WHERE command LIKE 'watch%'"))
		r.Equal(expectedCount, len(canceledCommands))
		for i := range canceledCommands {
			r.Equal("watch", canceledCommands[i].Command)
			r.Equal(status.CancelStatus, canceledCommands[i].Status)
		}
	}

	checkWatchBackup(1)
	runKillCommand("watch")
	checkCanceledCommand(1)

	out, err := dockerExecOut("clickhouse", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/watch'")
	log.Debug().Msg(out)
	r.NoError(err)
	time.Sleep(7 * time.Second)

	checkWatchBackup(2)
	runKillCommand("watch")
	checkCanceledCommand(2)
}

func testAPIBackupDelete(r *require.Assertions) {
	log.Info().Msg("Check /backup/delete/{where}/{name}")
	for i := 1; i <= apiBackupNumber; i++ {
		out, err := dockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/local/z_backup_%d'", i))
		log.Info().Msg(out)
		r.NoError(err)
		r.NotContains(out, "another operation is currently running")
		r.NotContains(out, "\"status\":\"error\"")
		out, err = dockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/remote/z_backup_%d'", i))
		log.Info().Msg(out)
		r.NoError(err)
		r.NotContains(out, "another operation is currently running")
		r.NotContains(out, "\"status\":\"error\"")
	}
	out, err := dockerExecOut("clickhouse", "curl", "http://localhost:7171/metrics")
	r.NoError(err)
	r.Contains(out, "clickhouse_backup_last_delete_status 1")
}

func testAPIMetrics(r *require.Assertions, ch *TestClickHouse) {
	log.Info().Msg("Check /metrics clickhouse_backup_last_backup_size_remote")
	var lastRemoteSize int64
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&lastRemoteSize, "SELECT size FROM system.backup_list WHERE name='z_backup_5' AND location='remote'"))

	var realTotalBytes uint64
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		r.NoError(ch.chbackend.SelectSingleRowNoCtx(&realTotalBytes, "SELECT sum(total_bytes) FROM system.tables WHERE database='long_schema'"))
	} else {
		r.NoError(ch.chbackend.SelectSingleRowNoCtx(&realTotalBytes, "SELECT sum(bytes_on_disk) FROM system.parts WHERE database='long_schema'"))
	}
	r.Greater(realTotalBytes, uint64(0))
	r.Greater(uint64(lastRemoteSize), realTotalBytes)

	out, err := dockerExecOut("clickhouse", "curl", "-sL", "http://localhost:7171/metrics")
	log.Debug().Msg(out)
	r.NoError(err)
	r.Contains(out, fmt.Sprintf("clickhouse_backup_last_backup_size_remote %d", lastRemoteSize))

	log.Info().Msg("Check /metrics clickhouse_backup_number_backups_*")
	r.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_local %d", apiBackupNumber))
	// +1 watch backup
	r.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_remote %d", apiBackupNumber+1))
	r.Contains(out, "clickhouse_backup_number_backups_local_expected 0")
	r.Contains(out, "clickhouse_backup_number_backups_remote_expected 0")
}

func testAPIDeleteLocalDownloadRestore(r *require.Assertions) {
	log.Info().Msg("Check /backup/delete/local/{name} + /backup/download/{name} + /backup/restore/{name}?rm=1")
	out, err := dockerExecOut(
		"clickhouse",
		"bash", "-xe", "-c",
		fmt.Sprintf("for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/delete/local/z_backup_$i\"; curl -sfL -XPOST \"http://localhost:7171/backup/download/z_backup_$i\"; sleep 2; curl -sfL -XPOST \"http://localhost:7171/backup/restore/z_backup_$i?rm=1\"; sleep 8; done", apiBackupNumber),
	)
	log.Debug().Msg(out)
	r.NoError(err)
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")

	out, err = dockerExecOut("clickhouse", "curl", "http://localhost:7171/metrics")
	r.NoError(err)
	r.Contains(out, "clickhouse_backup_last_delete_status 1")
	r.Contains(out, "clickhouse_backup_last_download_status 1")
	r.Contains(out, "clickhouse_backup_last_restore_status 1")
}

func testAPIBackupList(t *testing.T, r *require.Assertions) {
	log.Info().Msg("Check /backup/list")
	out, err := dockerExecOut("clickhouse", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list'")
	log.Debug().Msg(out)
	r.NoError(err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"regular\"}", i)), out))
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar, regular\"}", i)), out))
	}

	log.Info().Msg("Check /backup/list/local")
	out, err = dockerExecOut("clickhouse", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list/local'")
	log.Debug().Msg(out)
	r.NoError(err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"regular\"}", i)), out))
		r.True(assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar, regular\"}", i)), out))
	}

	log.Info().Msg("Check /backup/list/remote")
	out, err = dockerExecOut("clickhouse", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list/remote'")
	log.Debug().Msg(out)
	r.NoError(err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"regular\"}", i)), out))
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar, regular\"}", i)), out))
	}
}

func testAPIBackupUpload(r *require.Assertions) {
	log.Info().Msg("Check /backup/upload")
	out, err := dockerExecOut(
		"clickhouse",
		"bash", "-xe", "-c",
		fmt.Sprintf("for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/upload/z_backup_$i\"; sleep 2; done", apiBackupNumber),
	)
	log.Debug().Msg(out)
	r.NoError(err)
	r.NotContains(out, "\"status\":\"error\"")
	r.NotContains(out, "another operation is currently running")
	out, err = dockerExecOut("clickhouse", "curl", "http://localhost:7171/metrics")
	r.NoError(err)
	r.Contains(out, "clickhouse_backup_last_upload_status 1")
}

func testAPIBackupTables(r *require.Assertions) {
	log.Info().Msg("Check /backup/tables")
	out, err := dockerExecOut(
		"clickhouse",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables\"",
	)
	log.Debug().Msg(out)
	r.NoError(err)
	r.Contains(out, "long_schema")
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	r.NotContains(out, "system")
	r.NotContains(out, "INFORMATION_SCHEMA")
	r.NotContains(out, "information_schema")

	log.Info().Msg("Check /backup/tables/all")
	out, err = dockerExecOut(
		"clickhouse",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables/all\"",
	)
	log.Debug().Msg(out)
	r.NoError(err)
	r.Contains(out, "long_schema")
	r.Contains(out, "system")
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.3") >= 0 {
		r.Contains(out, "INFORMATION_SCHEMA")
		r.Contains(out, "information_schema")
	}
}

func testAPIBackupCreate(r *require.Assertions) {
	log.Info().Msg("Check /backup/create")
	out, err := dockerExecOut(
		"clickhouse",
		"bash", "-xe", "-c",
		fmt.Sprintf("sleep 3; for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/create?table=long_schema.*&name=z_backup_$i\"; sleep 1.5; done", apiBackupNumber),
	)
	log.Debug().Msg(out)
	r.NoError(err)
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	out, err = dockerExecOut("clickhouse", "curl", "http://localhost:7171/metrics")
	r.NoError(err)
	r.Contains(out, "clickhouse_backup_last_create_status 1")

}

func fillDatabaseForAPIServer(maxTables int, minFields int, randFields int, ch *TestClickHouse, r *require.Assertions, fieldTypes []string) {
	log.Info().Msgf("Create %d `long_schema`.`t%%d` tables with with %d..%d fields...", maxTables, minFields, minFields+randFields)
	ch.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS long_schema")
	for i := 0; i < maxTables; i++ {
		sql := fmt.Sprintf("CREATE TABLE long_schema.t%d (id UInt64", i)
		fieldsCount := minFields + rand.Intn(randFields)
		for j := 0; j < fieldsCount; j++ {
			fieldType := fieldTypes[rand.Intn(len(fieldTypes))]
			sql += fmt.Sprintf(", f%d %s", j, fieldType)
		}
		sql += ") ENGINE=MergeTree() ORDER BY id"
		ch.queryWithNoError(r, sql)
		sql = fmt.Sprintf("INSERT INTO long_schema.t%d(id) SELECT number FROM numbers(100)", i)
		ch.queryWithNoError(r, sql)
	}
	log.Info().Msg("...DONE")
}

type TestClickHouse struct {
	chbackend *clickhouse.ClickHouse
}

func (ch *TestClickHouse) connectWithWait(r *require.Assertions, sleepBefore, timeOut time.Duration) {
	time.Sleep(sleepBefore)
	for i := 1; i < 11; i++ {
		err := ch.connect(timeOut.String())
		if i == 10 {
			r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker", "logs", "clickhouse"))
			out, dockerErr := dockerExecOut("clickhouse", "clickhouse-client", "--echo", "-q", "'SELECT version()'")
			r.NoError(dockerErr)
			log.Warn().Msg(out)
			r.NoError(err)
		}
		if err != nil {
			log.Warn().Msgf("clickhouse not ready %v, wait %d seconds", err, i*2)
			r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker", "ps", "-a"))
			if out, dockerErr := dockerExecOut("clickhouse", "clickhouse-client", "--echo", "-q", "SELECT version()"); dockerErr == nil {
				log.Warn().Msg(out)
			} else {
				log.Info().Msg(out)
			}
			time.Sleep(time.Second * time.Duration(i*2))
		} else {
			if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") == 1 {
				var count uint64
				err = ch.chbackend.SelectSingleRowNoCtx(&count, "SELECT count() FROM mysql('mysql:3306','mysql','user','root','root')")
				if err == nil {
					break
				} else {
					log.Warn().Msgf("mysql not ready %v, wait %d seconds", err, i)
					time.Sleep(time.Second * time.Duration(i))
				}
			} else {
				break
			}
		}
	}
}

func (ch *TestClickHouse) connect(timeOut string) error {
	ch.chbackend = &clickhouse.ClickHouse{
		Config: &config.ClickHouseConfig{
			Host:    "127.0.0.1",
			Port:    9000,
			Timeout: timeOut,
		},
	}
	var err error
	for i := 0; i < 3; i++ {
		err = ch.chbackend.Connect()
		if err == nil {
			return nil
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}
	return err
}

func (ch *TestClickHouse) createTestSchema(data TestDataStruct) error {
	if !data.IsFunction {
		// 20.8 doesn't respect DROP TABLE ... NO DELAY, so Atomic works but --rm is not applicable
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") > 0 {
			if err := ch.chbackend.CreateDatabaseWithEngine(data.Database, data.DatabaseEngine, "cluster"); err != nil {
				return err
			}
		} else {
			if err := ch.chbackend.CreateDatabase(data.Database, "cluster"); err != nil {
				return err
			}
		}
	}
	if data.CheckDatabaseOnly {
		return nil
	}
	createSQL := "CREATE "
	if data.IsFunction {
		createSQL += " FUNCTION "
	} else if data.IsMaterializedView {
		createSQL += " MATERIALIZED VIEW "
	} else if data.IsView {
		createSQL += " VIEW "
	} else if data.IsDictionary {
		createSQL += " DICTIONARY "
	} else {
		createSQL += " TABLE "
	}

	if data.IsFunction {
		createSQL += fmt.Sprintf(" IF NOT EXISTS `%s` ", data.Name)
	} else {
		createSQL += fmt.Sprintf(" IF NOT EXISTS `%s`.`%s` ", data.Database, data.Name)
	}

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.0") == 1 && !data.IsFunction {
		createSQL += " ON CLUSTER 'cluster' "
	}
	createSQL += data.Schema
	// old 1.x clickhouse versions doesn't contains {table} and {database} macros
	if strings.Contains(createSQL, "{table}") || strings.Contains(createSQL, "{database}") {
		var isMacrosExists uint64
		if err := ch.chbackend.SelectSingleRowNoCtx(&isMacrosExists, "SELECT count() FROM system.functions WHERE name='getMacro'"); err != nil {
			return err
		}
		if isMacrosExists == 0 {
			createSQL = strings.Replace(createSQL, "{table}", data.Name, -1)
			createSQL = strings.Replace(createSQL, "{database}", data.Database, -1)
		}
	}
	// old clickhouse version doesn't know about `{uuid}` macros
	if strings.Contains(createSQL, "{uuid}") && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") <= 0 {
		createSQL = strings.Replace(createSQL, "{uuid}", uuid.New().String(), -1)
	}
	// functions supported only after 21.12
	if data.IsFunction && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") == -1 {
		return nil
	}
	err := ch.chbackend.CreateTable(
		clickhouse.Table{
			Database: data.Database,
			Name:     data.Name,
		},
		createSQL,
		false, false, "", 0,
	)
	return err
}

func (ch *TestClickHouse) createTestData(data TestDataStruct) error {
	if data.SkipInsert || data.CheckDatabaseOnly {
		return nil
	}
	insertSQL := fmt.Sprintf("INSERT INTO `%s`.`%s`", data.Database, data.Name)

	batch, err := ch.chbackend.GetConn().PrepareBatch(context.Background(), insertSQL)

	if err != nil {
		return err
	}

	for _, row := range data.Rows {
		insertData := make([]interface{}, len(data.Fields))
		for idx, field := range data.Fields {
			insertData[idx] = row[field]
		}
		if err = batch.Append(insertData...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (ch *TestClickHouse) dropDatabase(database string) (err error) {
	var isAtomic bool
	dropDatabaseSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", database)
	if isAtomic, err = ch.chbackend.IsAtomic(database); isAtomic {
		dropDatabaseSQL += " SYNC"
	} else if err != nil {
		return err
	}
	return ch.chbackend.Query(dropDatabaseSQL)
}

func (ch *TestClickHouse) checkData(t *testing.T, data TestDataStruct, r *require.Assertions) error {
	assert.NotNil(t, data.Rows)
	log.Info().Msgf("Check '%d' rows in '%s.%s'\n", len(data.Rows), data.Database, data.Name)
	selectSQL := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY `%s`", data.Database, data.Name, data.OrderBy)

	if data.IsFunction && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") == -1 {
		return nil
	}
	if data.IsFunction {
		selectSQL = fmt.Sprintf("SELECT %s(number, number+1) AS test_result FROM numbers(3)", data.Name)
	}
	log.Debug().Msg(selectSQL)
	rows, err := ch.chbackend.GetConn().Query(context.TODO(), selectSQL)
	if err != nil {
		return err
	}

	columnTypes := rows.ColumnTypes()
	rowsNumber := 0

	for rows.Next() {

		vars := make([]interface{}, len(columnTypes))
		for i := range columnTypes {
			vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
		}

		if err = rows.Scan(vars...); err != nil {
			panic(err)
		}

		for idx, v := range vars {
			switch v := v.(type) {
			case *string:
				vars[idx] = *v
			case *time.Time:
				vars[idx] = *v
			case *uint64:
				vars[idx] = *v
			case *float64:
				vars[idx] = *v
			case *time.Location:
				vars[idx] = *v
			}
		}

		expectedVars := make([]interface{}, 0)

		for _, v := range data.Rows[rowsNumber] {
			expectedVars = append(expectedVars, v)
		}
		r.ElementsMatch(vars, expectedVars)
		rowsNumber += 1
	}

	r.Equal(len(data.Rows), rowsNumber)

	return nil
}

func (ch *TestClickHouse) checkDatabaseEngine(t *testing.T, data TestDataStruct) error {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") <= 0 {
		return nil
	}
	selectSQL := fmt.Sprintf("SELECT engine FROM system.databases WHERE name='%s'", data.Database)
	var engine string
	if err := ch.chbackend.SelectSingleRowNoCtx(&engine, selectSQL); err != nil {
		return err
	}
	assert.True(
		t, strings.HasPrefix(data.DatabaseEngine, engine),
		fmt.Sprintf("expect '%s' have prefix '%s'", data.DatabaseEngine, engine),
	)
	return nil
}

func (ch *TestClickHouse) queryWithNoError(r *require.Assertions, query string, args ...interface{}) {
	err := ch.chbackend.Query(query, args...)
	r.NoError(err)
}

func dockerExec(container string, cmd ...string) error {
	out, err := dockerExecOut(container, cmd...)
	log.Info().Msg(out)
	return err
}

func dockerExecOut(container string, cmd ...string) (string, error) {
	dcmd := []string{"exec", container}
	dcmd = append(dcmd, cmd...)
	return utils.ExecCmdOut(context.Background(), 180*time.Second, "docker", dcmd...)
}

func dockerCP(src, dst string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	dcmd := []string{"cp", src, dst}
	log.Info().Msgf("docker %s", strings.Join(dcmd, " "))
	out, err := exec.CommandContext(ctx, "docker", dcmd...).CombinedOutput()
	log.Info().Msg(string(out))
	cancel()
	return err
}

func toDate(s string) time.Time {
	result, _ := time.Parse("2006-01-02", s)
	return result
}

func toTS(s string) time.Time {
	result, _ := time.Parse("2006-01-02 15:04:05", s)
	return result
}

func isTableSkip(ch *TestClickHouse, data TestDataStruct, dataExists bool) bool {
	if strings.Contains(data.DatabaseEngine, "PostgreSQL") && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") <= 0 {
		return true
	}
	if data.IsDictionary && os.Getenv("COMPOSE_FILE") != "docker-compose.yml" && dataExists {
		var dictEngines []struct {
			Engine string `ch:"engine"`
		}
		dictSQL := fmt.Sprintf(
			"SELECT engine FROM system.tables WHERE name='%s' AND database='%s'",
			data.Name, data.Database,
		)
		_ = ch.chbackend.Select(&dictEngines, dictSQL)
		return len(dictEngines) == 0
	}
	return os.Getenv("COMPOSE_FILE") == "docker-compose.yml" && (data.Name == "jbod" || data.IsDictionary)
}

func compareVersion(v1, v2 string) int {
	if v2 == "head" && v1 == "head" {
		return 0
	}
	if v1 == "head" {
		return 1
	}
	if v2 == "head" {
		return -1
	}
	v1 = "v" + v1
	v2 = "v" + v2
	if strings.Count(v1, ".") > 2 {
		v1 = strings.Join(strings.Split(v1, ".")[0:2], ".")
	}
	return semver.Compare(v1, v2)
}

func isTestShouldSkip(envName string) bool {
	isSkip, _ := map[string]bool{"": true, "0": true, "false": true, "False": true, "1": false, "True": false, "true": false}[os.Getenv(envName)]
	return isSkip
}

func installDebIfNotExists(r *require.Assertions, container, pkg string) {
	r.NoError(dockerExec(
		container,
		"bash", "-c",
		fmt.Sprintf(
			"if [[ '0' == $(dpkg -l %s | grep -c -E \"^ii\\s+%s\" ) ]]; then apt-get -y update; apt-get install -y %s; fi",
			pkg, pkg, pkg,
		),
	))
}

func testBackupSpecifiedPartitions(r *require.Assertions, ch *TestClickHouse, remoteStorageType string) {
	log.Info().Msg("testBackupSpecifiedPartitions started")
	var err error
	var out string
	var result, expectedCount uint64

	partitionBackupName := fmt.Sprintf("partition_backup_%d", rand.Int())
	fullBackupName := fmt.Sprintf("full_backup_%d", rand.Int())
	// Create and fill tables
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.t1")
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.t2")
	ch.queryWithNoError(r, "CREATE TABLE default.t1 (dt Date, v UInt64) ENGINE=MergeTree() PARTITION BY toYYYYMMDD(dt) ORDER BY dt")
	ch.queryWithNoError(r, "CREATE TABLE default.t2 (dt String, v UInt64) ENGINE=MergeTree() PARTITION BY dt ORDER BY dt")
	for _, dt := range []string{"2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04"} {
		ch.queryWithNoError(r, fmt.Sprintf("INSERT INTO default.t1 SELECT '%s', number FROM numbers(10)", dt))
		ch.queryWithNoError(r, fmt.Sprintf("INSERT INTO default.t2 SELECT '%s', number FROM numbers(10)", dt))
	}

	// check create_remote full > download + partitions > delete local > download > restore --partitions > restore
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create_remote", "--tables=default.t*", fullBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", fullBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", "--partitions=('2022-01-02'),('2022-01-03')", fullBackupName))
	out, err = dockerExecOut("clickhouse", "bash", "-c", "ls -la /var/lib/clickhouse/backup/"+fullBackupName+"/shadow/default/t?/default/ | wc -l")
	r.NoError(err)
	expectedLines := "13"
	// custom storage doesn't support --partitions for upload / download now
	if remoteStorageType == "CUSTOM" || compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") == -1 {
		expectedLines = "17"
	}
	r.Equal(expectedLines, strings.Trim(out, "\r\n\t "))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", fullBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", fullBackupName))
	out, err = dockerExecOut("clickhouse", "bash", "-c", "ls -la /var/lib/clickhouse/backup/"+fullBackupName+"/shadow/default/t?/default/ | wc -l")
	r.NoError(err)
	r.Equal("17", strings.Trim(out, "\r\n\t "))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--partitions=('2022-01-02'),('2022-01-03')", fullBackupName))
	result = 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT sum(c) FROM (SELECT count() AS c FROM default.t1 UNION ALL SELECT count() AS c FROM default.t2)"))
	expectedCount = 40
	// old 1.x clickhouse versions doesn't have is_in_partition_key
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") == -1 {
		expectedCount = 80
	}
	r.Equal(expectedCount, result, fmt.Sprintf("expect count=%d", expectedCount))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", fullBackupName))
	result = 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT sum(c) FROM (SELECT count() AS c FROM default.t1 UNION ALL SELECT count() AS c FROM default.t2)"))
	r.Equal(uint64(80), result, "expect count=80")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", fullBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", fullBackupName))

	// check create + partitions
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "--tables=default.t1", "--partitions=20220102,20220103", partitionBackupName))
	out, err = dockerExecOut("clickhouse", "bash", "-c", "ls -la /var/lib/clickhouse/backup/"+partitionBackupName+"/shadow/default/t1/default/ | wc -l")
	r.NoError(err)
	r.Equal("5", strings.Trim(out, "\r\n\t "))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", partitionBackupName))

	// check create > upload + partitions
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "--tables=default.t1", partitionBackupName))
	out, err = dockerExecOut("clickhouse", "bash", "-c", "ls -la /var/lib/clickhouse/backup/"+partitionBackupName+"/shadow/default/t1/default/ | wc -l")
	r.NoError(err)
	r.Equal("7", strings.Trim(out, "\r\n\t "))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "--tables=default.t1", "--partitions=20220102,20220103", partitionBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", partitionBackupName))

	// restore partial uploaded
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore_remote", partitionBackupName))

	// Check partial restored t1
	result = 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT count() FROM default.t1"))

	expectedCount = 20
	// custom doesn't support --partitions in upload and download
	if remoteStorageType == "CUSTOM" {
		expectedCount = 40
	}
	r.Equal(expectedCount, result, fmt.Sprintf("expect count=%d", expectedCount))

	// Check only selected partitions restored
	result = 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT count() FROM default.t1 WHERE dt NOT IN ('2022-01-02','2022-01-03')"))
	expectedCount = 0
	// custom doesn't support --partitions in upload and download
	if remoteStorageType == "CUSTOM" {
		expectedCount = 20
	}
	r.Equal(expectedCount, result, "expect count=0")

	// DELETE backup.
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", partitionBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", partitionBackupName))

	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.t1")
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.t2")

	log.Info().Msg("testBackupSpecifiedPartitions finish")
}
