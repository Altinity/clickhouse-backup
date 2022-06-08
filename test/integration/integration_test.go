//go:build integration

package main

import (
	"context"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/logcli"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/apex/log"
	"golang.org/x/mod/semver"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dbNameAtomic = "_test.ДБ_atomic_"
const dbNameOrdinary = "_test.ДБ_ordinary_"
const dbNameMySQL = "mysql_db"
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
		Schema: "(Date Date, TimeStamp DateTime, Log String) ENGINE = MergeTree(Date, (TimeStamp, Log), 8192)",
		Rows: []map[string]interface{}{
			{"Date": toDate("2018-10-23"), "TimeStamp": toTS("2018-10-23 07:37:14"), "Log": "One"},
			{"Date": toDate("2018-10-23"), "TimeStamp": toTS("2018-10-23 07:37:15"), "Log": "Two"},
			{"Date": toDate("2018-10-24"), "TimeStamp": toTS("2018-10-24 07:37:16"), "Log": "Three"},
			{"Date": toDate("2018-10-24"), "TimeStamp": toTS("2018-10-24 07:37:17"), "Log": "Four"},
			{"Date": toDate("2019-10-25"), "TimeStamp": toTS("2019-01-25 07:37:18"), "Log": "Five"},
			{"Date": toDate("2019-10-25"), "TimeStamp": toTS("2019-01-25 07:37:19"), "Log": "Six"},
		},
		Fields:  []string{"Date", "TimeStamp", "Log"},
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
		Name:   Issue331Atomic, // need cover fix https://github.com/AlexAkulov/clickhouse-backup/issues/331
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
		Name:   Issue331Ordinary, // need cover fix https://github.com/AlexAkulov/clickhouse-backup/issues/331
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
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() ORDER BY (order_time, order_id)",
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
		Schema:         "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','replica1') ORDER BY id",
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
		Schema:             fmt.Sprintf("(id UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','replica1') ORDER BY id AS SELECT max(id) AS id FROM `%s`.`mv_src_table`", dbNameAtomic),
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
			Issue331Atomic, Issue331Atomic, Issue331Atomic, Issue331Atomic), // same table and name need cover fix https://github.com/AlexAkulov/clickhouse-backup/issues/331
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
		Schema: "(Date Date, TimeStamp DateTime, Log String) ENGINE = MergeTree(Date, (TimeStamp, Log), 8192)",
		Rows: []map[string]interface{}{
			{"Date": toDate("2019-10-26"), "TimeStamp": toTS("2019-01-26 07:37:19"), "Log": "Seven"},
		},
		Fields:  []string{"Date", "TimeStamp", "Log"},
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
		Name:   Issue331Atomic, // need cover fix https://github.com/AlexAkulov/clickhouse-backup/issues/331
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
		Name:   Issue331Ordinary, // need cover fix https://github.com/AlexAkulov/clickhouse-backup/issues/331
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
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{"order_id": "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameAtomic, DatabaseEngine: "Atomic",
		Name:   "jbod",
		Schema: "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'jbod'",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 100; i < 200; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i)})
			}
			return result
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
}

func init() {
	log.SetHandler(logcli.New(os.Stdout))
	logLevel := "info"
	if os.Getenv("LOG_LEVEL") != "" {
		logLevel = os.Getenv("LOG_LEVEL")
	}
	log.SetLevelFromString(logLevel)
}

func TestDoRestoreRBAC(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.4") == -1 {
		t.Skipf("Test skipped, RBAC not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	ch := &TestClickHouse{}
	r := require.New(t)

	ch.connectWithWait(r, 1*time.Second)

	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_rbac")
	ch.queryWithNoError(r, "CREATE TABLE default.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")

	ch.queryWithNoError(r, "DROP SETTINGS PROFILE  IF EXISTS test_rbac")
	ch.queryWithNoError(r, "DROP QUOTA IF EXISTS test_rbac")
	ch.queryWithNoError(r, "DROP ROW POLICY IF EXISTS test_rbac ON default.test_rbac")
	ch.queryWithNoError(r, "DROP ROLE IF EXISTS test_rbac")
	ch.queryWithNoError(r, "DROP USER IF EXISTS test_rbac")

	log.Info("create RBAC related objects")
	ch.queryWithNoError(r, "CREATE SETTINGS PROFILE test_rbac SETTINGS max_execution_time=60")
	ch.queryWithNoError(r, "CREATE ROLE test_rbac SETTINGS PROFILE 'test_rbac'")
	ch.queryWithNoError(r, "CREATE USER test_rbac IDENTIFIED BY 'test_rbac' DEFAULT ROLE test_rbac")
	ch.queryWithNoError(r, "CREATE QUOTA test_rbac KEYED BY user_name FOR INTERVAL 1 hour NO LIMITS TO test_rbac")
	ch.queryWithNoError(r, "CREATE ROW POLICY test_rbac ON default.test_rbac USING 1=1 AS RESTRICTIVE TO test_rbac")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "--backup-rbac", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	log.Info("drop all RBAC related objects after backup")
	ch.queryWithNoError(r, "DROP SETTINGS PROFILE test_rbac")
	ch.queryWithNoError(r, "DROP QUOTA test_rbac")
	ch.queryWithNoError(r, "DROP ROW POLICY test_rbac ON default.test_rbac")
	ch.queryWithNoError(r, "DROP ROLE test_rbac")
	ch.queryWithNoError(r, "DROP USER test_rbac")

	ch.chbackend.Close()
	r.NoError(execCmd("docker-compose", "-f", os.Getenv("COMPOSE_FILE"), "restart", "clickhouse"))
	ch.connectWithWait(r, 2*time.Second)

	log.Info("download+restore RBAC")
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", "--rbac", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	// we can't restart clickhouse inside container, we need restart container
	ch.chbackend.Close()
	_, err := execCmdOut("docker", "restart", "clickhouse")
	r.NoError(err)
	ch.connectWithWait(r, 2*time.Second)

	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	rbacTypes := map[string]string{
		"PROFILES": "test_rbac",
		"QUOTAS":   "test_rbac",
		"POLICIES": "test_rbac ON default.test_rbac",
		"ROLES":    "test_rbac",
		"USERS":    "test_rbac",
	}
	for rbacType, expectedValue := range rbacTypes {
		var rbacRows []string
		_ = ch.chbackend.Select(&rbacRows, fmt.Sprintf("SHOW %s", rbacType))
		found := false
		for _, row := range rbacRows {
			if row == expectedValue {
				found = true
				break
			}
		}
		if !found {
			r.NoError(dockerExec("clickhouse", "cat", "/var/log/clickhouse-server/clickhouse-server.log"))
		}
		r.Contains(rbacRows, expectedValue, "Invalid result for SHOW %s", rbacType)
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
	ch.connectWithWait(r, 0*time.Millisecond)
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_rbac")
	ch.queryWithNoError(r, "CREATE TABLE default.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")

	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	r.NoError(dockerExec("clickhouse", "bash", "-c", "echo '<yandex><profiles><default><empty_result_for_aggregation_by_empty_set>1</empty_result_for_aggregation_by_empty_set></default></profiles></yandex>' > /etc/clickhouse-server/users.d/test_config.xml"))

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "--configs", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_configs_backup"))

	ch.chbackend.Close()
	ch.connectWithWait(r, 2*time.Second)

	var settings []string
	r.NoError(ch.chbackend.Select(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
	r.Equal([]string{"1"}, settings, "expect empty_result_for_aggregation_by_empty_set=1")

	r.NoError(dockerExec("clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", "test_configs_backup"))

	ch.chbackend.Close()
	ch.connectWithWait(r, 2*time.Second)

	settings = []string{}
	r.NoError(ch.chbackend.Select(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
	r.Equal([]string{"0"}, settings, "expect empty_result_for_aggregation_by_empty_set=0")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", "--configs", "test_configs_backup"))
	_, err := ch.chbackend.Query("SYSTEM RELOAD CONFIG")
	r.NoError(err)
	ch.chbackend.Close()
	ch.connectWithWait(r, 2*time.Second)

	settings = []string{}
	r.NoError(ch.chbackend.Select(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
	r.Equal([]string{"1"}, settings, "expect empty_result_for_aggregation_by_empty_set=1")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml"))

	ch.chbackend.Close()
}

func TestTablePatterns(t *testing.T) {
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 500*time.Millisecond)
	defer ch.chbackend.Close()

	testBackupName := "test_backup_patterns"
	fullCleanup(r, ch, []string{testBackupName}, false)
	generateTestData(ch, r)
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create_remote", "--tables", " "+dbNameOrdinary+".*", testBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", testBackupName))
	dropDatabasesFromTestDataDataSet(r, ch)
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore_remote", "--tables", " "+dbNameOrdinary+".*", testBackupName))

	var restoredTables []uint64
	r.NoError(ch.chbackend.Select(&restoredTables, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameOrdinary)))
	r.NotZero(len(restoredTables))
	r.NotZero(restoredTables[0])

	restoredTables = make([]uint64, 0)
	r.NoError(ch.chbackend.Select(&restoredTables, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameAtomic)))
	// old versions of clickhouse will return empty recordset
	if len(restoredTables) > 0 {
		r.Zero(restoredTables[0])
	}

	fullCleanup(r, ch, []string{testBackupName}, true)

}

func TestLongListRemote(t *testing.T) {
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second)
	defer ch.chbackend.Close()
	totalCacheCount := 20
	testBackupName := "test_list_remote"
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))

	for i := 0; i < totalCacheCount; i++ {
		r.NoError(dockerExec("clickhouse", "bash", "-c", fmt.Sprintf("ALLOW_EMPTY_BACKUPS=true clickhouse-backup create_remote %s_%d", testBackupName, i)))
	}

	r.NoError(dockerExec("clickhouse", "rm", "-rfv", "/tmp/.clickhouse-backup-metadata.cache.S3"))
	startFirst := time.Now()
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "list", "remote"))
	firstDuration := time.Since(startFirst)

	r.NoError(dockerExec("clickhouse", "chmod", "-Rv", "+r", "/tmp/.clickhouse-backup-metadata.cache.S3"))

	startCashed := time.Now()
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "list", "remote"))
	cashedDuration := time.Since(startCashed)

	r.Greater(firstDuration, cashedDuration)

	r.NoError(dockerExec("clickhouse", "rm", "-Rfv", "/tmp/.clickhouse-backup-metadata.cache.S3"))
	startCacheClear := time.Now()
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "list", "remote"))
	cacheClearDuration := time.Since(startCacheClear)

	r.Greater(cacheClearDuration, cashedDuration)
	log.Infof("firstDuration=%s cachedDuration=%s cacheClearDuration=%s", firstDuration.String(), cashedDuration.String(), cacheClearDuration.String())

	testListRemoteAllBackups := make([]string, totalCacheCount)
	for i := 0; i < totalCacheCount; i++ {
		testListRemoteAllBackups[i] = fmt.Sprintf("%s_%d", testBackupName, i)
	}
	fullCleanup(r, ch, testListRemoteAllBackups, true)

}

func TestSkipNotExistsTable(t *testing.T) {
	t.Skip("TestSkipNotExistsTable is flaky now, need more precise algorithm for pause calculation")
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second)
	defer ch.chbackend.Close()

	log.Info("Check skip not exist errors")
	ifNotExistsCreateSQL := "CREATE TABLE IF NOT EXISTS default.if_not_exists (id UInt64) ENGINE=MergeTree() ORDER BY id"
	ifNotExistsInsertSQL := "INSERT INTO default.if_not_exists SELECT number FROM numbers(1000)"
	chVersion, err := ch.chbackend.GetVersion()
	r.NoError(err)

	errorCatched := false
	pauseChannel := make(chan int64)
	resumeChannel := make(chan int64)
	rand.Seed(time.Now().UnixNano())

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
			_, err = ch.chbackend.Query(ifNotExistsCreateSQL)
			r.NoError(err)
			_, err = ch.chbackend.Query(ifNotExistsInsertSQL)
			r.NoError(err)
			pauseChannel <- pause
			startTime := time.Now()
			out, err := dockerExecOut("clickhouse", "bash", "-c", "LOG_LEVEL=debug clickhouse-backup create --table default.if_not_exists "+testBackupName)
			pause = time.Since(startTime).Nanoseconds() * pausePercent / 100
			log.Info(out)
			if err != nil {
				if !strings.Contains(out, "no tables for backup") {
					assert.NoError(t, err)
				} else {
					pausePercent += 1
				}
			}

			if strings.Contains(out, "warn") && strings.Contains(out, "can't freeze") && strings.Contains(out, "code: 60") && err == nil {
				errorCatched = true
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
				log.Infof("pause=%s", time.Duration(pause).String())
				err = ch.chbackend.DropTable(clickhouse.Table{Database: "default", Name: "if_not_exists"}, ifNotExistsCreateSQL, "", chVersion)
				r.NoError(err)
			}
			resumeChannel <- 1
		}
	}()
	wg.Wait()
	r.True(errorCatched)
}

func TestProjections(t *testing.T) {
	var err error
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") == -1 {
		t.Skipf("Test skipped, PROJECTION available only 21.8+, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}

	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second)
	defer ch.chbackend.Close()

	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	_, err = ch.chbackend.Query("CREATE TABLE default.table_with_projection(dt DateTime, v UInt64, PROJECTION x (SELECT toStartOfMonth(dt) m, sum(v) GROUP BY m)) ENGINE=MergeTree() ORDER BY dt")
	r.NoError(err)

	_, err = ch.chbackend.Query("INSERT INTO default.table_with_projection SELECT today() - INTERVAL number DAY, number FROM numbers(10)")
	r.NoError(err)

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "test_backup_projection"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", "test_backup_projection"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_backup_projection"))
	counts := make([]int, 0)
	r.NoError(ch.chbackend.Select(&counts, "SELECT count() FROM default.table_with_projection"))
	r.Equal(1, len(counts))
	r.Equal(10, counts[0])
	_, err = ch.chbackend.Query("DROP TABLE default.table_with_projection NO DELAY")
	r.NoError(err)

}

func TestKeepBackupRemoteAndDiffFromRemote(t *testing.T) {
	if isTestShouldSkip("RUN_ADVANCED_TESTS") {
		t.Skip("Skipping Advanced integration tests...")
		return
	}
	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond)
	backupNames := make([]string, 5)
	for i := 0; i < 5; i++ {
		backupNames[i] = fmt.Sprintf("keep_remote_backup_%d", i)
	}

	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	fullCleanup(r, ch, backupNames, false)
	generateTestData(ch, r)
	for i, backupName := range backupNames {
		generateIncrementTestData(ch, r)
		if i == 0 {
			r.NoError(dockerExec("clickhouse", "bash", "-c", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 clickhouse-backup create_remote %s", backupName)))
		} else {
			r.NoError(dockerExec("clickhouse", "bash", "-c", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 clickhouse-backup create_remote --diff-from-remote=%s %s", backupNames[i-1], backupName)))
		}
	}
	out, err := dockerExecOut("clickhouse", "bash", "-c", "clickhouse-backup list local")
	r.NoError(err)
	// shall not delete any backup, cause all deleted backup have links as required in other backups
	for _, backupName := range backupNames {
		r.Contains(out, backupName)
		r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", backupName))
	}
	latestIncrementBackup := fmt.Sprintf("keep_remote_backup_%d", len(backupNames)-1)
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", latestIncrementBackup))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", latestIncrementBackup))
	res := make([]int, 0)
	r.NoError(ch.chbackend.Select(&res, fmt.Sprintf("SELECT count() FROM `%s`.`%s`", Issue331Atomic, Issue331Atomic)))
	r.Greater(len(res), 0)
	r.Equal(200, res[0])
	fullCleanup(r, ch, backupNames, true)
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
	ch.connectWithWait(r, 500*time.Millisecond)
	defer ch.chbackend.Close()
	generateTestData(ch, r)
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create_remote", "no_delete_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "no_delete_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore_remote", "no_delete_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "no_delete_backup"))
	r.Error(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "no_delete_backup"))
	dropDatabasesFromTestDataDataSet(r, ch)
}

func TestSyncReplicaTimeout(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.11") == -1 {
		t.Skipf("Test skipped, SYNC REPLICA ignore receive_timeout for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Millisecond)
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

func TestServerAPI(t *testing.T) {
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second)
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	ch.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS long_schema")
	defer func() {
		ch.chbackend.Close()
	}()
	fieldTypes := []string{"UInt64", "String", "Int"}
	installDebIfNotExists(r, "clickhouse", "curl")
	maxTables := 10
	minFields := 10
	randFields := 10
	log.Infof("Create %d `long_schema`.`t%%d` tables with with %d..%d fields...", maxTables, minFields, minFields+randFields)
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
	log.Info("...DONE")

	log.Info("Run `clickhouse-backup server` in background")
	r.NoError(dockerExec("-d", "clickhouse", "bash", "-c", "clickhouse-backup server &>>/tmp/clickhouse-backup-server.log"))
	time.Sleep(1 * time.Second)

	log.Info("Check /backup/create")
	out, err := dockerExecOut(
		"clickhouse",
		"bash", "-xe", "-c", "sleep 3; for i in {1..5}; do date; curl -sL -XPOST \"http://localhost:7171/backup/create?table=long_schema.*&name=z_backup_$i\"; sleep 1.5; done",
	)
	log.Debug(out)
	r.NoError(err)
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")

	log.Info("Check /backup/tables")
	out, err = dockerExecOut(
		"clickhouse",
		"bash", "-xe", "-c", "curl -sL \"http://localhost:7171/backup/tables\"",
	)
	log.Debug(out)
	r.NoError(err)
	r.NotContains(out, "system")

	log.Info("Check /backup/tables/all")
	out, err = dockerExecOut(
		"clickhouse",
		"bash", "-xe", "-c", "curl -sL \"http://localhost:7171/backup/tables/all\"",
	)
	log.Debug(out)
	r.NoError(err)
	r.Contains(out, "system")

	log.Info("Check /backup/actions")
	ch.queryWithNoError(r, "SELECT count() FROM system.backup_actions")

	log.Info("Check /backup/upload")
	out, err = dockerExecOut(
		"clickhouse",
		"bash", "-xe", "-c", "for i in {1..5}; do date; curl -sL -XPOST \"http://localhost:7171/backup/upload/z_backup_$i\"; sleep 2; done",
	)
	log.Debug(out)
	r.NoError(err)
	r.NotContains(out, "\"status\":\"error\"")
	r.NotContains(out, "another operation is currently running")

	log.Info("Check /backup/list")
	out, err = dockerExecOut("clickhouse", "bash", "-c", "curl -sL 'http://localhost:7171/backup/list'")
	log.Debug(out)
	r.NoError(err)
	totalBackupNumber := 5
	for i := 1; i <= totalBackupNumber; i++ {
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"\"}", i)), out))
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar\"}", i)), out))
	}

	log.Info("Check /backup/list/local")
	out, err = dockerExecOut("clickhouse", "bash", "-c", "curl -sL 'http://localhost:7171/backup/list/local'")
	log.Debug(out)
	r.NoError(err)
	for i := 1; i <= totalBackupNumber; i++ {
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"\"}", i)), out))
		r.True(assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar\"}", i)), out))
	}

	log.Info("Check /backup/list/remote")
	out, err = dockerExecOut("clickhouse", "bash", "-c", "curl -sL 'http://localhost:7171/backup/list/remote'")
	log.Debug(out)
	r.NoError(err)
	for i := 1; i <= totalBackupNumber; i++ {
		r.True(assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"\"}", i)), out))
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar\"}", i)), out))
	}

	log.Info("Check /backup/download/{name} + /backup/restore/{name}?rm=1")
	out, err = dockerExecOut(
		"clickhouse",
		"bash", "-xe", "-c", "for i in {1..5}; do date; curl -sL -XPOST \"http://localhost:7171/backup/delete/local/z_backup_$i\"; curl -sL -XPOST \"http://localhost:7171/backup/download/z_backup_$i\"; sleep 2; curl -sL -XPOST \"http://localhost:7171/backup/restore/z_backup_$i?rm=1\"; sleep 4; done",
	)
	log.Debug(out)
	r.NoError(err)
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")

	log.Info("Check /metrics clickhouse_backup_last_backup_size_remote")
	lastRemoteSize := make([]int64, 0)
	r.NoError(ch.chbackend.Select(&lastRemoteSize, "SELECT size FROM system.backup_list WHERE name='z_backup_5' AND location='remote'"))

	realTotalBytes := make([]uint64, 0)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		r.NoError(ch.chbackend.Select(&realTotalBytes, "SELECT sum(total_bytes) FROM system.tables WHERE database='long_schema'"))
	} else {
		r.NoError(ch.chbackend.Select(&realTotalBytes, "SELECT sum(bytes_on_disk) FROM system.parts WHERE database='long_schema'"))
	}

	r.Greater(lastRemoteSize[0], int64(realTotalBytes[0]))
	out, err = dockerExecOut("clickhouse", "curl", "-sL", "http://localhost:7171/metrics")
	log.Debug(out)
	r.NoError(err)
	r.Contains(out, fmt.Sprintf("clickhouse_backup_last_backup_size_remote %d", lastRemoteSize[0]))

	log.Info("Check /metrics clickhouse_backup_number_backups_*")
	r.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_local %d", totalBackupNumber))
	r.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_remote %d", totalBackupNumber))
	r.Contains(out, "clickhouse_backup_number_backups_local_expected 0")
	r.Contains(out, "clickhouse_backup_number_backups_remote_expected 0")

	log.Info("Check /backup/delete/{where}/{name}")
	for i := 1; i <= 5; i++ {
		out, err = dockerExecOut("clickhouse", "bash", "-c", fmt.Sprintf("curl -sL -XPOST 'http://localhost:7171/backup/delete/local/z_backup_%d'", i))
		log.Infof(out)
		r.NoError(err)
		r.NotContains(out, "another operation is currently running")
		r.NotContains(out, "\"status\":\"error\"")
		out, err = dockerExecOut("clickhouse", "bash", "-c", fmt.Sprintf("curl -sL -XPOST 'http://localhost:7171/backup/delete/remote/z_backup_%d'", i))
		log.Infof(out)
		r.NoError(err)
		r.NotContains(out, "another operation is currently running")
		r.NotContains(out, "\"status\":\"error\"")
	}

	r.NoError(dockerExec("clickhouse", "pkill", "-n", "-f", "clickhouse-backup"))
	r.NoError(ch.dropDatabase("long_schema"))
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

func TestIntegrationSFTPAuthKey(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-sftp-auth-key.yaml", "clickhouse:/etc/clickhouse-backup/config.yml"))

	r.NoError(dockerCP("sftp/clickhouse-backup_rsa", "clickhouse:/id_rsa"))
	r.NoError(dockerExec("clickhouse", "cp", "-vf", "/id_rsa", "/tmp/id_rsa"))
	r.NoError(dockerExec("clickhouse", "chmod", "-v", "0600", "/tmp/id_rsa"))

	r.NoError(dockerCP("sftp/clickhouse-backup_rsa.pub", "sshd:/root/.ssh/authorized_keys"))
	r.NoError(dockerExec("sshd", "chown", "-v", "root:root", "/root/.ssh/authorized_keys"))
	r.NoError(dockerExec("sshd", "chmod", "-v", "0600", "/root/.ssh/authorized_keys"))

	runMainIntegrationScenario(t, "SFTP")
}

func TestIntegrationFTP(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-ftp.yaml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	runMainIntegrationScenario(t, "FTP")
}

func runMainIntegrationScenario(t *testing.T, remoteStorageType string) {
	var out string
	var err error

	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond)
	defer ch.chbackend.Close()

	rand.Seed(time.Now().UnixNano())

	// test for specified partitions backup
	testBackupSpecifiedPartitions(r, ch)

	// main test scenario
	testBackupName := fmt.Sprintf("test_backup_%d", rand.Int())
	incrementBackupName := fmt.Sprintf("increment_%d", rand.Int())

	log.Info("Clean before start")
	fullCleanup(r, ch, []string{testBackupName, incrementBackupName}, false)

	r.NoError(dockerExec("minio", "mc", "ls", "local/clickhouse/disk_s3"))
	generateTestData(ch, r)

	r.NoError(dockerExec("minio", "mc", "ls", "local/clickhouse/disk_s3"))
	log.Info("Create backup")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", testBackupName))

	generateIncrementTestData(ch, r)

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", incrementBackupName))

	log.Info("Upload")
	r.NoError(dockerExec("clickhouse", "bash", "-c", fmt.Sprintf("%s_COMPRESSION_FORMAT=zstd clickhouse-backup upload %s", remoteStorageType, testBackupName)))

	//diffFrom := []string{"--diff-from", "--diff-from-remote"}[rand.Intn(2)]
	diffFrom := "--diff-from-remote"
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", incrementBackupName, diffFrom, testBackupName))

	out, err = dockerExecOut("clickhouse", "ls", "-lha", "/var/lib/clickhouse/backup")
	r.NoError(err)
	r.Equal(5, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect two backups exists in backup directory")
	log.Info("Delete backup")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", testBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", incrementBackupName))
	out, err = dockerExecOut("clickhouse", "ls", "-lha", "/var/lib/clickhouse/backup")
	r.NoError(err)
	r.Equal(3, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect no backup exists in backup directory")

	dropDatabasesFromTestDataDataSet(r, ch)
	r.NoError(dockerExec("minio", "mc", "ls", "local/clickhouse/disk_s3"))

	log.Info("Download")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", testBackupName))

	log.Info("Restore schema")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--schema", testBackupName))

	log.Info("Restore data")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--data", testBackupName))

	log.Info("Full restore with rm")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", testBackupName))

	log.Info("Check data")
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
	dropDatabasesFromTestDataDataSet(r, ch)

	log.Info("Delete backup")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", testBackupName))

	log.Info("Download increment")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", incrementBackupName))

	log.Info("Restore")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--schema", "--data", incrementBackupName))

	log.Info("Check increment data")
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
	log.Info("Clean after finish")
	fullCleanup(r, ch, []string{testBackupName, incrementBackupName}, true)
}

func fullCleanup(r *require.Assertions, ch *TestClickHouse, backupNames []string, checkDeleteErr bool) {
	for _, backupName := range backupNames {
		for _, backupType := range []string{"remote", "local"} {
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

	dropDatabasesFromTestDataDataSet(r, ch)
}

func generateTestData(ch *TestClickHouse, r *require.Assertions) {
	log.Info("Generate test data")
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
	log.Info("Generate increment test data")
	for _, data := range incrementData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestData(data))
	}
}

func dropDatabasesFromTestDataDataSet(r *require.Assertions, ch *TestClickHouse) {
	log.Info("Drop all databases")
	for _, db := range []string{dbNameOrdinary, dbNameAtomic, dbNameMySQL, Issue331Atomic, Issue331Ordinary} {
		r.NoError(ch.dropDatabase(db))
	}
}

type TestClickHouse struct {
	chbackend *clickhouse.ClickHouse
}

func (ch *TestClickHouse) connectWithWait(r *require.Assertions, sleepBefore time.Duration) {
	time.Sleep(sleepBefore)
	for i := 1; i < 11; i++ {
		err := ch.connect()
		if i == 10 {
			r.NoError(execCmd("docker", "logs", "clickhouse"))
			r.NoError(err)
		}
		if err != nil {
			log.Warnf("clickhouse not ready %v, wait %d seconds", err, i*2)
			r.NoError(execCmd("docker", "ps", "-a"))
			time.Sleep(time.Second * time.Duration(i*2))
		} else {
			if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") == 1 {
				var rows []string
				err = ch.chbackend.Select(&rows, "SELECT count() FROM mysql('mysql:3306','mysql','user','root','root')")
				if err == nil {
					break
				} else {
					log.Warnf("mysql not ready %v, wait %d seconds", err, i)
					time.Sleep(time.Second * time.Duration(i))
				}
			} else {
				break
			}
		}
	}
}

func (ch *TestClickHouse) connect() error {
	ch.chbackend = &clickhouse.ClickHouse{
		Config: &config.ClickHouseConfig{
			Host:    "127.0.0.1",
			Port:    9000,
			Timeout: "5m",
		},
	}
	return ch.chbackend.Connect()
}

func (ch *TestClickHouse) createTestSchema(data TestDataStruct) error {
	if !data.IsFunction {
		// 20.8 doesn't respect DROP TABLE .. NO DELAY, so Atomic works but --rm is not applicable
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") == 1 {
			if err := ch.chbackend.CreateDatabaseWithEngine(data.Database, data.DatabaseEngine); err != nil {
				return err
			}
		} else {
			if err := ch.chbackend.CreateDatabase(data.Database); err != nil {
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
		var isMacrosExists []int
		if err := ch.chbackend.Select(&isMacrosExists, "SELECT count() FROM system.functions WHERE name='getMacro'"); err != nil {
			return err
		}
		if len(isMacrosExists) == 0 || isMacrosExists[0] == 0 {
			createSQL = strings.Replace(createSQL, "{table}", data.Name, -1)
			createSQL = strings.Replace(createSQL, "{database}", data.Database, -1)
		}
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
		false, "", 0,
	)
	return err
}

func (ch *TestClickHouse) createTestData(data TestDataStruct) error {
	if data.SkipInsert || data.CheckDatabaseOnly {
		return nil
	}
	tx, err := ch.chbackend.GetConn().Beginx()
	if err != nil {
		return fmt.Errorf("can't begin transaction: %v", err)
	}
	insertSQL := fmt.Sprintf("INSERT INTO `%s`.`%s` (`%s`) VALUES (:%s)",
		data.Database,
		data.Name,
		strings.Join(data.Fields, "`,`"),
		strings.Join(data.Fields, ",:"),
	)
	log.Debug(insertSQL)
	statement, err := tx.PrepareNamed(insertSQL)
	if err != nil {
		return fmt.Errorf("can't prepare %s: %v", insertSQL, err)
	}
	defer func() {
		err = statement.Close()
		if err != nil {
			log.Warnf("can't close SQL statement")
		}
	}()

	for _, row := range data.Rows {
		log.Infof("%#v", row)
		if _, err := statement.Exec(row); err != nil {
			return fmt.Errorf("can't add insert to transaction: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("can't commit transaction: %v", err)
	}
	return nil
}

func (ch *TestClickHouse) dropDatabase(database string) (err error) {
	var isAtomic bool
	dropDatabaseSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", database)
	if isAtomic, err = ch.chbackend.IsAtomic(database); isAtomic {
		dropDatabaseSQL += " SYNC"
	} else if err != nil {
		return err
	}
	_, err = ch.chbackend.Query(dropDatabaseSQL)
	return err
}

func (ch *TestClickHouse) checkData(t *testing.T, data TestDataStruct, r *require.Assertions) error {
	assert.NotNil(t, data.Rows)
	log.Infof("Check '%d' rows in '%s.%s'\n", len(data.Rows), data.Database, data.Name)
	selectSQL := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY `%s`", data.Database, data.Name, data.OrderBy)

	if data.IsFunction && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") == -1 {
		return nil
	}
	if data.IsFunction {
		selectSQL = fmt.Sprintf("SELECT %s(number, number+1) AS test_result FROM numbers(3)", data.Name)
	}
	log.Debug(selectSQL)
	rows, err := ch.chbackend.GetConn().Queryx(selectSQL)
	if err != nil {
		return err
	}
	var result []map[string]interface{}
	for rows.Next() {
		row := map[string]interface{}{}
		if err = rows.MapScan(row); err != nil {
			return err
		}
		result = append(result, row)
	}
	r.Equal(len(data.Rows), len(result))
	for i := range data.Rows {
		//goland:noinspection GoNilness
		r.EqualValues(data.Rows[i], result[i])
	}
	return nil
}

func (ch *TestClickHouse) checkDatabaseEngine(t *testing.T, data TestDataStruct) error {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") <= 0 {
		return nil
	}
	selectSQL := fmt.Sprintf("SELECT engine FROM system.databases WHERE name='%s'", data.Database)
	log.Debug(selectSQL)
	rows, err := ch.chbackend.GetConn().Queryx(selectSQL)
	for rows.Next() {
		row := map[string]interface{}{}
		if err = rows.MapScan(row); err != nil {
			return err
		}
		assert.True(
			t, strings.HasPrefix(data.DatabaseEngine, row["engine"].(string)),
			fmt.Sprintf("expect '%s' have prefix '%s'", data.DatabaseEngine, row["engine"].(string)),
		)
	}
	return nil
}

func (ch *TestClickHouse) queryWithNoError(r *require.Assertions, query string, args ...interface{}) {
	_, err := ch.chbackend.Query(query, args...)
	r.NoError(err)
}

func dockerExec(container string, cmd ...string) error {
	out, err := dockerExecOut(container, cmd...)
	log.Info(out)
	return err
}

func dockerExecOut(container string, cmd ...string) (string, error) {
	dcmd := []string{"exec", container}
	dcmd = append(dcmd, cmd...)
	return execCmdOut("docker", dcmd...)
}

func execCmd(cmd string, args ...string) error {
	out, err := execCmdOut(cmd, args...)
	log.Info(out)
	return err
}

func execCmdOut(cmd string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	log.Infof("%s %s", cmd, strings.Join(args, " "))
	out, err := exec.CommandContext(ctx, cmd, args...).CombinedOutput()
	cancel()
	return string(out), err
}

func dockerCP(src, dst string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	dcmd := []string{"cp", src, dst}
	log.Infof("docker %s", strings.Join(dcmd, " "))
	out, err := exec.CommandContext(ctx, "docker", dcmd...).CombinedOutput()
	log.Info(string(out))
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
	if data.IsDictionary && os.Getenv("COMPOSE_FILE") != "docker-compose.yml" && dataExists {
		var dictEngines []string
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

func testBackupSpecifiedPartitions(r *require.Assertions, ch *TestClickHouse) {
	log.Info("testBackupSpecifiedPartitions started")

	partitionBackupName := fmt.Sprintf("partition_backup_%d", rand.Int())
	// Create table
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.t1")
	ch.queryWithNoError(r, "CREATE TABLE default.t1 (dt DateTime, v UInt64) ENGINE=MergeTree() PARTITION BY toYYYYMMDD(dt) ORDER BY dt")
	ch.queryWithNoError(r, "INSERT INTO default.t1 SELECT '2022-01-01 00:00:00', number FROM numbers(10)")
	ch.queryWithNoError(r, "INSERT INTO default.t1 SELECT '2022-01-02 00:00:00', number FROM numbers(10)")
	ch.queryWithNoError(r, "INSERT INTO default.t1 SELECT '2022-01-03 00:00:00', number FROM numbers(10)")
	// Backup

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create_remote", "--tables=default.t1", "--partitions=20220101,20220102", partitionBackupName))

	// TRUNCATE TABLE
	ch.queryWithNoError(r, "TRUNCATE table default.t1")
	// DELETE local backup before restore
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", partitionBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore_remote", partitionBackupName))

	log.Debug("testBackupSpecifiedPartitions begin check \n")
	// Check
	var result []int

	r.NoError(ch.chbackend.Select(&result, "SELECT count() FROM default.t1 WHERE dt IN ('2022-01-01 00:00:00','2022-01-02 00:00:00')"))

	// Must have one value
	log.Debugf("testBackupSpecifiedPartitions result : '%v'", result)
	log.Debugf("testBackupSpecifiedPartitions result' length '%v'", len(result))

	r.Equal(1, len(result), "expect one row")
	r.Equal(20, result[0], "expect count=20")

	// Reset the result.
	result = make([]int, 0)
	r.NoError(ch.chbackend.Select(&result, "SELECT count() FROM default.t1 WHERE dt NOT IN ('2022-01-01 00:00:00','2022-01-02 00:00:00')"))

	log.Debugf("testBackupSpecifiedPartitions result : '%v'", result)
	log.Debugf("testBackupSpecifiedPartitions result' length '%d'", len(result))
	r.Equal(1, len(result), "expect one row")
	r.Equal(0, result[0], "expect count=0")

	// DELETE remote backup.
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", partitionBackupName))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", partitionBackupName))

	log.Info("testBackupSpecifiedPartitions finish")
}
