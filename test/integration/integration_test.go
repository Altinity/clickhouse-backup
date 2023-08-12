//go:build integration

package main

import (
	"context"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/logcli"
	"github.com/Altinity/clickhouse-backup/pkg/partition"
	"github.com/Altinity/clickhouse-backup/pkg/status"
	"github.com/Altinity/clickhouse-backup/pkg/utils"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/apex/log"
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

var defaultTestData = []TestDataStruct{
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
		OrderBy: Issue331Atomic + "_{test}",
	}, {
		Database: Issue331Ordinary, DatabaseEngine: "Ordinary",
		Name:   Issue331Ordinary, // need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		Schema: fmt.Sprintf("(`%s` String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMM(order_time) ORDER BY (order_time, `%s`)", Issue331Ordinary, Issue331Ordinary),
		Rows: []map[string]interface{}{
			{Issue331Ordinary: "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{Issue331Ordinary: "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{Issue331Ordinary, "order_time", "amount"},
		OrderBy: Issue331Ordinary + "_{test}",
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
		Name:   "jbod_table",
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
		Name:   "jbod_table",
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
		Schema:             fmt.Sprintf("(id UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}/{uuid}','replica1') ORDER BY id AS SELECT max(id) AS id FROM `%s`.`mv_src_table_{test}`", dbNameAtomic),
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
		Schema:         fmt.Sprintf(" AS SELECT count() AS cnt FROM `%s`.`mv_src_table_{test}`", dbNameAtomic),
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
		Schema:             fmt.Sprintf(" TO `%s`.`mv_dst_table_{test}` AS SELECT max(id) AS id FROM `%s`.mv_src_table_{test}", dbNameAtomic, dbNameAtomic),
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
		Schema:             fmt.Sprintf(" TO `%s`.`mv_dst_table_{test}` AS SELECT min(id) * 2 AS id FROM `%s`.mv_src_table_{test}", dbNameAtomic, dbNameAtomic),
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
		OrderBy: Issue331Atomic + "_{test}",
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

var defaultIncrementData = []TestDataStruct{
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
		OrderBy: Issue331Atomic + "_{test}",
	}, {
		Database: Issue331Ordinary, DatabaseEngine: "Ordinary",
		Name:   Issue331Ordinary, // need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		Schema: fmt.Sprintf("(`%s` String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMM(order_time) ORDER BY (order_time, `%s`)", Issue331Ordinary, Issue331Ordinary),
		Rows: []map[string]interface{}{
			{Issue331Ordinary: "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{Issue331Ordinary: "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{Issue331Ordinary, "order_time", "amount"},
		OrderBy: Issue331Ordinary + "_{test}",
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
		Name:   "jbod_table",
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
	log.SetHandler(logcli.New(os.Stdout))
	logLevel := "info"
	if os.Getenv("LOG_LEVEL") != "" {
		logLevel = os.Getenv("LOG_LEVEL")
	}
	log.SetLevelFromString(logLevel)
	r := require.New(&testing.T{})
	installDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "curl")
	r.NoError(dockerExec("clickhouse-backup", "update-ca-certificates"))
	r.NoError(dockerExec("clickhouse-backup", "bash", "-xce", "curl -sL \"https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture)\" -o /usr/bin/yq && chmod +x /usr/bin/yq"))
	installDebIfNotExists(r, "clickhouse-backup", "jq", "bzip2", "pgp", "git")
	// rsync
	installDebIfNotExists(r, "clickhouse-backup", "openssh-client", "rsync")
	// kopia
	r.NoError(dockerExec("clickhouse-backup", "bash", "-ce", "curl -sfL https://kopia.io/signing-key | gpg --dearmor -o /usr/share/keyrings/kopia-keyring.gpg"))
	r.NoError(dockerExec("clickhouse-backup", "bash", "-ce", "echo 'deb [signed-by=/usr/share/keyrings/kopia-keyring.gpg] https://packages.kopia.io/apt/ stable main' > /etc/apt/sources.list.d/kopia.list"))
	installDebIfNotExists(r, "clickhouse-backup", "kopia")
	// restic
	r.NoError(dockerExec("clickhouse-backup", "bash", "-xec", "RELEASE_TAG=$(curl -H 'Accept: application/json' -sL https://github.com/restic/restic/releases/latest | jq -c -r -M '.tag_name'); RELEASE=$(echo ${RELEASE_TAG} | sed -e 's/v//'); curl -sfL \"https://github.com/restic/restic/releases/download/${RELEASE_TAG}/restic_${RELEASE}_linux_amd64.bz2\" | bzip2 -d > /bin/restic; chmod +x /bin/restic"))
}

// TestS3NoDeletePermission - no parallel
func TestS3NoDeletePermission(t *testing.T) {
	if isTestShouldSkip("RUN_ADVANCED_TESTS") {
		t.Skip("Skipping Advanced integration tests...")
		return
	}
	r := require.New(t)
	r.NoError(dockerExec("minio", "/bin/minio_nodelete.sh"))
	r.NoError(dockerCP("config-s3-nodelete.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))

	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 2*time.Second)
	defer ch.chbackend.Close()
	generateTestData(t, r, ch, "S3", defaultTestData)
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "create_remote", "no_delete_backup"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "delete", "local", "no_delete_backup"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "restore_remote", "no_delete_backup"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "delete", "local", "no_delete_backup"))
	r.Error(dockerExec("clickhouse-backup", "clickhouse-backup", "delete", "remote", "no_delete_backup"))
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameMySQL, dbNamePostgreSQL, Issue331Atomic, Issue331Ordinary}
	dropDatabasesFromTestDataDataSet(t, r, ch, databaseList)
	r.NoError(dockerExec("minio", "bash", "-ce", "rm -rf /data/clickhouse/*"))
}

// TestDoRestoreRBAC need clickhouse-server restart, no parallel
func TestDoRestoreRBAC(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.4") == -1 {
		t.Skipf("Test skipped, RBAC not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	ch := &TestClickHouse{}
	r := require.New(t)

	ch.connectWithWait(r, 1*time.Second, 1*time.Second)

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

	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--rbac", "--rbac-only", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup upload test_rbac_backup"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	log.Info("drop all RBAC related objects after backup")
	ch.queryWithNoError(r, "DROP SETTINGS PROFILE test_rbac")
	ch.queryWithNoError(r, "DROP QUOTA test_rbac")
	ch.queryWithNoError(r, "DROP ROW POLICY test_rbac ON default.test_rbac")
	ch.queryWithNoError(r, "DROP ROLE test_rbac")
	ch.queryWithNoError(r, "DROP USER test_rbac")

	log.Info("download+restore RBAC")
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))
	r.NoError(dockerExec("clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup download test_rbac_backup"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", "--rbac", "--rbac-only", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	ch.chbackend.Close()
	ch.connectWithWait(r, 2*time.Second, 8*time.Second)

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
			//r.NoError(dockerExec("clickhouse", "cat", "/var/log/clickhouse-server/clickhouse-server.log"))
			r.Failf("wrong RBAC", "SHOW %s, %#v doesn't contain %#v", rbacType, rbacRows, expectedValue)
		}
	}
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", "test_rbac_backup"))

	ch.queryWithNoError(r, "DROP SETTINGS PROFILE test_rbac")
	ch.queryWithNoError(r, "DROP QUOTA test_rbac")
	ch.queryWithNoError(r, "DROP ROW POLICY test_rbac ON default.test_rbac")
	ch.queryWithNoError(r, "DROP ROLE test_rbac")
	ch.queryWithNoError(r, "DROP USER test_rbac")
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_rbac")
	ch.chbackend.Close()

}

// TestDoRestoreConfigs - require direct access to `/etc/clickhouse-backup/`, so executed inside `clickhouse` container
// need clickhouse-server restart, no parallel
func TestDoRestoreConfigs(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "1.1.54391") < 0 {
		t.Skipf("Test skipped, users.d is not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Millisecond, 1*time.Second)
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_configs")
	ch.queryWithNoError(r, "CREATE TABLE default.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")

	r.NoError(dockerExec("clickhouse", "bash", "-ce", "echo '<yandex><profiles><default><empty_result_for_aggregation_by_empty_set>1</empty_result_for_aggregation_by_empty_set></default></profiles></yandex>' > /etc/clickhouse-server/users.d/test_config.xml"))

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--configs", "--configs-only", "test_configs_backup"))
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_configs")
	r.NoError(dockerExec("clickhouse", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml S3_COMPRESSION_FORMAT=none ALLOW_EMPTY_BACKUPS=1 clickhouse-backup upload test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_configs_backup"))

	ch.chbackend.Close()
	ch.connectWithWait(r, 1*time.Second, 1*time.Second)
	ch.queryWithNoError(r, "SYSTEM RELOAD CONFIG")
	selectEmptyResultForAggQuery := "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"
	var settings string
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&settings, selectEmptyResultForAggQuery))
	r.Equal("1", settings, "expect empty_result_for_aggregation_by_empty_set=1")

	r.NoError(dockerExec("clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml"))
	r.NoError(dockerExec("clickhouse", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml ALLOW_EMPTY_BACKUPS=1 clickhouse-backup download test_configs_backup"))

	r.NoError(ch.chbackend.Query("SYSTEM RELOAD CONFIG"))
	ch.chbackend.Close()
	ch.connectWithWait(r, 1*time.Second, 1*time.Second)

	settings = ""
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
	r.Equal("0", settings, "expect empty_result_for_aggregation_by_empty_set=0")

	r.NoError(dockerExec("clickhouse", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml CLICKHOUSE_RESTART_COMMAND='sql:SYSTEM RELOAD CONFIG' clickhouse-backup restore --rm --configs --configs-only test_configs_backup"))

	ch.chbackend.Close()
	ch.connectWithWait(r, 1*time.Second, 1*time.Second)

	settings = ""
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
	r.Equal("1", settings, "expect empty_result_for_aggregation_by_empty_set=1")

	isTestConfigsTablePresent := 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&isTestConfigsTablePresent, "SELECT count() FROM system.tables WHERE database='default' AND name='test_configs' SETTINGS empty_result_for_aggregation_by_empty_set=1"))
	r.Equal(0, isTestConfigsTablePresent, "expect default.test_configs is not present")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml"))

	ch.chbackend.Close()
}

// TestLongListRemote - no parallel, cause need to restart minito
func TestLongListRemote(t *testing.T) {
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second, 1*time.Second)
	defer ch.chbackend.Close()
	totalCacheCount := 20
	testBackupName := "test_list_remote"

	for i := 0; i < totalCacheCount; i++ {
		r.NoError(dockerExec("clickhouse-backup", "bash", "-ce", fmt.Sprintf("CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml ALLOW_EMPTY_BACKUPS=true clickhouse-backup create_remote %s_%d", testBackupName, i)))
	}

	r.NoError(dockerExec("clickhouse-backup", "rm", "-rfv", "/tmp/.clickhouse-backup-metadata.cache.S3"))
	r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker-compose", "-f", os.Getenv("COMPOSE_FILE"), "restart", "minio"))
	time.Sleep(2 * time.Second)

	startFirst := time.Now()
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "remote"))
	noCacheDuration := time.Since(startFirst)

	r.NoError(dockerExec("clickhouse-backup", "chmod", "-Rv", "+r", "/tmp/.clickhouse-backup-metadata.cache.S3"))

	startCashed := time.Now()
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "remote"))
	cashedDuration := time.Since(startCashed)

	r.Greater(noCacheDuration, cashedDuration)

	r.NoError(dockerExec("clickhouse-backup", "rm", "-Rfv", "/tmp/.clickhouse-backup-metadata.cache.S3"))
	r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker-compose", "-f", os.Getenv("COMPOSE_FILE"), "restart", "minio"))
	time.Sleep(2 * time.Second)

	startCacheClear := time.Now()
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "remote"))
	cacheClearDuration := time.Since(startCacheClear)

	r.Greater(cacheClearDuration, cashedDuration)
	log.Infof("noCacheDuration=%s cachedDuration=%s cacheClearDuration=%s", noCacheDuration.String(), cashedDuration.String(), cacheClearDuration.String())

	testListRemoteAllBackups := make([]string, totalCacheCount)
	for i := 0; i < totalCacheCount; i++ {
		testListRemoteAllBackups[i] = fmt.Sprintf("%s_%d", testBackupName, i)
	}
	fullCleanup(t, r, ch, testListRemoteAllBackups, []string{"remote", "local"}, []string{}, true, true, "config-s3.yml")
}

func TestServerAPI(t *testing.T) {
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second, 10*time.Second)
	defer func() {
		ch.chbackend.Close()
	}()
	r.NoError(dockerCP("config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	fieldTypes := []string{"UInt64", "String", "Int"}
	installDebIfNotExists(r, "clickhouse-backup", "curl")
	maxTables := 10
	minFields := 10
	randFields := 10
	fillDatabaseForAPIServer(maxTables, minFields, randFields, ch, r, fieldTypes)

	log.Info("Run `clickhouse-backup server --watch` in background")
	r.NoError(dockerExec("-d", "clickhouse-backup", "bash", "-ce", "clickhouse-backup server --watch &>>/tmp/clickhouse-backup-server.log"))
	time.Sleep(1 * time.Second)

	testAPIBackupCreate(r)

	testAPIBackupTables(r)

	log.Info("Check /backup/actions")
	ch.queryWithNoError(r, "SELECT count() FROM system.backup_actions")

	testAPIBackupUpload(r)

	testAPIBackupList(t, r)

	testAPIDeleteLocalDownloadRestore(r)

	testAPIMetrics(r, ch)

	testAPIWatchAndKill(r, ch)

	testAPIBackupActions(r, ch)

	testAPIRestart(r, ch)

	testAPIBackupDelete(r)

	r.NoError(dockerExec("clickhouse-backup", "pkill", "-n", "-f", "clickhouse-backup"))
	r.NoError(ch.dropDatabase("long_schema"))
}

func testAPIRestart(r *require.Assertions, ch *TestClickHouse) {
	out, err := dockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL -XPOST 'http://localhost:7171/restart'")
	log.Debug(out)
	r.NoError(err)
	r.Contains(out, "acknowledged")

	//some actions need time for restart
	time.Sleep(6 * time.Second)

	var inProgressActions uint64
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&inProgressActions, "SELECT count() FROM system.backup_actions WHERE status!=?", status.CancelStatus))
	r.Equal(uint64(0), inProgressActions)
}

func runClickHouseClientInsertSystemBackupActions(r *require.Assertions, ch *TestClickHouse, commands []string, needWait bool) {
	sql := "INSERT INTO system.backup_actions(command) " + "VALUES ('" + strings.Join(commands, "'),('") + "')"
	out, err := dockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("clickhouse client --echo -mn -q \"%s\"", sql))
	log.Debug(out)
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
func testAPIBackupActions(r *require.Assertions, ch *TestClickHouse) {
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{"create_remote actions_backup1"}, true)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{"delete local actions_backup1", "restore_remote --rm actions_backup1"}, true)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{"delete local actions_backup1", "delete remote actions_backup1"}, false)

	runClickHouseClientInsertSystemBackupActions(r, ch, []string{"create actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{"upload actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{"delete local actions_backup2"}, false)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{"download actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{"restore --rm actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{"delete local actions_backup2", "delete remote actions_backup2"}, false)

	inProgressActions := make([]struct {
		Command string `ch:"command"`
		Status  string `ch:"status"`
	}, 0)
	r.NoError(ch.chbackend.StructSelect(&inProgressActions, "SELECT command, status FROM system.backup_actions WHERE command LIKE '%actions%' AND status IN (?,?)", status.InProgressStatus, status.ErrorStatus))
	r.Equal(0, len(inProgressActions), "inProgressActions=%+v", inProgressActions)

	var actionsBackups uint64
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&actionsBackups, "SELECT count() FROM system.backup_list WHERE name LIKE 'backup_action%'"))
	r.Equal(uint64(0), actionsBackups)

	out, err := dockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err)
	r.Contains(out, "clickhouse_backup_last_create_remote_status 1")
	r.Contains(out, "clickhouse_backup_last_create_status 1")
	r.Contains(out, "clickhouse_backup_last_upload_status 1")
	r.Contains(out, "clickhouse_backup_last_delete_status 1")
	r.Contains(out, "clickhouse_backup_last_download_status 1")
	r.Contains(out, "clickhouse_backup_last_restore_status 1")
}

func testAPIWatchAndKill(r *require.Assertions, ch *TestClickHouse) {
	log.Info("Check /backup/watch + /backup/kill")
	runKillCommand := func(command string) {
		out, err := dockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL 'http://localhost:7171/backup/kill?command=%s'", command))
		log.Debug(out)
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

	out, err := dockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/watch'")
	log.Debug(out)
	r.NoError(err)
	time.Sleep(7 * time.Second)

	checkWatchBackup(2)
	runKillCommand("watch")
	checkCanceledCommand(2)
}

func testAPIBackupDelete(r *require.Assertions) {
	log.Info("Check /backup/delete/{where}/{name}")
	for i := 1; i <= apiBackupNumber; i++ {
		out, err := dockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/local/z_backup_%d'", i))
		log.Infof(out)
		r.NoError(err)
		r.NotContains(out, "another operation is currently running")
		r.NotContains(out, "\"status\":\"error\"")
		out, err = dockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/remote/z_backup_%d'", i))
		log.Infof(out)
		r.NoError(err)
		r.NotContains(out, "another operation is currently running")
		r.NotContains(out, "\"status\":\"error\"")
	}
	out, err := dockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err)
	r.Contains(out, "clickhouse_backup_last_delete_status 1")
}

func testAPIMetrics(r *require.Assertions, ch *TestClickHouse) {
	log.Info("Check /metrics clickhouse_backup_last_backup_size_remote")
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

	out, err := dockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/metrics")
	log.Debug(out)
	r.NoError(err)
	r.Contains(out, fmt.Sprintf("clickhouse_backup_last_backup_size_remote %d", lastRemoteSize))

	log.Info("Check /metrics clickhouse_backup_number_backups_*")
	r.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_local %d", apiBackupNumber))
	// +1 watch backup
	r.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_remote %d", apiBackupNumber+1))
	r.Contains(out, "clickhouse_backup_number_backups_local_expected 0")
	r.Contains(out, "clickhouse_backup_number_backups_remote_expected 0")
}

func testAPIDeleteLocalDownloadRestore(r *require.Assertions) {
	log.Info("Check /backup/delete/local/{name} + /backup/download/{name} + /backup/restore/{name}?rm=1")
	out, err := dockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c",
		fmt.Sprintf("for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/delete/local/z_backup_$i\"; curl -sfL -XPOST \"http://localhost:7171/backup/download/z_backup_$i\"; sleep 2; curl -sfL -XPOST \"http://localhost:7171/backup/restore/z_backup_$i?rm=1\"; sleep 8; done", apiBackupNumber),
	)
	log.Debug(out)
	r.NoError(err)
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")

	out, err = dockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err)
	r.Contains(out, "clickhouse_backup_last_delete_status 1")
	r.Contains(out, "clickhouse_backup_last_download_status 1")
	r.Contains(out, "clickhouse_backup_last_restore_status 1")
}

func testAPIBackupList(t *testing.T, r *require.Assertions) {
	log.Info("Check /backup/list")
	out, err := dockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list'")
	log.Debug(out)
	r.NoError(err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"regular\"}", i)), out))
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar, regular\"}", i)), out))
	}

	log.Info("Check /backup/list/local")
	out, err = dockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list/local'")
	log.Debug(out)
	r.NoError(err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"regular\"}", i)), out))
		r.True(assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar, regular\"}", i)), out))
	}

	log.Info("Check /backup/list/remote")
	out, err = dockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list/remote'")
	log.Debug(out)
	r.NoError(err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"regular\"}", i)), out))
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar, regular\"}", i)), out))
	}
}

func testAPIBackupUpload(r *require.Assertions) {
	log.Info("Check /backup/upload")
	out, err := dockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c",
		fmt.Sprintf("for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/upload/z_backup_$i\"; sleep 2; done", apiBackupNumber),
	)
	log.Debug(out)
	r.NoError(err)
	r.NotContains(out, "\"status\":\"error\"")
	r.NotContains(out, "another operation is currently running")
	out, err = dockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err)
	r.Contains(out, "clickhouse_backup_last_upload_status 1")
}

func testAPIBackupTables(r *require.Assertions) {
	log.Info("Check /backup/tables")
	out, err := dockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables\"",
	)
	log.Debug(out)
	r.NoError(err)
	r.Contains(out, "long_schema")
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	r.NotContains(out, "system")
	r.NotContains(out, "INFORMATION_SCHEMA")
	r.NotContains(out, "information_schema")

	log.Info("Check /backup/tables/all")
	out, err = dockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables/all\"",
	)
	log.Debug(out)
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
	log.Info("Check /backup/create")
	out, err := dockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c",
		fmt.Sprintf("sleep 3; for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/create?table=long_schema.*&name=z_backup_$i\"; sleep 1.5; done", apiBackupNumber),
	)
	log.Debug(out)
	r.NoError(err)
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	out, err = dockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err)
	r.Contains(out, "clickhouse_backup_last_create_status 1")

}

func fillDatabaseForAPIServer(maxTables int, minFields int, randFields int, ch *TestClickHouse, r *require.Assertions, fieldTypes []string) {
	log.Infof("Create %d `long_schema`.`t%%d` tables with with %d..%d fields...", maxTables, minFields, minFields+randFields)
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
	log.Info("...DONE")
}

func TestSkipNotExistsTable(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.1") < 0 {
		t.Skip("TestSkipNotExistsTable too small time between `SELECT DISTINCT partition_id` and `ALTER TABLE ... FREEZE PARTITION`")
	}
	//t.Parallel()
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second, 1*time.Second)
	defer ch.chbackend.Close()

	log.Info("Check skip not exist errors")
	ch.queryWithNoError(r, "CREATE DATABASE freeze_not_exists")
	ifNotExistsCreateSQL := "CREATE TABLE IF NOT EXISTS freeze_not_exists.freeze_not_exists (id UInt64) ENGINE=MergeTree() ORDER BY id"
	ifNotExistsInsertSQL := "INSERT INTO freeze_not_exists.freeze_not_exists SELECT number FROM numbers(1000)"
	chVersion, err := ch.chbackend.GetVersion(context.Background())
	r.NoError(err)

	freezeErrorHandled := false
	pauseChannel := make(chan int64)
	resumeChannel := make(chan int64)
	ch.chbackend.Config.LogSQLQueries = true
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer func() {
			close(pauseChannel)
			wg.Done()
		}()
		pause := int64(0)
		// pausePercent := int64(90)
		for i := int64(0); i < 100; i++ {
			testBackupName := fmt.Sprintf("not_exists_%d", i)
			err = ch.chbackend.Query(ifNotExistsCreateSQL)
			r.NoError(err)
			err = ch.chbackend.Query(ifNotExistsInsertSQL)
			r.NoError(err)
			if i < 5 {
				log.Infof("pauseChannel <- %d", 0)
				pauseChannel <- 0
			} else {
				log.Infof("pauseChannel <- %d", pause/i)
				pauseChannel <- pause / i
			}
			startTime := time.Now()
			out, err := dockerExecOut("clickhouse-backup", "bash", "-ce", "LOG_LEVEL=debug CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create --table freeze_not_exists.freeze_not_exists "+testBackupName)
			log.Info(out)
			if (err != nil && (strings.Contains(out, "can't freeze") || strings.Contains(out, "no tables for backup"))) ||
				(err == nil && !strings.Contains(out, "can't freeze")) {
				parseTime := func(line string) time.Time {
					parsedTime, err := time.Parse("2006/01/02 15:04:05.999999", line[:26])
					if err != nil {
						r.Failf("Error parsing time", "%s, : %v", line, err)
					}
					return parsedTime
				}
				lines := strings.Split(out, "\n")
				firstTime := parseTime(lines[0])
				var freezeTime time.Time
				for _, line := range lines {
					if strings.Contains(line, "create_table_query") {
						freezeTime = parseTime(line)
						break
					}
					if strings.Contains(line, "SELECT DISTINCT partition_id") {
						freezeTime = parseTime(line)
						break
					}
				}
				pause += (firstTime.Sub(startTime) + freezeTime.Sub(firstTime)).Nanoseconds()
			}
			if err != nil {
				if !strings.Contains(out, "no tables for backup") {
					assert.NoError(t, err)
				}
			}

			if strings.Contains(out, "code: 60") && err == nil {
				freezeErrorHandled = true
				<-resumeChannel
				r.NoError(dockerExec("clickhouse-backup", "bash", "-ec", "CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup delete local "+testBackupName))
				break
			}
			if err == nil {
				err = dockerExec("clickhouse-backup", "bash", "-ec", "CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup delete local "+testBackupName)
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
			log.Infof("%d <- pauseChannel", pause)
			if pause > 0 {
				pauseStart := time.Now()
				time.Sleep(time.Duration(pause) * time.Nanosecond)
				log.Infof("pause=%s pauseStart=%s", time.Duration(pause).String(), pauseStart.String())
				err = ch.chbackend.DropTable(clickhouse.Table{Database: "freeze_not_exists", Name: "freeze_not_exists"}, ifNotExistsCreateSQL, "", false, chVersion)
				r.NoError(err)
			}
			resumeChannel <- 1
		}
	}()
	wg.Wait()
	r.True(freezeErrorHandled)
	dropDbSQL := "DROP DATABASE freeze_not_exists"
	if isAtomic, err := ch.chbackend.IsAtomic("freeze_not_exists"); err == nil && isAtomic {
		dropDbSQL += " SYNC"
	}
	ch.queryWithNoError(r, dropDbSQL)
}

func TestTablePatterns(t *testing.T) {
	//t.Parallel()
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 500*time.Millisecond, 5*time.Second)
	defer ch.chbackend.Close()

	testBackupName := "test_backup_patterns"
	databaseList := []string{dbNameOrdinary, dbNameAtomic}
	var dbNameOrdinaryTest = dbNameOrdinary + "_" + t.Name()
	var dbNameAtomicTest = dbNameAtomic + "_" + t.Name()

	for _, createPattern := range []bool{true, false} {
		for _, restorePattern := range []bool{true, false} {
			fullCleanup(t, r, ch, []string{testBackupName}, []string{"remote", "local"}, databaseList, false, false, "config-s3.yml")
			generateTestData(t, r, ch, "S3", defaultTestData)
			if createPattern {
				r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create_remote", "--tables", " "+dbNameOrdinaryTest+".*", testBackupName))
			} else {
				r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "create_remote", testBackupName))
			}

			r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "delete", "local", testBackupName))
			dropDatabasesFromTestDataDataSet(t, r, ch, databaseList)
			if restorePattern {
				r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "restore_remote", "--tables", " "+dbNameOrdinaryTest+".*", testBackupName))
			} else {
				r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "restore_remote", testBackupName))
			}

			restored := uint64(0)
			r.NoError(ch.chbackend.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameOrdinaryTest)))
			r.NotZero(restored)

			if createPattern || restorePattern {
				restored = 0
				r.NoError(ch.chbackend.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameAtomicTest)))
				// todo, old versions of clickhouse will return empty recordset
				r.Zero(restored)

				restored = 0
				r.NoError(ch.chbackend.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.databases WHERE name='%s'", dbNameAtomicTest)))
				// todo, old versions of clickhouse will return empty recordset
				r.Zero(restored)
			} else {
				restored = 0
				r.NoError(ch.chbackend.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameAtomicTest)))
				r.NotZero(restored)
			}

			fullCleanup(t, r, ch, []string{testBackupName}, []string{"remote", "local"}, databaseList, true, true, "config-s3.yml")

		}
	}
}

func TestProjections(t *testing.T) {
	var err error
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") == -1 {
		t.Skipf("Test skipped, PROJECTION available only 21.8+, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	//t.Parallel()
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 0*time.Second, 1*time.Second)
	defer ch.chbackend.Close()

	r.NoError(dockerCP("config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	err = ch.chbackend.Query("CREATE TABLE default.table_with_projection(dt DateTime, v UInt64, PROJECTION x (SELECT toStartOfMonth(dt) m, sum(v) GROUP BY m)) ENGINE=MergeTree() ORDER BY dt")
	r.NoError(err)

	err = ch.chbackend.Query("INSERT INTO default.table_with_projection SELECT today() - INTERVAL number DAY, number FROM numbers(10)")
	r.NoError(err)

	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "create", "test_backup_projection"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "restore", "--rm", "test_backup_projection"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "delete", "local", "test_backup_projection"))
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
	//t.Parallel()
	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 2*time.Second)
	backupNames := make([]string, 5)
	for i := 0; i < 5; i++ {
		backupNames[i] = fmt.Sprintf("keep_remote_backup_%d", i)
	}
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameMySQL, dbNamePostgreSQL, Issue331Atomic, Issue331Ordinary}
	fullCleanup(t, r, ch, backupNames, []string{"remote", "local"}, databaseList, false, false, "config-s3.yml")
	generateTestData(t, r, ch, "S3", defaultTestData)
	for i, backupName := range backupNames {
		generateIncrementTestData(t, ch, r, defaultIncrementData)
		if i == 0 {
			r.NoError(dockerExec("clickhouse-backup", "bash", "-ce", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create_remote %s", backupName)))
		} else {
			r.NoError(dockerExec("clickhouse-backup", "bash", "-ce", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create_remote --diff-from-remote=%s %s", backupNames[i-1], backupName)))
		}
	}
	out, err := dockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list local")
	r.NoError(err)
	// shall not delete any backup, cause all deleted backup have links as required in other backups
	for _, backupName := range backupNames {
		r.Contains(out, backupName)
		r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName))
	}
	latestIncrementBackup := fmt.Sprintf("keep_remote_backup_%d", len(backupNames)-1)
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", latestIncrementBackup))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", latestIncrementBackup))
	var res uint64
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&res, fmt.Sprintf("SELECT count() FROM `%s_%s`.`%s_%s`", Issue331Atomic, t.Name(), Issue331Atomic, t.Name())))
	r.Equal(uint64(200), res)
	fullCleanup(t, r, ch, backupNames, []string{"remote", "local"}, databaseList, true, true, "config-s3.yml")
}

func TestSyncReplicaTimeout(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.11") == -1 {
		t.Skipf("Test skipped, SYNC REPLICA ignore receive_timeout for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	//t.Parallel()
	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 0*time.Millisecond, 2*time.Second)
	defer ch.chbackend.Close()

	createDbSQL := "CREATE DATABASE IF NOT EXISTS " + t.Name()
	ch.queryWithNoError(r, createDbSQL)
	dropReplTables := func() {
		for _, table := range []string{"repl1", "repl2"} {
			query := "DROP TABLE IF EXISTS " + t.Name() + "." + table
			if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") == 1 {
				query += " NO DELAY"
			}
			ch.queryWithNoError(r, query)
		}
	}
	dropReplTables()
	ch.queryWithNoError(r, "CREATE TABLE "+t.Name()+".repl1 (v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/"+t.Name()+"/repl','repl1') ORDER BY tuple()")
	ch.queryWithNoError(r, "CREATE TABLE "+t.Name()+".repl2 (v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/"+t.Name()+"/repl','repl2') ORDER BY tuple()")

	ch.queryWithNoError(r, "INSERT INTO "+t.Name()+".repl1 SELECT number FROM numbers(10)")

	ch.queryWithNoError(r, "SYSTEM STOP REPLICATED SENDS "+t.Name()+".repl1")
	ch.queryWithNoError(r, "SYSTEM STOP FETCHES "+t.Name()+".repl2")

	ch.queryWithNoError(r, "INSERT INTO "+t.Name()+".repl1 SELECT number FROM numbers(100)")

	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+t.Name()+".repl*", "test_not_synced_backup"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "upload", "test_not_synced_backup"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_not_synced_backup"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", "test_not_synced_backup"))

	ch.queryWithNoError(r, "SYSTEM START REPLICATED SENDS "+t.Name()+".repl1")
	ch.queryWithNoError(r, "SYSTEM START FETCHES "+t.Name()+".repl2")

	dropReplTables()
	r.NoError(ch.dropDatabase(t.Name()))
}

func TestGetPartitionId(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") == -1 {
		t.Skipf("Test skipped, is_in_partition_key not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	//t.Parallel()
	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 1*time.Second)
	defer ch.chbackend.Close()

	type testData struct {
		CreateTableSQL string
		Database       string
		Table          string
		Partition      string
		ExpectedId     string
		ExpectedName   string
	}
	testCases := []testData{
		{
			"CREATE TABLE default.test_part_id_1 UUID 'b45e751f-6c06-42a3-ab4a-f5bb9ac3716e' (dt Date, version DateTime, category String, name String) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}','{replica}',version) ORDER BY dt PARTITION BY (toYYYYMM(dt),category)",
			"default",
			"test_part_id_1",
			"('2023-01-01','category1')",
			"cc1ad6ede2e7f708f147e132cac7a590",
			"(202301,'category1')",
		},
		{
			"CREATE TABLE default.test_part_id_2 (dt Date, version DateTime, name String) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}','{replica}',version) ORDER BY dt PARTITION BY toYYYYMM(dt)",
			"default",
			"test_part_id_2",
			"'2023-01-01'",
			"202301",
			"202301",
		},
		{
			"CREATE TABLE default.test_part_id_3 ON CLUSTER '{cluster}' (i UInt32, name String) ENGINE = ReplicatedMergeTree() ORDER BY i PARTITION BY i",
			"default",
			"test_part_id_3",
			"202301",
			"202301",
			"202301",
		},
		{
			"CREATE TABLE default.test_part_id_4 (dt String, name String) ENGINE = MergeTree ORDER BY dt PARTITION BY dt",
			"default",
			"test_part_id_4",
			"'2023-01-01'",
			"c487903ebbb25a533634d6ec3485e3a9",
			"2023-01-01",
		},
		{
			"CREATE TABLE default.test_part_id_5 (dt String, name String) ENGINE = Memory",
			"default",
			"test_part_id_5",
			"'2023-01-01'",
			"",
			"",
		},
	}
	if isAtomic, _ := ch.chbackend.IsAtomic("default"); !isAtomic {
		testCases[0].CreateTableSQL = strings.Replace(testCases[0].CreateTableSQL, "UUID 'b45e751f-6c06-42a3-ab4a-f5bb9ac3716e'", "", 1)
	}
	for _, tc := range testCases {
		partitionId, partitionName, err := partition.GetPartitionIdAndName(context.Background(), ch.chbackend, tc.Database, tc.Table, tc.CreateTableSQL, tc.Partition)
		assert.NoError(t, err)
		assert.Equal(t, tc.ExpectedId, partitionId)
		assert.Equal(t, tc.ExpectedName, partitionName)
	}
}

func TestRestoreMutationInProgress(t *testing.T) {
	//t.Parallel()
	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 0*time.Second, 5*time.Second)
	defer ch.chbackend.Close()
	version, err := ch.chbackend.GetVersion(context.Background())
	r.NoError(err)
	zkPath := "/clickhouse/tables/{shard}/" + t.Name() + "/test_restore_mutation_in_progress"
	onCluster := ""
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		zkPath = "/clickhouse/tables/{shard}/{database}/{table}"
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.3") >= 0 {
		zkPath = "/clickhouse/tables/{shard}/{database}/{table}/{uuid}"
		onCluster = " ON CLUSTER '{cluster}'"
	}
	createDbSQL := "CREATE DATABASE IF NOT EXISTS " + t.Name()
	ch.queryWithNoError(r, createDbSQL)

	createSQL := fmt.Sprintf("CREATE TABLE %s.test_restore_mutation_in_progress %s (id UInt64, attr String) ENGINE=ReplicatedMergeTree('%s','{replica}') PARTITION BY id ORDER BY id", t.Name(), onCluster, zkPath)
	ch.queryWithNoError(r, createSQL)
	ch.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_restore_mutation_in_progress SELECT number, if(number>0,'a',toString(number)) FROM numbers(2)")

	mutationSQL := "ALTER TABLE " + t.Name() + ".test_restore_mutation_in_progress MODIFY COLUMN attr UInt64"
	err = ch.chbackend.QueryContext(context.Background(), mutationSQL)
	if err != nil {
		errStr := strings.ToLower(err.Error())
		r.True(strings.Contains(errStr, "code: 341") || strings.Contains(errStr, "code: 517") || strings.Contains(errStr, "timeout"), "UNKNOWN ERROR: %s", err.Error())
		t.Logf("%s RETURN EXPECTED ERROR=%#v", mutationSQL, err)
	}

	attrs := make([]struct {
		Attr uint64 `ch:"attr"`
	}, 0)
	err = ch.chbackend.Select(&attrs, "SELECT attr FROM "+t.Name()+".test_restore_mutation_in_progress ORDER BY id")
	r.NotEqual(nil, err)
	errStr := strings.ToLower(err.Error())
	r.True(strings.Contains(errStr, "code: 53") || strings.Contains(errStr, "code: 6"))
	r.Zero(len(attrs))

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		mutationSQL = "ALTER TABLE " + t.Name() + ".test_restore_mutation_in_progress RENAME COLUMN attr TO attr_1"
		err = ch.chbackend.QueryContext(context.Background(), mutationSQL)
		r.NotEqual(nil, err)
		errStr = strings.ToLower(err.Error())
		r.True(strings.Contains(errStr, "code: 517") || strings.Contains(errStr, "timeout"))
		t.Logf("%s RETURN EXPECTED ERROR=%#v", mutationSQL, err)
	}
	r.NoError(dockerExec("clickhouse", "clickhouse", "client", "-q", "SELECT * FROM system.mutations WHERE is_done=0 FORMAT Vertical"))

	// backup with check consistency
	out, createErr := dockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+t.Name()+".test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
	r.NotEqual(createErr, nil)
	r.Contains(out, "have inconsistent data types")
	t.Log(out)

	// backup without check consistency
	out, createErr = dockerExecOut("clickhouse-backup", "clickhouse-backup", "create", "-c", "/etc/clickhouse-backup/config-s3.yml", "--skip-check-parts-columns", "--tables="+t.Name()+".test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
	t.Log(out)
	r.NoError(createErr)
	r.NotContains(out, "have inconsistent data types")

	r.NoError(ch.chbackend.DropTable(clickhouse.Table{Database: t.Name(), Name: "test_restore_mutation_in_progress"}, "", "", false, version))
	var restoreErr error
	restoreErr = dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", "--tables="+t.Name()+".test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
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
	selectSQL := fmt.Sprintf("SELECT %s FROM "+t.Name()+".test_restore_mutation_in_progress ORDER BY id", checkRestoredData)
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

	r.NoError(ch.chbackend.DropTable(clickhouse.Table{Database: t.Name(), Name: "test_restore_mutation_in_progress"}, "", "", false, version))
	r.NoError(ch.dropDatabase(t.Name()))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_restore_mutation_in_progress"))
}

func TestInnerTablesMaterializedView(t *testing.T) {
	//t.Parallel()
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 1*time.Second, 10*time.Second)
	defer ch.chbackend.Close()

	ch.queryWithNoError(r, "CREATE DATABASE test_mv")
	ch.queryWithNoError(r, "CREATE TABLE test_mv.src_table (v UInt64) ENGINE=MergeTree() ORDER BY v")
	ch.queryWithNoError(r, "CREATE TABLE test_mv.dst_table (v UInt64) ENGINE=MergeTree() ORDER BY v")
	ch.queryWithNoError(r, "CREATE MATERIALIZED VIEW test_mv.mv_with_inner (v UInt64) ENGINE=MergeTree() ORDER BY v AS SELECT v FROM test_mv.src_table")
	ch.queryWithNoError(r, "CREATE MATERIALIZED VIEW test_mv.mv_with_dst TO test_mv.dst_table AS SELECT v FROM test_mv.src_table")
	ch.queryWithNoError(r, "INSERT INTO test_mv.src_table SELECT number FROM numbers(100)")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "create", "test_mv", "--tables=test_mv.mv_with*,test_mv.dst*"))
	dropSQL := "DROP DATABASE test_mv"
	isAtomic, err := ch.chbackend.IsAtomic("test_mv")
	r.NoError(err)
	if isAtomic {
		dropSQL += " NO DELAY"
	}
	ch.queryWithNoError(r, dropSQL)
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "test_mv", "--tables=test_mv.mv_with*,test_mv.dst*"))
	var rowCnt uint64
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&rowCnt, "SELECT count() FROM test_mv.mv_with_inner"))
	r.Equal(uint64(100), rowCnt)
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&rowCnt, "SELECT count() FROM test_mv.mv_with_dst"))
	r.Equal(uint64(100), rowCnt)
	r.NoError(ch.dropDatabase("test_mv"))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_mv"))
}

func TestFIPS(t *testing.T) {
	if os.Getenv("QA_AWS_ACCESS_KEY") == "" {
		t.Skip("QA_AWS_ACCESS_KEY is empty, TestFIPS will skip")
	}
	//t.Parallel()
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r, 1*time.Second, 10*time.Second)
	defer ch.chbackend.Close()
	fipsBackupName := fmt.Sprintf("fips_backup_%d", rand.Int())
	r.NoError(dockerExec("clickhouse", "rm", "-fv", "/etc/apt/sources.list.d/clickhouse.list"))
	installDebIfNotExists(r, "clickhouse", "ca-certificates", "curl", "gettext-base", "bsdmainutils", "dnsutils", "git")
	r.NoError(dockerExec("clickhouse-backup", "update-ca-certificates"))
	r.NoError(dockerCP("config-s3-fips.yml", "clickhouse:/etc/clickhouse-backup/config.yml.fips-template"))
	r.NoError(dockerExec("clickhouse", "git", "clone", "--depth", "1", "https://github.com/drwetter/testssl.sh.git", "/opt/testssl"))
	r.NoError(dockerExec("clickhouse", "chmod", "+x", "/opt/testssl/testssl.sh"))

	generateCerts := func(certType, keyLength, curveType string) {
		r.NoError(dockerExec("clickhouse", "bash", "-xce", "openssl rand -out /root/.rnd 2048"))
		switch certType {
		case "rsa":
			r.NoError(dockerExec("clickhouse", "bash", "-xce", fmt.Sprintf("openssl genrsa -out /etc/clickhouse-backup/ca-key.pem %s", keyLength)))
			r.NoError(dockerExec("clickhouse", "bash", "-xce", fmt.Sprintf("openssl genrsa -out /etc/clickhouse-backup/server-key.pem %s", keyLength)))
		case "ecdsa":
			r.NoError(dockerExec("clickhouse", "bash", "-xce", fmt.Sprintf("openssl ecparam -name %s -genkey -out /etc/clickhouse-backup/ca-key.pem", curveType)))
			r.NoError(dockerExec("clickhouse", "bash", "-xce", fmt.Sprintf("openssl ecparam -name %s -genkey -out /etc/clickhouse-backup/server-key.pem", curveType)))
		}
		r.NoError(dockerExec("clickhouse", "bash", "-xce", "openssl req -subj \"/O=altinity\" -x509 -new -nodes -key /etc/clickhouse-backup/ca-key.pem -sha256 -days 365000 -out /etc/clickhouse-backup/ca-cert.pem"))
		r.NoError(dockerExec("clickhouse", "bash", "-xce", "openssl req -subj \"/CN=localhost\" -addext \"subjectAltName = DNS:localhost,DNS:*.cluster.local\" -new -key /etc/clickhouse-backup/server-key.pem -out /etc/clickhouse-backup/server-req.csr"))
		r.NoError(dockerExec("clickhouse", "bash", "-xce", "openssl x509 -req -days 365000 -extensions SAN -extfile <(printf \"\\n[SAN]\\nsubjectAltName=DNS:localhost,DNS:*.cluster.local\") -in /etc/clickhouse-backup/server-req.csr -out /etc/clickhouse-backup/server-cert.pem -CA /etc/clickhouse-backup/ca-cert.pem -CAkey /etc/clickhouse-backup/ca-key.pem -CAcreateserial"))
	}
	r.NoError(dockerExec("clickhouse", "bash", "-xec", "cat /etc/clickhouse-backup/config-s3-fips.yml.template | envsubst > /etc/clickhouse-backup/config-s3-fips.yml"))

	generateCerts("rsa", "4096", "")
	ch.queryWithNoError(r, "CREATE DATABASE "+t.Name())
	createSQL := "CREATE TABLE " + t.Name() + ".fips_table (v UInt64) ENGINE=MergeTree() ORDER BY tuple()"
	ch.queryWithNoError(r, createSQL)
	ch.queryWithNoError(r, "INSERT INTO "+t.Name()+".fips_table SELECT number FROM numbers(1000)")
	r.NoError(dockerExec("clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml create_remote --tables="+t.Name()+".fips_table "+fipsBackupName))
	r.NoError(dockerExec("clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml delete local "+fipsBackupName))
	r.NoError(dockerExec("clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml restore_remote --tables="+t.Name()+".fips_table "+fipsBackupName))
	r.NoError(dockerExec("clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml delete local "+fipsBackupName))
	r.NoError(dockerExec("clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml delete remote "+fipsBackupName))

	log.Info("Run `clickhouse-backup-fips server` in background")
	r.NoError(dockerExec("-d", "clickhouse", "bash", "-ce", "AWS_USE_FIPS_ENDPOINT=true clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml server &>>/tmp/clickhouse-backup-server-fips.log"))
	time.Sleep(1 * time.Second)

	runClickHouseClientInsertSystemBackupActions(r, ch, []string{fmt.Sprintf("create_remote --tables="+t.Name()+".fips_table %s", fipsBackupName)}, true)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{fmt.Sprintf("delete local %s", fipsBackupName)}, false)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{fmt.Sprintf("restore_remote --tables="+t.Name()+".fips_table  %s", fipsBackupName)}, true)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{fmt.Sprintf("delete local %s", fipsBackupName)}, false)
	runClickHouseClientInsertSystemBackupActions(r, ch, []string{fmt.Sprintf("delete remote %s", fipsBackupName)}, false)

	inProgressActions := make([]struct {
		Command string `ch:"command"`
		Status  string `ch:"status"`
	}, 0)
	r.NoError(ch.chbackend.StructSelect(&inProgressActions,
		"SELECT command, status FROM system.backup_actions WHERE command LIKE ? AND status IN (?,?)",
		fmt.Sprintf("%%%s%%", fipsBackupName), status.InProgressStatus, status.ErrorStatus,
	))
	r.Equal(0, len(inProgressActions), "inProgressActions=%+v", inProgressActions)
	r.NoError(dockerExec("clickhouse", "pkill", "-n", "-f", "clickhouse-backup-fips"))

	testTLSCerts := func(certType, keyLength, curveName string, cipherList ...string) {
		generateCerts(certType, keyLength, curveName)
		log.Infof("Run `clickhouse-backup-fips server` in background for %s %s %s", certType, keyLength, curveName)
		r.NoError(dockerExec("-d", "clickhouse", "bash", "-ce", "AWS_USE_FIPS_ENDPOINT=true clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml server &>>/tmp/clickhouse-backup-server-fips.log"))
		time.Sleep(1 * time.Second)

		r.NoError(dockerExec("clickhouse", "bash", "-ce", "rm -rf /tmp/testssl* && /opt/testssl/testssl.sh -e -s -oC /tmp/testssl.csv --color 0 --disable-rating --quiet -n min --mode parallel --add-ca /etc/clickhouse-backup/ca-cert.pem localhost:7172"))
		out, err := dockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("grep -c -E '%s' /tmp/testssl.csv", strings.Join(cipherList, "|")))
		r.NoError(err)
		r.Equal(strconv.Itoa(len(cipherList)), strings.Trim(out, " \t\r\n"))

		inProgressActions := make([]struct {
			Command string `ch:"command"`
			Status  string `ch:"status"`
		}, 0)
		r.NoError(ch.chbackend.StructSelect(&inProgressActions,
			"SELECT command, status FROM system.backup_actions WHERE command LIKE ? AND status IN (?,?)",
			fmt.Sprintf("%%%s%%", fipsBackupName), status.InProgressStatus, status.ErrorStatus,
		))
		r.Equal(0, len(inProgressActions), "inProgressActions=%+v", inProgressActions)
		r.NoError(dockerExec("clickhouse", "pkill", "-n", "-f", "clickhouse-backup-fips"))
	}
	// https://www.perplexity.ai/search/0920f1e8-59ec-4e14-b779-ba7b2e037196
	testTLSCerts("rsa", "4096", "", "ECDHE-RSA-AES128-GCM-SHA256", "ECDHE-RSA-AES256-GCM-SHA384", "AES128-GCM-SHA256", "AES256-GCM-SHA384")
	testTLSCerts("ecdsa", "", "prime256v1", "ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-ECDSA-AES256-GCM-SHA384")
	r.NoError(ch.chbackend.DropTable(clickhouse.Table{Database: t.Name(), Name: "fips_table"}, createSQL, "", false, 0))
	r.NoError(ch.dropDatabase(t.Name()))

}

func TestIntegrationS3Glacier(t *testing.T) {
	if isTestShouldSkip("GLACIER_TESTS") {
		t.Skip("Skipping GLACIER integration tests...")
		return
	}
	r := require.New(t)
	r.NoError(dockerCP("config-s3-glacier.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml.s3glacier-template"))
	installDebIfNotExists(r, "clickhouse-backup", "curl", "gettext-base", "bsdmainutils", "dnsutils", "git", "ca-certificates")
	r.NoError(dockerExec("clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config.yml.s3glacier-template | envsubst > /etc/clickhouse-backup/config-s3-glacier.yml"))
	dockerExecTimeout = 60 * time.Minute
	runMainIntegrationScenario(t, "GLACIER", "config-s3-glacier.yml")
	dockerExecTimeout = 3 * time.Minute
}

func TestIntegrationS3(t *testing.T) {
	//t.Parallel()
	runMainIntegrationScenario(t, "S3", "config-s3.yml")
}

func TestIntegrationGCS(t *testing.T) {
	if isTestShouldSkip("GCS_TESTS") {
		t.Skip("Skipping GCS integration tests...")
		return
	}
	//t.Parallel()
	runMainIntegrationScenario(t, "GCS", "config-gcs.yml")
}

func TestIntegrationAzure(t *testing.T) {
	if isTestShouldSkip("AZURE_TESTS") {
		t.Skip("Skipping Azure integration tests...")
		return
	}
	//t.Parallel()
	runMainIntegrationScenario(t, "AZBLOB", "config-azblob.yml")
}

func TestIntegrationSFTPAuthPassword(t *testing.T) {
	//t.Parallel()
	runMainIntegrationScenario(t, "SFTP", "config-sftp-auth-password.yaml")
}

func TestIntegrationFTP(t *testing.T) {
	//t.Parallel()
	runMainIntegrationScenario(t, "FTP", "config-ftp.yaml")
}

func TestIntegrationSFTPAuthKey(t *testing.T) {
	uploadSSHKeys(require.New(t), "clickhouse-backup")
	//t.Parallel()
	runMainIntegrationScenario(t, "SFTP", "config-sftp-auth-key.yaml")
}

func TestIntegrationCustomKopia(t *testing.T) {
	//t.Parallel()
	r := require.New(t)
	runIntegrationCustom(t, r, "kopia")
}
func TestIntegrationCustomRestic(t *testing.T) {
	//t.Parallel()
	r := require.New(t)
	runIntegrationCustom(t, r, "restic")
}

func TestIntegrationCustomRsync(t *testing.T) {
	r := require.New(t)
	uploadSSHKeys(r, "clickhouse-backup")
	//t.Parallel()
	runIntegrationCustom(t, r, "rsync")
}

func runIntegrationCustom(t *testing.T, r *require.Assertions, customType string) {
	r.NoError(dockerExec("clickhouse-backup", "mkdir", "-pv", "/custom/"+customType))
	r.NoError(dockerCP("./"+customType+"/", "clickhouse-backup:/custom/"))
	runMainIntegrationScenario(t, "CUSTOM", "config-custom-"+customType+".yml")
}

func TestIntegrationEmbedded(t *testing.T) {
	//t.Skipf("Test skipped, wait 23.8, RESTORE Ordinary table and RESTORE MATERIALIZED VIEW and {uuid} not works for %s version, look https://github.com/ClickHouse/ClickHouse/issues/43971 and https://github.com/ClickHouse/ClickHouse/issues/42709", os.Getenv("CLICKHOUSE_VERSION"))
	//dependencies restore https://github.com/ClickHouse/ClickHouse/issues/39416, fixed in 23.3
	version := os.Getenv("CLICKHOUSE_VERSION")
	if version != "head" && compareVersion(version, "23.3") < 0 {
		t.Skipf("Test skipped, BACKUP/RESTORE not production ready for %s version", version)
	}
	//t.Parallel()
	r := require.New(t)
	//CUSTOM backup create folder in each disk
	r.NoError(dockerExec("clickhouse", "rm", "-rfv", "/var/lib/clickhouse/disks/backups_s3/backup/"))
	runMainIntegrationScenario(t, "EMBEDDED_S3", "config-s3-embedded.yml")
	//@TODO uncomment when resolve slow azure BACKUP/RESTORE https://github.com/ClickHouse/ClickHouse/issues/52088
	//r.NoError(dockerExec("clickhouse", "rm", "-rf", "/var/lib/clickhouse/disks/backups_azure/backup/"))
	//runMainIntegrationScenario(t, "EMBEDDED_AZURE", "config-azblob-embedded.yml")
	//@TODO think about how to implements embedded backup for s3_plain disks
	//r.NoError(dockerExec("clickhouse", "rm", "-rf", "/var/lib/clickhouse/disks/backups_s3_plain/backup/"))
	//runMainIntegrationScenario(t, "EMBEDDED_S3_PLAIN", "config-s3-plain-embedded.yml")
}

func TestRestoreDatabaseMapping(t *testing.T) {
	//t.Parallel()
	r := require.New(t)
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
	fullCleanup(t, r, ch, []string{testBackupName}, []string{"local"}, databaseList, false, false, "config-database-mapping.yml")

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

	log.Info("Create backup")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "create", testBackupName))

	log.Info("Restore schema")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--schema", "--rm", "--restore-database-mapping", "database1:database2", "--tables", "database1.*", testBackupName))

	log.Info("Check result database1")
	ch.queryWithNoError(r, "INSERT INTO database1.t1 SELECT '2023-01-01 00:00:00', number FROM numbers(10)")
	checkRecordset(1, 20, "SELECT count() FROM database1.t1")
	checkRecordset(1, 20, "SELECT count() FROM database1.d1")
	checkRecordset(1, 20, "SELECT count() FROM database1.mv1")
	checkRecordset(1, 20, "SELECT count() FROM database1.v1")

	log.Info("Drop database1")
	r.NoError(ch.dropDatabase("database1"))

	log.Info("Restore data")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--data", "--restore-database-mapping", "database1:database2", "--tables", "database1.*", testBackupName))

	log.Info("Check result database2")
	checkRecordset(1, 10, "SELECT count() FROM database2.t1")
	checkRecordset(1, 10, "SELECT count() FROM database2.d1")
	checkRecordset(1, 10, "SELECT count() FROM database2.mv1")
	checkRecordset(1, 10, "SELECT count() FROM database2.v1")

	log.Info("Check database1 not exists")
	checkRecordset(1, 0, "SELECT count() FROM system.databases WHERE name='database1'")

	fullCleanup(t, r, ch, []string{testBackupName}, []string{"local"}, databaseList, true, true, "config-database-mapping.yml")
}

func TestMySQLMaterialized(t *testing.T) {
	t.Skipf("Wait when fix DROP TABLE not supported by MaterializedMySQL, just attach will not help")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.12") == -1 {
		t.Skipf("MaterializedMySQL doens't support for clickhouse version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	//t.Parallel()
	r := require.New(t)
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

	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "test_mysql_materialized"))
	ch.queryWithNoError(r, "DROP DATABASE ch_mysql_repl")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "test_mysql_materialized"))

	result := 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT count() FROM ch_mysql_repl.t1"))
	r.Equal(3, result, "expect count=3")

	ch.queryWithNoError(r, "DROP DATABASE ch_mysql_repl")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_mysql_materialized"))
}

func TestPostgreSQLMaterialized(t *testing.T) {
	t.Skipf("Wait when fix https://github.com/ClickHouse/ClickHouse/issues/44250")

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.11") == -1 {
		t.Skipf("MaterializedPostgreSQL doens't support for clickhouse version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	//t.Parallel()
	r := require.New(t)
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

	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "test_pgsql_materialized"))
	ch.queryWithNoError(r, "DROP DATABASE ch_pgsql_repl")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "test_pgsql_materialized"))

	result := 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT count() FROM ch_pgsql_repl.t1"))
	r.Equal(3, result, "expect count=3")

	ch.queryWithNoError(r, "DROP DATABASE ch_pgsql_repl")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_pgsql_materialized"))
}

func uploadSSHKeys(r *require.Assertions, container string) {
	r.NoError(dockerCP("sftp/clickhouse-backup_rsa", container+":/id_rsa"))
	r.NoError(dockerExec(container, "cp", "-vf", "/id_rsa", "/tmp/id_rsa"))
	r.NoError(dockerExec(container, "chmod", "-v", "0600", "/tmp/id_rsa"))

	r.NoError(dockerCP("sftp/clickhouse-backup_rsa.pub", "sshd:/root/.ssh/authorized_keys"))
	r.NoError(dockerExec("sshd", "chown", "-v", "root:root", "/root/.ssh/authorized_keys"))
	r.NoError(dockerExec("sshd", "chmod", "-v", "0600", "/root/.ssh/authorized_keys"))
}

func runMainIntegrationScenario(t *testing.T, remoteStorageType, backupConfig string) {
	var out string
	var err error

	r := require.New(t)
	ch := &TestClickHouse{}
	ch.connectWithWait(r, 500*time.Millisecond, 1*time.Minute)
	defer ch.chbackend.Close()

	// test for specified partitions backup
	testBackupSpecifiedPartitions(t, r, ch, remoteStorageType, backupConfig)

	// main test scenario
	testBackupName := fmt.Sprintf("%s_full_%d", t.Name(), rand.Int())
	incrementBackupName := fmt.Sprintf("%s_increment_%d", t.Name(), rand.Int())
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameMySQL, dbNamePostgreSQL, Issue331Atomic, Issue331Ordinary}
	tablesPattern := fmt.Sprintf("*_%s.*", t.Name())
	log.Info("Clean before start")
	fullCleanup(t, r, ch, []string{testBackupName, incrementBackupName}, []string{"remote", "local"}, databaseList, false, false, backupConfig)

	r.NoError(dockerExec("minio", "mc", "ls", "local/clickhouse/disk_s3"))
	testData := generateTestData(t, r, ch, remoteStorageType, defaultTestData)

	r.NoError(dockerExec("minio", "mc", "ls", "local/clickhouse/disk_s3"))
	log.Info("Create backup")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--tables", tablesPattern, testBackupName))
	generateIncrementTestData(t, ch, r, defaultIncrementData)

	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--tables", tablesPattern, incrementBackupName))

	log.Info("Upload")
	uploadCmd := fmt.Sprintf("%s_COMPRESSION_FORMAT=zstd CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/%s clickhouse-backup upload --resume %s", remoteStorageType, backupConfig, testBackupName)
	checkResumeAlreadyProcessed(uploadCmd, testBackupName, "upload", r, remoteStorageType)

	//diffFrom := []string{"--diff-from", "--diff-from-remote"}[rand.Intn(2)]
	diffFrom := "--diff-from-remote"
	uploadCmd = fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s upload %s %s %s --resume", backupConfig, incrementBackupName, diffFrom, testBackupName)
	checkResumeAlreadyProcessed(uploadCmd, incrementBackupName, "upload", r, remoteStorageType)

	backupDir := "/var/lib/clickhouse/backup"
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		backupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED"))
	}
	out, err = dockerExecOut("clickhouse-backup", "bash", "-ce", "ls -lha "+backupDir+" | grep "+t.Name())
	r.NoError(err)
	r.Equal(2, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect '2' backups exists in backup directory")
	log.Info("Delete backup")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", testBackupName))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", incrementBackupName))
	out, err = dockerExecOut("clickhouse-backup", "bash", "-ce", "ls -lha "+backupDir+" | grep "+t.Name())
	r.NotNil(err)
	r.Equal("", strings.Trim(out, " \t\r\n"), "expect '0' backup exists in backup directory")

	dropDatabasesFromTestDataDataSet(t, r, ch, databaseList)

	log.Info("Download")
	downloadCmd := fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s download --resume %s", backupConfig, testBackupName)
	checkResumeAlreadyProcessed(downloadCmd, testBackupName, "download", r, remoteStorageType)

	log.Info("Restore schema")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--schema", testBackupName))

	log.Info("Restore data")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--data", testBackupName))

	log.Info("Full restore with rm")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--rm", testBackupName))

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
	dropDatabasesFromTestDataDataSet(t, r, ch, databaseList)

	log.Info("Delete backup")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", testBackupName))

	log.Info("Download increment")
	downloadCmd = fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s download --resume %s", backupConfig, incrementBackupName)
	checkResumeAlreadyProcessed(downloadCmd, incrementBackupName, "download", r, remoteStorageType)

	log.Info("Restore")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--schema", "--data", incrementBackupName))

	log.Info("Check increment data")
	for i := range testData {
		testDataItem := testData[i]
		if isTableSkip(ch, testDataItem, true) || testDataItem.IsDictionary {
			continue
		}
		for _, incrementDataItem := range defaultIncrementData {
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
	// CUSTOM and EMBEDDED download increment doesn't download full
	if remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		fullCleanup(t, r, ch, []string{incrementBackupName}, []string{"local"}, nil, true, false, backupConfig)
		fullCleanup(t, r, ch, []string{testBackupName, incrementBackupName}, []string{"remote"}, databaseList, true, true, backupConfig)
	} else {
		fullCleanup(t, r, ch, []string{testBackupName, incrementBackupName}, []string{"remote", "local"}, databaseList, true, true, backupConfig)
	}
}

func testBackupSpecifiedPartitions(t *testing.T, r *require.Assertions, ch *TestClickHouse, remoteStorageType string, backupConfig string) {
	log.Info("testBackupSpecifiedPartitions started")
	var err error
	var out string
	var result, expectedCount uint64

	partitionBackupName := fmt.Sprintf("partition_backup_%d", rand.Int())
	fullBackupName := fmt.Sprintf("full_backup_%d", rand.Int())
	dbName := "test_partitions_" + t.Name()
	// Create and fill tables
	ch.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS "+dbName+".t1")
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS "+dbName+".t2")
	ch.queryWithNoError(r, "CREATE TABLE "+dbName+".t1 (dt Date, v UInt64) ENGINE=MergeTree() PARTITION BY toYYYYMMDD(dt) ORDER BY dt")
	ch.queryWithNoError(r, "CREATE TABLE "+dbName+".t2 (dt String, v UInt64) ENGINE=MergeTree() PARTITION BY dt ORDER BY dt")
	for _, dt := range []string{"2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04"} {
		ch.queryWithNoError(r, fmt.Sprintf("INSERT INTO "+dbName+".t1 SELECT '%s', number FROM numbers(10)", dt))
		ch.queryWithNoError(r, fmt.Sprintf("INSERT INTO "+dbName+".t2 SELECT '%s', number FROM numbers(10)", dt))
	}

	// check create_remote full > download + partitions > delete local > download > restore --partitions > restore
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create_remote", "--tables="+dbName+".t*", fullBackupName))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "download", "--partitions=('2022-01-02'),('2022-01-03')", fullBackupName))
	fullBackupDir := "/var/lib/clickhouse/backup/" + fullBackupName + "/shadow/" + dbName + "/t?/default/"
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		fullBackupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED")) + "/" + fullBackupName + "/data/" + dbName + "/t?"
	}
	out, err = dockerExecOut("clickhouse-backup", "bash", "-c", "ls -la "+fullBackupDir+" | wc -l")
	r.NoError(err)
	expectedLines := "13"
	// custom storage doesn't support --partitions for upload / download now
	// embedded storage contain hardLink files and will download additional data parts
	if remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		expectedLines = "17"
	}
	r.Equal(expectedLines, strings.Trim(out, "\r\n\t "))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "download", fullBackupName))

	fullBackupDir = "/var/lib/clickhouse/backup/" + fullBackupName + "/shadow/" + dbName + "/t?/default/"
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		fullBackupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED")) + "/" + fullBackupName + "/data/" + dbName + "/t?"
	}
	out, err = dockerExecOut("clickhouse-backup", "bash", "-c", "ls -la "+fullBackupDir+"| wc -l")
	r.NoError(err)
	r.Equal("17", strings.Trim(out, "\r\n\t "))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--partitions=('2022-01-02'),('2022-01-03')", fullBackupName))
	result = 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT sum(c) FROM (SELECT count() AS c FROM "+dbName+".t1 UNION ALL SELECT count() AS c FROM "+dbName+".t2)"))
	expectedCount = 40
	r.Equal(expectedCount, result, fmt.Sprintf("expect count=%d", expectedCount))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", fullBackupName))
	result = 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT sum(c) FROM (SELECT count() AS c FROM "+dbName+".t1 UNION ALL SELECT count() AS c FROM "+dbName+".t2)"))
	r.Equal(uint64(80), result, "expect count=80")
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "remote", fullBackupName))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName))

	// check create + partitions
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--tables="+dbName+".t1", "--partitions=20220102,20220103", partitionBackupName))
	partitionBackupDir := "/var/lib/clickhouse/backup/" + partitionBackupName + "/shadow/" + dbName + "/t1/default/"
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		partitionBackupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED")) + "/" + partitionBackupName + "/data/" + dbName + "/t1"
	}
	out, err = dockerExecOut("clickhouse-backup", "bash", "-c", "ls -la "+partitionBackupDir+"| wc -l")
	r.NoError(err)
	r.Equal("5", strings.Trim(out, "\r\n\t "))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", partitionBackupName))

	// check create > upload + partitions
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--tables="+dbName+".t1", partitionBackupName))
	partitionBackupDir = "/var/lib/clickhouse/backup/" + partitionBackupName + "/shadow/" + dbName + "/t1/default/"
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		partitionBackupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED")) + "/" + partitionBackupName + "/data/" + dbName + "/t1"
	}
	out, err = dockerExecOut("clickhouse-backup", "bash", "-c", "ls -la "+partitionBackupDir+" | wc -l")
	r.NoError(err)
	r.Equal("7", strings.Trim(out, "\r\n\t "))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "upload", "--tables="+dbName+".t1", "--partitions=20220102,20220103", partitionBackupName))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", partitionBackupName))

	// restore partial uploaded
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore_remote", partitionBackupName))

	// Check partial restored t1
	result = 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT count() FROM "+dbName+".t1"))

	expectedCount = 20
	// custom and embedded doesn't support --partitions in upload and download
	if remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		expectedCount = 40
	}
	r.Equal(expectedCount, result, fmt.Sprintf("expect count=%d", expectedCount))

	// Check only selected partitions restored
	result = 0
	r.NoError(ch.chbackend.SelectSingleRowNoCtx(&result, "SELECT count() FROM "+dbName+".t1 WHERE dt NOT IN ('2022-01-02','2022-01-03')"))
	expectedCount = 0
	// custom and embedded doesn't support --partitions in upload and download
	if remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		expectedCount = 20
	}
	r.Equal(expectedCount, result, "expect count=0")

	// DELETE backup.
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "remote", partitionBackupName))
	r.NoError(dockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", partitionBackupName))

	if err = ch.dropDatabase(dbName); err != nil {
		t.Fatal(err)
	}
	log.Info("testBackupSpecifiedPartitions finish")
}

func checkResumeAlreadyProcessed(backupCmd, testBackupName, resumeKind string, r *require.Assertions, remoteStorageType string) {
	// backupCmd = fmt.Sprintf("%s & PID=$!; sleep 0.7; kill -9 $PID; cat /var/lib/clickhouse/backup/%s/upload.state; sleep 0.3; %s", backupCmd, testBackupName, backupCmd)
	if remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		backupCmd = strings.Replace(backupCmd, "--resume", "", 1)
	} else {
		backupCmd = fmt.Sprintf("%s; cat /var/lib/clickhouse/backup/%s/%s.state; %s", backupCmd, testBackupName, resumeKind, backupCmd)
	}
	out, err := dockerExecOut("clickhouse-backup", "bash", "-xce", backupCmd)
	log.Info(out)
	r.NoError(err)
	if strings.Contains(backupCmd, "--resume") {
		r.Contains(out, "already processed")
	}
}

func fullCleanup(t *testing.T, r *require.Assertions, ch *TestClickHouse, backupNames, backupTypes, databaseList []string, checkDeleteErr, checkDropErr bool, backupConfig string) {
	for _, backupName := range backupNames {
		for _, backupType := range backupTypes {
			err := dockerExec("clickhouse-backup", "bash", "-xce", "clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" delete "+backupType+" "+backupName)
			if checkDeleteErr {
				r.NoError(err)
			}
		}
	}
	otherBackupList, err := dockerExecOut("clickhouse", "ls", "-1", "/var/lib/clickhouse/backup/*"+t.Name()+"*")
	if err == nil {
		for _, backupName := range strings.Split(otherBackupList, "\n") {
			if backupName != "" {
				err := dockerExec("clickhouse-backup", "bash", "-xce", "clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" delete local "+backupName)
				if checkDropErr {
					r.NoError(err)
				}
			}
		}
	}

	dropDatabasesFromTestDataDataSet(t, r, ch, databaseList)
}

func generateTestData(t *testing.T, r *require.Assertions, ch *TestClickHouse, remoteStorageType string, testData []TestDataStruct) []TestDataStruct {
	log.Infof("Generate test data %s with _%s suffix", remoteStorageType, t.Name())
	testData = generateTestDataWithDifferentStoragePolicy(remoteStorageType, testData)
	for _, data := range testData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestSchema(t, data, remoteStorageType))
	}
	for _, data := range testData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestData(t, data))
	}
	return testData
}

func generateTestDataWithDifferentStoragePolicy(remoteStorageType string, testData []TestDataStruct) []TestDataStruct {
	for databaseName, databaseEngine := range map[string]string{dbNameOrdinary: "Ordinary", dbNameAtomic: "Atomic"} {
		testDataWithStoragePolicy := TestDataStruct{
			Database: databaseName, DatabaseEngine: databaseEngine,
			Rows: func() []map[string]interface{} {
				result := make([]map[string]interface{}, 100)
				for i := 0; i < 100; i++ {
					result[i] = map[string]interface{}{"id": uint64(i)}
				}
				return result
			}(),
			Fields:  []string{"id"},
			OrderBy: "id",
		}
		addTestDataIfNotExists := func() {
			found := false
			for _, data := range testData {
				if data.Name == testDataWithStoragePolicy.Name && data.Database == testDataWithStoragePolicy.Database {
					found = true
					break
				}
			}
			if !found {
				testData = append(testData, testDataWithStoragePolicy)
			}
		}
		//s3 disks support after 21.8
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 && remoteStorageType == "S3" {
			testDataWithStoragePolicy.Name = "test_s3"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 's3_only'"
			addTestDataIfNotExists()
		}
		//encrypted disks support after 21.10
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.10") >= 0 {
			testDataWithStoragePolicy.Name = "test_hdd3_encrypted"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'hdd3_only_encrypted'"
			addTestDataIfNotExists()
		}
		//encrypted s3 disks support after 21.12
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 && remoteStorageType == "S3" {
			testDataWithStoragePolicy.Name = "test_s3_encrypted"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 's3_only_encrypted'"
			addTestDataIfNotExists()
		}
		//gcs over s3 support added in 22.6
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.6") >= 0 && remoteStorageType == "GCS" && os.Getenv("QA_GCS_OVER_S3_BUCKET") != "" {
			testDataWithStoragePolicy.Name = "test_gcs"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'gcs_only'"
			addTestDataIfNotExists()
		}
		//check azure_blob_storage only in 23.3+ (added in 22.1)
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") >= 0 && remoteStorageType == "AZBLOB" {
			testDataWithStoragePolicy.Name = "test_azure"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'azure_only'"
			addTestDataIfNotExists()
		}
	}
	return testData
}

func generateIncrementTestData(t *testing.T, ch *TestClickHouse, r *require.Assertions, incrementData []TestDataStruct) {
	log.Info("Generate increment test data")
	for _, data := range incrementData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestData(t, data))
	}
}

func dropDatabasesFromTestDataDataSet(t *testing.T, r *require.Assertions, ch *TestClickHouse, databaseList []string) {
	log.Info("Drop all databases")
	for _, db := range databaseList {
		r.NoError(ch.dropDatabase(db + "_" + t.Name()))
	}
}

const apiBackupNumber = 5

type TestClickHouse struct {
	chbackend *clickhouse.ClickHouse
}

func (ch *TestClickHouse) connectWithWait(r *require.Assertions, sleepBefore, timeOut time.Duration) {
	time.Sleep(sleepBefore)
	for i := 1; i < 11; i++ {
		err := ch.connect(timeOut.String())
		if i == 10 {
			r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker", "logs", "clickhouse"))
			out, dockerErr := dockerExecOut("clickhouse", "clickhouse client", "--echo", "-q", "'SELECT version()'")
			r.NoError(dockerErr)
			ch.chbackend.Log.Debug(out)
			r.NoError(err)
		}
		if err != nil {
			r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker", "ps", "-a"))
			if out, dockerErr := dockerExecOut("clickhouse", "clickhouse client", "--echo", "-q", "SELECT version()"); dockerErr == nil {
				log.Info(out)
			} else {
				log.Warn(out)
			}
			log.Warnf("clickhouse not ready %v, wait %v seconds", err, (time.Duration(i) * timeOut).Seconds())
			time.Sleep(time.Duration(i) * timeOut)
		} else {
			if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") == 1 {
				var count uint64
				err = ch.chbackend.SelectSingleRowNoCtx(&count, "SELECT count() FROM mysql('mysql:3306','mysql','user','root','root')")
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

func (ch *TestClickHouse) connect(timeOut string) error {
	ch.chbackend = &clickhouse.ClickHouse{
		Config: &config.ClickHouseConfig{
			Host:    "127.0.0.1",
			Port:    9000,
			Timeout: timeOut,
		},
		Log: log.WithField("logger", "integration-test"),
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

var mergeTreeOldSyntax = regexp.MustCompile(`(?m)MergeTree\(([^,]+),([\w\s,)(]+),(\s*\d+\s*)\)`)

func (ch *TestClickHouse) createTestSchema(t *testing.T, data TestDataStruct, remoteStorageType string) error {
	origDatabase := data.Database
	origName := data.Name
	if !data.IsFunction {
		data.Database = data.Database + "_" + t.Name()
		data.Name = data.Name + "_" + t.Name()
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
		createSQL += fmt.Sprintf(" IF NOT EXISTS `%s_%s` ", data.Name, t.Name())
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
	// @TODO remove it when resolve https://github.com/ClickHouse/ClickHouse/issues/43971
	if strings.Contains(createSQL, "8192)") && strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		matches := mergeTreeOldSyntax.FindStringSubmatch(createSQL)
		if len(matches) >= 3 {
			substitution := "MergeTree() PARTITION BY toYYYYMMDD($1) ORDER BY $2 SETTINGS index_granularity=$3"
			createSQL = mergeTreeOldSyntax.ReplaceAllString(createSQL, substitution)
		} else {
			log.Fatalf("Wrong %s, matches=%#v", createSQL, matches)
		}
	}
	if !data.IsFunction {
		createSQL = strings.NewReplacer("`"+origDatabase+"`", "`"+data.Database+"`", "'"+origDatabase+"'", "'"+data.Database+"'").Replace(createSQL)
		createSQL = strings.NewReplacer("."+origName, "."+data.Name, "`"+origName+"`", "`"+data.Name+"`", "'"+origName+"'", "'"+data.Name+"'").Replace(createSQL)
	}
	createSQL = strings.Replace(createSQL, "{test}", t.Name(), -1)
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

func (ch *TestClickHouse) createTestData(t *testing.T, data TestDataStruct) error {
	data.Database = data.Database + "_" + t.Name()
	data.Name = data.Name + "_" + t.Name()
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
	data.Database += "_" + t.Name()
	data.Name += "_" + t.Name()
	log.Infof("Check '%d' rows in '%s.%s'\n", len(data.Rows), data.Database, data.Name)
	selectSQL := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY `%s`", data.Database, data.Name, strings.Replace(data.OrderBy, "{test}", t.Name(), -1))

	if data.IsFunction && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") == -1 {
		return nil
	}
	if data.IsFunction {
		selectSQL = fmt.Sprintf("SELECT %s(number, number+1) AS test_result FROM numbers(%d)", data.Name, len(data.Rows))
	}
	log.Debug(selectSQL)
	rows, err := ch.chbackend.GetConn().Query(context.Background(), selectSQL)
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
	data.Database += "_" + t.Name()
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

var dockerExecTimeout = 180 * time.Second

func dockerExec(container string, cmd ...string) error {
	out, err := dockerExecOut(container, cmd...)
	log.Info(out)
	return err
}

func dockerExecOut(container string, cmd ...string) (string, error) {
	dcmd := []string{"exec", container}
	dcmd = append(dcmd, cmd...)
	return utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker", dcmd...)
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
	return os.Getenv("COMPOSE_FILE") == "docker-compose.yml" && (strings.Contains(data.Name, "jbod_table") || data.IsDictionary)
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

func installDebIfNotExists(r *require.Assertions, container string, pkgs ...string) {
	r.NoError(dockerExec(
		container,
		"bash", "-xec",
		fmt.Sprintf(
			"export DEBIAN_FRONTEND=noniteractive; if [[ '%d' != $(dpkg -l | grep -c -E \"%s\" ) ]]; then rm -fv /etc/apt/sources.list.d/clickhouse.list; find /etc/apt/ -type f -name *.list -exec sed -i 's/ru.archive.ubuntu.com/archive.ubuntu.com/g' {} +; apt-get -y update; apt-get install --no-install-recommends -y %s; fi",
			len(pkgs), "^ii\\s+"+strings.Join(pkgs, "|^ii\\s+"), strings.Join(pkgs, " "),
		),
	))
}
