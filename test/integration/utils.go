//go:build integration

package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	pool "github.com/jolestar/go-commons-pool/v2"

	stdlog "log"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"golang.org/x/mod/semver"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Altinity/clickhouse-backup/v2/pkg/log_helper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
)

var projectId atomic.Uint32
var dockerPool *pool.ObjectPool

var dbNameAtomic = "_test#$.ДБ_atomic_/issue\\_1091"
var dbNameOrdinary = "_test#$.ДБ_ordinary_/issue\\_1091"
var dbNameReplicated = "_test#$.ДБ_Replicated_/issue\\_1091"
var dbNameMySQL = "mysql_db"
var dbNamePostgreSQL = "pgsql_db"
var Issue331Issue1091Atomic = "_issue331._atomic_/issue\\_1091"
var Issue331Issue1091Ordinary = "_issue331.ordinary_/issue\\_1091"

// setup log level
func init() {
	// old version replace \ to nothing,  https://github.com/Altinity/clickhouse-backup/issue/1091
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.5") < 0 {
		dbNameAtomic = "_test#$.ДБ_atomic_/issue_1091"
		dbNameOrdinary = "_test#$.ДБ_ordinary_/issue_1091"
		dbNameMySQL = "mysql_db"
		dbNamePostgreSQL = "pgsql_db"
		Issue331Issue1091Atomic = "_issue331._atomic_/issue_1091"
		Issue331Issue1091Ordinary = "_issue331.ordinary_/issue_1091"
	}
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, NoColor: true, TimeFormat: "2006-01-02 15:04:05.000"}
	log.Logger = zerolog.New(zerolog.SyncWriter(consoleWriter)).With().Timestamp().Logger()
	stdlog.SetOutput(log.Logger)
	logLevel := "info"
	if os.Getenv("LOG_LEVEL") != "" && os.Getenv("LOG_LEVEL") != "info" {
		logLevel = os.Getenv("LOG_LEVEL")
	}
	if os.Getenv("TEST_LOG_LEVEL") != "" && os.Getenv("TEST_LOG_LEVEL") != "info" {
		logLevel = os.Getenv("TEST_LOG_LEVEL")
	}
	log_helper.SetLogLevelFromString(logLevel)

	runParallel, isExists := os.LookupEnv("RUN_PARALLEL")
	if !isExists {
		runParallel = "1"
	}
	runParallelInt, err := strconv.Atoi(runParallel)
	if err != nil {
		log.Fatal().Stack().Msgf("invalid RUN_PARALLEL environment variable value %s", runParallel)
	}
	ctx := context.Background()
	factory := pool.NewPooledObjectFactorySimple(func(context.Context) (interface{}, error) {
		id := projectId.Add(1)
		env := TestEnvironment{
			ProjectName: fmt.Sprintf("project%d", id%uint32(runParallelInt)),
		}
		return &env, nil
	})
	dockerPool = pool.NewObjectPoolWithDefaultConfig(ctx, factory)
	dockerPool.Config.MaxTotal = runParallelInt
}

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

type TestEnvironment struct {
	ch          *clickhouse.ClickHouse
	ProjectName string
}

var defaultTestData = []TestDataStruct{
	{
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		// .inner. shall resolve in https://github.com/ClickHouse/ClickHouse/issues/67669
		Name:   ".inner_table1",
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
		Name:   "-table-$-",
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
		Database: Issue331Issue1091Atomic, DatabaseEngine: "Atomic",
		Name:   Issue331Issue1091Atomic, // need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		Schema: fmt.Sprintf("(`%s` UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) ENGINE = MergeTree PARTITION BY `%s` ORDER BY (`%s`, Col1, Col2, Col3, Col4, Col5) SETTINGS index_granularity = 8192", Issue331Issue1091Atomic, Issue331Issue1091Atomic, Issue331Issue1091Atomic),
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{Issue331Issue1091Atomic: uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{Issue331Issue1091Atomic, "Col1", "Col2", "Col3", "Col4", "Col5"},
		OrderBy: Issue331Issue1091Atomic + "_{test}",
	}, {
		Database: Issue331Issue1091Ordinary, DatabaseEngine: "Ordinary",
		Name:   Issue331Issue1091Ordinary, // need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		Schema: fmt.Sprintf("(`%s` String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMM(order_time) ORDER BY (order_time, `%s`)", Issue331Issue1091Ordinary, Issue331Issue1091Ordinary),
		Rows: []map[string]interface{}{
			{Issue331Issue1091Ordinary: "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{Issue331Issue1091Ordinary: "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{Issue331Issue1091Ordinary, "order_time", "amount"},
		OrderBy: Issue331Issue1091Ordinary + "_{test}",
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
		Name:   "jbod#$_table",
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
		Name:   "jbod#$_table",
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
		Database: dbNameAtomic, DatabaseEngine: "Atomic",
		Name:   "replicated_empty_engine",
		Schema: "(id UInt64) Engine=ReplicatedMergeTree() ORDER BY id",
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
	// https://github.com/Altinity/clickhouse-backup/issues/1199
	{
		Database:       dbNameAtomic,
		DatabaseEngine: "Atomic",
		IsView:         true,
		Name:           "test_view_from_view",
		Schema:         fmt.Sprintf(" AS SELECT count() AS cnt FROM `%s`.`test_view_{test}`", dbNameAtomic),
		SkipInsert:     true,
		Rows: func() []map[string]interface{} {
			return []map[string]interface{}{
				{"cnt": uint64(1)},
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
		Name:               "mv_min_with_nested_dependency",
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
			Issue331Issue1091Atomic, Issue331Issue1091Atomic, Issue331Issue1091Atomic, Issue331Issue1091Atomic), // same table and name need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		SkipInsert: true,
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{Issue331Issue1091Atomic: uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{},
		OrderBy: Issue331Issue1091Atomic + "_{test}",
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
		// .inner. shall resolve in https://github.com/ClickHouse/ClickHouse/issues/67669
		Name:   ".inner_table1",
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
		Name:   "-table-$-",
		Schema: "(TimeStamp DateTime, Item String, Date Date MATERIALIZED toDate(TimeStamp)) ENGINE = MergeTree() PARTITION BY Date ORDER BY TimeStamp SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"TimeStamp": toTS("2019-01-26 07:37:18"), "Item": "Seven"},
			{"TimeStamp": toTS("2019-01-27 07:37:19"), "Item": "Eight"},
		},
		Fields:  []string{"TimeStamp", "Item"},
		OrderBy: "TimeStamp",
	}, {
		Database: Issue331Issue1091Atomic, DatabaseEngine: "Atomic",
		Name:   Issue331Issue1091Atomic, // need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		Schema: fmt.Sprintf("(`%s` UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) ENGINE = MergeTree PARTITION BY `%s` ORDER BY (`%s`, Col1, Col2, Col3, Col4, Col5) SETTINGS index_granularity = 8192", Issue331Issue1091Atomic, Issue331Issue1091Atomic, Issue331Issue1091Atomic),
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 200; i < 220; i++ {
				result = append(result, map[string]interface{}{Issue331Issue1091Atomic: uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{Issue331Issue1091Atomic, "Col1", "Col2", "Col3", "Col4", "Col5"},
		OrderBy: Issue331Issue1091Atomic + "_{test}",
	}, {
		Database: Issue331Issue1091Ordinary, DatabaseEngine: "Ordinary",
		Name:   Issue331Issue1091Ordinary, // need cover fix https://github.com/Altinity/clickhouse-backup/issues/331
		Schema: fmt.Sprintf("(`%s` String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMM(order_time) ORDER BY (order_time, `%s`)", Issue331Issue1091Ordinary, Issue331Issue1091Ordinary),
		Rows: []map[string]interface{}{
			{Issue331Issue1091Ordinary: "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{Issue331Issue1091Ordinary: "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{Issue331Issue1091Ordinary, "order_time", "amount"},
		OrderBy: Issue331Issue1091Ordinary + "_{test}",
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
		Name:   "jbod#$_table",
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

var listTimeMsRE = regexp.MustCompile(`list_duration=(\d+.\d+)`)
var dockerExecTimeout = 900 * time.Second
var mergeTreeOldSyntax = regexp.MustCompile(`(?m)MergeTree\(([^,]+),([\w\s,)(]+),(\s*\d+\s*)\)`)

func NewTestEnvironment(t *testing.T) (*TestEnvironment, *require.Assertions) {
	isParallel := os.Getenv("RUN_PARALLEL") != "1"
	if os.Getenv("COMPOSE_FILE") == "" || os.Getenv("CUR_DIR") == "" {
		t.Fatal("please setup COMPOSE_FILE and CUR_DIR environment variables")
	}
	t.Helper()
	if isParallel {
		t.Parallel()
	}

	r := require.New(t)
	envObj, err := dockerPool.BorrowObject(t.Context())
	if err != nil {
		t.Fatalf("dockerPool.BorrowObject retrun error: %v", err)
	}
	env := envObj.(*TestEnvironment)

	if isParallel {
		t.Logf("%s run in parallel mode project=%s", t.Name(), env.ProjectName)
	} else {
		t.Logf("%s run in sequence mode project=%s", t.Name(), env.ProjectName)
	}

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "1.1.54394") <= 0 {
		r := require.New(&testing.T{})
		env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "curl")
		env.DockerExecNoError(r, "clickhouse-backup", "update-ca-certificates")
	}
	return env, r
}

func (env *TestEnvironment) Cleanup(t *testing.T, r *require.Assertions) {
	env.ch.Close()

	if t.Name() == "TestS3" || t.Name() == "TestEmbeddedS3" || t.Name() == "TestSkipDisk" || t.Name() == "TestRestoreMapping" {
		env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/disk_s3")
	}

	if t.Name() == "TestRBAC" || t.Name() == "TestConfigs" || strings.HasPrefix(t.Name(), "TestEmbedded") {
		env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/backups_s3")
	}
	if t.Name() == "TestCustomRsync" {
		env.DockerExecNoError(r, "sshd", "rm", "-rf", "/root/rsync_backups")
	}
	if t.Name() == "TestCustomRestic" {
		env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/restic")
	}
	if t.Name() == "TestCustomKopia" {
		env.DockerExecNoError(r, "minio", "rm", "-rf", "/minio/data/clickhouse/kopia")
	}

	if err := dockerPool.ReturnObject(t.Context(), env); err != nil {
		t.Fatalf("dockerPool.ReturnObject error: %+v", err)
	}

}

// Docker execution methods

func (env *TestEnvironment) GetDefaultComposeCommand() []string {
	return []string{"compose", "-f", path.Join(os.Getenv("CUR_DIR"), os.Getenv("COMPOSE_FILE")), "--progress", "plain", "--project-name", env.ProjectName}
}

func (env *TestEnvironment) GetExecDockerCommand(container string) []string {
	return []string{"exec", fmt.Sprintf("%s-%s-1", env.ProjectName, container)}
}

func (env *TestEnvironment) DockerExecNoError(r *require.Assertions, container string, cmd ...string) {
	out, err := env.DockerExecOut(container, cmd...)
	if err == nil {
		log.Debug().Msg(out)
	}
	r.NoError(err, "%s\n\n%s\n[ERROR]\n%v", strings.Join(append(env.GetExecDockerCommand(container), cmd...), " "), out, err)
}

func (env *TestEnvironment) DockerExec(container string, cmd ...string) error {
	out, err := env.DockerExecOut(container, cmd...)
	log.Debug().Msg(out)
	return err
}

func (env *TestEnvironment) DockerExecOut(container string, cmd ...string) (string, error) {
	dcmd := append(env.GetExecDockerCommand(container), cmd...)
	return utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker", dcmd...)
}

func (env *TestEnvironment) DockerExecBackgroundNoError(r *require.Assertions, container string, cmd ...string) {
	out, err := env.DockerExecBackgroundOut(container, cmd...)
	r.NoError(err, "%s\n\n%s\n[ERROR]\n%v", strings.Join(append(append(env.GetDefaultComposeCommand(), "exec", "-d", container), cmd...), " "), out, err)
}

func (env *TestEnvironment) DockerExecBackground(container string, cmd ...string) error {
	out, err := env.DockerExecBackgroundOut(container, cmd...)
	log.Debug().Msg(out)
	return err
}

func (env *TestEnvironment) DockerExecBackgroundOut(container string, cmd ...string) (string, error) {
	dcmd := append(env.GetDefaultComposeCommand(), "exec", "-d", container)
	dcmd = append(dcmd, cmd...)
	return utils.ExecCmdOut(context.Background(), dockerExecTimeout, "docker", dcmd...)
}

func (env *TestEnvironment) DockerCP(src, dst string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	dcmd := append(env.GetDefaultComposeCommand(), "cp", src, dst)

	log.Debug().Msgf("docker %s", strings.Join(dcmd, " "))
	out, err := exec.CommandContext(ctx, "docker", dcmd...).CombinedOutput()
	log.Debug().Msg(string(out))
	cancel()
	return err
}

func (env *TestEnvironment) InstallDebIfNotExists(r *require.Assertions, container string, pkgs ...string) {
	out, err := env.DockerExecOut(
		container,
		"bash", "-xec",
		fmt.Sprintf(
			"export DEBIAN_FRONTEND=noniteractive; if [[ '%d' != $(dpkg -l | grep -c -E \"%s\" ) ]]; then rm -fv /etc/apt/sources.list.d/clickhouse.list; find /etc/apt/ -type f -name *.list -exec sed -i 's/ru.archive.ubuntu.com/archive.ubuntu.com/g' {} +; apt-get -y update; apt-get install --no-install-recommends -y %s; fi",
			len(pkgs), "^ii\\s+"+strings.Join(pkgs, "|^ii\\s+"), strings.Join(pkgs, " "),
		),
	)
	r.NoError(err, out)
}

// Connection methods

func (env *TestEnvironment) connectWithWait(t *testing.T, r *require.Assertions, sleepBefore, pollInterval, timeOut time.Duration) {
	time.Sleep(sleepBefore)
	maxTry := 100
	for i := 1; i <= maxTry; i++ {
		err := env.connect(t, timeOut.String())
		if i == maxTry {
			r.NoError(utils.ExecCmd(t.Context(), 180*time.Second, "docker", append(env.GetDefaultComposeCommand(), "ps", "clickhouse")...))
			out, dockerErr := env.DockerExecOut("clickhouse", "clickhouse", "client", "--echo", "-q", "'SELECT version()'")
			log.Info().Msg(out)
			r.NoError(dockerErr)
			r.NoError(err)
		}
		if err != nil {
			if out, dockerErr := env.DockerExecOut("clickhouse", "clickhouse", "client", "--echo", "-q", "SELECT version()"); dockerErr == nil {
				log.Debug().Msg(out)
			} else {
				log.Info().Msg(out)
			}
			log.Warn().Msgf("%s clickhouse not ready %v, wait %v seconds", env.ProjectName, err, (pollInterval).Seconds())
			time.Sleep(pollInterval)
		} else {
			if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") > 0 {
				var count uint64
				err = env.ch.SelectSingleRowNoCtx(&count, "SELECT count() FROM mysql('mysql:3306','mysql','user','root','root')")
				if err == nil {
					break
				} else {
					log.Warn().Msgf("%s mysql not ready %v, wait %d seconds", env.ProjectName, err, i)
					time.Sleep(time.Second * time.Duration(i))
				}
			} else {
				break
			}
		}
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.4") >= 0 {
		r.NoError(env.ch.QueryContext(t.Context(), "SET show_table_uuid_in_table_create_query_if_not_nil=1"))
	}
}

func (env *TestEnvironment) connect(t *testing.T, timeOut string) error {
	for i := 0; i < 10; i++ {
		statusOut, statusErr := utils.ExecCmdOut(t.Context(), 10*time.Second, "docker", append(env.GetDefaultComposeCommand(), "ps", "--status", "running", "clickhouse")...)
		if statusErr == nil {
			break
		}
		log.Warn().Msg(statusOut)
		level := zerolog.WarnLevel
		if i == 9 {
			level = zerolog.FatalLevel
		}
		log.WithLevel(level).Msgf("can't ps --status running clickhouse: %v", statusErr)
		time.Sleep(1 * time.Second)
	}
	env.ch = clickhouse.NewClickHouse(&config.ClickHouseConfig{})
	portMaxTry := 10
	for i := 1; i <= portMaxTry; i++ {
		portOut, portErr := utils.ExecCmdOut(t.Context(), 10*time.Second, "docker", append(env.GetDefaultComposeCommand(), "port", "clickhouse", "9000")...)
		if portErr != nil {
			log.Error().Msg(portOut)
			if i == portMaxTry {
				log.Fatal().Msgf("%s: %s can't get port for clickhouse: %v", t.Name(), env.ProjectName, portErr)
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}
		hostAndPort := strings.Split(strings.Trim(portOut, " \r\n\t"), ":")
		if len(hostAndPort) < 1 {
			log.Fatal().Msgf("%s: %s invalid port for clickhouse: %v", t.Name(), env.ProjectName, portOut)
		}
		port, err := strconv.Atoi(hostAndPort[1])
		if err != nil {
			return err
		}
		env.ch.Config.Host = hostAndPort[0]
		env.ch.Config.Port = uint(port)
		env.ch.Config.Timeout = timeOut
		env.ch.Config.MaxConnections = 1
		env.ch.BreakConnectOnError = true
		err = env.ch.Connect()
		if err == nil {
			return nil
		}

		if i == portMaxTry {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

// Query and data methods

func (env *TestEnvironment) queryWithNoError(r *require.Assertions, query string, args ...interface{}) {
	err := env.ch.Query(query, args...)
	if err != nil {
		log.Error().Err(err).Msgf("queryWithNoError(%s) error", query)
	}
	r.NoError(err)
}

func (env *TestEnvironment) checkCount(r *require.Assertions, expectedRows int, expectedCount uint64, query string) {
	result := make([]struct {
		Count uint64 `ch:"count()"`
	}, 0)
	r.NoError(env.ch.Select(&result, query))
	r.Equal(expectedRows, len(result), "expect %d row", expectedRows)
	r.Equal(expectedCount, result[0].Count, "expect count=%d", expectedCount)
}

func (env *TestEnvironment) checkData(t *testing.T, r *require.Assertions, data TestDataStruct) error {
	assert.NotNil(t, data.Rows)
	data.Database += "_" + t.Name()
	data.Name += "_" + t.Name()
	log.Debug().Msgf("Check '%d' rows in '%s.%s'\n", len(data.Rows), data.Database, data.Name)
	selectSQL := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY `%s`", data.Database, data.Name, strings.Replace(data.OrderBy, "{test}", t.Name(), -1))

	if data.IsFunction && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") == -1 {
		return nil
	}
	if data.IsFunction {
		selectSQL = fmt.Sprintf("SELECT %s(number, number+1) AS test_result FROM numbers(%d)", data.Name, len(data.Rows))
	}
	log.Debug().Msg(selectSQL)
	rows, err := env.ch.GetConn().Query(t.Context(), selectSQL)
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
		r.ElementsMatch(vars, expectedVars, "different data in `%s`.`%s`", data.Database, data.Name)
		rowsNumber += 1
	}

	r.Equal(len(data.Rows), rowsNumber, "Unexpected rows length in `%s`.`%s`", data.Database, data.Name)

	return nil
}

func (env *TestEnvironment) checkDatabaseEngine(t *testing.T, data TestDataStruct) error {
	data.Database += "_" + t.Name()
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") <= 0 {
		return nil
	}
	selectSQL := fmt.Sprintf("SELECT engine FROM system.databases WHERE name='%s'", data.Database)
	var engine string
	if err := env.ch.SelectSingleRowNoCtx(&engine, selectSQL); err != nil {
		return err
	}
	assert.True(
		t, strings.HasPrefix(data.DatabaseEngine, engine),
		fmt.Sprintf("expect '%s' have prefix '%s'", data.DatabaseEngine, engine),
	)
	return nil
}

func (env *TestEnvironment) createTestSchema(t *testing.T, data TestDataStruct, remoteStorageType string) error {
	origDatabase := data.Database
	origName := data.Name
	if !data.IsFunction {
		data.Database = data.Database + "_" + t.Name()
		data.Name = data.Name + "_" + t.Name()
		// 20.8 doesn't respect DROP TABLE ... NO DELAY, so Atomic works but --rm is not applicable
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") > 0 {
			if err := env.ch.CreateDatabaseWithEngine(data.Database, data.DatabaseEngine, "cluster", convertVersionToInt(os.Getenv("CLICKHOUSE_VERSION"))); err != nil {
				return err
			}
		} else {
			if err := env.ch.CreateDatabase(data.Database, "cluster"); err != nil {
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

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.0") > 0 && !data.IsFunction && !strings.HasPrefix(data.DatabaseEngine, "Replicated") {
		createSQL += " ON CLUSTER 'cluster' "
	}
	createSQL += data.Schema
	// 20.8 can't create Ordinary with empty ReplicatedMergeTree() and can't create Atomic, cause doesn't respect `DROP ... NO DELAY`
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") <= 0 && strings.Contains(createSQL, "ReplicatedMergeTree()") {
		createSQL = strings.Replace(createSQL, "ReplicatedMergeTree()", "ReplicatedMergeTree('/clickhouse/tables/{database}/{table}/{shard}','{replica}')", -1)
	}
	// old 1.x clickhouse versions doesn't contains {table} and {database} macros
	if strings.Contains(createSQL, "{table}") || strings.Contains(createSQL, "{database}") {
		var isMacrosExists uint64
		if err := env.ch.SelectSingleRowNoCtx(&isMacrosExists, "SELECT count() FROM system.functions WHERE name='getMacro'"); err != nil {
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
			log.Fatal().Stack().Msgf("Wrong %s, matches=%#v", createSQL, matches)
		}
	}
	if !data.IsFunction {
		createSQL = strings.NewReplacer("`"+origDatabase+"`", "`"+data.Database+"`", "'"+origDatabase+"'", "'"+data.Database+"'").Replace(createSQL)
		createSQL = strings.NewReplacer("."+origName, "."+data.Name, "`"+origName+"`", "`"+data.Name+"`", "'"+origName+"'", "'"+data.Name+"'").Replace(createSQL)
	}
	createSQL = strings.Replace(createSQL, "{test}", t.Name(), -1)
	err := env.ch.CreateTable(
		clickhouse.Table{
			Database: data.Database,
			Name:     data.Name,
		},
		createSQL,
		false, false, "", 0, "/var/lib/clickhouse", false, data.DatabaseEngine,
	)
	return err
}

func (env *TestEnvironment) createTestData(t *testing.T, data TestDataStruct) error {
	data.Database = data.Database + "_" + t.Name()
	data.Name = data.Name + "_" + t.Name()
	if data.SkipInsert || data.CheckDatabaseOnly {
		return nil
	}
	insertSQL := fmt.Sprintf("INSERT INTO `%s`.`%s`", data.Database, data.Name)
	log.Debug().Msg(insertSQL)
	batch, err := env.ch.GetConn().PrepareBatch(t.Context(), insertSQL)

	if err != nil {
		return fmt.Errorf("createTestData PrepareBatch(%s) error: %v", insertSQL, err)
	}

	for _, row := range data.Rows {
		insertData := make([]interface{}, len(data.Fields))
		log.Debug().Msgf("VALUES %v", row)
		for idx, field := range data.Fields {
			insertData[idx] = row[field]
		}
		if err = batch.Append(insertData...); err != nil {
			return fmt.Errorf("createTestData batch.Append(%#v) error: %v", insertData, err)
		}
	}
	err = batch.Send()
	if err != nil {
		return fmt.Errorf("createTestData batch.Send(%s) error: %v", insertSQL, err)
	}
	return err
}

func (env *TestEnvironment) dropDatabase(database string, ifExists bool) (err error) {
	var isAtomicOrReplicated bool
	dropDatabaseSQL := "DROP DATABASE "
	if ifExists {
		dropDatabaseSQL += "IF EXISTS "
	}
	dropDatabaseSQL += fmt.Sprintf("`%s`", database)
	if isAtomicOrReplicated, err = env.ch.IsDbAtomicOrReplicated(database); err != nil {
		return err
	} else if isAtomicOrReplicated {
		dropDatabaseSQL += " SYNC"
	}
	return env.ch.Query(dropDatabaseSQL)
}

// Validation methods

func (env *TestEnvironment) checkObjectStorageIsEmpty(t *testing.T, r *require.Assertions, remoteStorageType string) {
	if remoteStorageType == "AZBLOB" || remoteStorageType == "AZBLOB_EMBEDDED_URL" {
		t.Log("wait when resolve https://github.com/Azure/Azurite/issues/2362, todo try to use mysql as azurite storage")
	}
	checkRemoteDir := func(expected string, container string, cmd ...string) {
		out, err := env.DockerExecOut(container, cmd...)
		r.NoError(err, "%s\nunexpected checkRemoteDir error: %v", out, err)
		r.Equal(expected, strings.Trim(out, "\r\n\t "))
	}
	if remoteStorageType == "S3" || remoteStorageType == "S3_EMBEDDED_URL" {
		checkRemoteDir("total 0", "minio", "bash", "-c", "ls -lh /minio/data/clickhouse/")
	}
	if remoteStorageType == "SFTP" {
		checkRemoteDir("total 0", "sshd", "bash", "-c", "ls -lh /root/")
	}
	if remoteStorageType == "FTP" {
		if strings.Contains(os.Getenv("COMPOSE_FILE"), "advanced") {
			checkRemoteDir("total 0", "ftp", "bash", "-c", "ls -lh /home/ftpusers/test_backup/backup/")
		} else {
			checkRemoteDir("total 0", "ftp", "sh", "-c", "ls -lh /home/test_backup/backup/")
		}
	}
	if remoteStorageType == "GCS_EMULATOR" {
		checkRemoteDir("total 0", "gcs", "sh", "-c", "ls -lh /data/altinity-qa-test/")
	}
}

func (env *TestEnvironment) checkResumeAlreadyProcessed(backupCmd, testBackupName, resumeKind string, r *require.Assertions, remoteStorageType string) {
	if remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED") || (compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") < 0 && (strings.Contains(backupCmd, "create") || strings.Contains(backupCmd, "restore"))) {
		backupCmd = strings.Replace(backupCmd, "--resume", "", 1)
	} else {
		backupCmd = fmt.Sprintf("%s; ls -la /var/lib/clickhouse/backup/%s/%s.state2; %s", backupCmd, testBackupName, resumeKind, backupCmd)
	}
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-xce", backupCmd)
	r.NoError(err, "%s\nunexpected checkResumeAlreadyProcessed error: %v", out, err)
	const alreadyProcesses = "already processed"
	const resumableWarning = "resumable state: can't"
	const resumableCleanup = "state2 cleanup begin"
	if strings.Contains(backupCmd, "--resume") {
		if strings.Contains(backupCmd, "restore") && !strings.Contains(backupCmd, "--data") {
			r.NotContains(out, resumableWarning)
			r.NotContains(out, alreadyProcesses)
			r.Contains(out, resumableCleanup)
			return
		}
		if !strings.Contains(out, alreadyProcesses) || strings.Contains(out, resumableWarning) || strings.Contains(out, resumableCleanup) {
			log.Debug().Msg(out)
		}
		r.NotContains(out, resumableWarning)
		r.NotContains(out, resumableCleanup)
		r.Contains(out, alreadyProcesses)
	} else {
		log.Debug().Msg(out)
	}
}

// SSH methods

func (env *TestEnvironment) uploadSSHKeys(r *require.Assertions, container string) {
	r.NoError(env.DockerCP("sftp/clickhouse-backup_rsa", container+":/id_rsa"))
	env.DockerExecNoError(r, container, "cp", "-vf", "/id_rsa", "/tmp/id_rsa")
	env.DockerExecNoError(r, container, "chmod", "-v", "0600", "/tmp/id_rsa")

	r.NoError(env.DockerCP("sftp/clickhouse-backup_rsa.pub", "sshd:/authorized_keys"))
	env.DockerExecNoError(r, "sshd", "cp", "-vf", "/authorized_keys", "/etc/authorized_keys/root")
	env.DockerExecNoError(r, "sshd", "chown", "-v", "root:root", "/etc/authorized_keys/root")
	env.DockerExecNoError(r, "sshd", "chmod", "-v", "0600", "/etc/authorized_keys/root")
}

// Utility functions

func toDate(s string) time.Time {
	result, _ := time.Parse("2006-01-02", s)
	return result
}

func toTS(s string) time.Time {
	result, _ := time.Parse("2006-01-02 15:04:05", s)
	return result
}

func isTableSkip(ch *TestEnvironment, data TestDataStruct, dataExists bool) bool {
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
		_ = ch.ch.Select(&dictEngines, dictSQL)
		return len(dictEngines) == 0
	}
	isSkipDictionaryOrJBOD := os.Getenv("COMPOSE_FILE") == "docker-compose.yml" && (strings.Contains(data.Name, "jbod#$_table") || data.IsDictionary)
	isSkipEmptyReplicatedMergeTree := compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.9") < 0 && strings.Contains(data.Schema, "ReplicatedMergeTree()")
	return isSkipDictionaryOrJBOD || isSkipEmptyReplicatedMergeTree
}

func convertVersionToInt(v string) int {
	vParts := strings.Split(v, ".")
	vIntStr := vParts[0]
	for _, vPart := range vParts[1:] {
		vIntStr += fmt.Sprintf("%06s", vPart)
	}
	vInt, _ := strconv.Atoi(vIntStr)
	return vInt
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

// Test data generation functions

func generateTestData(t *testing.T, r *require.Assertions, env *TestEnvironment, remoteStorageType string, createAllTypesOfObjectTables bool, testData []TestDataStruct) []TestDataStruct {
	log.Debug().Msgf("Generate test data %s with _%s suffix", remoteStorageType, t.Name())
	testData = generateTestDataForDifferentServerVersion(remoteStorageType, 0, 5, testData)
	testData = generateTestDataForDifferentStoragePolicy(remoteStorageType, createAllTypesOfObjectTables, 0, 5, testData)
	for _, data := range testData {
		if isTableSkip(env, data, false) {
			continue
		}
		r.NoError(env.createTestSchema(t, data, remoteStorageType))
	}
	for _, data := range testData {
		if isTableSkip(env, data, false) {
			continue
		}
		r.NoError(env.createTestData(t, data))
	}
	return testData
}

func addTestDataIfNotExistsAndReplaceRowsIfExists(testData []TestDataStruct, newTestData TestDataStruct) []TestDataStruct {
	found := false
	for i, data := range testData {
		if data.Name == newTestData.Name && data.Database == newTestData.Database {
			found = true
			testData[i].Rows = newTestData.Rows
			break
		}
	}
	if !found {
		testData = append(testData, newTestData)
	}
	return testData
}

func generateTestDataForDifferentServerVersion(remoteStorageType string, offset, rowsCount int, testData []TestDataStruct) []TestDataStruct {
	log.Debug().Msgf("generateTestDataForDifferentServerVersion remoteStorageType=%s", remoteStorageType)
	// https://github.com/Altinity/clickhouse-backup/issues/1127
	// CREATE TABLE engine=Replicated available from 21.3, but ATTACH PART, available only from 21.11
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.11") >= 0 {
		databaseEngine := "Replicated('/clickhouse/{cluster}/{database}','{shard}','{replica}')"
		testData = addTestDataIfNotExistsAndReplaceRowsIfExists(testData, TestDataStruct{
			Name:     "table_in_replicated_db",
			Schema:   "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') ORDER BY id PARTITION BY id",
			Database: dbNameReplicated, DatabaseEngine: databaseEngine,
			Rows: func() []map[string]interface{} {
				result := make([]map[string]interface{}, rowsCount)
				for i := 0; i < rowsCount; i++ {
					result[i] = map[string]interface{}{"id": uint64(i + offset)}
				}
				return result
			}(),
			Fields:  []string{"id"},
			OrderBy: "id",
		})
	}
	// refreshable materialized view allowable 23.12+, https://github.com/ClickHouse/ClickHouse/issues/86922
	// 24.3 + 24.8 got segmentation fault during CREATE MATERIALIZED VIEW ... REFRESH .. ON CLUSTER
	// EMPTY hack doesn't work for EMBEDDED https://github.com/ClickHouse/ClickHouse/issues/87013
	// add data only once only for offset == 0
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "24.8") > 0 && offset == 0 && !strings.Contains(remoteStorageType, "EMBEDDED") {
		testData = addTestDataIfNotExistsAndReplaceRowsIfExists(testData, TestDataStruct{
			Database:       dbNameAtomic,
			DatabaseEngine: "Atomic",
			Name:           "mv_refreshable_dst_table",
			Schema:         "(id UInt64) Engine=MergeTree ORDER BY id",
			SkipInsert:     true,
			//table shall be empty cause restore mv_refreshable with EMPTY option
			Rows: func() []map[string]interface{} {
				return []map[string]interface{}{}
			}(),
			Fields:  []string{"id"},
			OrderBy: "id",
		})
		testData = addTestDataIfNotExistsAndReplaceRowsIfExists(testData, TestDataStruct{
			Database:           dbNameAtomic,
			DatabaseEngine:     "Atomic",
			IsMaterializedView: true,
			Name:               "mv_refreshable",
			Schema:             fmt.Sprintf("REFRESH EVERY 24 HOUR TO `%s`.`mv_refreshable_dst_table_{test}` EMPTY AS SELECT max(id) AS id FROM `%s`.`mv_src_table_{test}`", dbNameAtomic, dbNameAtomic),
			SkipInsert:         true,
			//table shall be empty cause restore mv_refreshable with EMPTY option
			Rows: func() []map[string]interface{} {
				return []map[string]interface{}{}
			}(),
			Fields:  []string{"id"},
			OrderBy: "id",
		})
	}
	return testData
}

func generateTestDataForDifferentStoragePolicy(remoteStorageType string, createAllTypesOfObjectTables bool, offset, rowsCount int, testData []TestDataStruct) []TestDataStruct {
	log.Debug().Msgf("generateTestDataForDifferentServerVersion remoteStorageType=%s", remoteStorageType)
	dbNameEngineMapping := map[string]string{dbNameOrdinary: "Ordinary", dbNameAtomic: "Atomic"}
	for databaseName, databaseEngine := range dbNameEngineMapping {
		testDataWithStoragePolicy := TestDataStruct{
			Database: databaseName, DatabaseEngine: databaseEngine,
			Rows: func() []map[string]interface{} {
				result := make([]map[string]interface{}, rowsCount)
				for i := 0; i < rowsCount; i++ {
					result[i] = map[string]interface{}{"id": uint64(i + offset)}
				}
				return result
			}(),
			Fields:  []string{"id"},
			OrderBy: "id",
		}
		//encrypted disks support after 21.10
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.10") >= 0 {
			testDataWithStoragePolicy.Name = "test_hdd3_encrypted"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') ORDER BY id PARTITION BY id  SETTINGS storage_policy = 'hdd3_only_encrypted'"
			testData = addTestDataIfNotExistsAndReplaceRowsIfExists(testData, testDataWithStoragePolicy)
		}
		//s3 disks support after 21.8
		if (createAllTypesOfObjectTables || strings.Contains(remoteStorageType, "S3")) && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
			testDataWithStoragePolicy.Name = "test_s3"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') ORDER BY id PARTITION BY id SETTINGS storage_policy = 's3_only'"
			testData = addTestDataIfNotExistsAndReplaceRowsIfExists(testData, testDataWithStoragePolicy)
		}
		//encrypted s3 disks support after 21.12
		if (createAllTypesOfObjectTables || strings.Contains(remoteStorageType, "S3")) && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
			testDataWithStoragePolicy.Name = "test_s3_encrypted"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=MergeTree ORDER BY id PARTITION BY id SETTINGS storage_policy = 's3_only_encrypted'"
			testData = addTestDataIfNotExistsAndReplaceRowsIfExists(testData, testDataWithStoragePolicy)
		}
		//gcs over s3 support added in 22.6
		if (createAllTypesOfObjectTables || strings.Contains(remoteStorageType, "GCS")) && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.6") >= 0 && os.Getenv("QA_GCS_OVER_S3_BUCKET") != "" {
			testDataWithStoragePolicy.Name = "test_gcs"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') ORDER BY id PARTITION BY id SETTINGS storage_policy = 'gcs_only'"
			testData = addTestDataIfNotExistsAndReplaceRowsIfExists(testData, testDataWithStoragePolicy)
		}
		//check azure_blob_storage only in 23.3+ (added in 22.1)
		if (createAllTypesOfObjectTables || strings.Contains(remoteStorageType, "AZBLOB")) && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") >= 0 {
			testDataWithStoragePolicy.Name = "test_azure"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') ORDER BY id PARTITION BY id SETTINGS storage_policy = 'azure_only'"
			testData = addTestDataIfNotExistsAndReplaceRowsIfExists(testData, testDataWithStoragePolicy)
		}
	}
	return testData
}

func generateIncrementTestData(t *testing.T, r *require.Assertions, ch *TestEnvironment, remoteStorageType string, createObjectTables bool, incrementData []TestDataStruct, incrementNumber int) []TestDataStruct {
	log.Debug().Msgf("Generate increment test data for %s", remoteStorageType)
	incrementData = generateTestDataForDifferentServerVersion(remoteStorageType, 5*incrementNumber, 5, incrementData)
	incrementData = generateTestDataForDifferentStoragePolicy(remoteStorageType, createObjectTables, 5*incrementNumber, 5, incrementData)
	for _, data := range incrementData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestData(t, data))
	}
	return incrementData
}

// Cleanup functions

func fullCleanup(t *testing.T, r *require.Assertions, env *TestEnvironment, backupNames, backupTypes, databaseList []string, useTestName, checkDeleteErr, checkDeleteOtherErr bool, backupConfig string) {
	for _, backupName := range backupNames {
		for _, backupType := range backupTypes {
			out, err := env.DockerExecOut("clickhouse-backup", "bash", "-xce", "clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" delete "+backupType+" "+backupName)
			if checkDeleteErr {
				r.NoError(err, "checkDeleteErr delete %s %s output: \n%s\nerror: %v", backupType, backupName, out, err)
			}
			log.Debug().Msg(out)
		}
	}
	otherBackupList, lsErr := env.DockerExecOut("clickhouse", "ls", "-1", "/var/lib/clickhouse/backup/*"+t.Name()+"*")
	if lsErr == nil {
		for _, backupName := range strings.Split(otherBackupList, "\n") {
			if backupName != "" {
				out, err := env.DockerExecOut("clickhouse-backup", "bash", "-xce", "clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" delete local "+backupName)
				if checkDeleteOtherErr {
					r.NoError(err, "%s\nunexpected delete local %s output: \n%s\nerror: %v, ", backupName, out, err)
				}
				log.Debug().Msg(out)
			}
		}
	}

	dropDatabasesFromTestDataDataSet(t, r, env, databaseList, useTestName)
}

func dropDatabasesFromTestDataDataSet(t *testing.T, r *require.Assertions, ch *TestEnvironment, databaseList []string, useTestName bool) {
	log.Debug().Msg("Drop all databases")
	for _, db := range databaseList {
		if useTestName {
			db = db + "_" + t.Name()
		}
		r.NoError(ch.dropDatabase(db, true))
	}
}

// Additional helper functions

func replaceStorageDiskNameForReBalance(t *testing.T, r *require.Assertions, env *TestEnvironment, remoteStorageType string, isRebalanced bool) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") < 0 {
		return
	}
	if remoteStorageType != "S3" && remoteStorageType != "GCS" && remoteStorageType != "AZBLOB" {
		return
	}
	oldDisk := "disk_" + strings.ToLower(remoteStorageType)
	newDisk := oldDisk + "_rebalanced"
	if isRebalanced {
		oldDisk = "disk_" + strings.ToLower(remoteStorageType) + "_rebalanced"
		newDisk = strings.TrimSuffix(oldDisk, "_rebalanced")
	}
	fileNames := []string{"storage_configuration_" + strings.ToLower(remoteStorageType) + ".xml"}
	if remoteStorageType == "S3" {
		fileNames = append(fileNames, "storage_configuration_encrypted_"+strings.ToLower(remoteStorageType)+".xml")
	}
	for _, fileName := range fileNames {
		origFile := "/etc/clickhouse-server/config.d/" + fileName
		dstFile := "/var/lib/clickhouse/" + fileName
		sedCmd := fmt.Sprintf("s/<%s>/<%s>/g; s/<\\/%s>/<\\/%s>/g; s/<disk>%s<\\/disk>/<disk>%s<\\/disk>/g", oldDisk, newDisk, oldDisk, newDisk, oldDisk, newDisk)
		env.DockerExecNoError(r, "clickhouse", "sed", "-i", sedCmd, origFile)
		env.DockerExecNoError(r, "clickhouse", "cp", "-vf", origFile, dstFile)
	}
	if isRebalanced {
		env.DockerExecNoError(r, "clickhouse", "bash", "-xc", "cp -aflv -t /var/lib/clickhouse/disks/"+newDisk+"/ /var/lib/clickhouse/disks/"+oldDisk+"/*")
		env.DockerExecNoError(r, "clickhouse", "rm", "-rf", "/var/lib/clickhouse/disks/"+oldDisk+"")
	}
	env.ch.Close()
	r.NoError(utils.ExecCmd(t.Context(), 180*time.Second, "docker", append(env.GetDefaultComposeCommand(), "restart", "clickhouse")...))
	env.connectWithWait(t, r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
}

func testBackupSpecifiedPartitions(t *testing.T, r *require.Assertions, env *TestEnvironment, remoteStorageType string, backupConfig string) {
	log.Debug().Msg("testBackupSpecifiedPartitions started")
	var err error
	var out string
	var result, expectedCount uint64

	partitionBackupName := fmt.Sprintf("partition_backup_%d", rand.Int())
	fullBackupName := fmt.Sprintf("full_backup_%d", rand.Int())
	incrementBackupName := fmt.Sprintf("increment_backup_%d", rand.Int())
	dbName := "test_partitions_" + t.Name()
	fillTables := func(partitions []string) {
		for _, dt := range partitions {
			env.queryWithNoError(r, fmt.Sprintf("INSERT INTO "+dbName+".t1(dt, v) SELECT '%s', number FROM numbers(10)", dt))
			env.queryWithNoError(r, fmt.Sprintf("INSERT INTO "+dbName+".t2(dt, v) SELECT '%s', number FROM numbers(10)", dt))
		}
	}
	createAndFillTables := func() {
		log.Debug().Msg("Create and fill tables")
		env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
		env.queryWithNoError(r, "DROP TABLE IF EXISTS "+dbName+".t1")
		env.queryWithNoError(r, "DROP TABLE IF EXISTS "+dbName+".t2")
		env.queryWithNoError(r, "CREATE TABLE "+dbName+".t1 (dt Date, category Int64, v UInt64) ENGINE=MergeTree() PARTITION BY (category, toYYYYMMDD(dt)) ORDER BY dt")
		env.queryWithNoError(r, "CREATE TABLE "+dbName+".t2 (dt String, category Int64, v UInt64) ENGINE=MergeTree() PARTITION BY (category, dt) ORDER BY dt")
		fillTables([]string{"2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04"})
	}
	createAndFillTables()

	log.Debug().Msg("check create_remote full > create_remote increment > delete local > download full --partitions > restore --data --partitions full > restore_remote increment --partitions")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create_remote", "--tables="+dbName+".t*", fullBackupName)

	//increment backup
	fillTables([]string{"2022-01-05"})
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create_remote", "--delete-source", "--diff-from-remote="+fullBackupName, "--tables="+dbName+".t*", incrementBackupName)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "download", "--partitions="+dbName+".t?:(0,'2022-01-02'),(0,'2022-01-03')", fullBackupName)
	fullBackupDir := "/var/lib/clickhouse/backup/" + fullBackupName + "/shadow/" + dbName + "/t?/default/"
	// embedded storage with embedded disks contains object disk files and will download additional data parts
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		fullBackupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED")) + "/" + fullBackupName + "/data/" + dbName + "/t?"
	}
	// embedded storage without embedded disks doesn't contain `shadow` and contain only `metadata`
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") && strings.HasSuffix(remoteStorageType, "_URL") {
		fullBackupDir = "/var/lib/clickhouse/backup/" + fullBackupName + "/metadata/" + dbName + "/t?.json"
	}
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls -la "+fullBackupDir+" | wc -l")
	r.NoError(err)
	expectedLines := "13"
	// custom storage doesn't support --partitions for upload / download now
	// embedded storage with embedded disks contain hardLink files and will download additional data parts
	if remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		expectedLines = "17"
	}
	// embedded storage without embedded disks doesn't contain `shadow` and contain only `metadata`
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") && strings.HasSuffix(remoteStorageType, "_URL") {
		expectedLines = "2"
	}
	r.Equal(expectedLines, strings.Trim(out, "\r\n\t "))
	checkRestoredDataWithPartitions := func(expectedCount uint64) {
		result = 0
		r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT sum(c) FROM (SELECT count() AS c FROM "+dbName+".t1 UNION ALL SELECT count() AS c FROM "+dbName+".t2)"))
		r.Equal(expectedCount, result, "expect count=%d", expectedCount)
	}

	if remoteStorageType == "FTP" && !strings.Contains(backupConfig, "old") {
		// during DROP PARTITION, we create empty covered part, and cant restore via ATTACH TABLE properly, https://github.com/Altinity/clickhouse-backup/issues/756
		out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" restore --data --partitions=\"(0,'2022-01-02'),(0,'2022-01-03')\" "+fullBackupName)
		r.Error(err)
		out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "CLICKHOUSE_RESTORE_AS_ATTACH=0 clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" restore --data --partitions=\"(0,'2022-01-02'),(0,'2022-01-03')\" "+fullBackupName)
	} else {
		out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" restore --data --partitions=\"(0,'2022-01-02'),(0,'2022-01-03')\" "+fullBackupName)
	}
	log.Debug().Msg(out)
	r.NoError(err, "%s\nunexpected error: %v", out, err)
	r.Contains(out, "DROP PARTITION")
	// we just replace partition in exists table, and have incremented data in 2 tables
	checkRestoredDataWithPartitions(100)

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" restore_remote --partitions=\"(0,'2022-01-01')\" "+incrementBackupName)
	log.Debug().Msg(out)
	r.NoError(err)
	r.NotContains(out, "DROP PARTITION")
	// we recreate tables, and have ONLY one partition in two tables
	checkRestoredDataWithPartitions(20)

	log.Debug().Msg("delete local > download > restore --partitions > restore")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "download", fullBackupName)

	expectedLines = "17"
	fullBackupDir = "/var/lib/clickhouse/backup/" + fullBackupName + "/shadow/" + dbName + "/t?/default/"
	// embedded storage with embedded disks contains hardLink files and will download additional data parts
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		fullBackupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED")) + "/" + fullBackupName + "/data/" + dbName + "/t?"
	}
	// embedded storage without embedded disks doesn't contain `shadow` and contain only `metadata`
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") && strings.HasSuffix(remoteStorageType, "_URL") {
		fullBackupDir = "/var/lib/clickhouse/backup/" + fullBackupName + "/metadata/" + dbName + "/t?.json"
		expectedLines = "2"
	}
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls -la "+fullBackupDir+"| wc -l")
	r.NoError(err)
	r.Equal(expectedLines, strings.Trim(out, "\r\n\t "))

	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--partitions=(0,'2022-01-02'),(0,'2022-01-03')", fullBackupName)
	r.NoError(err, "%s\nunexpected error: %v", out, err)
	r.NotContains(out, "DROP PARTITION")
	checkRestoredDataWithPartitions(40)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", fullBackupName)
	checkRestoredDataWithPartitions(80)

	log.Debug().Msg("check delete remote > delete local")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "remote", fullBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName)

	log.Debug().Msg("check create --partitions > upload > delete local > restore_remote")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--tables="+dbName+".t1", "--partitions=(0,'2022-01-02'),(0,'2022-01-03')", partitionBackupName)
	expectedLines = "5"
	partitionBackupDir := "/var/lib/clickhouse/backup/" + partitionBackupName + "/shadow/" + dbName + "/t1/default/"
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") && !strings.HasSuffix(remoteStorageType, "_URL") {
		partitionBackupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED")) + "/" + partitionBackupName + "/data/" + dbName + "/t1"
	}
	//embedded backup without a disk has only local metadata
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") && strings.HasSuffix(remoteStorageType, "_URL") {
		partitionBackupDir = "/var/lib/clickhouse/backup/" + partitionBackupName + "/metadata/" + dbName + "/t?.json"
		expectedLines = "1"
	}
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls -la "+partitionBackupDir+"| wc -l")
	r.NoError(err)
	r.Equal(expectedLines, strings.Trim(out, "\r\n\t "))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "upload", partitionBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", partitionBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore_remote", partitionBackupName)
	checkPartialRestoredT1 := func(createPartial bool) {
		log.Debug().Msg("Check partial restored t1")
		result = 0
		r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM "+dbName+".t1"))

		expectedCount = 20
		// custom and embedded doesn't support --partitions in upload and download
		if !createPartial && (remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED")) {
			expectedCount = 40
		}
		r.Equal(expectedCount, result, fmt.Sprintf("expect count=%d", expectedCount))

		log.Debug().Msg("Check only selected partitions restored")
		result = 0
		r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM "+dbName+".t1 WHERE dt NOT IN ('2022-01-02','2022-01-03')"))
		expectedCount = 0
		// custom and embedded doesn't support --partitions in upload and download
		if !createPartial && (remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED")) {
			expectedCount = 20
		}
		r.Equal(expectedCount, result, "expect count=%s", expectedCount)
	}
	checkPartialRestoredT1(true)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", partitionBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "remote", partitionBackupName)

	log.Debug().Msg("check create > upload --partitions > delete local > restore_remote")
	createAndFillTables()
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--tables="+dbName+".t1", partitionBackupName)
	partitionBackupDir = "/var/lib/clickhouse/backup/" + partitionBackupName + "/shadow/" + dbName + "/t1/default/"
	expectedLines = "7"
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") && !strings.HasSuffix(remoteStorageType, "_URL") {
		partitionBackupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED")) + "/" + partitionBackupName + "/data/" + dbName + "/t1"
	}
	//embedded backup without a disk has only local metadata
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") && strings.HasSuffix(remoteStorageType, "_URL") {
		partitionBackupDir = "/var/lib/clickhouse/backup/" + partitionBackupName + "/metadata/" + dbName + "/t?.json"
		expectedLines = "1"
	}
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-c", "ls -la "+partitionBackupDir+" | wc -l")
	r.NoError(err)
	r.Equal(expectedLines, strings.Trim(out, "\r\n\t "))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "upload", "--tables="+dbName+".t1", "--partitions=0-20220102,0-20220103", partitionBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", partitionBackupName)

	// restore partial uploaded
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore_remote", partitionBackupName)
	checkPartialRestoredT1(false)

	log.Debug().Msg("DELETE partition backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "remote", partitionBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", partitionBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "remote", incrementBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", incrementBackupName)

	if err = env.dropDatabase(dbName, true); err != nil {
		t.Fatal(err)
	}
	log.Debug().Msg("testBackupSpecifiedPartitions finish")
}

// Unused import placeholders to ensure compilation
var _ = bufio.Scanner{}
var _ = rand.Int
