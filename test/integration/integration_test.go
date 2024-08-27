//go:build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	pool "github.com/jolestar/go-commons-pool/v2"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"golang.org/x/mod/semver"
	stdlog "log"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/log_helper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/partition"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
)

var projectId atomic.Uint32
var dockerPool *pool.ObjectPool

// setup log level
func init() {
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
		log.Fatal().Msgf("invalid RUN_PARALLEL environment variable value %s", runParallel)
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

const dbNameAtomic = "_test#$.ДБ_atomic_"
const dbNameOrdinary = "_test#$.ДБ_ordinary_"
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

func NewTestEnvironment(t *testing.T) (*TestEnvironment, *require.Assertions) {
	isParallel := os.Getenv("RUN_PARALLEL") != "1" && slices.Index([]string{"TestLongListRemote" /*,"TestIntegrationAzure"*/}, t.Name()) == -1
	if os.Getenv("COMPOSE_FILE") == "" || os.Getenv("CUR_DIR") == "" {
		t.Fatal("please setup COMPOSE_FILE and CUR_DIR environment variables")
	}
	t.Helper()
	if isParallel {
		t.Parallel()
	}

	r := require.New(t)
	envObj, err := dockerPool.BorrowObject(context.Background())
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

	if t.Name() == "TestIntegrationS3" || t.Name() == "TestIntegrationEmbedded" {
		env.DockerExecNoError(r, "minio", "rm", "-rf", "/bitnami/minio/data/clickhouse/disk_s3")
	}

	if t.Name() == "TestRBAC" || t.Name() == "TestConfigs" || t.Name() == "TestIntegrationEmbedded" {
		env.DockerExecNoError(r, "minio", "rm", "-rf", "/bitnami/minio/data/clickhouse/backups_s3")
	}
	if t.Name() == "TestIntegrationCustomRsync" {
		env.DockerExecNoError(r, "sshd", "rm", "-rf", "/root/rsync_backups")
	}
	if t.Name() == "TestIntegrationCustomRestic" {
		env.DockerExecNoError(r, "minio", "rm", "-rf", "/bitnami/minio/data/clickhouse/restic")
	}
	if t.Name() == "TestIntegrationCustomKopia" {
		env.DockerExecNoError(r, "minio", "rm", "-rf", "/bitnami/minio/data/clickhouse/kopia")
	}

	if err := dockerPool.ReturnObject(context.Background(), env); err != nil {
		t.Fatalf("dockerPool.ReturnObject error: %+v", err)
	}

}

// TestLongListRemote - no parallel, cause need to restart minio
func TestLongListRemote(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 0*time.Second, 1*time.Second, 1*time.Minute)
	totalCacheCount := 20
	testBackupName := "test_list_remote"

	for i := 0; i < totalCacheCount; i++ {
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml ALLOW_EMPTY_BACKUPS=true clickhouse-backup create_remote %s_%d", testBackupName, i))
	}

	r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker", append(env.GetDefaultComposeCommand(), "restart", "minio")...))
	time.Sleep(2 * time.Second)

	startFirst := time.Now()
	env.DockerExecNoError(r, "clickhouse-backup", "rm", "-rfv", "/tmp/.clickhouse-backup-metadata.cache.S3")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "remote")
	noCacheDuration := time.Since(startFirst)

	env.DockerExecNoError(r, "clickhouse-backup", "chmod", "-Rv", "+r", "/tmp/.clickhouse-backup-metadata.cache.S3")

	r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker", append(env.GetDefaultComposeCommand(), "restart", "minio")...))
	time.Sleep(2 * time.Second)

	startCashed := time.Now()
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "remote")
	cachedDuration := time.Since(startCashed)

	r.Greater(noCacheDuration, cachedDuration, "noCacheDuration=%s shall be greater cachedDuration=%s", noCacheDuration, cachedDuration)

	r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker", append(env.GetDefaultComposeCommand(), "restart", "minio")...))
	time.Sleep(2 * time.Second)

	startCacheClear := time.Now()
	env.DockerExecNoError(r, "clickhouse-backup", "rm", "-Rfv", "/tmp/.clickhouse-backup-metadata.cache.S3")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "remote")
	cacheClearDuration := time.Since(startCacheClear)

	r.Greater(cacheClearDuration, cachedDuration, "cacheClearDuration=%s shall be greater cachedDuration=%s", cacheClearDuration.String(), cachedDuration.String())
	log.Debug().Msgf("noCacheDuration=%s cachedDuration=%s cacheClearDuration=%s", noCacheDuration.String(), cachedDuration.String(), cacheClearDuration.String())

	testListRemoteAllBackups := make([]string, totalCacheCount)
	for i := 0; i < totalCacheCount; i++ {
		testListRemoteAllBackups[i] = fmt.Sprintf("%s_%d", testBackupName, i)
	}
	fullCleanup(t, r, env, testListRemoteAllBackups, []string{"remote", "local"}, []string{}, true, true, "config-s3.yml")
	env.Cleanup(t, r)
}

func TestIntegrationAzure(t *testing.T) {
	if isTestShouldSkip("AZURE_TESTS") {
		t.Skip("Skipping Azure integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	env.runMainIntegrationScenario(t, "AZBLOB", "config-azblob.yml")
	env.Cleanup(t, r)
}

func TestIntegrationGCSWithCustomEndpoint(t *testing.T) {
	if isTestShouldSkip("GCS_TESTS") {
		t.Skip("Skipping GCS_EMULATOR integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	env.runMainIntegrationScenario(t, "GCS_EMULATOR", "config-gcs-custom-endpoint.yml")
	env.Cleanup(t, r)
}

func TestIntegrationSFTPAuthKey(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.uploadSSHKeys(r, "clickhouse-backup")
	env.runMainIntegrationScenario(t, "SFTP", "config-sftp-auth-key.yaml")
	env.Cleanup(t, r)
}

func TestIntegrationSFTPAuthPassword(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.runMainIntegrationScenario(t, "SFTP", "config-sftp-auth-password.yaml")
	env.Cleanup(t, r)
}

func TestIntegrationFTP(t *testing.T) {
	env, r := NewTestEnvironment(t)
	// 21.8 can't execute SYSTEM RESTORE REPLICA
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") > 1 {
		env.runMainIntegrationScenario(t, "FTP", "config-ftp.yaml")
	} else {
		env.runMainIntegrationScenario(t, "FTP", "config-ftp-old.yaml")
	}
	env.Cleanup(t, r)
}

func TestIntegrationS3Glacier(t *testing.T) {
	if isTestShouldSkip("GLACIER_TESTS") {
		t.Skip("Skipping GLACIER integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	r.NoError(env.DockerCP("config-s3-glacier.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml.s3glacier-template"))
	env.InstallDebIfNotExists(r, "clickhouse-backup", "curl", "gettext-base", "bsdmainutils", "dnsutils", "git", "ca-certificates")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config.yml.s3glacier-template | envsubst > /etc/clickhouse-backup/config-s3-glacier.yml")
	dockerExecTimeout = 60 * time.Minute
	env.runMainIntegrationScenario(t, "GLACIER", "config-s3-glacier.yml")
	dockerExecTimeout = 3 * time.Minute
	env.Cleanup(t, r)
}

func TestIntegrationS3(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.checkObjectStorageIsEmpty(t, r, "S3")
	env.runMainIntegrationScenario(t, "S3", "config-s3.yml")
	env.Cleanup(t, r)
}

func TestIntegrationGCS(t *testing.T) {
	if isTestShouldSkip("GCS_TESTS") {
		t.Skip("Skipping GCS integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	env.runMainIntegrationScenario(t, "GCS", "config-gcs.yml")
	env.Cleanup(t, r)
}

func TestIntegrationEmbedded(t *testing.T) {
	//t.Skipf("Test skipped, wait 23.8, RESTORE Ordinary table and RESTORE MATERIALIZED VIEW and {uuid} not works for %s version, look https://github.com/ClickHouse/ClickHouse/issues/43971 and https://github.com/ClickHouse/ClickHouse/issues/42709", os.Getenv("CLICKHOUSE_VERSION"))
	//dependencies restore https://github.com/ClickHouse/ClickHouse/issues/39416, fixed in 23.3
	version := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(version, "23.3") < 0 {
		t.Skipf("Test skipped, BACKUP/RESTORE not production ready for %s version", version)
	}
	env, r := NewTestEnvironment(t)

	//CUSTOM backup creates folder in each disk, need to clear
	env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/var/lib/clickhouse/disks/backups_s3/backup/")
	env.runMainIntegrationScenario(t, "EMBEDDED_S3", "config-s3-embedded.yml")

	//@TODO think about how to implements embedded backup for s3_plain disks
	//env.DockerExecNoError(r, "clickhouse", "rm", "-rf", "/var/lib/clickhouse/disks/backups_s3_plain/backup/")
	//runMainIntegrationScenario(t, "EMBEDDED_S3_PLAIN", "config-s3-plain-embedded.yml")

	t.Log("@TODO clickhouse-server don't close connection properly after FIN from azurite during BACKUP/RESTORE https://github.com/ClickHouse/ClickHouse/issues/60447, https://github.com/Azure/Azurite/issues/2053")
	//env.DockerExecNoError(r, "azure", "apk", "add", "tcpdump")
	//r.NoError(env.DockerExecBackground("azure", "tcpdump", "-i", "any", "-w", "/tmp/azurite_http.pcap", "port", "10000"))
	////CUSTOM backup create folder in each disk
	//env.DockerExecNoError(r, "clickhouse", "rm", "-rf", "/var/lib/clickhouse/disks/backups_azure/backup/")
	//if compareVersion(version, "24.2") >= 0 {
	//	env.runMainIntegrationScenario(t, "EMBEDDED_AZURE_URL", "config-azblob-embedded-url.yml")
	//}
	//env.runMainIntegrationScenario(t, "EMBEDDED_AZURE", "config-azblob-embedded.yml")
	//env.DockerExecNoError(r, "azure", "pkill", "tcpdump")
	//r.NoError(env.DockerCP("azure:/tmp/azurite_http.pcap", "./azurite_http.pcap"))

	if compareVersion(version, "23.8") >= 0 {
		//CUSTOM backup creates folder in each disk, need to clear
		env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/var/lib/clickhouse/disks/backups_local/backup/")
		env.runMainIntegrationScenario(t, "EMBEDDED_LOCAL", "config-s3-embedded-local.yml")
	}
	if compareVersion(version, "24.3") >= 0 {
		//@todo think about named collections to avoid show credentials in logs look to https://github.com/fsouza/fake-gcs-server/issues/1330, https://github.com/fsouza/fake-gcs-server/pull/1164
		env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "gettext-base")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "cat /etc/clickhouse-backup/config-gcs-embedded-url.yml.template | envsubst > /etc/clickhouse-backup/config-gcs-embedded-url.yml")
		env.runMainIntegrationScenario(t, "EMBEDDED_GCS_URL", "config-gcs-embedded-url.yml")
		env.runMainIntegrationScenario(t, "EMBEDDED_S3_URL", "config-s3-embedded-url.yml")
	}
	env.Cleanup(t, r)
}

func TestIntegrationCustomKopia(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "curl")
	env.DockerExecNoError(r, "clickhouse-backup", "update-ca-certificates")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xce", "command -v yq || curl -sL \"https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture)\" -o /usr/bin/yq && chmod +x /usr/bin/yq")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "jq", "bzip2", "pgp", "git")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "curl -sfL https://kopia.io/signing-key | gpg --dearmor -o /usr/share/keyrings/kopia-keyring.gpg")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "echo 'deb [signed-by=/usr/share/keyrings/kopia-keyring.gpg] https://packages.kopia.io/apt/ stable main' > /etc/apt/sources.list.d/kopia.list")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "kopia", "xxd", "bsdmainutils", "parallel")

	env.runIntegrationCustom(t, r, "kopia")
	env.Cleanup(t, r)
}

func TestIntegrationCustomRestic(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "curl")
	env.DockerExecNoError(r, "clickhouse-backup", "update-ca-certificates")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xce", "command -v yq || curl -sL \"https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture)\" -o /usr/bin/yq && chmod +x /usr/bin/yq")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "jq", "bzip2", "pgp", "git")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "command -v restic || RELEASE_TAG=$(curl -H 'Accept: application/json' -sL https://github.com/restic/restic/releases/latest | jq -c -r -M '.tag_name'); RELEASE=$(echo ${RELEASE_TAG} | sed -e 's/v//'); curl -sfL \"https://github.com/restic/restic/releases/download/${RELEASE_TAG}/restic_${RELEASE}_linux_amd64.bz2\" | bzip2 -d > /bin/restic; chmod +x /bin/restic")
	env.runIntegrationCustom(t, r, "restic")
	env.Cleanup(t, r)
}

func TestIntegrationCustomRsync(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.uploadSSHKeys(r, "clickhouse-backup")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "ca-certificates", "curl")
	env.DockerExecNoError(r, "clickhouse-backup", "update-ca-certificates")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xce", "command -v yq || curl -sL \"https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture)\" -o /usr/bin/yq && chmod +x /usr/bin/yq")
	env.InstallDebIfNotExists(r, "clickhouse-backup", "jq", "openssh-client", "rsync")
	env.runIntegrationCustom(t, r, "rsync")
	env.Cleanup(t, r)
}

func (env *TestEnvironment) runIntegrationCustom(t *testing.T, r *require.Assertions, customType string) {
	env.DockerExecNoError(r, "clickhouse-backup", "mkdir", "-pv", "/custom/"+customType)
	r.NoError(env.DockerCP("./"+customType+"/", "clickhouse-backup:/custom/"))
	env.runMainIntegrationScenario(t, "CUSTOM", "config-custom-"+customType+".yml")
}

// TestS3NoDeletePermission - no parallel
func TestS3NoDeletePermission(t *testing.T) {
	if isTestShouldSkip("RUN_ADVANCED_TESTS") {
		t.Skip("Skipping Advanced integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	env.DockerExecNoError(r, "minio", "/bin/minio_nodelete.sh")
	r.NoError(env.DockerCP("config-s3-nodelete.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))

	generateTestData(t, r, env, "S3", false, defaultTestData)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create_remote", "no_delete_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "no_delete_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore_remote", "no_delete_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "no_delete_backup")
	r.Error(env.DockerExec("clickhouse-backup", "clickhouse-backup", "delete", "remote", "no_delete_backup"))
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameMySQL, dbNamePostgreSQL, Issue331Atomic, Issue331Ordinary}
	dropDatabasesFromTestDataDataSet(t, r, env, databaseList)
	r.NoError(env.DockerCP("config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", "no_delete_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "list", "remote")
	env.checkObjectStorageIsEmpty(t, r, "S3")
	env.Cleanup(t, r)
}

// TestRBAC need clickhouse-server restart, no parallel
func TestRBAC(t *testing.T) {
	chVersion := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(chVersion, "20.4") < 0 {
		t.Skipf("Test skipped, RBAC not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	testRBACScenario := func(config string) {
		env.connectWithWait(r, 1*time.Second, 1*time.Second, 1*time.Minute)

		env.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_rbac")
		env.queryWithNoError(r, "CREATE TABLE default.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")
		env.queryWithNoError(r, "DROP SETTINGS PROFILE IF EXISTS `test.rbac-name`")
		env.queryWithNoError(r, "DROP QUOTA IF EXISTS `test.rbac-name`")
		env.queryWithNoError(r, "DROP ROW POLICY IF EXISTS `test.rbac-name` ON default.test_rbac")
		env.queryWithNoError(r, "DROP ROLE IF EXISTS `test.rbac-name`")
		env.queryWithNoError(r, "DROP USER IF EXISTS `test.rbac-name`")

		createRBACObjects := func(drop bool) {
			if drop {
				log.Debug().Msg("drop all RBAC related objects")
				env.queryWithNoError(r, "DROP SETTINGS PROFILE `test.rbac-name`")
				env.queryWithNoError(r, "DROP QUOTA `test.rbac-name`")
				env.queryWithNoError(r, "DROP ROW POLICY `test.rbac-name` ON default.test_rbac")
				env.queryWithNoError(r, "DROP ROLE `test.rbac-name`")
				env.queryWithNoError(r, "DROP USER `test.rbac-name`")
			}
			log.Debug().Msg("create RBAC related objects")
			env.queryWithNoError(r, "CREATE SETTINGS PROFILE `test.rbac-name` SETTINGS max_execution_time=60")
			env.queryWithNoError(r, "CREATE ROLE `test.rbac-name` SETTINGS PROFILE `test.rbac-name`")
			env.queryWithNoError(r, "CREATE USER `test.rbac-name` IDENTIFIED BY 'test_rbac_password' DEFAULT ROLE `test.rbac-name`")
			env.queryWithNoError(r, "CREATE QUOTA `test.rbac-name` KEYED BY user_name FOR INTERVAL 1 hour NO LIMITS TO `test.rbac-name`")
			env.queryWithNoError(r, "CREATE ROW POLICY `test.rbac-name` ON default.test_rbac USING 1=1 AS RESTRICTIVE TO `test.rbac-name`")
		}
		createRBACObjects(false)

		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "create", "--rbac", "--rbac-only", "--env", "S3_COMPRESSION_FORMAT=zstd", "test_rbac_backup")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup upload test_rbac_backup")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "delete", "local", "test_rbac_backup")
		env.DockerExecNoError(r, "clickhouse", "ls", "-lah", "/var/lib/clickhouse/access")

		log.Debug().Msg("create conflicted RBAC objects")
		createRBACObjects(true)

		env.DockerExecNoError(r, "clickhouse", "ls", "-lah", "/var/lib/clickhouse/access")

		log.Debug().Msg("download+restore RBAC")
		env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 CLICKHOUSE_BACKUP_CONFIG="+config+" clickhouse-backup download test_rbac_backup")

		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 clickhouse-backup -c "+config+" restore --rm --rbac test_rbac_backup")
		log.Debug().Msg(out)
		r.Contains(out, "RBAC successfully restored")
		r.NoError(err, "%s\nunexpected RBAC error: %v", out, err)

		out, err = env.DockerExecOut("clickhouse-backup", "bash", "-xec", "ALLOW_EMPTY_BACKUPS=1 clickhouse-backup -c "+config+" restore --rm --rbac-only test_rbac_backup")
		log.Debug().Msg(out)
		r.Contains(out, "RBAC successfully restored")
		r.NoError(err, "%s\nunexpected RBAC error: %v", out, err)
		env.DockerExecNoError(r, "clickhouse", "ls", "-lah", "/var/lib/clickhouse/access")

		env.ch.Close()
		// r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, append(env.GetDefaultComposeCommand(), "restart", "clickhouse")))
		env.connectWithWait(r, 2*time.Second, 2*time.Second, 1*time.Minute)

		env.DockerExecNoError(r, "clickhouse", "ls", "-lah", "/var/lib/clickhouse/access")

		rbacTypes := map[string]string{
			"PROFILES": "test.rbac-name",
			"QUOTAS":   "test.rbac-name",
			"POLICIES": "`test.rbac-name` ON default.test_rbac",
			"ROLES":    "test.rbac-name",
			"USERS":    "test.rbac-name",
		}
		for rbacType, expectedValue := range rbacTypes {
			var rbacRows []struct {
				Name string `ch:"name"`
			}
			err := env.ch.Select(&rbacRows, fmt.Sprintf("SHOW %s", rbacType))
			r.NoError(err)
			found := false
			for _, row := range rbacRows {
				log.Debug().Msgf("rbacType=%s expectedValue=%s row.Name=%s", rbacType, expectedValue, row.Name)
				if expectedValue == row.Name {
					found = true
					break
				}
			}
			if !found {
				//env.DockerExecNoError(r, "clickhouse", "cat", "/var/log/clickhouse-server/clickhouse-server.log")
				r.Failf("wrong RBAC", "SHOW %s, %#v doesn't contain %#v", rbacType, rbacRows, expectedValue)
			}
		}
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "delete", "local", "test_rbac_backup")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", config, "delete", "remote", "test_rbac_backup")

		env.queryWithNoError(r, "DROP SETTINGS PROFILE `test.rbac-name`")
		env.queryWithNoError(r, "DROP QUOTA `test.rbac-name`")
		env.queryWithNoError(r, "DROP ROW POLICY `test.rbac-name` ON default.test_rbac")
		env.queryWithNoError(r, "DROP ROLE `test.rbac-name`")
		env.queryWithNoError(r, "DROP USER `test.rbac-name`")
		env.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_rbac")
		env.ch.Close()
	}
	testRBACScenario("/etc/clickhouse-backup/config-s3.yml")
	if compareVersion(chVersion, "24.1") >= 0 {
		testRBACScenario("/etc/clickhouse-backup/config-s3-embedded.yml")
		testRBACScenario("/etc/clickhouse-backup/config-s3-embedded-url.yml")
		testRBACScenario("/etc/clickhouse-backup/config-azblob-embedded.yml")
	}
	if compareVersion(chVersion, "24.2") >= 0 {
		testRBACScenario("/etc/clickhouse-backup/config-azblob-embedded-url.yml")
	}
	env.Cleanup(t, r)
}

// TestConfigs - require direct access to `/etc/clickhouse-backup/`, so executed inside `clickhouse` container
// need clickhouse-server restart, no parallel
func TestConfigs(t *testing.T) {
	env, r := NewTestEnvironment(t)
	testConfigsScenario := func(config string) {
		env.connectWithWait(r, 0*time.Millisecond, 1*time.Second, 1*time.Minute)
		env.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_configs")
		env.queryWithNoError(r, "CREATE TABLE default.test_configs (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")

		env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "echo '<yandex><profiles><default><empty_result_for_aggregation_by_empty_set>1</empty_result_for_aggregation_by_empty_set></default></profiles></yandex>' > /etc/clickhouse-server/users.d/test_config.xml")

		env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", config, "create", "--configs", "--configs-only", "test_configs_backup")
		env.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_configs")
		compression := ""
		if !strings.Contains(config, "embedded") {
			compression = "--env AZBLOB_COMPRESSION_FORMAT=zstd --env S3_COMPRESSION_FORMAT=zstd"
		}
		env.DockerExecNoError(r, "clickhouse", "bash", "-xec", "clickhouse-backup upload "+compression+" --env CLICKHOUSE_BACKUP_CONFIG="+config+" --env S3_COMPRESSION_FORMAT=none --env ALLOW_EMPTY_BACKUPS=1 test_configs_backup")
		env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", config, "delete", "local", "test_configs_backup")

		env.queryWithNoError(r, "SYSTEM RELOAD CONFIG")
		env.ch.Close()
		env.connectWithWait(r, 1*time.Second, 1*time.Second, 1*time.Minute)
		selectEmptyResultForAggQuery := "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"
		var settings string
		r.NoError(env.ch.SelectSingleRowNoCtx(&settings, selectEmptyResultForAggQuery))
		if settings != "1" {
			env.DockerExecNoError(r, "clickhouse", "grep", "empty_result_for_aggregation_by_empty_set", "-r", "/var/lib/clickhouse/preprocessed_configs/")
		}
		r.Equal("1", settings, "expect empty_result_for_aggregation_by_empty_set=1")

		env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml")
		env.DockerExecNoError(r, "clickhouse", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG="+config+" ALLOW_EMPTY_BACKUPS=1 clickhouse-backup download test_configs_backup")

		r.NoError(env.ch.Query("SYSTEM RELOAD CONFIG"))
		env.ch.Close()
		env.connectWithWait(r, 1*time.Second, 1*time.Second, 1*time.Minute)

		settings = ""
		r.NoError(env.ch.SelectSingleRowNoCtx(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
		r.Equal("0", settings, "expect empty_result_for_aggregation_by_empty_set=0")

		env.DockerExecNoError(r, "clickhouse", "bash", "-xec", "CLICKHOUSE_BACKUP_CONFIG="+config+" CLICKHOUSE_RESTART_COMMAND='sql:SYSTEM RELOAD CONFIG' clickhouse-backup restore --rm --configs --configs-only test_configs_backup")

		env.ch.Close()
		env.connectWithWait(r, 1*time.Second, 1*time.Second, 1*time.Second)

		settings = ""
		r.NoError(env.ch.SelectSingleRowNoCtx(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
		r.Equal("1", settings, "expect empty_result_for_aggregation_by_empty_set=1")

		isTestConfigsTablePresent := 0
		r.NoError(env.ch.SelectSingleRowNoCtx(&isTestConfigsTablePresent, "SELECT count() FROM system.tables WHERE database='default' AND name='test_configs' SETTINGS empty_result_for_aggregation_by_empty_set=1"))
		r.Equal(0, isTestConfigsTablePresent, "expect default.test_configs is not present")

		env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", config, "delete", "local", "test_configs_backup")
		env.DockerExecNoError(r, "clickhouse", "clickhouse-backup", "-c", config, "delete", "remote", "test_configs_backup")
		env.DockerExecNoError(r, "clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml")

		env.ch.Close()
	}
	testConfigsScenario("/etc/clickhouse-backup/config-s3.yml")
	chVersion := os.Getenv("CLICKHOUSE_VERSION")
	if compareVersion(chVersion, "24.1") >= 0 {
		testConfigsScenario("/etc/clickhouse-backup/config-s3-embedded.yml")
		testConfigsScenario("/etc/clickhouse-backup/config-s3-embedded-url.yml")
		testConfigsScenario("/etc/clickhouse-backup/config-azblob-embedded.yml")
	}
	if compareVersion(chVersion, "24.2") >= 0 {
		testConfigsScenario("/etc/clickhouse-backup/config-azblob-embedded-url.yml")
	}
	env.Cleanup(t, r)
}

const apiBackupNumber = 5

func TestServerAPI(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 0*time.Second, 1*time.Second, 1*time.Minute)
	r.NoError(env.DockerCP("config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	fieldTypes := []string{"UInt64", "String", "Int"}
	env.InstallDebIfNotExists(r, "clickhouse-backup", "curl", "jq")
	maxTables := 10
	minFields := 10
	randFields := 10
	fillDatabaseForAPIServer(maxTables, minFields, randFields, env, r, fieldTypes)

	log.Debug().Msg("Run `clickhouse-backup server --watch` in background")
	env.DockerExecBackgroundNoError(r, "clickhouse-backup", "bash", "-ce", "clickhouse-backup server --watch &>>/tmp/clickhouse-backup-server.log")
	time.Sleep(1 * time.Second)

	testAPIBackupVersion(r, env)

	testAPIBackupCreate(r, env)

	testAPIBackupTables(r, env)

	testAPIBackupUpload(r, env)

	testAPIBackupTablesRemote(r, env)

	log.Debug().Msg("Check /backup/actions")
	env.queryWithNoError(r, "SELECT count() FROM system.backup_actions")

	testAPIBackupList(t, r, env)

	testAPIDeleteLocalDownloadRestore(r, env)

	testAPIMetrics(r, env)

	testAPIWatchAndKill(r, env)

	testAPIBackupActions(r, env)

	testAPIRestart(r, env)

	testAPIBackupDelete(r, env)

	testAPIBackupClean(r, env)

	env.DockerExecNoError(r, "clickhouse-backup", "pkill", "-n", "-f", "clickhouse-backup")
	r.NoError(env.dropDatabase("long_schema"))
	env.Cleanup(t, r)
}

func testAPIRestart(r *require.Assertions, env *TestEnvironment) {
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL -XPOST 'http://localhost:7171/restart'")
	log.Debug().Msg(out)
	r.NoError(err, "%s\nunexpected POST /restart error %v", out, err)
	r.Contains(out, "acknowledged")

	//some actions need time for restart
	time.Sleep(6 * time.Second)

	var inProgressActions uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&inProgressActions, "SELECT count() FROM system.backup_actions WHERE status!=?", status.CancelStatus))
	r.Equal(uint64(0), inProgressActions)
}

func runClickHouseClientInsertSystemBackupActions(r *require.Assertions, env *TestEnvironment, commands []string, needWait bool) {
	sql := "INSERT INTO system.backup_actions(command) " + "VALUES ('" + strings.Join(commands, "'),('") + "')"
	out, err := env.DockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("clickhouse client --echo -mn -q \"%s\"", sql))
	r.NoError(err, "%s -> %s unexpected error: %v", sql, out, err)
	if needWait {
		for _, command := range commands {
			for {
				time.Sleep(500 * time.Millisecond)
				var commandStatus string
				r.NoError(env.ch.SelectSingleRowNoCtx(&commandStatus, "SELECT status FROM system.backup_actions WHERE command=?", command))
				if commandStatus != status.InProgressStatus {
					break
				}
			}
		}
	}
}

func testAPIBackupActions(r *require.Assertions, env *TestEnvironment) {
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"create_remote actions_backup1"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"delete local actions_backup1", "restore_remote --rm actions_backup1"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"delete local actions_backup1", "delete remote actions_backup1"}, false)

	runClickHouseClientInsertSystemBackupActions(r, env, []string{"create actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"upload actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"delete local actions_backup2"}, false)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"download actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"restore --rm actions_backup2"}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{"delete local actions_backup2", "delete remote actions_backup2"}, false)

	inProgressActions := make([]struct {
		Command string `ch:"command"`
		Status  string `ch:"status"`
	}, 0)
	r.NoError(env.ch.StructSelect(&inProgressActions, "SELECT command, status FROM system.backup_actions WHERE command LIKE '%actions%' AND status IN (?,?)", status.InProgressStatus, status.ErrorStatus))
	r.Equal(0, len(inProgressActions), "inProgressActions=%+v", inProgressActions)

	var actionsBackups uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&actionsBackups, "SELECT count() FROM system.backup_list WHERE name LIKE 'backup_action%'"))
	r.Equal(uint64(0), actionsBackups)

	out, err := env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_create_remote_status 1")
	r.Contains(out, "clickhouse_backup_last_create_status 1")
	r.Contains(out, "clickhouse_backup_last_upload_status 1")
	r.Contains(out, "clickhouse_backup_last_delete_status 1")
	r.Contains(out, "clickhouse_backup_last_download_status 1")
	r.Contains(out, "clickhouse_backup_last_restore_status 1")
}

func testAPIWatchAndKill(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/watch + /backup/kill")
	runKillCommand := func(command string) {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL 'http://localhost:7171/backup/kill?command=%s'", command))
		r.NoError(err, "%s\nunexpected GET /kill error: %v", out, err)
	}
	checkWatchBackup := func(expectedCount uint64) {
		var watchBackups uint64
		r.NoError(env.ch.SelectSingleRowNoCtx(&watchBackups, "SELECT count() FROM system.backup_list WHERE name LIKE 'shard%'"))
		r.Equal(expectedCount, watchBackups)
	}

	checkCanceledCommand := func(expectedCount int) {
		canceledCommands := make([]struct {
			Status  string `ch:"status"`
			Command string `ch:"command"`
		}, 0)
		r.NoError(env.ch.StructSelect(&canceledCommands, "SELECT status, command FROM system.backup_actions WHERE command LIKE 'watch%'"))
		r.Equal(expectedCount, len(canceledCommands))
		for i := range canceledCommands {
			r.Equal("watch", canceledCommands[i].Command)
			r.Equal(status.CancelStatus, canceledCommands[i].Status)
		}
	}

	checkWatchBackup(1)
	runKillCommand("watch")
	checkCanceledCommand(1)

	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/watch'")
	r.NoError(err, "%s\nunexpected GET /backup/watch error: %v", out, err)
	time.Sleep(7 * time.Second)

	checkWatchBackup(1)
	runKillCommand("watch")
	checkCanceledCommand(2)
}

func testAPIBackupDelete(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/delete/{where}/{name}")
	for i := 1; i <= apiBackupNumber; i++ {
		out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/local/z_backup_%d'", i))
		r.NoError(err, "%s\nunexpected POST /backup/delete/local error: %v", out, err)
		r.NotContains(out, "another operation is currently running")
		r.NotContains(out, "\"status\":\"error\"")
		out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/remote/z_backup_%d'", i))
		r.NoError(err, "%s\nunexpected POST /backup/delete/remote error: %v", out, err)
		r.NotContains(out, "another operation is currently running")
		r.NotContains(out, "\"status\":\"error\"")
	}
	out, err := env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_delete_status 1")

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XGET 'http://localhost:7171/backup/list'"))
	r.NoError(err, "%s\nunexpected GET /backup/list error: %v", out, err)
	scanner := bufio.NewScanner(strings.NewReader(out))
	for scanner.Scan() {
		type backupJSON struct {
			Name           string `json:"name"`
			Created        string `json:"created"`
			Size           uint64 `json:"size,omitempty"`
			Location       string `json:"location"`
			RequiredBackup string `json:"required"`
			Desc           string `json:"desc"`
		}
		listItem := backupJSON{}
		r.NoError(json.Unmarshal(scanner.Bytes(), &listItem))
		out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/delete/%s/%s'", listItem.Location, listItem.Name))
		r.NoError(err, "%s\nunexpected POST /backup/delete/%s/%s error: %v", out, listItem.Location, listItem.Name, err)
	}

	r.NoError(scanner.Err())

}

func testAPIBackupClean(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/clean/ /backup/clean_remote_broken/ and /backup/actions fot these two commands")
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/clean'"))
	r.NoError(err, "%s\nunexpected POST /backup/clean error: %v", out, err)
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")

	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", fmt.Sprintf("curl -sfL -XPOST 'http://localhost:7171/backup/clean/remote_broken'"))
	r.NoError(err, "%s\nunexpected POST /backup/clean/remote_broken error: %v", out, err)
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")

	runClickHouseClientInsertSystemBackupActions(r, env, []string{"clean", "clean_remote_broken"}, false)
}

func testAPIMetrics(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /metrics clickhouse_backup_last_backup_size_remote")
	var lastRemoteSize int64
	r.NoError(env.ch.SelectSingleRowNoCtx(&lastRemoteSize, "SELECT size FROM system.backup_list WHERE name='z_backup_5' AND location='remote'"))

	var realTotalBytes uint64
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		r.NoError(env.ch.SelectSingleRowNoCtx(&realTotalBytes, "SELECT sum(total_bytes) FROM system.tables WHERE database='long_schema'"))
	} else {
		r.NoError(env.ch.SelectSingleRowNoCtx(&realTotalBytes, "SELECT sum(bytes_on_disk) FROM system.parts WHERE database='long_schema'"))
	}
	r.Greater(realTotalBytes, uint64(0))
	r.Greater(uint64(lastRemoteSize), realTotalBytes)

	out, err := env.DockerExecOut("clickhouse-backup", "curl", "-sL", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, fmt.Sprintf("clickhouse_backup_last_backup_size_remote %d", lastRemoteSize))

	log.Debug().Msg("Check /metrics clickhouse_backup_number_backups_*")
	r.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_local %d", apiBackupNumber))
	// +1 watch backup
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "list", "remote")
	r.Contains(out, fmt.Sprintf("clickhouse_backup_number_backups_remote %d", apiBackupNumber+1))
	r.Contains(out, "clickhouse_backup_number_backups_local_expected 0")
	r.Contains(out, "clickhouse_backup_number_backups_remote_expected 0")
}

func testAPIDeleteLocalDownloadRestore(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/delete/local/{name} + /backup/download/{name} + /backup/restore/{name}?rm=1")
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c",
		fmt.Sprintf("for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/delete/local/z_backup_$i\"; curl -sfL -XPOST \"http://localhost:7171/backup/download/z_backup_$i\"; sleep 2; curl -sfL -XPOST \"http://localhost:7171/backup/restore/z_backup_$i?rm=1\"; sleep 8; done", apiBackupNumber),
	)
	r.NoError(err, "%s\nunexpected POST /backup/delete/local error: %v", out, err)
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "error")

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "-sfL", "http://localhost:7171/backup/actions?filter=download")
	r.NoError(err, "%s\nunexpected GET /backup/actions?filter=download error: %v", out, err)
	r.NotContains(out, "\"status\":\"error\"")

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_delete_status 1")
	r.Contains(out, "clickhouse_backup_last_download_status 1")
	r.Contains(out, "clickhouse_backup_last_restore_status 1")
}

func testAPIBackupList(t *testing.T, r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/list")
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list'")
	r.NoError(err, "%s\nunexpected GET /backup/list error: %v", out, err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"regular\"}", i)), out))
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar, regular\"}", i)), out))
	}

	log.Debug().Msg("Check /backup/list/local")
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list/local'")
	r.NoError(err, "%s\nunexpected GET /backup/list/local error: %v", out, err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"regular\"}", i)), out))
		r.True(assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar, regular\"}", i)), out))
	}

	log.Debug().Msg("Check /backup/list/remote")
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sfL 'http://localhost:7171/backup/list/remote'")
	r.NoError(err, "%s\nunexpected GET /backup/list/remote error: %v", out, err)
	for i := 1; i <= apiBackupNumber; i++ {
		r.True(assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"local\",\"required\":\"\",\"desc\":\"regular\"}", i)), out))
		r.True(assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("{\"name\":\"z_backup_%d\",\"created\":\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\",\"size\":\\d+,\"location\":\"remote\",\"required\":\"\",\"desc\":\"tar, regular\"}", i)), out))
	}
}

func testAPIBackupUpload(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/upload")
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c",
		fmt.Sprintf("for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/upload/z_backup_$i\"; sleep 2; done", apiBackupNumber),
	)
	r.NoError(err, "%s\nunexpected POST /backup/upload error: %v", out, err)
	r.NotContains(out, "error")
	r.NotContains(out, "another operation is currently running")

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "-sfL", "http://localhost:7171/backup/actions?filter=upload")
	r.NoError(err, "%s\nunexpected GET /backup/actions?filter=upload error: %v", out, err)
	r.NotContains(out, "error")

	out, err = env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_upload_status 1")
}

func testAPIBackupTables(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/tables")
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables\"",
	)
	r.NoError(err, "%s\nunexpected GET /backup/tables error: %v", out, err)
	r.Contains(out, "long_schema")
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	r.NotContains(out, "system")
	r.NotContains(out, "INFORMATION_SCHEMA")
	r.NotContains(out, "information_schema")

	log.Debug().Msg("Check /backup/tables/all")
	out, err = env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables/all\"",
	)
	r.NoError(err, "%s\nunexpected GET /backup/tables/all error: %v", out, err)
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

func testAPIBackupTablesRemote(r *require.Assertions, env *TestEnvironment) {

	log.Debug().Msg("Check /backup/tables?remote_backup=z_backup_1")
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c", "curl -sfL \"http://localhost:7171/backup/tables?remote_backup=z_backup_1\"",
	)
	r.NoError(err, "%s\nunexpected GET /backup/tables?remote_backup=z_backup_1 error: %v", out, err)
	r.Contains(out, "long_schema")
	r.NotContains(out, "system")
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	r.NotContains(out, "INFORMATION_SCHEMA")
	r.NotContains(out, "information_schema")

}

func testAPIBackupVersion(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/version")
	cliVersion, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup --version 2>/dev/null --version | grep 'Version' | cut -d ':' -f 2 | xargs")
	r.NoError(err)
	apiVersion, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "curl -sL http://localhost:7171/backup/version | jq -r .version")
	r.NoError(err)
	r.Equal(cliVersion, apiVersion)
	tablesVersion, err := env.DockerExecOut("clickhouse", "bash", "-ce", "clickhouse client -q 'SELECT * FROM system.backup_version FORMAT TSVRaw'")
	r.NoError(err)
	r.Equal(cliVersion, tablesVersion)
}

func testAPIBackupCreate(r *require.Assertions, env *TestEnvironment) {
	log.Debug().Msg("Check /backup/create")
	out, err := env.DockerExecOut(
		"clickhouse-backup",
		"bash", "-xe", "-c",
		fmt.Sprintf("sleep 3; for i in {1..%d}; do date; curl -sfL -XPOST \"http://localhost:7171/backup/create?table=long_schema.*&name=z_backup_$i\"; sleep 1.5; done", apiBackupNumber),
	)
	r.NoError(err, "%s\nunexpected POST /backup/create?table=long_schema.*&name=z_backup_$i error: %v", out, err)
	r.NotContains(out, "Connection refused")
	r.NotContains(out, "another operation is currently running")
	r.NotContains(out, "\"status\":\"error\"")
	out, err = env.DockerExecOut("clickhouse-backup", "curl", "http://localhost:7171/metrics")
	r.NoError(err, "%s\nunexpected GET /metrics error: %v", out, err)
	r.Contains(out, "clickhouse_backup_last_create_status 1")
}

func fillDatabaseForAPIServer(maxTables int, minFields int, randFields int, ch *TestEnvironment, r *require.Assertions, fieldTypes []string) {
	log.Debug().Msgf("Create %d `long_schema`.`t%%d` tables with with %d..%d fields...", maxTables, minFields, minFields+randFields)
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
	log.Debug().Msg("...DONE")
}

func TestSkipNotExistsTable(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.1") < 0 {
		t.Skip("TestSkipNotExistsTable too small time between `SELECT DISTINCT partition_id` and `ALTER TABLE ... FREEZE PARTITION`")
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 0*time.Second, 1*time.Second, 1*time.Minute)

	log.Debug().Msg("Check skip not exist errors")
	env.queryWithNoError(r, "CREATE DATABASE freeze_not_exists")
	ifNotExistsCreateSQL := "CREATE TABLE IF NOT EXISTS freeze_not_exists.freeze_not_exists (id UInt64) ENGINE=MergeTree() ORDER BY id"
	ifNotExistsInsertSQL := "INSERT INTO freeze_not_exists.freeze_not_exists SELECT number FROM numbers(1000)"
	chVersion, err := env.ch.GetVersion(context.Background())
	r.NoError(err)

	freezeErrorHandled := false
	pauseChannel := make(chan int64)
	resumeChannel := make(chan int64)
	if os.Getenv("TEST_LOG_LEVEL") == "debug" {
		env.ch.Config.LogSQLQueries = true
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer func() {
			close(pauseChannel)
			wg.Done()
		}()
		pause := int64(0)
		// pausePercent := int64(90)
		for i := int64(0); i < 100; i++ {
			testBackupName := fmt.Sprintf("not_exists_%d", i)
			err = env.ch.Query(ifNotExistsCreateSQL)
			r.NoError(err)
			err = env.ch.Query(ifNotExistsInsertSQL)
			r.NoError(err)
			if i < 5 {
				log.Debug().Msgf("pauseChannel <- %d", 0)
				pauseChannel <- 0
			} else {
				log.Debug().Msgf("pauseChannel <- %d", pause/i)
				pauseChannel <- pause / i
			}
			startTime := time.Now()
			out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "LOG_LEVEL=debug CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create --table freeze_not_exists.freeze_not_exists "+testBackupName)
			log.Debug().Msg(out)
			if (err != nil && (strings.Contains(out, "can't freeze") || strings.Contains(out, "no tables for backup"))) ||
				(err == nil && !strings.Contains(out, "can't freeze")) {
				parseTime := func(line string) time.Time {
					parsedTime, err := time.Parse("2006-01-02 15:04:05.999", line[:23])
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
				if !strings.Contains(out, "no tables for backup") && !strings.Contains(out, "code: 473, message: Possible deadlock avoided") {
					assert.NoError(t, err)
				}
			}

			if strings.Contains(out, "code: 60") && err == nil {
				freezeErrorHandled = true
				log.Debug().Msg("CODE 60 catched")
				<-resumeChannel
				env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ec", "CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup delete local "+testBackupName)
				break
			}
			if err == nil {
				err = env.DockerExec("clickhouse-backup", "bash", "-ec", "CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup delete local "+testBackupName)
				assert.NoError(t, err)
			}
			<-resumeChannel
		}
	}()
	go func() {
		defer func() {
			close(resumeChannel)
			wg.Done()
		}()
		for pause := range pauseChannel {
			log.Debug().Msgf("%d <- pauseChannel", pause)
			if pause > 0 {
				pauseStart := time.Now()
				time.Sleep(time.Duration(pause) * time.Nanosecond)
				log.Debug().Msgf("pause=%s pauseStart=%s", time.Duration(pause).String(), pauseStart.String())
				err = env.ch.DropTable(clickhouse.Table{Database: "freeze_not_exists", Name: "freeze_not_exists"}, ifNotExistsCreateSQL, "", false, chVersion, "")
				r.NoError(err)
			}
			resumeChannel <- 1
		}
	}()
	wg.Wait()
	r.True(freezeErrorHandled, "freezeErrorHandled false")
	dropDbSQL := "DROP DATABASE IF EXISTS freeze_not_exists"
	if isAtomic, err := env.ch.IsAtomic("freeze_not_exists"); err == nil && isAtomic {
		dropDbSQL += " SYNC"
	}
	env.queryWithNoError(r, dropDbSQL)
	err = env.ch.Query(dropDbSQL)
	if err != nil {
		log.Error().Msgf("%s error: %v", dropDbSQL, err)
	}
	r.NoError(err)
	t.Log("TestSkipNotExistsTable DONE, ALL OK")
	env.Cleanup(t, r)
}

func TestSkipTablesAndSkipTableEngines(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 0*time.Second, 1*time.Second, 1*time.Minute)
	version, err := env.ch.GetVersion(context.Background())
	r.NoError(err)
	env.queryWithNoError(r, "CREATE DATABASE test_skip_tables")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_skip_tables.test_merge_tree (id UInt64, s String) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_skip_tables.test_memory (id UInt64) ENGINE=Memory")
	env.queryWithNoError(r, "CREATE MATERIALIZED VIEW IF NOT EXISTS test_skip_tables.test_mv (id UInt64) ENGINE=MergeTree() ORDER BY id AS SELECT * FROM test_skip_tables.test_merge_tree")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		query := "CREATE LIVE VIEW IF NOT EXISTS test_skip_tables.test_live_view AS SELECT count() FROM test_skip_tables.test_merge_tree"
		allowExperimentalAnalyzer, err := env.ch.TurnAnalyzerOffIfNecessary(version, query, "")
		r.NoError(err)
		env.queryWithNoError(r, query)
		r.NoError(env.ch.TurnAnalyzerOnIfNecessary(version, query, allowExperimentalAnalyzer))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		query := "CREATE WINDOW VIEW IF NOT EXISTS test_skip_tables.test_window_view ENGINE=MergeTree() ORDER BY s AS SELECT count(), s, tumbleStart(w_id) as w_start FROM test_skip_tables.test_merge_tree GROUP BY s, tumble(now(), INTERVAL '5' SECOND) AS w_id"
		allowExperimentalAnalyzer, err := env.ch.TurnAnalyzerOffIfNecessary(version, query, "")
		r.NoError(err)
		env.queryWithNoError(r, query)
		r.NoError(env.ch.TurnAnalyzerOnIfNecessary(version, query, allowExperimentalAnalyzer))
	}
	// create
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_TABLES=*.test_merge_tree clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create skip_table_pattern")
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/test_merge_tree.json"))
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/test_memory.json")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/test_mv.json")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ls -la /var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/test_live_view.json")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/test_window_view.json")
	}

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_TABLE_ENGINES=memory,materializedview,windowview,liveview clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create skip_engines")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/test_merge_tree.json")
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/test_memory.json"))
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/test_mv.json"))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ls -la /var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/test_live_view.json"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/test_window_view.json"))
	}

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_table_pattern")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_engines")

	//upload
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 CLICKHOUSE_SKIP_TABLES=*.test_memory clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload test_skip_full_backup")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json")
	r.Error(env.DockerExec("minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_memory.json"))
	env.DockerExecNoError(r, "minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_mv.json")
	env.DockerExecNoError(r, "minio", "bash", "-ce", "ls -la /bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		env.DockerExecNoError(r, "minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		env.DockerExecNoError(r, "minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json")
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 CLICKHOUSE_SKIP_TABLE_ENGINES=memory,materializedview,liveview,windowview clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload test_skip_full_backup")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json")
	r.Error(env.DockerExec("minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_memory.json"))
	r.Error(env.DockerExec("minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_mv.json"))
	env.DockerExecNoError(r, "minio", "bash", "-ce", "ls -la /bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		r.Error(env.DockerExec("minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		r.Error(env.DockerExec("minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json"))
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload test_skip_full_backup")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_memory.json")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_mv.json")
	env.DockerExecNoError(r, "minio", "bash", "-ce", "ls -la /bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		env.DockerExecNoError(r, "minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		env.DockerExecNoError(r, "minio", "ls", "-la", "/bitnami/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json")
	}

	//download
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 CLICKHOUSE_SKIP_TABLES=*.test_merge_tree clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download test_skip_full_backup")
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json"))
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_memory.json")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_mv.json")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ls -la /var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json")
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 CLICKHOUSE_SKIP_TABLE_ENGINES=memory,materializedview,liveview,windowview clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json")
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_memory.json"))
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_mv.json"))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ls -la /var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json"))
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_memory.json")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_mv.json")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ls -la /var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json")
	}

	//restore
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		env.queryWithNoError(r, "DROP DATABASE test_skip_tables NO DELAY")
	} else {
		env.queryWithNoError(r, "DROP DATABASE test_skip_tables")
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_TABLES=*.test_memory clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore test_skip_full_backup")
	result := uint64(0)
	r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM system.tables WHERE database='test_skip_tables' AND name!='test_memory'"))
	expectedTables := uint64(3)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		expectedTables = 4
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		expectedTables = 6
	}
	//*.inner.target.* for WINDOW VIEW created only after 22.6
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.6") >= 0 {
		expectedTables = 7
	}
	r.Equal(expectedTables, result)
	result = uint64(1)
	r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM system.tables WHERE database='test_skip_tables' AND name='test_memory'"))
	r.Equal(uint64(0), result)

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		env.queryWithNoError(r, "DROP DATABASE test_skip_tables NO DELAY")
	} else {
		env.queryWithNoError(r, "DROP DATABASE test_skip_tables")
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_TABLE_ENGINES=memory,materializedview,liveview,windowview clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore --schema test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_TABLE_ENGINES=memory,materializedview,liveview,windowview clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore --data test_skip_full_backup")
	result = uint64(0)
	expectedTables = uint64(2)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		expectedTables = 3
	}
	r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM system.tables WHERE database='test_skip_tables' AND engine='MergeTree'"))
	r.Equal(expectedTables, result)
	result = uint64(1)
	r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM system.tables WHERE database='test_skip_tables' AND engine IN ('Memory','MaterializedView','LiveView','WindowView')"))
	r.Equal(uint64(0), result)

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		env.queryWithNoError(r, "DROP DATABASE test_skip_tables NO DELAY")
	} else {
		env.queryWithNoError(r, "DROP DATABASE test_skip_tables")
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore test_skip_full_backup")
	result = uint64(0)
	r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM system.tables WHERE database='test_skip_tables'"))
	expectedTables = uint64(4)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		expectedTables = 5
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		expectedTables = 7
	}
	//*.inner.target.* for WINDOW VIEW created only after 22.6
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.6") >= 0 {
		expectedTables = 8
	}
	r.Equal(expectedTables, result)

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.1") >= 0 {
		env.queryWithNoError(r, "DROP DATABASE test_skip_tables NO DELAY")
	} else {
		env.queryWithNoError(r, "DROP DATABASE test_skip_tables")
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote test_skip_full_backup")
	env.Cleanup(t, r)
}

func TestTablePatterns(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	testBackupName := "test_backup_patterns"
	databaseList := []string{dbNameOrdinary, dbNameAtomic}
	var dbNameOrdinaryTest = dbNameOrdinary + "_" + t.Name()
	var dbNameAtomicTest = dbNameAtomic + "_" + t.Name()
	for _, createPattern := range []bool{true, false} {
		for _, restorePattern := range []bool{true, false} {
			fullCleanup(t, r, env, []string{testBackupName}, []string{"remote", "local"}, databaseList, false, false, "config-s3.yml")
			generateTestData(t, r, env, "S3", false, defaultTestData)
			if createPattern {
				env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create_remote", "--tables", " "+dbNameOrdinaryTest+".*", testBackupName)
				out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", "--tables", " "+dbNameOrdinaryTest+".*", testBackupName)
				r.NoError(err, "%s\nunexpected tables error: %v", out, err)
				r.Contains(out, dbNameOrdinaryTest)
				r.NotContains(out, dbNameAtomicTest)
				out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", "--remote-backup", testBackupName, "--tables", " "+dbNameOrdinaryTest+".*", testBackupName)
				r.NoError(err, "%s\nunexpected tables --remote-backup error: %v", out, err)
				r.Contains(out, dbNameOrdinaryTest)
				r.NotContains(out, dbNameAtomicTest)
			} else {
				env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create_remote", testBackupName)
				out, err := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", testBackupName)
				r.NoError(err, "%s\nunexpected tables error: %v", out, err)
				r.Contains(out, dbNameOrdinaryTest)
				r.Contains(out, dbNameAtomicTest)
				out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "tables", "--remote-backup", testBackupName, testBackupName)
				r.NoError(err, "%s\nunexpected tables --remote-backup error: %v", out, err)
				r.Contains(out, dbNameOrdinaryTest)
				r.Contains(out, dbNameAtomicTest)
			}

			env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", testBackupName)
			dropDatabasesFromTestDataDataSet(t, r, env, databaseList)

			if restorePattern {
				env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore_remote", "--tables", " "+dbNameOrdinaryTest+".*", testBackupName)
			} else {
				env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore_remote", testBackupName)
			}

			restored := uint64(0)
			r.NoError(env.ch.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameOrdinaryTest)))
			r.NotZero(restored)

			if createPattern || restorePattern {
				restored = 0
				r.NoError(env.ch.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameAtomicTest)))
				// todo, old versions of clickhouse will return empty recordset
				r.Zero(restored)

				restored = 0
				r.NoError(env.ch.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.databases WHERE name='%s'", dbNameAtomicTest)))
				// todo, old versions of clickhouse will return empty recordset
				r.Zero(restored)
			} else {
				restored = 0
				r.NoError(env.ch.SelectSingleRowNoCtx(&restored, fmt.Sprintf("SELECT count() FROM system.tables WHERE database='%s'", dbNameAtomicTest)))
				r.NotZero(restored)
			}

			fullCleanup(t, r, env, []string{testBackupName}, []string{"remote", "local"}, databaseList, true, true, "config-s3.yml")

		}
	}
	env.checkObjectStorageIsEmpty(t, r, "S3")
	env.Cleanup(t, r)
}

func TestProjections(t *testing.T) {
	var err error
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") == -1 {
		t.Skipf("Test skipped, PROJECTION available only 21.8+, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 0*time.Second, 1*time.Second, 1*time.Minute)

	r.NoError(env.DockerCP("config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	err = env.ch.Query("CREATE TABLE default.table_with_projection(dt DateTime, v UInt64, PROJECTION x (SELECT toStartOfMonth(dt) m, sum(v) GROUP BY m)) ENGINE=MergeTree() PARTITION BY toYYYYMMDD(dt) ORDER BY dt")
	r.NoError(err)

	env.queryWithNoError(r, "INSERT INTO default.table_with_projection SELECT today() - INTERVAL number DAY, number FROM numbers(5)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create_remote", "test_backup_projection_full")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_backup_projection_full")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore_remote", "--rm", "test_backup_projection_full")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_backup_projection_full")

	env.queryWithNoError(r, "INSERT INTO default.table_with_projection SELECT today() - INTERVAL number WEEK, number FROM numbers(5)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create_remote", "--diff-from-remote", "test_backup_projection_full", "test_backup_projection_increment")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_backup_projection_increment")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "restore_remote", "--rm", "test_backup_projection_increment")

	var counts uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&counts, "SELECT count() FROM default.table_with_projection"))
	r.Equal(uint64(10), counts)
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.9") >= 0 {
		counts = 0
		r.NoError(env.ch.SelectSingleRowNoCtx(&counts, "SELECT count() FROM system.parts WHERE database='default' AND table='table_with_projection' AND has(projections,'x')"))
		r.Equal(uint64(10), counts)
	}

	err = env.ch.Query("DROP TABLE default.table_with_projection NO DELAY")
	r.NoError(err)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", "test_backup_projection_increment")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "remote", "test_backup_projection_full")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_backup_projection_increment")
	env.Cleanup(t, r)
}

func TestCheckSystemPartsColumns(t *testing.T) {
	var err error
	var version int
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") == -1 {
		t.Skipf("Test skipped, system.parts_columns have inconsistency only in 23.3+, current version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 0*time.Second, 1*time.Second, 1*time.Minute)
	version, err = env.ch.GetVersion(context.Background())
	r.NoError(err)

	r.NoError(env.DockerCP("config-s3.yml", "clickhouse-backup:/etc/clickhouse-backup/config.yml"))
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+t.Name())

	// test compatible data types
	createSQL := "CREATE TABLE " + t.Name() + ".test_system_parts_columns(dt DateTime, v UInt64, e Enum('test' = 1)) ENGINE=MergeTree() ORDER BY tuple()"
	env.queryWithNoError(r, createSQL)
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_system_parts_columns SELECT today() - INTERVAL number DAY, number, 'test' FROM numbers(10)")

	env.queryWithNoError(r, "ALTER TABLE "+t.Name()+".test_system_parts_columns MODIFY COLUMN dt Nullable(DateTime('Europe/Moscow')), MODIFY COLUMN v Nullable(UInt64), MODIFY COLUMN e Enum16('test2'=1, 'test'=2)", t.Name())
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_system_parts_columns SELECT today() - INTERVAL number DAY, number, 'test2' FROM numbers(10)")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "create", "test_system_parts_columns")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "delete", "local", "test_system_parts_columns")

	r.NoError(env.ch.DropTable(clickhouse.Table{Database: t.Name(), Name: "test_system_parts_columns"}, createSQL, "", false, version, ""))

	// test incompatible data types
	env.queryWithNoError(r, "CREATE TABLE "+t.Name()+".test_system_parts_columns(dt Date, v String) ENGINE=MergeTree() PARTITION BY dt ORDER BY tuple()")
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_system_parts_columns SELECT today() - INTERVAL number DAY, if(number>0,'a',toString(number)) FROM numbers(2)")

	mutationSQL := "ALTER TABLE " + t.Name() + ".test_system_parts_columns MODIFY COLUMN v UInt64"
	err = env.ch.QueryContext(context.Background(), mutationSQL)
	if err != nil {
		errStr := strings.ToLower(err.Error())
		r.True(strings.Contains(errStr, "code: 341") || strings.Contains(errStr, "code: 517") || strings.Contains(errStr, "code: 524") || strings.Contains(errStr, "timeout"), "UNKNOWN ERROR: %s", err.Error())
		log.Debug().Msgf("%s RETURN EXPECTED ERROR=%#v", mutationSQL, err)
	}
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_system_parts_columns SELECT today() - INTERVAL number DAY, number FROM numbers(10)")
	r.Error(env.DockerExec("clickhouse-backup", "clickhouse-backup", "create", "test_system_parts_columns"))
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-lah", "/var/lib/clickhouse/backup/test_system_parts_columns"))
	r.Error(env.DockerExec("clickhouse-backup", "clickhouse-backup", "delete", "local", "test_system_parts_columns"))

	r.NoError(env.ch.DropTable(clickhouse.Table{Database: t.Name(), Name: "test_system_parts_columns"}, createSQL, "", false, version, ""))
	r.NoError(env.dropDatabase(t.Name()))
	env.Cleanup(t, r)
}

func TestKeepBackupRemoteAndDiffFromRemote(t *testing.T) {
	if isTestShouldSkip("RUN_ADVANCED_TESTS") {
		t.Skip("Skipping Advanced integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	backupNames := make([]string, 5)
	for i := 0; i < 5; i++ {
		backupNames[i] = fmt.Sprintf("keep_remote_backup_%d", i)
	}
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameMySQL, dbNamePostgreSQL, Issue331Atomic, Issue331Ordinary}
	fullCleanup(t, r, env, backupNames, []string{"remote", "local"}, databaseList, false, false, "config-s3.yml")
	incrementData := defaultIncrementData
	generateTestData(t, r, env, "S3", false, defaultTestData)
	for backupNumber, backupName := range backupNames {
		if backupNumber == 0 {
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create_remote %s", backupName))
		} else {
			incrementData = generateIncrementTestData(t, r, env, "S3", false, incrementData, backupNumber)
			env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", fmt.Sprintf("BACKUPS_TO_KEEP_REMOTE=3 CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml clickhouse-backup create_remote --diff-from-remote=%s %s", backupNames[backupNumber-1], backupName))
		}
	}
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list local")
	r.NoError(err, "%s\nunexpected list local error: %v", out, err)
	// shall not delete any backup, cause all deleted backups have links as required in other backups
	for _, backupName := range backupNames {
		r.Contains(out, backupName)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", backupName)
	}
	latestIncrementBackup := fmt.Sprintf("keep_remote_backup_%d", len(backupNames)-1)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", latestIncrementBackup)
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml list local")
	r.NoError(err, "%s\nunexpected list local error: %v", out, err)
	prevIncrementBackup := fmt.Sprintf("keep_remote_backup_%d", len(backupNames)-2)
	for _, backupName := range backupNames {
		if backupName == latestIncrementBackup {
			r.Contains(out, backupName)
		} else if backupName == prevIncrementBackup {
			r.Contains(out, "+"+backupName)
		} else {
			r.NotContains(out, backupName)
		}
	}
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", latestIncrementBackup)
	var res uint64
	r.NoError(env.ch.SelectSingleRowNoCtx(&res, fmt.Sprintf("SELECT count() FROM `%s_%s`.`%s_%s`", Issue331Atomic, t.Name(), Issue331Atomic, t.Name())))
	r.Equal(uint64(100+20*4), res)
	fullCleanup(t, r, env, []string{latestIncrementBackup}, []string{"local"}, nil, true, true, "config-s3.yml")
	fullCleanup(t, r, env, backupNames, []string{"remote"}, databaseList, true, true, "config-s3.yml")
	env.checkObjectStorageIsEmpty(t, r, "S3")
	env.Cleanup(t, r)
}

func TestSyncReplicaTimeout(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.11") == -1 {
		t.Skipf("Test skipped, SYNC REPLICA ignore receive_timeout for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 0*time.Millisecond, 1*time.Second, 1*time.Minute)

	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+t.Name())
	dropReplTables := func() {
		for _, table := range []string{"repl1", "repl2"} {
			query := "DROP TABLE IF EXISTS " + t.Name() + "." + table
			if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") == 1 {
				query += " NO DELAY"
			}
			env.queryWithNoError(r, query)
		}
	}
	dropReplTables()
	env.queryWithNoError(r, "CREATE TABLE "+t.Name()+".repl1 (v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/"+t.Name()+"/repl','repl1') ORDER BY tuple()")
	env.queryWithNoError(r, "CREATE TABLE "+t.Name()+".repl2 (v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/"+t.Name()+"/repl','repl2') ORDER BY tuple()")

	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".repl1 SELECT number FROM numbers(10)")

	env.queryWithNoError(r, "SYSTEM STOP REPLICATED SENDS "+t.Name()+".repl1")
	env.queryWithNoError(r, "SYSTEM STOP FETCHES "+t.Name()+".repl2")

	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".repl1 SELECT number FROM numbers(100)")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+t.Name()+".repl*", "test_not_synced_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "upload", "test_not_synced_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_not_synced_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", "test_not_synced_backup")

	env.queryWithNoError(r, "SYSTEM START REPLICATED SENDS "+t.Name()+".repl1")
	env.queryWithNoError(r, "SYSTEM START FETCHES "+t.Name()+".repl2")

	dropReplTables()
	r.NoError(env.dropDatabase(t.Name()))
	env.Cleanup(t, r)
}

func TestGetPartitionId(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.17") == -1 {
		t.Skipf("Test skipped, is_in_partition_key not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

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
	if isAtomic, _ := env.ch.IsAtomic("default"); !isAtomic {
		testCases[0].CreateTableSQL = strings.Replace(testCases[0].CreateTableSQL, "UUID 'b45e751f-6c06-42a3-ab4a-f5bb9ac3716e'", "", 1)
	}
	for _, tc := range testCases {
		partitionId, partitionName, err := partition.GetPartitionIdAndName(context.Background(), env.ch, tc.Database, tc.Table, tc.CreateTableSQL, tc.Partition)
		assert.NoError(t, err)
		assert.Equal(t, tc.ExpectedId, partitionId)
		assert.Equal(t, tc.ExpectedName, partitionName)
	}
	env.Cleanup(t, r)
}

func TestRestoreMutationInProgress(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 0*time.Second, 1*time.Second, 1*time.Minute)
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
	env.queryWithNoError(r, createDbSQL)
	version, err := env.ch.GetVersion(context.Background())
	r.NoError(err)

	createSQL := fmt.Sprintf("CREATE TABLE %s.test_restore_mutation_in_progress %s (id UInt64, attr String) ENGINE=ReplicatedMergeTree('%s','{replica}') PARTITION BY id ORDER BY id", t.Name(), onCluster, zkPath)
	env.queryWithNoError(r, createSQL)
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".test_restore_mutation_in_progress SELECT number, if(number>0,'a',toString(number)) FROM numbers(2)")

	mutationSQL := "ALTER TABLE " + t.Name() + ".test_restore_mutation_in_progress MODIFY COLUMN attr UInt64"
	err = env.ch.QueryContext(context.Background(), mutationSQL)
	if err != nil {
		errStr := strings.ToLower(err.Error())
		r.True(strings.Contains(errStr, "code: 341") || strings.Contains(errStr, "code: 517") || strings.Contains(errStr, "timeout"), "UNKNOWN ERROR: %s", err.Error())
		log.Debug().Msgf("%s RETURN EXPECTED ERROR=%#v", mutationSQL, err)
	}

	attrs := make([]struct {
		Attr uint64 `ch:"attr"`
	}, 0)
	err = env.ch.Select(&attrs, "SELECT attr FROM "+t.Name()+".test_restore_mutation_in_progress ORDER BY id")
	r.NotEqual(nil, err)
	errStr := strings.ToLower(err.Error())
	r.True(strings.Contains(errStr, "code: 53") || strings.Contains(errStr, "code: 6"))
	r.Zero(len(attrs))

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") >= 0 {
		mutationSQL = "ALTER TABLE " + t.Name() + ".test_restore_mutation_in_progress RENAME COLUMN attr TO attr_1"
		err = env.ch.QueryContext(context.Background(), mutationSQL)
		r.NotEqual(nil, err)
		errStr = strings.ToLower(err.Error())
		r.True(strings.Contains(errStr, "code: 517") || strings.Contains(errStr, "timeout"))
		log.Debug().Msgf("%s RETURN EXPECTED ERROR=%#v", mutationSQL, err)
	}
	env.DockerExecNoError(r, "clickhouse", "clickhouse", "client", "-q", "SELECT * FROM system.mutations WHERE is_done=0 FORMAT Vertical")

	// backup with check consistency
	out, createErr := env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "--tables="+t.Name()+".test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
	r.NotEqual(createErr, nil)
	r.Contains(out, "have inconsistent data types")
	log.Debug().Msg(out)

	// backup without check consistency
	out, createErr = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "create", "-c", "/etc/clickhouse-backup/config-s3.yml", "--skip-check-parts-columns", "--tables="+t.Name()+".test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
	log.Debug().Msg(out)
	r.NoError(createErr)
	r.NotContains(out, "have inconsistent data types")

	r.NoError(env.ch.DropTable(clickhouse.Table{Database: t.Name(), Name: "test_restore_mutation_in_progress"}, "", "", false, version, ""))
	var restoreErr error
	restoreErr = env.DockerExec("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "--rm", "--tables="+t.Name()+".test_restore_mutation_in_progress", "test_restore_mutation_in_progress")
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
	selectErr := env.ch.Select(&attrs, selectSQL)
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
		log.Debug().Msgf("%s RETURN EXPECTED ERROR=%#v", selectSQL, selectErr)
	} else {
		r.NoError(selectErr)
	}

	env.DockerExecNoError(r, "clickhouse", "clickhouse", "client", "-q", "SELECT * FROM system.mutations FORMAT Vertical")

	r.NoError(env.ch.DropTable(clickhouse.Table{Database: t.Name(), Name: "test_restore_mutation_in_progress"}, "", "", false, version, ""))
	r.NoError(env.dropDatabase(t.Name()))
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_restore_mutation_in_progress")
	env.Cleanup(t, r)
}

func TestInnerTablesMaterializedView(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 1*time.Second, 1*time.Second, 1*time.Minute)

	env.queryWithNoError(r, "CREATE DATABASE test_mv")
	env.queryWithNoError(r, "CREATE TABLE test_mv.src_table (v UInt64) ENGINE=MergeTree() ORDER BY v")
	env.queryWithNoError(r, "CREATE TABLE test_mv.dst_table (v UInt64) ENGINE=MergeTree() ORDER BY v")
	env.queryWithNoError(r, "CREATE MATERIALIZED VIEW test_mv.mv_with_inner (v UInt64) ENGINE=MergeTree() ORDER BY v AS SELECT v FROM test_mv.src_table")
	env.queryWithNoError(r, "CREATE MATERIALIZED VIEW test_mv.mv_with_dst TO test_mv.dst_table AS SELECT v FROM test_mv.src_table")
	env.queryWithNoError(r, "INSERT INTO test_mv.src_table SELECT number FROM numbers(100)")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "test_mv", "--tables=test_mv.mv_with*,test_mv.dst*")
	dropSQL := "DROP DATABASE test_mv"
	isAtomic, err := env.ch.IsAtomic("test_mv")
	r.NoError(err)
	if isAtomic {
		dropSQL += " NO DELAY"
	}
	env.queryWithNoError(r, dropSQL)
	var rowCnt uint64

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "test_mv", "--tables=test_mv.mv_with*,test_mv.dst*")
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCnt, "SELECT count() FROM test_mv.mv_with_inner"))
	r.Equal(uint64(100), rowCnt)
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCnt, "SELECT count() FROM test_mv.mv_with_dst"))
	r.Equal(uint64(100), rowCnt)

	r.NoError(env.dropDatabase("test_mv"))
	// https://github.com/Altinity/clickhouse-backup/issues/777
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "upload", "test_mv", "--delete-source", "--tables=test_mv.mv_with*,test_mv.dst*")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "download", "test_mv", "--tables=test_mv.mv_with*,test_mv.dst*")

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "test_mv", "--tables=test_mv.mv_with*,test_mv.dst*")
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCnt, "SELECT count() FROM test_mv.mv_with_inner"))
	r.Equal(uint64(100), rowCnt)
	r.NoError(env.ch.SelectSingleRowNoCtx(&rowCnt, "SELECT count() FROM test_mv.mv_with_dst"))
	r.Equal(uint64(100), rowCnt)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_mv")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "remote", "test_mv")
	env.Cleanup(t, r)
}

func TestFIPS(t *testing.T) {
	if os.Getenv("QA_AWS_ACCESS_KEY") == "" {
		t.Skip("QA_AWS_ACCESS_KEY is empty, TestFIPS will skip")
	}
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 1*time.Second, 1*time.Second, 1*time.Minute)
	fipsBackupName := fmt.Sprintf("fips_backup_%d", rand.Int())
	env.DockerExecNoError(r, "clickhouse", "rm", "-fv", "/etc/apt/sources.list.d/clickhouse.list")
	env.InstallDebIfNotExists(r, "clickhouse", "ca-certificates", "curl", "gettext-base", "bsdmainutils", "dnsutils", "git")
	env.DockerExecNoError(r, "clickhouse", "update-ca-certificates")
	r.NoError(env.DockerCP("config-s3-fips.yml", "clickhouse:/etc/clickhouse-backup/config.yml.fips-template"))
	env.DockerExecNoError(r, "clickhouse", "git", "clone", "--depth", "1", "--branch", "v3.2rc3", "https://github.com/drwetter/testssl.sh.git", "/opt/testssl")
	env.DockerExecNoError(r, "clickhouse", "chmod", "+x", "/opt/testssl/testssl.sh")

	generateCerts := func(certType, keyLength, curveType string) {
		env.DockerExecNoError(r, "clickhouse", "bash", "-xce", "openssl rand -out /root/.rnd 2048")
		switch certType {
		case "rsa":
			env.DockerExecNoError(r, "clickhouse", "bash", "-xce", fmt.Sprintf("openssl genrsa -out /etc/clickhouse-backup/ca-key.pem %s", keyLength))
			env.DockerExecNoError(r, "clickhouse", "bash", "-xce", fmt.Sprintf("openssl genrsa -out /etc/clickhouse-backup/server-key.pem %s", keyLength))
		case "ecdsa":
			env.DockerExecNoError(r, "clickhouse", "bash", "-xce", fmt.Sprintf("openssl ecparam -name %s -genkey -out /etc/clickhouse-backup/ca-key.pem", curveType))
			env.DockerExecNoError(r, "clickhouse", "bash", "-xce", fmt.Sprintf("openssl ecparam -name %s -genkey -out /etc/clickhouse-backup/server-key.pem", curveType))
		}
		env.DockerExecNoError(r, "clickhouse", "bash", "-xce", "openssl req -subj \"/O=altinity\" -x509 -new -nodes -key /etc/clickhouse-backup/ca-key.pem -sha256 -days 365000 -out /etc/clickhouse-backup/ca-cert.pem")
		env.DockerExecNoError(r, "clickhouse", "bash", "-xce", "openssl req -subj \"/CN=localhost\" -addext \"subjectAltName = DNS:localhost,DNS:*.cluster.local\" -new -key /etc/clickhouse-backup/server-key.pem -out /etc/clickhouse-backup/server-req.csr")
		env.DockerExecNoError(r, "clickhouse", "bash", "-xce", "openssl x509 -req -days 365000 -extensions SAN -extfile <(printf \"\\n[SAN]\\nsubjectAltName=DNS:localhost,DNS:*.cluster.local\") -in /etc/clickhouse-backup/server-req.csr -out /etc/clickhouse-backup/server-cert.pem -CA /etc/clickhouse-backup/ca-cert.pem -CAkey /etc/clickhouse-backup/ca-key.pem -CAcreateserial")
	}
	env.DockerExecNoError(r, "clickhouse", "bash", "-xec", "cat /etc/clickhouse-backup/config-s3-fips.yml.template | envsubst > /etc/clickhouse-backup/config-s3-fips.yml")

	generateCerts("rsa", "4096", "")
	env.queryWithNoError(r, "CREATE DATABASE "+t.Name())
	createSQL := "CREATE TABLE " + t.Name() + ".fips_table (v UInt64) ENGINE=MergeTree() ORDER BY tuple()"
	env.queryWithNoError(r, createSQL)
	env.queryWithNoError(r, "INSERT INTO "+t.Name()+".fips_table SELECT number FROM numbers(1000)")
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml create_remote --tables="+t.Name()+".fips_table "+fipsBackupName)
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml delete local "+fipsBackupName)
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml restore_remote --tables="+t.Name()+".fips_table "+fipsBackupName)
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml delete local "+fipsBackupName)
	env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml delete remote "+fipsBackupName)

	log.Debug().Msg("Run `clickhouse-backup-fips server` in background")
	env.DockerExecBackgroundNoError(r, "clickhouse", "bash", "-ce", "AWS_USE_FIPS_ENDPOINT=true clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml server &>>/tmp/clickhouse-backup-server-fips.log")
	time.Sleep(1 * time.Second)

	runClickHouseClientInsertSystemBackupActions(r, env, []string{fmt.Sprintf("create_remote --tables="+t.Name()+".fips_table %s", fipsBackupName)}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{fmt.Sprintf("delete local %s", fipsBackupName)}, false)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{fmt.Sprintf("restore_remote --tables="+t.Name()+".fips_table  %s", fipsBackupName)}, true)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{fmt.Sprintf("delete local %s", fipsBackupName)}, false)
	runClickHouseClientInsertSystemBackupActions(r, env, []string{fmt.Sprintf("delete remote %s", fipsBackupName)}, false)

	inProgressActions := make([]struct {
		Command string `ch:"command"`
		Status  string `ch:"status"`
	}, 0)
	r.NoError(env.ch.StructSelect(&inProgressActions,
		"SELECT command, status FROM system.backup_actions WHERE command LIKE ? AND status IN (?,?)",
		fmt.Sprintf("%%%s%%", fipsBackupName), status.InProgressStatus, status.ErrorStatus,
	))
	r.Equal(0, len(inProgressActions), "inProgressActions=%+v", inProgressActions)
	env.DockerExecNoError(r, "clickhouse", "pkill", "-n", "-f", "clickhouse-backup-fips")

	testTLSCerts := func(certType, keyLength, curveName string, cipherList ...string) {
		generateCerts(certType, keyLength, curveName)
		log.Debug().Msgf("Run `clickhouse-backup-fips server` in background for %s %s %s", certType, keyLength, curveName)
		env.DockerExecBackgroundNoError(r, "clickhouse", "bash", "-ce", "AWS_USE_FIPS_ENDPOINT=true clickhouse-backup-fips -c /etc/clickhouse-backup/config-s3-fips.yml server &>>/tmp/clickhouse-backup-server-fips.log")
		time.Sleep(1 * time.Second)

		env.DockerExecNoError(r, "clickhouse", "bash", "-ce", "rm -rf /tmp/testssl* && /opt/testssl/testssl.sh -e -s -oC /tmp/testssl.csv --color 0 --disable-rating --quiet -n min --mode parallel --add-ca /etc/clickhouse-backup/ca-cert.pem localhost:7172")
		env.DockerExecNoError(r, "clickhouse", "cat", "/tmp/testssl.csv")
		out, err := env.DockerExecOut("clickhouse", "bash", "-ce", fmt.Sprintf("grep -o -E '%s' /tmp/testssl.csv | uniq | wc -l", strings.Join(cipherList, "|")))
		r.NoError(err, "%s\nunexpected grep testssl.csv error: %v", out, err)
		r.Equal(strconv.Itoa(len(cipherList)), strings.Trim(out, " \t\r\n"))

		inProgressActions := make([]struct {
			Command string `ch:"command"`
			Status  string `ch:"status"`
		}, 0)
		r.NoError(env.ch.StructSelect(&inProgressActions,
			"SELECT command, status FROM system.backup_actions WHERE command LIKE ? AND status IN (?,?)",
			fmt.Sprintf("%%%s%%", fipsBackupName), status.InProgressStatus, status.ErrorStatus,
		))
		r.Equal(0, len(inProgressActions), "inProgressActions=%+v", inProgressActions)
		env.DockerExecNoError(r, "clickhouse", "pkill", "-n", "-f", "clickhouse-backup-fips")
	}
	// https://www.perplexity.ai/search/0920f1e8-59ec-4e14-b779-ba7b2e037196
	testTLSCerts("rsa", "4096", "", "ECDHE-RSA-AES128-GCM-SHA256", "ECDHE-RSA-AES256-GCM-SHA384", "AES_128_GCM_SHA256", "AES_256_GCM_SHA384")
	testTLSCerts("ecdsa", "", "prime256v1", "ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-ECDSA-AES256-GCM-SHA384")
	r.NoError(env.ch.DropTable(clickhouse.Table{Database: t.Name(), Name: "fips_table"}, createSQL, "", false, 0, ""))
	r.NoError(env.dropDatabase(t.Name()))
	env.Cleanup(t, r)
}

func TestRestoreMapping(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)

	checkRecordset := func(expectedRows int, expectedCount uint64, query string) {
		result := make([]struct {
			Count uint64 `ch:"count()"`
		}, 0)
		r.NoError(env.ch.Select(&result, query))
		r.Equal(expectedRows, len(result), "expect %d row", expectedRows)
		r.Equal(expectedCount, result[0].Count, "expect count=%d", expectedCount)
	}

	testBackupName := "test_restore_database_mapping"
	databaseList := []string{"database1", "database-2"}
	fullCleanup(t, r, env, []string{testBackupName}, []string{"local"}, databaseList, false, false, "config-database-mapping.yml")

	env.queryWithNoError(r, "CREATE DATABASE database1")
	env.queryWithNoError(r, "CREATE TABLE database1.t1 (dt DateTime, v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/database1/t1','{replica}') PARTITION BY toYYYYMM(dt) ORDER BY dt")
	env.queryWithNoError(r, "CREATE TABLE database1.d1 AS database1.t1 ENGINE=Distributed('{cluster}', 'database1', 't1')")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.3") < 0 {
		env.queryWithNoError(r, "CREATE TABLE database1.t2 AS database1.t1 ENGINE=ReplicatedMergeTree('/clickhouse/tables/database1/t2','{replica}') PARTITION BY toYYYYMM(dt) ORDER BY dt")
	} else {
		env.queryWithNoError(r, "CREATE TABLE database1.t2 AS database1.t1 ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','{replica}') PARTITION BY toYYYYMM(dt) ORDER BY dt")
	}
	env.queryWithNoError(r, "CREATE MATERIALIZED VIEW database1.mv1 TO database1.t2 AS SELECT * FROM database1.t1")
	env.queryWithNoError(r, "CREATE VIEW database1.v1 AS SELECT * FROM database1.t1")
	env.queryWithNoError(r, "INSERT INTO database1.t1 SELECT '2022-01-01 00:00:00', number FROM numbers(10)")

	log.Debug().Msg("Create backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "create", testBackupName)

	log.Debug().Msg("Restore schema")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--schema", "--rm", "--restore-database-mapping", "database1:database-2", "--restore-table-mapping", "t1:t3,t2:t4,d1:d2,mv1:mv2,v1:v2", "--tables", "database1.*", testBackupName)

	log.Debug().Msg("Check result database1")
	env.queryWithNoError(r, "INSERT INTO database1.t1 SELECT '2023-01-01 00:00:00', number FROM numbers(10)")
	checkRecordset(1, 20, "SELECT count() FROM database1.t1")
	checkRecordset(1, 20, "SELECT count() FROM database1.d1")
	checkRecordset(1, 20, "SELECT count() FROM database1.mv1")
	checkRecordset(1, 20, "SELECT count() FROM database1.v1")

	log.Debug().Msg("Drop database1")
	r.NoError(env.dropDatabase("database1"))

	log.Debug().Msg("Restore data")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-database-mapping.yml", "restore", "--data", "--restore-database-mapping", "database1:database-2", "--restore-table-mapping", "t1:t3,t2:t4,d1:d2,mv1:mv2,v1:v2", "--tables", "database1.*", testBackupName)

	log.Debug().Msg("Check result database-2")
	checkRecordset(1, 10, "SELECT count() FROM `database-2`.t3")
	checkRecordset(1, 10, "SELECT count() FROM `database-2`.d2")
	checkRecordset(1, 10, "SELECT count() FROM `database-2`.mv2")
	checkRecordset(1, 10, "SELECT count() FROM `database-2`.v2")

	log.Debug().Msg("Check database1 not exists")
	checkRecordset(1, 0, "SELECT count() FROM system.databases WHERE name='database1' SETTINGS empty_result_for_aggregation_by_empty_set=0")

	fullCleanup(t, r, env, []string{testBackupName}, []string{"local"}, databaseList, true, true, "config-database-mapping.yml")
	env.Cleanup(t, r)
}

func TestMySQLMaterialized(t *testing.T) {
	t.Skipf("Wait when fix DROP TABLE not supported by MaterializedMySQL, just attach will not help, https://github.com/ClickHouse/ClickHouse/issues/57543")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.12") == -1 {
		t.Skipf("MaterializedMySQL doens't support for clickhouse version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	env, r := NewTestEnvironment(t)
	env.DockerExecNoError(r, "mysql", "mysql", "-u", "root", "--password=root", "-v", "-e", "CREATE DATABASE ch_mysql_repl")
	env.connectWithWait(r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	engine := "MaterializedMySQL"
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.9") == -1 {
		engine = "MaterializeMySQL"
	}
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE ch_mysql_repl ENGINE=%s('mysql:3306','ch_mysql_repl','root','root')", engine))
	env.DockerExecNoError(r, "mysql", "mysql", "-u", "root", "--password=root", "-v", "-e", "CREATE TABLE ch_mysql_repl.t1 (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, s VARCHAR(255)); INSERT INTO ch_mysql_repl.t1(s) VALUES('s1'),('s2'),('s3')")
	time.Sleep(1 * time.Second)

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "test_mysql_materialized")
	env.queryWithNoError(r, "DROP DATABASE ch_mysql_repl")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "test_mysql_materialized")

	result := 0
	r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM ch_mysql_repl.t1"))
	r.Equal(3, result, "expect count=3")

	env.queryWithNoError(r, "DROP DATABASE ch_mysql_repl")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_mysql_materialized")
	env.Cleanup(t, r)
}

func TestPostgreSQLMaterialized(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.11") == -1 {
		t.Skipf("MaterializedPostgreSQL doens't support for clickhouse version %s", os.Getenv("CLICKHOUSE_VERSION"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.10") == -1 {
		t.Skipf("Serial type, support in 23.10+, look https://github.com/ClickHouse/ClickHouse/issues/44250")
	}
	t.Skip("FREEZE don't support for MaterializedPostgreSQL, https://github.com/ClickHouse/ClickHouse/issues/32902")

	env, r := NewTestEnvironment(t)
	env.DockerExecNoError(r, "pgsql", "bash", "-ce", "echo 'CREATE DATABASE ch_pgsql_repl' | PGPASSWORD=root psql -v ON_ERROR_STOP=1 -U root")
	env.DockerExecNoError(r, "pgsql", "bash", "-ce", "echo \"CREATE TABLE t1 (id BIGINT PRIMARY KEY, s VARCHAR(255)); INSERT INTO t1(id, s) VALUES(1,'s1'),(2,'s2'),(3,'s3')\" | PGPASSWORD=root psql -v ON_ERROR_STOP=1 -U root -d ch_pgsql_repl")
	env.connectWithWait(r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	env.queryWithNoError(r,
		"CREATE DATABASE ch_pgsql_repl ENGINE=MaterializedPostgreSQL('pgsql:5432','ch_pgsql_repl','root','root') "+
			"SETTINGS materialized_postgresql_schema = 'public'",
	)
	// time to initial snapshot
	count := uint64(0)
	for {
		err := env.ch.SelectSingleRowNoCtx(&count, "SELECT count() FROM system.tables WHERE database='ch_pgsql_repl'")
		r.NoError(err)
		if count > 0 {
			break
		}
		log.Debug().Msgf("ch_pgsql_repl contains %d tables, wait 5 seconds", count)
		time.Sleep(5 * time.Second)
	}

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "create", "test_pgsql_materialized")
	env.queryWithNoError(r, "DROP DATABASE ch_pgsql_repl")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "restore", "test_pgsql_materialized")

	result := 0
	r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM ch_pgsql_repl.t1"))
	r.Equal(3, result, "expect count=3")

	env.queryWithNoError(r, "DROP DATABASE ch_pgsql_repl")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_pgsql_materialized")
	env.Cleanup(t, r)
}

func (env *TestEnvironment) uploadSSHKeys(r *require.Assertions, container string) {
	r.NoError(env.DockerCP("sftp/clickhouse-backup_rsa", container+":/id_rsa"))
	env.DockerExecNoError(r, container, "cp", "-vf", "/id_rsa", "/tmp/id_rsa")
	env.DockerExecNoError(r, container, "chmod", "-v", "0600", "/tmp/id_rsa")

	r.NoError(env.DockerCP("sftp/clickhouse-backup_rsa.pub", "sshd:/root/.ssh/authorized_keys"))
	env.DockerExecNoError(r, "sshd", "chown", "-v", "root:root", "/root/.ssh/authorized_keys")
	env.DockerExecNoError(r, "sshd", "chmod", "-v", "0600", "/root/.ssh/authorized_keys")
}

func (env *TestEnvironment) runMainIntegrationScenario(t *testing.T, remoteStorageType, backupConfig string) {
	var out string
	var err error
	r := require.New(t)
	env.connectWithWait(r, 500*time.Millisecond, 1500*time.Millisecond, 3*time.Minute)
	// test for specified partitions backup
	testBackupSpecifiedPartitions(t, r, env, remoteStorageType, backupConfig)

	// main test scenario
	fullBackupName := fmt.Sprintf("%s_full_%d", t.Name(), rand.Int())
	incrementBackupName := fmt.Sprintf("%s_increment_%d", t.Name(), rand.Int())
	incrementBackupName2 := fmt.Sprintf("%s_increment2_%d", t.Name(), rand.Int())
	databaseList := []string{dbNameOrdinary, dbNameAtomic, dbNameMySQL, dbNamePostgreSQL, Issue331Atomic, Issue331Ordinary}
	tablesPattern := fmt.Sprintf("*_%s.*", t.Name())
	log.Debug().Msg("Clean before start")
	fullCleanup(t, r, env, []string{fullBackupName, incrementBackupName}, []string{"remote", "local"}, databaseList, false, false, backupConfig)
	createAllTypesOfObjectTables := !strings.Contains(remoteStorageType, "CUSTOM")
	testData := generateTestData(t, r, env, remoteStorageType, createAllTypesOfObjectTables, defaultTestData)

	log.Debug().Msg("Create backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--tables", tablesPattern, fullBackupName)

	incrementData := generateIncrementTestData(t, r, env, remoteStorageType, createAllTypesOfObjectTables, defaultIncrementData, 1)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--tables", tablesPattern, incrementBackupName)

	log.Debug().Msg("Upload full")
	uploadCmd := fmt.Sprintf("%s_COMPRESSION_FORMAT=zstd CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/%s clickhouse-backup upload --resume %s", remoteStorageType, backupConfig, fullBackupName)
	env.checkResumeAlreadyProcessed(uploadCmd, fullBackupName, "upload", r, remoteStorageType)

	// https://github.com/Altinity/clickhouse-backup/pull/900
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
		log.Debug().Msg("create --diff-from-remote backup")
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create", "--diff-from-remote", fullBackupName, "--tables", tablesPattern, incrementBackupName2)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "upload", incrementBackupName2)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "remote", incrementBackupName2)
		env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", incrementBackupName2)
	}

	log.Debug().Msg("Upload increment")
	uploadCmd = fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s upload %s --diff-from-remote %s --resume", backupConfig, incrementBackupName, fullBackupName)
	env.checkResumeAlreadyProcessed(uploadCmd, incrementBackupName, "upload", r, remoteStorageType)

	backupDir := "/var/lib/clickhouse/backup"
	if strings.HasPrefix(remoteStorageType, "EMBEDDED") && !strings.HasSuffix(remoteStorageType, "_URL") {
		backupDir = "/var/lib/clickhouse/disks/backups" + strings.ToLower(strings.TrimPrefix(remoteStorageType, "EMBEDDED"))
	}
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "ls -lha "+backupDir+" | grep "+t.Name())
	r.NoError(err)
	r.Equal(2, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect '2' backups exists in backup directory")
	log.Debug().Msg("Delete backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", incrementBackupName)
	out, err = env.DockerExecOut("clickhouse-backup", "bash", "-ce", "ls -lha "+backupDir+" | grep "+t.Name())
	r.NotNil(err)
	r.Equal("", strings.Trim(out, " \t\r\n"), "expect '0' backup exists in backup directory")

	dropDatabasesFromTestDataDataSet(t, r, env, databaseList)

	log.Debug().Msg("Download")
	replaceStorageDiskNameForReBalance(r, env, remoteStorageType, false)
	downloadCmd := fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s download --resume %s", backupConfig, fullBackupName)
	env.checkResumeAlreadyProcessed(downloadCmd, fullBackupName, "download", r, remoteStorageType)

	log.Debug().Msg("Restore schema")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--schema", fullBackupName)

	log.Debug().Msg("Restore data")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--data", fullBackupName)

	log.Debug().Msg("Full restore with rm")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--rm", fullBackupName)

	log.Debug().Msg("Check data")
	for i := range testData {
		if testData[i].CheckDatabaseOnly {
			r.NoError(env.checkDatabaseEngine(t, testData[i]))
		} else {
			if isTableSkip(env, testData[i], true) {
				continue
			}
			r.NoError(env.checkData(t, r, testData[i]))
		}
	}
	// test increment
	dropDatabasesFromTestDataDataSet(t, r, env, databaseList)

	log.Debug().Msg("Delete backup")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName)

	log.Debug().Msg("Download increment")
	downloadCmd = fmt.Sprintf("clickhouse-backup -c /etc/clickhouse-backup/%s download --resume %s", backupConfig, incrementBackupName)
	env.checkResumeAlreadyProcessed(downloadCmd, incrementBackupName, "download", r, remoteStorageType)

	log.Debug().Msg("Restore")
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "restore", "--schema", "--data", incrementBackupName)

	log.Debug().Msg("Check increment data")
	for i := range testData {
		testDataItem := testData[i]
		if isTableSkip(env, testDataItem, true) || testDataItem.IsDictionary {
			continue
		}
		for _, incrementDataItem := range incrementData {
			if testDataItem.Database == incrementDataItem.Database && testDataItem.Name == incrementDataItem.Name {
				testDataItem.Rows = append(testDataItem.Rows, incrementDataItem.Rows...)
			}
		}
		if testDataItem.CheckDatabaseOnly {
			r.NoError(env.checkDatabaseEngine(t, testDataItem))
		} else {
			r.NoError(env.checkData(t, r, testDataItem))
		}
	}

	// test end
	log.Debug().Msg("Clean after finish")
	// during download increment, partially downloaded full will also clean
	fullCleanup(t, r, env, []string{incrementBackupName}, []string{"local"}, nil, true, false, backupConfig)
	fullCleanup(t, r, env, []string{fullBackupName, incrementBackupName}, []string{"remote"}, databaseList, true, true, backupConfig)
	replaceStorageDiskNameForReBalance(r, env, remoteStorageType, true)
	env.checkObjectStorageIsEmpty(t, r, remoteStorageType)
}

func (env *TestEnvironment) checkObjectStorageIsEmpty(t *testing.T, r *require.Assertions, remoteStorageType string) {
	if remoteStorageType == "AZBLOB" || remoteStorageType == "AZBLOB_EMBEDDED_URL" {
		t.Log("wait when resolve https://github.com/Azure/Azurite/issues/2362, todo try to use mysql as azurite storage")
		/*
			env.DockerExecNoError(r, "azure", "apk", "add", "jq")
			checkBlobCollection := func(containerName string, expected string) {
				out, err := env.DockerExecOut("azure", "sh", "-c", "jq '.collections[] | select(.name == \"$BLOBS_COLLECTION$\") | .data[] | select(.containerName == \""+containerName+"\") | .name' /data/__azurite_db_blob__.json")
				r.NoError(err)
				actual := strings.Trim(out, "\n\r\t ")
				if expected != actual {
					env.DockerExecNoError(r, "azure", "sh", "-c", "cat /data/__azurite_db_blob__.json | jq")
					env.DockerExecNoError(r, "azure", "sh", "-c", "stat -c '%y' /data/__azurite_db_blob__.json")
					env.DockerExecNoError(r, "azure", "sh", "-c", "cat /data/debug.log")
				}
				r.Equal(expected, actual)
			}
			// docker run --network=integration_clickhouse-backup -it --rm mcr.microsoft.com/azure-cli:latest
			// export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azure:10000/devstoreaccount1;"
			// az storage blob list --container-name azure-disk
			// az storage blob delete-batch --source azure-disk
			// az storage blob list --container-name azure-disk
			time.Sleep(15 * time.Second)
			checkBlobCollection("azure-disk", "")
			checkBlobCollection("container1", "")
			checkBlobCollection("azure-backup-disk", "")
		*/
	}
	checkRemoteDir := func(expected string, container string, cmd ...string) {
		out, err := env.DockerExecOut(container, cmd...)
		r.NoError(err, "%s\nunexpected checkRemoteDir error: %v", out, err)
		r.Equal(expected, strings.Trim(out, "\r\n\t "))
	}
	if remoteStorageType == "S3" || remoteStorageType == "S3_EMBEDDED_URL" {
		checkRemoteDir("total 0", "minio", "bash", "-c", "ls -lh /bitnami/minio/data/clickhouse/")
	}
	if remoteStorageType == "SFTP" {
		checkRemoteDir("total 0", "sshd", "bash", "-c", "ls -lh /root/")
	}
	if remoteStorageType == "FTP" {
		if strings.Contains(os.Getenv("COMPOSE_FILE"), "advanced") {
			checkRemoteDir("total 0", "ftp", "bash", "-c", "ls -lh /home/ftpusers/test_backup/backup/")
		} else {
			checkRemoteDir("total 0", "ftp", "bash", "-c", "ls -lh /home/vsftpd/test_backup/backup/")
		}
	}
	if remoteStorageType == "GCS_EMULATOR" {
		checkRemoteDir("total 0", "gcs", "sh", "-c", "ls -lh /data/altinity-qa-test/")
	}
}

func replaceStorageDiskNameForReBalance(r *require.Assertions, env *TestEnvironment, remoteStorageType string, isRebalanced bool) {
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
	r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker", append(env.GetDefaultComposeCommand(), "restart", "clickhouse")...))
	env.connectWithWait(r, 3*time.Second, 1500*time.Millisecond, 3*time.Minute)
}

func testBackupSpecifiedPartitions(t *testing.T, r *require.Assertions, env *TestEnvironment, remoteStorageType string, backupConfig string) {
	log.Debug().Msg("testBackupSpecifiedPartitions started")
	var err error
	var out string
	var result, expectedCount uint64

	partitionBackupName := fmt.Sprintf("partition_backup_%d", rand.Int())
	fullBackupName := fmt.Sprintf("full_backup_%d", rand.Int())
	dbName := "test_partitions_" + t.Name()
	// Create and fill tables
	env.queryWithNoError(r, "CREATE DATABASE IF NOT EXISTS "+dbName)
	env.queryWithNoError(r, "DROP TABLE IF EXISTS "+dbName+".t1")
	env.queryWithNoError(r, "DROP TABLE IF EXISTS "+dbName+".t2")
	env.queryWithNoError(r, "CREATE TABLE "+dbName+".t1 (dt Date, category Int64, v UInt64) ENGINE=MergeTree() PARTITION BY (category, toYYYYMMDD(dt)) ORDER BY dt")
	env.queryWithNoError(r, "CREATE TABLE "+dbName+".t2 (dt String, category Int64, v UInt64) ENGINE=MergeTree() PARTITION BY (category, dt) ORDER BY dt")
	for _, dt := range []string{"2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04"} {
		env.queryWithNoError(r, fmt.Sprintf("INSERT INTO "+dbName+".t1(dt, v) SELECT '%s', number FROM numbers(10)", dt))
		env.queryWithNoError(r, fmt.Sprintf("INSERT INTO "+dbName+".t2(dt, v) SELECT '%s', number FROM numbers(10)", dt))
	}

	// check create_remote full > download + partitions > restore --data --partitions > delete local > download > restore --partitions > restore
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "create_remote", "--tables="+dbName+".t*", fullBackupName)
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
	// we just replace data in exists table
	checkRestoredDataWithPartitions(80)

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

	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "remote", fullBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", fullBackupName)

	// check create + partitions
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
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", partitionBackupName)

	// check create > upload + partitions
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

	// Check partial restored t1
	result = 0
	r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM "+dbName+".t1"))

	expectedCount = 20
	// custom and embedded doesn't support --partitions in upload and download
	if remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		expectedCount = 40
	}
	r.Equal(expectedCount, result, fmt.Sprintf("expect count=%d", expectedCount))

	// Check only selected partitions restored
	result = 0
	r.NoError(env.ch.SelectSingleRowNoCtx(&result, "SELECT count() FROM "+dbName+".t1 WHERE dt NOT IN ('2022-01-02','2022-01-03')"))
	expectedCount = 0
	// custom and embedded doesn't support --partitions in upload and download
	if remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		expectedCount = 20
	}
	r.Equal(expectedCount, result, "expect count=0")

	// DELETE backup.
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "remote", partitionBackupName)
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/"+backupConfig, "delete", "local", partitionBackupName)

	if err = env.dropDatabase(dbName); err != nil {
		t.Fatal(err)
	}
	log.Debug().Msg("testBackupSpecifiedPartitions finish")
}

func (env *TestEnvironment) checkResumeAlreadyProcessed(backupCmd, testBackupName, resumeKind string, r *require.Assertions, remoteStorageType string) {
	// backupCmd = fmt.Sprintf("%s & PID=$!; sleep 0.7; kill -9 $PID; cat /var/lib/clickhouse/backup/%s/upload.state; sleep 0.3; %s", backupCmd, testBackupName, backupCmd)
	if remoteStorageType == "CUSTOM" || strings.HasPrefix(remoteStorageType, "EMBEDDED") {
		backupCmd = strings.Replace(backupCmd, "--resume", "", 1)
	} else {
		backupCmd = fmt.Sprintf("%s; cat /var/lib/clickhouse/backup/%s/%s.state; %s", backupCmd, testBackupName, resumeKind, backupCmd)
	}
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-xce", backupCmd)
	r.NoError(err, "%s\nunexpected checkResumeAlreadyProcessed error: %v", out, err)
	if strings.Contains(backupCmd, "--resume") {
		r.Contains(out, "already processed")
	}
}

func fullCleanup(t *testing.T, r *require.Assertions, env *TestEnvironment, backupNames, backupTypes, databaseList []string, checkDeleteErr, checkDropErr bool, backupConfig string) {
	for _, backupName := range backupNames {
		for _, backupType := range backupTypes {
			out, err := env.DockerExecOut("clickhouse-backup", "bash", "-xce", "clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" delete "+backupType+" "+backupName)
			if checkDeleteErr {
				r.NoError(err, "checkDeleteErr delete %s %s output: \n%s\nerror: %v", backupType, backupName, out, err)
			}
		}
	}
	otherBackupList, lsErr := env.DockerExecOut("clickhouse", "ls", "-1", "/var/lib/clickhouse/backup/*"+t.Name()+"*")
	if lsErr == nil {
		for _, backupName := range strings.Split(otherBackupList, "\n") {
			if backupName != "" {
				out, err := env.DockerExecOut("clickhouse-backup", "bash", "-xce", "clickhouse-backup -c /etc/clickhouse-backup/"+backupConfig+" delete local "+backupName)
				if checkDeleteErr {
					r.NoError(err, "%s\nunexpected delete local %s output: \n%s\nerror: %v, ", backupName, out, err)
				}
			}
		}
	}

	dropDatabasesFromTestDataDataSet(t, r, env, databaseList)
}

func generateTestData(t *testing.T, r *require.Assertions, env *TestEnvironment, remoteStorageType string, createAllTypesOfObjectTables bool, testData []TestDataStruct) []TestDataStruct {
	log.Debug().Msgf("Generate test data %s with _%s suffix", remoteStorageType, t.Name())
	testData = generateTestDataWithDifferentStoragePolicy(remoteStorageType, createAllTypesOfObjectTables, 0, 5, testData)
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

func generateTestDataWithDifferentStoragePolicy(remoteStorageType string, createAllTypesOfObjectTables bool, offset, rowsCount int, testData []TestDataStruct) []TestDataStruct {
	log.Debug().Msgf("generateTestDataWithDifferentStoragePolicy remoteStorageType=%s", remoteStorageType)
	for databaseName, databaseEngine := range map[string]string{dbNameOrdinary: "Ordinary", dbNameAtomic: "Atomic"} {
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
		addTestDataIfNotExists := func() {
			found := false
			for i, data := range testData {
				if data.Name == testDataWithStoragePolicy.Name && data.Database == testDataWithStoragePolicy.Database {
					found = true
					testData[i].Rows = testDataWithStoragePolicy.Rows
					break
				}
			}
			if !found {
				testData = append(testData, testDataWithStoragePolicy)
			}
		}
		//encrypted disks support after 21.10
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.10") >= 0 {
			testDataWithStoragePolicy.Name = "test_hdd3_encrypted"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') ORDER BY id PARTITION BY id  SETTINGS storage_policy = 'hdd3_only_encrypted'"
			addTestDataIfNotExists()
		}
		//s3 disks support after 21.8
		if (createAllTypesOfObjectTables || strings.Contains(remoteStorageType, "S3")) && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.8") >= 0 {
			testDataWithStoragePolicy.Name = "test_s3"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') ORDER BY id PARTITION BY id SETTINGS storage_policy = 's3_only'"
			addTestDataIfNotExists()
		}
		//encrypted s3 disks support after 21.12
		if (createAllTypesOfObjectTables || strings.Contains(remoteStorageType, "S3")) && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
			testDataWithStoragePolicy.Name = "test_s3_encrypted"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=MergeTree ORDER BY id PARTITION BY id SETTINGS storage_policy = 's3_only_encrypted'"
			// @todo wait when fix https://github.com/ClickHouse/ClickHouse/issues/58247
			//if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.12") >= 0 {
			//	testDataWithStoragePolicy.Schema = "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') ORDER BY id PARTITION BY id SETTINGS storage_policy = 's3_only_encrypted'"
			//}
			addTestDataIfNotExists()
		}
		//gcs over s3 support added in 22.6
		if (createAllTypesOfObjectTables || strings.Contains(remoteStorageType, "GCS")) && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.6") >= 0 && os.Getenv("QA_GCS_OVER_S3_BUCKET") != "" {
			testDataWithStoragePolicy.Name = "test_gcs"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') ORDER BY id PARTITION BY id SETTINGS storage_policy = 'gcs_only'"
			addTestDataIfNotExists()
		}
		//check azure_blob_storage only in 23.3+ (added in 22.1)
		if (createAllTypesOfObjectTables || strings.Contains(remoteStorageType, "AZBLOB")) && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "23.3") >= 0 {
			testDataWithStoragePolicy.Name = "test_azure"
			testDataWithStoragePolicy.Schema = "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/{table}','{replica}') ORDER BY id PARTITION BY id SETTINGS storage_policy = 'azure_only'"
			addTestDataIfNotExists()
		}
	}
	return testData
}

func generateIncrementTestData(t *testing.T, r *require.Assertions, ch *TestEnvironment, remoteStorageType string, createObjectTables bool, incrementData []TestDataStruct, incrementNumber int) []TestDataStruct {
	log.Debug().Msgf("Generate increment test data for %s", remoteStorageType)
	incrementData = generateTestDataWithDifferentStoragePolicy(remoteStorageType, createObjectTables, 5*incrementNumber, 5, incrementData)
	for _, data := range incrementData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestData(t, data))
	}
	return incrementData
}

func dropDatabasesFromTestDataDataSet(t *testing.T, r *require.Assertions, ch *TestEnvironment, databaseList []string) {
	log.Debug().Msg("Drop all databases")
	for _, db := range databaseList {
		db = db + "_" + t.Name()
		r.NoError(ch.dropDatabase(db))
	}
}

func (env *TestEnvironment) connectWithWait(r *require.Assertions, sleepBefore, pollInterval, timeOut time.Duration) {
	time.Sleep(sleepBefore)
	for i := 1; i < 11; i++ {
		err := env.connect(timeOut.String())
		if i == 10 {
			r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker", append(env.GetDefaultComposeCommand(), "logs", "clickhouse")...))
			out, dockerErr := env.DockerExecOut("clickhouse", "clickhouse", "client", "--echo", "-q", "'SELECT version()'")
			r.NoError(dockerErr)
			log.Debug().Msg(out)
			r.NoError(err)
		}
		if err != nil {
			r.NoError(utils.ExecCmd(context.Background(), 180*time.Second, "docker", "ps", "-a"))
			if out, dockerErr := env.DockerExecOut("clickhouse", "clickhouse", "client", "--echo", "-q", "SELECT version()"); dockerErr == nil {
				log.Debug().Msg(out)
			} else {
				log.Info().Msg(out)
			}
			log.Warn().Msgf("clickhouse not ready %v, wait %v seconds", err, (pollInterval).Seconds())
			time.Sleep(pollInterval)
		} else {
			if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") > 0 {
				var count uint64
				err = env.ch.SelectSingleRowNoCtx(&count, "SELECT count() FROM mysql('mysql:3306','mysql','user','root','root')")
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

func (env *TestEnvironment) connect(timeOut string) error {
	portOut, err := utils.ExecCmdOut(context.Background(), 10*time.Second, "docker", append(env.GetDefaultComposeCommand(), "port", "clickhouse", "9000")...)
	if err != nil {
		log.Error().Msg(portOut)
		log.Fatal().Msgf("can't get port for clickhouse: %v", err)
	}
	hostAndPort := strings.Split(strings.Trim(portOut, " \r\n\t"), ":")
	if len(hostAndPort) < 1 {
		log.Error().Msg(portOut)
		log.Fatal().Msgf("invalid port for clickhouse: %v", err)
	}
	port, err := strconv.Atoi(hostAndPort[1])
	if err != nil {
		return err
	}
	env.ch = &clickhouse.ClickHouse{
		Config: &config.ClickHouseConfig{
			Host:    hostAndPort[0],
			Port:    uint(port),
			Timeout: timeOut,
		},
	}
	for i := 0; i < 3; i++ {
		err = env.ch.Connect()
		if err == nil {
			return nil
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}
	return err
}

var mergeTreeOldSyntax = regexp.MustCompile(`(?m)MergeTree\(([^,]+),([\w\s,)(]+),(\s*\d+\s*)\)`)

func (env *TestEnvironment) createTestSchema(t *testing.T, data TestDataStruct, remoteStorageType string) error {
	origDatabase := data.Database
	origName := data.Name
	if !data.IsFunction {
		data.Database = data.Database + "_" + t.Name()
		data.Name = data.Name + "_" + t.Name()
		// 20.8 doesn't respect DROP TABLE ... NO DELAY, so Atomic works but --rm is not applicable
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") > 0 {
			if err := env.ch.CreateDatabaseWithEngine(data.Database, data.DatabaseEngine, "cluster"); err != nil {
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

	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.0") == 1 && !data.IsFunction {
		createSQL += " ON CLUSTER 'cluster' "
	}
	createSQL += data.Schema
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
		false, false, "", 0, "/var/lib/clickhouse",
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
	batch, err := env.ch.GetConn().PrepareBatch(context.Background(), insertSQL)

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

func (env *TestEnvironment) dropDatabase(database string) (err error) {
	var isAtomic bool
	dropDatabaseSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", database)
	if isAtomic, err = env.ch.IsAtomic(database); isAtomic {
		dropDatabaseSQL += " SYNC"
	} else if err != nil {
		return err
	}
	return env.ch.Query(dropDatabaseSQL)
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
	rows, err := env.ch.GetConn().Query(context.Background(), selectSQL)
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

func (env *TestEnvironment) queryWithNoError(r *require.Assertions, query string, args ...interface{}) {
	err := env.ch.Query(query, args...)
	if err != nil {
		log.Error().Err(err).Msgf("queryWithNoError(%s) error", query)
	}
	r.NoError(err)
}

var dockerExecTimeout = 600 * time.Second

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

func (env *TestEnvironment) DockerCP(src, dst string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	dcmd := append(env.GetDefaultComposeCommand(), "cp", src, dst)

	log.Debug().Msgf("docker %s", strings.Join(dcmd, " "))
	out, err := exec.CommandContext(ctx, "docker", dcmd...).CombinedOutput()
	log.Debug().Msgf(string(out))
	cancel()
	return err
}

func (env *TestEnvironment) InstallDebIfNotExists(r *require.Assertions, container string, pkgs ...string) {
	err := env.DockerExec(
		container,
		"bash", "-xec",
		fmt.Sprintf(
			"export DEBIAN_FRONTEND=noniteractive; if [[ '%d' != $(dpkg -l | grep -c -E \"%s\" ) ]]; then rm -fv /etc/apt/sources.list.d/clickhouse.list; find /etc/apt/ -type f -name *.list -exec sed -i 's/ru.archive.ubuntu.com/archive.ubuntu.com/g' {} +; apt-get -y update; apt-get install --no-install-recommends -y %s; fi",
			len(pkgs), "^ii\\s+"+strings.Join(pkgs, "|^ii\\s+"), strings.Join(pkgs, " "),
		),
	)
	r.NoError(err)
}

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
	return os.Getenv("COMPOSE_FILE") == "docker-compose.yml" && (strings.Contains(data.Name, "jbod#$_table") || data.IsDictionary)
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
