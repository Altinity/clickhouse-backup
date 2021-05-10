// +build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/apex/log"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dbName = "_test.ДБ_"

type TestDataStruct struct {
	Database           string
	Table              string
	Schema             string
	Rows               []map[string]interface{}
	Fields             []string
	OrderBy            string
	IsMaterializedView bool
	IsDictionary       bool
	SkipInsert         bool
}

var testData = []TestDataStruct{
	{
		Database: dbName,
		Table:    ".inner.table1",
		Schema:   "(Date Date, TimeStamp DateTime, Log String) ENGINE = MergeTree(Date, (TimeStamp, Log), 8192)",
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
		Database: dbName,
		Table:    "2. Таблица №2",
		Schema:   "(id UInt64, User String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
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
		Database: dbName,
		Table:    "-table-3-",
		Schema:   "(TimeStamp DateTime, Item String, Date Date MATERIALIZED toDate(TimeStamp)) ENGINE = MergeTree() PARTITION BY Date ORDER BY TimeStamp SETTINGS index_granularity = 8192",
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
		Database: dbName,
		Table:    "table4",
		Schema:   "(id UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) ENGINE = MergeTree PARTITION BY id ORDER BY (id, Col1, Col2, Col3, Col4, Col5) SETTINGS index_granularity = 8192",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{"id", "Col1", "Col2", "Col3", "Col4", "Col5"},
		OrderBy: "id",
	}, {
		Database: dbName,
		Table:    "yuzhichang_table2",
		Schema:   "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMM(order_time) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{"order_id": "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbName,
		Table:    "yuzhichang_table3",
		Schema:   "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(order_time) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{"order_id": "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbName,
		Table:    "yuzhichang_table4",
		Schema:   "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{"order_id": "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbName,
		Table:    "jbod",
		Schema:   "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'jbod'",
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
		Database: dbName,
		Table:    "mv_src_table",
		Schema:   "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','replica1') ORDER BY id",
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
		Database:   dbName,
		Table:      "mv_dst_table",
		Schema:     "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','replica1') ORDER BY id",
		SkipInsert: true,
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
		Database:           dbName,
		IsMaterializedView: true,
		Table:              "mv_max_with_inner",
		Schema:             fmt.Sprintf("(id UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','replica1') ORDER BY id AS SELECT max(id) AS id FROM `%s`.`mv_src_table`", dbName),
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
		Database:           dbName,
		IsMaterializedView: true,
		Table:              "mv_max_with_dst",
		Schema:             fmt.Sprintf(" TO `%s`.`mv_dst_table` AS SELECT max(id) AS id FROM `%s`.mv_src_table", dbName, dbName),
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
		Database:           dbName,
		IsMaterializedView: true,
		Table:              "mv_min_with_nested_depencency",
		Schema:             fmt.Sprintf(" TO `%s`.`mv_dst_table` AS SELECT min(id) * 2 AS id FROM `%s`.mv_src_table", dbName, dbName),
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
		Database:     dbName,
		IsDictionary: true,
		Table:        "dict_example",
		Schema: fmt.Sprintf(
			" (id UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) PRIMARY KEY id "+
				" SOURCE(CLICKHOUSE(host 'localhost' port 9000 database '%s' table 'table4' user 'default' password ''))"+
				" LAYOUT(HASHED()) LIFETIME(60)",
			dbName),
		SkipInsert: true,
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
}

var incrementData = []TestDataStruct{
	{
		Database: dbName,
		Table:    ".inner.table1",
		Schema:   "(Date Date, TimeStamp DateTime, Log String) ENGINE = MergeTree(Date, (TimeStamp, Log), 8192)",
		Rows: []map[string]interface{}{
			{"Date": toDate("2019-10-26"), "TimeStamp": toTS("2019-01-26 07:37:19"), "Log": "Seven"},
		},
		Fields:  []string{"Date", "TimeStamp", "Log"},
		OrderBy: "TimeStamp",
	}, {
		Database: dbName,
		Table:    "2. Таблица №2",
		Schema:   "(id UInt64, User String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"id": uint64(7), "User": "Alice"},
			{"id": uint64(8), "User": "Bob"},
			{"id": uint64(9), "User": "John"},
			{"id": uint64(10), "User": "Frank"},
		},
		Fields:  []string{"id", "User"},
		OrderBy: "id",
	}, {
		Database: dbName,
		Table:    "-table-3-",
		Schema:   "(TimeStamp DateTime, Item String, Date Date MATERIALIZED toDate(TimeStamp)) ENGINE = MergeTree() PARTITION BY Date ORDER BY TimeStamp SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"TimeStamp": toTS("2019-01-26 07:37:18"), "Item": "Seven"},
			{"TimeStamp": toTS("2019-01-27 07:37:19"), "Item": "Eight"},
		},
		Fields:  []string{"TimeStamp", "Item"},
		OrderBy: "TimeStamp",
	}, {
		Database: dbName,
		Table:    "table4",
		Schema:   "(id UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) ENGINE = MergeTree PARTITION BY id ORDER BY (id, Col1, Col2, Col3, Col4, Col5) SETTINGS index_granularity = 8192",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 200; i < 220; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{"id", "Col1", "Col2", "Col3", "Col4", "Col5"},
		OrderBy: "id",
	}, {
		Database: dbName,
		Table:    "yuzhichang_table2",
		Schema:   "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMM(order_time) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{"order_id": "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbName,
		Table:    "yuzhichang_table3",
		Schema:   "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(order_time) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{"order_id": "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbName,
		Table:    "yuzhichang_table4",
		Schema:   "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{"order_id": "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbName,
		Table:    "jbod",
		Schema:   "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'jbod'",
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
	logLevel := "info"
	if os.Getenv("LOG_LEVEL") != "" {
		logLevel = os.Getenv("LOG_LEVEL")
	}
	log.SetLevelFromString(logLevel)
}

func TestIntegrationS3(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-s3.yml", "/etc/clickhouse-backup/config.yml"))
	// testRestoreLegacyBackupFormat(t)
	testCommon(t)
}

func TestIntegrationGCS(t *testing.T) {
	if os.Getenv("GCS_TESTS") == "" || os.Getenv("TRAVIS_PULL_REQUEST") != "false" {
		t.Skip("Skipping GCS integration tests...")
		return
	}
	r := require.New(t)
	r.NoError(dockerCP("config-gcs.yml", "/etc/clickhouse-backup/config.yml"))
	r.NoError(dockerExec("apt-get", "-y", "update"))
	r.NoError(dockerExec("apt-get", "-y", "install", "ca-certificates"))
	// testRestoreLegacyBackupFormat(t)
	testCommon(t)
}
func TestIntegrationAzure(t *testing.T) {
	if os.Getenv("AZURE_TESTS") == "" || os.Getenv("TRAVIS_PULL_REQUEST") != "false" {
		t.Skip("Skipping Azure integration tests...")
		return
	}
	r := require.New(t)
	r.NoError(dockerCP("config-azblob.yml", "/etc/clickhouse-backup/config.yml"))
	r.NoError(dockerExec("apt-get", "-y", "update"))
	r.NoError(dockerExec("apt-get", "-y", "install", "ca-certificates"))
	// testRestoreLegacyBackupFormat(t)
	testCommon(t)
}

func testCommon(t *testing.T) {
	var out string
	var err error

	time.Sleep(time.Second * 5)
	ch := &TestClickHouse{}
	r := require.New(t)
	r.NoError(ch.connect())
	r.NoError(ch.dropDatabase(dbName))
	log.Info("Generate test data")
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

	log.Info("Create backup")
	r.NoError(dockerExec("clickhouse-backup", "create", "test_backup"))
	// log.Info("Generate increment test data")
	// for _, data := range incrementData {
	// 	if isTableSkip(data) {
	// 		continue
	// 	}
	// 	r.NoError(ch.createTestData(data))
	// }
	// time.Sleep(time.Second * 5)
	// r.NoError(dockerExec("clickhouse-backup", "create", "increment"))

	log.Info("Upload")
	r.NoError(dockerExec("clickhouse-backup", "upload", "test_backup"))
	// r.NoError(dockerExec("clickhouse-backup", "upload", "increment", "--diff-from", "test_backup"))

	log.Info("Drop database")
	r.NoError(ch.dropDatabase(dbName))
	out, err = dockerExecOut("ls", "-lha", "/var/lib/clickhouse/backup")
	r.NoError(err)
	r.Equal(4, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect one backup exists in backup directory")
	log.Info("Delete backup")
	r.NoError(dockerExec("clickhouse-backup", "delete", "local", "test_backup"))
	out, err = dockerExecOut("ls", "-lha", "/var/lib/clickhouse/backup")
	r.NoError(err)
	r.Equal(3, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect no backup exists in backup directory")

	log.Info("Download")
	r.NoError(dockerExec("clickhouse-backup", "download", "test_backup"))

	log.Info("Restore schema")
	r.NoError(dockerExec("clickhouse-backup", "restore", "--schema", "test_backup"))

	log.Info("Restore data")
	r.NoError(dockerExec("clickhouse-backup", "restore", "--data", "test_backup"))

	log.Info("Full restore with rm")
	r.NoError(dockerExec("clickhouse-backup", "restore", "--rm", "test_backup"))

	log.Info("Check data")
	for i := range testData {
		if isTableSkip(ch, testData[i], true) {
			continue
		}
		r.NoError(ch.checkData(t, testData[i]))
	}
	// test increment
	// log.Info("Drop database")
	// r.NoError(ch.dropDatabase(dbName))

	// dockerExec("ls", "-lha", "/var/lib/clickhouse/backup")
	// log.Info("Delete backup")
	// r.NoError(dockerExec("/bin/rm", "-rf", "/var/lib/clickhouse/backup/test_backup", "/var/lib/clickhouse/backup/increment"))
	// dockerExec("ls", "-lha", "/var/lib/clickhouse/backup")

	// log.Info("Download increment")
	// r.NoError(dockerExec("clickhouse-backup", "download", "increment"))

	// log.Info("Restore")
	// r.NoError(dockerExec("clickhouse-backup", "restore", "--schema", "--data", "increment"))

	// log.Info("Check increment data")
	// for i := range testData {
	// 	if isTableSkip(testData[i]) {
	// 		continue
	// 	}
	// 	ti := testData[i]
	// 	ti.Rows = append(ti.Rows, incrementData[i].Rows...)
	// 	r.NoError(ch.checkData(t, ti))
	// }

	log.Info("Clean")
	r.NoError(dockerExec("clickhouse-backup", "delete", "remote", "test_backup"))
	r.NoError(dockerExec("clickhouse-backup", "delete", "local", "test_backup"))
	// r.NoError(dockerExec("clickhouse-backup", "delete", "remote", "increment"))
	// r.NoError(dockerExec("clickhouse-backup", "delete", "local", "increment"))

}

type TestClickHouse struct {
	chbackup *clickhouse.ClickHouse
}

func (ch *TestClickHouse) connect() error {
	ch.chbackup = &clickhouse.ClickHouse{
		Config: &config.ClickHouseConfig{
			Host:    "localhost",
			Port:    9000,
			Timeout: "5m",
		},
	}
	return ch.chbackup.Connect()
}

func (ch *TestClickHouse) createTestSchema(data TestDataStruct) error {
	if err := ch.chbackup.CreateDatabase(data.Database); err != nil {
		return err
	}
	createSQL := "CREATE "
	if data.IsMaterializedView {
		createSQL += " MATERIALIZED VIEW "
	} else if data.IsDictionary {
		createSQL += " DICTIONARY "
	} else {
		createSQL += " TABLE "
	}
	createSQL += fmt.Sprintf(" IF NOT EXISTS `%s`.`%s` ", data.Database, data.Table)
	createSQL += data.Schema
	// old 1.x clickhouse versions doesn't contains {table} and {database} macros
	var isMacrosExists []int
	if err := ch.chbackup.Select(&isMacrosExists, "SELECT count() FROM system.functions WHERE name='getMacro'"); err != nil {
		return err
	}
	if len(isMacrosExists) == 0 || isMacrosExists[0] == 0 {
		createSQL = strings.Replace(createSQL, "{table}", data.Table, -1)
		createSQL = strings.Replace(createSQL, "{database}", data.Database, -1)
	}
	err := ch.chbackup.CreateTable(
		clickhouse.Table{
			Database: data.Database,
			Name:     data.Table,
		},
		createSQL,
		false,
	)
	return err
}

func (ch *TestClickHouse) createTestData(data TestDataStruct) error {
	if data.SkipInsert {
		return nil
	}
	tx, err := ch.chbackup.GetConn().Beginx()
	if err != nil {
		return fmt.Errorf("can't begin transaction: %v", err)
	}
	insertSQL := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (:%s)",
		data.Database,
		data.Table,
		strings.Join(data.Fields, ","),
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
		log.Debugf("%#v", row)
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
	if isAtomic, err = ch.chbackup.IsAtomic(database); isAtomic {
		dropDatabaseSQL += " SYNC"
	} else if err != nil {
		return err
	}
	_, err = ch.chbackup.Query(dropDatabaseSQL)
	return err
}

func (ch *TestClickHouse) checkData(t *testing.T, data TestDataStruct) error {
	log.Infof("Check '%d' rows in '%s.%s'\n", len(data.Rows), data.Database, data.Table)
	selectSQL := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY %s", data.Database, data.Table, data.OrderBy)
	log.Debug(selectSQL)
	rows, err := ch.chbackup.GetConn().Queryx(selectSQL)
	if err != nil {
		return err
	}
	var result []map[string]interface{}
	for rows.Next() {
		row := map[string]interface{}{}
		if rows.MapScan(row) != nil {
			return err
		}
		result = append(result, row)
	}
	assert.Equal(t, len(data.Rows), len(result))
	for i := range data.Rows {
		assert.EqualValues(t, data.Rows[i], result[i])
	}
	return nil
}

func dockerExec(cmd ...string) error {
	out, err := dockerExecOut(cmd...)
	log.Debug(out)
	return err
}

func dockerExecOut(cmd ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	dcmd := []string{"exec", "clickhouse"}
	dcmd = append(dcmd, cmd...)
	log.Info(strings.Join(dcmd, " "))
	out, err := exec.CommandContext(ctx, "docker", dcmd...).CombinedOutput()
	cancel()
	return string(out), err
}

func dockerCP(src, dst string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	dcmd := []string{"cp", src, "clickhouse:" + dst}
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
			data.Table, data.Database,
		)
		ch.chbackup.Select(&dictEngines, dictSQL)
		return len(dictEngines) == 0
	}
	return (os.Getenv("COMPOSE_FILE") == "docker-compose.yml") && (data.Table == "jbod" || data.IsDictionary)
}
