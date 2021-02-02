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

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dbName = "_test.ДБ_"

type TestDataStruct struct {
	Database string
	Table    string
	Schema   string
	Rows     []map[string]interface{}
	Fields   []string
	OrderBy  string
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
			result := []map[string]interface{}{}
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
			result := []map[string]interface{}{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i)})
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
			result := []map[string]interface{}{}
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
			result := []map[string]interface{}{}
			for i := 100; i < 200; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i)})
			}
			return result
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
}

func testRestoreLegacyBackupFormat(t *testing.T) {
	time.Sleep(time.Second * 5)
	ch := &TestClickHouse{}
	r := require.New(t)
	r.NoError(ch.connect())
	r.NoError(ch.dropDatabase(dbName))
	fmt.Println("Generate test data")
	for _, data := range testData {
		if (os.Getenv("COMPOSE_FILE") == "docker-compose.yml") && (data.Table == "jbod") {
			continue
		}
		r.NoError(ch.createTestData(data))
	}
	fmt.Println("Create backup")
	r.NoError(dockerExec("clickhouse-backup", "freeze"))
	dockerExec("mkdir", "-p", "/var/lib/clickhouse/backup/old_format")
	r.NoError(dockerExec("cp", "-r", "/var/lib/clickhouse/metadata", "/var/lib/clickhouse/backup/old_format/"))
	r.NoError(dockerExec("mv", "/var/lib/clickhouse/shadow", "/var/lib/clickhouse/backup/old_format/"))
	dockerExec("ls", "-lha", "/var/lib/clickhouse/backup/old_format/")

	fmt.Println("Upload")
	r.NoError(dockerExec("clickhouse-backup", "upload", "old_format"))

	fmt.Println("Create backup")
	r.NoError(dockerExec("clickhouse-backup", "create", "increment_old_format"))
	fmt.Println("Upload increment")
	r.Error(dockerExec("clickhouse-backup", "upload", "increment_old_format", "--diff-from", "old_format"))

	fmt.Println("Drop database")
	r.NoError(ch.dropDatabase(dbName))

	dockerExec("ls", "-lha", "/var/lib/clickhouse/backup")
	fmt.Println("Delete backup")
	r.NoError(dockerExec("/bin/rm", "-rf", "/var/lib/clickhouse/backup/old_format"))
	dockerExec("ls", "-lha", "/var/lib/clickhouse/backup")

	fmt.Println("Download")
	r.NoError(dockerExec("clickhouse-backup", "download", "old_format"))

	fmt.Println("Restore")
	r.NoError(dockerExec("clickhouse-backup", "restore", "-t", dbName+".*", "old_format"))

	fmt.Println("Check data")
	for i := range testData {
		if (os.Getenv("COMPOSE_FILE") == "docker-compose.yml") && (testData[i].Table == "jbod") {
			continue
		}
		r.NoError(ch.checkData(t, testData[i]))
	}
	fmt.Println("Clean")
	r.NoError(dockerExec("/bin/rm", "-rf", "/var/lib/clickhouse/backup/old_format", "/var/lib/clickhouse/backup/increment_old_format", "/var/lib/clickhouse/shadow"))
	r.NoError(dockerExec("clickhouse-backup", "delete", "remote", "old_format.tar.gz"))
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

func testCommon(t *testing.T) {
	time.Sleep(time.Second * 5)
	ch := &TestClickHouse{}
	r := require.New(t)
	r.NoError(ch.connect())
	r.NoError(ch.dropDatabase(dbName))
	fmt.Println("Generate test data")
	for _, data := range testData {
		if (os.Getenv("COMPOSE_FILE") == "docker-compose.yml") && (data.Table == "jbod") {
			continue
		}
		r.NoError(ch.createTestData(data))
	}
	fmt.Println("Create backup")
	r.NoError(dockerExec("clickhouse-backup", "create", "test_backup"))
	fmt.Println("Generate increment test data")
	for _, data := range incrementData {
		if (os.Getenv("COMPOSE_FILE") == "docker-compose.yml") && (data.Table == "jbod") {
			continue
		}
		r.NoError(ch.createTestData(data))
	}
	time.Sleep(time.Second * 5)
	r.NoError(dockerExec("clickhouse-backup", "create", "increment"))

	fmt.Println("Upload")
	r.NoError(dockerExec("clickhouse-backup", "upload", "test_backup"))
	r.NoError(dockerExec("clickhouse-backup", "upload", "increment", "--diff-from", "test_backup"))

	fmt.Println("Drop database")
	r.NoError(ch.dropDatabase(dbName))

	dockerExec("ls", "-lha", "/var/lib/clickhouse/backup")
	fmt.Println("Delete backup")
	r.NoError(dockerExec("/bin/rm", "-rf", "/var/lib/clickhouse/backup/test_backup", "/var/lib/clickhouse/backup/increment"))
	dockerExec("ls", "-lha", "/var/lib/clickhouse/backup")

	fmt.Println("Download")
	r.NoError(dockerExec("clickhouse-backup", "download", "test_backup"))

	fmt.Println("Restore schema")
	r.NoError(dockerExec("clickhouse-backup", "restore", "--schema", "test_backup"))

	fmt.Println("Restore data")
	r.NoError(dockerExec("clickhouse-backup", "restore", "--data", "test_backup"))

	fmt.Println("Check data")
	for i := range testData {
		if (os.Getenv("COMPOSE_FILE") == "docker-compose.yml") && (testData[i].Table == "jbod") {
			continue
		}
		r.NoError(ch.checkData(t, testData[i]))
	}
	// test increment
	fmt.Println("Drop database")
	r.NoError(ch.dropDatabase(dbName))

	dockerExec("ls", "-lha", "/var/lib/clickhouse/backup")
	fmt.Println("Delete backup")
	r.NoError(dockerExec("/bin/rm", "-rf", "/var/lib/clickhouse/backup/test_backup", "/var/lib/clickhouse/backup/increment"))
	dockerExec("ls", "-lha", "/var/lib/clickhouse/backup")

	fmt.Println("Download increment")
	r.NoError(dockerExec("clickhouse-backup", "download", "increment"))

	fmt.Println("Restore")
	r.NoError(dockerExec("clickhouse-backup", "restore", "--schema", "--data", "increment"))

	fmt.Println("Check increment data")
	for i := range testData {
		if (os.Getenv("COMPOSE_FILE") == "docker-compose.yml") && (testData[i].Table == "jbod") {
			continue
		}
		ti := testData[i]
		ti.Rows = append(ti.Rows, incrementData[i].Rows...)
		r.NoError(ch.checkData(t, ti))
	}

	fmt.Println("Clean")
	r.NoError(dockerExec("/bin/rm", "-rf", "/var/lib/clickhouse/backup/test_backup", "/var/lib/clickhouse/backup/increment"))
	r.NoError(dockerExec("clickhouse-backup", "delete", "remote", "test_backup"))
	r.NoError(dockerExec("clickhouse-backup", "delete", "remote", "increment"))
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

func (ch *TestClickHouse) createTestData(data TestDataStruct) error {
	if err := ch.chbackup.CreateDatabase(data.Database); err != nil {
		return err
	}
	if err := ch.chbackup.CreateTable(clickhouse.Table{
		Database: data.Database,
		Name:     data.Table,
	}, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` %s", data.Database, data.Table, data.Schema), false); err != nil {
		return err
	}

	for _, row := range data.Rows {
		tx, err := ch.chbackup.GetConn().Beginx()
		if err != nil {
			return fmt.Errorf("can't begin transaction: %v", err)
		}
		if _, err := tx.NamedExec(
			fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (:%s)",
				data.Database,
				data.Table,
				strings.Join(data.Fields, ","),
				strings.Join(data.Fields, ",:"),
			), row); err != nil {
			return fmt.Errorf("can't add insert to transaction: %v", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("can't commit: %v", err)
		}
	}
	return nil
}

func (ch *TestClickHouse) dropDatabase(database string) error {
	fmt.Println("DROP DATABASE IF EXISTS ", database)
	_, err := ch.chbackup.GetConn().Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", database))
	return err
}

func (ch *TestClickHouse) checkData(t *testing.T, data TestDataStruct) error {
	fmt.Printf("Check '%d' rows in '%s.%s'\n", len(data.Rows), data.Database, data.Table)
	rows, err := ch.chbackup.GetConn().Queryx(fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY %s", data.Database, data.Table, data.OrderBy))
	if err != nil {
		return err
	}
	result := []map[string]interface{}{}
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
	fmt.Print(string(out))
	return err
}

func dockerExecOut(cmd ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	dcmd := []string{"exec", "clickhouse"}
	dcmd = append(dcmd, cmd...)
	out, err := exec.CommandContext(ctx, "docker", dcmd...).CombinedOutput()
	cancel()
	return string(out), err
}

func dockerCP(src, dst string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	dcmd := []string{"cp", src, "clickhouse:" + dst}
	out, err := exec.CommandContext(ctx, "docker", dcmd...).CombinedOutput()
	fmt.Println(string(out))
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
