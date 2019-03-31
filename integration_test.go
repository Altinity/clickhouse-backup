// +build integration

package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/kshvakov/clickhouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestDataStuct struct {
	Database string
	Table    string
	Schema   string
	Rows     []map[string]interface{}
	Fields   []string
	OrderBy  string
}

var testData = []TestDataStuct{
	TestDataStuct{
		Database: "testdb",
		Table:    "table1",
		Schema:   "(Date Date, TimeStamp DateTime, Log String) ENGINE = MergeTree(Date, (TimeStamp, Log), 8192)",
		Rows: []map[string]interface{}{
			map[string]interface{}{"Date": toDate("2018-10-23"), "TimeStamp": toTS("2018-10-23 07:37:14"), "Log": "One"},
			map[string]interface{}{"Date": toDate("2018-10-23"), "TimeStamp": toTS("2018-10-23 07:37:15"), "Log": "Two"},
			map[string]interface{}{"Date": toDate("2018-10-24"), "TimeStamp": toTS("2018-10-24 07:37:16"), "Log": "Three"},
			map[string]interface{}{"Date": toDate("2018-10-24"), "TimeStamp": toTS("2018-10-24 07:37:17"), "Log": "Four"},
			map[string]interface{}{"Date": toDate("2019-10-25"), "TimeStamp": toTS("2019-01-25 07:37:18"), "Log": "Five"},
			map[string]interface{}{"Date": toDate("2019-10-25"), "TimeStamp": toTS("2019-01-25 07:37:19"), "Log": "Six"},
		},
		Fields:  []string{"Date", "TimeStamp", "Log"},
		OrderBy: "TimeStamp",
	},
	TestDataStuct{
		Database: "testdb",
		Table:    "table2",
		Schema:   "(id UInt64, User String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			map[string]interface{}{"id": uint64(1), "User": "Alice"},
			map[string]interface{}{"id": uint64(2), "User": "Bob"},
			map[string]interface{}{"id": uint64(3), "User": "John"},
			map[string]interface{}{"id": uint64(4), "User": "Frank"},
			map[string]interface{}{"id": uint64(5), "User": "Nancy"},
			map[string]interface{}{"id": uint64(6), "User": "Brandon"},
		},
		Fields:  []string{"id", "User"},
		OrderBy: "id",
	},
	TestDataStuct{
		Database: "testdb",
		Table:    "table3",
		Schema:   "(TimeStamp DateTime, Item String, Date Date MATERIALIZED toDate(TimeStamp)) ENGINE = MergeTree() PARTITION BY Date ORDER BY TimeStamp SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			map[string]interface{}{"TimeStamp": toTS("2018-10-23 07:37:14"), "Item": "One"},
			map[string]interface{}{"TimeStamp": toTS("2018-10-23 07:37:15"), "Item": "Two"},
			map[string]interface{}{"TimeStamp": toTS("2018-10-24 07:37:16"), "Item": "Three"},
			map[string]interface{}{"TimeStamp": toTS("2018-10-24 07:37:17"), "Item": "Four"},
			map[string]interface{}{"TimeStamp": toTS("2019-01-25 07:37:18"), "Item": "Five"},
			map[string]interface{}{"TimeStamp": toTS("2019-01-25 07:37:19"), "Item": "Six"},
		},
		Fields:  []string{"TimeStamp", "Item"},
		OrderBy: "TimeStamp",
	},
	TestDataStuct{
		Database: "testdb",
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
	},
}

func TestIntegration(t *testing.T) {
	ch := &ClickHouse{
		Config: &ClickHouseConfig{
			Host: "localhost",
			Port: 9000,
		},
	}
	r := require.New(t)
	r.NoError(ch.Connect())
	r.NoError(ch.dropDatabase("testdb"))

	fmt.Println("Generate test data")
	for _, data := range testData {
		r.NoError(ch.createTestData(data))
	}

	time.Sleep(time.Second * 5)
	fmt.Println("Create backup")
	r.NoError(dockerExec("clickhouse-backup", "create"))

	out, _ := dockerExecOut("clickhouse-backup", "upload")
	r.True(strings.HasPrefix(out, "Select backup for upload:"))
	backupFileName := strings.Split(out, "\n")[1]
	backupName := strings.TrimSuffix(backupFileName, ".tar")

	fmt.Println("Upload")
	r.NoError(dockerExec("clickhouse-backup", "upload", backupFileName))

	fmt.Println("Drop database")
	r.NoError(ch.dropDatabase("testdb"))

	fmt.Println("Delete backup")
	r.NoError(dockerExec("rm", "-rf", fmt.Sprintf("/var/lib/clickhouse/backup/%s", backupFileName)))

	fmt.Println("Download")
	r.NoError(dockerExec("clickhouse-backup", "download", backupFileName))

	fmt.Println("Create tables")
	r.NoError(dockerExec("clickhouse-backup", "restore-schema", backupName))

	fmt.Println("Restore")
	r.NoError(dockerExec("clickhouse-backup", "restore-data", backupName))

	fmt.Println("Check data")
	for i := range testData {
		r.NoError(ch.checkData(t, testData[i]))
	}
}

func (ch *ClickHouse) createTestData(data TestDataStuct) error {
	if err := ch.CreateDatabase(data.Database); err != nil {
		return err
	}
	if err := ch.CreateTable(RestoreTable{
		Database: data.Database,
		Table:    data.Table,
		Query:    fmt.Sprintf("CREATE TABLE %s.%s %s", data.Database, data.Table, data.Schema),
	}); err != nil {
		return err
	}

	for _, row := range data.Rows {
		tx, err := ch.conn.Beginx()
		if err != nil {
			return fmt.Errorf("can't begin transaction with: %v", err)
		}
		if _, err := tx.NamedExec(
			fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (:%s)",
				data.Database,
				data.Table,
				strings.Join(data.Fields, ","),
				strings.Join(data.Fields, ",:"),
			), row); err != nil {
			return fmt.Errorf("can't add insert to transaction with: %v", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("can't commit with: %v", err)
		}
	}
	return nil
}

func (ch *ClickHouse) dropDatabase(database string) error {
	fmt.Println("DROP DATABASE IF EXISTS ", database)
	_, err := ch.conn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", database))
	return err
}

func (ch *ClickHouse) checkData(t *testing.T, data TestDataStuct) error {
	rows, err := ch.conn.Queryx(fmt.Sprintf("SELECT * FROM %s.%s ORDER BY %s", data.Database, data.Table, data.OrderBy))
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
	fmt.Println(string(out))
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

func toDate(s string) time.Time {
	result, _ := time.Parse("2006-01-02", s)
	return result
}

func toTS(s string) time.Time {
	result, _ := time.Parse("2006-01-02 15:04:05", s)
	return result
}
