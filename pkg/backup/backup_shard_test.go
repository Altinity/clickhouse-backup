package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestInShard(t *testing.T) {
	d := shardDetermination{
		"`present_db`.`present_table`": true,
		"`present_db`.`absent_table`":  false,
		"`absent_db`.`present_table`":  false,
		"`absent_db`.`absent_table`":   false,
		"`a.b`.`c`":                    true,
		"`a`.`b.c`":                    false,
	}
	testcases := []struct {
		name          string
		database      string
		table         string
		expectPresent bool
		expectErr     error
	}{
		{
			name:          "Test nonexistent database",
			database:      "nonexistent",
			table:         "present_table",
			expectPresent: false,
			expectErr:     errUnknownBackupShard,
		},
		{
			name:          "Test nonexistent table",
			database:      "present_db",
			table:         "nonexistent",
			expectPresent: false,
			expectErr:     errUnknownBackupShard,
		},
		{
			name:          "Test in-shard table",
			database:      "present_db",
			table:         "present_table",
			expectPresent: true,
		},
		{
			name:          "Test out-shard table",
			database:      "present_db",
			table:         "absent_table",
			expectPresent: false,
		},
		{
			name:          "Test out-shard database with in-shard table name for other database",
			database:      "absent_db",
			table:         "present_table",
			expectPresent: false,
		},
		{
			name:          "Test out-shard database",
			database:      "absent_db",
			table:         "absent_table",
			expectPresent: false,
		},
		{
			name:          "Test ambiguous database/table name combination",
			database:      "a.b",
			table:         "c",
			expectPresent: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name,
			func(t *testing.T) {
				present, err := d.inShard(tc.database, tc.table)
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected err %q, got %q", tc.expectErr, err)
				}
				if present != tc.expectPresent {
					t.Fatalf("expected in-shard status %v, got %v", tc.expectPresent, present)
				}
			},
		)
	}
}

func TestFullName(t *testing.T) {
	testcases := []struct {
		md     tableReplicaMetadata
		expect string
	}{
		{
			md: tableReplicaMetadata{
				Database: "a.b",
				Table:    "c",
			},
			expect: "`a.b`.`c`",
		},
		{
			md: tableReplicaMetadata{
				Database: "a",
				Table:    "b.c",
			},
			expect: "`a`.`b.c`",
		},
		{
			md: tableReplicaMetadata{
				Database: "db",
			},
			expect: "`db`.``",
		},
		{
			md: tableReplicaMetadata{
				Table: "t",
			},
			expect: "``.`t`",
		},
	}
	for _, tc := range testcases {
		if tc.md.fullName() != tc.expect {
			t.Fatalf("expected %q, got %q", tc.expect, tc.md.fullName())
		}
	}
}

func TestDoesShard(t *testing.T) {
	testcases := []struct {
		name      string
		shardName string
		expect    bool
	}{
		{
			name:      "Test present and sharding function name string",
			shardName: "table",
			expect:    true,
		},
		{
			name:      "Test present and non-sharding function name string",
			shardName: "none",
			expect:    false,
		},
		{
			name:      "Test empty function name string",
			shardName: "",
			expect:    false,
		},
		{
			name:      "Test absent name string",
			shardName: "nonexistent",
			expect:    false,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name,
			func(t *testing.T) {
				got := doesShard(tc.shardName)
				if got != tc.expect {
					t.Fatalf("expected %v, got %v", tc.expect, got)
				}
			},
		)
	}
}

func TestShardFunc(t *testing.T) {
	t.Run("Test obtaining nonexistent shard func",
		func(t *testing.T) {
			_, err := shardFuncByName("non-existent")
			if err == nil {
				t.Fatalf("expected error when trying to get a nonexistent shard function")
			}
		},
	)

	testcases := []struct {
		name      string
		md        *tableReplicaMetadata
		expect    map[string]bool
		expectErr error
	}{
		{
			name: "Test no active replicas",
			md: &tableReplicaMetadata{
				Database:       "database",
				Table:          "table",
				ReplicaName:    "replica",
				ActiveReplicas: []string{},
			},
			expect: map[string]bool{
				"table":         false,
				"database":      false,
				"first-replica": false,
			},
			expectErr: errNoActiveReplicas,
		},
		{
			name: "Test no active replicas for no sharding",
			md: &tableReplicaMetadata{
				Database:       "database",
				Table:          "table",
				ReplicaName:    "replica",
				ActiveReplicas: []string{},
			},
			expect: map[string]bool{
				"none": true,
			},
		},
		{
			name: "Test single active replica",
			md: &tableReplicaMetadata{
				Database:       "database",
				Table:          "table",
				ReplicaName:    "replica",
				ActiveReplicas: []string{"replica"},
			},
			expect: map[string]bool{
				"table":         true,
				"database":      true,
				"first-replica": true,
				"none":          true,
			},
		},
		{
			name: "Test not assigned replica",
			md: &tableReplicaMetadata{
				Database:       "database",
				Table:          "table",
				ReplicaName:    "replica",
				ActiveReplicas: []string{"different"},
			},
			expect: map[string]bool{
				"table":         false,
				"database":      false,
				"first-replica": false,
				"none":          true,
			},
		},
	}
	for _, tc := range testcases {
		for name, expect := range tc.expect {
			t.Run(fmt.Sprintf("%s - shard mode: %s", tc.name, name),
				func(t *testing.T) {
					shardFunc, err := shardFuncByName(name)
					if err != nil {
						t.Fatalf("unable to get shard function: %v", err)
					}
					got, err := shardFunc(tc.md)
					if !errors.Is(err, tc.expectErr) {
						t.Fatalf("expected error %v, got %v", tc.expectErr, err)
					}
					if tc.expectErr == nil {
						return
					}
					if got != expect {
						t.Fatalf("expected shard membership %v, got %v", tc.expect, got)
					}
				},
			)
		}
	}
}

type testQuerier struct {
	data      any
	returnErr error
}

func (tq *testQuerier) SelectContext(_ context.Context, dest interface{}, _ string,
	args ...interface{}) error {
	if tq.returnErr != nil {
		return tq.returnErr
	}
	jsonData, err := json.Marshal(tq.data)
	if err != nil {
		return fmt.Errorf("error encoding data: %w", err)
	}
	return json.NewDecoder(bytes.NewReader(jsonData)).Decode(dest)
}

func TestGetReplicaState(t *testing.T) {
	data := []tableReplicaMetadata{
		{
			Database:       "db",
			Table:          "table",
			ReplicaName:    "replica",
			ActiveReplicas: []string{},
		},
		{
			Database:       "db2",
			Table:          "table2",
			ReplicaName:    "replica2",
			ActiveReplicas: []string{"replica2"},
		},
	}
	expectedErr := errors.New("expected error")
	testcases := []struct {
		name      string
		q         querier
		expect    []tableReplicaMetadata
		expectErr error
	}{
		{
			name: "Test error on obtaining replica state",
			q: &testQuerier{
				returnErr: expectedErr,
			},
			expectErr: expectedErr,
		},
		{
			name: "Test pulling data",
			q: &testQuerier{
				data: data,
			},
			expect: data,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name,
			func(t *testing.T) {
				rd := newReplicaDeterminer(tc.q, nil)
				got, err := rd.getReplicaState(context.Background())
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected error %v, got %v", tc.expectErr, err)
				}
				if err != nil {
					return
				}
				if !reflect.DeepEqual(got, tc.expect) {
					t.Fatalf("expected data %v, got %v", tc.expect, got)
				}
			},
		)

	}
}

var errNameSharder = errors.New("expected error")

func nameSharder(md *tableReplicaMetadata) (bool, error) {
	if md.Table == "error" {
		return false, errNameSharder
	}
	if md.Table == "present" {
		return true, nil
	}
	return false, nil
}

func TestDetermineShards(t *testing.T) {
	expectErr := errors.New("expected error")
	testcases := []struct {
		name      string
		q         querier
		expect    shardDetermination
		expectErr error
	}{
		{
			name: "Test query error",
			q: &testQuerier{
				returnErr: expectErr,
			},
			expectErr: expectErr,
		},
		{
			name: "Test shard func error",
			q: &testQuerier{
				data: []tableReplicaMetadata{
					{
						Table: "error",
					},
				},
			},
			expectErr: errNameSharder,
		},
		{
			name: "Test normal operation",
			q: &testQuerier{
				data: []tableReplicaMetadata{
					{
						Database: "a",
						Table:    "present",
					},
					{
						Database: "a",
						Table:    "absent",
					},
					{
						Database: "b",
						Table:    "present",
					},
				},
			},
			expect: shardDetermination{
				"`a`.`present`": true,
				"`a`.`absent`":  false,
				"`b`.`present`": true,
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name,
			func(t *testing.T) {
				rd := newReplicaDeterminer(tc.q, nameSharder)
				got, err := rd.determineShards(context.Background())
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected %v, got %v", tc.expectErr, err)
				}
				if err != nil {
					return
				}
				if !reflect.DeepEqual(got, tc.expect) {
					t.Fatalf("expected data %v, got %v", tc.expect, got)
				}
			},
		)
	}
}
