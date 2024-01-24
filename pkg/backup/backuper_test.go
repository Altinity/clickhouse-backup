package backup

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
)

type testVersioner struct {
	err error
}

func newTestVersioner(err error) *testVersioner {
	return &testVersioner{err: err}
}

func (v *testVersioner) CanShardOperation(ctx context.Context) error {
	return v.err
}

type testBackupSharder struct {
	data shardDetermination
	err  error
}

func (bs *testBackupSharder) determineShards(_ context.Context) (shardDetermination, error) {
	if bs.err != nil {
		return nil, bs.err
	}
	return bs.data, nil
}

func TestPopulateBackupShardField(t *testing.T) {
	errVersion := errors.New("versioner error")
	errVersioner := newTestVersioner(errVersion)
	goodVersioner := newTestVersioner(nil)
	oldVersioner := newTestVersioner(clickhouse.ErrShardOperationVers)

	// Create tables to reset field state
	tableData := func() []clickhouse.Table {
		return []clickhouse.Table{
			{
				Database: "a",
				Name:     "present",
			},
			{
				Database: "a",
				Name:     "absent",
			},
			{
				Database: "b",
				Name:     "present",
				Skip:     true,
			},
		}
	}

	errShard := errors.New("backup sharder error")
	errSharder := &testBackupSharder{err: errShard}
	staticSharder := &testBackupSharder{
		data: shardDetermination{
			"`a`.`present`": true,
			"`a`.`absent`":  false,
		},
	}
	emptySharder := &testBackupSharder{
		data: shardDetermination{},
	}

	testcases := []struct {
		name        string
		shardOpMode string
		v           versioner
		bs          backupSharder
		expect      []clickhouse.Table
		expectErr   error
	}{
		{
			name:        "Test versioner error",
			shardOpMode: "table",
			v:           errVersioner,
			bs:          staticSharder,
			expectErr:   errVersion,
		},
		{
			name:        "Test incompatible version",
			shardOpMode: "table",
			v:           oldVersioner,
			bs:          staticSharder,
			expectErr:   clickhouse.ErrShardOperationVers,
		},
		{
			name:        "Test incompatible version without sharding config",
			shardOpMode: "none",
			v:           oldVersioner,
			bs:          staticSharder,
			expect: []clickhouse.Table{
				{
					Database:   "a",
					Name:       "present",
					BackupType: clickhouse.ShardBackupFull,
				},
				{
					Database:   "a",
					Name:       "absent",
					BackupType: clickhouse.ShardBackupFull,
				},
				{
					Database:   "b",
					Name:       "present",
					Skip:       true,
					BackupType: clickhouse.ShardBackupNone,
				},
			},
		},
		{
			name:        "Test sharder error",
			shardOpMode: "table",
			v:           goodVersioner,
			bs:          errSharder,
			expectErr:   errShard,
		},
		{
			name:        "Test incomplete replica data",
			shardOpMode: "table",
			v:           goodVersioner,
			bs:          emptySharder,
			expectErr:   errUnknownBackupShard,
		},
		{
			name:        "Test normal sharding",
			shardOpMode: "table",
			v:           goodVersioner,
			bs:          staticSharder,
			expect: []clickhouse.Table{
				{
					Database:   "a",
					Name:       "present",
					BackupType: clickhouse.ShardBackupFull,
				},
				{
					Database:   "a",
					Name:       "absent",
					BackupType: clickhouse.ShardBackupSchema,
				},
				{
					Database:   "b",
					Name:       "present",
					Skip:       true,
					BackupType: clickhouse.ShardBackupNone,
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name,
			func(t *testing.T) {
				cfg := &config.Config{
					General: config.GeneralConfig{
						ShardedOperationMode: tc.shardOpMode,
					},
				}
				b := NewBackuper(cfg,
					WithVersioner(tc.v),
					WithBackupSharder(tc.bs),
				)
				tables := tableData()
				err := b.populateBackupShardField(context.Background(), tables)
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected error %v, got %v", tc.expectErr, err)
				}
				if err != nil {
					return
				}
				if !reflect.DeepEqual(tables, tc.expect) {
					t.Fatalf("expected %+v, got %+v", tc.expect, tables)
				}
			},
		)
	}
}
