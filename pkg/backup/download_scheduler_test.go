package backup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

func TestDownloadTransferConcurrency(t *testing.T) {
	testCases := []struct {
		name     string
		dc       uint8
		transfer int
		expected int
	}{
		{"auto is download_concurrency^2", 4, 0, 16},
		{"auto with download_concurrency=1", 1, 0, 1},
		{"explicit value", 4, 64, 64},
		{"negative is legacy", 4, -1, 0},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.General.DownloadConcurrency = tc.dc
			cfg.General.DownloadTransferConcurrency = tc.transfer
			b := &Backuper{cfg: cfg}
			require.Equal(t, tc.expected, b.downloadTransferConcurrency())
		})
	}
}

func TestDownloadPartGroupLimit(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.General.DownloadConcurrency = 4
	b := &Backuper{cfg: cfg}
	require.Equal(t, 4, b.downloadPartGroupLimit(), "legacy model keeps download_concurrency per table")
	b.downloadPartLimit = 16
	require.Equal(t, 16, b.downloadPartGroupLimit(), "with a global budget one table may use the whole budget")
}

// TestAcquireTransferSlotBoundsTotal - many tables with many parts each never run more transfers than the budget,
// and a single remaining table can use the whole budget
func TestAcquireTransferSlotBoundsTotal(t *testing.T) {
	const budget = 6
	b := &Backuper{downloadTransferSem: semaphore.NewWeighted(budget), downloadPartLimit: budget}
	var inFlight, peak atomic.Int64
	transfer := func(ctx context.Context) error {
		release, err := acquireTransferSlot(ctx, b.downloadTransferSem)
		if err != nil {
			return err
		}
		defer release()
		n := inFlight.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(10 * time.Millisecond)
		inFlight.Add(-1)
		return nil
	}
	runTable := func(ctx context.Context, parts int) error {
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(b.downloadPartGroupLimit())
		for i := 0; i < parts; i++ {
			g.Go(func() error { return transfer(gCtx) })
		}
		return g.Wait()
	}

	// one table alone reaches the whole budget
	require.NoError(t, runTable(context.Background(), budget*3))
	require.Equal(t, int64(budget), peak.Load())

	// several tables together never exceed it
	peak.Store(0)
	tables := errgroup.Group{}
	tables.SetLimit(3)
	for i := 0; i < 5; i++ {
		tables.Go(func() error {
			return runTable(context.Background(), 10)
		})
	}
	require.NoError(t, tables.Wait())
	require.LessOrEqual(t, peak.Load(), int64(budget))
}

func TestAcquireTransferSlotLegacyIsNoop(t *testing.T) {
	release, err := acquireTransferSlot(context.Background(), nil)
	require.NoError(t, err)
	release()
}

func TestAcquireTransferSlotCanceled(t *testing.T) {
	sem := semaphore.NewWeighted(1)
	release, err := acquireTransferSlot(context.Background(), sem)
	require.NoError(t, err)
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = acquireTransferSlot(ctx, sem)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDownloadTableOrder(t *testing.T) {
	part := func(size uint64) metadata.Part { return metadata.Part{Size: size} }
	tables := []*metadata.TableMetadata{
		{Table: "small", Parts: map[string][]metadata.Part{"default": {part(10), part(10)}}},
		nil,
		{Table: "schema_only", MetadataOnly: true, TotalBytes: 1000},
		{Table: "large", Parts: map[string][]metadata.Part{"default": {part(100)}, "hdd": {part(100)}}},
		{Table: "old_backup_without_part_size", TotalBytes: 50, Parts: map[string][]metadata.Part{"default": {{Name: "all_1_1_0"}}}},
		{Table: "same_as_small", Parts: map[string][]metadata.Part{"default": {part(20)}}},
	}
	require.Equal(t, []int{0, 3, 4, 5}, downloadTableOrder(tables, "metadata"))
	require.Equal(t, []int{3, 4, 0, 5}, downloadTableOrder(tables, "largest_first"), "stable for equal cost")
}
