package backup

import (
	"context"
	"sort"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"golang.org/x/sync/semaphore"
)

// downloadTransferConcurrency returns the size of the global transfer budget shared by every table of one
// `download`: `general.download_transfer_concurrency`, 0 means auto = download_concurrency^2 (the most
// transfers the nested table/part errgroups can already run today, so the default never opens more
// connections than before), a negative value keeps the legacy model where each table is limited only by
// its own `download_concurrency` part slots, see https://github.com/Altinity/clickhouse-backup/issues/1591
func (b *Backuper) downloadTransferConcurrency() int {
	n := b.cfg.General.DownloadTransferConcurrency
	if n == 0 {
		dc := max(int(b.cfg.General.DownloadConcurrency), 1)
		n = dc * dc
	}
	return max(n, 0)
}

// downloadPartGroupLimit - per-table part goroutine limit; with a global transfer budget a single remaining
// table may use the whole budget, the budget itself bounds the total number of concurrent transfers
func (b *Backuper) downloadPartGroupLimit() int {
	if b.downloadPartLimit > 0 {
		return b.downloadPartLimit
	}
	return int(b.cfg.General.DownloadConcurrency)
}

// acquireTransferSlot blocks until one slot of the global transfer budget is free, it is a no-op without a budget
func acquireTransferSlot(ctx context.Context, sem *semaphore.Weighted) (func(), error) {
	if sem == nil {
		return func() {}, nil
	}
	if err := sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return func() { sem.Release(1) }, nil
}

// downloadTableOrder returns indexes of tables with data to download in dispatch order, `largest_first` puts
// big tables first, so a big table is never dispatched last and left to drain alone at the tail
func downloadTableOrder(tables []*metadata.TableMetadata, order string) []int {
	result := make([]int, 0, len(tables))
	for i, t := range tables {
		if t == nil || t.MetadataOnly {
			continue
		}
		result = append(result, i)
	}
	if order != "metadata" {
		sort.SliceStable(result, func(x, y int) bool {
			return tableDownloadCost(tables[result[x]]) > tableDownloadCost(tables[result[y]])
		})
	}
	return result
}

// tableDownloadCost - bytes a table is expected to transfer, per-part sizes when the backup carries them
// (2.8.0+), TotalBytes otherwise
func tableDownloadCost(tm *metadata.TableMetadata) uint64 {
	var sum uint64
	for _, parts := range tm.Parts {
		for _, p := range parts {
			sum += p.Size
		}
	}
	if sum > 0 {
		return sum
	}
	return tm.TotalBytes
}
