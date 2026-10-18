package storage

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// SetFileTransfers enables `general.file_transfer_concurrency` for UploadPath, DownloadPath and DownloadPathWithManifest:
// the files of one path are transferred by up to `workers` streams, the first one is the caller goroutine, which already
// holds its own slot of `slots`, every extra stream runs only while it can take a free slot of `slots` without blocking,
// so the total number of streams never exceeds the transfer budget, extra streams just use the slots left idle by other
// parts, typically at the tail of a transfer; workers <= 1 or nil slots keeps the sequential transfer,
// see https://github.com/Altinity/clickhouse-backup/issues/1454
func (bd *BackupDestination) SetFileTransfers(workers int, slots *semaphore.Weighted) {
	bd.fileTransferConcurrency = workers
	bd.fileTransferSlots = slots
}

// fileTransfers runs the files of one path transfer, see SetFileTransfers
type fileTransfers struct {
	g      errgroup.Group
	ctx    context.Context
	cancel context.CancelFunc
	slots  *semaphore.Weighted
	errMu  sync.Mutex
	err    error
}

func (bd *BackupDestination) newFileTransfers(ctx context.Context) *fileTransfers {
	ft := &fileTransfers{}
	ft.ctx, ft.cancel = context.WithCancel(ctx)
	if bd.fileTransferConcurrency > 1 && bd.fileTransferSlots != nil {
		ft.g.SetLimit(bd.fileTransferConcurrency - 1)
		ft.slots = bd.fileTransferSlots
	}
	return ft
}

// fail records the first error before the other streams are canceled, so their `context canceled` never hides it
func (ft *fileTransfers) fail(err error) error {
	if err != nil {
		ft.errMu.Lock()
		if ft.err == nil {
			ft.err = err
		}
		ft.errMu.Unlock()
		ft.cancel()
	}
	return err
}

// run transfers one file in an extra stream when a worker and a free transfer slot are available,
// in the caller goroutine otherwise, `fn` shall use the passed context
func (ft *fileTransfers) run(fn func(ctx context.Context) error) error {
	if ft.slots != nil && ft.slots.TryAcquire(1) {
		if ft.g.TryGo(func() error {
			defer ft.slots.Release(1)
			return ft.fail(fn(ft.ctx))
		}) {
			return nil
		}
		ft.slots.Release(1)
	}
	return ft.fail(fn(ft.ctx))
}

// wait stops the extra streams when the caller failed, waits for them and returns the first error
func (ft *fileTransfers) wait(err error) error {
	ft.fail(err)
	_ = ft.g.Wait()
	ft.cancel()
	ft.errMu.Lock()
	defer ft.errMu.Unlock()
	return ft.err
}
