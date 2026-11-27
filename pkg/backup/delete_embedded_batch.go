package backup

import (
	"context"
	"io"
	"sync"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/object_disk"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// cleanEmbeddedFiles reads pointers concurrently and streams their native keys
// into bounded batches. It leaves the catalog intact; the caller removes that
// only after every native deletion succeeds.
func (b *Backuper) cleanEmbeddedFiles(ctx context.Context,
	walk func(context.Context, func(string) error) error,
	read func(context.Context, string, func(string) error) error,
) error {
	concurrency := b.cfg.S3.DeleteConcurrency
	if connection, ok := object_disk.DisksConnections.Load(b.cfg.ClickHouse.EmbeddedBackupDisk); ok && connection.Type == "azure_blob_storage" {
		concurrency = b.cfg.AzureBlob.DeleteConcurrency
	}
	retry := retrier.New(retrier.ExponentialBackoff(b.cfg.General.RetriesOnFailure,
		common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter)), b)
	return deleteEmbeddedFiles(ctx, max(1, concurrency), max(1, b.cfg.General.DeleteBatchSize), walk, read,
		func(ctx context.Context, keys []string) error {
			return retry.RunCtx(ctx, func(ctx context.Context) error {
				return object_disk.DeleteFiles(ctx, b.cfg.ClickHouse.EmbeddedBackupDisk, keys)
			})
		})
}

func readEmbeddedObjectKeys(reader io.ReadCloser, name string, emit func(string) error) error {
	log.Debug().Msgf("object_disk.ReadMetadataFromReader(%s)", name)
	// ReadMetadataFromReader closes the reader on both success and failure.
	meta, err := object_disk.ReadMetadataFromReader(reader, name)
	if err != nil {
		return errors.Wrapf(err, "object_disk.ReadMetadataFromReader(%s)", name)
	}
	for _, object := range meta.StorageObjects {
		if err = emit(object.ObjectPath); err != nil {
			return err
		}
	}
	return nil
}

func deleteEmbeddedFiles(ctx context.Context, concurrency, batchSize int,
	walk func(context.Context, func(string) error) error,
	read func(context.Context, string, func(string) error) error,
	deleteBatch func(context.Context, []string) error,
) error {
	if concurrency < 1 || batchSize < 1 {
		return errors.New("embedded deletion concurrency and batch size must be positive")
	}
	g, ctx := errgroup.WithContext(ctx)
	files := make(chan string, concurrency)
	keys := make(chan string, batchSize)
	g.Go(func() error {
		defer close(files)
		return walk(ctx, func(name string) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case files <- name:
				return nil
			}
		})
	})
	var readers sync.WaitGroup
	readers.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			defer readers.Done()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case name, ok := <-files:
					if !ok {
						return nil
					}
					if err := read(ctx, name, func(key string) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case keys <- key:
							return nil
						}
					}); err != nil {
						return err
					}
				}
			}
		})
	}
	g.Go(func() error {
		readers.Wait()
		close(keys)
		return nil
	})
	g.Go(func() error {
		batch := make([]string, 0, batchSize)
		flush := func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(batch) == 0 {
				return nil
			}
			if err := deleteBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
			return nil
		}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case key, ok := <-keys:
				if !ok {
					return flush()
				}
				batch = append(batch, key)
				if len(batch) == batchSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}
		}
	})
	return g.Wait()
}
