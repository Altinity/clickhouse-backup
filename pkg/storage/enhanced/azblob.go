package enhanced

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/rs/zerolog/log"
)

// EnhancedAzureBlob implements BatchRemoteStorage for Azure Blob Storage with parallel operations
type EnhancedAzureBlob struct {
	storage.RemoteStorage
	container azblob.ContainerURL
	config    *config.AzureBlobConfig
	metrics   *DeleteMetrics
}

// AzureBlobDeleteJob represents a delete job for parallel processing
type AzureBlobDeleteJob struct {
	blobName string
	result   chan AzureBlobDeleteResult
}

// AzureBlobDeleteResult represents the result of a delete operation
type AzureBlobDeleteResult struct {
	blobName string
	error    error
}

// AzureBlobWorkerPool manages parallel delete workers
type AzureBlobWorkerPool struct {
	container azblob.ContainerURL
	workers   int
	jobs      chan AzureBlobDeleteJob
	results   chan AzureBlobDeleteResult
	wg        sync.WaitGroup
}

// NewEnhancedAzureBlob creates a new enhanced Azure Blob storage implementation
func NewEnhancedAzureBlob(baseStorage storage.RemoteStorage, container azblob.ContainerURL, cfg *config.Config) (*EnhancedAzureBlob, error) {
	azConfig := &cfg.AzureBlob

	// Validate configuration
	if azConfig.Container == "" {
		return nil, &OptimizationConfigError{
			Field:   "container",
			Value:   azConfig.Container,
			Message: "Azure Blob container name cannot be empty",
		}
	}

	return &EnhancedAzureBlob{
		RemoteStorage: baseStorage,
		container:     container,
		config:        azConfig,
		metrics:       &DeleteMetrics{},
	}, nil
}

// DeleteBatch implements BatchRemoteStorage interface for Azure Blob parallel delete
func (az *EnhancedAzureBlob) DeleteBatch(ctx context.Context, keys []string) (*BatchResult, error) {
	startTime := time.Now()
	defer func() {
		az.metrics.TotalDuration = time.Since(startTime)
		az.updateThroughputMetrics()
	}()

	if len(keys) == 0 {
		return &BatchResult{SuccessCount: 0}, nil
	}

	log.Debug().Int("key_count", len(keys)).Msg("starting Azure Blob parallel delete")
	az.metrics.FilesProcessed = int64(len(keys))

	// Use parallel workers since the old Azure SDK doesn't support batch operations
	return az.deleteParallel(ctx, keys)
}

// deleteParallel uses parallel workers for delete operations
func (az *EnhancedAzureBlob) deleteParallel(ctx context.Context, keys []string) (*BatchResult, error) {
	log.Debug().Int("key_count", len(keys)).Msg("using Azure Blob parallel delete")

	workerCount := az.getOptimalWorkerCount(len(keys))
	workerPool := az.createWorkerPool(workerCount)

	// Start workers
	workerPool.start(ctx)
	defer workerPool.stop()

	// Send jobs
	go func() {
		defer close(workerPool.jobs)
		for _, key := range keys {
			job := AzureBlobDeleteJob{
				blobName: key,
				result:   workerPool.results,
			}
			select {
			case workerPool.jobs <- job:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collect results
	var successCount int
	var failedKeys []FailedKey
	var errors []error

	for i := 0; i < len(keys); i++ {
		select {
		case result := <-workerPool.results:
			if result.error != nil {
				failedKeys = append(failedKeys, FailedKey{
					Key:   result.blobName,
					Error: result.error,
				})
				errors = append(errors, fmt.Errorf("failed to delete %s: %w", result.blobName, result.error))
			} else {
				successCount++
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	az.metrics.FilesDeleted = int64(successCount)
	az.metrics.FilesFailed = int64(len(failedKeys))

	return &BatchResult{
		SuccessCount: successCount,
		FailedKeys:   failedKeys,
		Errors:       errors,
	}, nil
}

// createWorkerPool creates a new worker pool for parallel operations
func (az *EnhancedAzureBlob) createWorkerPool(workerCount int) *AzureBlobWorkerPool {
	return &AzureBlobWorkerPool{
		container: az.container,
		workers:   workerCount,
		jobs:      make(chan AzureBlobDeleteJob, workerCount*2),
		results:   make(chan AzureBlobDeleteResult, workerCount*2),
	}
}

// getOptimalWorkerCount determines the optimal number of workers
func (az *EnhancedAzureBlob) getOptimalWorkerCount(jobCount int) int {
	// Start with default max workers for Azure
	maxWorkers := 20 // Conservative default for Azure Blob

	// Don't create more workers than jobs
	if jobCount < maxWorkers {
		maxWorkers = jobCount
	}

	// Ensure we have at least 1 worker
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	return maxWorkers
}

// updateThroughputMetrics calculates and updates throughput metrics
func (az *EnhancedAzureBlob) updateThroughputMetrics() {
	if az.metrics.TotalDuration > 0 && az.metrics.BytesDeleted > 0 {
		throughputBytes := float64(az.metrics.BytesDeleted) / az.metrics.TotalDuration.Seconds()
		az.metrics.ThroughputMBps = throughputBytes / (1024 * 1024) // Convert to MB/s
	}
}

// SupportsBatchDelete returns false as the old Azure SDK doesn't support batch operations
func (az *EnhancedAzureBlob) SupportsBatchDelete() bool {
	return false // Old SDK doesn't support batch delete
}

// GetOptimalBatchSize returns the optimal batch size for parallel processing
func (az *EnhancedAzureBlob) GetOptimalBatchSize() int {
	// For Azure with parallel workers, process in reasonable chunks
	return 100
}

// GetDeleteMetrics returns current delete operation metrics
func (az *EnhancedAzureBlob) GetDeleteMetrics() *DeleteMetrics {
	return az.metrics
}

// ResetDeleteMetrics resets the delete operation metrics
func (az *EnhancedAzureBlob) ResetDeleteMetrics() {
	az.metrics = &DeleteMetrics{}
}

// AzureBlobWorkerPool implementation

// start starts the worker pool
func (pool *AzureBlobWorkerPool) start(ctx context.Context) {
	for i := 0; i < pool.workers; i++ {
		pool.wg.Add(1)
		go pool.worker(ctx, i)
	}
}

// stop stops the worker pool and waits for workers to finish
func (pool *AzureBlobWorkerPool) stop() {
	pool.wg.Wait()
	close(pool.results)
}

// worker processes delete jobs
func (pool *AzureBlobWorkerPool) worker(ctx context.Context, workerID int) {
	defer pool.wg.Done()

	for job := range pool.jobs {
		select {
		case <-ctx.Done():
			job.result <- AzureBlobDeleteResult{
				blobName: job.blobName,
				error:    ctx.Err(),
			}
			return
		default:
			err := pool.deleteBlob(ctx, job.blobName)
			job.result <- AzureBlobDeleteResult{
				blobName: job.blobName,
				error:    err,
			}
		}
	}
}

// deleteBlob deletes a single blob with retry logic
func (pool *AzureBlobWorkerPool) deleteBlob(ctx context.Context, blobName string) error {
	blob := pool.container.NewBlockBlobURL(blobName)

	var err error
	maxRetries := 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		_, err = blob.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
		if err == nil {
			return nil
		}

		// Check if blob doesn't exist (which is success for delete)
		var storageErr azblob.StorageError
		if errors.As(err, &storageErr) && storageErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
			log.Debug().Str("blob", blobName).Msg("blob already deleted or doesn't exist")
			return nil
		}

		// Check if error is retriable
		if !IsRetriableError(err) {
			break
		}

		// Exponential backoff for retriable errors
		if attempt < maxRetries {
			backoff := time.Duration(1<<attempt) * time.Second
			log.Debug().
				Err(err).
				Str("blob", blobName).
				Int("attempt", attempt+1).
				Str("backoff", backoff.String()).
				Msg("retrying Azure blob delete")

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return fmt.Errorf("failed to delete blob %s after %d attempts: %w", blobName, maxRetries+1, err)
}

// validateAzureConnection validates the Azure connection and permissions
func (az *EnhancedAzureBlob) validateAzureConnection(ctx context.Context) error {
	// Try to get container properties to validate connection
	_, err := az.container.GetProperties(ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		return fmt.Errorf("failed to access Azure container: %w", err)
	}

	return nil
}

// optimizeBatchSize optimizes the batch size based on performance characteristics
func (az *EnhancedAzureBlob) optimizeBatchSize(totalItems int) int {
	// For Azure, we use parallel processing rather than true batching
	// Return a reasonable chunk size based on total items and worker count

	workerCount := az.getOptimalWorkerCount(totalItems)
	if workerCount == 0 {
		return totalItems
	}

	// Distribute items evenly across workers
	chunkSize := totalItems / workerCount
	if chunkSize == 0 {
		chunkSize = 1
	}

	// Ensure we don't create too small chunks
	minChunkSize := 10
	if chunkSize < minChunkSize && totalItems > minChunkSize {
		chunkSize = minChunkSize
	}

	return chunkSize
}
