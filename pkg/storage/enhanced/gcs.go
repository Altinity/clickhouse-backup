package enhanced

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// EnhancedGCS implements BatchRemoteStorage for Google Cloud Storage with both parallel and batch delete
type EnhancedGCS struct {
	storage.RemoteStorage
	client              *gcs.Client
	config              *config.GCSConfig
	bucket              string
	clientPool          *GCSClientPool
	batchDeleter        *GCSBatchDeleter
	batchDeletionConfig *config.BatchDeletionConfig
	metrics             *DeleteMetrics
}

// GCSClientPool manages a pool of GCS clients for concurrent operations
type GCSClientPool struct {
	clients chan *gcs.Client
	size    int
	mutex   sync.RWMutex
}

// GCSDeleteJob represents a delete job for the worker pool
type GCSDeleteJob struct {
	key    string
	result chan GCSDeleteResult
}

// GCSDeleteResult represents the result of a delete operation
type GCSDeleteResult struct {
	key   string
	error error
}

// NewEnhancedGCS creates a new enhanced GCS storage implementation
func NewEnhancedGCS(baseStorage storage.RemoteStorage, client *gcs.Client, cfg *config.Config) (*EnhancedGCS, error) {
	gcsConfig := &cfg.GCS

	// Validate configuration
	if gcsConfig.Bucket == "" {
		return nil, &OptimizationConfigError{
			Field:   "bucket",
			Value:   gcsConfig.Bucket,
			Message: "GCS bucket name cannot be empty",
		}
	}

	// Calculate optimal client pool size
	poolSize := gcsConfig.ClientPoolSize
	if poolSize <= 0 {
		poolSize = maxInt(10, runtime.NumCPU()*2) // At least 10, or 2x CPU cores
	}

	// Create client pool
	clientPool, err := NewGCSClientPool(client, poolSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client pool: %w", err)
	}

	// Create batch deleter if JSON API batching is enabled
	var batchDeleter *GCSBatchDeleter
	if cfg.GCS.BatchDeletion.UseBatchAPI {
		// Get token source from the client for batch operations
		tokenSource, err := getTokenSourceFromClient(client)
		if err != nil {
			return nil, fmt.Errorf("failed to get token source for batch operations: %w", err)
		}

		batchDeleter = NewGCSBatchDeleter(gcsConfig.Bucket, tokenSource, gcsConfig)
	}

	enhancedGCS := &EnhancedGCS{
		RemoteStorage:       baseStorage,
		client:              client,
		config:              gcsConfig,
		bucket:              gcsConfig.Bucket,
		clientPool:          clientPool,
		batchDeleter:        batchDeleter,
		batchDeletionConfig: &cfg.General.BatchDeletion,
		metrics:             &DeleteMetrics{},
	}

	return enhancedGCS, nil
}

// DeleteBatch implements BatchRemoteStorage interface for GCS delete operations
func (g *EnhancedGCS) DeleteBatch(ctx context.Context, keys []string) (*BatchResult, error) {
	startTime := time.Now()
	defer func() {
		g.metrics.TotalDuration = time.Since(startTime)
		g.updateThroughputMetrics()
	}()

	if len(keys) == 0 {
		return &BatchResult{SuccessCount: 0}, nil
	}

	g.metrics.FilesProcessed = int64(len(keys))

	// Choose between JSON API batching and high-concurrency parallel delete
	if g.config.BatchDeletion.UseBatchAPI && g.batchDeleter != nil {
		log.Debug().
			Int("key_count", len(keys)).
			Msg("starting GCS JSON API batch delete")

		result, err := g.batchDeleter.DeleteBatch(ctx, keys)
		if err != nil {
			// Fallback to parallel delete if batch fails
			log.Warn().
				Err(err).
				Msg("GCS batch delete failed, falling back to parallel delete")
			return g.deleteParallel(ctx, keys)
		}

		g.metrics.FilesDeleted = int64(result.SuccessCount)
		g.metrics.FilesFailed = int64(len(result.FailedKeys))
		g.metrics.APICallsCount++ // Batch counts as single API call

		return result, nil
	}

	// Use high-concurrency parallel delete
	log.Debug().
		Int("key_count", len(keys)).
		Msg("starting GCS high-concurrency parallel delete")

	return g.deleteParallel(ctx, keys)
}

// deleteParallel performs high-concurrency parallel delete operations
func (g *EnhancedGCS) deleteParallel(ctx context.Context, keys []string) (*BatchResult, error) {
	// Determine optimal worker count
	workerCount := g.getOptimalWorkerCount(len(keys))

	// Create job and result channels
	jobs := make(chan GCSDeleteJob, len(keys))
	results := make(chan GCSDeleteResult, len(keys))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go g.deleteWorker(ctx, jobs, &wg)
	}

	// Send jobs to workers
	for _, key := range keys {
		job := GCSDeleteJob{
			key:    key,
			result: results,
		}
		jobs <- job
	}
	close(jobs)

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var successCount int
	var failedKeys []FailedKey
	var errors []error

	for result := range results {
		if result.error != nil {
			failedKeys = append(failedKeys, FailedKey{
				Key:   result.key,
				Error: result.error,
			})
			errors = append(errors, fmt.Errorf("failed to delete %s: %w", result.key, result.error))
		} else {
			successCount++
		}
	}

	g.metrics.FilesDeleted = int64(successCount)
	g.metrics.FilesFailed = int64(len(failedKeys))

	batchResult := &BatchResult{
		SuccessCount: successCount,
		FailedKeys:   failedKeys,
		Errors:       errors,
	}

	log.Debug().
		Int("success_count", successCount).
		Int("failed_count", len(failedKeys)).
		Int("workers_used", workerCount).
		Msg("GCS parallel delete completed")

	return batchResult, nil
}

// deleteWorker processes delete jobs from the job channel
func (g *EnhancedGCS) deleteWorker(ctx context.Context, jobs <-chan GCSDeleteJob, wg *sync.WaitGroup) {
	defer wg.Done()

	// Get a client from the pool
	client := g.clientPool.Get()
	defer g.clientPool.Put(client)

	for job := range jobs {
		select {
		case <-ctx.Done():
			job.result <- GCSDeleteResult{
				key:   job.key,
				error: ctx.Err(),
			}
			return
		default:
			err := g.deleteObject(ctx, client, job.key)
			job.result <- GCSDeleteResult{
				key:   job.key,
				error: err,
			}
		}
	}
}

// deleteObject deletes a single object with retry logic
func (g *EnhancedGCS) deleteObject(ctx context.Context, client *gcs.Client, key string) error {
	obj := client.Bucket(g.bucket).Object(key)

	var err error
	maxRetries := 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err = obj.Delete(ctx)
		if err == nil {
			g.metrics.APICallsCount++
			return nil
		}

		// Check if object doesn't exist (which is actually success for delete)
		if err == gcs.ErrObjectNotExist {
			log.Debug().Str("key", key).Msg("object already deleted or doesn't exist")
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
				Str("key", key).
				Int("attempt", attempt+1).
				Str("backoff", backoff.String()).
				Msg("retrying GCS object delete")

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	g.metrics.APICallsCount++
	return fmt.Errorf("failed to delete object %s after %d attempts: %w", key, maxRetries+1, err)
}

// getOptimalWorkerCount determines the optimal number of workers based on job count and config
func (g *EnhancedGCS) getOptimalWorkerCount(jobCount int) int {
	// Start with configured max workers
	maxWorkers := g.config.BatchDeletion.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 50 // Default
	}

	// Don't create more workers than jobs
	if jobCount < maxWorkers {
		maxWorkers = jobCount
	}

	// Ensure we have at least 1 worker
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	// Don't exceed client pool size
	if maxWorkers > g.clientPool.size {
		maxWorkers = g.clientPool.size
	}

	return maxWorkers
}

// updateThroughputMetrics calculates and updates throughput metrics
func (g *EnhancedGCS) updateThroughputMetrics() {
	if g.metrics.TotalDuration > 0 && g.metrics.BytesDeleted > 0 {
		throughputBytes := float64(g.metrics.BytesDeleted) / g.metrics.TotalDuration.Seconds()
		g.metrics.ThroughputMBps = throughputBytes / (1024 * 1024) // Convert to MB/s
	}
}

// SupportsBatchDelete returns true if JSON API batching is enabled, otherwise false
func (g *EnhancedGCS) SupportsBatchDelete() bool {
	return g.config.BatchDeletion.UseBatchAPI && g.batchDeleter != nil
}

// GetOptimalBatchSize returns the optimal batch size based on the delete method
func (g *EnhancedGCS) GetOptimalBatchSize() int {
	if g.config.BatchDeletion.UseBatchAPI && g.batchDeleter != nil {
		// GCS JSON API supports up to 100 requests per batch
		return 100
	}
	// For parallel processing, return a reasonable chunk size
	return 50
}

// GetDeleteMetrics returns current delete operation metrics
func (g *EnhancedGCS) GetDeleteMetrics() *DeleteMetrics {
	return g.metrics
}

// ResetDeleteMetrics resets the delete operation metrics
func (g *EnhancedGCS) ResetDeleteMetrics() {
	g.metrics = &DeleteMetrics{}
}

// GCSClientPool implementation

// NewGCSClientPool creates a new GCS client pool
func NewGCSClientPool(baseClient *gcs.Client, size int) (*GCSClientPool, error) {
	if size <= 0 {
		return nil, fmt.Errorf("client pool size must be greater than 0")
	}

	pool := &GCSClientPool{
		clients: make(chan *gcs.Client, size),
		size:    size,
	}

	// Initialize pool with clients (for now, we'll reuse the base client)
	// In a production environment, you might want to create separate client instances
	for i := 0; i < size; i++ {
		pool.clients <- baseClient
	}

	return pool, nil
}

// Get retrieves a client from the pool
func (pool *GCSClientPool) Get() *gcs.Client {
	select {
	case client := <-pool.clients:
		return client
	default:
		// This shouldn't happen if pool size is managed correctly
		log.Warn().Msg("GCS client pool exhausted, this may indicate a resource leak")
		return nil
	}
}

// Put returns a client to the pool
func (pool *GCSClientPool) Put(client *gcs.Client) {
	if client == nil {
		return
	}

	select {
	case pool.clients <- client:
		// Successfully returned to pool
	default:
		// Pool is full, which shouldn't happen
		log.Warn().Msg("GCS client pool is full, dropping client")
	}
}

// Size returns the pool size
func (pool *GCSClientPool) Size() int {
	pool.mutex.RLock()
	defer pool.mutex.RUnlock()
	return pool.size
}

// Available returns the number of available clients in the pool
func (pool *GCSClientPool) Available() int {
	return len(pool.clients)
}

// Close closes the client pool and all clients
func (pool *GCSClientPool) Close() error {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	close(pool.clients)

	// Close all clients in the pool
	for client := range pool.clients {
		if err := client.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close GCS client")
		}
	}

	return nil
}

// optimizeBatchSize optimizes the batch size based on performance characteristics
func (g *EnhancedGCS) optimizeBatchSize(totalItems int) int {
	// For GCS, we don't use traditional batching but rather parallel processing
	// Return a reasonable chunk size based on total items and worker count

	workerCount := g.getOptimalWorkerCount(totalItems)
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

// validateGCSConnection validates the GCS connection and permissions
func (g *EnhancedGCS) validateGCSConnection(ctx context.Context) error {
	// Try to access bucket attributes to validate connection
	client := g.clientPool.Get()
	defer g.clientPool.Put(client)

	bucket := client.Bucket(g.bucket)
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("failed to access GCS bucket %s: %w", g.bucket, err)
	}

	return nil
}

// getTokenSourceFromClient extracts the token source from a GCS client
func getTokenSourceFromClient(client *gcs.Client) (oauth2.TokenSource, error) {
	// Unfortunately, the GCS client doesn't expose its token source directly
	// We need to create a new token source using the same authentication method
	// This is a limitation of the current GCS client library

	// For now, we'll create a default token source
	// In a production environment, you might want to pass the token source explicitly
	// or use the same credentials that were used to create the client

	// Use default credentials (same as what the GCS client would use)
	tokenSource, err := google.DefaultTokenSource(context.Background(), gcs.ScopeFullControl)
	if err != nil {
		return nil, fmt.Errorf("failed to create default token source: %w", err)
	}

	return tokenSource, nil
}

// maxInt returns the maximum of two integers
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
