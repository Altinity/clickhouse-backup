package enhanced

import (
	"context"
	"fmt"
	"math"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/rs/zerolog/log"
)

// BatchManager orchestrates parallel deletion workflows and manages worker pools
type BatchManager struct {
	config   *config.BatchDeletionConfig
	storage  BatchRemoteStorage
	metrics  *DeleteMetrics
	cache    *BackupExistenceCache
	progress *ProgressTracker
	mu       sync.RWMutex
}

// ProgressTracker tracks deletion progress and provides ETA calculations
type ProgressTracker struct {
	totalFiles     int64
	processedFiles int64
	startTime      time.Time
	lastUpdate     time.Time
	throughputMBps float64
	estimatedETA   time.Duration
	mu             sync.RWMutex
}

// BatchJob represents a batch deletion job
type BatchJob struct {
	ID       string
	Keys     []string
	Retry    int
	MaxRetry int
	Context  context.Context
}

// BatchJobResult represents the result of a batch job
type BatchJobResult struct {
	Job       *BatchJob
	Result    *BatchResult
	Error     error
	Duration  time.Duration
	Timestamp time.Time
}

// WorkerPool manages a pool of workers for batch operations
type WorkerPool struct {
	workerCount int
	jobQueue    chan *BatchJob
	resultQueue chan *BatchJobResult
	workers     []*Worker
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// Worker represents a single worker that processes batch jobs
type Worker struct {
	id      int
	manager *BatchManager
	pool    *WorkerPool
}

// NewBatchManager creates a new batch manager with the given configuration
func NewBatchManager(cfg *config.BatchDeletionConfig, storage BatchRemoteStorage, cache *BackupExistenceCache) *BatchManager {
	return &BatchManager{
		config:  cfg,
		storage: storage,
		metrics: &DeleteMetrics{},
		cache:   cache,
		progress: &ProgressTracker{
			startTime:  time.Now(),
			lastUpdate: time.Now(),
		},
	}
}

// DeleteBackupBatch orchestrates the deletion of a complete backup using batch operations
func (bm *BatchManager) DeleteBackupBatch(ctx context.Context, backupName string) error {
	log.Info().Str("backup", backupName).Msg("starting batch delete operation")

	bm.progress.reset()
	bm.metrics = &DeleteMetrics{}

	// Get file list from storage (this would typically come from the backup metadata)
	// For now, we'll simulate this step
	fileList, err := bm.getBackupFileList(ctx, backupName)
	if err != nil {
		return fmt.Errorf("failed to get backup file list: %w", err)
	}

	if len(fileList) == 0 {
		log.Info().Str("backup", backupName).Msg("no files to delete")
		return nil
	}

	bm.progress.setTotal(int64(len(fileList)))

	// Apply cache optimizations to skip unnecessary checks
	filteredList := bm.applyCacheOptimizations(fileList)

	log.Info().
		Str("backup", backupName).
		Int("total_files", len(fileList)).
		Int("filtered_files", len(filteredList)).
		Msg("prepared file list for deletion")

	// Divide into optimal batches based on storage type
	batches := bm.createOptimalBatches(filteredList)

	// Create and configure worker pool
	workerCount := bm.getOptimalWorkerCount()
	pool := bm.createWorkerPool(ctx, workerCount)
	defer pool.shutdown()

	// Execute batches with configured concurrency
	return bm.executeBatches(ctx, pool, batches, backupName)
}

// getBackupFileList retrieves the list of files for a backup
func (bm *BatchManager) getBackupFileList(ctx context.Context, backupName string) ([]string, error) {
	log.Debug().Str("backup", backupName).Msg("retrieving backup file list")

	var fileList []string

	// Walk the backup directory to collect all files
	err := bm.storage.Walk(ctx, backupName+"/", true, func(ctx context.Context, f storage.RemoteFile) error {
		// Construct full path like the original implementation does
		fullPath := path.Join(backupName, f.Name())
		fileList = append(fileList, fullPath)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk backup directory %s: %w", backupName, err)
	}

	log.Debug().
		Str("backup", backupName).
		Int("file_count", len(fileList)).
		Msg("collected backup file list")

	return fileList, nil
}

// applyCacheOptimizations uses cache to skip files that don't need to be deleted
func (bm *BatchManager) applyCacheOptimizations(fileList []string) []string {
	if bm.cache == nil {
		return fileList
	}

	var filteredList []string
	cacheHits := 0

	for _, file := range fileList {
		// Check if file existence is cached
		if metadata, exists := bm.cache.Get(file); exists {
			if metadata != nil {
				// File exists, include in deletion list
				filteredList = append(filteredList, file)
			}
			// File doesn't exist, skip it
			cacheHits++
		} else {
			// Not in cache, include in deletion list
			filteredList = append(filteredList, file)
		}
	}

	log.Debug().
		Int("cache_hits", cacheHits).
		Int("original_count", len(fileList)).
		Int("filtered_count", len(filteredList)).
		Msg("applied cache optimizations")

	return filteredList
}

// createOptimalBatches divides files into optimal batches based on storage characteristics
func (bm *BatchManager) createOptimalBatches(fileList []string) []*BatchJob {
	if len(fileList) == 0 {
		return []*BatchJob{}
	}

	batchSize := bm.storage.GetOptimalBatchSize()
	if batchSize <= 0 {
		batchSize = bm.config.BatchSize
	}

	var batches []*BatchJob
	for i := 0; i < len(fileList); i += batchSize {
		end := i + batchSize
		if end > len(fileList) {
			end = len(fileList)
		}

		batch := &BatchJob{
			ID:       fmt.Sprintf("batch-%d", len(batches)+1),
			Keys:     fileList[i:end],
			Retry:    0,
			MaxRetry: bm.config.RetryAttempts,
		}
		batches = append(batches, batch)
	}

	log.Debug().
		Int("total_files", len(fileList)).
		Int("batch_count", len(batches)).
		Int("batch_size", batchSize).
		Msg("created optimal batches")

	return batches
}

// getOptimalWorkerCount determines the optimal number of workers
func (bm *BatchManager) getOptimalWorkerCount() int {
	workers := bm.config.Workers
	if workers <= 0 {
		// Auto-detect based on storage type
		switch bm.storage.(type) {
		case *EnhancedS3:
			workers = 10 // S3 can handle higher concurrency
		case *EnhancedGCS:
			workers = 50 // GCS optimal from config
		case *EnhancedAzureBlob:
			workers = 20 // Azure conservative default
		default:
			workers = 4 // Safe default
		}
	}

	if workers > 100 {
		workers = 100 // Reasonable upper limit
	}

	return workers
}

// createWorkerPool creates and starts a worker pool
func (bm *BatchManager) createWorkerPool(ctx context.Context, workerCount int) *WorkerPool {
	ctx, cancel := context.WithCancel(ctx)

	pool := &WorkerPool{
		workerCount: workerCount,
		jobQueue:    make(chan *BatchJob, workerCount*2),
		resultQueue: make(chan *BatchJobResult, workerCount*2),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Start workers
	for i := 0; i < workerCount; i++ {
		worker := &Worker{
			id:      i,
			manager: bm,
			pool:    pool,
		}
		pool.workers = append(pool.workers, worker)
		pool.wg.Add(1)
		go worker.run()
	}

	log.Debug().Int("worker_count", workerCount).Msg("started worker pool")
	return pool
}

// executeBatches executes all batches using the worker pool and retries failed files
func (bm *BatchManager) executeBatches(ctx context.Context, pool *WorkerPool, batches []*BatchJob, backupName string) error {
	if len(batches) == 0 {
		return nil
	}

	// Execute initial batches and collect failed files with error info
	failedFilesWithErrors, err := bm.executeBatchRound(ctx, pool, batches, backupName, 1)
	if err != nil && bm.config.ErrorStrategy == "fail_fast" {
		return err
	}

	// Retry failed files until none remain or max retries reached
	retryRound := 1
	maxRetryRounds := 10 // Prevent infinite loops

	for len(failedFilesWithErrors) > 0 && retryRound < maxRetryRounds {
		retryRound++

		log.Info().
			Str("backup", backupName).
			Int("retry_round", retryRound).
			Int("failed_files", len(failedFilesWithErrors)).
			Msg("retrying failed files")

		// Filter to only retriable files
		retriableFiles := bm.filterRetriableFilesWithErrors(failedFilesWithErrors)
		if len(retriableFiles) == 0 {
			log.Info().Str("backup", backupName).Msg("no retriable failed files remaining")
			break
		}

		// Create new batches from failed files
		retryBatches := bm.createOptimalBatches(retriableFiles)

		// Create new worker pool for retry
		retryPool := bm.createWorkerPool(ctx, bm.getOptimalWorkerCount())

		// Execute retry round
		newFailedFilesWithErrors, retryErr := bm.executeBatchRound(ctx, retryPool, retryBatches, backupName, retryRound)
		retryPool.shutdown()

		if retryErr != nil && bm.config.ErrorStrategy == "fail_fast" {
			return retryErr
		}

		// Check if we made progress
		if len(newFailedFilesWithErrors) >= len(retriableFiles) {
			// No progress made, apply backoff
			backoffDuration := time.Duration(retryRound*retryRound) * time.Second
			if backoffDuration > 30*time.Second {
				backoffDuration = 30 * time.Second
			}

			log.Warn().
				Str("backup", backupName).
				Int("retry_round", retryRound).
				Str("backoff_duration", backoffDuration.String()).
				Msg("no progress made, applying backoff")

			select {
			case <-time.After(backoffDuration):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		failedFilesWithErrors = newFailedFilesWithErrors
	}

	// Final status
	if len(failedFilesWithErrors) > 0 {
		if retryRound >= maxRetryRounds {
			log.Error().
				Str("backup", backupName).
				Int("remaining_failed_files", len(failedFilesWithErrors)).
				Int("max_retry_rounds", maxRetryRounds).
				Msg("maximum retry rounds reached, some files could not be deleted")
		} else {
			log.Warn().
				Str("backup", backupName).
				Int("remaining_failed_files", len(failedFilesWithErrors)).
				Msg("some files could not be deleted (non-retriable errors)")
		}
	}

	// Get final metrics
	metrics := bm.GetDeleteMetrics()
	log.Info().
		Str("backup", backupName).
		Int64("success_count", metrics.FilesDeleted).
		Int64("failed_count", metrics.FilesFailed).
		Int("total_retry_rounds", retryRound).
		Str("duration", bm.progress.getTotalDuration().String()).
		Float64("throughput_mbps", bm.progress.getThroughputMBps()).
		Msg("batch delete operation completed")

	return nil
}

// FailedFileWithError represents a failed file with its associated error
type FailedFileWithError struct {
	FilePath string
	Error    error
}

// executeBatchRound executes a single round of batches and returns failed files with errors
func (bm *BatchManager) executeBatchRound(ctx context.Context, pool *WorkerPool, batches []*BatchJob, backupName string, round int) ([]FailedFileWithError, error) {
	if len(batches) == 0 {
		return nil, nil
	}

	// Start result collector
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)

	var finalError error
	var totalSuccess int64
	var totalFailed int64
	var allFailedFiles []FailedFileWithError
	var failedFilesMutex sync.Mutex

	go func() {
		defer collectorWg.Done()

		processedBatches := 0
		failedBatches := 0

		for result := range pool.resultQueue {
			processedBatches++

			if result.Error != nil {
				failedBatches++
				log.Debug().
					Err(result.Error).
					Str("batch_id", result.Job.ID).
					Int("round", round).
					Int("retry", result.Job.Retry).
					Msg("batch failed")

				// Handle error according to strategy
				if shouldRetryBatch(result, bm.config) {
					log.Debug().
						Str("batch_id", result.Job.ID).
						Int("round", round).
						Int("retry", result.Job.Retry+1).
						Msg("retrying failed batch within round")

					result.Job.Retry++
					pool.jobQueue <- result.Job
					processedBatches-- // Don't count this as processed yet
					continue
				}

				// Batch failed permanently, add all its files to failed list with the batch error
				failedFilesMutex.Lock()
				for _, filePath := range result.Job.Keys {
					allFailedFiles = append(allFailedFiles, FailedFileWithError{
						FilePath: filePath,
						Error:    result.Error,
					})
				}
				failedFilesMutex.Unlock()

				if bm.config.ErrorStrategy == "fail_fast" {
					finalError = result.Error
					pool.cancel() // Stop all workers
					break
				}
			} else {
				// Batch succeeded, but check for individual file failures
				atomic.AddInt64(&totalSuccess, int64(result.Result.SuccessCount))
				atomic.AddInt64(&totalFailed, int64(len(result.Result.FailedKeys)))

				// Collect individual failed files with their specific errors
				if len(result.Result.FailedKeys) > 0 {
					failedFilesMutex.Lock()
					for _, failedKey := range result.Result.FailedKeys {
						allFailedFiles = append(allFailedFiles, FailedFileWithError{
							FilePath: failedKey.Key,
							Error:    failedKey.Error,
						})
					}
					failedFilesMutex.Unlock()
				}

				// Update progress
				bm.progress.updateProgress(int64(len(result.Job.Keys)))

				// Update metrics
				bm.updateMetrics(result.Result)
			}

			// Check if we've processed all batches
			if processedBatches >= len(batches) {
				break
			}

			// Check failure threshold
			if bm.shouldStopDueToFailures(failedBatches, len(batches)) {
				finalError = fmt.Errorf("stopping due to failure threshold: %d of %d batches failed",
					failedBatches, len(batches))
				pool.cancel()
				break
			}
		}
	}()

	// Send all jobs to workers
	go func() {
		defer close(pool.jobQueue)
		for _, batch := range batches {
			batch.Context = ctx
			select {
			case pool.jobQueue <- batch:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for all results to be processed
	collectorWg.Wait()
	close(pool.resultQueue)

	log.Debug().
		Str("backup", backupName).
		Int("round", round).
		Int64("success_count", totalSuccess).
		Int64("failed_count", totalFailed).
		Int("collected_failed_files", len(allFailedFiles)).
		Msg("batch round completed")

	return allFailedFiles, finalError
}

// filterRetriableFilesWithErrors filters failed files to only include those that should be retried
func (bm *BatchManager) filterRetriableFilesWithErrors(failedFiles []FailedFileWithError) []string {
	var retriableFiles []string
	var nonRetriableCount int

	for _, failedFile := range failedFiles {
		if IsRetriableFileError(failedFile.FilePath, failedFile.Error) {
			retriableFiles = append(retriableFiles, failedFile.FilePath)
		} else {
			nonRetriableCount++
			log.Debug().
				Str("file", failedFile.FilePath).
				Err(failedFile.Error).
				Msg("file marked as non-retriable")
		}
	}

	log.Debug().
		Int("total_failed", len(failedFiles)).
		Int("retriable", len(retriableFiles)).
		Int("non_retriable", nonRetriableCount).
		Msg("filtered failed files for retry")

	return retriableFiles
}

// shouldRetryBatch determines if a batch should be retried
func shouldRetryBatch(result *BatchJobResult, config *config.BatchDeletionConfig) bool {
	if result.Job.Retry >= result.Job.MaxRetry {
		return false
	}

	if config.ErrorStrategy != "retry_batch" {
		return false
	}

	// Only retry on retriable errors
	return IsRetriableError(result.Error)
}

// shouldStopDueToFailures checks if we should stop due to failure threshold
func (bm *BatchManager) shouldStopDueToFailures(failedBatches, totalBatches int) bool {
	if totalBatches == 0 {
		return false
	}

	failureRate := float64(failedBatches) / float64(totalBatches)
	return failureRate > bm.config.FailureThreshold
}

// updateMetrics aggregates metrics from batch results
func (bm *BatchManager) updateMetrics(result *BatchResult) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.metrics.FilesDeleted += int64(result.SuccessCount)
	bm.metrics.FilesFailed += int64(len(result.FailedKeys))
	bm.metrics.APICallsCount++
}

// GetDeleteMetrics returns aggregated metrics
func (bm *BatchManager) GetDeleteMetrics() *DeleteMetrics {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	metrics := *bm.metrics
	metrics.TotalDuration = bm.progress.getTotalDuration()
	metrics.ThroughputMBps = bm.progress.getThroughputMBps()
	metrics.FilesProcessed = bm.progress.getProcessedFiles()

	return &metrics
}

// GetProgress returns current progress information
func (bm *BatchManager) GetProgress() (processed, total int64, eta time.Duration) {
	return bm.progress.getProgress()
}

// Worker implementation

// run starts the worker and processes jobs from the queue
func (w *Worker) run() {
	defer w.pool.wg.Done()

	log.Debug().Int("worker_id", w.id).Msg("worker started")
	defer log.Debug().Int("worker_id", w.id).Msg("worker stopped")

	for {
		select {
		case job := <-w.pool.jobQueue:
			if job == nil {
				return
			}
			w.processJob(job)
		case <-w.pool.ctx.Done():
			return
		}
	}
}

// processJob processes a single batch job
func (w *Worker) processJob(job *BatchJob) {
	startTime := time.Now()

	log.Debug().
		Int("worker_id", w.id).
		Str("batch_id", job.ID).
		Int("file_count", len(job.Keys)).
		Msg("processing batch")

	result, err := w.manager.storage.DeleteBatch(job.Context, job.Keys)
	duration := time.Since(startTime)

	jobResult := &BatchJobResult{
		Job:       job,
		Result:    result,
		Error:     err,
		Duration:  duration,
		Timestamp: time.Now(),
	}

	select {
	case w.pool.resultQueue <- jobResult:
	case <-w.pool.ctx.Done():
		return
	}
}

// WorkerPool methods

// shutdown gracefully shuts down the worker pool
func (wp *WorkerPool) shutdown() {
	wp.cancel()
	wp.wg.Wait()
	log.Debug().Msg("worker pool shut down")
}

// ProgressTracker methods

// reset resets the progress tracker
func (pt *ProgressTracker) reset() {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.totalFiles = 0
	pt.processedFiles = 0
	pt.startTime = time.Now()
	pt.lastUpdate = time.Now()
	pt.throughputMBps = 0
	pt.estimatedETA = 0
}

// setTotal sets the total number of files to process
func (pt *ProgressTracker) setTotal(total int64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.totalFiles = total
}

// updateProgress updates the progress with newly processed files
func (pt *ProgressTracker) updateProgress(processed int64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.processedFiles += processed
	pt.lastUpdate = time.Now()

	// Calculate ETA
	if pt.processedFiles > 0 {
		elapsed := time.Since(pt.startTime)
		rate := float64(pt.processedFiles) / elapsed.Seconds()
		remaining := pt.totalFiles - pt.processedFiles

		if rate > 0 && remaining > 0 {
			pt.estimatedETA = time.Duration(float64(remaining)/rate) * time.Second
		}
	}
}

// getProgress returns current progress information
func (pt *ProgressTracker) getProgress() (processed, total int64, eta time.Duration) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return pt.processedFiles, pt.totalFiles, pt.estimatedETA
}

// getTotalDuration returns the total duration since start
func (pt *ProgressTracker) getTotalDuration() time.Duration {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return time.Since(pt.startTime)
}

// getProcessedFiles returns the number of processed files
func (pt *ProgressTracker) getProcessedFiles() int64 {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return pt.processedFiles
}

// getThroughputMBps calculates and returns throughput in MB/s
func (pt *ProgressTracker) getThroughputMBps() float64 {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	elapsed := time.Since(pt.startTime)
	if elapsed.Seconds() == 0 {
		return 0
	}

	// This would ideally use actual bytes processed, but for now we'll estimate
	// based on files processed (assuming average file size)
	filesPerSecond := float64(pt.processedFiles) / elapsed.Seconds()
	return filesPerSecond // Placeholder calculation
}

// GetProgressPercentage returns the completion percentage
func (pt *ProgressTracker) GetProgressPercentage() float64 {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	if pt.totalFiles == 0 {
		return 0
	}

	return math.Min(100.0, (float64(pt.processedFiles)/float64(pt.totalFiles))*100.0)
}
