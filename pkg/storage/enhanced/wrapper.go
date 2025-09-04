package enhanced

import (
	"context"
	"fmt"
	"io"
	"strings"

	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/rs/zerolog/log"
)

// EnhancedStorageWrapper wraps existing storage implementations with enhanced capabilities
type EnhancedStorageWrapper struct {
	storage.RemoteStorage
	enhanced    BatchRemoteStorage
	config      *config.Config
	batchMgr    *BatchManager
	cache       *BackupExistenceCache
	storageType string
}

// WrapperOptions contains options for creating enhanced storage wrappers
type WrapperOptions struct {
	EnableCache     bool
	CacheTTL        string
	EnableMetrics   bool
	FallbackOnError bool
	DisableEnhanced bool
}

// NewEnhancedStorageWrapper creates a new enhanced storage wrapper
func NewEnhancedStorageWrapper(baseStorage storage.RemoteStorage, cfg *config.Config, opts *WrapperOptions) (*EnhancedStorageWrapper, error) {
	if baseStorage == nil {
		return nil, fmt.Errorf("base storage cannot be nil")
	}

	storageKind := baseStorage.Kind()

	wrapper := &EnhancedStorageWrapper{
		RemoteStorage: baseStorage,
		config:        cfg,
		storageType:   storageKind,
	}

	// Initialize cache if enabled
	if opts != nil && opts.EnableCache && cfg.DeleteOptimizations.CacheEnabled {
		wrapper.cache = NewBackupExistenceCache(cfg.DeleteOptimizations.CacheTTL)
	}

	// Create enhanced storage implementation if optimizations are enabled
	if !cfg.DeleteOptimizations.Enabled || (opts != nil && opts.DisableEnhanced) {
		log.Debug().Str("storage", storageKind).Msg("enhanced delete optimizations disabled")
		return wrapper, nil
	}

	// Detect storage type and create appropriate enhanced implementation
	enhanced, err := wrapper.createEnhancedStorage(baseStorage, cfg)
	if err != nil {
		if opts != nil && opts.FallbackOnError {
			log.Warn().Err(err).Str("storage", storageKind).Msg("failed to create enhanced storage, falling back to base implementation")
			return wrapper, nil
		}
		return nil, fmt.Errorf("failed to create enhanced storage for %s: %w", storageKind, err)
	}

	wrapper.enhanced = enhanced

	// Create batch manager
	wrapper.batchMgr = NewBatchManager(&cfg.DeleteOptimizations, enhanced, wrapper.cache)

	log.Info().Str("storage", storageKind).Msg("enhanced storage wrapper created successfully")
	return wrapper, nil
}

// S3StorageAdapter adapts base storage to provide enhanced S3 batch operations
type S3StorageAdapter struct {
	storage.RemoteStorage
	config  *config.S3Config
	bucket  string
	metrics *DeleteMetrics
	mu      sync.RWMutex
}

// GCSStorageAdapter adapts base storage to provide enhanced GCS batch operations
type GCSStorageAdapter struct {
	storage.RemoteStorage
	config  *config.GCSConfig
	bucket  string
	metrics *DeleteMetrics
	mu      sync.RWMutex
}

// AzureBlobStorageAdapter adapts base storage to provide enhanced Azure Blob batch operations
type AzureBlobStorageAdapter struct {
	storage.RemoteStorage
	config    *config.AzureBlobConfig
	container string
	metrics   *DeleteMetrics
	mu        sync.RWMutex
}

// DeleteBatch implements BatchRemoteStorage for S3 adapter
func (s *S3StorageAdapter) DeleteBatch(ctx context.Context, keys []string) (*BatchResult, error) {
	startTime := time.Now()
	defer func() {
		s.mu.Lock()
		s.metrics.TotalDuration = time.Since(startTime)
		s.mu.Unlock()
	}()

	s.mu.Lock()
	s.metrics.FilesProcessed = int64(len(keys))
	s.mu.Unlock()

	// Use parallel deletion for better performance
	return s.deleteParallel(ctx, keys)
}

// deleteParallel performs parallel deletion using goroutines
func (s *S3StorageAdapter) deleteParallel(ctx context.Context, keys []string) (*BatchResult, error) {
	const maxWorkers = 10
	workerCount := maxWorkers
	if len(keys) < workerCount {
		workerCount = len(keys)
	}

	jobs := make(chan string, len(keys))
	results := make(chan struct {
		key   string
		err   error
		bytes int64
	}, len(keys))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				// Get file size before deletion for metrics
				var fileSize int64
				if fileInfo, statErr := s.RemoteStorage.StatFile(ctx, key); statErr == nil {
					fileSize = fileInfo.Size()
				}

				err := s.RemoteStorage.DeleteFile(ctx, key)
				results <- struct {
					key   string
					err   error
					bytes int64
				}{key: key, err: err, bytes: fileSize}
			}
		}()
	}

	// Send jobs
	go func() {
		defer close(jobs)
		for _, key := range keys {
			select {
			case jobs <- key:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for workers
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var successCount int
	var failedKeys []FailedKey
	var errors []error
	var totalBytes int64

	for result := range results {
		if result.err != nil {
			failedKeys = append(failedKeys, FailedKey{
				Key:   result.key,
				Error: result.err,
			})
			errors = append(errors, result.err)
		} else {
			successCount++
			totalBytes += result.bytes
		}
	}

	s.mu.Lock()
	s.metrics.FilesDeleted = int64(successCount)
	s.metrics.FilesFailed = int64(len(failedKeys))
	s.metrics.BytesDeleted += totalBytes
	s.metrics.APICallsCount += int64(len(keys))
	s.mu.Unlock()

	return &BatchResult{
		SuccessCount: successCount,
		FailedKeys:   failedKeys,
		Errors:       errors,
	}, nil
}

// SupportsBatchDelete returns true for S3 adapter
func (s *S3StorageAdapter) SupportsBatchDelete() bool {
	return true
}

// GetOptimalBatchSize returns optimal batch size for S3
func (s *S3StorageAdapter) GetOptimalBatchSize() int {
	return 1000
}

// DeleteBatch implements BatchRemoteStorage for GCS adapter
func (g *GCSStorageAdapter) DeleteBatch(ctx context.Context, keys []string) (*BatchResult, error) {
	return g.deleteParallel(ctx, keys)
}

// deleteParallel performs parallel deletion for GCS
func (g *GCSStorageAdapter) deleteParallel(ctx context.Context, keys []string) (*BatchResult, error) {
	const maxWorkers = 20
	workerCount := maxWorkers
	if len(keys) < workerCount {
		workerCount = len(keys)
	}

	jobs := make(chan string, len(keys))
	results := make(chan struct {
		key   string
		err   error
		bytes int64
	}, len(keys))

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				// Get file size before deletion for metrics
				var fileSize int64
				if fileInfo, statErr := g.RemoteStorage.StatFile(ctx, key); statErr == nil {
					fileSize = fileInfo.Size()
				}

				err := g.RemoteStorage.DeleteFile(ctx, key)
				results <- struct {
					key   string
					err   error
					bytes int64
				}{key: key, err: err, bytes: fileSize}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, key := range keys {
			select {
			case jobs <- key:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var successCount int
	var failedKeys []FailedKey
	var errors []error
	var totalBytes int64

	for result := range results {
		if result.err != nil {
			failedKeys = append(failedKeys, FailedKey{
				Key:   result.key,
				Error: result.err,
			})
			errors = append(errors, result.err)
		} else {
			successCount++
			totalBytes += result.bytes
		}
	}

	g.mu.Lock()
	g.metrics.FilesDeleted += int64(successCount)
	g.metrics.FilesFailed += int64(len(failedKeys))
	g.metrics.BytesDeleted += totalBytes
	g.metrics.APICallsCount += int64(len(keys))
	g.mu.Unlock()

	return &BatchResult{
		SuccessCount: successCount,
		FailedKeys:   failedKeys,
		Errors:       errors,
	}, nil
}

// SupportsBatchDelete returns false for GCS (parallel only)
func (g *GCSStorageAdapter) SupportsBatchDelete() bool {
	return false
}

// GetOptimalBatchSize returns optimal batch size for GCS
func (g *GCSStorageAdapter) GetOptimalBatchSize() int {
	return 100
}

// DeleteBatch implements BatchRemoteStorage for Azure Blob adapter
func (a *AzureBlobStorageAdapter) DeleteBatch(ctx context.Context, keys []string) (*BatchResult, error) {
	return a.deleteParallel(ctx, keys)
}

// deleteParallel performs parallel deletion for Azure Blob
func (a *AzureBlobStorageAdapter) deleteParallel(ctx context.Context, keys []string) (*BatchResult, error) {
	const maxWorkers = 15
	workerCount := maxWorkers
	if len(keys) < workerCount {
		workerCount = len(keys)
	}

	jobs := make(chan string, len(keys))
	results := make(chan struct {
		key   string
		err   error
		bytes int64
	}, len(keys))

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				// Get file size before deletion for metrics
				var fileSize int64
				if fileInfo, statErr := a.RemoteStorage.StatFile(ctx, key); statErr == nil {
					fileSize = fileInfo.Size()
				}

				err := a.RemoteStorage.DeleteFile(ctx, key)
				results <- struct {
					key   string
					err   error
					bytes int64
				}{key: key, err: err, bytes: fileSize}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, key := range keys {
			select {
			case jobs <- key:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var successCount int
	var failedKeys []FailedKey
	var errors []error
	var totalBytes int64

	for result := range results {
		if result.err != nil {
			failedKeys = append(failedKeys, FailedKey{
				Key:   result.key,
				Error: result.err,
			})
			errors = append(errors, result.err)
		} else {
			successCount++
			totalBytes += result.bytes
		}
	}

	a.mu.Lock()
	a.metrics.FilesDeleted += int64(successCount)
	a.metrics.FilesFailed += int64(len(failedKeys))
	a.metrics.BytesDeleted += totalBytes
	a.metrics.APICallsCount += int64(len(keys))
	a.mu.Unlock()

	return &BatchResult{
		SuccessCount: successCount,
		FailedKeys:   failedKeys,
		Errors:       errors,
	}, nil
}

// SupportsBatchDelete returns false for Azure Blob (parallel only)
func (a *AzureBlobStorageAdapter) SupportsBatchDelete() bool {
	return false
}

// GetOptimalBatchSize returns optimal batch size for Azure Blob
func (a *AzureBlobStorageAdapter) GetOptimalBatchSize() int {
	return 100
}

// createEnhancedStorage creates the appropriate enhanced storage implementation
func (w *EnhancedStorageWrapper) createEnhancedStorage(baseStorage storage.RemoteStorage, cfg *config.Config) (BatchRemoteStorage, error) {
	switch strings.ToLower(w.storageType) {
	case "s3":
		return w.createEnhancedS3(baseStorage, cfg)
	case "gcs":
		return w.createEnhancedGCS(baseStorage, cfg)
	case "azblob":
		return w.createEnhancedAzureBlob(baseStorage, cfg)
	default:
		return nil, fmt.Errorf("enhanced storage not supported for type: %s", w.storageType)
	}
}

// createEnhancedS3 creates an enhanced S3 storage implementation
func (w *EnhancedStorageWrapper) createEnhancedS3(baseStorage storage.RemoteStorage, cfg *config.Config) (BatchRemoteStorage, error) {
	log.Debug().Msg("creating enhanced S3 storage")

	// Try to create the actual enhanced S3 implementation with native batch operations
	// For now, fall back to adapter since we can't easily access the S3 client from base storage
	enhancedS3 := &S3StorageAdapter{
		RemoteStorage: baseStorage,
		config:        &cfg.S3,
		bucket:        cfg.S3.Bucket,
		metrics:       &DeleteMetrics{},
	}

	log.Info().Str("bucket", cfg.S3.Bucket).Msg("enhanced S3 storage adapter created")
	return enhancedS3, nil
}

// createEnhancedGCS creates an enhanced GCS storage implementation
func (w *EnhancedStorageWrapper) createEnhancedGCS(baseStorage storage.RemoteStorage, cfg *config.Config) (BatchRemoteStorage, error) {
	log.Debug().Msg("creating enhanced GCS storage")

	// Create a wrapper that implements BatchRemoteStorage using the base storage
	enhancedGCS := &GCSStorageAdapter{
		RemoteStorage: baseStorage,
		config:        &cfg.GCS,
		bucket:        cfg.GCS.Bucket,
		metrics:       &DeleteMetrics{},
	}

	log.Info().Str("bucket", cfg.GCS.Bucket).Msg("enhanced GCS storage created")
	return enhancedGCS, nil
}

// createEnhancedAzureBlob creates an enhanced Azure Blob storage implementation
func (w *EnhancedStorageWrapper) createEnhancedAzureBlob(baseStorage storage.RemoteStorage, cfg *config.Config) (BatchRemoteStorage, error) {
	log.Debug().Msg("creating enhanced Azure Blob storage")

	// Create a wrapper that implements BatchRemoteStorage using the base storage
	enhancedAzure := &AzureBlobStorageAdapter{
		RemoteStorage: baseStorage,
		config:        &cfg.AzureBlob,
		container:     cfg.AzureBlob.Container,
		metrics:       &DeleteMetrics{},
	}

	log.Info().Str("container", cfg.AzureBlob.Container).Msg("enhanced Azure Blob storage created")
	return enhancedAzure, nil
}

// EnhancedDeleteBackup performs enhanced delete if available, otherwise falls back to base implementation
func (w *EnhancedStorageWrapper) EnhancedDeleteBackup(ctx context.Context, backupName string) error {
	if w.enhanced == nil || w.batchMgr == nil {
		log.Debug().Str("backup", backupName).Msg("using base storage delete implementation")
		return w.deleteBackupFallback(ctx, backupName)
	}

	log.Info().Str("backup", backupName).Msg("using enhanced batch delete")

	// Use batch manager for orchestrated deletion
	err := w.batchMgr.DeleteBackupBatch(ctx, backupName)
	if err != nil {
		log.Warn().Err(err).Str("backup", backupName).Msg("enhanced delete failed, trying fallback")
		return w.deleteBackupFallback(ctx, backupName)
	}

	return nil
}

// deleteBackupFallback falls back to the base storage implementation
func (w *EnhancedStorageWrapper) deleteBackupFallback(ctx context.Context, backupName string) error {
	log.Debug().Str("backup", backupName).Msg("using fallback delete implementation")

	// Get list of files for the backup
	files, err := w.listBackupFiles(ctx, backupName)
	if err != nil {
		return fmt.Errorf("failed to list backup files: %w", err)
	}

	// Delete files sequentially using base storage
	var errors []error
	for _, file := range files {
		if err := w.RemoteStorage.DeleteFile(ctx, file); err != nil {
			errors = append(errors, fmt.Errorf("failed to delete %s: %w", file, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to delete %d files: %v", len(errors), errors)
	}

	return nil
}

// listBackupFiles lists all files belonging to a backup
func (w *EnhancedStorageWrapper) listBackupFiles(ctx context.Context, backupName string) ([]string, error) {
	var files []string

	// List all files with the backup prefix
	err := w.RemoteStorage.Walk(ctx, backupName+"/", true, func(ctx context.Context, r storage.RemoteFile) error {
		files = append(files, r.Name())
		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

// DeleteBatch implements BatchRemoteStorage interface by routing to enhanced storage
func (w *EnhancedStorageWrapper) DeleteBatch(ctx context.Context, keys []string) (*BatchResult, error) {
	if w.enhanced == nil {
		return w.deleteKeysSequentially(ctx, keys)
	}

	return w.enhanced.DeleteBatch(ctx, keys)
}

// deleteKeysSequentially provides fallback for batch delete when enhanced storage isn't available
func (w *EnhancedStorageWrapper) deleteKeysSequentially(ctx context.Context, keys []string) (*BatchResult, error) {
	result := &BatchResult{
		SuccessCount: 0,
		FailedKeys:   []FailedKey{},
		Errors:       []error{},
	}

	for _, key := range keys {
		err := w.RemoteStorage.DeleteFile(ctx, key)
		if err != nil {
			result.FailedKeys = append(result.FailedKeys, FailedKey{
				Key:   key,
				Error: err,
			})
			result.Errors = append(result.Errors, err)
		} else {
			result.SuccessCount++
		}
	}

	return result, nil
}

// SupportsBatchDelete returns true if enhanced storage supports batch delete
func (w *EnhancedStorageWrapper) SupportsBatchDelete() bool {
	if w.enhanced == nil {
		return false
	}
	return w.enhanced.SupportsBatchDelete()
}

// GetOptimalBatchSize returns optimal batch size for the storage type
func (w *EnhancedStorageWrapper) GetOptimalBatchSize() int {
	if w.enhanced == nil {
		return w.config.DeleteOptimizations.BatchSize
	}
	return w.enhanced.GetOptimalBatchSize()
}

// GetDeleteMetrics returns delete operation metrics
func (w *EnhancedStorageWrapper) GetDeleteMetrics() *DeleteMetrics {
	if w.batchMgr != nil {
		return w.batchMgr.GetDeleteMetrics()
	}
	return &DeleteMetrics{}
}

// GetProgress returns current deletion progress if available
func (w *EnhancedStorageWrapper) GetProgress() (processed, total int64, eta string) {
	if w.batchMgr == nil {
		return 0, 0, "unknown"
	}

	p, t, etaDuration := w.batchMgr.GetProgress()
	return p, t, etaDuration.String()
}

// IsOptimizationEnabled returns true if delete optimizations are enabled
func (w *EnhancedStorageWrapper) IsOptimizationEnabled() bool {
	return w.config.DeleteOptimizations.Enabled && w.enhanced != nil
}

// GetBatchSize returns the configured batch size
func (w *EnhancedStorageWrapper) GetBatchSize() int {
	return w.config.DeleteOptimizations.BatchSize
}

// GetWorkerCount returns the configured worker count
func (w *EnhancedStorageWrapper) GetWorkerCount() int {
	return w.config.DeleteOptimizations.Workers
}

// GetBackupFromCache retrieves backup metadata from cache
func (w *EnhancedStorageWrapper) GetBackupFromCache(backupName string) (*BackupMetadata, bool) {
	if w.cache == nil {
		return nil, false
	}
	return w.cache.Get(backupName)
}

// SetBackupInCache stores backup metadata in cache
func (w *EnhancedStorageWrapper) SetBackupInCache(backupName string, metadata *BackupMetadata) {
	if w.cache != nil {
		w.cache.Set(backupName, metadata)
	}
}

// InvalidateBackupCache removes backup from cache
func (w *EnhancedStorageWrapper) InvalidateBackupCache(backupName string) {
	if w.cache != nil {
		w.cache.Invalidate(backupName)
	}
}

// Close closes the enhanced storage wrapper and cleans up resources
func (w *EnhancedStorageWrapper) Close(ctx context.Context) error {
	var errs []error

	// Close cache if exists
	if w.cache != nil {
		// Cache doesn't have a close method in our implementation
		// but we could add cleanup here if needed
	}

	// Close enhanced storage if it exists and has a close method
	if closer, ok := w.enhanced.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close enhanced storage: %w", err))
		}
	}

	// Close base storage
	if err := w.RemoteStorage.Close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("failed to close base storage: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing enhanced storage wrapper: %v", errs)
	}

	return nil
}

// ValidateConfig validates the enhanced storage configuration
func (w *EnhancedStorageWrapper) ValidateConfig() error {
	if !w.config.DeleteOptimizations.Enabled {
		return nil // No validation needed if optimizations are disabled
	}

	// Validate batch size
	if w.config.DeleteOptimizations.BatchSize <= 0 {
		return &OptimizationConfigError{
			Field:   "batch_size",
			Value:   w.config.DeleteOptimizations.BatchSize,
			Message: "batch size must be greater than 0",
		}
	}

	// Validate worker count
	if w.config.DeleteOptimizations.Workers < 0 {
		return &OptimizationConfigError{
			Field:   "workers",
			Value:   w.config.DeleteOptimizations.Workers,
			Message: "worker count cannot be negative",
		}
	}

	// Validate failure threshold
	if w.config.DeleteOptimizations.FailureThreshold < 0 || w.config.DeleteOptimizations.FailureThreshold > 1 {
		return &OptimizationConfigError{
			Field:   "failure_threshold",
			Value:   w.config.DeleteOptimizations.FailureThreshold,
			Message: "failure threshold must be between 0 and 1",
		}
	}

	// Validate error strategy
	validStrategies := map[string]bool{
		"fail_fast":   true,
		"continue":    true,
		"retry_batch": true,
	}
	if !validStrategies[w.config.DeleteOptimizations.ErrorStrategy] {
		return &OptimizationConfigError{
			Field:   "error_strategy",
			Value:   w.config.DeleteOptimizations.ErrorStrategy,
			Message: "error strategy must be one of: fail_fast, continue, retry_batch",
		}
	}

	return nil
}

// StorageFactory provides factory methods for creating enhanced storage wrappers
type StorageFactory struct {
	config *config.Config
}

// NewStorageFactory creates a new storage factory
func NewStorageFactory(config *config.Config) *StorageFactory {
	return &StorageFactory{config: config}
}

// CreateEnhancedStorage creates an enhanced storage wrapper for the given storage type
func (sf *StorageFactory) CreateEnhancedStorage(baseStorage storage.RemoteStorage, opts *WrapperOptions) (*EnhancedStorageWrapper, error) {
	if baseStorage == nil {
		return nil, fmt.Errorf("base storage cannot be nil")
	}

	log.Info().Str("storage_type", baseStorage.Kind()).Msg("creating enhanced storage wrapper")

	// Create the enhanced wrapper with the base storage
	wrapper, err := NewEnhancedStorageWrapper(baseStorage, sf.config, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create enhanced storage wrapper: %w", err)
	}

	// Validate configuration
	if err := wrapper.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return wrapper, nil
}

// GetSupportedStorageTypes returns list of storage types that support enhancements
func (sf *StorageFactory) GetSupportedStorageTypes() []string {
	return []string{"s3", "gcs", "azblob"}
}

// IsStorageSupported checks if a storage type supports enhancements
func (sf *StorageFactory) IsStorageSupported(storageType string) bool {
	supported := sf.GetSupportedStorageTypes()
	for _, s := range supported {
		if s == storageType {
			return true
		}
	}
	return false
}
