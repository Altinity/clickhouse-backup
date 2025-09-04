package enhanced

import (
	"context"
	"fmt"
	"io"

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

// createEnhancedStorage creates the appropriate enhanced storage implementation
func (w *EnhancedStorageWrapper) createEnhancedStorage(baseStorage storage.RemoteStorage, cfg *config.Config) (BatchRemoteStorage, error) {
	switch w.storageType {
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
	// We need to extract the S3 client from the base storage
	// This would require access to the internal client, which may require
	// modifying the base storage interface or using reflection

	// For now, we'll create a placeholder implementation
	// In a real implementation, you'd need to either:
	// 1. Modify the base storage to expose the client
	// 2. Use dependency injection to pass the client separately
	// 3. Create the client again here (less efficient)

	log.Debug().Msg("creating enhanced S3 storage (placeholder)")

	// This is a simplified approach - in practice you'd need the actual S3 client
	// For now, return nil to indicate enhanced storage isn't available
	return nil, fmt.Errorf("enhanced S3 requires access to underlying S3 client")
}

// createEnhancedGCS creates an enhanced GCS storage implementation
func (w *EnhancedStorageWrapper) createEnhancedGCS(baseStorage storage.RemoteStorage, cfg *config.Config) (BatchRemoteStorage, error) {
	log.Debug().Msg("creating enhanced GCS storage (placeholder)")

	// Similar to S3, we'd need access to the underlying GCS client
	// This would require modifying the storage architecture
	return nil, fmt.Errorf("enhanced GCS requires access to underlying GCS client")
}

// createEnhancedAzureBlob creates an enhanced Azure Blob storage implementation
func (w *EnhancedStorageWrapper) createEnhancedAzureBlob(baseStorage storage.RemoteStorage, cfg *config.Config) (BatchRemoteStorage, error) {
	log.Debug().Msg("creating enhanced Azure Blob storage (placeholder)")

	// Similar to others, we'd need access to the underlying Azure client
	return nil, fmt.Errorf("enhanced Azure Blob requires access to underlying container client")
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
	// This would call the existing delete logic from the base storage
	// The exact implementation depends on how the base storage handles backup deletion
	log.Debug().Str("backup", backupName).Msg("using fallback delete implementation")

	// For now, return an error indicating this needs to be implemented
	return fmt.Errorf("fallback delete implementation needs to be integrated with existing backup delete logic")
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
func (sf *StorageFactory) CreateEnhancedStorage(storageType string, opts *WrapperOptions) (*EnhancedStorageWrapper, error) {
	// This would integrate with the existing storage creation logic
	// For now, it's a placeholder that shows the intended interface

	log.Info().Str("storage_type", storageType).Msg("creating enhanced storage")

	// This would need to create the base storage first, then wrap it
	// The exact implementation depends on how storage is currently created in the project

	return nil, fmt.Errorf("storage factory needs integration with existing storage creation logic")
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
