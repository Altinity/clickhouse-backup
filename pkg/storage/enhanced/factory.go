package enhanced

import (
	"context"
	"fmt"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/rs/zerolog/log"
)

// EnhancedStorageFactory creates enhanced storage instances with proper client integration
type EnhancedStorageFactory struct {
	config *config.Config
}

// NewEnhancedStorageFactory creates a new enhanced storage factory
func NewEnhancedStorageFactory(cfg *config.Config) *EnhancedStorageFactory {
	return &EnhancedStorageFactory{config: cfg}
}

// CreateEnhancedWrapper creates an enhanced storage wrapper from a base storage
func (f *EnhancedStorageFactory) CreateEnhancedWrapper(ctx context.Context, baseStorage storage.RemoteStorage, opts *WrapperOptions) (*EnhancedStorageWrapper, error) {
	if baseStorage == nil {
		return nil, fmt.Errorf("base storage cannot be nil")
	}

	if opts == nil {
		opts = f.getDefaultWrapperOptions()
	}

	log.Debug().
		Str("storage_type", baseStorage.Kind()).
		Bool("cache_enabled", opts.EnableCache).
		Bool("metrics_enabled", opts.EnableMetrics).
		Msg("creating enhanced storage wrapper")

	wrapper, err := NewEnhancedStorageWrapper(baseStorage, f.config, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create enhanced storage wrapper: %w", err)
	}

	// Validate the configuration
	if err := wrapper.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("invalid enhanced storage configuration: %w", err)
	}

	log.Info().
		Str("storage_type", baseStorage.Kind()).
		Bool("optimizations_enabled", wrapper.IsOptimizationEnabled()).
		Int("batch_size", wrapper.GetBatchSize()).
		Int("workers", wrapper.GetWorkerCount()).
		Msg("enhanced storage wrapper created successfully")

	return wrapper, nil
}

// getDefaultWrapperOptions returns default wrapper options based on configuration
func (f *EnhancedStorageFactory) getDefaultWrapperOptions() *WrapperOptions {
	return &WrapperOptions{
		EnableCache:     f.config.DeleteOptimizations.CacheEnabled,
		CacheTTL:        f.config.DeleteOptimizations.CacheTTL.String(),
		EnableMetrics:   true,
		FallbackOnError: true,
		DisableEnhanced: !f.config.DeleteOptimizations.Enabled,
	}
}

// CreateForBackupDeletion creates an enhanced storage wrapper specifically for backup deletion
func (f *EnhancedStorageFactory) CreateForBackupDeletion(ctx context.Context, baseStorage storage.RemoteStorage) (*EnhancedStorageWrapper, error) {
	opts := &WrapperOptions{
		EnableCache:     f.config.DeleteOptimizations.CacheEnabled,
		CacheTTL:        f.config.DeleteOptimizations.CacheTTL.String(),
		EnableMetrics:   true,
		FallbackOnError: true,
		DisableEnhanced: !f.config.DeleteOptimizations.Enabled,
	}

	return f.CreateEnhancedWrapper(ctx, baseStorage, opts)
}

// CreateForObjectDiskCleanup creates an enhanced storage wrapper for object disk cleanup
func (f *EnhancedStorageFactory) CreateForObjectDiskCleanup(ctx context.Context, baseStorage storage.RemoteStorage) (*EnhancedStorageWrapper, error) {
	opts := &WrapperOptions{
		EnableCache:     false, // Cache may not be as useful for cleanup operations
		EnableMetrics:   true,
		FallbackOnError: true,
		DisableEnhanced: !f.config.DeleteOptimizations.Enabled,
	}

	return f.CreateEnhancedWrapper(ctx, baseStorage, opts)
}

// IsEnhancedDeleteSupported checks if enhanced delete is supported for the given storage type
func (f *EnhancedStorageFactory) IsEnhancedDeleteSupported(storageType string) bool {
	if !f.config.DeleteOptimizations.Enabled {
		return false
	}

	switch storageType {
	case "s3":
		return true
	case "gcs":
		return true
	case "azblob":
		return true
	default:
		// Enhanced delete can still provide benefits for other storage types
		// through parallel operations and caching
		return true
	}
}

// GetOptimalBatchSize returns the optimal batch size for the given storage type
func (f *EnhancedStorageFactory) GetOptimalBatchSize(storageType string) int {
	if !f.config.DeleteOptimizations.Enabled {
		return 1
	}

	// Check if custom batch size is configured
	if f.config.DeleteOptimizations.BatchSize > 0 {
		return f.config.DeleteOptimizations.BatchSize
	}

	// Return storage-specific optimal batch sizes
	switch storageType {
	case "s3":
		return 1000 // S3 batch delete API limit
	case "gcs":
		return 100 // Optimal for parallel GCS operations
	case "azblob":
		return 100 // Optimal for parallel Azure operations
	default:
		return 50 // Conservative default for other storage types
	}
}

// GetOptimalWorkerCount returns the optimal worker count for the given storage type
func (f *EnhancedStorageFactory) GetOptimalWorkerCount(storageType string) int {
	if !f.config.DeleteOptimizations.Enabled {
		return 1
	}

	// Check if custom worker count is configured
	if f.config.DeleteOptimizations.Workers > 0 {
		return f.config.DeleteOptimizations.Workers
	}

	// Return storage-specific optimal worker counts
	switch storageType {
	case "s3":
		if f.config.DeleteOptimizations.S3Optimizations.VersionConcurrency > 0 {
			return f.config.DeleteOptimizations.S3Optimizations.VersionConcurrency
		}
		return 10
	case "gcs":
		if f.config.DeleteOptimizations.GCSOptimizations.MaxWorkers > 0 {
			return f.config.DeleteOptimizations.GCSOptimizations.MaxWorkers
		}
		return 50
	case "azblob":
		if f.config.DeleteOptimizations.AzureOptimizations.MaxWorkers > 0 {
			return f.config.DeleteOptimizations.AzureOptimizations.MaxWorkers
		}
		return 20
	default:
		return 4 // Conservative default for other storage types
	}
}

// ValidateStorageOptimizations validates optimization settings for the given storage type
func (f *EnhancedStorageFactory) ValidateStorageOptimizations(storageType string) error {
	if !f.config.DeleteOptimizations.Enabled {
		return nil
	}

	// Validate general settings
	if f.config.DeleteOptimizations.BatchSize < 0 {
		return &OptimizationConfigError{
			Field:   "batch_size",
			Value:   f.config.DeleteOptimizations.BatchSize,
			Message: "batch size cannot be negative",
		}
	}

	if f.config.DeleteOptimizations.Workers < 0 {
		return &OptimizationConfigError{
			Field:   "workers",
			Value:   f.config.DeleteOptimizations.Workers,
			Message: "worker count cannot be negative",
		}
	}

	// Validate storage-specific settings
	switch storageType {
	case "s3":
		return f.validateS3Optimizations()
	case "gcs":
		return f.validateGCSOptimizations()
	case "azblob":
		return f.validateAzureOptimizations()
	}

	return nil
}

// validateS3Optimizations validates S3-specific optimization settings
func (f *EnhancedStorageFactory) validateS3Optimizations() error {
	s3Opts := f.config.DeleteOptimizations.S3Optimizations

	if s3Opts.VersionConcurrency < 0 {
		return &OptimizationConfigError{
			Field:   "s3_optimizations.version_concurrency",
			Value:   s3Opts.VersionConcurrency,
			Message: "version concurrency cannot be negative",
		}
	}

	if s3Opts.VersionConcurrency > 100 {
		return &OptimizationConfigError{
			Field:   "s3_optimizations.version_concurrency",
			Value:   s3Opts.VersionConcurrency,
			Message: "version concurrency should not exceed 100 to avoid overwhelming S3",
		}
	}

	return nil
}

// validateGCSOptimizations validates GCS-specific optimization settings
func (f *EnhancedStorageFactory) validateGCSOptimizations() error {
	gcsOpts := f.config.DeleteOptimizations.GCSOptimizations

	if gcsOpts.MaxWorkers < 0 {
		return &OptimizationConfigError{
			Field:   "gcs_optimizations.max_workers",
			Value:   gcsOpts.MaxWorkers,
			Message: "max workers cannot be negative",
		}
	}

	if gcsOpts.MaxWorkers > 100 {
		return &OptimizationConfigError{
			Field:   "gcs_optimizations.max_workers",
			Value:   gcsOpts.MaxWorkers,
			Message: "max workers should not exceed 100 to avoid rate limiting",
		}
	}

	return nil
}

// validateAzureOptimizations validates Azure-specific optimization settings
func (f *EnhancedStorageFactory) validateAzureOptimizations() error {
	azureOpts := f.config.DeleteOptimizations.AzureOptimizations

	if azureOpts.MaxWorkers < 0 {
		return &OptimizationConfigError{
			Field:   "azure_optimizations.max_workers",
			Value:   azureOpts.MaxWorkers,
			Message: "max workers cannot be negative",
		}
	}

	if azureOpts.MaxWorkers > 50 {
		return &OptimizationConfigError{
			Field:   "azure_optimizations.max_workers",
			Value:   azureOpts.MaxWorkers,
			Message: "max workers should not exceed 50 for Azure Blob to avoid throttling",
		}
	}

	return nil
}

// CreateMetricsCollector creates a metrics collector for enhanced storage operations
func (f *EnhancedStorageFactory) CreateMetricsCollector() *DeleteMetrics {
	return &DeleteMetrics{
		FilesProcessed: 0,
		FilesDeleted:   0,
		FilesFailed:    0,
		BytesDeleted:   0,
		APICallsCount:  0,
		TotalDuration:  0,
		ThroughputMBps: 0.0,
	}
}
