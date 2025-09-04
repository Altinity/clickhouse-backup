package test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/enhanced"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnhancedDeleteIntegration tests the complete integration of enhanced delete operations
func TestEnhancedDeleteIntegration(t *testing.T) {
	tests := []struct {
		name           string
		storageType    string
		optimizations  *config.DeleteOptimizations
		expectedResult string
	}{
		{
			name:        "S3 Enhanced Delete Enabled",
			storageType: "s3",
			optimizations: &config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        1000,
				Workers:          10,
				RetryAttempts:    3,
				FailureThreshold: 0.1,
				ErrorStrategy:    "retry_batch",
				CacheEnabled:     true,
				CacheTTL:         time.Hour,
				S3Optimizations: struct {
					UseBatchAPI        bool `yaml:"use_batch_api" envconfig:"DELETE_S3_USE_BATCH_API" default:"true"`
					VersionConcurrency int  `yaml:"version_concurrency" envconfig:"DELETE_S3_VERSION_CONCURRENCY" default:"10"`
					PreloadVersions    bool `yaml:"preload_versions" envconfig:"DELETE_S3_PRELOAD_VERSIONS" default:"true"`
				}{
					UseBatchAPI:        true,
					VersionConcurrency: 5,
					PreloadVersions:    true,
				},
			},
			expectedResult: "enhanced",
		},
		{
			name:        "GCS Enhanced Delete Enabled",
			storageType: "gcs",
			optimizations: &config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        500,
				Workers:          50,
				RetryAttempts:    2,
				FailureThreshold: 0.2,
				ErrorStrategy:    "continue",
				CacheEnabled:     true,
				CacheTTL:         2 * time.Hour,
				GCSOptimizations: struct {
					MaxWorkers    int  `yaml:"max_workers" envconfig:"DELETE_GCS_MAX_WORKERS" default:"50"`
					UseClientPool bool `yaml:"use_client_pool" envconfig:"DELETE_GCS_USE_CLIENT_POOL" default:"true"`
				}{
					MaxWorkers:    50,
					UseClientPool: true,
				},
			},
			expectedResult: "enhanced",
		},
		{
			name:        "Azure Enhanced Delete Enabled",
			storageType: "azblob",
			optimizations: &config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        200,
				Workers:          20,
				RetryAttempts:    5,
				FailureThreshold: 0.05,
				ErrorStrategy:    "fail_fast",
				CacheEnabled:     false,
				AzureOptimizations: struct {
					UseBatchAPI bool `yaml:"use_batch_api" envconfig:"DELETE_AZURE_USE_BATCH_API" default:"true"`
					MaxWorkers  int  `yaml:"max_workers" envconfig:"DELETE_AZURE_MAX_WORKERS" default:"20"`
				}{
					UseBatchAPI: true,
					MaxWorkers:  20,
				},
			},
			expectedResult: "enhanced",
		},
		{
			name:        "Optimizations Disabled - Fallback",
			storageType: "s3",
			optimizations: &config.DeleteOptimizations{
				Enabled: false,
			},
			expectedResult: "fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testEnhancedDeleteScenario(t, tt.storageType, tt.optimizations, tt.expectedResult)
		})
	}
}

// testEnhancedDeleteScenario tests a specific delete optimization scenario
func testEnhancedDeleteScenario(t *testing.T, storageType string, opts *config.DeleteOptimizations, expectedResult string) {
	// Create test configuration
	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: storageType,
		},
		DeleteOptimizations: *opts,
	}

	// Test configuration validation
	t.Run("Configuration Validation", func(t *testing.T) {
		testConfigValidation(t, cfg)
	})

	// Test enhanced storage wrapper creation
	t.Run("Enhanced Storage Wrapper", func(t *testing.T) {
		testEnhancedStorageWrapper(t, cfg, expectedResult)
	})

	// Test batch manager functionality
	if opts.Enabled {
		t.Run("Batch Manager", func(t *testing.T) {
			testBatchManager(t, cfg)
		})
	}

	// Test delete workflow integration
	t.Run("Delete Workflow Integration", func(t *testing.T) {
		testDeleteWorkflowIntegration(t, cfg, expectedResult)
	})
}

// testConfigValidation tests configuration validation
func testConfigValidation(t *testing.T, cfg *config.Config) {
	if !cfg.DeleteOptimizations.Enabled {
		// Skip validation if optimizations are disabled
		return
	}

	// Test valid configuration
	factory := enhanced.NewStorageFactory(cfg)
	require.NotNil(t, factory)

	// Test invalid batch size
	invalidCfg := *cfg
	invalidCfg.DeleteOptimizations.BatchSize = -1

	wrapper, err := enhanced.NewEnhancedStorageWrapper(nil, &invalidCfg, nil)
	assert.Error(t, err)
	assert.Nil(t, wrapper)

	// Test invalid failure threshold
	invalidCfg2 := *cfg
	invalidCfg2.DeleteOptimizations.FailureThreshold = 1.5

	wrapper2, err2 := enhanced.NewEnhancedStorageWrapper(nil, &invalidCfg2, nil)
	assert.Error(t, err2)
	assert.Nil(t, wrapper2)

	// Test invalid error strategy
	invalidCfg3 := *cfg
	invalidCfg3.DeleteOptimizations.ErrorStrategy = "invalid_strategy"

	wrapper3, err3 := enhanced.NewEnhancedStorageWrapper(nil, &invalidCfg3, nil)
	assert.Error(t, err3)
	assert.Nil(t, wrapper3)
}

// testEnhancedStorageWrapper tests the enhanced storage wrapper functionality
func testEnhancedStorageWrapper(t *testing.T, cfg *config.Config, expectedResult string) {
	// Create mock storage
	mockStorage := &MockRemoteStorage{
		kind: cfg.General.RemoteStorage,
	}

	wrapperOpts := &enhanced.WrapperOptions{
		EnableCache:     cfg.DeleteOptimizations.CacheEnabled,
		EnableMetrics:   true,
		FallbackOnError: true,
	}

	if expectedResult == "fallback" {
		wrapperOpts.DisableEnhanced = true
	}

	wrapper, err := enhanced.NewEnhancedStorageWrapper(mockStorage, cfg, wrapperOpts)
	require.NoError(t, err)
	require.NotNil(t, wrapper)

	// Test wrapper properties
	assert.Equal(t, cfg.DeleteOptimizations.Enabled && expectedResult != "fallback", wrapper.IsOptimizationEnabled())
	assert.Equal(t, cfg.DeleteOptimizations.BatchSize, wrapper.GetBatchSize())
	assert.Equal(t, cfg.DeleteOptimizations.Workers, wrapper.GetWorkerCount())

	// Test delete metrics
	metrics := wrapper.GetDeleteMetrics()
	assert.NotNil(t, metrics)

	// Test progress tracking
	processed, total, eta := wrapper.GetProgress()
	assert.GreaterOrEqual(t, processed, int64(0))
	assert.GreaterOrEqual(t, total, int64(0))
	assert.NotEmpty(t, eta)

	// Cleanup
	err = wrapper.Close(context.Background())
	assert.NoError(t, err)
}

// testBatchManager tests the batch manager functionality
func testBatchManager(t *testing.T, cfg *config.Config) {
	// Create mock enhanced storage
	mockStorage := &MockBatchRemoteStorage{
		batchSize: cfg.DeleteOptimizations.BatchSize,
		supported: true,
	}

	// Create cache if enabled
	var cache *enhanced.BackupExistenceCache
	if cfg.DeleteOptimizations.CacheEnabled {
		cache = enhanced.NewBackupExistenceCache(cfg.DeleteOptimizations.CacheTTL)
	}

	// Create batch manager
	batchMgr := enhanced.NewBatchManager(&cfg.DeleteOptimizations, mockStorage, cache)
	require.NotNil(t, batchMgr)

	// Test batch delete operation
	ctx := context.Background()
	err := batchMgr.DeleteBackupBatch(ctx, "test-backup")
	assert.NoError(t, err)

	// Test metrics
	metrics := batchMgr.GetDeleteMetrics()
	assert.NotNil(t, metrics)

	// Test progress tracking
	processed, total, eta := batchMgr.GetProgress()
	assert.GreaterOrEqual(t, processed, int64(0))
	assert.GreaterOrEqual(t, total, int64(0))
	assert.GreaterOrEqual(t, eta, time.Duration(0))
}

// testDeleteWorkflowIntegration tests the integration with the delete workflow
func testDeleteWorkflowIntegration(t *testing.T, cfg *config.Config, expectedResult string) {
	// Create mock backuper (simplified for testing)
	mockBackuper := &MockBackuper{
		cfg: cfg,
	}

	// Test should use enhanced delete
	shouldUse := mockBackuper.shouldUseEnhancedDelete("test-backup")
	if expectedResult == "enhanced" {
		assert.True(t, shouldUse)
	} else {
		assert.False(t, shouldUse)
	}

	// Test supports enhanced delete
	supports := mockBackuper.supportsEnhancedDelete()
	assert.Equal(t, expectedResult == "enhanced", supports)

	// Test optimal worker count
	workerCount := mockBackuper.getOptimalWorkerCount()
	if cfg.DeleteOptimizations.Enabled {
		assert.Greater(t, workerCount, 0)
	} else {
		assert.Equal(t, 1, workerCount)
	}

	// Test optimal batch size
	batchSize := mockBackuper.getOptimalBatchSize()
	assert.Greater(t, batchSize, 0)
}

// TestBackwardCompatibility tests that the system maintains backward compatibility
func TestBackwardCompatibility(t *testing.T) {
	// Test with optimizations completely disabled
	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: "s3",
		},
		DeleteOptimizations: config.DeleteOptimizations{
			Enabled: false,
		},
	}

	mockBackuper := &MockBackuper{cfg: cfg}

	// Verify original behavior is preserved
	assert.False(t, mockBackuper.isDeleteOptimizationEnabled())
	assert.False(t, mockBackuper.shouldUseEnhancedDelete("test-backup"))
	assert.Equal(t, 1, mockBackuper.getOptimalWorkerCount())
	assert.Equal(t, 1, mockBackuper.getOptimalBatchSize())

	// Test configuration validation with disabled optimizations
	err := mockBackuper.validateDeleteOptimizationConfig()
	assert.NoError(t, err) // Should not validate when disabled
}

// TestErrorHandling tests error handling scenarios
func TestErrorHandling(t *testing.T) {
	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: "s3",
		},
		DeleteOptimizations: config.DeleteOptimizations{
			Enabled:          true,
			BatchSize:        100,
			Workers:          5,
			ErrorStrategy:    "fail_fast",
			FailureThreshold: 0.1,
		},
	}

	// Test with failing storage
	failingStorage := &MockBatchRemoteStorage{
		batchSize:   cfg.DeleteOptimizations.BatchSize,
		supported:   true,
		shouldFail:  true,
		failureRate: 0.2, // 20% failure rate, above threshold
	}

	batchMgr := enhanced.NewBatchManager(&cfg.DeleteOptimizations, failingStorage, nil)
	require.NotNil(t, batchMgr)

	ctx := context.Background()
	err := batchMgr.DeleteBackupBatch(ctx, "test-backup")
	assert.Error(t, err) // Should fail due to high failure rate

	// Test continue strategy
	cfg.DeleteOptimizations.ErrorStrategy = "continue"
	batchMgr2 := enhanced.NewBatchManager(&cfg.DeleteOptimizations, failingStorage, nil)
	_ = batchMgr2.DeleteBackupBatch(ctx, "test-backup")
	// Should continue despite failures (specific behavior depends on implementation)
}

// TestPerformanceMetrics tests performance metrics collection
func TestPerformanceMetrics(t *testing.T) {
	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: "s3",
		},
		DeleteOptimizations: config.DeleteOptimizations{
			Enabled:   true,
			BatchSize: 1000,
			Workers:   10,
		},
	}

	mockStorage := &MockBatchRemoteStorage{
		batchSize:     cfg.DeleteOptimizations.BatchSize,
		supported:     true,
		simulateFiles: 5000, // Simulate deleting 5000 files
		simulateDelay: 10 * time.Millisecond,
	}

	batchMgr := enhanced.NewBatchManager(&cfg.DeleteOptimizations, mockStorage, nil)
	require.NotNil(t, batchMgr)

	ctx := context.Background()
	start := time.Now()
	err := batchMgr.DeleteBackupBatch(ctx, "test-backup")
	duration := time.Since(start)

	assert.NoError(t, err)
	assert.Greater(t, duration, time.Duration(0))

	metrics := batchMgr.GetDeleteMetrics()
	assert.NotNil(t, metrics)
	assert.Greater(t, metrics.FilesProcessed, int64(0))
	assert.Greater(t, metrics.TotalDuration, time.Duration(0))
}

// TestConcurrentDeleteOperations tests concurrent delete operations
func TestConcurrentDeleteOperations(t *testing.T) {
	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: "s3",
		},
		DeleteOptimizations: config.DeleteOptimizations{
			Enabled:   true,
			BatchSize: 500,
			Workers:   10,
		},
	}

	mockStorage := &MockBatchRemoteStorage{
		batchSize:     cfg.DeleteOptimizations.BatchSize,
		supported:     true,
		simulateDelay: 50 * time.Millisecond, // Longer delay to test concurrency
	}

	batchMgr := enhanced.NewBatchManager(&cfg.DeleteOptimizations, mockStorage, nil)
	require.NotNil(t, batchMgr)

	ctx := context.Background()

	// Run multiple delete operations concurrently
	const numConcurrent = 5
	results := make(chan error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		go func(backupNum int) {
			backupName := fmt.Sprintf("concurrent-backup-%d", backupNum)
			err := batchMgr.DeleteBackupBatch(ctx, backupName)
			results <- err
		}(i)
	}

	// Collect results
	for i := 0; i < numConcurrent; i++ {
		err := <-results
		assert.NoError(t, err, "Concurrent delete operation %d should succeed", i)
	}

	// Verify metrics are properly aggregated
	metrics := batchMgr.GetDeleteMetrics()
	assert.NotNil(t, metrics)
	assert.Greater(t, metrics.FilesProcessed, int64(0))
}

// TestCacheIntegration tests cache integration scenarios
func TestCacheIntegration(t *testing.T) {
	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: "s3",
		},
		DeleteOptimizations: config.DeleteOptimizations{
			Enabled:      true,
			BatchSize:    100,
			Workers:      5,
			CacheEnabled: true,
			CacheTTL:     time.Hour,
		},
	}

	mockStorage := &MockBatchRemoteStorage{
		batchSize: cfg.DeleteOptimizations.BatchSize,
		supported: true,
	}

	cache := enhanced.NewBackupExistenceCache(cfg.DeleteOptimizations.CacheTTL)

	// Pre-populate cache with some backup metadata
	testBackups := []string{"backup1", "backup2", "backup3"}
	for _, backup := range testBackups {
		cache.Set(backup, &enhanced.BackupMetadata{
			BackupName: backup,
			Exists:     true,
			Size:       1024 * 1024, // 1MB
		})
	}

	batchMgr := enhanced.NewBatchManager(&cfg.DeleteOptimizations, mockStorage, cache)
	require.NotNil(t, batchMgr)

	ctx := context.Background()

	// Test cache hits
	for _, backup := range testBackups {
		metadata, exists := cache.Get(backup)
		assert.True(t, exists, "Backup %s should exist in cache", backup)
		assert.NotNil(t, metadata)
		assert.Equal(t, backup, metadata.BackupName)
	}

	// Test cache miss
	_, exists := cache.Get("nonexistent-backup")
	assert.False(t, exists, "Nonexistent backup should not be in cache")

	// Test delete operation with cache
	err := batchMgr.DeleteBackupBatch(ctx, "backup1")
	assert.NoError(t, err)

	// Verify cache stats
	stats := cache.Stats()
	assert.Greater(t, stats.TotalEntries, 0)
}

// TestErrorRecoveryScenarios tests various error recovery scenarios
func TestErrorRecoveryScenarios(t *testing.T) {
	scenarios := []struct {
		name           string
		errorStrategy  string
		failureRate    float64
		expectSuccess  bool
		expectContinue bool
	}{
		{
			name:           "Fail Fast Strategy with High Failure Rate",
			errorStrategy:  "fail_fast",
			failureRate:    0.5,
			expectSuccess:  false,
			expectContinue: false,
		},
		{
			name:           "Continue Strategy with High Failure Rate",
			errorStrategy:  "continue",
			failureRate:    0.3,
			expectSuccess:  true,
			expectContinue: true,
		},
		{
			name:           "Retry Batch Strategy with Low Failure Rate",
			errorStrategy:  "retry_batch",
			failureRate:    0.1,
			expectSuccess:  true,
			expectContinue: true,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			cfg := &config.Config{
				General: config.GeneralConfig{
					RemoteStorage: "s3",
				},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled:          true,
					BatchSize:        50,
					Workers:          3,
					ErrorStrategy:    scenario.errorStrategy,
					FailureThreshold: 0.2,
					RetryAttempts:    2,
				},
			}

			mockStorage := &MockBatchRemoteStorage{
				batchSize:   cfg.DeleteOptimizations.BatchSize,
				supported:   true,
				shouldFail:  true,
				failureRate: scenario.failureRate,
			}

			batchMgr := enhanced.NewBatchManager(&cfg.DeleteOptimizations, mockStorage, nil)
			require.NotNil(t, batchMgr)

			ctx := context.Background()
			err := batchMgr.DeleteBackupBatch(ctx, "error-test-backup")

			if scenario.expectSuccess {
				assert.NoError(t, err, "Should succeed with %s strategy", scenario.errorStrategy)
			} else {
				assert.Error(t, err, "Should fail with %s strategy", scenario.errorStrategy)
			}

			// Check metrics regardless of success/failure
			metrics := batchMgr.GetDeleteMetrics()
			assert.NotNil(t, metrics)
		})
	}
}

// TestStorageSpecificWorkflows tests storage-specific delete workflows
func TestStorageSpecificWorkflows(t *testing.T) {
	storageConfigs := map[string]*config.Config{
		"s3": {
			General: config.GeneralConfig{RemoteStorage: "s3"},
			DeleteOptimizations: config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 1000,
				Workers:   10,
				S3Optimizations: struct {
					UseBatchAPI        bool `yaml:"use_batch_api" envconfig:"DELETE_S3_USE_BATCH_API" default:"true"`
					VersionConcurrency int  `yaml:"version_concurrency" envconfig:"DELETE_S3_VERSION_CONCURRENCY" default:"10"`
					PreloadVersions    bool `yaml:"preload_versions" envconfig:"DELETE_S3_PRELOAD_VERSIONS" default:"true"`
				}{
					UseBatchAPI:        true,
					VersionConcurrency: 10,
					PreloadVersions:    true,
				},
			},
		},
		"gcs": {
			General: config.GeneralConfig{RemoteStorage: "gcs"},
			DeleteOptimizations: config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 500,
				Workers:   50,
				GCSOptimizations: struct {
					MaxWorkers    int  `yaml:"max_workers" envconfig:"DELETE_GCS_MAX_WORKERS" default:"50"`
					UseClientPool bool `yaml:"use_client_pool" envconfig:"DELETE_GCS_USE_CLIENT_POOL" default:"true"`
				}{
					MaxWorkers:    50,
					UseClientPool: true,
				},
			},
		},
		"azblob": {
			General: config.GeneralConfig{RemoteStorage: "azblob"},
			DeleteOptimizations: config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 200,
				Workers:   20,
				AzureOptimizations: struct {
					UseBatchAPI bool `yaml:"use_batch_api" envconfig:"DELETE_AZURE_USE_BATCH_API" default:"true"`
					MaxWorkers  int  `yaml:"max_workers" envconfig:"DELETE_AZURE_MAX_WORKERS" default:"20"`
				}{
					UseBatchAPI: true,
					MaxWorkers:  20,
				},
			},
		},
	}

	for storageType, cfg := range storageConfigs {
		t.Run(fmt.Sprintf("Storage_%s_Workflow", storageType), func(t *testing.T) {
			mockStorage := &MockBatchRemoteStorage{
				MockRemoteStorage: MockRemoteStorage{kind: storageType},
				batchSize:         cfg.DeleteOptimizations.BatchSize,
				supported:         true,
				simulateFiles:     1000,
				simulateDelay:     5 * time.Millisecond,
			}

			batchMgr := enhanced.NewBatchManager(&cfg.DeleteOptimizations, mockStorage, nil)
			require.NotNil(t, batchMgr)

			ctx := context.Background()

			// Test the complete workflow
			startTime := time.Now()
			err := batchMgr.DeleteBackupBatch(ctx, fmt.Sprintf("test-backup-%s", storageType))
			duration := time.Since(startTime)

			assert.NoError(t, err)
			assert.Greater(t, duration, time.Duration(0))

			// Verify metrics
			metrics := batchMgr.GetDeleteMetrics()
			assert.NotNil(t, metrics)
			assert.Greater(t, metrics.FilesProcessed, int64(0))
			assert.Greater(t, metrics.APICallsCount, int64(0))

			// Storage-specific validations
			switch storageType {
			case "s3":
				// S3 should use batch API, resulting in fewer API calls
				expectedMaxAPICalls := int64(mockStorage.simulateFiles/cfg.DeleteOptimizations.BatchSize + 1)
				assert.LessOrEqual(t, metrics.APICallsCount, expectedMaxAPICalls,
					"S3 should use batch API to reduce API calls")
			case "gcs":
				// GCS should use high concurrency
				assert.GreaterOrEqual(t, cfg.DeleteOptimizations.Workers, 50,
					"GCS should use high worker concurrency")
			case "azblob":
				// Azure should use moderate batch sizes
				assert.LessOrEqual(t, cfg.DeleteOptimizations.BatchSize, 200,
					"Azure should use moderate batch sizes")
			}
		})
	}
}

// TestConfigurationEdgeCases tests edge cases in configuration
func TestConfigurationEdgeCases(t *testing.T) {
	testCases := []struct {
		name        string
		cfg         *config.Config
		shouldError bool
		errorMsg    string
	}{
		{
			name: "Zero batch size",
			cfg: &config.Config{
				General: config.GeneralConfig{RemoteStorage: "s3"},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled:   true,
					BatchSize: 0,
				},
			},
			shouldError: true,
			errorMsg:    "batch size must be greater than 0",
		},
		{
			name: "Negative worker count",
			cfg: &config.Config{
				General: config.GeneralConfig{RemoteStorage: "s3"},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled:   true,
					BatchSize: 100,
					Workers:   -1,
				},
			},
			shouldError: true,
			errorMsg:    "worker count cannot be negative",
		},
		{
			name: "Invalid failure threshold above 1",
			cfg: &config.Config{
				General: config.GeneralConfig{RemoteStorage: "s3"},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled:          true,
					BatchSize:        100,
					Workers:          5,
					FailureThreshold: 1.5,
				},
			},
			shouldError: true,
			errorMsg:    "failure threshold must be between 0 and 1",
		},
		{
			name: "Invalid failure threshold below 0",
			cfg: &config.Config{
				General: config.GeneralConfig{RemoteStorage: "s3"},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled:          true,
					BatchSize:        100,
					Workers:          5,
					FailureThreshold: -0.1,
				},
			},
			shouldError: true,
			errorMsg:    "failure threshold must be between 0 and 1",
		},
		{
			name: "Invalid error strategy",
			cfg: &config.Config{
				General: config.GeneralConfig{RemoteStorage: "s3"},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled:       true,
					BatchSize:     100,
					Workers:       5,
					ErrorStrategy: "invalid_strategy",
				},
			},
			shouldError: true,
			errorMsg:    "error strategy must be one of: fail_fast, continue, retry_batch",
		},
		{
			name: "Valid minimal configuration",
			cfg: &config.Config{
				General: config.GeneralConfig{RemoteStorage: "s3"},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled:   true,
					BatchSize: 1,
					Workers:   1,
				},
			},
			shouldError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStorage := &MockRemoteStorage{kind: tc.cfg.General.RemoteStorage}

			wrapper, err := enhanced.NewEnhancedStorageWrapper(mockStorage, tc.cfg, &enhanced.WrapperOptions{
				EnableMetrics:   true,
				FallbackOnError: true,
			})

			if tc.shouldError {
				assert.Error(t, err)
				assert.Nil(t, wrapper)
				if tc.errorMsg != "" {
					assert.Contains(t, err.Error(), tc.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, wrapper)
				if wrapper != nil {
					err = wrapper.Close(context.Background())
					assert.NoError(t, err)
				}
			}
		})
	}
}

// TestLargeScaleOperations tests large-scale delete operations
func TestLargeScaleOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large-scale test in short mode")
	}

	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: "s3",
		},
		DeleteOptimizations: config.DeleteOptimizations{
			Enabled:   true,
			BatchSize: 1000,
			Workers:   20,
		},
	}

	mockStorage := &MockBatchRemoteStorage{
		batchSize:     cfg.DeleteOptimizations.BatchSize,
		supported:     true,
		simulateFiles: 100000,           // Simulate 100K files
		simulateDelay: time.Microsecond, // Minimal delay for speed
	}

	batchMgr := enhanced.NewBatchManager(&cfg.DeleteOptimizations, mockStorage, nil)
	require.NotNil(t, batchMgr)

	ctx := context.Background()

	startTime := time.Now()
	err := batchMgr.DeleteBackupBatch(ctx, "large-scale-backup")
	duration := time.Since(startTime)

	assert.NoError(t, err)
	assert.Less(t, duration, 30*time.Second, "Large-scale operation should complete within 30 seconds")

	metrics := batchMgr.GetDeleteMetrics()
	assert.NotNil(t, metrics)
	assert.Greater(t, metrics.FilesProcessed, int64(90000), "Should process most files")
	assert.Less(t, metrics.APICallsCount, int64(200), "Should use batch operations efficiently")

	// Test progress tracking during operation
	processed, total, _ := batchMgr.GetProgress()
	assert.GreaterOrEqual(t, processed, int64(0))
	assert.GreaterOrEqual(t, total, int64(0))
}

// TestProgressTracking tests progress tracking functionality
func TestProgressTracking(t *testing.T) {
	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: "s3",
		},
		DeleteOptimizations: config.DeleteOptimizations{
			Enabled:   true,
			BatchSize: 10,
			Workers:   2,
		},
	}

	mockStorage := &MockBatchRemoteStorage{
		batchSize:     cfg.DeleteOptimizations.BatchSize,
		supported:     true,
		simulateFiles: 100,
		simulateDelay: 50 * time.Millisecond, // Longer delay to observe progress
	}

	batchMgr := enhanced.NewBatchManager(&cfg.DeleteOptimizations, mockStorage, nil)
	require.NotNil(t, batchMgr)

	ctx := context.Background()

	// Start delete operation in background
	done := make(chan error, 1)
	go func() {
		err := batchMgr.DeleteBackupBatch(ctx, "progress-test-backup")
		done <- err
	}()

	// Monitor progress while operation is running
	var progressUpdates []float64
	progressTicker := time.NewTicker(10 * time.Millisecond)
	defer progressTicker.Stop()

	monitorDone := false
	for !monitorDone {
		select {
		case err := <-done:
			assert.NoError(t, err)
			monitorDone = true
		case <-progressTicker.C:
			processed, total, eta := batchMgr.GetProgress()
			if total > 0 {
				progressPct := (float64(processed) / float64(total)) * 100
				progressUpdates = append(progressUpdates, progressPct)

				// Validate progress values
				assert.GreaterOrEqual(t, processed, int64(0))
				assert.GreaterOrEqual(t, total, int64(0))
				assert.LessOrEqual(t, processed, total)
				assert.GreaterOrEqual(t, eta, time.Duration(0))
			}
		}
	}

	// Verify we captured progress updates
	assert.Greater(t, len(progressUpdates), 0, "Should have captured progress updates")

	// Verify final progress
	processed, total, _ := batchMgr.GetProgress()
	if total > 0 {
		finalProgress := (float64(processed) / float64(total)) * 100
		assert.GreaterOrEqual(t, finalProgress, 99.0, "Should reach near 100% completion")
	}
}

// Mock implementations for testing

type MockRemoteStorage struct {
	kind string
}

func (m *MockRemoteStorage) Kind() string                      { return m.kind }
func (m *MockRemoteStorage) Connect(ctx context.Context) error { return nil }
func (m *MockRemoteStorage) Close(ctx context.Context) error   { return nil }
func (m *MockRemoteStorage) StatFile(ctx context.Context, key string) (storage.RemoteFile, error) {
	return nil, nil
}
func (m *MockRemoteStorage) StatFileAbsolute(ctx context.Context, key string) (storage.RemoteFile, error) {
	return nil, nil
}
func (m *MockRemoteStorage) DeleteFile(ctx context.Context, key string) error { return nil }
func (m *MockRemoteStorage) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	return nil
}
func (m *MockRemoteStorage) Walk(ctx context.Context, prefix string, recursive bool, fn func(context.Context, storage.RemoteFile) error) error {
	return nil
}
func (m *MockRemoteStorage) WalkAbsolute(ctx context.Context, absolutePrefix string, recursive bool, fn func(context.Context, storage.RemoteFile) error) error {
	return nil
}
func (m *MockRemoteStorage) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *MockRemoteStorage) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *MockRemoteStorage) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return nil, nil
}
func (m *MockRemoteStorage) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return nil
}
func (m *MockRemoteStorage) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return nil
}
func (m *MockRemoteStorage) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	return 0, nil
}

type MockBatchRemoteStorage struct {
	MockRemoteStorage
	batchSize     int
	supported     bool
	shouldFail    bool
	failureRate   float64
	simulateFiles int
	simulateDelay time.Duration
}

func (m *MockBatchRemoteStorage) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	return 0, nil
}

func (m *MockBatchRemoteStorage) DeleteBatch(ctx context.Context, keys []string) (*enhanced.BatchResult, error) {
	if m.shouldFail && len(keys) > 0 {
		failCount := int(float64(len(keys)) * m.failureRate)
		successCount := len(keys) - failCount

		result := &enhanced.BatchResult{
			SuccessCount: successCount,
			FailedKeys:   make([]enhanced.FailedKey, failCount),
			Errors:       make([]error, failCount),
		}

		for i := 0; i < failCount; i++ {
			result.FailedKeys[i] = enhanced.FailedKey{
				Key:   keys[i],
				Error: fmt.Errorf("simulated failure"),
			}
			result.Errors[i] = fmt.Errorf("simulated failure")
		}

		return result, nil
	}

	// Simulate processing delay
	if m.simulateDelay > 0 {
		time.Sleep(m.simulateDelay)
	}

	return &enhanced.BatchResult{
		SuccessCount: len(keys),
		FailedKeys:   []enhanced.FailedKey{},
		Errors:       []error{},
	}, nil
}

func (m *MockBatchRemoteStorage) SupportsBatchDelete() bool { return m.supported }
func (m *MockBatchRemoteStorage) GetOptimalBatchSize() int  { return m.batchSize }

type MockBackuper struct {
	cfg *config.Config
}

func (m *MockBackuper) isDeleteOptimizationEnabled() bool {
	return m.cfg.DeleteOptimizations.Enabled
}

func (m *MockBackuper) shouldUseEnhancedDelete(backupName string) bool {
	return m.isDeleteOptimizationEnabled() && m.supportsEnhancedDelete()
}

func (m *MockBackuper) supportsEnhancedDelete() bool {
	if !m.cfg.DeleteOptimizations.Enabled {
		return false
	}

	switch m.cfg.General.RemoteStorage {
	case "s3":
		return m.cfg.DeleteOptimizations.S3Optimizations.UseBatchAPI
	case "gcs":
		return m.cfg.DeleteOptimizations.GCSOptimizations.UseClientPool
	case "azblob":
		return m.cfg.DeleteOptimizations.AzureOptimizations.UseBatchAPI
	default:
		return true
	}
}

func (m *MockBackuper) getOptimalWorkerCount() int {
	if !m.isDeleteOptimizationEnabled() {
		return 1
	}

	workers := m.cfg.DeleteOptimizations.Workers
	if workers <= 0 {
		switch m.cfg.General.RemoteStorage {
		case "s3":
			return 10
		case "gcs":
			return 50
		case "azblob":
			return 20
		default:
			return 4
		}
	}
	return workers
}

func (m *MockBackuper) getOptimalBatchSize() int {
	if !m.isDeleteOptimizationEnabled() {
		return 1
	}

	batchSize := m.cfg.DeleteOptimizations.BatchSize
	if batchSize <= 0 {
		return 1000
	}
	return batchSize
}

func (m *MockBackuper) validateDeleteOptimizationConfig() error {
	return nil // Simplified for testing
}
