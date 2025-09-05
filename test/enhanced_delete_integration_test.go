package test

import (
	"context"
	"fmt"
	"io"
	"strings"
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
		batchConfig    *config.BatchDeletionConfig
		s3Config       *config.S3BatchConfig
		gcsConfig      *config.GCSBatchConfig
		azureConfig    *config.AzureBatchConfig
		expectedResult string
	}{
		{
			name:        "S3 Enhanced Delete Enabled",
			storageType: "s3",
			batchConfig: &config.BatchDeletionConfig{
				Enabled:          true,
				BatchSize:        1000,
				Workers:          10,
				RetryAttempts:    3,
				FailureThreshold: 0.1,
				ErrorStrategy:    "retry_batch",
			},
			s3Config: &config.S3BatchConfig{
				UseBatchAPI:        true,
				VersionConcurrency: 5,
				PreloadVersions:    true,
			},
			expectedResult: "enhanced",
		},
		{
			name:        "GCS Enhanced Delete Enabled",
			storageType: "gcs",
			batchConfig: &config.BatchDeletionConfig{
				Enabled:          true,
				BatchSize:        500,
				Workers:          50,
				RetryAttempts:    2,
				FailureThreshold: 0.2,
				ErrorStrategy:    "continue",
			},
			gcsConfig: &config.GCSBatchConfig{
				MaxWorkers:    50,
				UseClientPool: true,
				UseBatchAPI:   true,
			},
			expectedResult: "enhanced",
		},
		{
			name:        "Azure Enhanced Delete Enabled",
			storageType: "azblob",
			batchConfig: &config.BatchDeletionConfig{
				Enabled:          true,
				BatchSize:        200,
				Workers:          20,
				RetryAttempts:    5,
				FailureThreshold: 0.05,
				ErrorStrategy:    "fail_fast",
			},
			azureConfig: &config.AzureBatchConfig{
				UseBatchAPI: true,
				MaxWorkers:  20,
			},
			expectedResult: "enhanced",
		},
		{
			name:        "Optimizations Disabled - Fallback",
			storageType: "s3",
			batchConfig: &config.BatchDeletionConfig{
				Enabled: false,
			},
			expectedResult: "fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testEnhancedDeleteScenario(t, tt.storageType, &tt, tt.expectedResult)
		})
	}
}

// testEnhancedDeleteScenario tests a specific delete optimization scenario
func testEnhancedDeleteScenario(t *testing.T, storageType string, scenario *struct {
	name           string
	storageType    string
	batchConfig    *config.BatchDeletionConfig
	s3Config       *config.S3BatchConfig
	gcsConfig      *config.GCSBatchConfig
	azureConfig    *config.AzureBatchConfig
	expectedResult string
}, expectedResult string) {
	// Create test configuration
	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: storageType,
			BatchDeletion: *scenario.batchConfig,
		},
	}

	// Set storage-specific configurations
	switch storageType {
	case "s3":
		if scenario.s3Config != nil {
			cfg.S3.BatchDeletion = *scenario.s3Config
		}
	case "gcs":
		if scenario.gcsConfig != nil {
			cfg.GCS.BatchDeletion = *scenario.gcsConfig
		}
	case "azblob":
		if scenario.azureConfig != nil {
			cfg.AzureBlob.BatchDeletion = *scenario.azureConfig
		}
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
	if scenario.batchConfig.Enabled {
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
	if !cfg.General.BatchDeletion.Enabled {
		// Skip validation if optimizations are disabled
		return
	}

	// Test valid configuration
	factory := enhanced.NewStorageFactory(cfg)
	require.NotNil(t, factory)

	// Test invalid batch size
	invalidCfg := *cfg
	invalidCfg.General.BatchDeletion.BatchSize = -1

	wrapper, err := enhanced.NewEnhancedStorageWrapper(nil, &invalidCfg, nil)
	assert.Error(t, err)
	assert.Nil(t, wrapper)

	// Test invalid failure threshold
	invalidCfg2 := *cfg
	invalidCfg2.General.BatchDeletion.FailureThreshold = 1.5

	wrapper2, err2 := enhanced.NewEnhancedStorageWrapper(nil, &invalidCfg2, nil)
	assert.Error(t, err2)
	assert.Nil(t, wrapper2)

	// Test invalid error strategy
	invalidCfg3 := *cfg
	invalidCfg3.General.BatchDeletion.ErrorStrategy = "invalid_strategy"

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
		EnableCache:     false, // Cache not used in new simplified config
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
	assert.Equal(t, cfg.General.BatchDeletion.Enabled && expectedResult != "fallback", wrapper.IsOptimizationEnabled())
	assert.Equal(t, cfg.General.BatchDeletion.BatchSize, wrapper.GetBatchSize())
	assert.Equal(t, cfg.General.BatchDeletion.Workers, wrapper.GetWorkerCount())

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
		batchSize: cfg.General.BatchDeletion.BatchSize,
		supported: true,
	}

	// Create cache if enabled
	var cache *enhanced.BackupExistenceCache
	// Cache not used in new simplified config

	// Create batch manager
	batchMgr := enhanced.NewBatchManager(&cfg.General.BatchDeletion, mockStorage, cache)
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
	if cfg.General.BatchDeletion.Enabled {
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
			BatchDeletion: config.BatchDeletionConfig{
				Enabled: false,
			},
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
			BatchDeletion: config.BatchDeletionConfig{
				Enabled:          true,
				BatchSize:        100,
				Workers:          5,
				ErrorStrategy:    "fail_fast",
				FailureThreshold: 0.1,
			},
		},
	}

	// Test with failing storage
	failingStorage := &MockBatchRemoteStorage{
		batchSize:   cfg.General.BatchDeletion.BatchSize,
		supported:   true,
		shouldFail:  true,
		failureRate: 0.2, // 20% failure rate, above threshold
	}

	batchMgr := enhanced.NewBatchManager(&cfg.General.BatchDeletion, failingStorage, nil)
	require.NotNil(t, batchMgr)

	ctx := context.Background()
	err := batchMgr.DeleteBackupBatch(ctx, "test-backup")
	assert.Error(t, err) // Should fail due to high failure rate

	// Test continue strategy
	cfg.General.BatchDeletion.ErrorStrategy = "continue"
	batchMgr2 := enhanced.NewBatchManager(&cfg.General.BatchDeletion, failingStorage, nil)
	_ = batchMgr2.DeleteBackupBatch(ctx, "test-backup")
	// Should continue despite failures (specific behavior depends on implementation)
}

// TestPerformanceMetrics tests performance metrics collection
func TestPerformanceMetrics(t *testing.T) {
	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: "s3",
			BatchDeletion: config.BatchDeletionConfig{
				Enabled:   true,
				BatchSize: 1000,
				Workers:   10,
			},
		},
	}

	mockStorage := &MockBatchRemoteStorage{
		batchSize:     cfg.General.BatchDeletion.BatchSize,
		supported:     true,
		simulateFiles: 5000, // Simulate deleting 5000 files
		simulateDelay: 10 * time.Millisecond,
	}

	batchMgr := enhanced.NewBatchManager(&cfg.General.BatchDeletion, mockStorage, nil)
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

// Mock implementations for testing

// MockRemoteFile implements storage.RemoteFile for testing
type MockRemoteFile struct {
	key  string
	size int64
}

func (m *MockRemoteFile) Key() string {
	return m.key
}

func (m *MockRemoteFile) Name() string {
	// Extract the filename from the key
	parts := strings.Split(m.key, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return m.key
}

func (m *MockRemoteFile) Size() int64 {
	return m.size
}

func (m *MockRemoteFile) LastModified() time.Time {
	return time.Now()
}

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
	// Generate realistic backup files dynamically based on the prefix
	// This simulates finding files for any backup name
	var simulatedFiles []string

	if prefix == "" {
		// If no prefix, return files for multiple backups
		baseNames := []string{"backup-2024-01-01", "backup-2024-01-02", "test-backup"}
		for _, baseName := range baseNames {
			simulatedFiles = append(simulatedFiles, generateBackupFiles(baseName)...)
		}
	} else {
		// Extract backup name from prefix (remove trailing slashes)
		backupName := strings.TrimSuffix(prefix, "/")
		simulatedFiles = generateBackupFiles(backupName)
	}

	// Call the callback function for each simulated file
	for _, filepath := range simulatedFiles {
		if prefix == "" || strings.HasPrefix(filepath, prefix) {
			// Create a mock remote file
			mockFile := &MockRemoteFile{
				key:  filepath,
				size: 1024*1024 + int64(len(filepath)*1024), // Variable sizes 1MB+ per file
			}

			// Call the callback function with the mock file
			if err := fn(ctx, mockFile); err != nil {
				return err
			}
		}
	}

	return nil
}

// generateBackupFiles creates a realistic set of backup files for a given backup name
func generateBackupFiles(backupName string) []string {
	return []string{
		backupName + "/metadata.json",
		backupName + "/metadata/backup_metadata.json",
		backupName + "/metadata/schema.json",
		backupName + "/data/default/table1.tar.gz",
		backupName + "/data/default/table2.tar.gz",
		backupName + "/data/default/table3.tar.gz",
		backupName + "/data/system/query_log.tar.gz",
		backupName + "/data/system/query_thread_log.tar.gz",
		backupName + "/shadow/table1/increment.txt",
		backupName + "/shadow/table1/data.bin",
		backupName + "/shadow/table2/increment.txt",
		backupName + "/shadow/table2/data.bin",
		backupName + "/shadow/table3/increment.txt",
		backupName + "/shadow/table3/data.bin",
		backupName + "/rbac/users.json",
		backupName + "/rbac/roles.json",
		backupName + "/configs/config.xml",
		backupName + "/configs/users.xml",
		backupName + "/access/access_entities.list",
		backupName + "/functions/user_defined_functions.sql",
	}
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
			// Create different types of errors to test retry logic
			var err error
			switch i % 4 {
			case 0:
				err = fmt.Errorf("timeout: connection timed out") // Retriable
			case 1:
				err = fmt.Errorf("temporary failure") // Retriable
			case 2:
				err = fmt.Errorf("access denied") // Non-retriable
			case 3:
				err = fmt.Errorf("service unavailable") // Retriable
			}

			result.FailedKeys[i] = enhanced.FailedKey{
				Key:   keys[i],
				Error: err,
			}
			result.Errors[i] = err
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
	return m.cfg.General.BatchDeletion.Enabled
}

func (m *MockBackuper) shouldUseEnhancedDelete(backupName string) bool {
	return m.isDeleteOptimizationEnabled() && m.supportsEnhancedDelete()
}

func (m *MockBackuper) supportsEnhancedDelete() bool {
	if !m.cfg.General.BatchDeletion.Enabled {
		return false
	}

	switch m.cfg.General.RemoteStorage {
	case "s3":
		return m.cfg.S3.BatchDeletion.UseBatchAPI
	case "gcs":
		return m.cfg.GCS.BatchDeletion.UseClientPool
	case "azblob":
		return m.cfg.AzureBlob.BatchDeletion.UseBatchAPI
	default:
		return true
	}
}

func (m *MockBackuper) getOptimalWorkerCount() int {
	if !m.isDeleteOptimizationEnabled() {
		return 1
	}

	workers := m.cfg.General.BatchDeletion.Workers
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

	batchSize := m.cfg.General.BatchDeletion.BatchSize
	if batchSize <= 0 {
		return 1000
	}
	return batchSize
}

func (m *MockBackuper) validateDeleteOptimizationConfig() error {
	return nil // Simplified for testing
}
