package enhanced

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
)

// MockRemoteStorage implements storage.RemoteStorage for testing
type MockRemoteStorage struct {
	kind        string
	deleteFiles map[string]error // file -> error (nil for success)
	walkFiles   []string
}

func NewMockRemoteStorage(kind string) *MockRemoteStorage {
	return &MockRemoteStorage{
		kind:        kind,
		deleteFiles: make(map[string]error),
		walkFiles:   []string{},
	}
}

func (m *MockRemoteStorage) Kind() string { return m.kind }

func (m *MockRemoteStorage) DeleteFile(ctx context.Context, key string) error {
	if err, exists := m.deleteFiles[key]; exists {
		return err
	}
	// Simulate successful deletion if not explicitly set to fail
	return nil
}

func (m *MockRemoteStorage) Walk(ctx context.Context, path string, recursive bool, walkFn func(context.Context, storage.RemoteFile) error) error {
	for _, file := range m.walkFiles {
		if err := walkFn(ctx, &mockRemoteFile{name: file}); err != nil {
			return err
		}
	}
	return nil
}

// Implement other required methods with minimal functionality for testing
func (m *MockRemoteStorage) Connect(ctx context.Context) error { return nil }
func (m *MockRemoteStorage) Close(ctx context.Context) error   { return nil }
func (m *MockRemoteStorage) StatFile(ctx context.Context, key string) (storage.RemoteFile, error) {
	return &mockRemoteFile{name: key}, nil
}
func (m *MockRemoteStorage) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	return m.DeleteFile(ctx, key)
}
func (m *MockRemoteStorage) WalkAbsolute(ctx context.Context, path string, recursive bool, walkFn func(context.Context, storage.RemoteFile) error) error {
	return m.Walk(ctx, path, recursive, walkFn)
}
func (m *MockRemoteStorage) GetFileReader(ctx context.Context, key string) (storage.ReadSeekCloser, error) {
	return nil, nil
}
func (m *MockRemoteStorage) PutFile(ctx context.Context, key string, r storage.ReadSeekCloser) error {
	return nil
}
func (m *MockRemoteStorage) CopyObject(ctx context.Context, srcKey, dstKey string) error { return nil }

type mockRemoteFile struct {
	name         string
	size         int64
	lastModified time.Time
}

func (m *mockRemoteFile) Name() string            { return m.name }
func (m *mockRemoteFile) Size() int64             { return m.size }
func (m *mockRemoteFile) LastModified() time.Time { return m.lastModified }

// Test configuration setup
func getTestConfig() *config.Config {
	return &config.Config{
		DeleteOptimizations: config.DeleteOptimizations{
			Enabled:          true,
			BatchSize:        100,
			Workers:          4,
			RetryAttempts:    3,
			FailureThreshold: 0.5,
			ErrorStrategy:    "continue",
			CacheEnabled:     true,
			CacheTTL:         5 * time.Minute,
			S3Optimizations: config.S3Optimizations{
				UseBatchAPI:        true,
				VersionConcurrency: 10,
				PreloadVersions:    true,
			},
			GCSOptimizations: config.GCSOptimizations{
				MaxWorkers:    50,
				UseClientPool: true,
			},
			AzureOptimizations: config.AzureOptimizations{
				UseBatchAPI: false,
				MaxWorkers:  20,
			},
		},
		S3: config.S3Config{
			Bucket: "test-bucket",
		},
		GCS: config.GCSConfig{
			Bucket: "test-bucket",
		},
		AzureBlob: config.AzureBlobConfig{
			Container: "test-container",
		},
	}
}

func TestEnhancedStorageFactory(t *testing.T) {
	cfg := getTestConfig()
	factory := NewEnhancedStorageFactory(cfg)

	testCases := []struct {
		name        string
		storageType string
		shouldWork  bool
	}{
		{"S3 Storage", "s3", true},
		{"GCS Storage", "gcs", true},
		{"Azure Blob Storage", "azblob", true},
		{"FTP Storage", "ftp", true}, // Should still work with adapters
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			baseStorage := NewMockRemoteStorage(tc.storageType)
			ctx := context.Background()

			wrapper, err := factory.CreateEnhancedWrapper(ctx, baseStorage, nil)
			if tc.shouldWork {
				if err != nil {
					t.Fatalf("Expected success but got error: %v", err)
				}
				if wrapper == nil {
					t.Fatal("Expected wrapper but got nil")
				}

				// Test that wrapper supports batch delete
				supportsBatch := wrapper.SupportsBatchDelete()
				t.Logf("Storage %s supports batch delete: %v", tc.storageType, supportsBatch)

				// Test batch size
				batchSize := wrapper.GetOptimalBatchSize()
				if batchSize <= 0 {
					t.Errorf("Expected positive batch size, got %d", batchSize)
				}

				// Test worker count
				workerCount := wrapper.GetWorkerCount()
				if workerCount <= 0 {
					t.Errorf("Expected positive worker count, got %d", workerCount)
				}

				// Clean up
				if err := wrapper.Close(ctx); err != nil {
					t.Errorf("Error closing wrapper: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("Expected error but got success")
				}
			}
		})
	}
}

func TestEnhancedDeleteBackup(t *testing.T) {
	cfg := getTestConfig()
	factory := NewEnhancedStorageFactory(cfg)

	testCases := []struct {
		name        string
		storageType string
		files       []string
		expectError bool
	}{
		{
			name:        "S3 with small file set",
			storageType: "s3",
			files:       []string{"backup1/file1.txt", "backup1/file2.txt", "backup1/metadata.json"},
			expectError: false,
		},
		{
			name:        "GCS with medium file set",
			storageType: "gcs",
			files:       generateFileList("backup2", 50),
			expectError: false,
		},
		{
			name:        "Azure with large file set",
			storageType: "azblob",
			files:       generateFileList("backup3", 200),
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			baseStorage := NewMockRemoteStorage(tc.storageType)
			baseStorage.walkFiles = tc.files

			ctx := context.Background()
			wrapper, err := factory.CreateForBackupDeletion(ctx, baseStorage)
			if err != nil {
				t.Fatalf("Failed to create enhanced wrapper: %v", err)
			}
			defer wrapper.Close(ctx)

			// Test enhanced delete backup
			backupName := "test-backup"
			err = wrapper.EnhancedDeleteBackup(ctx, backupName)

			if tc.expectError && err == nil {
				t.Error("Expected error but got success")
			} else if !tc.expectError && err != nil {
				t.Errorf("Expected success but got error: %v", err)
			}

			// Check metrics
			metrics := wrapper.GetDeleteMetrics()
			if metrics == nil {
				t.Error("Expected metrics but got nil")
			} else {
				t.Logf("Delete metrics: Files processed: %d, Files deleted: %d, Files failed: %d",
					metrics.FilesProcessed, metrics.FilesDeleted, metrics.FilesFailed)
			}
		})
	}
}

func TestBatchDelete(t *testing.T) {
	cfg := getTestConfig()
	factory := NewEnhancedStorageFactory(cfg)

	testCases := []struct {
		name        string
		storageType string
		keys        []string
		expectFail  map[string]bool
	}{
		{
			name:        "S3 successful batch",
			storageType: "s3",
			keys:        []string{"file1.txt", "file2.txt", "file3.txt"},
			expectFail:  map[string]bool{},
		},
		{
			name:        "GCS with some failures",
			storageType: "gcs",
			keys:        []string{"file1.txt", "file2.txt", "file3.txt", "file4.txt"},
			expectFail:  map[string]bool{"file2.txt": true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			baseStorage := NewMockRemoteStorage(tc.storageType)

			// Set up failures for specific files
			for key, shouldFail := range tc.expectFail {
				if shouldFail {
					baseStorage.deleteFiles[key] = storage.ErrNotFound
				}
			}

			ctx := context.Background()
			wrapper, err := factory.CreateEnhancedWrapper(ctx, baseStorage, nil)
			if err != nil {
				t.Fatalf("Failed to create enhanced wrapper: %v", err)
			}
			defer wrapper.Close(ctx)

			// Test batch delete
			result, err := wrapper.DeleteBatch(ctx, tc.keys)
			if err != nil {
				t.Errorf("Batch delete failed: %v", err)
			}

			if result == nil {
				t.Fatal("Expected result but got nil")
			}

			expectedSuccess := len(tc.keys) - len(tc.expectFail)
			if result.SuccessCount != expectedSuccess {
				t.Errorf("Expected %d successful deletes, got %d", expectedSuccess, result.SuccessCount)
			}

			if len(result.FailedKeys) != len(tc.expectFail) {
				t.Errorf("Expected %d failed deletes, got %d", len(tc.expectFail), len(result.FailedKeys))
			}

			t.Logf("Batch delete result: %d success, %d failed", result.SuccessCount, len(result.FailedKeys))
		})
	}
}

func TestConfigurationValidation(t *testing.T) {
	testCases := []struct {
		name        string
		configFunc  func() *config.Config
		expectError bool
	}{
		{
			name:        "Valid configuration",
			configFunc:  getTestConfig,
			expectError: false,
		},
		{
			name: "Invalid batch size",
			configFunc: func() *config.Config {
				cfg := getTestConfig()
				cfg.DeleteOptimizations.BatchSize = -1
				return cfg
			},
			expectError: true,
		},
		{
			name: "Invalid worker count",
			configFunc: func() *config.Config {
				cfg := getTestConfig()
				cfg.DeleteOptimizations.Workers = -1
				return cfg
			},
			expectError: true,
		},
		{
			name: "Invalid failure threshold",
			configFunc: func() *config.Config {
				cfg := getTestConfig()
				cfg.DeleteOptimizations.FailureThreshold = 1.5
				return cfg
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.configFunc()
			factory := NewEnhancedStorageFactory(cfg)
			baseStorage := NewMockRemoteStorage("s3")

			ctx := context.Background()
			wrapper, err := factory.CreateEnhancedWrapper(ctx, baseStorage, nil)

			if tc.expectError {
				if err == nil {
					t.Error("Expected validation error but got success")
				}
				if wrapper != nil {
					wrapper.Close(ctx)
				}
			} else {
				if err != nil {
					t.Errorf("Expected success but got error: %v", err)
				}
				if wrapper == nil {
					t.Error("Expected wrapper but got nil")
				} else {
					wrapper.Close(ctx)
				}
			}
		})
	}
}

// Helper function to generate file lists for testing
func generateFileList(backupName string, count int) []string {
	files := make([]string, count)
	for i := 0; i < count; i++ {
		files[i] = fmt.Sprintf("%s/data_%03d.dat", backupName, i)
	}
	return files
}

func TestPerformanceMetrics(t *testing.T) {
	cfg := getTestConfig()
	factory := NewEnhancedStorageFactory(cfg)
	baseStorage := NewMockRemoteStorage("s3")

	ctx := context.Background()
	wrapper, err := factory.CreateEnhancedWrapper(ctx, baseStorage, nil)
	if err != nil {
		t.Fatalf("Failed to create enhanced wrapper: %v", err)
	}
	defer wrapper.Close(ctx)

	// Create a large set of files to test performance metrics
	largeFileSet := generateFileList("performance-test", 1000)

	startTime := time.Now()
	result, err := wrapper.DeleteBatch(ctx, largeFileSet)
	duration := time.Since(startTime)

	if err != nil {
		t.Errorf("Batch delete failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result but got nil")
	}

	// Get metrics
	metrics := wrapper.GetDeleteMetrics()
	if metrics == nil {
		t.Fatal("Expected metrics but got nil")
	}

	t.Logf("Performance test results:")
	t.Logf("  Files processed: %d", metrics.FilesProcessed)
	t.Logf("  Files deleted: %d", metrics.FilesDeleted)
	t.Logf("  Duration: %v", duration)
	t.Logf("  Throughput: %.2f files/second", float64(metrics.FilesDeleted)/duration.Seconds())

	// Basic performance assertions
	if metrics.FilesProcessed == 0 {
		t.Error("Expected files to be processed")
	}

	if metrics.FilesDeleted == 0 {
		t.Error("Expected files to be deleted")
	}
}
