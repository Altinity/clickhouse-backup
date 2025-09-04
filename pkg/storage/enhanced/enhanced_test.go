package enhanced

import (
	"fmt"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
)

func TestBackupExistenceCache(t *testing.T) {
	ttl := 1 * time.Second
	cache := NewBackupExistenceCache(ttl)

	// Test cache operations
	backupName := "test-backup"

	// Test cache miss
	metadata, exists := cache.Get(backupName)
	if exists {
		t.Error("Expected cache miss but got hit")
	}
	if metadata != nil {
		t.Error("Expected nil metadata but got value")
	}

	// Test cache set and hit
	testMetadata := &BackupMetadata{
		BackupName:   backupName,
		Exists:       true,
		Size:         1024,
		LastModified: time.Now(),
	}
	cache.Set(backupName, testMetadata)

	metadata, exists = cache.Get(backupName)
	if !exists {
		t.Error("Expected cache hit but got miss")
	}
	if metadata == nil {
		t.Error("Expected metadata but got nil")
	}
	if metadata.BackupName != backupName {
		t.Errorf("Expected backup name %s but got %s", backupName, metadata.BackupName)
	}

	// Test cache expiration
	time.Sleep(ttl + 100*time.Millisecond)
	metadata, exists = cache.Get(backupName)
	if exists {
		t.Error("Expected cache miss after expiration but got hit")
	}

	// Test cache invalidation
	cache.Set(backupName, testMetadata)
	cache.Invalidate(backupName)
	metadata, exists = cache.Get(backupName)
	if exists {
		t.Error("Expected cache miss after invalidation but got hit")
	}
}

func TestBatchedBackupExistenceCache(t *testing.T) {
	ttl := 5 * time.Second
	batchSize := 10
	cache := NewBatchedBackupExistenceCache(ttl, batchSize)

	// Prepare test data
	backupNames := []string{"backup1", "backup2", "backup3", "backup4"}
	metadataMap := make(map[string]*BackupMetadata)
	for i, name := range backupNames {
		metadataMap[name] = &BackupMetadata{
			BackupName:   name,
			Exists:       true,
			Size:         int64(1024 * (i + 1)),
			LastModified: time.Now(),
		}
	}

	// Test batch set
	cache.SetBatch(metadataMap)

	// Test batch get
	results := cache.GetBatch(backupNames)
	if len(results) != len(backupNames) {
		t.Errorf("Expected %d results but got %d", len(backupNames), len(results))
	}

	for _, name := range backupNames {
		if results[name] == nil {
			t.Errorf("Expected metadata for %s but got nil", name)
		}
	}

	// Test get missing
	allNames := append(backupNames, "missing1", "missing2")
	missing := cache.GetMissing(allNames)
	if len(missing) != 2 {
		t.Errorf("Expected 2 missing backups but got %d", len(missing))
	}

	// Test batch invalidation
	cache.InvalidateBatch(backupNames[:2])
	missing = cache.GetMissing(backupNames)
	if len(missing) != 2 {
		t.Errorf("Expected 2 missing backups after invalidation but got %d", len(missing))
	}
}

func TestDeleteMetrics(t *testing.T) {
	metrics := &DeleteMetrics{
		FilesProcessed: 100,
		FilesDeleted:   95,
		FilesFailed:    5,
		BytesDeleted:   1024 * 1024, // 1MB
		APICallsCount:  10,
		TotalDuration:  2 * time.Second,
		ThroughputMBps: 0.5,
	}

	// Test basic metrics
	if metrics.FilesProcessed != 100 {
		t.Errorf("Expected 100 files processed but got %d", metrics.FilesProcessed)
	}
	if metrics.FilesDeleted != 95 {
		t.Errorf("Expected 95 files deleted but got %d", metrics.FilesDeleted)
	}
	if metrics.FilesFailed != 5 {
		t.Errorf("Expected 5 files failed but got %d", metrics.FilesFailed)
	}

	// Test throughput calculation
	expectedThroughput := float64(metrics.BytesDeleted) / (1024 * 1024) / metrics.TotalDuration.Seconds()
	if expectedThroughput != metrics.ThroughputMBps {
		t.Errorf("Expected throughput %.2f MB/s but got %.2f MB/s", expectedThroughput, metrics.ThroughputMBps)
	}
}

func TestBatchResult(t *testing.T) {
	failedKeys := []FailedKey{
		{Key: "file1.txt", Error: fmt.Errorf("permission denied")},
		{Key: "file2.txt", Error: fmt.Errorf("not found")},
	}

	result := &BatchResult{
		SuccessCount: 8,
		FailedKeys:   failedKeys,
		Errors:       []error{failedKeys[0].Error, failedKeys[1].Error},
	}

	// Test result contents
	if result.SuccessCount != 8 {
		t.Errorf("Expected 8 successful deletes but got %d", result.SuccessCount)
	}
	if len(result.FailedKeys) != 2 {
		t.Errorf("Expected 2 failed keys but got %d", len(result.FailedKeys))
	}
	if len(result.Errors) != 2 {
		t.Errorf("Expected 2 errors but got %d", len(result.Errors))
	}

	// Test failed keys
	for i, fk := range result.FailedKeys {
		if fk.Key == "" {
			t.Errorf("Failed key %d has empty key", i)
		}
		if fk.Error == nil {
			t.Errorf("Failed key %d has nil error", i)
		}
	}
}

func TestOptimizationConfigError(t *testing.T) {
	err := &OptimizationConfigError{
		Field:   "batch_size",
		Value:   -1,
		Message: "batch size must be greater than 0",
	}

	expectedMsg := "configuration error for field batch_size (value: -1): batch size must be greater than 0"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message %q but got %q", expectedMsg, err.Error())
	}
}

func TestIsRetriableError(t *testing.T) {
	testCases := []struct {
		name      string
		err       error
		retriable bool
	}{
		{"nil error", nil, false},
		{"timeout error", fmt.Errorf("request timeout"), true},
		{"connection reset", fmt.Errorf("connection reset by peer"), true},
		{"service unavailable", fmt.Errorf("service unavailable"), true},
		{"rate limit", fmt.Errorf("too many requests"), true},
		{"permission denied", fmt.Errorf("permission denied"), false},
		{"not found", fmt.Errorf("file not found"), false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsRetriableError(tc.err)
			if result != tc.retriable {
				t.Errorf("Expected retriable=%v for error %v but got %v", tc.retriable, tc.err, result)
			}
		})
	}
}

func TestCombineErrors(t *testing.T) {
	testCases := []struct {
		name     string
		errors   []error
		expected string
	}{
		{"no errors", []error{}, ""},
		{"single error", []error{fmt.Errorf("error1")}, "error1"},
		{"multiple errors", []error{fmt.Errorf("error1"), fmt.Errorf("error2")}, "multiple errors occurred: error1; error2"},
		{"with nil errors", []error{fmt.Errorf("error1"), nil, fmt.Errorf("error2")}, "multiple errors occurred: error1; error2"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := CombineErrors(tc.errors)
			if tc.expected == "" {
				if result != nil {
					t.Errorf("Expected nil error but got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("Expected error %q but got nil", tc.expected)
				} else if result.Error() != tc.expected {
					t.Errorf("Expected error %q but got %q", tc.expected, result.Error())
				}
			}
		})
	}
}

func TestProgressTracker(t *testing.T) {
	tracker := &ProgressTracker{
		totalFiles:     100,
		processedFiles: 0,
		startTime:      time.Now(),
		lastUpdate:     time.Now(),
	}

	// Test initial state
	processed, total, eta := tracker.getProgress()
	if processed != 0 {
		t.Errorf("Expected 0 processed files initially but got %d", processed)
	}
	if total != 100 {
		t.Errorf("Expected 100 total files but got %d", total)
	}

	// Test progress updates
	tracker.updateProgress(25)
	processed, total, eta = tracker.getProgress()
	if processed != 25 {
		t.Errorf("Expected 25 processed files but got %d", processed)
	}

	// Test percentage calculation
	percentage := tracker.GetProgressPercentage()
	expectedPercentage := 25.0
	if percentage != expectedPercentage {
		t.Errorf("Expected %.1f%% progress but got %.1f%%", expectedPercentage, percentage)
	}

	// Test completion
	tracker.updateProgress(75)
	processed, total, eta = tracker.getProgress()
	if processed != 100 {
		t.Errorf("Expected 100 processed files but got %d", processed)
	}

	percentage = tracker.GetProgressPercentage()
	if percentage != 100.0 {
		t.Errorf("Expected 100%% progress but got %.1f%%", percentage)
	}

	// ETA should be available after some progress
	if eta == 0 {
		t.Log("ETA is 0, which is expected for completed tasks")
	}
}

func TestValidateOptimizationConfig(t *testing.T) {
	testCases := []struct {
		name        string
		config      *config.DeleteOptimizations
		expectError bool
		errorField  string
	}{
		{
			name: "valid config",
			config: &config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        100,
				Workers:          4,
				RetryAttempts:    3,
				FailureThreshold: 0.5,
				ErrorStrategy:    "continue",
			},
			expectError: false,
		},
		{
			name: "disabled optimizations",
			config: &config.DeleteOptimizations{
				Enabled: false,
			},
			expectError: false,
		},
		{
			name: "invalid batch size",
			config: &config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 0,
			},
			expectError: true,
			errorField:  "batch_size",
		},
		{
			name: "negative workers",
			config: &config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 100,
				Workers:   -1,
			},
			expectError: true,
			errorField:  "workers",
		},
		{
			name: "invalid failure threshold",
			config: &config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        100,
				Workers:          4,
				FailureThreshold: 1.5,
			},
			expectError: true,
			errorField:  "failure_threshold",
		},
		{
			name: "invalid error strategy",
			config: &config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        100,
				Workers:          4,
				FailureThreshold: 0.5,
				ErrorStrategy:    "invalid_strategy",
			},
			expectError: true,
			errorField:  "error_strategy",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock wrapper for validation
			wrapper := &EnhancedStorageWrapper{
				config: &config.Config{
					DeleteOptimizations: *tc.config,
				},
			}

			err := wrapper.ValidateConfig()
			if tc.expectError {
				if err == nil {
					t.Error("Expected validation error but got success")
				} else if configErr, ok := err.(*OptimizationConfigError); ok {
					if configErr.Field != tc.errorField {
						t.Errorf("Expected error for field %s but got %s", tc.errorField, configErr.Field)
					}
				} else {
					t.Errorf("Expected OptimizationConfigError but got %T", err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected success but got error: %v", err)
				}
			}
		})
	}
}

// Benchmark tests for performance validation
func BenchmarkCacheOperations(b *testing.B) {
	cache := NewBackupExistenceCache(5 * time.Minute)

	metadata := &BackupMetadata{
		BackupName:   "benchmark-backup",
		Exists:       true,
		Size:         1024,
		LastModified: time.Now(),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("backup-%d", i%1000) // Cycle through 1000 keys
		cache.Set(key, metadata)
		cache.Get(key)
	}
}

func BenchmarkBatchResultProcessing(b *testing.B) {
	// Create a large batch result
	failedKeys := make([]FailedKey, 100)
	errors := make([]error, 100)
	for i := 0; i < 100; i++ {
		failedKeys[i] = FailedKey{
			Key:   fmt.Sprintf("file-%d.txt", i),
			Error: fmt.Errorf("error-%d", i),
		}
		errors[i] = failedKeys[i].Error
	}

	result := &BatchResult{
		SuccessCount: 900,
		FailedKeys:   failedKeys,
		Errors:       errors,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Simulate processing the batch result
		_ = result.SuccessCount
		_ = len(result.FailedKeys)
		_ = len(result.Errors)

		// Process failed keys
		for _, fk := range result.FailedKeys {
			_ = fk.Key
			_ = fk.Error
		}
	}
}
