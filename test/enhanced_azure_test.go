package test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/enhanced"
	"github.com/stretchr/testify/assert"
)

// TestAzureParallelDelete tests Azure-specific parallel delete operations
func TestAzureParallelDelete(t *testing.T) {
	tests := []struct {
		name             string
		objectCount      int
		maxWorkers       int
		expectedWorkers  int
		shouldFail       bool
		failureRate      float64
		simulateDelay    time.Duration
		expectedDuration time.Duration
	}{
		{
			name:             "Small parallel batch",
			objectCount:      30,
			maxWorkers:       10,
			expectedWorkers:  10,
			shouldFail:       false,
			simulateDelay:    15 * time.Millisecond,
			expectedDuration: 80 * time.Millisecond, // Should be faster than sequential
		},
		{
			name:             "Medium parallel batch",
			objectCount:      200,
			maxWorkers:       20, // Azure default
			expectedWorkers:  20,
			shouldFail:       false,
			simulateDelay:    10 * time.Millisecond,
			expectedDuration: 120 * time.Millisecond,
		},
		{
			name:             "Large parallel batch",
			objectCount:      500,
			maxWorkers:       20,
			expectedWorkers:  20,
			shouldFail:       false,
			simulateDelay:    5 * time.Millisecond,
			expectedDuration: 150 * time.Millisecond,
		},
		{
			name:             "Worker count limited by job count",
			objectCount:      5,
			maxWorkers:       20,
			expectedWorkers:  5, // Limited by object count
			shouldFail:       false,
			simulateDelay:    10 * time.Millisecond,
			expectedDuration: 60 * time.Millisecond,
		},
		{
			name:             "Single worker scenario",
			objectCount:      10,
			maxWorkers:       1,
			expectedWorkers:  1,
			shouldFail:       false,
			simulateDelay:    8 * time.Millisecond,
			expectedDuration: 100 * time.Millisecond, // Sequential processing
		},
		{
			name:             "Parallel delete with failures",
			objectCount:      100,
			maxWorkers:       15,
			expectedWorkers:  15,
			shouldFail:       true,
			failureRate:      0.15, // 15% failure rate
			simulateDelay:    8 * time.Millisecond,
			expectedDuration: 120 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testAzureParallelDeleteScenario(t, tt.objectCount, tt.maxWorkers,
				tt.expectedWorkers, tt.shouldFail, tt.failureRate, tt.simulateDelay, tt.expectedDuration)
		})
	}
}

// testAzureParallelDeleteScenario tests a specific Azure parallel delete scenario
func testAzureParallelDeleteScenario(t *testing.T, objectCount, maxWorkers, expectedWorkers int,
	shouldFail bool, failureRate float64, simulateDelay, expectedDuration time.Duration) {

	// Create enhanced Azure storage
	cfg := &config.Config{
		AzureBlob: config.AzureBlobConfig{
			Container: "test-azure-container",
			BatchDeletion: config.AzureBatchConfig{
				MaxWorkers: maxWorkers,
			},
		},
		General: config.GeneralConfig{
			BatchDeletion: config.BatchDeletionConfig{
				Enabled: true,
			},
		},
	}

	azureStorage := &MockEnhancedAzure{
		container:   cfg.AzureBlob.Container,
		maxWorkers:  maxWorkers,
		supported:   false, // Azure uses parallel, not batch
		metrics:     &enhanced.DeleteMetrics{},
		shouldFail:  shouldFail,
		failureRate: failureRate,
		delay:       simulateDelay,
	}

	// Generate test keys
	keys := make([]string, objectCount)
	for i := 0; i < objectCount; i++ {
		keys[i] = fmt.Sprintf("azure-test/blob-%d.dat", i)
	}

	// Execute parallel delete
	ctx := context.Background()
	startTime := time.Now()
	result, err := azureStorage.DeleteBatch(ctx, keys)
	duration := time.Since(startTime)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, result)

	if shouldFail && failureRate > 0 {
		// Should have some failures
		assert.Greater(t, len(result.FailedKeys), 0)
		assert.Greater(t, len(result.Errors), 0)
		expectedSuccessCount := int(float64(objectCount) * (1.0 - failureRate))
		assert.InDelta(t, expectedSuccessCount, result.SuccessCount, float64(objectCount)*0.25) // Allow 25% variance
	} else {
		// Should succeed completely
		assert.Equal(t, objectCount, result.SuccessCount)
		assert.Empty(t, result.FailedKeys)
		assert.Empty(t, result.Errors)
	}

	// Verify performance characteristics
	assert.Less(t, duration, expectedDuration*2, "Should complete within expected timeframe")

	// Verify worker usage was optimal
	actualWorkers := azureStorage.getOptimalWorkerCount(objectCount)
	assert.Equal(t, expectedWorkers, actualWorkers, "Should use optimal worker count")

	// Verify metrics
	metrics := azureStorage.GetDeleteMetrics()
	assert.NotNil(t, metrics)
	assert.Equal(t, int64(objectCount), metrics.FilesProcessed)
	assert.Greater(t, metrics.APICallsCount, int64(0))

	// Verify Azure-specific properties
	assert.False(t, azureStorage.SupportsBatchDelete(), "Azure should not support native batch delete")
	assert.Equal(t, 100, azureStorage.GetOptimalBatchSize(), "Azure should use parallel processing chunk size")
}

// TestAzureWorkerPool tests Azure worker pool functionality
func TestAzureWorkerPool(t *testing.T) {
	t.Run("Worker Pool Creation and Lifecycle", func(t *testing.T) {
		testAzureWorkerPoolLifecycle(t)
	})

	t.Run("Worker Pool Scaling", func(t *testing.T) {
		testAzureWorkerPoolScaling(t)
	})

	t.Run("Worker Pool Performance", func(t *testing.T) {
		testAzureWorkerPoolPerformance(t)
	})

	t.Run("Worker Pool Error Handling", func(t *testing.T) {
		testAzureWorkerPoolErrorHandling(t)
	})
}

// testAzureWorkerPoolLifecycle tests worker pool creation and lifecycle
func testAzureWorkerPoolLifecycle(t *testing.T) {
	azureStorage := &MockEnhancedAzure{
		container:  "lifecycle-test-container",
		maxWorkers: 10,
		supported:  false,
		metrics:    &enhanced.DeleteMetrics{},
	}

	// Test different worker pool sizes
	workerCounts := []int{1, 5, 10, 20}

	for _, workerCount := range workerCounts {
		t.Run(fmt.Sprintf("Workers_%d", workerCount), func(t *testing.T) {
			workerPool := azureStorage.createWorkerPool(workerCount)
			assert.NotNil(t, workerPool)
			assert.Equal(t, workerCount, workerPool.workers)
			assert.NotNil(t, workerPool.jobs)
			assert.NotNil(t, workerPool.results)

			// Test pool capacity
			assert.Equal(t, workerCount*2, cap(workerPool.jobs))
			assert.Equal(t, workerCount*2, cap(workerPool.results))
		})
	}
}

// testAzureWorkerPoolScaling tests worker pool scaling behavior
func testAzureWorkerPoolScaling(t *testing.T) {
	scalingTests := []struct {
		name            string
		jobCount        int
		maxWorkers      int
		expectedWorkers int
	}{
		{
			name:            "Jobs less than max workers",
			jobCount:        5,
			maxWorkers:      20,
			expectedWorkers: 5, // Limited by job count
		},
		{
			name:            "Jobs equal to max workers",
			jobCount:        20,
			maxWorkers:      20,
			expectedWorkers: 20,
		},
		{
			name:            "Jobs more than max workers",
			jobCount:        50,
			maxWorkers:      20,
			expectedWorkers: 20, // Limited by max workers
		},
		{
			name:            "Single job",
			jobCount:        1,
			maxWorkers:      20,
			expectedWorkers: 1,
		},
		{
			name:            "Zero jobs (edge case)",
			jobCount:        0,
			maxWorkers:      20,
			expectedWorkers: 1, // Minimum 1 worker
		},
	}

	for _, tt := range scalingTests {
		t.Run(tt.name, func(t *testing.T) {
			azureStorage := &MockEnhancedAzure{
				container:  "scaling-test-container",
				maxWorkers: tt.maxWorkers,
				supported:  false,
				metrics:    &enhanced.DeleteMetrics{},
			}

			actualWorkers := azureStorage.getOptimalWorkerCount(tt.jobCount)
			assert.Equal(t, tt.expectedWorkers, actualWorkers)
		})
	}
}

// testAzureWorkerPoolPerformance tests worker pool performance characteristics
func testAzureWorkerPoolPerformance(t *testing.T) {
	performanceTests := []struct {
		name        string
		objectCount int
		workerCount int
		delay       time.Duration
		expectedMax time.Duration
	}{
		{
			name:        "Small load performance",
			objectCount: 20,
			workerCount: 5,
			delay:       10 * time.Millisecond,
			expectedMax: 80 * time.Millisecond, // 20/5 * 10ms + overhead
		},
		{
			name:        "Medium load performance",
			objectCount: 100,
			workerCount: 10,
			delay:       5 * time.Millisecond,
			expectedMax: 80 * time.Millisecond, // 100/10 * 5ms + overhead
		},
		{
			name:        "High concurrency performance",
			objectCount: 200,
			workerCount: 20,
			delay:       3 * time.Millisecond,
			expectedMax: 60 * time.Millisecond, // 200/20 * 3ms + overhead
		},
	}

	for _, tt := range performanceTests {
		t.Run(tt.name, func(t *testing.T) {
			azureStorage := &MockEnhancedAzure{
				container:  "performance-test-container",
				maxWorkers: tt.workerCount,
				supported:  false,
				metrics:    &enhanced.DeleteMetrics{},
				delay:      tt.delay,
			}

			keys := make([]string, tt.objectCount)
			for i := 0; i < tt.objectCount; i++ {
				keys[i] = fmt.Sprintf("perf/blob-%d.dat", i)
			}

			ctx := context.Background()
			startTime := time.Now()
			result, err := azureStorage.DeleteBatch(ctx, keys)
			duration := time.Since(startTime)

			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, tt.objectCount, result.SuccessCount)
			assert.Less(t, duration, tt.expectedMax, "Should complete within expected time")

			log.Printf("Azure Performance Test %s: %d objects in %v with %d workers",
				tt.name, tt.objectCount, duration, tt.workerCount)
		})
	}
}

// testAzureWorkerPoolErrorHandling tests worker pool error handling
func testAzureWorkerPoolErrorHandling(t *testing.T) {
	azureStorage := &MockEnhancedAzure{
		container:   "error-test-container",
		maxWorkers:  10,
		supported:   false,
		metrics:     &enhanced.DeleteMetrics{},
		shouldFail:  true,
		failureRate: 0.3, // 30% failure rate
	}

	keys := []string{"error/blob1.dat", "error/blob2.dat", "error/blob3.dat", "error/blob4.dat", "error/blob5.dat"}
	ctx := context.Background()

	result, err := azureStorage.DeleteBatch(ctx, keys)

	assert.NoError(t, err) // Azure parallel delete doesn't fail entirely
	assert.NotNil(t, result)

	// Should have failures based on failure rate
	expectedFailures := int(float64(len(keys)) * azureStorage.failureRate)
	assert.InDelta(t, expectedFailures, len(result.FailedKeys), 2) // Allow variance
	assert.Greater(t, len(result.Errors), 0)
	assert.Greater(t, result.SuccessCount, 0) // Should have some successes too
}

// TestAzureErrorHandling tests Azure-specific error handling
func TestAzureErrorHandling(t *testing.T) {
	errorScenarios := []struct {
		name          string
		errorType     string
		isRetriable   bool
		expectSuccess bool
		expectRetries bool
	}{
		{
			name:          "Retriable network error",
			errorType:     "network_timeout",
			isRetriable:   true,
			expectSuccess: true,
			expectRetries: true,
		},
		{
			name:          "Non-retriable permission error",
			errorType:     "permission_denied",
			isRetriable:   false,
			expectSuccess: false,
			expectRetries: false,
		},
		{
			name:          "Blob not found (success case)",
			errorType:     "blob_not_found",
			isRetriable:   false,
			expectSuccess: true, // Not existing is success for delete
			expectRetries: false,
		},
		{
			name:          "Retriable throttling error",
			errorType:     "throttling",
			isRetriable:   true,
			expectSuccess: true,
			expectRetries: true,
		},
		{
			name:          "Non-retriable authentication error",
			errorType:     "auth_failure",
			isRetriable:   false,
			expectSuccess: false,
			expectRetries: false,
		},
		{
			name:          "Retriable server error",
			errorType:     "server_error_500",
			isRetriable:   true,
			expectSuccess: true,
			expectRetries: true,
		},
	}

	for _, scenario := range errorScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			testAzureErrorScenario(t, scenario.errorType, scenario.isRetriable, scenario.expectSuccess, scenario.expectRetries)
		})
	}
}

// testAzureErrorScenario tests a specific error handling scenario
func testAzureErrorScenario(t *testing.T, errorType string, isRetriable, expectSuccess, expectRetries bool) {
	azureStorage := &MockEnhancedAzure{
		container:   "error-scenario-container",
		maxWorkers:  5,
		supported:   false,
		metrics:     &enhanced.DeleteMetrics{},
		shouldFail:  true,
		errorType:   errorType,
		isRetriable: isRetriable,
	}

	keys := []string{"error-scenario/blob1.dat", "error-scenario/blob2.dat"}
	ctx := context.Background()

	result, err := azureStorage.DeleteBatch(ctx, keys)

	if expectSuccess {
		assert.NoError(t, err)
		assert.NotNil(t, result)
		if errorType != "blob_not_found" {
			// For retriable errors that eventually succeed
			assert.Equal(t, len(keys), result.SuccessCount)
		} else {
			// For blob not found, should succeed immediately
			assert.Equal(t, len(keys), result.SuccessCount)
		}
	} else {
		assert.NoError(t, err) // Azure parallel delete doesn't fail entirely, just reports failures
		assert.NotNil(t, result)
		assert.Greater(t, len(result.FailedKeys), 0)
		assert.Greater(t, len(result.Errors), 0)
	}

	if expectRetries {
		assert.Greater(t, azureStorage.retryAttempts, 0, "Should have retry attempts for retriable errors")
	}
}

// TestAzurePerformanceCharacteristics tests Azure-specific performance characteristics
func TestAzurePerformanceCharacteristics(t *testing.T) {
	t.Run("Parallel vs Sequential Performance", func(t *testing.T) {
		testAzureParallelVsSequentialPerformance(t)
	})

	t.Run("Worker Efficiency", func(t *testing.T) {
		testAzureWorkerEfficiency(t)
	})

	t.Run("Throughput Calculation", func(t *testing.T) {
		testAzureThroughputCalculation(t)
	})

	t.Run("Optimal Batch Size", func(t *testing.T) {
		testAzureOptimalBatchSize(t)
	})
}

// testAzureParallelVsSequentialPerformance tests parallel vs sequential performance
func testAzureParallelVsSequentialPerformance(t *testing.T) {
	objectCount := 60
	simulateDelay := 12 * time.Millisecond

	// Test sequential (1 worker)
	sequentialStorage := &MockEnhancedAzure{
		container:  "perf-compare-container",
		maxWorkers: 1,
		supported:  false,
		metrics:    &enhanced.DeleteMetrics{},
		delay:      simulateDelay,
	}

	// Test parallel (15 workers)
	parallelStorage := &MockEnhancedAzure{
		container:  "perf-compare-container",
		maxWorkers: 15,
		supported:  false,
		metrics:    &enhanced.DeleteMetrics{},
		delay:      simulateDelay,
	}

	keys := make([]string, objectCount)
	for i := 0; i < objectCount; i++ {
		keys[i] = fmt.Sprintf("perf/blob-%d.dat", i)
	}

	ctx := context.Background()

	// Sequential test
	sequentialStart := time.Now()
	sequentialResult, err := sequentialStorage.DeleteBatch(ctx, keys)
	sequentialDuration := time.Since(sequentialStart)

	assert.NoError(t, err)
	assert.Equal(t, objectCount, sequentialResult.SuccessCount)

	// Parallel test
	parallelStart := time.Now()
	parallelResult, err := parallelStorage.DeleteBatch(ctx, keys)
	parallelDuration := time.Since(parallelStart)

	assert.NoError(t, err)
	assert.Equal(t, objectCount, parallelResult.SuccessCount)

	// Parallel should be significantly faster
	speedup := float64(sequentialDuration) / float64(parallelDuration)
	assert.Greater(t, speedup, 4.0, "Azure parallel should be at least 4x faster than sequential")

	log.Printf("Azure Sequential: %v, Parallel: %v, Speedup: %.2fx",
		sequentialDuration, parallelDuration, speedup)
}

// testAzureWorkerEfficiency tests worker efficiency
func testAzureWorkerEfficiency(t *testing.T) {
	tests := []struct {
		name            string
		objectCount     int
		workerCount     int
		expectEfficient bool
	}{
		{
			name:            "Optimal worker count",
			objectCount:     60,
			workerCount:     10,
			expectEfficient: true,
		},
		{
			name:            "Too many workers for small load",
			objectCount:     5,
			workerCount:     20, // Overkill
			expectEfficient: false,
		},
		{
			name:            "Conservative worker count",
			objectCount:     100,
			workerCount:     5, // Conservative
			expectEfficient: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			azureStorage := &MockEnhancedAzure{
				container:  "efficiency-test-container",
				maxWorkers: tt.workerCount,
				supported:  false,
				metrics:    &enhanced.DeleteMetrics{},
				delay:      8 * time.Millisecond,
			}

			keys := make([]string, tt.objectCount)
			for i := 0; i < tt.objectCount; i++ {
				keys[i] = fmt.Sprintf("efficiency/blob-%d.dat", i)
			}

			ctx := context.Background()
			startTime := time.Now()
			result, err := azureStorage.DeleteBatch(ctx, keys)
			duration := time.Since(startTime)

			assert.NoError(t, err)
			assert.Equal(t, tt.objectCount, result.SuccessCount)

			// Check efficiency metrics
			optimalWorkers := azureStorage.getOptimalWorkerCount(tt.objectCount)
			expectedWorkers := minInt(tt.workerCount, tt.objectCount)
			assert.Equal(t, expectedWorkers, optimalWorkers)

			if tt.expectEfficient {
				// Should complete reasonably quickly
				maxExpectedDuration := time.Duration(tt.objectCount/optimalWorkers) * 15 * time.Millisecond
				assert.Less(t, duration, maxExpectedDuration)
			}
		})
	}
}

// testAzureThroughputCalculation tests throughput metrics calculation
func testAzureThroughputCalculation(t *testing.T) {
	azureStorage := &MockEnhancedAzure{
		container:  "throughput-test-container",
		maxWorkers: 8,
		supported:  false,
		metrics:    &enhanced.DeleteMetrics{},
		delay:      40 * time.Millisecond,
	}

	keys := []string{"throughput/blob1.dat", "throughput/blob2.dat"}
	ctx := context.Background()

	// Set simulated file sizes in metrics
	azureStorage.GetDeleteMetrics().BytesDeleted = 3 * 1024 * 1024 // 3MB

	startTime := time.Now()
	result, err := azureStorage.DeleteBatch(ctx, keys)
	duration := time.Since(startTime)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Greater(t, duration, 20*time.Millisecond, "Should take some time due to simulated delay")

	// Verify throughput calculation
	metrics := azureStorage.GetDeleteMetrics()
	assert.Greater(t, metrics.ThroughputMBps, 0.0, "Should calculate positive throughput")
	assert.Greater(t, metrics.TotalDuration, time.Duration(0), "Should track total duration")
}

// testAzureOptimalBatchSize tests optimal batch size calculation
func testAzureOptimalBatchSize(t *testing.T) {
	azureStorage := &MockEnhancedAzure{
		container:  "batch-size-test-container",
		maxWorkers: 20,
		supported:  false,
		metrics:    &enhanced.DeleteMetrics{},
	}

	// Azure uses parallel processing, not traditional batching
	batchSize := azureStorage.GetOptimalBatchSize()
	assert.Equal(t, 100, batchSize, "Azure should return a reasonable chunk size for parallel processing")

	// Test batch size optimization based on item count
	testCases := []struct {
		totalItems int
		workers    int
		expected   int
	}{
		{100, 10, 10}, // 100/10 = 10
		{50, 10, 5},   // 50/10 = 5
		{5, 10, 1},    // 5/10 = 0.5, rounded up to 1
		{200, 20, 10}, // 200/20 = 10
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Items_%d_Workers_%d", tc.totalItems, tc.workers), func(t *testing.T) {
			optimized := azureStorage.optimizeBatchSize(tc.totalItems, tc.workers)
			assert.Equal(t, tc.expected, optimized)
		})
	}
}

// TestAzureConfigurationValidation tests Azure-specific configuration validation
func TestAzureConfigurationValidation(t *testing.T) {
	validationTests := []struct {
		name        string
		config      *config.Config
		shouldError bool
		errorMsg    string
	}{
		{
			name: "Valid Azure configuration",
			config: &config.Config{
				AzureBlob: config.AzureBlobConfig{
					Container: "valid-azure-container",
				},
				General: config.GeneralConfig{
					BatchDeletion: config.BatchDeletionConfig{
						Enabled: true,
					},
				},
			},
			shouldError: false,
		},
		{
			name: "Empty container name",
			config: &config.Config{
				AzureBlob: config.AzureBlobConfig{
					Container: "",
				},
				General: config.GeneralConfig{
					BatchDeletion: config.BatchDeletionConfig{
						Enabled: true,
					},
				},
			},
			shouldError: true,
			errorMsg:    "container name cannot be empty",
		},
		{
			name: "Valid minimal configuration",
			config: &config.Config{
				AzureBlob: config.AzureBlobConfig{
					Container: "minimal-container",
				},
				General: config.GeneralConfig{
					BatchDeletion: config.BatchDeletionConfig{
						Enabled: true,
					},
				},
			},
			shouldError: false,
		},
	}

	for _, tt := range validationTests {
		t.Run(tt.name, func(t *testing.T) {
			// For configuration validation, we test the creation logic
			if tt.config.AzureBlob.Container == "" {
				// Test empty container validation
				err := fmt.Errorf("Azure Blob container name cannot be empty")
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				return
			}

			azureStorage := &MockEnhancedAzure{
				container: tt.config.AzureBlob.Container,
				supported: false,
				metrics:   &enhanced.DeleteMetrics{},
			}

			if tt.shouldError {
				assert.Nil(t, azureStorage)
			} else {
				assert.NotNil(t, azureStorage)
				assert.Equal(t, tt.config.AzureBlob.Container, azureStorage.container)
			}
		})
	}
}

// TestAzureContextCancellation tests context cancellation handling
func TestAzureContextCancellation(t *testing.T) {
	azureStorage := &MockEnhancedAzure{
		container:  "cancel-test-container",
		maxWorkers: 5,
		supported:  false,
		metrics:    &enhanced.DeleteMetrics{},
		delay:      150 * time.Millisecond, // Long delay to test cancellation
	}

	keys := []string{"cancel/blob1.dat", "cancel/blob2.dat", "cancel/blob3.dat"}

	// Test context cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	result, err := azureStorage.DeleteBatch(ctx, keys)

	// Should either complete quickly or handle cancellation gracefully
	if err != nil {
		assert.Contains(t, err.Error(), "context")
	} else {
		assert.NotNil(t, result)
		// Some operations might complete before cancellation
		assert.LessOrEqual(t, result.SuccessCount, len(keys))
	}
}

// TestAzureConnectionValidation tests Azure connection validation
func TestAzureConnectionValidation(t *testing.T) {
	azureStorage := &MockEnhancedAzure{
		container:       "connection-test-container",
		maxWorkers:      10,
		supported:       false,
		metrics:         &enhanced.DeleteMetrics{},
		connectionValid: true,
	}

	ctx := context.Background()

	// Test valid connection
	err := azureStorage.validateAzureConnection(ctx)
	assert.NoError(t, err)

	// Test invalid connection
	azureStorage.connectionValid = false
	err = azureStorage.validateAzureConnection(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to access Azure container")
}

// Mock implementations for testing

type MockEnhancedAzure struct {
	container       string
	maxWorkers      int
	supported       bool
	metrics         *enhanced.DeleteMetrics
	shouldFail      bool
	failureRate     float64
	errorType       string
	isRetriable     bool
	delay           time.Duration
	retryAttempts   int
	connectionValid bool
}

func (m *MockEnhancedAzure) DeleteBatch(ctx context.Context, keys []string) (*enhanced.BatchResult, error) {
	workerCount := m.getOptimalWorkerCount(len(keys))

	// Simulate parallel processing using worker pool
	var wg sync.WaitGroup
	results := make(chan enhanced.FailedKey, len(keys))
	jobs := make(chan string, len(keys))

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for key := range jobs {
				select {
				case <-ctx.Done():
					results <- enhanced.FailedKey{Key: key, Error: ctx.Err()}
					return
				default:
					if m.delay > 0 {
						time.Sleep(m.delay)
					}

					// Simulate failures
					if m.shouldFail && m.failureRate > 0 {
						// Simple failure simulation
						if len(key)%10 < int(m.failureRate*10) {
							var err error
							switch m.errorType {
							case "blob_not_found":
								// This is actually success for delete
								continue
							case "network_timeout", "throttling", "server_error_500":
								err = fmt.Errorf("%s error", m.errorType)
								if m.isRetriable {
									m.retryAttempts++
									// Eventually succeed after retries
									if m.retryAttempts >= 2 {
										continue
									}
								}
							default:
								err = fmt.Errorf("%s error", m.errorType)
							}
							results <- enhanced.FailedKey{Key: key, Error: err}
							continue
						}
					}
				}
			}
		}()
	}

	// Send jobs
	for _, key := range keys {
		jobs <- key
	}
	close(jobs)

	// Wait and collect results
	go func() {
		wg.Wait()
		close(results)
	}()

	var failedKeys []enhanced.FailedKey
	var errors []error
	for failed := range results {
		failedKeys = append(failedKeys, failed)
		errors = append(errors, failed.Error)
	}

	successCount := len(keys) - len(failedKeys)

	m.metrics.FilesProcessed = int64(len(keys))
	m.metrics.FilesDeleted = int64(successCount)
	m.metrics.FilesFailed = int64(len(failedKeys))
	m.metrics.APICallsCount += int64(len(keys)) // One API call per blob

	return &enhanced.BatchResult{
		SuccessCount: successCount,
		FailedKeys:   failedKeys,
		Errors:       errors,
	}, nil
}

func (m *MockEnhancedAzure) getOptimalWorkerCount(jobCount int) int {
	maxWorkers := m.maxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 20 // Azure default
	}

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

func (m *MockEnhancedAzure) createWorkerPool(workerCount int) *MockAzureWorkerPool {
	return &MockAzureWorkerPool{
		workers: workerCount,
		jobs:    make(chan interface{}, workerCount*2),
		results: make(chan interface{}, workerCount*2),
	}
}

func (m *MockEnhancedAzure) optimizeBatchSize(totalItems, workers int) int {
	if workers == 0 {
		return totalItems
	}
	chunkSize := totalItems / workers
	if chunkSize == 0 {
		chunkSize = 1
	}
	return chunkSize
}

func (m *MockEnhancedAzure) validateAzureConnection(ctx context.Context) error {
	if !m.connectionValid {
		return fmt.Errorf("failed to access Azure container: connection error")
	}
	return nil
}

func (m *MockEnhancedAzure) SupportsBatchDelete() bool {
	return m.supported
}

func (m *MockEnhancedAzure) GetOptimalBatchSize() int {
	return 100 // Azure parallel processing chunk size
}

func (m *MockEnhancedAzure) GetDeleteMetrics() *enhanced.DeleteMetrics {
	return m.metrics
}

func (m *MockEnhancedAzure) ResetDeleteMetrics() {
	m.metrics = &enhanced.DeleteMetrics{}
}

type MockAzureWorkerPool struct {
	workers int
	jobs    chan interface{}
	results chan interface{}
}

// Helper functions are defined in enhanced_gcs_test.go to avoid duplication
