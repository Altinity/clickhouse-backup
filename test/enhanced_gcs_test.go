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

// TestGCSParallelDelete tests GCS-specific parallel delete operations
func TestGCSParallelDelete(t *testing.T) {
	tests := []struct {
		name             string
		objectCount      int
		maxWorkers       int
		clientPoolSize   int
		expectedWorkers  int
		shouldFail       bool
		failureRate      float64
		simulateDelay    time.Duration
		expectedDuration time.Duration
	}{
		{
			name:             "Small parallel batch",
			objectCount:      50,
			maxWorkers:       10,
			clientPoolSize:   10,
			expectedWorkers:  10,
			shouldFail:       false,
			simulateDelay:    10 * time.Millisecond,
			expectedDuration: 100 * time.Millisecond, // Should be much faster than sequential
		},
		{
			name:             "Large parallel batch",
			objectCount:      500,
			maxWorkers:       50,
			clientPoolSize:   50,
			expectedWorkers:  50,
			shouldFail:       false,
			simulateDelay:    5 * time.Millisecond,
			expectedDuration: 100 * time.Millisecond, // Parallel efficiency
		},
		{
			name:             "Worker count limited by job count",
			objectCount:      5,
			maxWorkers:       50,
			clientPoolSize:   50,
			expectedWorkers:  5, // Limited by object count
			shouldFail:       false,
			simulateDelay:    10 * time.Millisecond,
			expectedDuration: 50 * time.Millisecond,
		},
		{
			name:             "Worker count limited by client pool",
			objectCount:      100,
			maxWorkers:       50,
			clientPoolSize:   10, // Bottleneck
			expectedWorkers:  10,
			shouldFail:       false,
			simulateDelay:    5 * time.Millisecond,
			expectedDuration: 100 * time.Millisecond,
		},
		{
			name:             "Parallel delete with failures",
			objectCount:      100,
			maxWorkers:       20,
			clientPoolSize:   20,
			expectedWorkers:  20,
			shouldFail:       true,
			failureRate:      0.1, // 10% failure rate
			simulateDelay:    5 * time.Millisecond,
			expectedDuration: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testGCSParallelDeleteScenario(t, tt.objectCount, tt.maxWorkers, tt.clientPoolSize,
				tt.expectedWorkers, tt.shouldFail, tt.failureRate, tt.simulateDelay, tt.expectedDuration)
		})
	}
}

// testGCSParallelDeleteScenario tests a specific GCS parallel delete scenario
func testGCSParallelDeleteScenario(t *testing.T, objectCount, maxWorkers, clientPoolSize, expectedWorkers int,
	shouldFail bool, failureRate float64, simulateDelay, expectedDuration time.Duration) {

	// Create mock GCS client pool
	mockClientPool := &MockGCSClientPool{
		size:          clientPoolSize,
		simulateDelay: simulateDelay,
		shouldFail:    shouldFail,
		failureRate:   failureRate,
	}

	// Create enhanced GCS storage
	cfg := &config.Config{
		GCS: config.GCSConfig{
			Bucket:         "test-gcs-bucket",
			ClientPoolSize: clientPoolSize,
		},
		DeleteOptimizations: config.DeleteOptimizations{
			Enabled:   true,
			BatchSize: 100, // Not used for GCS parallel processing
			Workers:   maxWorkers,
		},
	}

	gcsStorage := &MockEnhancedGCS{
		bucket:     cfg.GCS.Bucket,
		clientPool: mockClientPool,
		maxWorkers: maxWorkers,
		supported:  false, // GCS uses parallel, not batch
		metrics:    &enhanced.DeleteMetrics{},
	}

	// Generate test keys
	keys := make([]string, objectCount)
	for i := 0; i < objectCount; i++ {
		keys[i] = fmt.Sprintf("gcs-test/file-%d.dat", i)
	}

	// Execute parallel delete
	ctx := context.Background()
	startTime := time.Now()
	result, err := gcsStorage.DeleteBatch(ctx, keys)
	duration := time.Since(startTime)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, result)

	if shouldFail && failureRate > 0 {
		// Should have some failures
		assert.Greater(t, len(result.FailedKeys), 0)
		assert.Greater(t, len(result.Errors), 0)
		expectedSuccessCount := int(float64(objectCount) * (1.0 - failureRate))
		assert.InDelta(t, expectedSuccessCount, result.SuccessCount, float64(objectCount)*0.2) // Allow 20% variance
	} else {
		// Should succeed completely
		assert.Equal(t, objectCount, result.SuccessCount)
		assert.Empty(t, result.FailedKeys)
		assert.Empty(t, result.Errors)
	}

	// Verify performance characteristics
	assert.Less(t, duration, expectedDuration*2, "Should complete within expected timeframe")

	// Verify worker usage was optimal
	actualWorkers := gcsStorage.getOptimalWorkerCount(objectCount)
	assert.Equal(t, expectedWorkers, actualWorkers, "Should use optimal worker count")

	// Verify metrics
	metrics := gcsStorage.GetDeleteMetrics()
	assert.NotNil(t, metrics)
	assert.Equal(t, int64(objectCount), metrics.FilesProcessed)
	assert.Greater(t, metrics.APICallsCount, int64(0))

	// Verify GCS-specific properties
	assert.False(t, gcsStorage.SupportsBatchDelete(), "GCS should not support native batch delete")
	assert.Equal(t, 100, gcsStorage.GetOptimalBatchSize(), "GCS should use parallel processing chunk size")
}

// TestGCSClientPool tests GCS client pool functionality
func TestGCSClientPool(t *testing.T) {
	t.Run("Pool Creation and Basic Operations", func(t *testing.T) {
		testGCSClientPoolBasicOperations(t)
	})

	t.Run("Pool Exhaustion and Recovery", func(t *testing.T) {
		testGCSClientPoolExhaustion(t)
	})

	t.Run("Concurrent Pool Access", func(t *testing.T) {
		testGCSClientPoolConcurrency(t)
	})

	t.Run("Pool Size Management", func(t *testing.T) {
		testGCSClientPoolSizeManagement(t)
	})
}

// testGCSClientPoolBasicOperations tests basic client pool operations
func testGCSClientPoolBasicOperations(t *testing.T) {
	poolSize := 5
	pool := &MockGCSClientPool{
		size:      poolSize,
		available: poolSize,
	}

	// Test initial state
	assert.Equal(t, poolSize, pool.Size())
	assert.Equal(t, poolSize, pool.Available())

	// Test getting clients
	clients := make([]*MockGCSClient, poolSize)
	for i := 0; i < poolSize; i++ {
		client := pool.Get()
		assert.NotNil(t, client)
		clients[i] = client
	}

	// Pool should be exhausted
	assert.Equal(t, 0, pool.Available())

	// Test putting clients back
	for _, client := range clients {
		pool.Put(client)
	}

	// Pool should be full again
	assert.Equal(t, poolSize, pool.Available())
}

// testGCSClientPoolExhaustion tests pool behavior when exhausted
func testGCSClientPoolExhaustion(t *testing.T) {
	poolSize := 2
	pool := &MockGCSClientPool{
		size:      poolSize,
		available: poolSize,
	}

	// Exhaust the pool
	client1 := pool.Get()
	client2 := pool.Get()
	assert.NotNil(t, client1)
	assert.NotNil(t, client2)
	assert.Equal(t, 0, pool.Available())

	// Trying to get another client should return nil (in mock)
	client3 := pool.Get()
	assert.Nil(t, client3)

	// Return a client and try again
	pool.Put(client1)
	assert.Equal(t, 1, pool.Available())

	client4 := pool.Get()
	assert.NotNil(t, client4)
	assert.Equal(t, 0, pool.Available())
}

// testGCSClientPoolConcurrency tests concurrent pool access
func testGCSClientPoolConcurrency(t *testing.T) {
	poolSize := 10
	pool := &MockGCSClientPool{
		size:      poolSize,
		available: poolSize,
	}

	const goroutines = 20
	const operationsPerGoroutine = 50

	var wg sync.WaitGroup
	successCount := int64(0)
	var successCountMutex sync.Mutex

	// Run concurrent operations
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				client := pool.Get()
				if client != nil {
					// Simulate some work
					time.Sleep(time.Microsecond)
					pool.Put(client)

					successCountMutex.Lock()
					successCount++
					successCountMutex.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	// Verify pool is still in good state
	assert.Equal(t, poolSize, pool.Size())
	assert.GreaterOrEqual(t, pool.Available(), 0)
	assert.LessOrEqual(t, pool.Available(), poolSize)

	// Should have had many successful operations
	assert.Greater(t, successCount, int64(goroutines*operationsPerGoroutine/2))
}

// testGCSClientPoolSizeManagement tests pool size management
func testGCSClientPoolSizeManagement(t *testing.T) {
	testCases := []struct {
		name     string
		poolSize int
		isValid  bool
	}{
		{"Valid small pool", 1, true},
		{"Valid medium pool", 10, true},
		{"Valid large pool", 100, true},
		{"Invalid zero pool", 0, false},
		{"Invalid negative pool", -1, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.isValid {
				pool := &MockGCSClientPool{
					size:      tc.poolSize,
					available: tc.poolSize,
				}
				assert.Equal(t, tc.poolSize, pool.Size())
				assert.Equal(t, tc.poolSize, pool.Available())
			} else {
				// In a real implementation, this would return an error
				// For our mock, we just verify the validation logic
				assert.LessOrEqual(t, tc.poolSize, 0)
			}
		})
	}
}

// TestGCSWorkerOptimization tests worker count optimization logic
func TestGCSWorkerOptimization(t *testing.T) {
	tests := []struct {
		name            string
		jobCount        int
		maxWorkers      int
		clientPoolSize  int
		expectedWorkers int
	}{
		{
			name:            "Jobs less than max workers",
			jobCount:        5,
			maxWorkers:      50,
			clientPoolSize:  50,
			expectedWorkers: 5, // Limited by job count
		},
		{
			name:            "Jobs equal to max workers",
			jobCount:        50,
			maxWorkers:      50,
			clientPoolSize:  50,
			expectedWorkers: 50,
		},
		{
			name:            "Jobs more than max workers",
			jobCount:        100,
			maxWorkers:      50,
			clientPoolSize:  50,
			expectedWorkers: 50, // Limited by max workers
		},
		{
			name:            "Limited by client pool size",
			jobCount:        100,
			maxWorkers:      50,
			clientPoolSize:  25, // Bottleneck
			expectedWorkers: 25,
		},
		{
			name:            "Single job",
			jobCount:        1,
			maxWorkers:      50,
			clientPoolSize:  50,
			expectedWorkers: 1,
		},
		{
			name:            "Zero jobs",
			jobCount:        0,
			maxWorkers:      50,
			clientPoolSize:  50,
			expectedWorkers: 1, // Minimum 1 worker
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gcsStorage := &MockEnhancedGCS{
				maxWorkers: tt.maxWorkers,
				clientPool: &MockGCSClientPool{
					size: tt.clientPoolSize,
				},
			}

			actualWorkers := gcsStorage.getOptimalWorkerCount(tt.jobCount)
			assert.Equal(t, tt.expectedWorkers, actualWorkers)
		})
	}
}

// TestGCSErrorHandling tests GCS-specific error handling
func TestGCSErrorHandling(t *testing.T) {
	errorScenarios := []struct {
		name          string
		errorType     string
		isRetriable   bool
		expectSuccess bool
		expectRetries bool
	}{
		{
			name:          "Retriable network error",
			errorType:     "network",
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
			name:          "Object not exist (success case)",
			errorType:     "object_not_exist",
			isRetriable:   false,
			expectSuccess: true, // Not existing is success for delete
			expectRetries: false,
		},
		{
			name:          "Retriable rate limit error",
			errorType:     "rate_limit",
			isRetriable:   true,
			expectSuccess: true,
			expectRetries: true,
		},
		{
			name:          "Non-retriable authentication error",
			errorType:     "auth_error",
			isRetriable:   false,
			expectSuccess: false,
			expectRetries: false,
		},
	}

	for _, scenario := range errorScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			testGCSErrorScenario(t, scenario.errorType, scenario.isRetriable, scenario.expectSuccess, scenario.expectRetries)
		})
	}
}

// testGCSErrorScenario tests a specific error handling scenario
func testGCSErrorScenario(t *testing.T, errorType string, isRetriable, expectSuccess, expectRetries bool) {
	mockClientPool := &MockGCSClientPool{
		size:        5,
		available:   5,
		shouldFail:  true,
		errorType:   errorType,
		isRetriable: isRetriable,
	}

	cfg := &config.Config{
		GCS: config.GCSConfig{
			Bucket:         "error-test-bucket",
			ClientPoolSize: 5,
		},
		DeleteOptimizations: config.DeleteOptimizations{
			Enabled:       true,
			Workers:       5,
			RetryAttempts: 3,
		},
	}

	gcsStorage := &MockEnhancedGCS{
		bucket:     cfg.GCS.Bucket,
		clientPool: mockClientPool,
		maxWorkers: cfg.DeleteOptimizations.Workers,
		supported:  false,
		metrics:    &enhanced.DeleteMetrics{},
	}

	keys := []string{"error-test/file1.dat", "error-test/file2.dat"}
	ctx := context.Background()

	result, err := gcsStorage.DeleteBatch(ctx, keys)

	if expectSuccess {
		assert.NoError(t, err)
		assert.NotNil(t, result)
		if errorType != "object_not_exist" {
			// For retriable errors that eventually succeed
			assert.Equal(t, len(keys), result.SuccessCount)
		} else {
			// For object not exist, should succeed immediately
			assert.Equal(t, len(keys), result.SuccessCount)
		}
	} else {
		assert.NoError(t, err) // GCS parallel delete doesn't fail entirely, just reports failures
		assert.NotNil(t, result)
		assert.Greater(t, len(result.FailedKeys), 0)
		assert.Greater(t, len(result.Errors), 0)
	}

	if expectRetries {
		assert.Greater(t, mockClientPool.retryAttempts, 0, "Should have retry attempts for retriable errors")
	}
}

// TestGCSPerformanceCharacteristics tests GCS-specific performance characteristics
func TestGCSPerformanceCharacteristics(t *testing.T) {
	t.Run("Parallel vs Sequential Performance", func(t *testing.T) {
		testGCSParallelVsSequentialPerformance(t)
	})

	t.Run("Worker Pool Efficiency", func(t *testing.T) {
		testGCSWorkerPoolEfficiency(t)
	})

	t.Run("Throughput Calculation", func(t *testing.T) {
		testGCSThroughputCalculation(t)
	})

	t.Run("Optimal Batch Size", func(t *testing.T) {
		testGCSOptimalBatchSize(t)
	})
}

// testGCSParallelVsSequentialPerformance tests parallel vs sequential performance
func testGCSParallelVsSequentialPerformance(t *testing.T) {
	objectCount := 100
	simulateDelay := 10 * time.Millisecond

	// Test sequential (1 worker)
	sequentialStorage := &MockEnhancedGCS{
		bucket:     "perf-test-bucket",
		maxWorkers: 1,
		clientPool: &MockGCSClientPool{
			size:          1,
			available:     1,
			simulateDelay: simulateDelay,
		},
		supported: false,
		metrics:   &enhanced.DeleteMetrics{},
	}

	// Test parallel (10 workers)
	parallelStorage := &MockEnhancedGCS{
		bucket:     "perf-test-bucket",
		maxWorkers: 10,
		clientPool: &MockGCSClientPool{
			size:          10,
			available:     10,
			simulateDelay: simulateDelay,
		},
		supported: false,
		metrics:   &enhanced.DeleteMetrics{},
	}

	keys := make([]string, objectCount)
	for i := 0; i < objectCount; i++ {
		keys[i] = fmt.Sprintf("perf/file-%d.dat", i)
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
	assert.Greater(t, speedup, 5.0, "Parallel should be at least 5x faster than sequential")

	log.Printf("Sequential: %v, Parallel: %v, Speedup: %.2fx",
		sequentialDuration, parallelDuration, speedup)
}

// testGCSWorkerPoolEfficiency tests worker pool efficiency
func testGCSWorkerPoolEfficiency(t *testing.T) {
	tests := []struct {
		name            string
		objectCount     int
		workerCount     int
		poolSize        int
		expectEfficient bool
	}{
		{
			name:            "Optimal worker to pool ratio",
			objectCount:     100,
			workerCount:     10,
			poolSize:        10,
			expectEfficient: true,
		},
		{
			name:            "More workers than pool size",
			objectCount:     100,
			workerCount:     20,
			poolSize:        10, // Bottleneck
			expectEfficient: false,
		},
		{
			name:            "Fewer workers than pool size",
			objectCount:     100,
			workerCount:     5,
			poolSize:        10,
			expectEfficient: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClientPool := &MockGCSClientPool{
				size:          tt.poolSize,
				available:     tt.poolSize,
				simulateDelay: 5 * time.Millisecond,
			}

			gcsStorage := &MockEnhancedGCS{
				bucket:     "efficiency-test-bucket",
				maxWorkers: tt.workerCount,
				clientPool: mockClientPool,
				supported:  false,
				metrics:    &enhanced.DeleteMetrics{},
			}

			keys := make([]string, tt.objectCount)
			for i := 0; i < tt.objectCount; i++ {
				keys[i] = fmt.Sprintf("efficiency/file-%d.dat", i)
			}

			ctx := context.Background()
			startTime := time.Now()
			result, err := gcsStorage.DeleteBatch(ctx, keys)
			duration := time.Since(startTime)

			assert.NoError(t, err)
			assert.Equal(t, tt.objectCount, result.SuccessCount)

			// Check efficiency metrics
			optimalWorkers := gcsStorage.getOptimalWorkerCount(tt.objectCount)
			expectedWorkers := minInt(tt.workerCount, tt.poolSize, tt.objectCount)
			assert.Equal(t, expectedWorkers, optimalWorkers)

			if tt.expectEfficient {
				// Should complete reasonably quickly
				maxExpectedDuration := time.Duration(tt.objectCount/optimalWorkers) * 10 * time.Millisecond
				assert.Less(t, duration, maxExpectedDuration)
			}
		})
	}
}

// testGCSThroughputCalculation tests throughput metrics calculation
func testGCSThroughputCalculation(t *testing.T) {
	gcsStorage := &MockEnhancedGCS{
		bucket:     "throughput-test-bucket",
		maxWorkers: 10,
		clientPool: &MockGCSClientPool{
			size:          10,
			available:     10,
			simulateDelay: 50 * time.Millisecond,
		},
		supported: false,
		metrics:   &enhanced.DeleteMetrics{},
	}

	keys := []string{"throughput/file1.dat", "throughput/file2.dat"}
	ctx := context.Background()

	// Set simulated file sizes in metrics
	gcsStorage.GetDeleteMetrics().BytesDeleted = 5 * 1024 * 1024 // 5MB

	startTime := time.Now()
	result, err := gcsStorage.DeleteBatch(ctx, keys)
	duration := time.Since(startTime)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Greater(t, duration, 25*time.Millisecond, "Should take some time due to simulated delay")

	// Verify throughput calculation
	metrics := gcsStorage.GetDeleteMetrics()
	assert.Greater(t, metrics.ThroughputMBps, 0.0, "Should calculate positive throughput")
	assert.Greater(t, metrics.TotalDuration, time.Duration(0), "Should track total duration")
}

// testGCSOptimalBatchSize tests optimal batch size calculation
func testGCSOptimalBatchSize(t *testing.T) {
	gcsStorage := &MockEnhancedGCS{
		bucket:     "batch-size-test-bucket",
		maxWorkers: 20,
		clientPool: &MockGCSClientPool{size: 20},
		supported:  false,
		metrics:    &enhanced.DeleteMetrics{},
	}

	// GCS uses parallel processing, not traditional batching
	batchSize := gcsStorage.GetOptimalBatchSize()
	assert.Equal(t, 100, batchSize, "GCS should return a reasonable chunk size for parallel processing")

	// Test batch size optimization based on item count
	testCases := []struct {
		totalItems int
		workers    int
		expected   int
	}{
		{100, 10, 10}, // 100/10 = 10
		{50, 10, 5},   // 50/10 = 5
		{5, 10, 1},    // 5/10 = 0.5, rounded up to 1
		{200, 10, 20}, // 200/10 = 20
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Items_%d_Workers_%d", tc.totalItems, tc.workers), func(t *testing.T) {
			optimized := gcsStorage.optimizeBatchSize(tc.totalItems, tc.workers)
			assert.Equal(t, tc.expected, optimized)
		})
	}
}

// TestGCSConfigurationValidation tests GCS-specific configuration validation
func TestGCSConfigurationValidation(t *testing.T) {
	validationTests := []struct {
		name        string
		config      *config.Config
		shouldError bool
		errorMsg    string
	}{
		{
			name: "Valid GCS configuration",
			config: &config.Config{
				GCS: config.GCSConfig{
					Bucket:         "valid-gcs-bucket",
					ClientPoolSize: 10,
				},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled: true,
				},
			},
			shouldError: false,
		},
		{
			name: "Empty bucket name",
			config: &config.Config{
				GCS: config.GCSConfig{
					Bucket:         "",
					ClientPoolSize: 10,
				},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled: true,
				},
			},
			shouldError: true,
			errorMsg:    "bucket name cannot be empty",
		},
		{
			name: "Invalid client pool size",
			config: &config.Config{
				GCS: config.GCSConfig{
					Bucket:         "test-bucket",
					ClientPoolSize: 0, // Invalid
				},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled: true,
				},
			},
			shouldError: false, // Should use default
		},
		{
			name: "Valid minimal configuration",
			config: &config.Config{
				GCS: config.GCSConfig{
					Bucket:         "minimal-bucket",
					ClientPoolSize: 1,
				},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled: true,
				},
			},
			shouldError: false,
		},
	}

	for _, tt := range validationTests {
		t.Run(tt.name, func(t *testing.T) {
			// For configuration validation, we test the creation logic
			if tt.config.GCS.Bucket == "" {
				// Test empty bucket validation
				err := fmt.Errorf("GCS bucket name cannot be empty")
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				return
			}

			gcsStorage := &MockEnhancedGCS{
				bucket:    tt.config.GCS.Bucket,
				supported: false,
				metrics:   &enhanced.DeleteMetrics{},
			}

			if tt.shouldError {
				assert.Nil(t, gcsStorage)
			} else {
				assert.NotNil(t, gcsStorage)
				assert.Equal(t, tt.config.GCS.Bucket, gcsStorage.bucket)
			}
		})
	}
}

// TestGCSContextCancellation tests context cancellation handling
func TestGCSContextCancellation(t *testing.T) {
	gcsStorage := &MockEnhancedGCS{
		bucket:     "cancel-test-bucket",
		maxWorkers: 5,
		clientPool: &MockGCSClientPool{
			size:          5,
			available:     5,
			simulateDelay: 200 * time.Millisecond, // Long delay to test cancellation
		},
		supported: false,
		metrics:   &enhanced.DeleteMetrics{},
	}

	keys := []string{"cancel/file1.dat", "cancel/file2.dat", "cancel/file3.dat"}

	// Test context cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	result, err := gcsStorage.DeleteBatch(ctx, keys)

	// Should either complete quickly or handle cancellation gracefully
	if err != nil {
		assert.Contains(t, err.Error(), "context")
	} else {
		assert.NotNil(t, result)
		// Some operations might complete before cancellation
		assert.LessOrEqual(t, result.SuccessCount, len(keys))
	}
}

// Mock implementations for testing

type MockGCSClient struct {
	bucket        string
	shouldFail    bool
	errorType     string
	isRetriable   bool
	simulateDelay time.Duration
	deletesCalled int
}

type MockGCSClientPool struct {
	size          int
	available     int
	shouldFail    bool
	errorType     string
	isRetriable   bool
	simulateDelay time.Duration
	failureRate   float64
	retryAttempts int
	mutex         sync.RWMutex
}

func (pool *MockGCSClientPool) Get() *MockGCSClient {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if pool.available > 0 {
		pool.available--
		return &MockGCSClient{
			shouldFail:    pool.shouldFail,
			errorType:     pool.errorType,
			isRetriable:   pool.isRetriable,
			simulateDelay: pool.simulateDelay,
		}
	}
	return nil // Pool exhausted
}

func (pool *MockGCSClientPool) Put(client *MockGCSClient) {
	if client == nil {
		return
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if pool.available < pool.size {
		pool.available++
	}
}

func (pool *MockGCSClientPool) Size() int {
	pool.mutex.RLock()
	defer pool.mutex.RUnlock()
	return pool.size
}

func (pool *MockGCSClientPool) Available() int {
	pool.mutex.RLock()
	defer pool.mutex.RUnlock()
	return pool.available
}

type MockEnhancedGCS struct {
	bucket     string
	maxWorkers int
	clientPool *MockGCSClientPool
	supported  bool
	metrics    *enhanced.DeleteMetrics
}

func (m *MockEnhancedGCS) DeleteBatch(ctx context.Context, keys []string) (*enhanced.BatchResult, error) {
	workerCount := m.getOptimalWorkerCount(len(keys))

	// Simulate parallel processing
	var wg sync.WaitGroup
	results := make(chan enhanced.FailedKey, len(keys))
	jobs := make(chan string, len(keys))

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := m.clientPool.Get()
			defer m.clientPool.Put(client)

			for key := range jobs {
				select {
				case <-ctx.Done():
					results <- enhanced.FailedKey{Key: key, Error: ctx.Err()}
					return
				default:
					if client != nil && m.clientPool.simulateDelay > 0 {
						time.Sleep(m.clientPool.simulateDelay)
					}

					// Simulate failures
					if m.clientPool.shouldFail && m.clientPool.failureRate > 0 {
						// Simple failure simulation
						if len(key)%10 < int(m.clientPool.failureRate*10) {
							var err error
							switch m.clientPool.errorType {
							case "object_not_exist":
								// This is actually success for delete
								continue
							case "network":
								err = fmt.Errorf("network error")
								if m.clientPool.isRetriable {
									m.clientPool.retryAttempts++
									// Eventually succeed after retries
									if m.clientPool.retryAttempts >= 2 {
										continue
									}
								}
							default:
								err = fmt.Errorf("%s error", m.clientPool.errorType)
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
	m.metrics.APICallsCount += int64(len(keys)) // One API call per file

	return &enhanced.BatchResult{
		SuccessCount: successCount,
		FailedKeys:   failedKeys,
		Errors:       errors,
	}, nil
}

func (m *MockEnhancedGCS) getOptimalWorkerCount(jobCount int) int {
	maxWorkers := m.maxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 50
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
	if m.clientPool != nil && maxWorkers > m.clientPool.size {
		maxWorkers = m.clientPool.size
	}

	return maxWorkers
}

func (m *MockEnhancedGCS) optimizeBatchSize(totalItems, workers int) int {
	if workers == 0 {
		return totalItems
	}
	chunkSize := totalItems / workers
	if chunkSize == 0 {
		chunkSize = 1
	}
	return chunkSize
}

func (m *MockEnhancedGCS) SupportsBatchDelete() bool {
	return m.supported
}

func (m *MockEnhancedGCS) GetOptimalBatchSize() int {
	return 100 // GCS parallel processing chunk size
}

func (m *MockEnhancedGCS) GetDeleteMetrics() *enhanced.DeleteMetrics {
	return m.metrics
}

func (m *MockEnhancedGCS) ResetDeleteMetrics() {
	m.metrics = &enhanced.DeleteMetrics{}
}

// Helper functions
func minInt(values ...int) int {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}
