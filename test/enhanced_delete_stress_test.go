package test

import (
	"context"
	"fmt"
	"io"
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/enhanced"
	"github.com/stretchr/testify/assert"
)

// TestLargeScaleDelete tests delete operations with large numbers of files
func TestLargeScaleDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	largeScaleTests := []struct {
		name        string
		objectCount int
		storageType string
		workers     int
		maxDuration time.Duration
		memoryLimit int64 // MB
	}{
		{
			name:        "S3 - 10K objects",
			objectCount: 10000,
			storageType: "s3",
			workers:     50,
			maxDuration: 30 * time.Second,
			memoryLimit: 100, // 100MB memory limit
		},
		{
			name:        "S3 - 25K objects",
			objectCount: 25000,
			storageType: "s3",
			workers:     100,
			maxDuration: 60 * time.Second,
			memoryLimit: 150,
		},
		{
			name:        "GCS - 10K objects",
			objectCount: 10000,
			storageType: "gcs",
			workers:     30,
			maxDuration: 45 * time.Second,
			memoryLimit: 100,
		},
		{
			name:        "GCS - 20K objects",
			objectCount: 20000,
			storageType: "gcs",
			workers:     50,
			maxDuration: 75 * time.Second,
			memoryLimit: 120,
		},
		{
			name:        "Azure - 8K objects",
			objectCount: 8000,
			storageType: "azure",
			workers:     20,
			maxDuration: 60 * time.Second,
			memoryLimit: 80,
		},
		{
			name:        "Azure - 15K objects",
			objectCount: 15000,
			storageType: "azure",
			workers:     30,
			maxDuration: 90 * time.Second,
			memoryLimit: 100,
		},
	}

	for _, tt := range largeScaleTests {
		t.Run(tt.name, func(t *testing.T) {
			testLargeScaleDeleteScenario(t, tt.objectCount, tt.storageType, tt.workers, tt.maxDuration, tt.memoryLimit)
		})
	}
}

// testLargeScaleDeleteScenario tests a specific large-scale delete scenario
func testLargeScaleDeleteScenario(t *testing.T, objectCount int, storageType string, workers int, maxDuration time.Duration, memoryLimitMB int64) {
	// Create mock storage based on type
	var storage enhanced.BatchRemoteStorage
	var err error

	switch storageType {
	case "s3":
		storage = createMockS3Storage(workers)
	case "gcs":
		storage = createMockGCSStorage(workers)
	case "azure":
		storage = createMockAzureStorage(workers)
	default:
		t.Fatalf("Unknown storage type: %s", storageType)
	}

	// Generate large number of test keys
	keys := make([]string, objectCount)
	for i := 0; i < objectCount; i++ {
		keys[i] = fmt.Sprintf("stress-test/%s/large-batch/file-%06d.dat", storageType, i)
	}

	// Measure initial memory
	runtime.GC()
	var initialMemStats runtime.MemStats
	runtime.ReadMemStats(&initialMemStats)
	initialMemoryMB := initialMemStats.Alloc / 1024 / 1024

	// Execute large-scale delete
	ctx := context.Background()
	startTime := time.Now()

	result, err := storage.DeleteBatch(ctx, keys)

	duration := time.Since(startTime)

	// Measure final memory
	runtime.GC()
	var finalMemStats runtime.MemStats
	runtime.ReadMemStats(&finalMemStats)
	finalMemoryMB := finalMemStats.Alloc / 1024 / 1024

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, objectCount, result.SuccessCount, "Should delete all objects")
	assert.Empty(t, result.FailedKeys, "Should have no failures in stress test")

	// Verify performance
	assert.Less(t, duration, maxDuration, "Should complete within time limit")

	// Verify memory usage remains reasonable
	memoryIncrease := int64(finalMemoryMB) - int64(initialMemoryMB)
	assert.Less(t, memoryIncrease, memoryLimitMB,
		"Memory usage should not increase significantly (increased by %dMB, limit %dMB)",
		memoryIncrease, memoryLimitMB)

	// Verify metrics (using type assertion since interface doesn't include GetDeleteMetrics)
	if metricsProvider, ok := storage.(interface {
		GetDeleteMetrics() *enhanced.DeleteMetrics
	}); ok {
		metrics := metricsProvider.GetDeleteMetrics()
		assert.NotNil(t, metrics)
		assert.Equal(t, int64(objectCount), metrics.FilesProcessed)
		assert.Equal(t, int64(objectCount), metrics.FilesDeleted)
		assert.Equal(t, int64(0), metrics.FilesFailed)
		assert.Greater(t, metrics.APICallsCount, int64(0))
	}

	// Calculate and verify throughput
	throughput := float64(objectCount) / duration.Seconds()
	log.Printf("Stress Test %s: %d objects in %v (%.2f objects/sec, memory: %dMB -> %dMB)",
		storageType, objectCount, duration, throughput, initialMemoryMB, finalMemoryMB)

	// Expect reasonable throughput (at least 100 objects/second)
	assert.Greater(t, throughput, 100.0, "Should achieve reasonable throughput")
}

// TestStressConcurrentDeleteOperations tests concurrent delete operations under stress
func TestStressConcurrentDeleteOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent stress test in short mode")
	}

	concurrentTests := []struct {
		name          string
		storageType   string
		concurrentOps int
		objectsPerOp  int
		workers       int
		maxDuration   time.Duration
	}{
		{
			name:          "S3 - 5 concurrent operations",
			storageType:   "s3",
			concurrentOps: 5,
			objectsPerOp:  1000,
			workers:       25,
			maxDuration:   45 * time.Second,
		},
		{
			name:          "S3 - 10 concurrent operations",
			storageType:   "s3",
			concurrentOps: 10,
			objectsPerOp:  500,
			workers:       50,
			maxDuration:   30 * time.Second,
		},
		{
			name:          "GCS - 3 concurrent operations",
			storageType:   "gcs",
			concurrentOps: 3,
			objectsPerOp:  1500,
			workers:       20,
			maxDuration:   60 * time.Second,
		},
		{
			name:          "GCS - 8 concurrent operations",
			storageType:   "gcs",
			concurrentOps: 8,
			objectsPerOp:  800,
			workers:       40,
			maxDuration:   45 * time.Second,
		},
		{
			name:          "Azure - 4 concurrent operations",
			storageType:   "azure",
			concurrentOps: 4,
			objectsPerOp:  1200,
			workers:       15,
			maxDuration:   75 * time.Second,
		},
		{
			name:          "Azure - 6 concurrent operations",
			storageType:   "azure",
			concurrentOps: 6,
			objectsPerOp:  800,
			workers:       20,
			maxDuration:   60 * time.Second,
		},
	}

	for _, tt := range concurrentTests {
		t.Run(tt.name, func(t *testing.T) {
			testConcurrentDeleteOperations(t, tt.storageType, tt.concurrentOps, tt.objectsPerOp, tt.workers, tt.maxDuration)
		})
	}
}

// testConcurrentDeleteOperations tests concurrent delete operations
func testConcurrentDeleteOperations(t *testing.T, storageType string, concurrentOps int, objectsPerOp int, workers int, maxDuration time.Duration) {
	// Create storage instances for each concurrent operation
	storages := make([]enhanced.BatchRemoteStorage, concurrentOps)
	for i := 0; i < concurrentOps; i++ {
		switch storageType {
		case "s3":
			storages[i] = createMockS3Storage(workers)
		case "gcs":
			storages[i] = createMockGCSStorage(workers)
		case "azure":
			storages[i] = createMockAzureStorage(workers)
		}
	}

	// Prepare keys for each operation
	allKeys := make([][]string, concurrentOps)
	for i := 0; i < concurrentOps; i++ {
		keys := make([]string, objectsPerOp)
		for j := 0; j < objectsPerOp; j++ {
			keys[j] = fmt.Sprintf("concurrent-test/%s/op-%d/file-%06d.dat", storageType, i, j)
		}
		allKeys[i] = keys
	}

	// Execute concurrent operations
	var wg sync.WaitGroup
	results := make([]*enhanced.BatchResult, concurrentOps)
	errors := make([]error, concurrentOps)
	durations := make([]time.Duration, concurrentOps)

	ctx := context.Background()
	startTime := time.Now()

	for i := 0; i < concurrentOps; i++ {
		wg.Add(1)
		go func(opIndex int) {
			defer wg.Done()

			opStartTime := time.Now()
			result, err := storages[opIndex].DeleteBatch(ctx, allKeys[opIndex])
			durations[opIndex] = time.Since(opStartTime)

			results[opIndex] = result
			errors[opIndex] = err
		}(i)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	// Verify results
	totalObjectsDeleted := 0
	totalAPICallsCount := int64(0)

	for i := 0; i < concurrentOps; i++ {
		assert.NoError(t, errors[i], "Operation %d should succeed", i)
		assert.NotNil(t, results[i], "Operation %d should return result", i)
		assert.Equal(t, objectsPerOp, results[i].SuccessCount, "Operation %d should delete all objects", i)
		assert.Empty(t, results[i].FailedKeys, "Operation %d should have no failures", i)

		totalObjectsDeleted += results[i].SuccessCount

		if metricsProvider, ok := storages[i].(interface {
			GetDeleteMetrics() *enhanced.DeleteMetrics
		}); ok {
			metrics := metricsProvider.GetDeleteMetrics()
			totalAPICallsCount += metrics.APICallsCount
		}
	}

	// Verify overall performance
	assert.Less(t, totalDuration, maxDuration, "Concurrent operations should complete within time limit")
	assert.Equal(t, concurrentOps*objectsPerOp, totalObjectsDeleted, "Should delete all objects across all operations")

	// Calculate throughput
	totalThroughput := float64(totalObjectsDeleted) / totalDuration.Seconds()
	log.Printf("Concurrent Test %s: %d ops × %d objects = %d total in %v (%.2f objects/sec)",
		storageType, concurrentOps, objectsPerOp, totalObjectsDeleted, totalDuration, totalThroughput)

	// Expect reasonable concurrent throughput
	assert.Greater(t, totalThroughput, 200.0, "Should achieve reasonable concurrent throughput")
}

// TestMemoryConstancy tests that memory usage remains constant during large operations
func TestMemoryConstancy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory constancy test in short mode")
	}

	memoryTests := []struct {
		name          string
		storageType   string
		objectCount   int
		batchSize     int
		workers       int
		memoryLimitMB int64
	}{
		{
			name:          "S3 - Memory constancy 50K objects",
			storageType:   "s3",
			objectCount:   50000,
			batchSize:     1000,
			workers:       100,
			memoryLimitMB: 200,
		},
		{
			name:          "GCS - Memory constancy 30K objects",
			storageType:   "gcs",
			objectCount:   30000,
			batchSize:     500,
			workers:       60,
			memoryLimitMB: 150,
		},
		{
			name:          "Azure - Memory constancy 20K objects",
			storageType:   "azure",
			objectCount:   20000,
			batchSize:     200,
			workers:       40,
			memoryLimitMB: 120,
		},
	}

	for _, tt := range memoryTests {
		t.Run(tt.name, func(t *testing.T) {
			testMemoryConstancy(t, tt.storageType, tt.objectCount, tt.batchSize, tt.workers, tt.memoryLimitMB)
		})
	}
}

// testMemoryConstancy tests memory usage constancy during large delete operations
func testMemoryConstancy(t *testing.T, storageType string, objectCount int, batchSize int, workers int, memoryLimitMB int64) {
	// Create storage
	var storage enhanced.BatchRemoteStorage
	switch storageType {
	case "s3":
		storage = createMockS3Storage(workers)
	case "gcs":
		storage = createMockGCSStorage(workers)
	case "azure":
		storage = createMockAzureStorage(workers)
	}

	// Measure baseline memory
	runtime.GC()
	var baselineMemStats runtime.MemStats
	runtime.ReadMemStats(&baselineMemStats)
	baselineMemoryMB := baselineMemStats.Alloc / 1024 / 1024

	memoryMeasurements := []int64{int64(baselineMemoryMB)}

	ctx := context.Background()
	totalProcessed := 0

	// Process in batches to measure memory usage over time
	for i := 0; i < objectCount; i += batchSize {
		end := i + batchSize
		if end > objectCount {
			end = objectCount
		}

		batchKeys := make([]string, end-i)
		for j := i; j < end; j++ {
			batchKeys[j-i] = fmt.Sprintf("memory-test/%s/batch-%d/file-%06d.dat", storageType, i/batchSize, j)
		}

		// Execute batch delete
		result, err := storage.DeleteBatch(ctx, batchKeys)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		totalProcessed += result.SuccessCount

		// Measure memory after batch
		runtime.GC()
		var currentMemStats runtime.MemStats
		runtime.ReadMemStats(&currentMemStats)
		currentMemoryMB := currentMemStats.Alloc / 1024 / 1024
		memoryMeasurements = append(memoryMeasurements, int64(currentMemoryMB))

		// Check memory hasn't grown excessively
		memoryIncrease := int64(currentMemoryMB) - int64(baselineMemoryMB)
		assert.Less(t, memoryIncrease, memoryLimitMB,
			"Memory should not increase excessively during batch %d (increased by %dMB)",
			i/batchSize, memoryIncrease)
	}

	// Verify total processing
	assert.Equal(t, objectCount, totalProcessed, "Should process all objects")

	// Analyze memory constancy
	maxMemory := int64(0)
	minMemory := int64(^uint64(0) >> 1) // Max int64
	for _, mem := range memoryMeasurements {
		if mem > maxMemory {
			maxMemory = mem
		}
		if mem < minMemory {
			minMemory = mem
		}
	}

	memoryVariation := maxMemory - minMemory
	assert.Less(t, memoryVariation, memoryLimitMB/2,
		"Memory variation should be minimal (variation: %dMB, limit: %dMB)",
		memoryVariation, memoryLimitMB/2)

	log.Printf("Memory Constancy Test %s: Processed %d objects, memory variation %dMB (baseline: %dMB, max: %dMB)",
		storageType, objectCount, memoryVariation, memoryMeasurements[0], maxMemory)
}

// TestHighConcurrencyLimits tests behavior under extreme concurrency
func TestHighConcurrencyLimits(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high concurrency test in short mode")
	}

	concurrencyTests := []struct {
		name          string
		storageType   string
		goroutines    int
		objectsPerGR  int
		workers       int
		expectSuccess bool
	}{
		{
			name:          "S3 - High concurrency 100 goroutines",
			storageType:   "s3",
			goroutines:    100,
			objectsPerGR:  50,
			workers:       200,
			expectSuccess: true,
		},
		{
			name:          "GCS - High concurrency 50 goroutines",
			storageType:   "gcs",
			goroutines:    50,
			objectsPerGR:  100,
			workers:       100,
			expectSuccess: true,
		},
		{
			name:          "Azure - High concurrency 30 goroutines",
			storageType:   "azure",
			goroutines:    30,
			objectsPerGR:  150,
			workers:       60,
			expectSuccess: true,
		},
	}

	for _, tt := range concurrencyTests {
		t.Run(tt.name, func(t *testing.T) {
			testHighConcurrencyLimits(t, tt.storageType, tt.goroutines, tt.objectsPerGR, tt.workers, tt.expectSuccess)
		})
	}
}

// testHighConcurrencyLimits tests behavior under extreme concurrency
func testHighConcurrencyLimits(t *testing.T, storageType string, goroutines int, objectsPerGR int, workers int, expectSuccess bool) {
	// Create shared storage instance
	var storage enhanced.BatchRemoteStorage
	switch storageType {
	case "s3":
		storage = createMockS3Storage(workers)
	case "gcs":
		storage = createMockGCSStorage(workers)
	case "azure":
		storage = createMockAzureStorage(workers)
	}

	var wg sync.WaitGroup
	successCount := int64(0)
	errorCount := int64(0)
	var countMutex sync.Mutex

	ctx := context.Background()
	startTime := time.Now()

	// Launch high number of concurrent goroutines
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(grIndex int) {
			defer wg.Done()

			// Generate keys for this goroutine
			keys := make([]string, objectsPerGR)
			for j := 0; j < objectsPerGR; j++ {
				keys[j] = fmt.Sprintf("high-concurrency/%s/gr-%d/file-%06d.dat", storageType, grIndex, j)
			}

			// Execute delete
			result, err := storage.DeleteBatch(ctx, keys)

			countMutex.Lock()
			if err != nil || result == nil {
				errorCount++
			} else {
				successCount += int64(result.SuccessCount)
			}
			countMutex.Unlock()
		}(i)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	totalExpected := int64(goroutines * objectsPerGR)

	if expectSuccess {
		assert.Equal(t, int64(0), errorCount, "Should have no errors under high concurrency")
		assert.Equal(t, totalExpected, successCount, "Should delete all objects under high concurrency")
	} else {
		// In failure scenarios, we might expect some errors but system should remain stable
		assert.GreaterOrEqual(t, successCount, totalExpected/2, "Should still process majority of objects")
	}

	// Calculate throughput
	throughput := float64(successCount) / totalDuration.Seconds()
	log.Printf("High Concurrency Test %s: %d goroutines × %d objects = %d processed in %v (%.2f objects/sec)",
		storageType, goroutines, objectsPerGR, successCount, totalDuration, throughput)

	// System should remain responsive under high concurrency
	assert.Less(t, totalDuration, 120*time.Second, "Should complete within reasonable time even under high concurrency")
}

// Helper functions to create mock storages

func createMockS3Storage(workers int) enhanced.BatchRemoteStorage {
	return &StressMockS3{
		bucket:     "stress-test-s3-bucket",
		maxWorkers: workers,
		supported:  true, // S3 supports batch delete
		metrics:    &enhanced.DeleteMetrics{},
		batchSize:  1000,                 // S3 batch size
		delay:      1 * time.Millisecond, // Minimal delay for stress tests
	}
}

func createMockGCSStorage(workers int) enhanced.BatchRemoteStorage {
	return &StressMockGCS{
		bucket:     "stress-test-gcs-bucket",
		maxWorkers: workers,
		supported:  false, // GCS uses parallel
		metrics:    &enhanced.DeleteMetrics{},
		delay:      2 * time.Millisecond, // Minimal delay for stress tests
	}
}

func createMockAzureStorage(workers int) enhanced.BatchRemoteStorage {
	return &StressMockAzure{
		container:  "stress-test-azure-container",
		maxWorkers: workers,
		supported:  false, // Azure uses parallel
		metrics:    &enhanced.DeleteMetrics{},
		delay:      3 * time.Millisecond, // Minimal delay for stress tests
	}
}

// Stress test specific mock implementations that focus on performance

type StressMockS3 struct {
	bucket     string
	maxWorkers int
	supported  bool
	metrics    *enhanced.DeleteMetrics
	batchSize  int
	delay      time.Duration
}

func (m *StressMockS3) DeleteBatch(ctx context.Context, keys []string) (*enhanced.BatchResult, error) {
	startTime := time.Now()

	// Simulate S3 batch processing with minimal overhead
	successCount := len(keys)
	if m.delay > 0 {
		// Simulate batch processing time
		batchCount := (len(keys) + m.batchSize - 1) / m.batchSize
		time.Sleep(time.Duration(batchCount) * m.delay)
	}

	m.metrics.FilesProcessed = int64(len(keys))
	m.metrics.FilesDeleted = int64(successCount)
	m.metrics.FilesFailed = 0
	m.metrics.APICallsCount += int64((len(keys) + m.batchSize - 1) / m.batchSize) // Batch API calls
	m.metrics.TotalDuration = time.Since(startTime)

	return &enhanced.BatchResult{
		SuccessCount: successCount,
		FailedKeys:   []enhanced.FailedKey{},
		Errors:       []error{},
	}, nil
}

// Implement RemoteStorage interface methods
func (m *StressMockS3) Kind() string                      { return "stress-mock-s3" }
func (m *StressMockS3) Connect(ctx context.Context) error { return nil }
func (m *StressMockS3) Close(ctx context.Context) error   { return nil }
func (m *StressMockS3) StatFile(ctx context.Context, key string) (storage.RemoteFile, error) {
	return nil, nil
}
func (m *StressMockS3) StatFileAbsolute(ctx context.Context, key string) (storage.RemoteFile, error) {
	return nil, nil
}
func (m *StressMockS3) DeleteFile(ctx context.Context, key string) error { return nil }
func (m *StressMockS3) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	return nil
}
func (m *StressMockS3) Walk(ctx context.Context, prefix string, recursive bool, fn func(context.Context, storage.RemoteFile) error) error {
	return nil
}
func (m *StressMockS3) WalkAbsolute(ctx context.Context, absolutePrefix string, recursive bool, fn func(context.Context, storage.RemoteFile) error) error {
	return nil
}
func (m *StressMockS3) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *StressMockS3) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *StressMockS3) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return nil, nil
}
func (m *StressMockS3) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return nil
}
func (m *StressMockS3) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return nil
}
func (m *StressMockS3) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	return 0, nil
}

// Implement BatchRemoteStorage interface methods
func (m *StressMockS3) SupportsBatchDelete() bool                 { return m.supported }
func (m *StressMockS3) GetOptimalBatchSize() int                  { return m.batchSize }
func (m *StressMockS3) GetDeleteMetrics() *enhanced.DeleteMetrics { return m.metrics }
func (m *StressMockS3) ResetDeleteMetrics()                       { m.metrics = &enhanced.DeleteMetrics{} }

type StressMockGCS struct {
	bucket     string
	maxWorkers int
	supported  bool
	metrics    *enhanced.DeleteMetrics
	delay      time.Duration
}

func (m *StressMockGCS) DeleteBatch(ctx context.Context, keys []string) (*enhanced.BatchResult, error) {
	startTime := time.Now()

	// Simulate GCS parallel processing with minimal overhead
	successCount := len(keys)
	if m.delay > 0 {
		// Simulate parallel processing time
		workerCount := m.minInt(m.maxWorkers, len(keys))
		if workerCount > 0 {
			parallelTime := time.Duration(len(keys)/workerCount) * m.delay
			time.Sleep(parallelTime)
		}
	}

	m.metrics.FilesProcessed = int64(len(keys))
	m.metrics.FilesDeleted = int64(successCount)
	m.metrics.FilesFailed = 0
	m.metrics.APICallsCount += int64(len(keys)) // Individual API calls
	m.metrics.TotalDuration = time.Since(startTime)

	return &enhanced.BatchResult{
		SuccessCount: successCount,
		FailedKeys:   []enhanced.FailedKey{},
		Errors:       []error{},
	}, nil
}

func (m *StressMockGCS) minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Implement RemoteStorage interface methods
func (m *StressMockGCS) Kind() string                      { return "stress-mock-gcs" }
func (m *StressMockGCS) Connect(ctx context.Context) error { return nil }
func (m *StressMockGCS) Close(ctx context.Context) error   { return nil }
func (m *StressMockGCS) StatFile(ctx context.Context, key string) (storage.RemoteFile, error) {
	return nil, nil
}
func (m *StressMockGCS) StatFileAbsolute(ctx context.Context, key string) (storage.RemoteFile, error) {
	return nil, nil
}
func (m *StressMockGCS) DeleteFile(ctx context.Context, key string) error { return nil }
func (m *StressMockGCS) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	return nil
}
func (m *StressMockGCS) Walk(ctx context.Context, prefix string, recursive bool, fn func(context.Context, storage.RemoteFile) error) error {
	return nil
}
func (m *StressMockGCS) WalkAbsolute(ctx context.Context, absolutePrefix string, recursive bool, fn func(context.Context, storage.RemoteFile) error) error {
	return nil
}
func (m *StressMockGCS) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *StressMockGCS) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *StressMockGCS) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return nil, nil
}
func (m *StressMockGCS) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return nil
}
func (m *StressMockGCS) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return nil
}
func (m *StressMockGCS) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	return 0, nil
}

// Implement BatchRemoteStorage interface methods
func (m *StressMockGCS) SupportsBatchDelete() bool                 { return m.supported }
func (m *StressMockGCS) GetOptimalBatchSize() int                  { return 100 }
func (m *StressMockGCS) GetDeleteMetrics() *enhanced.DeleteMetrics { return m.metrics }
func (m *StressMockGCS) ResetDeleteMetrics()                       { m.metrics = &enhanced.DeleteMetrics{} }

type StressMockAzure struct {
	container  string
	maxWorkers int
	supported  bool
	metrics    *enhanced.DeleteMetrics
	delay      time.Duration
}

func (m *StressMockAzure) DeleteBatch(ctx context.Context, keys []string) (*enhanced.BatchResult, error) {
	startTime := time.Now()

	// Simulate Azure parallel processing with minimal overhead
	successCount := len(keys)
	if m.delay > 0 {
		// Simulate parallel processing time
		workerCount := m.minInt(m.maxWorkers, len(keys))
		if workerCount > 0 {
			parallelTime := time.Duration(len(keys)/workerCount) * m.delay
			time.Sleep(parallelTime)
		}
	}

	m.metrics.FilesProcessed = int64(len(keys))
	m.metrics.FilesDeleted = int64(successCount)
	m.metrics.FilesFailed = 0
	m.metrics.APICallsCount += int64(len(keys)) // Individual API calls
	m.metrics.TotalDuration = time.Since(startTime)

	return &enhanced.BatchResult{
		SuccessCount: successCount,
		FailedKeys:   []enhanced.FailedKey{},
		Errors:       []error{},
	}, nil
}

func (m *StressMockAzure) minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Implement RemoteStorage interface methods
func (m *StressMockAzure) Kind() string                      { return "stress-mock-azure" }
func (m *StressMockAzure) Connect(ctx context.Context) error { return nil }
func (m *StressMockAzure) Close(ctx context.Context) error   { return nil }
func (m *StressMockAzure) StatFile(ctx context.Context, key string) (storage.RemoteFile, error) {
	return nil, nil
}
func (m *StressMockAzure) StatFileAbsolute(ctx context.Context, key string) (storage.RemoteFile, error) {
	return nil, nil
}
func (m *StressMockAzure) DeleteFile(ctx context.Context, key string) error { return nil }
func (m *StressMockAzure) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	return nil
}
func (m *StressMockAzure) Walk(ctx context.Context, prefix string, recursive bool, fn func(context.Context, storage.RemoteFile) error) error {
	return nil
}
func (m *StressMockAzure) WalkAbsolute(ctx context.Context, absolutePrefix string, recursive bool, fn func(context.Context, storage.RemoteFile) error) error {
	return nil
}
func (m *StressMockAzure) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *StressMockAzure) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *StressMockAzure) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return nil, nil
}
func (m *StressMockAzure) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return nil
}
func (m *StressMockAzure) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return nil
}
func (m *StressMockAzure) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	return 0, nil
}

// Implement BatchRemoteStorage interface methods
func (m *StressMockAzure) SupportsBatchDelete() bool                 { return m.supported }
func (m *StressMockAzure) GetOptimalBatchSize() int                  { return 100 }
func (m *StressMockAzure) GetDeleteMetrics() *enhanced.DeleteMetrics { return m.metrics }
func (m *StressMockAzure) ResetDeleteMetrics()                       { m.metrics = &enhanced.DeleteMetrics{} }
