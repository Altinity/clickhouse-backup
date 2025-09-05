package test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BenchmarkResult contains performance metrics from benchmark runs
type BenchmarkResult struct {
	TestName         string
	BackupSize       int
	StorageType      string
	IsEnhanced       bool
	Duration         time.Duration
	APICallsCount    int64
	FilesProcessed   int64
	ThroughputMBps   float64
	MemoryUsageMB    float64
	ImprovementRatio float64
}

// BenchmarkSuite runs comprehensive performance benchmarks
type BenchmarkSuite struct {
	results []BenchmarkResult
	mutex   sync.Mutex
}

func NewBenchmarkSuite() *BenchmarkSuite {
	return &BenchmarkSuite{
		results: make([]BenchmarkResult, 0),
	}
}

func (bs *BenchmarkSuite) AddResult(result BenchmarkResult) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	bs.results = append(bs.results, result)
}

func (bs *BenchmarkSuite) GetResults() []BenchmarkResult {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	return bs.results
}

// TestPerformanceComparison tests performance improvements across different scenarios
func TestPerformanceComparison(t *testing.T) {
	suite := NewBenchmarkSuite()

	// Test scenarios with different backup sizes
	scenarios := []struct {
		name      string
		fileCount int
		category  string
	}{
		{"Small_Backup", 50, "Small (< 100 files)"},
		{"Medium_Backup", 500, "Medium (100-1000 files)"},
		{"Large_Backup", 2000, "Large (1000+ files)"},
		{"XLarge_Backup", 5000, "XLarge (5000+ files)"},
	}

	storageTypes := []string{"s3", "gcs", "azblob"}

	for _, scenario := range scenarios {
		for _, storageType := range storageTypes {
			t.Run(fmt.Sprintf("%s_%s", scenario.name, storageType), func(t *testing.T) {
				// Run benchmark for original implementation
				originalResult := runDeleteBenchmark(t, scenario.fileCount, storageType, false)
				originalResult.TestName = fmt.Sprintf("%s_%s_Original", scenario.name, storageType)
				suite.AddResult(originalResult)

				// Run benchmark for enhanced implementation
				enhancedResult := runDeleteBenchmark(t, scenario.fileCount, storageType, true)
				enhancedResult.TestName = fmt.Sprintf("%s_%s_Enhanced", scenario.name, storageType)

				// Calculate improvement ratio
				if originalResult.Duration > 0 {
					enhancedResult.ImprovementRatio = float64(originalResult.Duration) / float64(enhancedResult.Duration)
				}
				suite.AddResult(enhancedResult)

				// Validate performance improvements
				validatePerformanceImprovement(t, originalResult, enhancedResult, scenario.category, storageType)
			})
		}
	}

	// Print comprehensive performance report
	printPerformanceReport(t, suite.GetResults())
}

func runDeleteBenchmark(t *testing.T, fileCount int, storageType string, useEnhanced bool) BenchmarkResult {
	r := require.New(t)

	// Create mock storage with simulated files
	mockStorage := createMockStorage(storageType, fileCount, useEnhanced)

	// Record memory usage before test
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	// Start performance measurement
	startTime := time.Now()

	ctx := context.Background()
	var err error
	var apiCalls int64
	var filesProcessed int64

	if useEnhanced {
		// Test enhanced delete using mock batch storage
		batchStorage, ok := mockStorage.(*MockBatchRemoteStorage)
		if !ok {
			t.Fatal("Expected MockBatchRemoteStorage for enhanced test")
		}

		// Simulate batch deletion
		keys := generateTestKeys(fileCount)
		batchSize := 1000
		for i := 0; i < len(keys); i += batchSize {
			end := i + batchSize
			if end > len(keys) {
				end = len(keys)
			}
			batch := keys[i:end]

			result, batchErr := batchStorage.DeleteBatch(ctx, batch)
			if batchErr != nil {
				err = batchErr
				break
			}
			apiCalls++
			filesProcessed += int64(result.SuccessCount)
		}
	} else {
		// Test original delete (sequential)
		keys := generateTestKeys(fileCount)
		for _, key := range keys {
			err = mockStorage.DeleteFile(ctx, key)
			if err != nil {
				break
			}
			apiCalls++
			filesProcessed++
		}
	}

	duration := time.Since(startTime)

	// Record memory usage after test
	var memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	r.NoError(err)

	return BenchmarkResult{
		BackupSize:     fileCount,
		StorageType:    storageType,
		IsEnhanced:     useEnhanced,
		Duration:       duration,
		APICallsCount:  apiCalls,
		FilesProcessed: filesProcessed,
		ThroughputMBps: calculateThroughput(fileCount, duration),
		MemoryUsageMB:  float64(memAfter.Sys-memBefore.Sys) / (1024 * 1024),
	}
}

func createTestConfig(storageType string, useEnhanced bool) *config.Config {
	cfg := &config.Config{
		General: config.GeneralConfig{
			RemoteStorage: storageType,
			BatchDeletion: config.BatchDeletionConfig{
				Enabled:          useEnhanced,
				BatchSize:        1000,
				Workers:          10,
				RetryAttempts:    3,
				FailureThreshold: 0.1,
				ErrorStrategy:    "retry_batch",
			},
		},
	}

	// Configure storage-specific optimizations
	switch storageType {
	case "s3":
		cfg.S3.BatchDeletion = config.S3BatchConfig{
			UseBatchAPI:        true,
			VersionConcurrency: 10,
			PreloadVersions:    true,
		}
	case "gcs":
		cfg.GCS.BatchDeletion = config.GCSBatchConfig{
			MaxWorkers:    50,
			UseClientPool: true,
			UseBatchAPI:   true,
		}
	case "azblob":
		cfg.AzureBlob.BatchDeletion = config.AzureBatchConfig{
			UseBatchAPI: true,
			MaxWorkers:  20,
		}
	}

	return cfg
}

func createMockStorage(storageType string, fileCount int, useEnhanced bool) storage.RemoteStorage {
	if useEnhanced {
		return &MockBatchRemoteStorage{
			MockRemoteStorage: MockRemoteStorage{kind: storageType},
			batchSize:         1000,
			supported:         true,
			simulateFiles:     fileCount,
			simulateDelay:     time.Millisecond, // Simulate network latency
		}
	}

	return &MockRemoteStorage{kind: storageType}
}

func generateTestKeys(count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("backup/test/file_%d.dat", i)
	}
	return keys
}

func calculateThroughput(fileCount int, duration time.Duration) float64 {
	if duration.Seconds() == 0 {
		return 0
	}
	// Assume average file size of 1MB for throughput calculation
	avgFileSizeMB := 1.0
	totalMB := float64(fileCount) * avgFileSizeMB
	return totalMB / duration.Seconds()
}

func validatePerformanceImprovement(t *testing.T, original, enhancedRes BenchmarkResult, category, storageType string) {
	// Define expected improvement ratios based on backup size and storage type
	expectedImprovements := map[string]map[string]float64{
		"Small (< 100 files)": {
			"s3":     2.0, // 2x improvement
			"gcs":    2.0,
			"azblob": 1.5,
		},
		"Medium (100-1000 files)": {
			"s3":     10.0, // 10x improvement
			"gcs":    8.0,
			"azblob": 5.0,
		},
		"Large (1000+ files)": {
			"s3":     25.0, // 25x improvement
			"gcs":    20.0,
			"azblob": 15.0,
		},
		"XLarge (5000+ files)": {
			"s3":     50.0, // 50x improvement
			"gcs":    40.0,
			"azblob": 25.0,
		},
	}

	expectedRatio := expectedImprovements[category][storageType]

	// Validate duration improvement
	assert.GreaterOrEqual(t, enhancedRes.ImprovementRatio, expectedRatio,
		"Duration improvement for %s on %s should be at least %.1fx", category, storageType, expectedRatio)

	// Validate API call reduction (should be significant for batch operations)
	if storageType == "s3" && enhancedRes.BackupSize >= 100 {
		apiCallReduction := float64(original.APICallsCount) / float64(enhancedRes.APICallsCount)
		assert.GreaterOrEqual(t, apiCallReduction, 10.0,
			"API call reduction for S3 should be at least 10x for large backups")
	}

	// Validate memory usage remains reasonable
	assert.LessOrEqual(t, enhancedRes.MemoryUsageMB, 100.0,
		"Memory usage should not exceed 100MB")

	// Validate throughput improvement
	if original.ThroughputMBps > 0 {
		throughputImprovement := enhancedRes.ThroughputMBps / original.ThroughputMBps
		assert.GreaterOrEqual(t, throughputImprovement, 2.0,
			"Throughput should improve by at least 2x")
	}
}

func printPerformanceReport(t *testing.T, results []BenchmarkResult) {
	fmt.Println("\n=== ENHANCED DELETE PERFORMANCE REPORT ===")
	fmt.Println()

	// Group results by test scenario
	scenarios := make(map[string][]BenchmarkResult)
	for _, result := range results {
		scenario := fmt.Sprintf("%s_%s", getBackupCategory(result.BackupSize), result.StorageType)
		scenarios[scenario] = append(scenarios[scenario], result)
	}

	for scenario, results := range scenarios {
		fmt.Printf("Scenario: %s\n", scenario)
		fmt.Println("----------------------------------------")

		var original, enhancedRes *BenchmarkResult
		for _, result := range results {
			if result.IsEnhanced {
				enhancedRes = &result
			} else {
				original = &result
			}
		}

		if original != nil && enhancedRes != nil {
			fmt.Printf("Original Duration:    %v\n", original.Duration)
			fmt.Printf("Enhanced Duration:    %v\n", enhancedRes.Duration)
			fmt.Printf("Improvement Ratio:    %.2fx\n", enhancedRes.ImprovementRatio)
			fmt.Printf("Original API Calls:   %d\n", original.APICallsCount)
			fmt.Printf("Enhanced API Calls:   %d\n", enhancedRes.APICallsCount)
			if original.APICallsCount > 0 {
				apiReduction := float64(original.APICallsCount) / float64(enhancedRes.APICallsCount)
				fmt.Printf("API Call Reduction:   %.2fx\n", apiReduction)
			}
			fmt.Printf("Enhanced Throughput:  %.2f MB/s\n", enhancedRes.ThroughputMBps)
			fmt.Printf("Memory Usage:         %.2f MB\n", enhancedRes.MemoryUsageMB)
		}
		fmt.Println()
	}

	// Print summary
	fmt.Println("=== PERFORMANCE SUMMARY ===")
	fmt.Printf("Total test scenarios: %d\n", len(scenarios))

	// Calculate average improvements
	var totalImprovement float64
	var count int
	for _, results := range scenarios {
		for _, result := range results {
			if result.IsEnhanced && result.ImprovementRatio > 0 {
				totalImprovement += result.ImprovementRatio
				count++
			}
		}
	}

	if count > 0 {
		avgImprovement := totalImprovement / float64(count)
		fmt.Printf("Average performance improvement: %.2fx\n", avgImprovement)
	}

	fmt.Println("=== END REPORT ===")
}

func getBackupCategory(fileCount int) string {
	switch {
	case fileCount < 100:
		return "Small"
	case fileCount < 1000:
		return "Medium"
	case fileCount < 5000:
		return "Large"
	default:
		return "XLarge"
	}
}

// BenchmarkS3DeleteOperations benchmarks S3 delete operations
func BenchmarkS3DeleteOperations(b *testing.B) {
	benchmarkDeleteOperations(b, "s3")
}

// BenchmarkGCSDeleteOperations benchmarks GCS delete operations
func BenchmarkGCSDeleteOperations(b *testing.B) {
	benchmarkDeleteOperations(b, "gcs")
}

// BenchmarkAzureBlobDeleteOperations benchmarks Azure Blob delete operations
func BenchmarkAzureBlobDeleteOperations(b *testing.B) {
	benchmarkDeleteOperations(b, "azblob")
}

func benchmarkDeleteOperations(b *testing.B, storageType string) {
	fileCounts := []int{100, 1000, 5000}

	for _, fileCount := range fileCounts {
		b.Run(fmt.Sprintf("Original_%d_files", fileCount), func(b *testing.B) {
			benchmarkSingleOperation(b, storageType, fileCount, false)
		})

		b.Run(fmt.Sprintf("Enhanced_%d_files", fileCount), func(b *testing.B) {
			benchmarkSingleOperation(b, storageType, fileCount, true)
		})
	}
}

func benchmarkSingleOperation(b *testing.B, storageType string, fileCount int, useEnhanced bool) {
	cfg := createTestConfig(storageType, useEnhanced)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		mockStorage := createMockStorage(storageType, fileCount, useEnhanced)

		ctx := context.Background()
		startTime := time.Now()

		if useEnhanced {
			batchStorage := mockStorage.(*MockBatchRemoteStorage)
			keys := generateTestKeys(fileCount)
			batchSize := cfg.General.BatchDeletion.BatchSize

			for j := 0; j < len(keys); j += batchSize {
				end := j + batchSize
				if end > len(keys) {
					end = len(keys)
				}
				batch := keys[j:end]

				_, err := batchStorage.DeleteBatch(ctx, batch)
				if err != nil {
					b.Fatal(err)
				}
			}
		} else {
			keys := generateTestKeys(fileCount)
			for _, key := range keys {
				err := mockStorage.DeleteFile(ctx, key)
				if err != nil {
					b.Fatal(err)
				}
			}
		}

		duration := time.Since(startTime)
		b.ReportMetric(duration.Seconds(), "duration_seconds")
		b.ReportMetric(float64(fileCount)/duration.Seconds(), "files_per_second")
	}
}

// TestAPICallReduction validates the reduction in API calls
func TestAPICallReduction(t *testing.T) {
	scenarios := []struct {
		storageType       string
		fileCount         int
		expectedReduction float64
	}{
		{"s3", 1000, 100.0},    // S3 batch delete: 1000 objects = 1 API call vs 1000 individual calls
		{"gcs", 1000, 10.0},    // GCS parallel workers reduce total time
		{"azblob", 1000, 20.0}, // Azure batch operations vs individual deletions
	}

	for _, scenario := range scenarios {
		t.Run(fmt.Sprintf("%s_%d_files", scenario.storageType, scenario.fileCount), func(t *testing.T) {
			r := require.New(t)

			// Test original implementation
			originalStorage := createMockStorage(scenario.storageType, scenario.fileCount, false)

			ctx := context.Background()
			keys := generateTestKeys(scenario.fileCount)

			originalAPICalls := int64(0)
			for _, key := range keys {
				err := originalStorage.DeleteFile(ctx, key)
				r.NoError(err)
				originalAPICalls++
			}

			// Test enhanced implementation
			enhancedStorage := createMockStorage(scenario.storageType, scenario.fileCount, true)
			batchStorage := enhancedStorage.(*MockBatchRemoteStorage)

			enhancedAPICalls := int64(0)
			batchSize := 1000
			for i := 0; i < len(keys); i += batchSize {
				end := i + batchSize
				if end > len(keys) {
					end = len(keys)
				}
				batch := keys[i:end]

				_, err := batchStorage.DeleteBatch(ctx, batch)
				r.NoError(err)
				enhancedAPICalls++
			}

			// Validate API call reduction
			reduction := float64(originalAPICalls) / float64(enhancedAPICalls)
			assert.GreaterOrEqual(t, reduction, scenario.expectedReduction,
				"API call reduction for %s should be at least %.1fx", scenario.storageType, scenario.expectedReduction)

			t.Logf("Storage: %s, Files: %d, Original API Calls: %d, Enhanced API Calls: %d, Reduction: %.2fx",
				scenario.storageType, scenario.fileCount, originalAPICalls, enhancedAPICalls, reduction)
		})
	}
}

// TestMemoryUsage validates that memory usage remains constant regardless of backup size
func TestMemoryUsage(t *testing.T) {
	fileCounts := []int{100, 1000, 5000, 10000}

	for _, fileCount := range fileCounts {
		t.Run(fmt.Sprintf("Memory_Usage_%d_files", fileCount), func(t *testing.T) {
			var memBefore, memAfter runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&memBefore)

			mockStorage := createMockStorage("s3", fileCount, true)
			batchStorage := mockStorage.(*MockBatchRemoteStorage)

			ctx := context.Background()
			keys := generateTestKeys(fileCount)
			batchSize := 1000

			for i := 0; i < len(keys); i += batchSize {
				end := i + batchSize
				if end > len(keys) {
					end = len(keys)
				}
				batch := keys[i:end]

				_, err := batchStorage.DeleteBatch(ctx, batch)
				require.NoError(t, err)
			}

			runtime.GC()
			runtime.ReadMemStats(&memAfter)

			memoryUsageMB := float64(memAfter.Sys-memBefore.Sys) / (1024 * 1024)

			// Memory usage should not scale with file count - should remain under 100MB
			assert.LessOrEqual(t, memoryUsageMB, 100.0,
				"Memory usage should not exceed 100MB regardless of backup size")

			t.Logf("File count: %d, Memory usage: %.2f MB", fileCount, memoryUsageMB)
		})
	}
}
