package enhanced

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog/log"
)

// EnhancedS3 implements BatchRemoteStorage for S3 with native batch delete support
type EnhancedS3 struct {
	storage.RemoteStorage
	client       *s3.Client
	config       *config.S3Config
	bucket       string
	versionCache *S3VersionCache
	metrics      *DeleteMetrics
}

// S3VersionCache caches object version information to reduce API calls
type S3VersionCache struct {
	cache map[string]S3VersionEntry
	mutex sync.RWMutex
	ttl   time.Duration
}

// S3VersionEntry represents a cached version entry
type S3VersionEntry struct {
	versions  []types.ObjectVersion
	timestamp time.Time
}

// S3ObjectIdentifier wraps object identification for batch operations
type S3ObjectIdentifier struct {
	Key       *string
	VersionId *string
}

// NewEnhancedS3 creates a new enhanced S3 storage implementation
func NewEnhancedS3(baseStorage storage.RemoteStorage, client *s3.Client, cfg *config.Config) (*EnhancedS3, error) {
	s3cfg := &cfg.S3

	// Validate configuration
	if s3cfg.Bucket == "" {
		return nil, &OptimizationConfigError{
			Field:   "bucket",
			Value:   s3cfg.Bucket,
			Message: "S3 bucket name cannot be empty",
		}
	}

	versionCache := &S3VersionCache{
		cache: make(map[string]S3VersionEntry),
		ttl:   5 * time.Minute, // Cache versions for 5 minutes
	}

	return &EnhancedS3{
		RemoteStorage: baseStorage,
		client:        client,
		config:        s3cfg,
		bucket:        s3cfg.Bucket,
		versionCache:  versionCache,
		metrics:       &DeleteMetrics{},
	}, nil
}

// DeleteBatch implements BatchRemoteStorage interface for S3 batch delete
func (s *EnhancedS3) DeleteBatch(ctx context.Context, keys []string) (*BatchResult, error) {
	startTime := time.Now()
	defer func() {
		s.metrics.TotalDuration = time.Since(startTime)
		s.updateThroughputMetrics()
	}()

	if len(keys) == 0 {
		return &BatchResult{SuccessCount: 0}, nil
	}

	log.Debug().Int("key_count", len(keys)).Msg("starting S3 batch delete")
	s.metrics.FilesProcessed = int64(len(keys))

	// Check for versioned bucket and preload versions if needed
	versions, err := s.preloadVersionsIfNeeded(ctx, keys)
	if err != nil {
		log.Warn().Err(err).Msg("failed to preload versions, proceeding with standard delete")
		versions = nil
	}

	// Split into batches of 1000 (S3 API limit)
	const maxBatchSize = 1000
	var allFailedKeys []FailedKey
	var allErrors []error
	totalSuccessCount := 0

	for i := 0; i < len(keys); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(keys) {
			end = len(keys)
		}

		batchKeys := keys[i:end]
		batchResult, err := s.deleteBatch(ctx, batchKeys, versions)
		if err != nil {
			// If batch fails entirely, mark all keys as failed
			for _, key := range batchKeys {
				allFailedKeys = append(allFailedKeys, FailedKey{
					Key:   key,
					Error: err,
				})
			}
			allErrors = append(allErrors, err)
		} else {
			totalSuccessCount += batchResult.SuccessCount
			allFailedKeys = append(allFailedKeys, batchResult.FailedKeys...)
			allErrors = append(allErrors, batchResult.Errors...)
		}
	}

	s.metrics.FilesDeleted = int64(totalSuccessCount)
	s.metrics.FilesFailed = int64(len(allFailedKeys))
	s.metrics.APICallsCount++ // Count the batch operations

	result := &BatchResult{
		SuccessCount: totalSuccessCount,
		FailedKeys:   allFailedKeys,
		Errors:       allErrors,
	}

	log.Debug().
		Int("success_count", totalSuccessCount).
		Int("failed_count", len(allFailedKeys)).
		Str("duration", time.Since(startTime).String()).
		Msg("S3 batch delete completed")

	return result, nil
}

// deleteBatch performs a single S3 DeleteObjects operation
func (s *EnhancedS3) deleteBatch(ctx context.Context, keys []string, versions map[string][]types.ObjectVersion) (*BatchResult, error) {
	objects := s.buildObjectIdentifiers(keys, versions)

	if len(objects) == 0 {
		return &BatchResult{SuccessCount: 0}, nil
	}

	// Prepare S3 DeleteObjects request
	deleteRequest := &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(false), // We want to see what was deleted
		},
	}

	// Execute batch delete with retry logic
	var resp *s3.DeleteObjectsOutput
	var err error

	for attempt := 0; attempt <= 3; attempt++ {
		resp, err = s.client.DeleteObjects(ctx, deleteRequest)
		if err == nil {
			break
		}

		if !IsRetriableError(err) {
			break
		}

		// Exponential backoff
		if attempt < 3 {
			backoff := time.Duration(1<<attempt) * time.Second
			log.Debug().
				Err(err).
				Int("attempt", attempt+1).
				Str("backoff", backoff.String()).
				Msg("retrying S3 batch delete")

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	if err != nil {
		return nil, fmt.Errorf("S3 batch delete failed after retries: %w", err)
	}

	return s.processBatchDeleteResponse(resp, keys), nil
}

// buildObjectIdentifiers creates S3 object identifiers for deletion
func (s *EnhancedS3) buildObjectIdentifiers(keys []string, versions map[string][]types.ObjectVersion) []types.ObjectIdentifier {
	var objects []types.ObjectIdentifier

	for _, key := range keys {
		// Add current version
		objects = append(objects, types.ObjectIdentifier{
			Key: aws.String(key),
		})

		// Add all versions if versioning is enabled and we have cached versions
		if versions != nil {
			if keyVersions, exists := versions[key]; exists {
				for _, version := range keyVersions {
					if version.VersionId != nil && *version.VersionId != "null" {
						objects = append(objects, types.ObjectIdentifier{
							Key:       aws.String(key),
							VersionId: version.VersionId,
						})
					}
				}
			}
		}
	}

	return objects
}

// processBatchDeleteResponse processes the S3 DeleteObjects response
func (s *EnhancedS3) processBatchDeleteResponse(resp *s3.DeleteObjectsOutput, originalKeys []string) *BatchResult {
	successCount := len(resp.Deleted)
	var failedKeys []FailedKey
	var errors []error

	// Process deletion errors
	for _, deleteError := range resp.Errors {
		failedKeys = append(failedKeys, FailedKey{
			Key:   aws.ToString(deleteError.Key),
			Error: fmt.Errorf("S3 delete error: %s - %s", aws.ToString(deleteError.Code), aws.ToString(deleteError.Message)),
		})
		errors = append(errors, fmt.Errorf("failed to delete %s: %s", aws.ToString(deleteError.Key), aws.ToString(deleteError.Message)))
	}

	return &BatchResult{
		SuccessCount: successCount,
		FailedKeys:   failedKeys,
		Errors:       errors,
	}
}

// preloadVersionsIfNeeded preloads object versions for efficient versioned deletion
func (s *EnhancedS3) preloadVersionsIfNeeded(ctx context.Context, keys []string) (map[string][]types.ObjectVersion, error) {
	// Use S3 optimizations from delete config
	// This will be properly configured when integrated with the wrapper
	// For now, assume preloading is enabled

	// Check if versioning is enabled on the bucket
	versioningEnabled, err := s.isBucketVersioningEnabled(ctx)
	if err != nil {
		log.Debug().Err(err).Msg("failed to check bucket versioning status")
		return nil, nil
	}

	if !versioningEnabled {
		log.Debug().Msg("bucket versioning not enabled, skipping version preload")
		return nil, nil
	}

	log.Debug().Int("key_count", len(keys)).Msg("preloading S3 object versions")

	versions := make(map[string][]types.ObjectVersion)

	// Use worker pool for parallel version collection
	workerCount := runtime.NumCPU()
	if workerCount > 10 {
		workerCount = 10 // Limit to avoid overwhelming S3
	}

	keyChannel := make(chan string, len(keys))
	resultChannel := make(chan map[string][]types.ObjectVersion, workerCount)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go s.versionWorker(ctx, keyChannel, resultChannel, &wg)
	}

	// Send keys to workers
	for _, key := range keys {
		keyChannel <- key
	}
	close(keyChannel)

	// Collect results
	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	for workerVersions := range resultChannel {
		for key, keyVersions := range workerVersions {
			versions[key] = keyVersions
		}
	}

	log.Debug().Int("versioned_objects", len(versions)).Msg("completed S3 version preload")
	return versions, nil
}

// versionWorker processes keys in parallel to collect version information
func (s *EnhancedS3) versionWorker(ctx context.Context, keyChannel <-chan string, resultChannel chan<- map[string][]types.ObjectVersion, wg *sync.WaitGroup) {
	defer wg.Done()

	versions := make(map[string][]types.ObjectVersion)

	for key := range keyChannel {
		select {
		case <-ctx.Done():
			return
		default:
			keyVersions, err := s.getObjectVersions(ctx, key)
			if err != nil {
				log.Debug().Err(err).Str("key", key).Msg("failed to get object versions")
				continue
			}
			if len(keyVersions) > 0 {
				versions[key] = keyVersions
			}
		}
	}

	resultChannel <- versions
}

// getObjectVersions retrieves all versions for a specific object
func (s *EnhancedS3) getObjectVersions(ctx context.Context, key string) ([]types.ObjectVersion, error) {
	// Check cache first
	if cached := s.versionCache.get(key); cached != nil {
		return cached, nil
	}

	// List object versions
	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(key),
	}

	var versions []types.ObjectVersion
	paginator := s3.NewListObjectVersionsPaginator(s.client, input)

	for paginator.HasMorePages() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list object versions for %s: %w", key, err)
		}

		for _, version := range resp.Versions {
			if aws.ToString(version.Key) == key {
				versions = append(versions, version)
			}
		}
	}

	// Cache the results
	s.versionCache.set(key, versions)
	return versions, nil
}

// isBucketVersioningEnabled checks if versioning is enabled on the bucket
func (s *EnhancedS3) isBucketVersioningEnabled(ctx context.Context) (bool, error) {
	input := &s3.GetBucketVersioningInput{
		Bucket: aws.String(s.bucket),
	}

	resp, err := s.client.GetBucketVersioning(ctx, input)
	if err != nil {
		return false, err
	}

	return resp.Status == types.BucketVersioningStatusEnabled, nil
}

// updateThroughputMetrics calculates and updates throughput metrics
func (s *EnhancedS3) updateThroughputMetrics() {
	if s.metrics.TotalDuration > 0 && s.metrics.BytesDeleted > 0 {
		throughputBytes := float64(s.metrics.BytesDeleted) / s.metrics.TotalDuration.Seconds()
		s.metrics.ThroughputMBps = throughputBytes / (1024 * 1024) // Convert to MB/s
	}
}

// SupportsBatchDelete returns true as S3 supports native batch delete
func (s *EnhancedS3) SupportsBatchDelete() bool {
	return true
}

// GetOptimalBatchSize returns the optimal batch size for S3 (1000 objects per request)
func (s *EnhancedS3) GetOptimalBatchSize() int {
	return 1000
}

// GetDeleteMetrics returns current delete operation metrics
func (s *EnhancedS3) GetDeleteMetrics() *DeleteMetrics {
	return s.metrics
}

// ResetDeleteMetrics resets the delete operation metrics
func (s *EnhancedS3) ResetDeleteMetrics() {
	s.metrics = &DeleteMetrics{}
}

// S3VersionCache methods

// get retrieves cached versions for a key if not expired
func (cache *S3VersionCache) get(key string) []types.ObjectVersion {
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()

	if entry, exists := cache.cache[key]; exists {
		if time.Since(entry.timestamp) < cache.ttl {
			return entry.versions
		}
		// Entry expired, remove it
		delete(cache.cache, key)
	}
	return nil
}

// set stores versions for a key in cache
func (cache *S3VersionCache) set(key string, versions []types.ObjectVersion) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	cache.cache[key] = S3VersionEntry{
		versions:  versions,
		timestamp: time.Now(),
	}
}

// optimizeVersionedDelete performs optimized deletion of versioned objects
func (s *EnhancedS3) optimizeVersionedDelete(keys []string) error {
	log.Debug().Int("key_count", len(keys)).Msg("optimizing versioned delete for S3")

	ctx := context.Background()
	versions, err := s.preloadVersionsIfNeeded(ctx, keys)
	if err != nil {
		return fmt.Errorf("failed to preload versions: %w", err)
	}

	if versions == nil {
		log.Debug().Msg("no versions to optimize, using standard delete")
		return nil
	}

	// Build complete list of objects to delete (current + all versions)
	var allObjects []types.ObjectIdentifier
	for _, key := range keys {
		allObjects = append(allObjects, types.ObjectIdentifier{
			Key: aws.String(key),
		})

		if keyVersions, exists := versions[key]; exists {
			for _, version := range keyVersions {
				if version.VersionId != nil && *version.VersionId != "null" {
					allObjects = append(allObjects, types.ObjectIdentifier{
						Key:       aws.String(key),
						VersionId: version.VersionId,
					})
				}
			}
		}
	}

	log.Debug().
		Int("original_keys", len(keys)).
		Int("total_objects", len(allObjects)).
		Msg("prepared versioned delete operation")

	// Execute batch delete for all versions
	const maxBatchSize = 1000
	for i := 0; i < len(allObjects); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(allObjects) {
			end = len(allObjects)
		}

		batchObjects := allObjects[i:end]
		deleteRequest := &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{
				Objects: batchObjects,
				Quiet:   aws.Bool(true), // Use quiet mode for version cleanup
			},
		}

		_, err := s.client.DeleteObjects(ctx, deleteRequest)
		if err != nil {
			return fmt.Errorf("failed to delete object batch: %w", err)
		}

		s.metrics.APICallsCount++
	}

	return nil
}
