package enhanced

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// BackupExistenceCache manages caching of backup existence checks
type BackupExistenceCache struct {
	cache map[string]CacheEntry
	mutex sync.RWMutex
	ttl   time.Duration
}

// CacheEntry represents a cached backup metadata entry
type CacheEntry struct {
	Exists    bool
	Timestamp time.Time
	Metadata  *BackupMetadata
}

// NewBackupExistenceCache creates a new backup existence cache
func NewBackupExistenceCache(ttl time.Duration) *BackupExistenceCache {
	return &BackupExistenceCache{
		cache: make(map[string]CacheEntry),
		ttl:   ttl,
	}
}

// Get retrieves backup metadata from cache if available and not expired
func (c *BackupExistenceCache) Get(backupName string) (*BackupMetadata, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	entry, exists := c.cache[backupName]
	if !exists {
		return nil, false
	}

	// Check if entry has expired
	if c.isExpired(entry) {
		return nil, false
	}

	return entry.Metadata, entry.Exists
}

// Set stores backup metadata in cache
func (c *BackupExistenceCache) Set(backupName string, metadata *BackupMetadata) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	entry := CacheEntry{
		Exists:    metadata != nil,
		Timestamp: time.Now(),
		Metadata:  metadata,
	}

	c.cache[backupName] = entry
	log.Debug().Str("backup", backupName).Bool("exists", entry.Exists).Msg("cached backup metadata")
}

// SetNotExists marks a backup as not existing in cache
func (c *BackupExistenceCache) SetNotExists(backupName string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	entry := CacheEntry{
		Exists:    false,
		Timestamp: time.Now(),
		Metadata:  nil,
	}

	c.cache[backupName] = entry
	log.Debug().Str("backup", backupName).Msg("cached backup as non-existent")
}

// Invalidate removes a backup from cache
func (c *BackupExistenceCache) Invalidate(backupName string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.cache, backupName)
	log.Debug().Str("backup", backupName).Msg("invalidated backup cache entry")
}

// InvalidateAll clears the entire cache
func (c *BackupExistenceCache) InvalidateAll() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache = make(map[string]CacheEntry)
	log.Debug().Msg("invalidated all backup cache entries")
}

// Cleanup removes expired entries from cache
func (c *BackupExistenceCache) Cleanup(ctx context.Context) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	before := len(c.cache)
	for backupName, entry := range c.cache {
		select {
		case <-ctx.Done():
			return
		default:
			if c.isExpired(entry) {
				delete(c.cache, backupName)
			}
		}
	}
	after := len(c.cache)

	if removed := before - after; removed > 0 {
		log.Debug().Int("removed", removed).Int("remaining", after).Msg("cleaned up expired cache entries")
	}
}

// Size returns the current number of entries in cache
func (c *BackupExistenceCache) Size() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return len(c.cache)
}

// Stats returns cache statistics
func (c *BackupExistenceCache) Stats() CacheStats {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	stats := CacheStats{
		TotalEntries: len(c.cache),
		TTL:          c.ttl,
	}

	now := time.Now()
	for _, entry := range c.cache {
		if entry.Exists {
			stats.ExistingBackups++
		} else {
			stats.NonExistentBackups++
		}

		if now.Sub(entry.Timestamp) > c.ttl {
			stats.ExpiredEntries++
		}
	}

	return stats
}

// StartCleanupRoutine starts a background cleanup routine
func (c *BackupExistenceCache) StartCleanupRoutine(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("stopping backup cache cleanup routine")
			return
		case <-ticker.C:
			c.Cleanup(ctx)
		}
	}
}

// isExpired checks if a cache entry has expired
func (c *BackupExistenceCache) isExpired(entry CacheEntry) bool {
	return time.Since(entry.Timestamp) > c.ttl
}

// CacheStats contains cache statistics
type CacheStats struct {
	TotalEntries       int
	ExistingBackups    int
	NonExistentBackups int
	ExpiredEntries     int
	TTL                time.Duration
}

// HitRate calculates the cache hit rate based on provided hit/miss counts
func (s *CacheStats) HitRate(hits, misses int) float64 {
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return float64(hits) / float64(total)
}

// BatchedBackupExistenceCache provides batch operations for backup existence checks
type BatchedBackupExistenceCache struct {
	*BackupExistenceCache
	batchSize int
}

// NewBatchedBackupExistenceCache creates a new batched backup existence cache
func NewBatchedBackupExistenceCache(ttl time.Duration, batchSize int) *BatchedBackupExistenceCache {
	return &BatchedBackupExistenceCache{
		BackupExistenceCache: NewBackupExistenceCache(ttl),
		batchSize:            batchSize,
	}
}

// GetBatch retrieves multiple backup metadata entries from cache
func (c *BatchedBackupExistenceCache) GetBatch(backupNames []string) map[string]*BackupMetadata {
	results := make(map[string]*BackupMetadata)

	for _, backupName := range backupNames {
		if metadata, exists := c.Get(backupName); exists {
			results[backupName] = metadata
		}
	}

	return results
}

// SetBatch stores multiple backup metadata entries in cache
func (c *BatchedBackupExistenceCache) SetBatch(metadataMap map[string]*BackupMetadata) {
	for backupName, metadata := range metadataMap {
		c.Set(backupName, metadata)
	}
}

// InvalidateBatch removes multiple backups from cache
func (c *BatchedBackupExistenceCache) InvalidateBatch(backupNames []string) {
	for _, backupName := range backupNames {
		c.Invalidate(backupName)
	}
}

// GetMissing returns backup names that are not in cache or have expired
func (c *BatchedBackupExistenceCache) GetMissing(backupNames []string) []string {
	var missing []string

	for _, backupName := range backupNames {
		if _, exists := c.Get(backupName); !exists {
			missing = append(missing, backupName)
		}
	}

	return missing
}
