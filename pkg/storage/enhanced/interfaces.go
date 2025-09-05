package enhanced

import (
	"context"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
)

// BatchRemoteStorage enhanced interface with batch operations
type BatchRemoteStorage interface {
	storage.RemoteStorage
	DeleteBatch(ctx context.Context, keys []string) (*BatchResult, error)
	SupportsBatchDelete() bool
	GetOptimalBatchSize() int
}

// BatchResult contains results of batch operations
type BatchResult struct {
	SuccessCount int
	FailedKeys   []FailedKey
	Errors       []error
}

// FailedKey represents a key that failed to delete with its error
type FailedKey struct {
	Key   string
	Error error
}

// DeleteMetrics tracks performance metrics for delete operations
type DeleteMetrics struct {
	FilesProcessed int64
	FilesDeleted   int64
	FilesFailed    int64
	BytesDeleted   int64
	APICallsCount  int64
	TotalDuration  time.Duration
	ThroughputMBps float64
}

// BackupMetadata contains cached backup information
type BackupMetadata struct {
	BackupName   string
	Exists       bool
	Size         int64
	LastModified time.Time
	Tags         string
	DataSize     int64
	MetadataSize int64
	RBACSize     int64
	DiskTypes    []string
}

// EnhancedBackupDestination provides optimized delete operations
type EnhancedBackupDestination interface {
	// Core batch operations
	DeleteBackupBatch(ctx context.Context, backupNames []string) (*BatchResult, error)

	// Cache operations
	GetBackupFromCache(backupName string) (*BackupMetadata, bool)
	SetBackupInCache(backupName string, metadata *BackupMetadata)
	InvalidateBackupCache(backupName string)

	// Metrics
	GetDeleteMetrics() *DeleteMetrics
	ResetDeleteMetrics()

	// Configuration
	IsOptimizationEnabled() bool
	GetBatchSize() int
	GetWorkerCount() int
}
