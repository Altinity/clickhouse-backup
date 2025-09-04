# ClickHouse Backup Performance Improvements

## Overview

This document outlines the comprehensive performance improvements made to address inconsistent download performance in clickhouse-backup. The improvements target the issue where downloads "start strong but become bumpy and progressively slower towards completion."

## Key Problems Identified

1. **Multi-level concurrency conflicts**: Fixed and object disk operations used separate, conflicting concurrency limits
2. **Fixed buffer sizes**: Static buffer sizes didn't adapt to file size or network conditions  
3. **Client pool exhaustion**: GCS client pools could become exhausted under high concurrency
4. **Storage-specific bottlenecks**: Each storage backend had different optimal concurrency patterns
5. **Lack of performance monitoring**: No visibility into download performance degradation

## Implemented Solutions

### 1. Adaptive Concurrency Management (`pkg/config/config.go`)

**New Methods:**
- `GetOptimalDownloadConcurrency()`: Calculates optimal concurrency based on storage type
- `GetOptimalUploadConcurrency()`: Storage-aware upload concurrency 
- `GetOptimalObjectDiskConcurrency()`: Unified object disk concurrency
- `CalculateOptimalBufferSize()`: Dynamic buffer sizing based on file size and concurrency
- `GetOptimalClientPoolSize()`: Adaptive GCS client pool sizing

**Storage-Specific Concurrency:**
- **GCS**: 2x CPU cores, max 50 (conservative due to client pool limits)
- **S3**: 3x CPU cores, max 100 (handles higher concurrency well)
- **Azure Blob**: 2x CPU cores, max 25 (more conservative)
- **Other storage**: 2x CPU cores, max 20

### 2. Enhanced Storage Backends

#### GCS Storage (`pkg/storage/gcs.go`)
- Added config reference for adaptive operations
- Dynamic client pool sizing: `gcs.cfg.GetOptimalClientPoolSize()`
- Adaptive buffer sizing for uploads
- Dynamic chunk sizing (256KB to 100MB based on file size)

#### S3 Storage (`pkg/storage/s3.go`)
- Adaptive buffer sizing in `GetFileReaderWithLocalPath()` and `PutFileAbsolute()`
- Buffer size calculation: `config.CalculateOptimalBufferSize(remoteSize, s.Concurrency)`
- Applied to both download and upload operations

#### Azure Blob Storage (`pkg/storage/azblob.go`)
- Adaptive buffer sizing in `PutFileAbsolute()`
- Dynamic buffer calculation based on file size and max buffers

### 3. Unified Concurrency Limits (`pkg/backup/restore.go` & `pkg/backup/create.go`)

**Before**: Separate concurrency limits caused bottlenecks
```go
// Old: Multiple conflicting limits
restoreBackupWorkingGroup.SetLimit(b.cfg.General.DownloadConcurrency)
downloadObjectDiskPartsWorkingGroup.SetLimit(b.cfg.General.ObjectDiskServerSideCopyConcurrency)
```

**After**: Unified, adaptive concurrency
```go
// New: Unified optimal concurrency
optimalConcurrency := b.cfg.GetOptimalDownloadConcurrency()
restoreBackupWorkingGroup.SetLimit(max(optimalConcurrency, 1))
objectDiskConcurrency := b.cfg.GetOptimalObjectDiskConcurrency()
downloadObjectDiskPartsWorkingGroup.SetLimit(objectDiskConcurrency)
```

### 4. Real-time Performance Monitoring (`pkg/backup/performance_monitor.go`)

**New Performance Monitor Features:**
- **Real-time speed tracking**: Monitors download speed every 5 seconds
- **Performance degradation detection**: Identifies 30%+ performance drops from peak
- **Adaptive concurrency adjustment**: Automatically reduces/increases concurrency based on performance
- **Comprehensive metrics**: Tracks current, average, and peak speeds
- **Event callbacks**: Notifies on performance changes and concurrency adjustments

**Integration Points:**
- Integrated into object disk download operations
- Tracks bytes downloaded and automatically adjusts concurrency
- Provides detailed performance logging with metrics

### 5. Smart Buffer Sizing Algorithm

```go
func CalculateOptimalBufferSize(fileSize int64, concurrency int) int64 {
    // Base buffer size
    baseBuffer := int64(64 * 1024) // 64KB
    
    // Scale with file size (logarithmic scaling)
    if fileSize > 0 {
        // More aggressive scaling for larger files
        fileSizeFactor := math.Log10(float64(fileSize)/1024/1024 + 1) // Log of MB + 1
        baseBuffer = int64(float64(baseBuffer) * (1 + fileSizeFactor))
    }
    
    // Reduce buffer size for higher concurrency to manage memory
    if concurrency > 1 {
        concurrencyFactor := math.Sqrt(float64(concurrency))
        baseBuffer = int64(float64(baseBuffer) / concurrencyFactor)
    }
    
    // Clamp to reasonable bounds
    minBuffer := int64(32 * 1024)  // 32KB minimum
    maxBuffer := int64(32 * 1024 * 1024) // 32MB maximum
    
    if baseBuffer < minBuffer {
        return minBuffer
    }
    if baseBuffer > maxBuffer {
        return maxBuffer
    }
    
    return baseBuffer
}
```

## Performance Benefits

### Expected Improvements

1. **Consistent Throughput**: Eliminates performance degradation towards completion
2. **Better Resource Utilization**: Adaptive concurrency matches system and network capabilities  
3. **Reduced Memory Pressure**: Smart buffer sizing prevents memory exhaustion
4. **Storage-Optimized Operations**: Each backend uses optimal concurrency patterns
5. **Self-Healing Performance**: Automatic adjustment when performance degrades

### Monitoring and Observability

**New Log Messages:**
```
# Performance degradation detection
WARN performance degradation detected current_speed_mbps=45.2 average_speed_mbps=67.8 peak_speed_mbps=89.1 concurrency=32

# Concurrency adjustments  
INFO concurrency adjusted for better performance concurrency=24 speed_mbps=52.3

# Final performance metrics
INFO object_disk data downloaded with performance metrics disk=s3_disk duration=2m34s size=15.2GB avg_speed_mbps=67.8 peak_speed_mbps=89.1 final_concurrency=28 degradation_detected=false
```

## Configuration

The improvements work automatically with existing configurations. For fine-tuning:

```yaml
general:
  # These settings now adapt automatically, but can still be overridden
  download_concurrency: 0  # 0 = auto-detect optimal
  upload_concurrency: 0    # 0 = auto-detect optimal
  
  # Object disk operations now unified with main concurrency
  object_disk_server_side_copy_concurrency: 0  # 0 = auto-detect optimal
```

## Backwards Compatibility

- All existing configurations continue to work
- New adaptive features activate when concurrency is set to 0 (auto-detect)
- Manual concurrency settings still override adaptive behavior
- No breaking changes to existing APIs or configurations

## Testing Recommendations

1. **Monitor logs** for performance degradation warnings
2. **Compare download times** before and after the improvements
3. **Watch memory usage** during large downloads
4. **Test with different storage backends** to verify optimizations
5. **Verify consistent performance** throughout entire download process

## Future Enhancements

1. **Upload performance monitoring**: Extend monitoring to upload operations
2. **Network condition detection**: Adapt to changing network conditions
3. **Historical performance learning**: Remember optimal settings per backup
4. **Cross-table optimization**: Coordinate concurrency across multiple table downloads
5. **Bandwidth throttling**: Respect network bandwidth limits