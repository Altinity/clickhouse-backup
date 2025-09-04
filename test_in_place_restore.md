# In-Place Restore Implementation Test

## Overview
This document outlines how to test the newly implemented in-place restore functionality.

## Implementation Summary

### 1. Configuration
- Added `RestoreInPlace bool` field to `GeneralConfig` struct in [`pkg/config/config.go`](pkg/config/config.go:56)
- Allows enabling in-place restore via configuration file

### 2. CLI Support  
- Added `--restore-in-place` flag to the restore command in [`cmd/clickhouse-backup/main.go`](cmd/clickhouse-backup/main.go:499)
- CLI flag overrides configuration file setting

### 3. Core Implementation
- **RestoreInPlace()**: Main function in [`pkg/backup/restore.go`](pkg/backup/restore.go:62) that orchestrates the in-place restore process
- **Part Comparison**: [`compareParts()`](pkg/backup/restore.go:258) compares backup parts vs current database parts
- **Part Removal**: [`removeUnwantedParts()`](pkg/backup/restore.go:304) removes parts that exist in DB but not in backup
- **Part Download/Attach**: [`downloadAndAttachMissingParts()`](pkg/backup/restore.go:320) downloads and attaches missing parts
- **Table Creation**: [`createTableFromBackup()`](pkg/backup/restore.go:288) creates tables that exist in backup but not in database
- **Parallelism**: Uses `errgroup` for concurrent processing across multiple tables

### 4. Integration
- Modified main [`Restore()`](pkg/backup/restore.go:375) function to detect in-place restore mode
- Automatically triggers when `RestoreInPlace=true`, `dataOnly=true`, and other conditions are met

## Testing Steps

### Basic Compilation Test
```bash
# Test that the code compiles
cd ~/workspace/clickhouse-backup
go build ./cmd/clickhouse-backup
```

### CLI Help Test
```bash
# Verify the new flag appears in help
./clickhouse-backup restore --help | grep "restore-in-place"
```

### Configuration Test
```bash
# Test config validation
./clickhouse-backup print-config | grep restore_in_place
```

### Integration Test Setup
1. **Create a test database and table**:
```sql
CREATE DATABASE test_db;
CREATE TABLE test_db.test_table (
    id UInt64,
    name String,
    date Date
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_db.test_table VALUES (1, 'test1', '2024-01-01'), (2, 'test2', '2024-01-02');
```

2. **Create a backup**:
```bash
./clickhouse-backup create test_backup
```

3. **Modify the table data**:
```sql
INSERT INTO test_db.test_table VALUES (3, 'test3', '2024-01-03');
DELETE FROM test_db.test_table WHERE id = 1;
```

4. **Test in-place restore**:
```bash
# Should only restore differential parts
./clickhouse-backup restore --data --restore-in-place test_backup
```

### Expected Behavior

#### When RestoreInPlace is enabled:
1. **Table exists in backup but not in DB**: Creates the table schema
2. **Parts exist in backup but not in DB**: Downloads and attaches these parts
3. **Parts exist in DB but not in backup**: Removes these parts using `ALTER TABLE ... DROP PART`
4. **Parts exist in both**: Keeps them unchanged (no action needed)
5. **Tables exist in DB but not in backup**: Leaves them unchanged

#### Performance Benefits:
- Only transfers differential parts instead of full backup
- Parallel processing across multiple tables
- Leverages existing part infrastructure for reliability

#### Safety Features:
- Only works with `--data` flag (data-only restore)
- Continues processing other parts even if one part operation fails
- Uses existing ClickHouse part management commands
- Maintains data consistency through atomic part operations

## Key Features Implemented

1. **Intelligent Part Comparison**: Uses part names as identifiers to determine differences
2. **Selective Operations**: Only processes parts that need changes
3. **Parallel Processing**: Concurrent table processing using errgroup with connection limits
4. **Error Resilience**: Continues with other parts if individual operations fail
5. **Integration**: Seamlessly integrates with existing restore infrastructure
6. **CLI Support**: Easy to use via command line flag
7. **Configuration Support**: Can be enabled permanently via config file

## Files Modified

1. [`pkg/config/config.go`](pkg/config/config.go) - Added RestoreInPlace configuration flag
2. [`pkg/backup/restore.go`](pkg/backup/restore.go) - Core in-place restore implementation
3. [`cmd/clickhouse-backup/main.go`](cmd/clickhouse-backup/main.go) - CLI flag support
4. [`pkg/backup/backuper.go`](pkg/backup/backuper.go) - SetRestoreInPlace method

## Usage Examples

### Via CLI flag:
```bash
clickhouse-backup restore --data --restore-in-place my_backup
```

### Via configuration:
```yaml
general:
  restore_in_place: true
```
```bash
clickhouse-backup restore --data my_backup
```

This implementation provides an efficient way to restore only the differential parts of a backup, significantly reducing transfer time and storage I/O for incremental restores.