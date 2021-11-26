# v1.2.2

IMPROVEMENTS
- Add REST API `POST /backup/tables/all`, fix `POST /backup/tables` to respect `CLICKHOUSE_SKIP_TABLES`

BUG FIXES
- fix [#297](https://github.com/AlexAkulov/clickhouse-backup/issues/297), properly restore tables where have fields with the same name as table name
- fix [#298](https://github.com/AlexAkulov/clickhouse-backup/issues/298), properly create `system.backup_actions` and `system.backup_list` integration tables for ClickHouse before 21.1
- fix [#303](https://github.com/AlexAkulov/clickhouse-backup/issues/303), ignore leading and trailing spaces in `skip_tables` and `--tables` parameters

# v1.2.1

IMPROVEMENTS
- Add REST API integration tests

BUG FIXES
- fix [#290](https://github.com/AlexAkulov/clickhouse-backup/issues/290)
- fix [#291](https://github.com/AlexAkulov/clickhouse-backup/issues/291)
- fix `CLICKHOUSE_DEBUG` settings behavior (now we can see debug log from clickhouse-go)

# v1.2.0

INCOMPATIBLE CHANGES
- REST API `/backup/status` now return only latest executed command with status and error message

IMPROVEMENTS
- Added REST API `/backup/list/local` and `/backup/list/remote` to allow list backup types separately
- Decreased background backup creation time via REST API `/backup/create`, during avoid list remote backups for update metrics value 
- Decreased backup creation time, during avoid scan whole `system.tables` when set `table` query string parameter or `--tables` cli parameter     
- Added `last` and `filter` query string parameters to REST API `/backup/actions`, to avoid pass to client long JSON documents
- Improved `FTP` remote storage parallel upload / download
- Added `FTP_CONCURRENCY` to allow, by default MAX_CPU / 2 
- Added `FTP_DEBUG` setting, to allow debug FTP commands
- Added `FTP` to CI/CD on any commit
- Added race condition check to CI/CD

BUG FIXES
- environment variable `LOG_LEVEL` now apply to `clickhouse-backup server` properly
- fix [#280](https://github.com/AlexAkulov/clickhouse-backup/issues/280), incorrect prometheus metrics measurement for `/backup/create`, `/backup/upload`, `/backup/download`
- fix [#273](https://github.com/AlexAkulov/clickhouse-backup/issues/273), return `S3_PART_SIZE` back, but calculates it smartly
- fix [#252](https://github.com/AlexAkulov/clickhouse-backup/issues/252), now you can pass `last` and `filter` query string parameters
- fix [#246](https://github.com/AlexAkulov/clickhouse-backup/issues/246), incorrect error messages when use `REMOTE_STORAGE=none`
- fix [#283](https://github.com/AlexAkulov/clickhouse-backup/issues/283), properly handle error message from `FTP` server
- fix [#268](https://github.com/AlexAkulov/clickhouse-backup/issues/268), properly restore legacy backup for schema without database name

# v1.1.1

BUG FIXES
- fix broken `system.backup_list` integration table after add `required field` in https://github.com/AlexAkulov/clickhouse-backup/pull/263
- fix [#274](https://github.com/AlexAkulov/clickhouse-backup/issues/274) invalid `SFTP_PASSWORD` environment usage 

# v1.1.0

IMPROVEMENTS
- Added concurrency settings for upload and download, which allow loading table data in parallel for each table and each disk for multi-disk storages
- Up golang version to 1.17
- Updated go libraries dependencies to actual version (exclude azure)
- Add Clickhouse 21.8 to test matrix
- Now `S3_PART_SIZE` not restrict upload size, partSize calculate depends on `MAX_FILE_SIZE`
- improve logging for delete operation
- Added `S3_DEBUG` option to allow debug S3 connection
- Decrease number of SQL queries to system.* during backup commands
- Added options for RBAC and CONFIGs backup, look to `clickhouse-backup help create` and `clickhouse-backup help restore` for details
- Add `S3_CONCURRENCY` option to speedup backup upload to `S3`
- Add `SFTP_CONCURRENCY` option to speedup backup upload to `SFTP`
- Add `AZBLOB_USE_MANAGED_IDENTITY` support for ManagedIdentity for azure remote storage, thanks https://github.com/roman-vynar
- Add clickhouse-operator kubernetes manifest which run `clickhouse-backup` in `server` mode on each clickhouse pod in kubernetes cluster
- Add detailed description and restrictions for incremental backups.
- Add `GCS_DEBUG` option
- Add `CLICKHOUSE_DEBUG` option to allow low-level debug for `clickhouse-go`

BUG FIXES
- fix [#266](https://github.com/AlexAkulov/clickhouse-backup/discussions/266) properly restore legacy backup format
- fix [#244](https://github.com/AlexAkulov/clickhouse-backup/issues/244) add `read_timeout`, `write_timeout` to client-side timeout for `clickhouse-go` 
- fix [#255](https://github.com/AlexAkulov/clickhouse-backup/issues/255) restrict connection pooling to 1 in `clickhouse-go` 
- fix [#256](https://github.com/AlexAkulov/clickhouse-backup/issues/256) remote_storage: none, was broke compression
- fix [#266](https://github.com/AlexAkulov/clickhouse-backup/discussions/266) legacy backups from version prior 1.0 can't restore without `allow_empty_backup: true`
- fix [#223](https://github.com/AlexAkulov/clickhouse-backup/issues/223) backup only database metadata for proxy integrated database engines like MySQL, PostgreSQL
- fix `GCS` global buffer wrong usage during UPLOAD_CONCURRENCY > 1
- Remove unused `SKIP_SYNC_REPLICA_TIMEOUTS` option

# v1.0.0

BUG FIXES
- Fixed silent cancel uploading when table has more than 4k files  (fix [#203](https://github.com/AlexAkulov/clickhouse-backup/issues/203), [#163](https://github.com/AlexAkulov/clickhouse-backup/issues/163). Thanks [mastertheknife](https://github.com/mastertheknife))
- Fixed download error for `zstd` and `brotli` compression formats
- Fixed bug when old-format backups hadn't cleared

# v1.0.0-beta2

IMPROVEMENTS

- Added diff backups
- Added retries to restore operation for resolve complex tables dependencies (Thanks [@Slach](https://github.com/Slach))
- Added SFTP remote storage (Thanks [@combin](https://github.com/combin))
- Now databases will be restored with the same engines (Thanks [@Slach](https://github.com/Slach))
- Added `create_remote` and `restore_remote` commands
- Changed of compression format list. Added `zstd`, `brotli` and disabled `bzip2`, `sz`, `xz`

BUG FIXES
- Fixed empty backup list when S3_PATH and AZBLOB_PATH is root
- Fixed azblob container issue (Thanks [@atykhyy](https://github.com/atykhyy))


# v1.0.0-beta1

IMPROVEMENTS

- Added 'allow_empty_backups' and 'api.create_integration_tables' options
- Wait for clickhouse in server mode (fix [#169](https://github.com/AlexAkulov/clickhouse-backup/issues/169))
- Added Disk Mapping feature (fix [#162](https://github.com/AlexAkulov/clickhouse-backup/issues/162))

BUG FIXES

- Fixed 'ftp' remote storage ([#164](https://github.com/AlexAkulov/clickhouse-backup/issues/164))

# v0.6.5

**It is the last release of v0.x.x**

IMPROVEMENTS
- Added 'create_remote' and 'restore_remote' commands
- Changed update config behavior in API mode

BUG FIXES
- fix [#154](https://github.com/AlexAkulov/clickhouse-backup/issues/154)
- fix [#165](https://github.com/AlexAkulov/clickhouse-backup/issues/165)

# v1.0.0-alpha1

IMPROVEMENTS
- Support for new versions of ClickHouse ([#155](https://github.com/AlexAkulov/clickhouse-backup/issues/155))
- Support of Atomic Database Engine ([#140](https://github.com/AlexAkulov/clickhouse-backup/issues/140), [#141](https://github.com/AlexAkulov/clickhouse-backup/issues/141), [#126](https://github.com/AlexAkulov/clickhouse-backup/issues/126))
- Support of multi disk ClickHouse configurations ([#51](https://github.com/AlexAkulov/clickhouse-backup/issues/51))
- Ability to upload and download specific tables from backup
- Added partitions backup on remote storage ([#83](https://github.com/AlexAkulov/clickhouse-backup/issues/83))
- Added support for backup/upload/download schema only ([#138](https://github.com/AlexAkulov/clickhouse-backup/issues/138))
- Added new backup format select it by `compression_format: none` option

BROKEN CHANGES
- Changed backup format
- Incremental backup on remote storage and 'flashback' feature is not supported now, but will supported in future versions

# v0.6.4

IMPROVEMENTS
- Added `CLICKHOUSE_AUTO_CLEAN_SHADOW` option for cleaning shadow folder before backup. Enabled by default.
- Added `CLICKHOUSE_SYNC_REPLICATED_TABLES` option for sync replicated tables before backup. Enabled by default.
- Improved statuses of operations in server mode

BUG FIXES
- Fixed bug with semaphores in server mode

