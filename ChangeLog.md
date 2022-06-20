# v1.4.3
IMPROVEMENTS
- add `API_INTEGRATION_TABLES_HOST` option to allow use DNS name in integration tables system.backup_list, system.backup_actions

BUG FIXES
- fix `upload_by_part: false` max file size calculation, fix [454](https://github.com/AlexAkulov/clickhouse-backup/issues/454)

# v1.4.2
BUG FIXES
- fix `--partitions` parameter parsing, fix [425](https://github.com/AlexAkulov/clickhouse-backup/issues/425)

# v1.4.1
BUG FIXES
- fix upload data go routines waiting, expect the same upload speed as 1.3.2

# v1.4.0
IMPROVEMENTS
- add `S3_ALLOW_MULTIPART_DOWNLOAD` to config, to improve download speed, fix [431](https://github.com/AlexAkulov/clickhouse-backup/issues/431)
- add support backup/restore [user defined functions](https://clickhouse.com/docs/en/sql-reference/statements/create/function), fix [420](https://github.com/AlexAkulov/clickhouse-backup/issues/420)
- add `clickhouse_backup_number_backups_remote`, `clickhouse_backup_number_backups_local`, `clickhouse_backup_number_backups_remote_expected`,`clickhouse_backup_number_backups_local_expected` prometheus metric, fix [437](https://github.com/AlexAkulov/clickhouse-backup/issues/437)
- add ability to apply `system.macros` values to `path` field in all types of `remote_storage`, fix [438](https://github.com/AlexAkulov/clickhouse-backup/issues/438) 
- use all disks for upload and download for mutli-disk volumes in parallel when `upload_by_part: true` fix [#400](https://github.com/AlexAkulov/clickhouse-backup/issues/400) 

BUG FIXES
- fix wrong warning for .gz, .bz2, .br archive extensions during download, fix [441](https://github.com/AlexAkulov/clickhouse-backup/issues/441)

# v1.3.2
IMPROVEMENTS
- add TLS certificates and TLS CA support for clickhouse connections, fix [410](https://github.com/AlexAkulov/clickhouse-backup/issues/410)
- switch to go 1.18
- add clickhouse version 22.3 to integration tests
- add `S3_MAX_PARTS_COUNT` and `AZBLOB_MAX_PARTS_COUNT` for properly calculate buffer sizes during upload and download for custom S3 implementation like Swift
- add multithreading GZIP implementation

BUG FIXES
- fix [406](https://github.com/AlexAkulov/clickhouse-backup/issues/406), properly handle `path` for S3, GCS for case when it begins from "/"
- fix [409](https://github.com/AlexAkulov/clickhouse-backup/issues/409), avoid delete partially uploaded backups via `backups_keep_remote` option
- fix [422](https://github.com/AlexAkulov/clickhouse-backup/issues/422), avoid cache broken (partially uploaded) remote backup metadata.
- fix [404](https://github.com/AlexAkulov/clickhouse-backup/issues/404), properly calculate S3_PART_SIZE to avoid freeze after 10000 multi parts uploading, properly handle error when upload and download go-routine failed to avoid pipe stuck 

# v1.3.1

IMPROVEMENTS
- fix [387](https://github.com/AlexAkulov/clickhouse-backup/issues/387#issuecomment-1034648447), improve documentation related to memory and CPU usage

BUG FIXES
- fix [392](https://github.com/AlexAkulov/clickhouse-backup/issues/392), correct download for recursive sequence of diff backups when `DOWNLOAD_BY_PART` true
- fix [390](https://github.com/AlexAkulov/clickhouse-backup/issues/390), respect skip_tables patterns during restore and skip all INFORMATION_SCHEMA related tables even skip_tables don't contain INFORMATION_SCHEMA pattern
- fix [388](https://github.com/AlexAkulov/clickhouse-backup/issues/388), improve restore ON CLUSTER for VIEW with TO clause
- fix [385](https://github.com/AlexAkulov/clickhouse-backup/issues/385), properly handle multiple incremental backup sequences + `BACKUPS_TO_KEEP_REMOTE`

# v1.3.0

IMPROVEMENTS
- Add `API_ALLOW_PARALLEL` to support multiple parallel execution calls for, WARNING, control command names don't try to execute multiple same commands and be careful, it could allocate much memory
  during upload / download, fix [#332](https://github.com/AlexAkulov/clickhouse-backup/issues/332)
- Add support for `--partitions` on create, upload, download, restore CLI commands and API endpoint fix [#378](https://github.com/AlexAkulov/clickhouse-backup/issues/378) properly implementation
  of [#356](https://github.com/AlexAkulov/clickhouse-backup/pull/356)
- Add implementation `--diff-from-remote` for `upload` command and properly handle `required` on download command, fix [#289](https://github.com/AlexAkulov/clickhouse-backup/issues/289)
- Add `print-config` cli command fix [#366](https://github.com/AlexAkulov/clickhouse-backup/issues/366)
- Add `UPLOAD_BY_PART` (default: true) option for improve upload/download concurrency fix [#324](https://github.com/AlexAkulov/clickhouse-backup/issues/324)
- Add support ARM platform for Docker images and pre-compiled binary files, fix [#312](https://github.com/AlexAkulov/clickhouse-backup/issues/312)
- KeepRemoteBackups should respect differential backups, fix [#111](https://github.com/AlexAkulov/clickhouse-backup/issues/111)
- Add `SFTP_DEBUG` option, fix [#335](https://github.com/AlexAkulov/clickhouse-backup/issues/335)
- Add ability to restore schema `ON CLUSTER`, fix [#145](https://github.com/AlexAkulov/clickhouse-backup/issues/145)
- Add support encrypted disk (include s3 encrypted disks), fix [#260](https://github.com/AlexAkulov/clickhouse-backup/issues/260)
- API Server optimization for speed of `last_backup_size_remote` metric calculation to make it async during REST API startup and after download/upload,
  fix [#309](https://github.com/AlexAkulov/clickhouse-backup/issues/309)
- Improve `list remote` speed via local metadata cache in `$TEMP/.clickhouse-backup.$REMOTE_STORAGE`, fix [#318](https://github.com/AlexAkulov/clickhouse-backup/issues/318)
- Add `CLICKHOUSE_IGNORE_NOT_EXISTS_ERROR_DURING_FREEZE` option, fix [#319](https://github.com/AlexAkulov/clickhouse-backup/issues/319)
- Add support for PROJECTION, fix [#320](https://github.com/AlexAkulov/clickhouse-backup/issues/320)
- Return `clean` cli command and API `POST /backup/clean` endpoint, fix [#379](https://github.com/AlexAkulov/clickhouse-backup/issues/379)

BUG FIXES
- fix [#300](https://github.com/AlexAkulov/clickhouse-backup/issues/300), allow GCP properly work with empty `GCP_PATH`
  value
- fix [#340](https://github.com/AlexAkulov/clickhouse-backup/issues/340), properly handle errors on S3 during Walk() and
  delete old backup
- fix [#331](https://github.com/AlexAkulov/clickhouse-backup/issues/331), properly restore tables where have table name
  with the same name as database name
- fix [#311](https://github.com/AlexAkulov/clickhouse-backup/issues/311), properly run clickhouse-backup inside docker
  container via entrypoint
- fix [#317](https://github.com/AlexAkulov/clickhouse-backup/issues/317), properly upload large files to Azure Blob
  Storage
- fix [#220](https://github.com/AlexAkulov/clickhouse-backup/issues/220), properly handle total_bytes for uint64 type
- fix [#304](https://github.com/AlexAkulov/clickhouse-backup/issues/304), properly handle archive extension during download instead of use config settings
- fix [#375](https://github.com/AlexAkulov/clickhouse-backup/issues/375), properly `REMOTE_STORAGE=none` error handle
- fix [#379](https://github.com/AlexAkulov/clickhouse-backup/issues/379), will try to clean `shadow` if `create` fail during `moveShadow`
- more precise calculation backup size during `upload`, for backups created with `--partitions`, fix bug after [#356](https://github.com/AlexAkulov/clickhouse-backup/pull/356)
- fix `restore --rm` behavior for 20.12+ for tables which have dependent objects (like dictionary)
- fix concurrency by `FTP` creation directories during upload, reduce connection pool usage
- properly handle `--schema` parameter for show local backup size after `download`
- fix restore bug for WINDOW VIEW, thanks @zvonand

EXPERIMENTAL
- Try to add experimental support for backup `MaterializedMySQL` and `MaterializedPostgeSQL` tables, restore MySQL tables not impossible now without replace `table_name.json` to `Engine=MergeTree`,
  PostgresSQL not supported now, see https://github.com/ClickHouse/ClickHouse/issues/32902

# v1.2.4

HOT FIXES
- fix [409](https://github.com/AlexAkulov/clickhouse-backup/issues/409), avoid delete partially uploaded backups via `backups_keep_remote` option

# v1.2.3

HOT FIXES
- fix [390](https://github.com/AlexAkulov/clickhouse-backup/issues/390), respect skip_tables patterns during restore and skip all INFORMATION_SCHEMA related tables even skip_tables don't contain INFORMATION_SCHEMA pattern

# v1.2.2

IMPROVEMENTS
- Add REST API `POST /backup/tables/all`, fix `POST /backup/tables` to respect `CLICKHOUSE_SKIP_TABLES`

BUG FIXES
- fix [#297](https://github.com/AlexAkulov/clickhouse-backup/issues/297), properly restore tables where have fields with the same name as table name
- fix [#298](https://github.com/AlexAkulov/clickhouse-backup/issues/298), properly create `system.backup_actions` and `system.backup_list` integration tables for ClickHouse before 21.1
- fix [#303](https://github.com/AlexAkulov/clickhouse-backup/issues/303), ignore leading and trailing spaces in `skip_tables` and `--tables` parameters
- fix [#292](https://github.com/AlexAkulov/clickhouse-backup/issues/292), lock clickhouse connection pool to single connection

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
- Added options for RBAC and CONFIG backup, look to `clickhouse-backup help create` and `clickhouse-backup help restore` for details
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
- fix [#223](https://github.com/AlexAkulov/clickhouse-backup/issues/223) backup only database metadata for proxy integrated database engines like MySQL, PostgresSQL
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
- Incremental backup on remote storage feature is not supported now, but will support in future versions

# v0.6.4

IMPROVEMENTS
- Added `CLICKHOUSE_AUTO_CLEAN_SHADOW` option for cleaning shadow folder before backup. Enabled by default.
- Added `CLICKHOUSE_SYNC_REPLICATED_TABLES` option for sync replicated tables before backup. Enabled by default.
- Improved statuses of operations in server mode

BUG FIXES
- Fixed bug with semaphores in server mode

