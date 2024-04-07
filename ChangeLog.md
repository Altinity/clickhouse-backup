# v2.5.0 (not released yet)
IMPROVEMENTS
- complete removed support for legacy backups, created with version prior v1.0 
- removed `disable_progress_bar` config option and related progress bar code
- added `--delete-source` parameter for `upload` and `create_remote` commands to explicitly delete local backup during upload, fix [777](https://github.com/Altinity/clickhouse-backup/issues/777)
- added support for `--env ENV_NAME=value` cli parameter for allow dynamically override any config parameter, fix [821](https://github.com/Altinity/clickhouse-backup/issues/821)
- added support for `use_embedded_backup_restore: true` with empty `embedded_backup_disk` value, tested on S3/GCS over S3/AzureBlobStorage, fix [695](https://github.com/Altinity/clickhouse-backup/issues/695)
- `--rbac, --rbac-only, --configs, --configs-only` now works with `use_embedded_backup_restore: true`
-- `--data` for `restore` with `use_embedded_backup_restore: true` will use `allow_non_empty_tables=true` to allow fix [756](https://github.com/Altinity/clickhouse-backup/issues/756)
- added `--diff-from-remote` parameter for `create` command, will copy only new data parts object disk data, also allows to download properly object disk data from required backup during `restore`, fix [865](https://github.com/Altinity/clickhouse-backup/issues/865)
- added support of native Clickhouse incremental backup for `use_embedded_backup_restore: true` fix [735](https://github.com/Altinity/clickhouse-backup/issues/735)
- added `GCS_CHUNK_SIZE` config parameter, try to speedup GCS upload fix [874](https://github.com/Altinity/clickhouse-backup/pull/874), thanks @dermasmid
- added `--remote-backup` cli parameter to `tables` command and `GET /backup/table`, fix [778](https://github.com/Altinity/clickhouse-backup/issues/778)
- added `rbac_always_backup: true` option to default config, will create backup for RBAC objects automatically, restore still require `--rbac` to avoid destructive actions, fix [793](https://github.com/Altinity/clickhouse-backup/issues/793)
- added `rbac_conflict_resolution: recreate` option for RBAC object name conflicts during restore, fix [851](https://github.com/Altinity/clickhouse-backup/issues/851)
- added `upload_max_bytes_per_seconds` and `download_max_bytes_per_seconds` config options to allow throttling without CAP_SYS_NICE, fix [817](https://github.com/Altinity/clickhouse-backup/issues/817)
- switched to golang 1.22
- updated all third-party SDK to latest versions
- added `clickhouse/clickhouse-server:24.3` to CI/CD

BUG FIXES
- continue `S3_MAX_PARTS_COUNT` default value from `2000` to `4000` to continue decrease memory usage for S3 
- changed minimal part size for multipart upload in CopyObject from `5Mb` to `10Mb`
- restore SQL UDF functions after restore tables
- execute `ALTER TABLE ... DROP PARTITION` instead of `DROP TABLE` for `restore` and `restore_remote` with parameters `--data --partitions=...`, fix [756](https://github.com/Altinity/clickhouse-backup/issues/756) 
- fix wrong behavior for `freeze_by_part` + `freeze_by_part_where`, fix [855](https://github.com/Altinity/clickhouse-backup/issues/855)
- apply `CLICKHOUSE_SKIP_TABLES_ENGINES` during `create` command
- fixed behavior for upload / download when .inner. table missing for MATERIALIZED VIEW  by table pattern, fix [765](https://github.com/Altinity/clickhouse-backup/issues/765)
- fixed `ObjectDisks` + `CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE: true` - shall skip upload object disk content, fix [799](https://github.com/Altinity/clickhouse-backup/issues/799)
- fixed connection to clickhouse-server behavior when long clickhouse-server startup time and `docker-entrypoint.d` processing, will infinite reconnect each 5 seconds, until success, fix [857](https://github.com/Altinity/clickhouse-backup/issues/857)
- fixed `USE_EMBEDDED_BACKUP_RESTORE=true` behavior to allow use backup disk with type `local`, fix [882](https://github.com/Altinity/clickhouse-backup/issues/882)
- fixed wrong list command behavior, it shall  scann all system.disks path not only default disk to find pratially created backups, fix [873](https://github.com/Altinity/clickhouse-backup/issues/873)
- fixed create `--rbac` behavior, don't create access folder if no RBAC objects is present
- fixed behavior when `system.disks` contains disk which not present in any `storage_policies`, fix [845](https://github.com/Altinity/clickhouse-backup/issues/845)
# v2.4.35
IMPROVEMENTS
- set part size for `s3:CopyObject` minimum 128Mb, look details https://repost.aws/questions/QUtW2_XaALTK63wv9XLSywiQ/s3-sync-command-is-slow-to-start-on-some-data

# v2.4.34
BUG FIXES
- fixed wrong behavior for CLICKHOUSE_SKIP_TABLES_ENGINES for engine=EngineName without parameters

# v2.4.33
BUG FIXES
- fixed wrong anonymous authorization for service account for GCS, added `GCS_SKIP_CREDENTIALS` fix [848](https://github.com/Altinity/clickhouse-backup/issues/848), fix [847](https://github.com/Altinity/clickhouse-backup/pull/847), thanks @sanadhis

# v2.4.32
IMPROVEMENTS
- added ability to make custom endpoint for `GCS`, fix [837](https://github.com/Altinity/clickhouse-backup/pull/837), thanks @sanadhis 

BUG FIXES
- fixed wrong config validation for `object_disk_path` even when no object disk present in backup during `restore`, fix [842](https://github.com/Altinity/clickhouse-backup/issues/842)

# v2.4.31
IMPROVEMENTS
- added `check_sum_algorithm` parameter for `s3` config section with "" default value, to avoid useless CPU usage during upload to `S3` storage, additional fix [829](https://github.com/Altinity/clickhouse-backup/issues/829) 
- `upload` will delete local backup if upload successful, fix [834](https://github.com/Altinity/clickhouse-backup/issues/834) 

BUG FIXES
- fixed miss checksum for CopyObject in `s3`, fix [835](https://github.com/Altinity/clickhouse-backup/issues/835), affected 2.4.30
- fixed wrong behavior for `restore --rbac-only` and `restore --rbac` for backups which not contains any schema, fix [832](https://github.com/Altinity/clickhouse-backup/issues/832)

# v2.4.30
BUG FIXES
- fixed `download` command corner cases for increment backup for tables with projections, fix [830](https://github.com/Altinity/clickhouse-backup/issues/830)
- added more informative error during try to `restore` not exists local backup
- fixex `upload` command for S3 when object lock policy turned on, fix [829](https://github.com/Altinity/clickhouse-backup/issues/829)

# v2.4.29
IMPROVEMENTS
- added `AZBLOB_DEBUG` environment and `debug` config parameter in `azblob` section

BUG FIXES
- force set `RefCount` to 0 during `restore` for parts in S3/GCS over S3/Azure disks, for properly works DROP TABLE / DROP DATABASE
- use `os.Link` instead `os.Rename` for ClickHouse 21.4+, to properly create backup object disks
- ignore `frozen_metadata` during, create, upload, download and restore commands, fix [826](https://github.com/Altinity/clickhouse-backup/issues/826)
- `allow_parallel: true` doesn't work after execute list command, fix [827](https://github.com/Altinity/clickhouse-backup/issues/827)
- fixed corner cases, when disk have encrypted type and underlying disk is object storage 

# v2.4.28
IMPROVEMENT
- refactoring `watch` command, after https://github.com/Altinity/clickhouse-backup/pull/804
BUG FIXES
- fixed deletion for `object_disk_path` and `embedded` backups, after `upload` to properly respect `backups_to_keep_remote`

# v2.4.27
BUG FIXES
- fixed deletion for `object_disk_path` (all backups with S3, GCS over S3, AZBLOB disks from 2.4.0-2.4.25 didn't delete properly their data from backup bucket)

# v2.4.26
IMPROVEMENTS
- improved re-balance disk during download if disk not exists in system.disks. Use least used for `local` disks and `random` for object disks, fix [561](https://github.com/Altinity/clickhouse-backup/issues/561)
BUG FIXES
- fixed regression `check_parts_columns` for Enum types (2.4.24+), fix [823](https://github.com/Altinity/clickhouse-backup/issues/823)
- properly applying marcos to `object_disk_path` during `delete`

# v2.4.25
BUG FIXES
- fixed `--restore-table-mapping` corner cases for when destination database contains special characters , fix [820](https://github.com/Altinity/clickhouse-backup/issues/820)

# v2.4.24
BUG FIXES
- fixed `check_parts_columns` corner cases for `AggregateFunction` and `SimpleAggregateFunction` versioning, fix [819](https://github.com/Altinity/clickhouse-backup/issues/819)

# v2.4.23
IMPROVEMENTS
- refactored of `restore` command to allow parallel execution of `ALTER TABLE ... ATTACH PART` and improve parallelization of CopyObject during restore.

# v2.4.22
BUG FIXES
- changed `S3_MAX_PARTS_COUNT` default value from `256` to `2000` to fix memory usage for s3 which increased for 2.4.16+

# v2.4.21
BUG FIXES
- refactored execution UpdateBackupMetrics, to avoid context canceled error, fix [814](https://github.com/Altinity/clickhouse-backup/issues/814)

# v2.4.20
IMPROVEMENTS
- refactored of `create` command to allow parallel execution of `FREEZE` and `UNFREEZE` and table level parallelization `object_disk.CopyObject`
- added `CLICKHOUSE_MAX_CONNECTIONS` config parameter to allow parallel execution `FREEZE` / `UNFREEZE`
- change go.mod to allow `GO111MODULE=on go install github.com/Altinity/clickhouse-backup/v2/cmd/clickhouse-backup@latest`

# v2.4.19
BUG FIXES
- use single `s3:CopyObject` call instead `s3:CreateMultipartUpload+s3:UploadCopyPart+s3:CompleteMultipartUpload` for files with size less 5Gb

# v2.4.18
BUG FIXES
- removed `HeadObject` request to calculate source key size in `CopyObject`, to allow cross region S3 disks backup, fix [813](https://github.com/Altinity/clickhouse-backup/issues/813)
- make optional `/backup/kill` query parameter `command` and optional arguments for `kill` command handled via `/backup/actions`, if omitted then will kill first command in "In progress" status, fix [808](https://github.com/Altinity/clickhouse-backup/issues/808) 

# v2.4.17
BUG FIXES
- skip `CopyObject` execution for keys which have zero size, to allow properly backup `S3`, `GCS over S3` and `Azure` disks

# v2.4.16
BUG FIXES
- increased `AZBLOB_TIMEOUT` to 4h, instead 15m to allow download long size data parts 
- changed `S3_MAX_PARTS_COUNT` default from `5000` to `256` and minimal `S3_PART_SIZE` from 5Mb to 25Mb from by default to improve speedup S3 uploading / downloading 
 
# v2.4.15
BUG FIXES
- fixed `create` and `restore` command for ReplicatedMergeTree tables with `frozen_metadata.txt` parsing

# v2.4.14
IMPROVEMENTS
- refactored `semaphore.NewWeighted()` to `errgroup.SetLimit()`
- added parallelization to `create` and `restore` command during call `CopyObject` 

# v2.4.13
BUG FIXES
- fixed `object_disk.CopyObject` during restore to allow use properly S3 endpoint
- fixed AWS IRSA environments handler, fix [798](https://github.com/Altinity/clickhouse-backup/issues/798)

# v2.4.12
BUG FIXES
- fixed `object_disk.CopyObject` to use simple `CopyObject` call, instead of multipart for zero object size, for backup S3 disks

# v2.4.11
BUG FIXES
- fixed `CopyObject` multipart upload complete Parts must be ordered by part number, for backup S3 disks

# v2.4.10
IMPROVEMENTS
- updated go modules to latest versions
- added `S3_REQUEST_PAYER` config parameter, look https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html for details, fix [795](https://github.com/Altinity/clickhouse-backup/issues/795)

# v2.4.9
BUG FIXES
- fixed list remote command date parsing for all `remote_storage: custom` integration examples
- `clickhouse-backup` should not fail when `--rbac` used but rbac object is not present in backup, but it should log warnings/errors, partial fix [793](https://github.com/Altinity/clickhouse-backup/issues/793)

# v2.4.8
BUG FIXES
- fixed Object Disks path parsing from config, remove unnecessary "/"
- if `S3_ACL` is empty then will not use ACL in PutObject, fix [785](https://github.com/Altinity/clickhouse-backup/issues/785) 

# v2.4.7
BUG FIXES
- `--partitions=(v1,'v2')` could calculate wrong partition expression if system.columns will return fields in different order than they described in PARTITION BY clause, fix [791](https://github.com/Altinity/clickhouse-backup/issues/791)

# v2.4.6
IMPROVEMENTS
- make 'kopia' custom scripts really increment fix [781](https://github.com/Altinity/clickhouse-backup/issues/781) 
- added `force_http` and improve retries in GCS upload [784](https://github.com/Altinity/clickhouse-backup/pull/784), thanks @minguyen9988

BUG FIXES
- added `Array(Tuple())` to exclude list for `check_parts_columns:true' fix [789](https://github.com/Altinity/clickhouse-backup/issues/789)
- fixed `delete remote` command for s3 buckets with enabled versioning fix [782](https://github.com/Altinity/clickhouse-backup/issues/782)
- fixed panic during create integration tables when `API_LISTEN` doesn't contain ":" character, fix [790](https://github.com/Altinity/clickhouse-backup/issues/790)

# v2.4.5
BUG FIXES
- added aws.LogResponse for `S3_DEBUG`  (affected 2.4.4+ versions)

# v2.4.4
BUG FIXES
- removed `aws.LogResponseWithBody` for `S3_DEBUG` to avoid too many logs (affected 2.4.0+ versions)

# v2.4.3
IMPROVEMENTS
- added `list` command to API /backup/actions, fix [772](https://github.com/Altinity/clickhouse-backup/issues/772)

BUG FIXES
- fixed behavior for `restore_as_attach: true` for non-replicated MergeTree, fix [773](https://github.com/Altinity/clickhouse-backup/issues/773)
- tables with `ENGINE=Dictionary` shall create after all `dictionaries` to avoid retry, fix [771](https://github.com/Altinity/clickhouse-backup/issues/771)

# v2.4.2
IMPROVEMENTS
- added `cpu_nice_priority` and `io_nice_priority` to config, which allow us to throttle CPU and IO usage for the whole `clickhouse-backup` process, fix [757](https://github.com/Altinity/clickhouse-backup/issues/757)

BUG FIXES
- fixed restore for object disk frozen_metadata.txt, fix [752](https://github.com/Altinity/clickhouse-backup/issues/752)
- fixed more corner cases for `check_parts_columns: true`, fix [747](https://github.com/Altinity/clickhouse-backup/issues/747)
- fixed applying macros to s3 endpoint in object disk during restore embedded backups, fix [750](https://github.com/Altinity/clickhouse-backup/issues/750)
- rewrited `GCS` clients pool, set default `GCS_CLIENT_POOL_SIZE` as max(upload_concurrency, download_concurrency) * 3  to avoid stuck, fix [753](https://github.com/Altinity/clickhouse-backup/pull/753), thanks @minguyen-jumptrading

# v2.4.1
IMPROVEMENTS
- switched to go-1.21
- added clickhouse-server:23.8 for integration and testflows tests
- added `FTP_SKIP_TLS_VERIFY` config option fix [742](https://github.com/Altinity/clickhouse-backup/issues/742)

BUG FIXES
- fixed calculation part size for `S3` and buffer size for `Azure` to avoid errors for upload big files, fix [739](https://github.com/Altinity/clickhouse-backup/issues/739) thanks @rodrigargar
- fixed GetFileReader for SSE encryption in `S3`, again fix [709](https://github.com/Altinity/clickhouse-backup/issues/709)

# v2.4.0
IMPROVEMENTS
- first implementation for properly backup S3/GCS/Azure disks, support server-side copy to back up bucket during `clickhouse-backup` create and during `clickhouse-backup restore`, requires add `object_disk_path` to `s3`,`gcs`,`azblob` section, fix [447](https://github.com/Altinity/clickhouse-backup/issues/447)
- implementation blacklist for table engines during backup / download / upload / restore [537](https://github.com/Altinity/clickhouse-backup/issues/537)
- restore RBAC / configs, refactoring restart clickhouse-server via `sql:SYSTEM SHUTDOWN` or `exec:systemctl restart clickhouse-server`, add `--rbac-only` and `--configs-only` options to `create`, `upload`, `download`, `restore` command. fix [706]https://github.com/Altinity/clickhouse-backup/issues/706
- Backup/Restore RBAC related objects from Zookeeper via direct connection to zookeeper/keeper, fix [604](https://github.com/Altinity/clickhouse-backup/issues/604)
- added `SHARDED_OPERATION_MODE` option, to easy create backup for sharded cluster, available values `none` (no sharding), `table` (table granularity), `database` (database granularity), `first-replica` (on the lexicographically sorted first active replica), thanks @mskwon, fix [639](https://github.com/Altinity/clickhouse-backup/issues/639), fix [648](https://github.com/Altinity/clickhouse-backup/pull/648)
- added support for `compression_format: none` for upload and download backups created with `--rbac` / `--rbac-only` or `--configs` / `--configs-only` options, fix [713](https://github.com/Altinity/clickhouse-backup/issues/713)
- added support for s3 `GLACIER` storage class, when GET return error, then, it requires 5 minutes per key and restore could be slow. Use `GLACIER_IR`, it looks more robust, fix [614](https://github.com/Altinity/clickhouse-backup/issues/614)
- restore functions via `CREATE OR REPLACE` for more atomic behavior
- prepared to make `./tests/integration/` test parallel execution fix [721](https://github.com/Altinity/clickhouse-backup/issues/721)
- touch `/var/lib/clickhouse/flags/force_drop_table` before every DROP TABLE execution, fix [683](https://github.com/Altinity/clickhouse-backup/issues/683)
- added support connection pool for google cloud storage, `GCS_CLIENT_POOL_SIZE`, fix [724](https://github.com/Altinity/clickhouse-backup/issues/724)

BUG FIXES
- fixed possible create backup failures during UNFREEZE not exists tables, affected 2.2.7+ version, fix [704](https://github.com/Altinity/clickhouse-backup/issues/704)
- fixed too strict `system.parts_columns` check when backup create, exclude Enum and Tuple (JSON) and Nullable(Type) vs Type corner cases, fix [685](https://github.com/Altinity/clickhouse-backup/issues/685), fix [699](https://github.com/Altinity/clickhouse-backup/issues/699)  
- fixed `--rbac` behavior when /var/lib/clickhouse/access not exists
- fixed `skip_database_engines` behavior
- fixed `skip_databases` behavior during restore for corner case `db.prefix*` and corner cases when conflict with `--tables="*pattern.*"`, fix [663](https://github.com/Altinity/clickhouse-backup/issues/663) 
- fixed S3 head object Server Side Encryption parameters, fix [709](https://github.com/Altinity/clickhouse-backup/issues/709) 

# v2.3.2
BUG FIXES
- fix error when `backups_to_keep_local: -1`, fix [698](https://github.com/Altinity/clickhouse-backup/issues/698)
- minimal value for `download_concurrency` and `upload_concurrency` 1, fix [688](https://github.com/Altinity/clickhouse-backup/issues/688)
- do not create UDF when use --data flag, fix [697](https://github.com/Altinity/clickhouse-backup/issues/697)

# v2.3.1
IMPROVEMENTS
- add support `use_environment_credentials` option inside `clickhouse-server` backup object disk definition, fix [691](https://github.com/Altinity/clickhouse-backup/issues/691)
- add but skip tests for `azure_blob_storage` backup disk for `use_embbeded_backup_restore: true`, it works, but slow, look https://github.com/ClickHouse/ClickHouse/issues/52088 for details

BUG FIXES
- fix static build for FIPS compatible mode fix [693](https://github.com/Altinity/clickhouse-backup/issues/693)
- complete success/failure server callback notification even when main context canceled, fix [680](https://github.com/Altinity/clickhouse-backup/pull/680)
- `clean` command will not return error when shadow directory not exists, fix [686](https://github.com/Altinity/clickhouse-backup/issues/686)

# v2.3.0
IMPROVEMENTS
- add FIPS compatible builds and examples, fix [656](https://github.com/Altinity/clickhouse-backup/issues/656), fix [674](https://github.com/Altinity/clickhouse-backup/issues/674)
- improve support for `use_embedded_backup_restore: true`, applied ugly workaround in test to avoid https://github.com/ClickHouse/ClickHouse/issues/43971, and applied restore workaround to resolve https://github.com/ClickHouse/ClickHouse/issues/42709
- migrate to `clickhouse-go/v2`, fix [540](https://github.com/Altinity/clickhouse-backup/issues/540), close [562](https://github.com/Altinity/clickhouse-backup/pull/562)
- add documentation for `AWS_ARN_ROLE` and `AWS_WEB_IDENTITY_TOKEN_FILE`, fix [563](https://github.com/Altinity/clickhouse-backup/issues/563)

BUG FIXES
- hotfix wrong empty files when disk_mapping contains not exists during create, affected 2.2.7 version, look details [676](https://github.com/Altinity/clickhouse-backup/issues/676#issue-1771732489) 
- add `FTP_ADDRESS` and `SFTP_PORT` in Default config Readme.md section fix [668](https://github.com/Altinity/clickhouse-backup/issues/668)
- when use `--tables=db.materialized_view` pattern, then create/restore backup also for `.inner.materialized_view` or `.inner_id.uuid`, fix [613](https://github.com/Altinity/clickhouse-backup/issues/613)

# v2.2.8
BUG FIXES
- hotfix wrong empty files when disk_mapping contains not exists during create, affected 2.2.7 version, look details [676](https://github.com/Altinity/clickhouse-backup/issues/676#issue-1771732489)

# v2.2.7
IMPROVEMENTS
- Auto-tuning concurrency and buffer size related parameters depending on remote storage type, fix [658](https://github.com/Altinity/clickhouse-backup/issues/658)
- add `CLICKHOUSE_BACKUP_MUTATIONS` and `CLICKHOUSE_RESTORE_AS_ATTACH` config options to allow backup and properly restore table with system.mutations is_done=0 status. fix [529](https://github.com/Altinity/clickhouse-backup/issues/529)
- add `CLICKHOUSE_CHECK_PARTS_COLUMNS` config option and `--skip-check-parts-column` CLI parameter to `watch`, `create` and `create_remote` commands to disallow backup with inconsistent column data types fix [529](https://github.com/Altinity/clickhouse-backup/issues/529)
- add test coverage reports for unit, testflows and integration tests, fix [644](https://github.com/Altinity/clickhouse-backup/issues/644)
- use UNFREEZE TABLE in ClickHouse after backup finished to allow s3 and other object storage disks unlock and delete remote keys during merge, fix [423](https://github.com/Altinity/clickhouse-backup/issues/423)

BUG FIXES
- apply `SETTINGS check_table_dependencies=0` to `DROP DATABASE` statement, when pass `--ignore-dependencies` together with `--rm` in `restore` command, fix [651](https://github.com/Altinity/clickhouse-backup/issues/651)
- add support for masked secrets for ClickHouse 23.3+, fix [640](https://github.com/Altinity/clickhouse-backup/issues/640)

# v2.2.6
BUG FIXES
- fix panic for resume upload after restart API server for boolean parameters, fix [653](https://github.com/Altinity/clickhouse-backup/issues/653)
- apply SETTINGS check_table_dependencies=0 to DROP DATABASE statement, when pass `--ignore-dependencies` together with `--rm` in `restore` command, fix [651](https://github.com/Altinity/clickhouse-backup/issues/651) 

# v2.2.5
BUG FIXES
- fix error after restart API server for boolean parameters, fix [646](https://github.com/Altinity/clickhouse-backup/issues/646)
- fix corner cases when `restore_schema_on_cluster: cluster`, fix [642](https://github.com/Altinity/clickhouse-backup/issues/642), error happens on 2.2.0-2.2.4
- fix `Makefile` targets `build-docker` and `build-race-docker` for old clickhouse-server version
- fix typo `retries_pause` config definition in general section

# v2.2.4
BUG FIXES
- fix wrong deletion on S3 for versioned buckets, use s3.HeadObject instead of s3.GetObjectAttributes, fix [643](https://github.com/Altinity/clickhouse-backup/pull/643)

# v2.2.3
BUG FIXES
- fix wrong parameters parsing from *.state file for resumable upload \ download after restart, fix [641](https://github.com/Altinity/clickhouse-backup/issues/641)

# v2.2.2
IMPROVEMENTS
- add `callback` parameter to upload, download, create, restore API endpoints, fix [636](https://github.com/Altinity/clickhouse-backup/pull/636)

BUG FIXES
- add system.macros could be applied to `path` config section to ReadMe.md, fix [638](https://github.com/Altinity/clickhouse-backup/issues/638)
- fix connection leaks for S3 versioned buckets during execution upload and delete command, fix [637](https://github.com/Altinity/clickhouse-backup/pull/637)

# v2.2.1
IMPROVEMENTS
- add additional server-side encryption parameters to s3 config section, fix [619](https://github.com/Altinity/clickhouse-backup/issues/619)
- `restore_remote` will not return error when backup already exists in local storage during download check, fix [625](https://github.com/Altinity/clickhouse-backup/issues/625)

BUG FIXES
- fix error after restart API server when .state file present in backup folder, fix [623](https://github.com/Altinity/clickhouse-backup/issues/623)
- fix upload / download files from projections multiple times, cause  backup create wrong create *.proj as separate data part, fix [622](https://github.com/Altinity/clickhouse-backup/issues/622)

# v2.2.0
IMPROVEMENTS
- switch to go 1.20
- after API server startup, if `/var/lib/clickhouse/backup/*/(upload|download).state` present, then operation will continue in background, fix [608](https://github.com/Altinity/clickhouse-backup/issues/608)
- make `use_resumable_state: true` behavior for `upload` and `download`, fix [608](https://github.com/Altinity/clickhouse-backup/issues/608) 
- improved behavior `--partitions` parameter, for cases when PARTITION BY clause return hashed value instead of numeric prefix for `partition_id` in `system.parts`, fix [602](https://github.com/Altinity/clickhouse-backup/issues/602) 
- apply `system.macros` values when use `restore_schema_on_cluster` and replace cluster name in engine=Distributed tables, fix [574](https://github.com/Altinity/clickhouse-backup/issues/574) 
- switch S3 storage backend to https://github.com/aws/aws-sdk-go-v2/, fix [534](https://github.com/Altinity/clickhouse-backup/issues/534)
- added `S3_OBJECT_LABLES` and `GCS_OBJECT_LABELS` to allow setup each backup object metadata during upload fix [588](https://github.com/Altinity/clickhouse-backup/issues/588)
- added `clickhouse-keeper` as zookeeper replacement for integration test during reproduce [416](https://github.com/Altinity/clickhouse-backup/issues/416)
- decrease memory buffers for S3 and GCS, change default value for `upload_concurrency` and `download_concurrency` to `round(sqrt(MAX_CPU / 2))`, fix [539](https://github.com/Altinity/clickhouse-backup/issues/539)
- added ability to set up custom storage class for GCS and S3 depends on backupName pattern, fix [584](https://github.com/Altinity/clickhouse-backup/issues/584)

BUG FIXES
- fix ssh connection leak for SFTP remote storage, fix [578](https://github.com/Altinity/clickhouse-backup/issues/578)
- fix wrong Content-Type header, fix [605](https://github.com/Altinity/clickhouse-backup/issues/605)
- fix wrong behavior for `download` with `--partitions` fix [606](https://github.com/Altinity/clickhouse-backup/issues/606) 
- fix wrong size of backup in list command if upload or download was break and resume, fix [526](https://github.com/Altinity/clickhouse-backup/issues/526)
- fix `_successful_` and `_failed_` metrics counter issue, happens after 2.1.0, fix [589](https://github.com/Altinity/clickhouse-backup/issues/589)
- fix wrong calculation date of last remote backup during startup
- fix wrong duration, status for metrics after 2.1.0 refactoring, fix [599](https://github.com/Altinity/clickhouse-backup/issues/599)
- fix panic on LIVE VIEW tables with option --restore-database-mapping db:db_new enabled), thanks @php53unit

# v2.1.3
IMPROVEMENTS
- during upload sort tables descending by `total_bytes` if this field present 
- improve ReadMe.md add description for all CLI commands and parameters
- add `use_resumable_state` to config to allow default resumable behavior in `create_remote`, `upload`, `restore_remote` and `download` commands, fix [576](https://github.com/Altinity/clickhouse-backup/issues/576)

BUG FIXES
- fix `--watch-backup-name-template` command line parsing, overridden after config reload, fix [548](https://github.com/Altinity/clickhouse-backup/issues/548)
- fix wrong regexp, when `restore_schema_on_cluster: cluster_name`, fix [552](https://github.com/Altinity/clickhouse-backup/issues/552)
- fix wrong `clean` command and API behavior, fix [533](https://github.com/Altinity/clickhouse-backup/issues/533)
- fix getMacro usage in Examples for backup / restore sharded cluster.
- fix deletion files from S3 versioned bucket, fix [555](https://github.com/Altinity/clickhouse-backup/issues/555)
- fix `--restore-database-mapping` behavior for `ReplicatedMergeTree` (replace database name in replication path) and `Distributed` (replace database name in underlying table) tables, fix [547](https://github.com/Altinity/clickhouse-backup/issues/547)
- `MaterializedPostgreSQL` doesn't support FREEZE, fix [550](https://github.com/Altinity/clickhouse-backup/issues/550), see also https://github.com/ClickHouse/ClickHouse/issues/32902, https://github.com/ClickHouse/ClickHouse/issues/44252
- `create` and `restore` commands will respect `skip_tables` config options and `--table` cli parameter, to avoid create unnecessary empty databases, fix [583](https://github.com/Altinity/clickhouse-backup/issues/583)
- fix `watch` unexpected connection closed behavior, fix [568](https://github.com/Altinity/clickhouse-backup/issues/568)
- fix `watch` validation parameters corner cases, close [569](https://github.com/Altinity/clickhouse-backup/pull/569)
- fix `--restore-database-mapping` behavior for `ATTACH MATERIALIZED VIEW`, `CREATE VIEW` and `restore --data` corner cases, fix [559](https://github.com/Altinity/clickhouse-backup/issues/559)

# v2.1.2
IMPROVEMENTS
- add `watch` description to Examples.md

BUG FIXES
- fix panic when use `--restore-database-mapping=db1:db2`, fix [545](https://github.com/Altinity/clickhouse-backup/issues/545)
- fix panic when use `--partitions=XXX`, fix [544](https://github.com/Altinity/clickhouse-backup/issues/545)

# v2.1.1
BUG FIXES
- return bash and clickhouse usergroup to Dockerfile image short, fix [542](https://github.com/Altinity/clickhouse-backup/issues/542)

# v2.1.0
IMPROVEMENTS
- complex refactoring to use contexts, AWS and SFTP storage not full supported
- complex refactoring for logging to avoid race condition when change log level during config reload
- improve kubernetes example for adjust incremental backup, fix [523](https://github.com/Altinity/clickhouse-backup/issues/523)
- add storage independent retries policy, fix [397](https://github.com/Altinity/clickhouse-backup/issues/397)
- add `clickhouse-backup-full` docker image with integrated `kopia`, `rsync`, `restic` and `clickhouse-local`, fix [507](https://github.com/Altinity/clickhouse-backup/issues/507)
- implement `GET /backup/kill?command=XXX` API to allow kill, fix [516](https://github.com/Altinity/clickhouse-backup/issues/516)  
- implement `kill "full command"` in `POST /backup/actions` handler, fix [516](https://github.com/Altinity/clickhouse-backup/issues/516)
- implement `watch` in `POST /backup/actions` handler API and CLI command, fix [430](https://github.com/Altinity/clickhouse-backup/issues/430)
- implement `clickhouse-backup server --watch` to allow server start watch after start, fix [430](https://github.com/Altinity/clickhouse-backup/issues/430)
- update metric `last_{create|create_remote|upload}_finish` metrics values during API server startup, fix [515](https://github.com/Altinity/clickhouse-backup/issues/515)
- implement `clean_remote_broken` command and `POST /backup/clean/remote_broken` API request, fix [520](https://github.com/Altinity/clickhouse-backup/issues/520)
- add metric `number_backups_remote_broken` to calculate broken remote backups, fix [530](https://github.com/Altinity/clickhouse-backup/issues/530)

BUG FIXES
- fix `keep_backups_remote` behavior for recursive incremental sequences, fix [525](https://github.com/Altinity/clickhouse-backup/issues/525)
- for `restore` command call `DROP DATABASE IF EXISTS db SYNC` when pass `--schema` and `--drop` together, fix [514](https://github.com/Altinity/clickhouse-backup/issues/514)
- close persistent connections for remote backup storage after command execution, fix [535](https://github.com/Altinity/clickhouse-backup/issues/535)
- lot of typos fixes
- fix all commands was always return 200 status (expect errors) and ignore status which passed from application code in API server 

# v2.0.0
IMPROVEMENTS
- implements `remote_storage: custom`, which allow us to adopt any external backup system like `restic`, `kopia`, `rsync`, rclone etc. fix [383](https://github.com/Altinity/clickhouse-backup/issues/383)
- add example workflow how to make backup / restore on sharded cluster, fix [469](https://github.com/Altinity/clickhouse-backup/discussions/469)
- add `use_embedded_backup_restore` to allow `BACKUP` and `RESTORE` SQL commands usage, fix [323](https://github.com/Altinity/clickhouse-backup/issues/323), need 22.7+ and resolve https://github.com/ClickHouse/ClickHouse/issues/39416
- add `timeout` to `azure` config `AZBLOB_TIMEOUT` to allow download with bad network quality, fix [467](https://github.com/Altinity/clickhouse-backup/issues/467)
- switch to go 1.19
- refactoring to remove legacy `storage` package
- add `table` parameter to `tables` cli command and `/backup/tables` API handler, fix [367](https://github.com/Altinity/clickhouse-backup/issues/367)
- add `--resumable` parameter to `create_remote`, `upload`, `restore_remote`, `donwload` commands to allow resume upload or download after break. Ignored for `remote_storage: custom`, fix [207](https://github.com/Altinity/clickhouse-backup/issues/207)
- add `--ignore-dependencies` parameter to `restore` and `restore_remote`, to allow drop object during restore schema on server where schema objects already exists and contains dependencies which not present in backup, fix [455](https://github.com/Altinity/clickhouse-backup/issues/455)
- add `restore --restore-database-mapping=<originDB>:<targetDB>[,<...>]`, fix [269](https://github.com/Altinity/clickhouse-backup/issues/269), thanks @mojerro

BUG FIXES
- fix wrong upload / download behavior for `compression_format: none` and `remote_storage: ftp`

# v1.6.2
IMPROVEMENTS
- add Azure to every CI/CD run, testing with `Azurite`

BUG FIXES
- fix azblob.Walk with recursive=True, for properly delete remote backups

# v1.6.1
BUG FIXES
- fix system.macros detect query

# v1.6.0
IMPROVEMENTS
- add `storage_class` (GCS_STORAGE_CLASS) support for `remote_storage: gcs` fix [502](https://github.com/Altinity/clickhouse-backup/issues/502)
- upgrade aws golang sdk and gcp golang sdk to latest versions

# v1.5.2
IMPROVEMENTS
- switch to go 1.19
- refactoring to remove legacy `storage` package

# v1.5.1
BUG FIXES
- properly execute `CREATE DATABASE IF NOT EXISTS ... ON CLUSTER` when setup `restore_schema_on_cluster`, fix [486](https://github.com/Altinity/clickhouse-backup/issues/486)

# v1.5.0
IMPROVEMENTS 
- try to improve implementation `check_replicas_before_attach` configuration to avoid concurrent ATTACH PART execution during `restore` command on multi-shard cluster, fix [474](https://github.com/Altinity/clickhouse-backup/issues/474)
- add `timeout` to `azure` config `AZBLOB_TIMEOUT` to allow download with bad network quality, fix [467](https://github.com/Altinity/clickhouse-backup/issues/467)

# v1.4.9
BUG FIXES
- fix `download` behavior for parts which contains special characters in name, fix [462](https://github.com/Altinity/clickhouse-backup/issues/462)

# v1.4.8
IMPROVEMENTS
- add `check_replicas_before_attach` configuration to avoid concurrent ATTACH PART execution during `restore` command on multi-shard cluster, fix [474](https://github.com/Altinity/clickhouse-backup/issues/474)
- allow backup list when clickhouse server offline, fix [476](https://github.com/Altinity/clickhouse-backup/issues/476)
- add `use_custom_storage_class` (`S3_USE_CUSTOM_STORAGE_CLASS`) option to `s3` section, thanks @realwhite

BUG FIXES
- resolve `{uuid}` marcos during restore for `ReplicatedMergeTree` table and ClickHouse server 22.5+, fix [466](https://github.com/Altinity/clickhouse-backup/issues/466)

# v1.4.7
IMPROVEMENTS
- PROPERLY restore to default disk if disks not found on destination clickhouse server, fix [457](https://github.com/Altinity/clickhouse-backup/issues/457)

# v1.4.6
BUG FIXES
- fix infinite loop `error can't acquire semaphore during Download: context canceled`, and `error can't acquire semaphore during Upload: context canceled` all 1.4.x users recommends upgrade to 1.4.6

# v1.4.5
IMPROVEMENTS
- add `CLICKHOUSE_FREEZE_BY_PART_WHERE` option which allow freeze by part with WHERE condition, thanks @vahid-sohrabloo 

# v1.4.4
IMPROVEMENTS
- download and restore to default disk if disks not found on destination clickhouse server, fix [457](https://github.com/Altinity/clickhouse-backup/issues/457)

# v1.4.3
IMPROVEMENTS
- add `API_INTEGRATION_TABLES_HOST` option to allow use DNS name in integration tables system.backup_list, system.backup_actions

BUG FIXES
- fix `upload_by_part: false` max file size calculation, fix [454](https://github.com/Altinity/clickhouse-backup/issues/454)

# v1.4.2
BUG FIXES
- fix `--partitions` parameter parsing, fix [425](https://github.com/Altinity/clickhouse-backup/issues/425)

# v1.4.1
BUG FIXES
- fix upload data go routines waiting, expect the same upload speed as 1.3.2

# v1.4.0
IMPROVEMENTS
- add `S3_ALLOW_MULTIPART_DOWNLOAD` to config, to improve download speed, fix [431](https://github.com/Altinity/clickhouse-backup/issues/431)
- add support backup/restore [user defined functions](https://clickhouse.com/docs/en/sql-reference/statements/create/function), fix [420](https://github.com/Altinity/clickhouse-backup/issues/420)
- add `clickhouse_backup_number_backups_remote`, `clickhouse_backup_number_backups_local`, `clickhouse_backup_number_backups_remote_expected`,`clickhouse_backup_number_backups_local_expected` prometheus metric, fix [437](https://github.com/Altinity/clickhouse-backup/issues/437)
- add ability to apply `system.macros` values to `path` field in all types of `remote_storage`, fix [438](https://github.com/Altinity/clickhouse-backup/issues/438) 
- use all disks for upload and download for multi-disk volumes in parallel when `upload_by_part: true` fix [#400](https://github.com/Altinity/clickhouse-backup/issues/400) 

BUG FIXES
- fix wrong warning for .gz, .bz2, .br archive extensions during download, fix [441](https://github.com/Altinity/clickhouse-backup/issues/441)

# v1.3.2
IMPROVEMENTS
- add TLS certificates and TLS CA support for clickhouse connections, fix [410](https://github.com/Altinity/clickhouse-backup/issues/410)
- switch to go 1.18
- add clickhouse version 22.3 to integration tests
- add `S3_MAX_PARTS_COUNT` and `AZBLOB_MAX_PARTS_COUNT` for properly calculate buffer sizes during upload and download for custom S3 implementation like Swift
- add multithreading GZIP implementation

BUG FIXES
- fix [406](https://github.com/Altinity/clickhouse-backup/issues/406), properly handle `path` for S3, GCS for case when it begins from "/"
- fix [409](https://github.com/Altinity/clickhouse-backup/issues/409), avoid delete partially uploaded backups via `backups_keep_remote` option
- fix [422](https://github.com/Altinity/clickhouse-backup/issues/422), avoid cache broken (partially uploaded) remote backup metadata.
- fix [404](https://github.com/Altinity/clickhouse-backup/issues/404), properly calculate S3_PART_SIZE to avoid freeze after 10000 multi parts uploading, properly handle error when upload and download go-routine failed to avoid pipe stuck 

# v1.3.1

IMPROVEMENTS
- fix [387](https://github.com/Altinity/clickhouse-backup/issues/387#issuecomment-1034648447), improve documentation related to memory and CPU usage

BUG FIXES
- fix [392](https://github.com/Altinity/clickhouse-backup/issues/392), correct download for recursive sequence of diff backups when `DOWNLOAD_BY_PART` true
- fix [390](https://github.com/Altinity/clickhouse-backup/issues/390), respect skip_tables patterns during restore and skip all INFORMATION_SCHEMA related tables even skip_tables don't contain INFORMATION_SCHEMA pattern
- fix [388](https://github.com/Altinity/clickhouse-backup/issues/388), improve restore ON CLUSTER for VIEW with TO clause
- fix [385](https://github.com/Altinity/clickhouse-backup/issues/385), properly handle multiple incremental backup sequences + `BACKUPS_TO_KEEP_REMOTE`

# v1.3.0

IMPROVEMENTS
- Add `API_ALLOW_PARALLEL` to support multiple parallel execution calls for, WARNING, control command names don't try to execute multiple same commands and be careful, it could allocate much memory
  during upload / download, fix [#332](https://github.com/Altinity/clickhouse-backup/issues/332)
- Add support for `--partitions` on create, upload, download, restore CLI commands and API endpoint fix [#378](https://github.com/Altinity/clickhouse-backup/issues/378) properly implementation
  of [#356](https://github.com/Altinity/clickhouse-backup/pull/356)
- Add implementation `--diff-from-remote` for `upload` command and properly handle `required` on download command, fix [#289](https://github.com/Altinity/clickhouse-backup/issues/289)
- Add `print-config` cli command fix [#366](https://github.com/Altinity/clickhouse-backup/issues/366)
- Add `UPLOAD_BY_PART` (default: true) option for improve upload/download concurrency fix [#324](https://github.com/Altinity/clickhouse-backup/issues/324)
- Add support ARM platform for Docker images and pre-compiled binary files, fix [#312](https://github.com/Altinity/clickhouse-backup/issues/312)
- KeepRemoteBackups should respect differential backups, fix [#111](https://github.com/Altinity/clickhouse-backup/issues/111)
- Add `SFTP_DEBUG` option, fix [#335](https://github.com/Altinity/clickhouse-backup/issues/335)
- Add ability to restore schema `ON CLUSTER`, fix [#145](https://github.com/Altinity/clickhouse-backup/issues/145)
- Add support encrypted disk (include s3 encrypted disks), fix [#260](https://github.com/Altinity/clickhouse-backup/issues/260)
- API Server optimization for speed of `last_backup_size_remote` metric calculation to make it async during REST API startup and after download/upload,
  fix [#309](https://github.com/Altinity/clickhouse-backup/issues/309)
- Improve `list remote` speed via local metadata cache in `$TEMP/.clickhouse-backup.$REMOTE_STORAGE`, fix [#318](https://github.com/Altinity/clickhouse-backup/issues/318)
- Add `CLICKHOUSE_IGNORE_NOT_EXISTS_ERROR_DURING_FREEZE` option, fix [#319](https://github.com/Altinity/clickhouse-backup/issues/319)
- Add support for PROJECTION, fix [#320](https://github.com/Altinity/clickhouse-backup/issues/320)
- Return `clean` cli command and API `POST /backup/clean` endpoint, fix [#379](https://github.com/Altinity/clickhouse-backup/issues/379)

BUG FIXES
- fix [#300](https://github.com/Altinity/clickhouse-backup/issues/300), allow GCP properly work with empty `GCP_PATH`
  value
- fix [#340](https://github.com/Altinity/clickhouse-backup/issues/340), properly handle errors on S3 during Walk() and
  delete old backup
- fix [#331](https://github.com/Altinity/clickhouse-backup/issues/331), properly restore tables where have table name
  with the same name as database name
- fix [#311](https://github.com/Altinity/clickhouse-backup/issues/311), properly run clickhouse-backup inside docker
  container via entrypoint
- fix [#317](https://github.com/Altinity/clickhouse-backup/issues/317), properly upload large files to Azure Blob
  Storage
- fix [#220](https://github.com/Altinity/clickhouse-backup/issues/220), properly handle total_bytes for uint64 type
- fix [#304](https://github.com/Altinity/clickhouse-backup/issues/304), properly handle archive extension during download instead of use config settings
- fix [#375](https://github.com/Altinity/clickhouse-backup/issues/375), properly `REMOTE_STORAGE=none` error handle
- fix [#379](https://github.com/Altinity/clickhouse-backup/issues/379), will try to clean `shadow` if `create` fail during `moveShadow`
- more precise calculation backup size during `upload`, for backups created with `--partitions`, fix bug after [#356](https://github.com/Altinity/clickhouse-backup/pull/356)
- fix `restore --rm` behavior for 20.12+ for tables which have dependent objects (like dictionary)
- fix concurrency by `FTP` creation directories during upload, reduce connection pool usage
- properly handle `--schema` parameter for show local backup size after `download`
- fix restore bug for WINDOW VIEW, thanks @zvonand

EXPERIMENTAL
- Try to add experimental support for backup `MaterializedMySQL` and `MaterializedPostgeSQL` tables, restore MySQL tables not impossible now without replace `table_name.json` to `Engine=MergeTree`,
  PostgresSQL not supported now, see https://github.com/ClickHouse/ClickHouse/issues/32902

# v1.2.4

HOT FIXES
- fix [409](https://github.com/Altinity/clickhouse-backup/issues/409), avoid delete partially uploaded backups via `backups_keep_remote` option

# v1.2.3

HOT FIXES
- fix [390](https://github.com/Altinity/clickhouse-backup/issues/390), respect skip_tables patterns during restore and skip all INFORMATION_SCHEMA related tables even skip_tables don't contain INFORMATION_SCHEMA pattern

# v1.2.2

IMPROVEMENTS
- Add REST API `POST /backup/tables/all`, fix `POST /backup/tables` to respect `CLICKHOUSE_SKIP_TABLES`

BUG FIXES
- fix [#297](https://github.com/Altinity/clickhouse-backup/issues/297), properly restore tables where have fields with the same name as table name
- fix [#298](https://github.com/Altinity/clickhouse-backup/issues/298), properly create `system.backup_actions` and `system.backup_list` integration tables for ClickHouse before 21.1
- fix [#303](https://github.com/Altinity/clickhouse-backup/issues/303), ignore leading and trailing spaces in `skip_tables` and `--tables` parameters
- fix [#292](https://github.com/Altinity/clickhouse-backup/issues/292), lock clickhouse connection pool to single connection

# v1.2.1

IMPROVEMENTS
- Add REST API integration tests

BUG FIXES
- fix [#290](https://github.com/Altinity/clickhouse-backup/issues/290)
- fix [#291](https://github.com/Altinity/clickhouse-backup/issues/291)
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
- fix [#280](https://github.com/Altinity/clickhouse-backup/issues/280), incorrect prometheus metrics measurement for `/backup/create`, `/backup/upload`, `/backup/download`
- fix [#273](https://github.com/Altinity/clickhouse-backup/issues/273), return `S3_PART_SIZE` back, but calculates it smartly
- fix [#252](https://github.com/Altinity/clickhouse-backup/issues/252), now you can pass `last` and `filter` query string parameters
- fix [#246](https://github.com/Altinity/clickhouse-backup/issues/246), incorrect error messages when use `REMOTE_STORAGE=none`
- fix [#283](https://github.com/Altinity/clickhouse-backup/issues/283), properly handle error message from `FTP` server
- fix [#268](https://github.com/Altinity/clickhouse-backup/issues/268), properly restore legacy backup for schema without database name

# v1.1.1

BUG FIXES
- fix broken `system.backup_list` integration table after add `required field` in https://github.com/Altinity/clickhouse-backup/pull/263
- fix [#274](https://github.com/Altinity/clickhouse-backup/issues/274) invalid `SFTP_PASSWORD` environment usage 

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
- fix [#266](https://github.com/Altinity/clickhouse-backup/discussions/266) properly restore legacy backup format
- fix [#244](https://github.com/Altinity/clickhouse-backup/issues/244) add `read_timeout`, `write_timeout` to client-side timeout for `clickhouse-go` 
- fix [#255](https://github.com/Altinity/clickhouse-backup/issues/255) restrict connection pooling to 1 in `clickhouse-go` 
- fix [#256](https://github.com/Altinity/clickhouse-backup/issues/256) remote_storage: none, was broke compression
- fix [#266](https://github.com/Altinity/clickhouse-backup/discussions/266) legacy backups from version prior 1.0 can't restore without `allow_empty_backup: true`
- fix [#223](https://github.com/Altinity/clickhouse-backup/issues/223) backup only database metadata for proxy integrated database engines like MySQL, PostgresSQL
- fix `GCS` global buffer wrong usage during UPLOAD_CONCURRENCY > 1
- Remove unused `SKIP_SYNC_REPLICA_TIMEOUTS` option

# v1.0.0

BUG FIXES
- Fixed silent cancel uploading when table has more than 4k files  (fix [#203](https://github.com/Altinity/clickhouse-backup/issues/203), [#163](https://github.com/Altinity/clickhouse-backup/issues/163). Thanks [mastertheknife](https://github.com/mastertheknife))
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
- Wait for clickhouse in server mode (fix [#169](https://github.com/Altinity/clickhouse-backup/issues/169))
- Added Disk Mapping feature (fix [#162](https://github.com/Altinity/clickhouse-backup/issues/162))

BUG FIXES

- Fixed 'ftp' remote storage ([#164](https://github.com/Altinity/clickhouse-backup/issues/164))

# v0.6.5

**It is the last release of v0.x.x**

IMPROVEMENTS
- Added 'create_remote' and 'restore_remote' commands
- Changed update config behavior in API mode

BUG FIXES
- fix [#154](https://github.com/Altinity/clickhouse-backup/issues/154)
- fix [#165](https://github.com/Altinity/clickhouse-backup/issues/165)

# v1.0.0-alpha1

IMPROVEMENTS
- Support for new versions of ClickHouse ([#155](https://github.com/Altinity/clickhouse-backup/issues/155))
- Support of Atomic Database Engine ([#140](https://github.com/Altinity/clickhouse-backup/issues/140), [#141](https://github.com/Altinity/clickhouse-backup/issues/141), [#126](https://github.com/Altinity/clickhouse-backup/issues/126))
- Support of multi disk ClickHouse configurations ([#51](https://github.com/Altinity/clickhouse-backup/issues/51))
- Ability to upload and download specific tables from backup
- Added partitions backup on remote storage ([#83](https://github.com/Altinity/clickhouse-backup/issues/83))
- Added support for backup/upload/download schema only ([#138](https://github.com/Altinity/clickhouse-backup/issues/138))
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

