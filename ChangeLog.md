# v2.6.34
IMPROVEMENTS
- add full support for backup and restore of `named collections`, which can be stored in `keeper` or on the `local` filesystem. fix [961](https://github.com/Altinity/clickhouse-backup/issues/961) 
- add `--named-collections`, `--named-collections-only` flag to `create_remote` and `restore_remote` commands.
- add `named_collections_backup_always` option to the `general` config section.
- add named collection size to the backup list output and API.
- add support for named collections parameters in API handlers.
- switch to Go 1.25.
- update default ClickHouse version to 25.8 and add it to the test matrix.

BUG FIXES
- fix restore schema on cluster for VIEW, fix [1199](https://github.com/Altinity/clickhouse-backup/issues/1199).
- disable free space check when using `--hardlink-exists-files`, fix [1198](https://github.com/Altinity/clickhouse-backup/issues/1198)
- add chmod 0640 for `--hardlink-exists-files` during `download` and `restore_remote`, fix [1164](https://github.com/Altinity/clickhouse-backup/issues/1164)

# v2.6.33
BUG FIXES
- add creation integration tables for `POST /restart` and `kill -SIGHUP` reaction, fix [1195](https://github.com/Altinity/clickhouse-backup/issues/1195)

# v2.6.32
IMPROVEMENTS
- add `--format` for `list` command which support json, yaml, csv, tsv, fix [1192](https://github.com/Altinity/clickhouse-backup/issues/1192), thanks @minguyen9988

# v2.6.31
IMPROVEMENTS
- implements IRSA inherit when we provide different serviceAccount and `assume_role_arn` in `s3` config section, fix [1191](https://github.com/Altinity/clickhouse-backup/issues/1191)
- add object labels configs to e2e integration tests
- add publish docker images into `ghcr.io/altinity/clickhouse-backup`, fix [389](https://github.com/Altinity/clickhouse-backup/issues/389)

# v2.6.30
IMPROVEMENTS
- add in REST API `operation_id` to result for all asynchronous commands (`create`,`upload`,`download`,`restore`) which allow poll /backup/status more precise, fix [1189](https://github.com/Altinity/clickhouse-backup/pull/1189), thanks @lepetitops
- add `POST /backup/create_remote` and `POST /backup/restore_remote` to API
- add `--hardlink-exists-files` to `download` and `restore_remote`, works only with backups created with 2.6.30 plus clickhouse-backup version, fix [1164](https://github.com/Altinity/clickhouse-backup/issues/1164)
- add `hardlink_exists_files` to API for `POST /backup/download` and `POST /backup/restore_remote`, works only with backups created with 2.6.30 plus clickhouse-backup version, fix [1164](https://github.com/Altinity/clickhouse-backup/issues/1164)

# v2.6.29
BUG FIXES
- respect local timezone when show backup time in list, fix [1185](https://github.com/Altinity/clickhouse-backup/issues/1185)

# v2.6.28
BUG FIXES
- time layout for watch backup name shall respect TZ environment variable, fix [1184](https://github.com/Altinity/clickhouse-backup/issues/1184)

# v2.6.27
IMPROVEMENTS
- add config option `configuration_backup_always` (default false), fix [1180](https://github.com/Altinity/clickhouse-backup/issues/1180)
- add `clean_local_broken` CLI command and `POST /backup/clean/local_broken` REST API, fix [1178](https://github.com/Altinity/clickhouse-backup/issues/1178)
- create pid file in `/var/lib/clickhouse/backup/backup_name/clickchouse-backup.pid` to avoid parallel work for the same backup even when `allow_parallel: true`, fix [966](https://github.com/Altinity/clickhouse-backup/issues/966)

BUG FIXES
- fix race condition for `server --watch` command, fix [1177](https://github.com/Altinity/clickhouse-backup/issues/1177)

# v2.6.26
BUG FIXES
- `object_disk.CopyObjectStreaming` can't copy files with size more 5Gb to S3, cause pass localSize 0 into PutFileAbsolute, fix [1176](https://github.com/Altinity/clickhouse-backup/issues/1176)

# v2.6.25
IMPROVEMENTS
- change retries from constant to exponential backoff and add RETRIES_JITTER configuration option, to avoid same time retries from parallel operation

# v2.6.24
IMPROVEMENTS
- add logs for retries, to allow figure out with blackbaze2 s3 compatible provider, rate limit errors

# v2.6.23
BUG FIXES
- `watch` command stop works with panic, fix [1166](https://github.com/Altinity/clickhouse-backup/issues/1166), affected versions 2.6.19-2.6.22 after [1152](https://github.com/Altinity/clickhouse-backup/issues/1152)

# v2.6.22
BUG FIXES
- fix corner cases when `server --watch` aborts watch process when `backups_to_keep_local: -1` and `--watch-delete-source=false`, fix [1157](https://github.com/Altinity/clickhouse-backup/issues/1157), fix [1156](https://github.com/Altinity/clickhouse-backup/issues/1156), thanks @elliott-logrocket 

# v2.6.21
BUG FIXES
- final changes to fix backup for azblob disk behavior when `storage_account_url` contains container as first part in hostname, fix [1153](https://github.com/Altinity/clickhouse-backup/issues/1153)

# v2.6.20
BUG FIXES
- fix backup for azblob disk behavior when `storage_account_url` contains container as first part in hostname, fix [1153](https://github.com/Altinity/clickhouse-backup/issues/1153)

# v2.6.19
BUG FIXES
- fix `clickhouse_backup_number_backups_remote` and `last_backup_size_remote` metrics behavior in `watch` command, fix [1152](https://github.com/Altinity/clickhouse-backup/issues/1152)
- fix empty `path` and non-empty `object_disk_path` config parameters, allows unexpected deletion backup object disk data, fix [859](https://github.com/Altinity/clickhouse-backup/issues/859) 
- reproduce behavior `<metadata_path>` parameter in clickhouse `<storage_configuration>` don't contain trailing slash, restore make hardlinks to wrong directory, look details in https://github.com/Altinity/clickhouse-backup/issues/859#issuecomment-2896880448, https://github.com/ClickHouse/ClickHouse/issues/80647 
- fix backup for azblob disk behavior when `storage_account_url` contains container as first part in hostname, fix [1153](https://github.com/Altinity/clickhouse-backup/issues/1153)
- fix wrong unescape special characters in table create query, fix [1151](https://github.com/Altinity/clickhouse-backup/issues/1151), affected aversions from 2.6.6 to 2.6.18, backups which created in these versions which contains `\` in create table definition, requires manual replacing  `\` to `\\` for properly restore.

# v2.6.18
IMPROVEMENTS
- add support multipart upload and multipart download for COS storage, add COS_ALLOW_MULTIPART_DOWNLOAD and COS_CONCURRENCY to config, fix [1074](https://github.com/Altinity/clickhouse-backup/issues/1074) 

BUG FIXES
- fix one more corner cases for --restore-database-mapping, fix [1146](https://github.com/Altinity/clickhouse-backup/issues/1146#issuecomment-2886456288)

# v2.6.17
BUG FIXES
- respect original database engine during `restore --restore-database-mapping`, fix [1146](https://github.com/Altinity/clickhouse-backup/issues/1146)
- fix wrong metadata size and object disk size fields values in CLI list command output

# v2.6.16
BUG FIXES
- fix database errors related to database with `engine=Replicated` fix [1127](https://github.com/Altinity/clickhouse-backup/issues/1127)
- avoid to replace {cluster} in engine=Distributed fix [574](https://github.com/Altinity/clickhouse-backup/issues/574)
- make some resumable state methods private and properly restore parameter from `*.state2` files after restart API server
- some logging cleanup

# v2.6.15
BUG FIXES
- fix wrong cleanup *.state2 file in ResumeOperationsAfterRestart, after 2.6.12 [1126](https://github.com/Altinity/clickhouse-backup/issues/1126), fix [1133](https://github.com/Altinity/clickhouse-backup/issues/1133)
- fix virtual host bucket URL style in s3 access disk

# v2.6.14
BUG FIXES
- fix restore RBAC, fix [1130](https://github.com/Altinity/clickhouse-backup/pull/1130), thanks @yudhiesh

# v2.6.13
BUG FIXES
- fix corner case for `--restore-database-mapping`, fix [820](https://github.com/Altinity/clickhouse-backup/issues/820#issuecomment-2773501803)

# v2.6.12
BUG FIXES
- fix corner case for wrong path for state file during `ResumeOperationsAfterRestart` fix [1126](https://github.com/Altinity/clickhouse-backup/issues/1126)

# v2.6.11
BUG FIXES
- fix another corner cases for check free space size before download, fix [878](https://github.com/Altinity/clickhouse-backup/issues/878)

# v2.6.10
BUG FIXES
- fix corner cases for `restore_schema_on_cluster: true` and `<user_defined_zookeeper_path>`, fix [1123](https://github.com/Altinity/clickhouse-backup/issues/1123)
- fix corner cases for check free space size before download, fix [878](https://github.com/Altinity/clickhouse-backup/issues/878)

# v2.6.9
IMPROVEMENTS
- add support for <object_storage_type> in <storage_configuration>, fix [1112](https://github.com/Altinity/clickhouse-backup/issues/1112)

BUG FIXES
- fix corner cases during restore for embedded backup and empty ReplicatedMergeTree() engine definition, fix [1115](https://github.com/Altinity/clickhouse-backup/issues/1115#issuecomment-2735531504)

# v2.6.8

BUG FIXES
- fix create --rbac works as create --rbac-only - fix [1111](https://github.com/Altinity/clickhouse-backup/issues/1111)

# v2.6.7

BUG FIXES
- improve workaround for `metdata_path` field change behavior in 25.1+, look details in https://github.com/ClickHouse/ClickHouse/issues/76546, fix [1093](https://github.com/Altinity/clickhouse-backup/issues/1093)

# v2.6.6

IMPROVEMENTS
- Add `--replicated-copy-to-detached` option to restore command, it allows faster restore on multiple replicas, second and follow replicas will handle ATTACH_PART restore events, fix [1104](https://github.com/Altinity/clickhouse-backup/issues/1104)
- Add `CLICKHOUSE_SKIP_DISKS` config option, to allow skip backup some disk like object disks, fix [908](https://github.com/Altinity/clickhouse-backup/issues/908)
- Add simple check free size before disk download, to avoid 100% disk space usage, fix [878](https://github.com/Altinity/clickhouse-backup/issues/878)
- Add `--restore-schema-as-attach` CLI parameter and `POST /backup/restore/{name}`, fix [868](https://github.com/Altinity/clickhouse-backup/issues/868)
- Add `S3_RETRY_MODE` with `standard` and `adaptive`, fix [1097](https://github.com/Altinity/clickhouse-backup/issues/1097)
- Add `server_side_encryption_kms_bucket_key_enabled` support for backup/restore s3 object disks, fix [1092](https://github.com/Altinity/clickhouse-backup/issues/1092)
- Add `AZBLOB_ASSUME_CONTAINER_EXISTS` config option, fix [1094](https://github.com/Altinity/clickhouse-backup/pull/1094), thanks @atykhyy
- Improve Azure authentication mechanism, fix [1047](https://github.com/Altinity/clickhouse-backup/issues/1047), thanks @dnovvak
- Add option `--skip-projections` to `create`, `upload`, `restore` commands, with table pattern to allow make backup
  without projection, restore supported only in `clickhouse-server` 24.3+, fix [861](https://github.com/Altinity/clickhouse-backup/issues/861)
- Remove `S3_PART_SIZE` and `AZBLOB_BUFFER_SIZE` parameter from configuration and significant decrease memory usage
  during upload and download, fix [854](https://github.com/Altinity/clickhouse-backup/issues/854)
- Add `--configs-only` and `--rbac-only` options to `upload` and `download` command, fix [1042](https://github.com/Altinity/clickhouse-backup/issues/1042)
- Add support `\` and `/` special characters in table name and database name, fix [1091](https://github.com/Altinity/clickhouse-backup/issues/1091)
- switch to golang-1.24

BUG FIXES

- `backblaze` s3 provider support only STANDARD storage class, fix [1086](https://github.com/Altinity/clickhouse-backup/issues/1086)
- fix `--restore-database-mapping` with special characters in source table, fix
  reopened [820](https://github.com/Altinity/clickhouse-backup/issues/820#issuecomment-2675628282),
  thanks @IvaskevychYuriy
- add workaround for `metdata_path` field change behavior in 25.1+, look details in https://github.com/ClickHouse/ClickHouse/issues/76546, fix [1093](https://github.com/Altinity/clickhouse-backup/issues/1093)
- add e2e tests for `AZBLOB_SAS`, fix [1060](https://github.com/Altinity/clickhouse-backup/issues/1060),  fix [313](https://github.com/Altinity/clickhouse-backup/issues/313)
- add alibaba/oss/aliyuncs worked config, fix [877](https://github.com/Altinity/clickhouse-backup/issues/877#issuecomment-2589164718),  fix [505](https://github.com/Altinity/clickhouse-backup/issues/505#issuecomment-2589163706)
- add test for COS, fix [1053](https://github.com/Altinity/clickhouse-backup/issues/1053)
- fix `AS WITH x AS` corner case for `restore_schema_on_cluster` option, fix [1075](https://github.com/Altinity/clickhouse-backup/issues/1075)

# v2.6.5

BUG FIXES

- properly decode varint in *.state2 files during check already processed files to avoid too large numbers in logs
- add support `s3://` in parsing endpoint in fix [1035](https://github.com/Altinity/clickhouse-backup/issues/1035)

# v2.6.4

BUG FIXES

- fix possible wrong merging after ATTACH PART for Collapsing and Replacing engines without version,
  look https://github.com/ClickHouse/ClickHouse/issues/71009 for details
- add `log_queries` properly during connection initialization
- fix backup / restore keeper stored RBAC objects for non-empty `/zookeeper/root` in `clickhouse-server` configuration
  fix [1058](https://github.com/Altinity/clickhouse-backup/issues/1058)

# v2.6.3
IMPROVEMENTS
- implement new format for *.state2 files boltdb key value (please, check memory RSS usage)
- clean resumable state if backup parameters changed, fix [840](https://github.com/Altinity/clickhouse-backup/issues/840)
- switch to golang 1.23
- add `clickhouse_backup_local_data_size` metric as alias for `TotalBytesOfMergeTreeTablesm` from `system.asychnrous_metrics`, fix [573](https://github.com/Altinity/clickhouse-backup/issues/573)
- API refactoring, query options with snake case, also allow with dash case.
- add `--resume` parameter to `create` and `restore` command to avoid unnecessary copy object disk data fix [828](https://github.com/Altinity/clickhouse-backup/issues/828) 


BUG FIXES
- after drop table, before create table, will check if replica path already exists, and will try to change `replication_patch`, it helpful for restoring Replicated tables which not contains macros in replication parameters fix [849](https://github.com/Altinity/clickhouse-backup/issues/849)
- fix `TestLongListRemote` for properly time measurement
- fix log_pointer handle from system.replicas during restore, fix [967](https://github.com/Altinity/clickhouse-backup/issues/967)
- fix `use_embedded_backup_restore: true` behavior for azblob, fix [1031](https://github.com/Altinity/clickhouse-backup/issues/1031)
- fix `Nullable(Enum())` types corner case for `check_parts_columns: true`, fix [1033](https://github.com/Altinity/clickhouse-backup/issues/1033)
- fix deletions for versioned s3 buckets, multiple object versions created during internal retry

# v2.6.2
BUG FIXES
- fix rare corner case, for system.disks query behavior fix [1007](https://github.com/Altinity/clickhouse-backup/issues/1007)
- fix --partitions and --restore-database-mapping, --restore-table-mapping works together, fix [1018](https://github.com/Altinity/clickhouse-backup/issues/1018)
- fix wrong slices initialization for `shardFuncByName` (rare used function for backup different tables from different shards), fix [1019](https://github.com/Altinity/clickhouse-backup/pull/1019), thanks @cuishuang

# v2.6.1 
BUG FIXES
- fix unnecessary warnings in `allow_object_disk_streaming: true` behavior during restore
- fix stuck with `gcs.clientPool.BorrowObject error: Timeout waiting for idle object` cause `OBJECT_DISK_SERVER_SIDE_COPY_CONCURRENCY` has default value 32, but it much more than calculated default pool

# v2.6.0
IMPROVEMENTS
- add `rbac-only` and `configs-only` parameters to `POST /backup/create` and `POST /backup/restore` API calls
- add `allow_object_disk_streaming` config option which will make object disk backup when CopyObject failed or when Object Storage have incompatible types, fix [979](https://github.com/Altinity/clickhouse-backup/issues/979)
- add `operation_id` to callback, fix [995](https://github.com/Altinity/clickhouse-backup/issues/995) thanks @manasmulay 

# v2.5.29
BUG FIXES
- fix corner case for backup/restore RBAC object with trailing slash, warn /clickhouse/access//uuid have no children, skip Dump

# v2.5.28
BUG FIXES
- fix corner cases for wrong *_last* metrics calculation after restart, fix [980](https://github.com/Altinity/clickhouse-backup/issues/980)

# v2.5.27
IMPROVEMENTS
- update Dockerfile and Makefile to speedup cross-platform building

BUG FIXES
- update clickhouse-go/v2, try fix [970](https://github.com/Altinity/clickhouse-backup/issues/970)

# v2.5.26
BUG FIXES
- fix corner cases when /var/lib/clickhouse/access already broken, fix [977](https://github.com/Altinity/clickhouse-backup/issues/977)
- finish migrate from `apex/log` to `rs/zerolog`, fix [624](https://github.com/Altinity/clickhouse-backup/issues/624), thanks @rdmrcv  

# v2.5.25
BUG FIXES
- fix corner cases for wrong parsing RBAC name, during resolve conflict for complex multi line RBAC objects, fix [976](https://github.com/Altinity/clickhouse-backup/issues/976)

# v2.5.24
BUG FIXES
- fix corner cases object disk parse endpoint for S3, to avoid wrong `.amazonaws.amazonaws.com` suffix
 
# v2.5.23
BUG FIXES
- fix corner case for LOG_LEVEL + --env, fix [972](https://github.com/Altinity/clickhouse-backup/issues/972)

# v2.5.22
IMPROVEMENTS
- redirect logs into stderr instead of stdout, fix [969](https://github.com/Altinity/clickhouse-backup/issues/969)
- migrate from `apex/log` to `rs/zerolog`, fix RaceConditions, fix [624](https://github.com/Altinity/clickhouse-backup/issues/624),see details https://github.com/apex/log/issues/103

# v2.5.21
IMPROVEMENTS
- switch from `docker-compose` (python) to `docker compose` (golang)
- add parallel integration test execution fix [888](https://github.com/Altinity/clickhouse-backup/issues/888)
 
BUG FIXES
- properly handle log_pointer=1 corner case for `check_replica_before_attach: true`, fix [967](https://github.com/Altinity/clickhouse-backup/issues/967)
- properly handle empty output for `list` command when `remote_storage: custom`, fix [963](https://github.com/Altinity/clickhouse-backup/issues/963), thanks @straysh
- fix corner cases when connect to S3 provider with self-signed TLS certificates, check `S3_DISABLE_CERT_VALIDATION=true` in tests fix [960](https://github.com/Altinity/clickhouse-backup/issues/960)

# v2.5.20
IMPROVEMENTS
- add `--restore-table-mapping` CLI and API parameter to `restore` and `restore_remote` command, fix [937](https://github.com/Altinity/clickhouse-backup/issues/937), thanks @nithin-vunet and @raspreet-vunet

BUG FIXES
- remove trailing `/` from `object_disk_path` to properly `create` and `restore`, fix [946](https://github.com/Altinity/clickhouse-backup/issues/946)

# v2.5.19
BUG FIXES
- fix `restore --rbac` behavior when RBAC objects contains `-`, `.` or any special characters new fixes for [930](https://github.com/Altinity/clickhouse-backup/issues/930)

# v2.5.18
BUG FIXES
- add `clean` command to `POST /backup/actions` API handler, fix [945](https://github.com/Altinity/clickhouse-backup/issues/945)

# v2.5.17
BUG FIXES
- Fix wrong restoration of Materialized views with view name starting with digits for `--restore-table-mapping`, fix [942](https://github.com/Altinity/clickhouse-backup/pull/942), thanks @praveenthuwat

# v2.5.16
BUG FIXES
- allow backup/restore tables and databases which contains additional special characters set, fix [938](https://github.com/Altinity/clickhouse-backup/issues/938)
- properly restore environment variables to avoid failures in config.ValidateConfig in REST API mode, fix [940](https://github.com/Altinity/clickhouse-backup/issues/940)

# v2.5.15
IMPROVEMENTS
- increase `s3_request_timeout_ms` (23.7+) and turn off `s3_use_adaptive_timeouts` (23.11+) when `use_embedded_backup_restore: true`

BUG FIXES
- fix hangs `create` and `restore` when CLICKHOUSE_MAX_CONNECTIONS=0, fix [933](https://github.com/Altinity/clickhouse-backup/issues/933)
- remove obsolete `CLICKHOUSE_EMBEDDED_BACKUP_THREADS`, `CLICKHOUSE_EMBEDDED_BACKUP_THREADS` these settings could configure only via server level, not profile and query settings after 23.3

# v2.5.14
IMPROVEMENTS
- add http_send_timeout=300, http_receive_timeout=300 to embedded backup/restore operations
- explicitly set `allow_s3_native_copy` and `allow_azure_native_copy` settings when `use_embedding_backup_restore: true`

BUG FIXES
- remove too aggressive logs for object disk upload and download operations during create and restore commands execution

# v2.5.13
IMPROVEMENTS
- return error instead of warning when replication in progress during restore operation 

BUG FIXES
- fixed wrong persistent behavior override for `--env` parameter when use it with API server, all 2.5.x versions was affected
- fixed errors during drop exists RBAC objects which contains special character, fix [930](https://github.com/Altinity/clickhouse-backup/issues/930)

# v2.5.12
IMPROVEMENTS
- added "object_disk_size" to upload and download command logs

BUG FIXES
- fixed corner case  in `API server` hang when `watch` background command failures, fix [929](https://github.com/Altinity/clickhouse-backup/pull/929) thanks @tadus21
- removed requirement `compression: none` for `use_embedded_backup_restore: true`
- refactored to fix corner case for backup size calculation for Object disks and Embedded backup, set consistent algorithm for CLI and API `list` command behavior

# v2.5.11
BUG FIXES
- fixed another corner case for `restore --data=1 --env=CLICKHOUSE_SKIP_TABLE_ENGINES=liveview,WindowView`

# v2.5.10
BUG FIXES
- fixed corner case when `use_resumable_state: true` and trying download already present local backup don't return error backup already exists [926](https://github.com/Altinity/clickhouse-backup/issues/926)
- fixed another corner case for `restore --data=1 --env=CLICKHOUSE_SKIP_TABLE_ENGINES=dictionary,view`


# v2.5.9
IMPROVEMENTS
- added to `--partitions` CLI and API parameter additional format `tablesPattern:partition1,partitionX` or `tablesPattern:(partition1),(partitionX)` fix https://github.com/Altinity/clickhouse-backup/issues/916
- added system.backup_version and version into logs fix [917](https://github.com/Altinity/clickhouse-backup/issues/917) 
- added progress=X/Y to logs fix [918](https://github.com/Altinity/clickhouse-backup/issues/918)

BUG FIXES
- allow stopping api server when watch command is stopped, fix [922](https://github.com/Altinity/clickhouse-backup/pull/922), thanks @tadus21
- fixed corner case for --env=CLICKHOUSE_SKIP_TABLE_ENGINES=dictionary,view 

# v2.5.8
IMPROVEMENTS
- added OCI compliant labels to containers, thanks https://github.com/denisok
- increased default clickhouse queries timeout from `5m` to `30m` for allow freeze very large tables with object disks

BUG FIXES
- fix corner cases for `ResumeOperationsAfterRestart` and `keep_backup_local: -1` behavior 
- fix wrong file extension recognition during download for `access` and `configs` , fix https://github.com/Altinity/clickhouse-backup/issues/921   

# v2.5.7
BUG FIXES
- wrong skip tables by engine when empty variables value `CLICKHOUSE_SKIP_TABLE_ENGINES=engine,` instead of `CLICKHOUSE_SKIP_TABLE_ENGINES=engine` fix [915](https://github.com/Altinity/clickhouse-backup/issues/915)
- restore stop works, if RBAC objects present in backup but user which used for connect to clickhouse don't have RBAC GRANTS or `access_management`, 2.5.0+ affected, fix [914](https://github.com/Altinity/clickhouse-backup/issues/914)

# v2.5.6
BUG FIXES
- skip `ValidateObjectDiskConfig` for `--diff-from-remote` when object disk doesn't contain data fix [910](https://github.com/Altinity/clickhouse-backup/issues/910)

# v2.5.5
IMPROVEMENTS
- added `object_disk_server_side_copy_concurrency` with default value `32`, to avoid slow `create` or `restore` backup process which was restricted by `upload_concurrency` or `download_concurrency` options, fix [903](https://github.com/Altinity/clickhouse-backup/issues/903)

BUG FIXES
- fixed `create --rbac` behavior when /var/lib/clickhouse/access not exists but present only `replicated` system.user_directories, fix [904](https://github.com/Altinity/clickhouse-backup/issues/904)

# v2.5.4
IMPROVEMENTS
- add `info` logging for `uploadObjectDiskParts` and `downloadObjectDiskParts` operation

# v2.5.3
BUG FIXES
- fixed `Unknown setting base_backup` for `use_embedded_backup_restore: true` and `create --diff-from-remote`, affected 2.5.0+ versions, fix [735](https://github.com/Altinity/clickhouse-backup/issues/735)

# v2.5.2
BUG FIXES
- fixed issue after [865](https://github.com/Altinity/clickhouse-backup/pull/865) we can't use `create_remote --diff-from-remote` for `remote_storage: custom`, affected versions 2.5.0, 2.5.1, fix [900](https://github.com/Altinity/clickhouse-backup/issue/900)

# v2.5.1
BUG FIXES
- fixed issue when set both `AWS_ROLE_ARN` and `S3_ASSUME_ROLE_ARN` then `S3_ASSUME_ROLE_ARN` shall have more priority than `AWS_ROLE_ARN` fix [898](https://github.com/Altinity/clickhouse-backup/issues/898)

# v2.5.0
IMPROVEMENTS
- complete removed support for legacy backups, created with version prior v1.0 
- removed `disable_progress_bar` config option and related progress bar code
- added `--delete-source` parameter for `upload` and `create_remote` commands to explicitly delete local backup during upload, fix [777](https://github.com/Altinity/clickhouse-backup/issues/777)
- added support for `--env ENV_NAME=value` cli parameter for allow dynamically overriding any config parameter, fix [821](https://github.com/Altinity/clickhouse-backup/issues/821)
- added support for `use_embedded_backup_restore: true` with empty `embedded_backup_disk` value, tested on S3/GCS over S3/AzureBlobStorage, fix [695](https://github.com/Altinity/clickhouse-backup/issues/695)
- `--rbac, --rbac-only, --configs, --configs-only` now works with `use_embedded_backup_restore: true`
- `--data` for `restore` with `use_embedded_backup_restore: true` will use `allow_non_empty_tables=true`, fix [756](https://github.com/Altinity/clickhouse-backup/issues/756)
- added `--diff-from-remote` parameter for `create` command, will copy only new data parts object disk data, also allows downloading properly object disk data from required backup during `restore`, fix [865](https://github.com/Altinity/clickhouse-backup/issues/865)
- added support of native Clickhouse incremental backup for `use_embedded_backup_restore: true` fix [735](https://github.com/Altinity/clickhouse-backup/issues/735)
- added `GCS_CHUNK_SIZE` config parameter, try to speed up GCS upload fix [874](https://github.com/Altinity/clickhouse-backup/pull/874), thanks @dermasmid
- added `--remote-backup` cli parameter to `tables` command and `GET /backup/table`, fix [778](https://github.com/Altinity/clickhouse-backup/issues/778)
- added `rbac_always_backup: true` option to default config, will create backup for RBAC objects automatically, restore still require `--rbac` to avoid destructive actions, fix [793](https://github.com/Altinity/clickhouse-backup/issues/793)
- added `rbac_conflict_resolution: recreate` option for RBAC object name conflicts during restore, fix [851](https://github.com/Altinity/clickhouse-backup/issues/851)
- added `upload_max_bytes_per_seconds` and `download_max_bytes_per_seconds` config options to allow throttling without CAP_SYS_NICE, fix [817](https://github.com/Altinity/clickhouse-backup/issues/817)
- added `clickhouse_backup_in_progress_commands` metric, fix [836](https://github.com/Altinity/clickhouse-backup/issues/836)
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
- fixed behavior for upload / download when .inner. table missing for `MATERIALIZED VIEW` by table pattern, fix [765](https://github.com/Altinity/clickhouse-backup/issues/765)
- fixed `ObjectDisks` + `CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE: true` - shall skip upload object disk content, fix [799](https://github.com/Altinity/clickhouse-backup/issues/799)
- fixed connection to clickhouse-server behavior when long clickhouse-server startup time and `docker-entrypoint.d` processing, will infinitely reconnect each 5 seconds, until success, fix [857](https://github.com/Altinity/clickhouse-backup/issues/857)
- fixed `USE_EMBEDDED_BACKUP_RESTORE=true` behavior to allow using backup disk with type `local`, fix [882](https://github.com/Altinity/clickhouse-backup/issues/882)
- fixed wrong list command behavior, it shall scann all `system.disks` path not only default disk to find partially created backups, fix [873](https://github.com/Altinity/clickhouse-backup/issues/873)
- fixed create `--rbac` behavior, don't create access folder if no RBAC objects are present
- fixed behavior when `system.disks` contains disk which not present in any `storage_policies`, fix [845](https://github.com/Altinity/clickhouse-backup/issues/845)

# v2.4.35
IMPROVEMENTS
- set part size for `s3:CopyObject` minimum 128Mb, look details https://repost.aws/questions/QUtW2_XaALTK63wv9XLSywiQ/s3-sync-command-is-slow-to-start-on-some-data

# v2.4.34
BUG FIXES
- fixed wrong behavior for CLICKHOUSE_SKIP_TABLES_ENGINES for engine=EngineName without parameters

# v2.4.33
BUG FIXES
- fixed wrong anonymous authorization for serviceAccount in GCS, added `GCS_SKIP_CREDENTIALS` fix [848](https://github.com/Altinity/clickhouse-backup/issues/848), fix [847](https://github.com/Altinity/clickhouse-backup/pull/847), thanks @sanadhis

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
- fixed `upload` command for S3 when object lock policy turned on, fix [829](https://github.com/Altinity/clickhouse-backup/issues/829)

# v2.4.29
IMPROVEMENTS
- added `AZBLOB_DEBUG` environment and `debug` config parameter in `azblob` section

BUG FIXES
- force set `RefCount` to 0 during `restore` for parts in S3/GCS over S3/Azure disks, for properly works DROP TABLE / DROP DATABASE
- use `os.Link` instead `os.Rename` for ClickHouse 21.4+, to properly create backup object disks
- ignore `frozen_metadata` during, create, upload, download and restore commands, fix [826](https://github.com/Altinity/clickhouse-backup/issues/826)
- `allow_parallel: true` doesn't work after execute list command, fix [827](https://github.com/Altinity/clickhouse-backup/issues/827)
- fixed corner cases, when disk has encrypted type and underlying disk is object storage 

# v2.4.28
IMPROVEMENT
- refactoring `watch` command, after https://github.com/Altinity/clickhouse-backup/pull/804
BUG FIXES
- fixed deletion for `object_disk_path` and `embedded` backups, after `upload` to properly respect `backups_to_keep_remote`

# v2.4.27
BUG FIXES
- fixed deletion for `object_disk_path` (all backups with S3, GCS over S3, AZBLOB disks from 2.4.0 to2.4.25 didn't properly delete their data from backup bucket)

# v2.4.26
IMPROVEMENTS
- improved re-balance disk during download if disk does not exist in `system.disks`. Use least used for `local` disks and `random` for object disks, fix [561](https://github.com/Altinity/clickhouse-backup/issues/561)

BUG FIXES
- fixed regression `check_parts_columns` for Enum types (2.4.24+), fix [823](https://github.com/Altinity/clickhouse-backup/issues/823)
- properly applying marcos to `object_disk_path` during `delete`

# v2.4.25
BUG FIXES
- fixed `--restore-table-mapping` corner cases for when a destination database contains special characters, fix [820](https://github.com/Altinity/clickhouse-backup/issues/820)

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
- change `go.mod` to allow `GO111MODULE=on go install github.com/Altinity/clickhouse-backup/v2/cmd/clickhouse-backup@latest`

# v2.4.19
BUG FIXES
- use single `s3:CopyObject` call instead `s3:CreateMultipartUpload+s3:UploadCopyPart+s3:CompleteMultipartUpload` for files with size less 5Gb

# v2.4.18
BUG FIXES
- removed `HeadObject` request to calculate source key size in `CopyObject`, to allow cross region S3 disks backup, fix [813](https://github.com/Altinity/clickhouse-backup/issues/813)
- make optional `/backup/kill` query parameter `command` and optional arguments for `kill` command handled via `/backup/actions`, if omitted then will kill first command in "In progress" status, fix [808](https://github.com/Altinity/clickhouse-backup/issues/808) 

# v2.4.17
BUG FIXES
- skip `CopyObject` execution for keys which have zero sizes, to allow properly backup `S3`, `GCS over S3` and `Azure` disks

# v2.4.16
BUG FIXES
- increased `AZBLOB_TIMEOUT` to 4h, instead 15m to allow downloading long size data parts 
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
- `--partitions=(v1,'v2')` could calculate wrong partition expression if `system.columns` will return fields in different order than they described in PARTITION BY clause, fix [791](https://github.com/Altinity/clickhouse-backup/issues/791)

# v2.4.6
IMPROVEMENTS
- make 'kopia' custom scripts really increment fix [781](https://github.com/Altinity/clickhouse-backup/issues/781) 
- added `force_http` and improve retries in GCS upload [784](https://github.com/Altinity/clickhouse-backup/pull/784), thanks @minguyen9988

BUG FIXES
- added `Array(Tuple())` to exclude list for `check_parts_columns:true`, fix [789](https://github.com/Altinity/clickhouse-backup/issues/789)
- fixed `delete remote` command for s3 buckets with enabled versioning, fix [782](https://github.com/Altinity/clickhouse-backup/issues/782)
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
- fixed applying macros to s3 endpoint in object disk during restoring embedded backups, fix [750](https://github.com/Altinity/clickhouse-backup/issues/750)
- rewrote `GCS` clients pool, set default `GCS_CLIENT_POOL_SIZE` as `max(upload_concurrency, download_concurrency) * 3` to avoid stuck, fix [753](https://github.com/Altinity/clickhouse-backup/pull/753), thanks @minguyen9988

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
- implementation blocklist for table engines during backup / download / upload / restore [537](https://github.com/Altinity/clickhouse-backup/issues/537)
- restore RBAC / configs, refactoring restart clickhouse-server via `sql:SYSTEM SHUTDOWN` or `exec:systemctl restart clickhouse-server`, add `--rbac-only` and `--configs-only` options to `create`, `upload`, `download`, `restore` command. fix [706]https://github.com/Altinity/clickhouse-backup/issues/706
- Backup/Restore RBAC related objects from Zookeeper via direct connection to zookeeper/keeper, fix [604](https://github.com/Altinity/clickhouse-backup/issues/604)
- added `SHARDED_OPERATION_MODE` option, to easily create backup for sharded cluster, available values `none` (no sharding), `table` (table granularity), `database` (database granularity), `first-replica` (on the lexicographically sorted first active replica), thanks @mskwon, fix [639](https://github.com/Altinity/clickhouse-backup/issues/639), fix [648](https://github.com/Altinity/clickhouse-backup/pull/648)
- added support for `compression_format: none` for upload and download backups created with `--rbac` / `--rbac-only` or `--configs` / `--configs-only` options, fix [713](https://github.com/Altinity/clickhouse-backup/issues/713)
- added support for s3 `GLACIER` storage class, when GET return error, then, it requires 5 minutes per key and restore could be slow. Use `GLACIER_IR`, it looks more robust, fix [614](https://github.com/Altinity/clickhouse-backup/issues/614)
- restore functions via `CREATE OR REPLACE` for more atomic behavior
- prepared to make `./tests/integration/` test parallel execution fix [721](https://github.com/Altinity/clickhouse-backup/issues/721)
- touch `/var/lib/clickhouse/flags/force_drop_table` before every DROP TABLE execution, fix [683](https://github.com/Altinity/clickhouse-backup/issues/683)
- added support connection pool for Google Cloud Storage, `GCS_CLIENT_POOL_SIZE`, fix [724](https://github.com/Altinity/clickhouse-backup/issues/724)

BUG FIXES
- fixed possible create backup failures during UNFREEZE not exists tables, affected 2.2.7+ version, fix [704](https://github.com/Altinity/clickhouse-backup/issues/704)
- fixed too strict `system.parts_columns` check when backup creates, exclude `Enum` and `Tuple (JSON)` and `Nullable(Type)` vs `Type` corner cases, fix [685](https://github.com/Altinity/clickhouse-backup/issues/685), fix [699](https://github.com/Altinity/clickhouse-backup/issues/699)  
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
- complete success/failure server callback notification even when the main context canceled, fix [680](https://github.com/Altinity/clickhouse-backup/pull/680)
- `clean` command will not return error when shadow directory not exists, fix [686](https://github.com/Altinity/clickhouse-backup/issues/686)

# v2.3.0
IMPROVEMENTS
- add FIPS compatible builds and examples, fix [656](https://github.com/Altinity/clickhouse-backup/issues/656), fix [674](https://github.com/Altinity/clickhouse-backup/issues/674)
- improve support for `use_embedded_backup_restore: true`, applied ugly workaround in test to avoid https://github.com/ClickHouse/ClickHouse/issues/43971, and applied restore workaround to resolve https://github.com/ClickHouse/ClickHouse/issues/42709
- migrate to `clickhouse-go/v2`, fix [540](https://github.com/Altinity/clickhouse-backup/issues/540), close [562](https://github.com/Altinity/clickhouse-backup/pull/562)
- add documentation for `AWS_ARN_ROLE` and `AWS_WEB_IDENTITY_TOKEN_FILE`, fix [563](https://github.com/Altinity/clickhouse-backup/issues/563)

BUG FIXES
- hotfix wrong empty files when disk_mapping contains don't exist during creation, affected 2.2.7 version, look details [676](https://github.com/Altinity/clickhouse-backup/issues/676#issue-1771732489) 
- add `FTP_ADDRESS` and `SFTP_PORT` in Default config Readme.md section fix [668](https://github.com/Altinity/clickhouse-backup/issues/668)
- when use `--tables=db.materialized_view` pattern, then create/restore backup also for `.inner.materialized_view` or `.inner_id.uuid`, fix [613](https://github.com/Altinity/clickhouse-backup/issues/613)

# v2.2.8
BUG FIXES
- hotfix wrong empty files when disk_mapping contains don't exist during creation, affected 2.2.7 version, look details [676](https://github.com/Altinity/clickhouse-backup/issues/676#issue-1771732489)

# v2.2.7
IMPROVEMENTS
- Auto-tuning concurrency and buffer size related parameters depending on the remote storage type, fix [658](https://github.com/Altinity/clickhouse-backup/issues/658)
- add `CLICKHOUSE_BACKUP_MUTATIONS` and `CLICKHOUSE_RESTORE_AS_ATTACH` config options to allow backup and properly restore table with `system.mutations` where is_done=0 status. fix [529](https://github.com/Altinity/clickhouse-backup/issues/529)
- add `CLICKHOUSE_CHECK_PARTS_COLUMNS` config option and `--skip-check-parts-column` CLI parameter to `watch`, `create` and `create_remote` commands to disallow backup with inconsistent column data types fix [529](https://github.com/Altinity/clickhouse-backup/issues/529)
- add test coverage reports for unit, testflows and integration tests, fix [644](https://github.com/Altinity/clickhouse-backup/issues/644)
- use UNFREEZE TABLE in ClickHouse after backup finished to allow s3 and other object storage disks to unlock and delete remote keys during merge, fix [423](https://github.com/Altinity/clickhouse-backup/issues/423)

BUG FIXES
- apply `SETTINGS check_table_dependencies=0` to `DROP DATABASE` statement, when pass `--ignore-dependencies` together with `--rm` in `restore` command, fix [651](https://github.com/Altinity/clickhouse-backup/issues/651)
- add support for masked secrets for ClickHouse 23.3+, fix [640](https://github.com/Altinity/clickhouse-backup/issues/640)

# v2.2.6
BUG FIXES
- fix panic for resume upload after restart API server for boolean parameters, fix [653](https://github.com/Altinity/clickhouse-backup/issues/653)
- apply `SETTINGS check_table_dependencies=0` to `DROP DATABASE` statement, when pass `--ignore-dependencies` together with `--rm` in `restore` command, fix [651](https://github.com/Altinity/clickhouse-backup/issues/651) 

# v2.2.5
BUG FIXES
- fix error after restart API server for boolean parameters, fix [646](https://github.com/Altinity/clickhouse-backup/issues/646)
- fix corner cases when `restore_schema_on_cluster: cluster`, fix [642](https://github.com/Altinity/clickhouse-backup/issues/642), error happens from 2.2.0 to 2.2.4
- fix `Makefile` targets `build-docker` and `build-race-docker` for old clickhouse-server version
- fix typo `retries_pause` config definition in a `general` config section

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
- add `system.macros` could be applied to `path` config section to ReadMe.md, fix [638](https://github.com/Altinity/clickhouse-backup/issues/638)
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
- added `S3_OBJECT_LABLES` and `GCS_OBJECT_LABELS` to allow setting each backup object metadata during upload fix [588](https://github.com/Altinity/clickhouse-backup/issues/588)
- added `clickhouse-keeper` as zookeeper replacement for integration test during reproducing [416](https://github.com/Altinity/clickhouse-backup/issues/416)
- decrease memory buffers for S3 and GCS, change default value for `upload_concurrency` and `download_concurrency` to `round(sqrt(MAX_CPU / 2))`, fix [539](https://github.com/Altinity/clickhouse-backup/issues/539)
- added ability to set up custom storage class for GCS and S3 depends on backupName pattern, fix [584](https://github.com/Altinity/clickhouse-backup/issues/584)

BUG FIXES
- fix ssh connection leak for SFTP remote storage, fix [578](https://github.com/Altinity/clickhouse-backup/issues/578)
- fix wrong Content-Type header, fix [605](https://github.com/Altinity/clickhouse-backup/issues/605)
- fix wrong behavior for `download` with `--partitions` fix [606](https://github.com/Altinity/clickhouse-backup/issues/606) 
- fix wrong size of backup in list command if upload or download was break and resume, fix [526](https://github.com/Altinity/clickhouse-backup/issues/526)
- fix `_successful_` and `_failed_` issue related to metrics counter, happens after 2.1.0, fix [589](https://github.com/Altinity/clickhouse-backup/issues/589)
- fix wrong calculation date of last remote backup during startup
- fix wrong duration, status for metrics after 2.1.0 refactoring, fix [599](https://github.com/Altinity/clickhouse-backup/issues/599)
- fix panic on `LIVE VIEW` tables with option `--restore-database-mapping db:db_new` enabled, thanks @php53unit

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
- fix `--restore-database-mapping` behavior for `ReplicatedMergeTree` (replace database name in a replication path) and `Distributed` (replace database name in underlying table) tables, fix [547](https://github.com/Altinity/clickhouse-backup/issues/547)
- `MaterializedPostgreSQL` doesn't support FREEZE, fix [550](https://github.com/Altinity/clickhouse-backup/issues/550), see also https://github.com/ClickHouse/ClickHouse/issues/32902, https://github.com/ClickHouse/ClickHouse/issues/44252
- `create` and `restore` commands will respect `skip_tables` config options and `--table` cli parameter, to avoid creating unnecessary empty databases, fix [583](https://github.com/Altinity/clickhouse-backup/issues/583)
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
- return `bash` and `clickhouse` user group to `Dockerfile` image short, fix [542](https://github.com/Altinity/clickhouse-backup/issues/542)

# v2.1.0
IMPROVEMENTS
- complex refactoring to use contexts, `AWS` and `SFTP` storage are not fully supported
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
- fix all commands was always return `200` status (expect errors) and ignore status which passed from application code in API server 

# v2.0.0
IMPROVEMENTS
- implements `remote_storage: custom`, which allow us to adopt any external backup system like `restic`, `kopia`, `rsync`, rclone etc. fix [383](https://github.com/Altinity/clickhouse-backup/issues/383)
- add example workflow how to make backup / restore on sharded cluster, fix [469](https://github.com/Altinity/clickhouse-backup/discussions/469)
- add `use_embedded_backup_restore` to allow `BACKUP` and `RESTORE` SQL commands usage, fix [323](https://github.com/Altinity/clickhouse-backup/issues/323), need 22.7+ and resolve https://github.com/ClickHouse/ClickHouse/issues/39416
- add `timeout` to `azure` config `AZBLOB_TIMEOUT` to allow download with bad network quality, fix [467](https://github.com/Altinity/clickhouse-backup/issues/467)
- switch to go 1.19
- refactoring to remove legacy `storage` package
- add `table` parameter to `tables` cli command and `/backup/tables` API handler, fix [367](https://github.com/Altinity/clickhouse-backup/issues/367)
- add `--resumable` parameter to `create_remote`, `upload`, `restore_remote`, `donwload` commands to allow resuming upload or download after break. Ignored for `remote_storage: custom`, fix [207](https://github.com/Altinity/clickhouse-backup/issues/207)
- add `--ignore-dependencies` parameter to `restore` and `restore_remote`, to allow drop object during restore schema on server where schema objects already exist and contain dependencies which not present in backup, fix [455](https://github.com/Altinity/clickhouse-backup/issues/455)
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
- try to improve implementation `check_replicas_before_attach` configuration to avoid concurrently ATTACH PART execution during `restore` command on multi-shard cluster, fix [474](https://github.com/Altinity/clickhouse-backup/issues/474)
- add `timeout` to `azure` config `AZBLOB_TIMEOUT` to allow download with bad network quality, fix [467](https://github.com/Altinity/clickhouse-backup/issues/467)

# v1.4.9
BUG FIXES
- fix `download` behavior for parts which contains special characters in name, fix [462](https://github.com/Altinity/clickhouse-backup/issues/462)

# v1.4.8
IMPROVEMENTS
- add `check_replicas_before_attach` configuration to avoid concurrent ATTACH PART execution during `restore` command on multi-shard cluster, fix [474](https://github.com/Altinity/clickhouse-backup/issues/474)
- allow a backup list when clickhouse server offline, fix [476](https://github.com/Altinity/clickhouse-backup/issues/476)
- add `use_custom_storage_class` (`S3_USE_CUSTOM_STORAGE_CLASS`) option to `s3` section, thanks @realwhite

BUG FIXES
- resolve `{uuid}` marcos during restore for `ReplicatedMergeTree` table and ClickHouse server 22.5+, fix [466](https://github.com/Altinity/clickhouse-backup/issues/466)

# v1.4.7
IMPROVEMENTS
- PROPERLY restore to default disk if disks are not found on destination clickhouse server, fix [457](https://github.com/Altinity/clickhouse-backup/issues/457)

# v1.4.6
BUG FIXES
- fix infinite loop `error can't acquire semaphore during Download: context canceled`, and `error can't acquire semaphore during Upload: context canceled` all 1.4.x users recommend upgrade to 1.4.6

# v1.4.5
IMPROVEMENTS
- add `CLICKHOUSE_FREEZE_BY_PART_WHERE` option which allow freezes by part with WHERE condition, thanks @vahid-sohrabloo 

# v1.4.4
IMPROVEMENTS
- download and restore to default disk if disks are not found on destination clickhouse server, fix [457](https://github.com/Altinity/clickhouse-backup/issues/457)

# v1.4.3
IMPROVEMENTS
- add `API_INTEGRATION_TABLES_HOST` option to allow using DNS name in integration tables system.backup_list, system.backup_actions

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
- fix [406](https://github.com/Altinity/clickhouse-backup/issues/406), properly handle `path` for S3, GCS for cases when it begins from "/"
- fix [409](https://github.com/Altinity/clickhouse-backup/issues/409), avoid delete partially uploaded backups via `backups_keep_remote` option
- fix [422](https://github.com/Altinity/clickhouse-backup/issues/422), avoid cache broken (partially uploaded) remote backup metadata.
- fix [404](https://github.com/Altinity/clickhouse-backup/issues/404), properly calculate S3_PART_SIZE to avoid freeze after 10000 multi parts uploading, properly handle error when upload and download go-routine failed to avoid pipe stuck 

# v1.3.1

IMPROVEMENTS
- fix [387](https://github.com/Altinity/clickhouse-backup/issues/387#issuecomment-1034648447), improve documentation related to memory and CPU usage

BUG FIXES
- fix [392](https://github.com/Altinity/clickhouse-backup/issues/392), correct download for a recursive sequence of diff backups when `DOWNLOAD_BY_PART` true
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
- Add a support ARM platform for Docker images and pre-compiled binary files, fix [#312](https://github.com/Altinity/clickhouse-backup/issues/312)
- KeepRemoteBackups should respect differential backups, fix [#111](https://github.com/Altinity/clickhouse-backup/issues/111)
- Add `SFTP_DEBUG` option, fix [#335](https://github.com/Altinity/clickhouse-backup/issues/335)
- Add the ability to restore schema `ON CLUSTER`, fix [#145](https://github.com/Altinity/clickhouse-backup/issues/145)
- Add support for encrypted disk (include s3 encrypted disks), fix [#260](https://github.com/Altinity/clickhouse-backup/issues/260)
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
- fix [#220](https://github.com/Altinity/clickhouse-backup/issues/220), properly handle total_bytes for `uint64` type
- fix [#304](https://github.com/Altinity/clickhouse-backup/issues/304), properly handle an archive extension during download instead of use config settings
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
- REST API `/backup/status` now return only the latest executed command with a status and error message

IMPROVEMENTS
- Added REST API `/backup/list/local` and `/backup/list/remote` to allow listing backup types separately
- Decreased background backup creation time via REST API `/backup/create`, during avoiding listing remote backups for update metrics value 
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
- fix [#268](https://github.com/Altinity/clickhouse-backup/issues/268), properly restore legacy backup for schema without a database name

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
- Decrease the number of SQL queries to `system.*` during backup commands
- Added options for RBAC and CONFIG backup, look to `clickhouse-backup help create` and `clickhouse-backup help restore` for details
- Add `S3_CONCURRENCY` option to speedup backup upload to `S3`
- Add `SFTP_CONCURRENCY` option to speedup backup upload to `SFTP`
- Add `AZBLOB_USE_MANAGED_IDENTITY` support for ManagedIdentity for azure remote storage, thanks https://github.com/roman-vynar
- Add clickhouse-operator kubernetes manifest which run `clickhouse-backup` in `server` mode on each clickhouse pod in kubernetes cluster
- Add detailed description and restrictions for incremental backups.
- Add `GCS_DEBUG` option
- Add `CLICKHOUSE_DEBUG` option to allow low-level debug for `clickhouse-go`

BUG FIXES
- fix [#266](https://github.com/Altinity/clickhouse-backup/discussions/266) properly restore a legacy backup format
- fix [#244](https://github.com/Altinity/clickhouse-backup/issues/244) add `read_timeout`, `write_timeout` to client-side timeout for `clickhouse-go` 
- fix [#255](https://github.com/Altinity/clickhouse-backup/issues/255) restrict connection pooling to 1 in `clickhouse-go` 
- fix [#256](https://github.com/Altinity/clickhouse-backup/issues/256) `remote_storage: none`, was broke compression
- fix [#266](https://github.com/Altinity/clickhouse-backup/discussions/266) legacy backups from version prior 1.0 can't restore without `allow_empty_backup: true`
- fix [#223](https://github.com/Altinity/clickhouse-backup/issues/223) backup only database metadata for proxy integrated database engines like MySQL, PostgresSQL
- fix `GCS` global buffer wrong usage during UPLOAD_CONCURRENCY > 1
- Remove unused `SKIP_SYNC_REPLICA_TIMEOUTS` option

# v1.0.0

BUG FIXES
- Fixed silent cancel uploading when table has more than 4k files, fix [#203](https://github.com/Altinity/clickhouse-backup/issues/203), [#163](https://github.com/Altinity/clickhouse-backup/issues/163). Thanks [mastertheknife](https://github.com/mastertheknife)
- Fixed download error for `zstd` and `brotli` compression formats
- Fixed bug when old-format backups hadn't cleared

# v1.0.0-beta2

IMPROVEMENTS

- Added diff backups
- Added retries to restore operation for resolve complex tables dependencies (Thanks [@Slach](https://github.com/Slach))
- Added SFTP remote storage (Thanks [@combin](https://github.com/combin))
- Now databases will be restored with the same engines (Thanks [@Slach](https://github.com/Slach))
- Added `create_remote` and `restore_remote` commands
- Changed of a compression format list. Added `zstd`, `brotli` and disabled `bzip2`, `sz`, `xz`

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
- Added a new backup format select it by `compression_format: none` option

BROKEN CHANGES
- Changed a backup format
- Incremental backup on remote storage feature is not supported now, but will be supported in future versions

# v0.6.4

IMPROVEMENTS
- Added `CLICKHOUSE_AUTO_CLEAN_SHADOW` option for cleaning shadow folder before backup. Enabled by default.
- Added `CLICKHOUSE_SYNC_REPLICATED_TABLES` option for sync replicated tables before backup. Enabled by default.
- Improved statuses of operations in server mode

BUG FIXES
- Fixed bug with semaphores in server mode

