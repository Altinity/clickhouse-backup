
# Altinity Backup for ClickHouse®

[![Build](https://github.com/Altinity/clickhouse-backup/actions/workflows/build.yaml/badge.svg?branch=master)](https://github.com/Altinity/clickhouse-backup/actions/workflows/build.yaml)
[![GoDoc](https://godoc.org/github.com/Altinity/clickhouse-backup?status.svg)](http://godoc.org/github.com/Altinity/clickhouse-backup)
[![Telegram](https://img.shields.io/badge/telegram-join%20chat-3796cd.svg)](https://t.me/clickhousebackup)
[![Docker Image](https://img.shields.io/docker/pulls/altinity/clickhouse-backup.svg)](https://hub.docker.com/r/altinity/clickhouse-backup)
[![Downloads](https://img.shields.io/github/downloads/Altinity/clickhouse-backup/total.svg)](http://github.com/Altinity/clickhouse-backup/releases)
[![Coverage Status](https://coveralls.io/repos/github/Altinity/clickhouse-backup/badge.svg)](https://coveralls.io/github/Altinity/clickhouse-backup)
<a href="https://altinity.com/slack">
  <img src="https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358" alt="AltinityDB Slack" />
</a>

A tool for easy backup and restore utility for ClickHouse databases with support for many cloud and non-cloud storage types.

### Don't run `clickhouse-backup` remotely
To backup data, `clickhouse-backup` requires access to the same files as `clickhouse-server` in `/var/lib/clickhouse` folders.
For that reason, it's required to run `clickhouse-backup` on the same host or same Kubernetes Pod or the neighbor container on the same host where `clickhouse-server` ran.
**WARNING** You can backup only schema when connect to remote `clickhouse-server` hosts.

## Features

- Easy creating and restoring backups of all or specific tables
- Efficient storing of multiple backups on the file system
- Uploading and downloading with streaming compression
- Works with AWS, GCS, Azure, Tencent COS, FTP, SFTP
- **Support for Atomic Database Engine**
- **Support for multi disks installations**
- **Support for custom remote storage types via `rclone`, `kopia`, `restic`, `rsync` etc**
- Support for incremental backups on remote storage

## Limitations

- ClickHouse above 1.1.54394 is supported
- Only MergeTree family tables engines (more table types for `clickhouse-server` 22.7+ and `USE_EMBEDDED_BACKUP_RESTORE=true`)

## Community

Altinity Backup for ClickHouse is a community effort sponsored by Altinity. The best way to reach us or ask questions is:

* Join the [Altinity Slack](https://altinity.com/slack) - Chat with the developers and other users
* Log an [issue on GitHub](https://github.com/Altinity/clickhouse-backup/issues) - Ask questions, log bugs and feature requests

## Support 

Altinity is the primary maintainer of clickhouse-backup. We offer a range of software and 
services related to ClickHouse. 

- [Official website](https://altinity.com/) - Get a high level overview of Altinity and our offerings.
- [Altinity.Cloud](https://altinity.com/cloud-database/) - Run ClickHouse in our cloud or yours.
- [Altinity Support](https://altinity.com/support/) - Get Enterprise-class support for ClickHouse.
- [Slack](https://altinity.com/slack) - Talk directly with ClickHouse users and Altinity devs.
- [Contact us](https://hubs.la/Q020sH3Z0) - Contact Altinity with your questions or issues.
- [Free consultation](https://hubs.la/Q020sHkv0) - Get a free consultation with a ClickHouse expert today.

## Installation

Download the latest binary from the [releases](https://github.com/Altinity/clickhouse-backup/releases) page and decompress with:

```shell
tar -zxvf clickhouse-backup.tar.gz
```

Use the official tiny Docker image and run it on a host with `clickhouse-server` installed:

```shell
docker run -u $(id -u clickhouse) --rm -it --network host -v "/var/lib/clickhouse:/var/lib/clickhouse" \
   -e CLICKHOUSE_PASSWORD="password" \
   -e S3_BUCKET="clickhouse-backup" \
   -e S3_ACCESS_KEY="access_key" \
   -e S3_SECRET_KEY="secret" \
   altinity/clickhouse-backup --help
```

Build from the sources (required go 1.21+):

```shell
GO111MODULE=on go install github.com/Altinity/clickhouse-backup/v2/cmd/clickhouse-backup@latest
```

## Brief description of how clickhouse-backup works

Data files are immutable in the `clickhouse-server`.
During a backup operation, `clickhouse-backup` creates file system hard links to existing `clickhouse-server` data parts via executing the `ALTER TABLE ... FREEZE` query.
During the restore operation, `clickhouse-backup` copies the hard links to the `detached` folder and executes the `ALTER TABLE ... ATTACH PART` query for each data part and each table in the backup.
A more detailed description is available here: https://www.youtube.com/watch?v=megsNh9Q-dw

## Default Config File

By default, the config file is located at `/etc/clickhouse-backup/config.yml`, but it can be redefined via the `CLICKHOUSE_BACKUP_CONFIG` environment variable or via `--config` command line parameter.
All options can be overwritten via environment variables.
Use `clickhouse-backup default-config` to print the default config.

## Configurable Parameters

Use `clickhouse-backup print-config` to print the current config.
Environment variables can override each config parameter defined in the config file. Their names should be UPPERCASE, and exact names are provided after the comment character `#.`
The following values are not defaults; they explain what each config parameter with an example.

```yaml
general:
  remote_storage: none           # REMOTE_STORAGE, choice from: `azblob`,`gcs`,`s3`, etc; if `none` then `upload` and `download` commands will fail.
  max_file_size: 1073741824      # MAX_FILE_SIZE, 1G by default, useless when upload_by_part is true, use to split data parts files by archives
  backups_to_keep_local: 0       # BACKUPS_TO_KEEP_LOCAL, how many latest local backup should be kept, 0 means all created backups will be stored on local disk
                                 # -1 means backup will keep after `create` but will delete after `create_remote` command
                                 # You can run `clickhouse-backup delete local <backup_name>` command to remove temporary backup files from the local disk
  backups_to_keep_remote: 0      # BACKUPS_TO_KEEP_REMOTE, how many latest backup should be kept on remote storage, 0 means all uploaded backups will be stored on remote storage.
                                 # If old backups are required for newer incremental backup then it won't be deleted. Be careful with long incremental backup sequences.
  log_level: info                # LOG_LEVEL, a choice from `debug`, `info`, `warning`, `error`
  allow_empty_backups: false     # ALLOW_EMPTY_BACKUPS
  # Concurrency means parallel tables and parallel parts inside tables
  # For example, 4 means max 4 parallel tables and 4 parallel parts inside one table, so equals 16 concurrent streams
  download_concurrency: 1        # DOWNLOAD_CONCURRENCY, max 255, by default, the value is round(sqrt(AVAILABLE_CPU_CORES / 2))
  upload_concurrency: 1          # UPLOAD_CONCURRENCY, max 255, by default, the value is round(sqrt(AVAILABLE_CPU_CORES / 2))
  
  # Throttling speed for upload and download, calculates on part level, not the socket level, it means short period for high traffic values and then time to sleep 
  download_max_bytes_per_second: 0  # DOWNLOAD_MAX_BYTES_PER_SECOND, 0 means no throttling 
  upload_max_bytes_per_second: 0    # UPLOAD_MAX_BYTES_PER_SECOND, 0 means no throttling
  
  # when table data contains in system.disks with type=ObjectStorage, then we need execute remote copy object in object storage service provider, this parameter can restrict how many files will copied in parallel  for each table 
  object_disk_server_side_copy_concurrency: 32
  # when CopyObject failure or object disk storage and backup destination have incompatible, will warning about possible high network traffic 
  allow_object_disk_streaming: false
  
  # RESTORE_SCHEMA_ON_CLUSTER, execute all schema related SQL queries with `ON CLUSTER` clause as Distributed DDL.
  # Check `system.clusters` table for the correct cluster name, also `system.macros` can be used.
  # This isn't applicable when `use_embedded_backup_restore: true`
  restore_schema_on_cluster: ""
  upload_by_part: true           # UPLOAD_BY_PART
  download_by_part: true         # DOWNLOAD_BY_PART
  use_resumable_state: true      # USE_RESUMABLE_STATE, allow resume upload and download according to the <backup_name>.resumable file. Resumable state is not supported for custom method in remote storage.

  # RESTORE_DATABASE_MAPPING, restore rules from backup databases to target databases, which is useful when changing destination database, all atomic tables will be created with new UUIDs.
  # The format for this env variable is "src_db1:target_db1,src_db2:target_db2". For YAML please continue using map syntax
  restore_database_mapping: {}

  # RESTORE_TABLE_MAPPING, restore rules from backup tables to target tables, which is useful when changing destination tables.
  # The format for this env variable is "src_table1:target_table1,src_table2:target_table2". For YAML please continue using map syntax
  restore_table_mapping: {}

  retries_on_failure: 3          # RETRIES_ON_FAILURE, how many times to retry after a failure during upload or download
  retries_pause: 5s              # RETRIES_PAUSE, duration time to pause after each download or upload failure

  watch_interval: 1h       # WATCH_INTERVAL, use only for `watch` command, backup will create every 1h
  full_interval: 24h       # FULL_INTERVAL, use only for `watch` command, full backup will create every 24h
  watch_backup_name_template: "shard{shard}-{type}-{time:20060102150405}" # WATCH_BACKUP_NAME_TEMPLATE, used only for `watch` command, macros values will apply from `system.macros` for time:XXX, look format in https://go.dev/src/time/format.go

  sharded_operation_mode: none       # SHARDED_OPERATION_MODE, how different replicas will shard backing up data for tables. Options are: none (no sharding), table (table granularity), database (database granularity), first-replica (on the lexicographically sorted first active replica). If left empty, then the "none" option will be set as default.
  
  cpu_nice_priority: 15    # CPU niceness priority, to allow throttling CPU intensive operation, more details https://manpages.ubuntu.com/manpages/xenial/man1/nice.1.html
  io_nice_priority: "idle" # IO niceness priority, to allow throttling DISK intensive operation, more details https://manpages.ubuntu.com/manpages/xenial/man1/ionice.1.html
  
  rbac_backup_always: true # always, backup RBAC objects
  rbac_resolve_conflicts: "recreate"  # action, when RBAC object with the same name already exists, allow "recreate", "ignore", "fail" values
clickhouse:
  username: default                # CLICKHOUSE_USERNAME
  password: ""                     # CLICKHOUSE_PASSWORD
  host: localhost                  # CLICKHOUSE_HOST, To make backup data `clickhouse-backup` requires access to the same file system as clickhouse-server, so `host` should localhost or address of another docker container on the same machine, or IP address bound to some network interface on the same host.
  port: 9000                       # CLICKHOUSE_PORT, don't use 8123, clickhouse-backup doesn't support HTTP protocol
  # CLICKHOUSE_DISK_MAPPING, use this mapping when your `system.disks` are different between the source and destination clusters during backup and restore process.
  # The format for this env variable is "disk_name1:disk_path1,disk_name2:disk_path2". For YAML please continue using map syntax.
  # If destination disk is different from source backup disk then you need to specify the destination disk in the config file:

  # disk_mapping:
  #  disk_destination: /var/lib/clickhouse/disks/destination
  
  # `disk_destination`  needs to be referenced in backup (source config), and all names from this map (`disk:path`) shall exist in `system.disks` on destination server.
  # During download of the backup from remote location (s3), if `name` is not present in `disk_mapping` (on the destination server config too) then `default` disk path will used for download.
  # `disk_mapping` is used to understand during download where downloaded parts shall be unpacked (which disk) on destination server and where to search for data parts directories during restore.
  disk_mapping: {}
  # CLICKHOUSE_SKIP_TABLES, the list of tables (pattern are allowed) which are ignored during backup and restore process
  # The format for this env variable is "pattern1,pattern2,pattern3". For YAML please continue using list syntax
  skip_tables:
    - system.*
    - INFORMATION_SCHEMA.*
    - information_schema.*
  # CLICKHOUSE_SKIP_TABLE_ENGINES, the list of tables engines which are ignored during backup, upload, download, restore process
  # The format for this env variable is "Engine1,Engine2,engine3". For YAML please continue using list syntax
  skip_table_engines: []
  # CLICKHOUSE_SKIP_DISKS, list of disk names which are ignored during create, upload, download and restore command
  # The format for this env variable is "Engine1,Engine2,engine3". For YAML please continue using list syntax
  skip_disks: []
  # CLICKHOUSE_SKIP_DISK_TYPES, list of disk types which are ignored during create, upload, download and restore command
  # The format for this env variable is "Engine1,Engine2,engine3". For YAML please continue using list syntax
  skip_disk_types: []
  timeout: 5m                  # CLICKHOUSE_TIMEOUT
  freeze_by_part: false        # CLICKHOUSE_FREEZE_BY_PART, allow freezing by part instead of freezing the whole table
  freeze_by_part_where: ""     # CLICKHOUSE_FREEZE_BY_PART_WHERE, allow parts filtering during freezing when freeze_by_part: true
  secure: false                # CLICKHOUSE_SECURE, use TLS encryption for connection
  skip_verify: false           # CLICKHOUSE_SKIP_VERIFY, skip certificate verification and allow potential certificate warnings
  sync_replicated_tables: true # CLICKHOUSE_SYNC_REPLICATED_TABLES
  tls_key: ""                  # CLICKHOUSE_TLS_KEY, filename with TLS key file
  tls_cert: ""                 # CLICKHOUSE_TLS_CERT, filename with TLS certificate file
  tls_ca: ""                   # CLICKHOUSE_TLS_CA, filename with TLS custom authority file
  log_sql_queries: true        # CLICKHOUSE_LOG_SQL_QUERIES, logging `clickhouse-backup` SQL queries on `info` level, when true, `debug` level when false
  debug: false                 # CLICKHOUSE_DEBUG
  config_dir:      "/etc/clickhouse-server"              # CLICKHOUSE_CONFIG_DIR
  # CLICKHOUSE_RESTART_COMMAND, use this command when restoring with --rbac, --rbac-only or --configs, --configs-only options
  # will split command by ; and execute one by one, all errors will logged and ignore
  # available prefixes
  # - sql: will execute SQL query
  # - exec: will execute command via shell
  restart_command: "exec:systemctl restart clickhouse-server" 
  ignore_not_exists_error_during_freeze: true # CLICKHOUSE_IGNORE_NOT_EXISTS_ERROR_DURING_FREEZE, helps to avoid backup failures when running frequent CREATE / DROP tables and databases during backup, `clickhouse-backup` will ignore `code: 60` and `code: 81` errors during execution of `ALTER TABLE ... FREEZE`
  check_replicas_before_attach: true # CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH, helps avoiding concurrent ATTACH PART execution when restoring ReplicatedMergeTree tables
  default_replica_path: "/clickhouse/tables/{cluster}/{shard}/{database}/{table}" # CLICKHOUSE_DEFAULT_REPLICA_PATH, will use during restore Replicated tables without macros in replication_path if replica already exists, to avoid restoring conflicts
  default_replica_name: "{replica}" # CLICKHOUSE_DEFAULT_REPLICA_NAME, will use during restore Replicated tables without macros in replica_name if replica already exists, to avoid restoring conflicts
  use_embedded_backup_restore: false # CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE, use BACKUP / RESTORE SQL statements instead of regular SQL queries to use features of modern ClickHouse server versions
  embedded_backup_disk: ""  # CLICKHOUSE_EMBEDDED_BACKUP_DISK - disk from system.disks which will use when `use_embedded_backup_restore: true` 
  backup_mutations: true # CLICKHOUSE_BACKUP_MUTATIONS, allow backup mutations from system.mutations WHERE is_done=0 and apply it during restore
  restore_as_attach: false # CLICKHOUSE_RESTORE_AS_ATTACH, allow restore tables which have inconsistent data parts structure and mutations in progress
  check_parts_columns: true # CLICKHOUSE_CHECK_PARTS_COLUMNS, check data types from system.parts_columns during create backup to guarantee mutation is complete
  max_connections: 0 # CLICKHOUSE_MAX_CONNECTIONS, how many parallel connections could be opened during operations
azblob:
  endpoint_suffix: "core.windows.net" # AZBLOB_ENDPOINT_SUFFIX
  account_name: ""               # AZBLOB_ACCOUNT_NAME
  account_key: ""                # AZBLOB_ACCOUNT_KEY
  sas: ""                        # AZBLOB_SAS
  use_managed_identity: false    # AZBLOB_USE_MANAGED_IDENTITY
  container: ""                  # AZBLOB_CONTAINER
  assume_container_exists: false # AZBLOB_ASSUME_CONTAINER_EXISTS, enables assignment of rights granting clickhouse-backup access only to blobs in the container
  path: ""                       # AZBLOB_PATH, `system.macros` values can be applied as {macro_name}
  object_disk_path: ""           # AZBLOB_OBJECT_DISK_PATH, path for backup of part from clickhouse object disks, if object disks present in clickhouse, then shall not be zero and shall not be prefixed by `path`
  compression_level: 1           # AZBLOB_COMPRESSION_LEVEL
  compression_format: tar        # AZBLOB_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  sse_key: ""                    # AZBLOB_SSE_KEY
  max_parts_count: 256           # AZBLOB_MAX_PARTS_COUNT, number of parts for AZBLOB uploads, for properly calculate buffer size
  max_buffers: 3                 # AZBLOB_MAX_BUFFERS, similar with S3_CONCURRENCY
  debug: false                   # AZBLOB_DEBUG
s3:
  access_key: ""                   # S3_ACCESS_KEY
  secret_key: ""                   # S3_SECRET_KEY
  bucket: ""                       # S3_BUCKET
  endpoint: ""                     # S3_ENDPOINT
  region: us-east-1                # S3_REGION
  # AWS changed S3 defaults in April 2023 so that all new buckets have ACL disabled: https://aws.amazon.com/blogs/aws/heads-up-amazon-s3-security-changes-are-coming-in-april-of-2023/
  # They also recommend that ACLs are disabled: https://docs.aws.amazon.com/AmazonS3/latest/userguide/ensure-object-ownership.html
  # use `acl: ""` if you see "api error AccessControlListNotSupported: The bucket does not allow ACLs"
  acl: private                     # S3_ACL 
  assume_role_arn: ""              # S3_ASSUME_ROLE_ARN
  force_path_style: false          # S3_FORCE_PATH_STYLE
  path: ""                         # S3_PATH, `system.macros` values can be applied as {macro_name}
  object_disk_path: ""             # S3_OBJECT_DISK_PATH, path for backup of part from clickhouse object disks, if object disks present in clickhouse, then shall not be zero and shall not be prefixed by `path`
  disable_ssl: false               # S3_DISABLE_SSL
  compression_level: 1             # S3_COMPRESSION_LEVEL
  compression_format: tar          # S3_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  # look at details in https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html
  sse: ""                          # S3_SSE, empty (default), AES256, or aws:kms
  sse_customer_algorithm: ""       # S3_SSE_CUSTOMER_ALGORITHM, encryption algorithm, for example, AES256
  sse_customer_key: ""             # S3_SSE_CUSTOMER_KEY, customer-provided encryption key use `openssl rand 32 > aws_sse.key` and `cat aws_sse.key | base64` 
  sse_customer_key_md5: ""         # S3_SSE_CUSTOMER_KEY_MD5, 128-bit MD5 digest of the encryption key according to RFC 1321 use `cat aws_sse.key |  openssl dgst -md5 -binary | base64`
  sse_kms_key_id: ""               # S3_SSE_KMS_KEY_ID, if S3_SSE is aws:kms then specifies the ID of the Amazon Web Services Key Management Service
  sse_kms_encryption_context: ""   # S3_SSE_KMS_ENCRYPTION_CONTEXT, base64-encoded UTF-8 string holding a JSON with the encryption context
                                   # Specifies the Amazon Web Services KMS Encryption Context to use for object encryption.
                                   # This is a collection of non-secret key-value pairs that represent additional authenticated data.
                                   # When you use an encryption context to encrypt data, you must specify the same (an exact case-sensitive match)
                                   # encryption context to decrypt the data. An encryption context is supported only on operations with symmetric encryption KMS keys
  disable_cert_verification: false # S3_DISABLE_CERT_VERIFICATION
  use_custom_storage_class: false  # S3_USE_CUSTOM_STORAGE_CLASS
  storage_class: STANDARD          # S3_STORAGE_CLASS, by default allow only from list https://github.com/aws/aws-sdk-go-v2/blob/main/service/s3/types/enums.go#L787-L799
  concurrency: 1                   # S3_CONCURRENCY
  max_parts_count: 4000            # S3_MAX_PARTS_COUNT, number of parts for S3 multipart uploads
  allow_multipart_download: false  # S3_ALLOW_MULTIPART_DOWNLOAD, allow faster multipart download speed, but will require additional disk space, download_concurrency * part size in worst case
  checksum_algorithm: ""           # S3_CHECKSUM_ALGORITHM, use it when you use object lock which allow to avoid delete keys from bucket until some timeout after creation, use CRC32 as fastest

  # S3_OBJECT_LABELS, allow setup metadata for each object during upload, use {macro_name} from system.macros and {backupName} for current backup name
  # The format for this env variable is "key1:value1,key2:value2". For YAML please continue using map syntax
  object_labels: {}
  # S3_CUSTOM_STORAGE_CLASS_MAP, allow setup storage class depending on the backup name regexp pattern, format nameRegexp > className
  custom_storage_class_map: {}
  # S3_REQUEST_PAYER, define who will pay to request, look https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html for details, possible values requester, if empty then bucket owner
  request_payer: ""
  debug: false                     # S3_DEBUG
gcs:
  credentials_file: ""         # GCS_CREDENTIALS_FILE
  credentials_json: ""         # GCS_CREDENTIALS_JSON
  credentials_json_encoded: "" # GCS_CREDENTIALS_JSON_ENCODED
  # look https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create how to get HMAC keys for access to bucket
  embedded_access_key: ""      # GCS_EMBEDDED_ACCESS_KEY, use it when `use_embedded_backup_restore: true`, `embedded_backup_disk: ""`, `remote_storage: gcs`
  embedded_secret_key: ""      # GCS_EMBEDDED_SECRET_KEY, use it when `use_embedded_backup_restore: true`, `embedded_backup_disk: ""`, `remote_storage: gcs`
  skip_credentials: false      # GCS_SKIP_CREDENTIALS, skip add credentials to requests to allow anonymous access to bucket
  endpoint: ""                 # GCS_ENDPOINT, use it for custom GCS endpoint/compatible storage. For example, when using custom endpoint via private service connect
  bucket: ""                   # GCS_BUCKET
  path: ""                     # GCS_PATH, `system.macros` values can be applied as {macro_name}
  object_disk_path: ""         # GCS_OBJECT_DISK_PATH, path for backup of part from clickhouse object disks, if object disks present in clickhouse, then shall not be zero and shall not be prefixed by `path`
  compression_level: 1         # GCS_COMPRESSION_LEVEL
  compression_format: tar      # GCS_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  storage_class: STANDARD      # GCS_STORAGE_CLASS
  chunk_size: 0                # GCS_CHUNK_SIZE, default 16 * 1024 * 1024 (16MB)
  client_pool_size: 500        # GCS_CLIENT_POOL_SIZE, default max(upload_concurrency, download concurrency) * 3, should be at least 3 times bigger than `UPLOAD_CONCURRENCY` or `DOWNLOAD_CONCURRENCY` in each upload and download case to avoid stuck
  # GCS_OBJECT_LABELS, allow setup metadata for each object during upload, use {macro_name} from system.macros and {backupName} for current backup name
  # The format for this env variable is "key1:value1,key2:value2". For YAML please continue using map syntax
  object_labels: {}
  # GCS_CUSTOM_STORAGE_CLASS_MAP, allow setup storage class depends on backup name regexp pattern, format nameRegexp > className
  custom_storage_class_map: {}
  debug: false                 # GCS_DEBUG
  force_http: false            # GCS_FORCE_HTTP
cos:
  url: ""                      # COS_URL
  timeout: 2m                  # COS_TIMEOUT
  secret_id: ""                # COS_SECRET_ID
  secret_key: ""               # COS_SECRET_KEY
  path: ""                     # COS_PATH, `system.macros` values can be applied as {macro_name}
  object_disk_path: ""         # GOS_OBJECT_DISK_PATH, path for backup of part from clickhouse object disks, if object disks present in clickhouse, then shall not be zero and shall not be prefixed by `path`
  compression_format: tar      # COS_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  compression_level: 1         # COS_COMPRESSION_LEVEL
ftp:
  address: ""                  # FTP_ADDRESS in format `host:port`
  timeout: 2m                  # FTP_TIMEOUT
  username: ""                 # FTP_USERNAME
  password: ""                 # FTP_PASSWORD
  tls: false                   # FTP_TLS
  tls_skip_verify: false       # FTP_TLS_SKIP_VERIFY
  path: ""                     # FTP_PATH, `system.macros` values can be applied as {macro_name}
  object_disk_path: ""         # FTP_OBJECT_DISK_PATH, path for backup of part from clickhouse object disks, if object disks present in clickhouse, then shall not be zero and shall not be prefixed by `path`
  compression_format: tar      # FTP_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  compression_level: 1         # FTP_COMPRESSION_LEVEL
  debug: false                 # FTP_DEBUG
sftp:
  address: ""                  # SFTP_ADDRESS
  username: ""                 # SFTP_USERNAME
  password: ""                 # SFTP_PASSWORD
  port: 22                     # SFTP_PORT
  key: ""                      # SFTP_KEY
  path: ""                     # SFTP_PATH, `system.macros` values can be applied as {macro_name}
  object_disk_path: ""         # SFTP_OBJECT_DISK_PATH, path for backup of part from clickhouse object disks, if object disks present in clickhouse, then shall not be zero and shall not be prefixed by `path`
  concurrency: 1               # SFTP_CONCURRENCY
  compression_format: tar      # SFTP_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  compression_level: 1         # SFTP_COMPRESSION_LEVEL
  debug: false                 # SFTP_DEBUG
custom:
  upload_command: ""           # CUSTOM_UPLOAD_COMMAND
  download_command: ""         # CUSTOM_DOWNLOAD_COMMAND
  delete_command: ""           # CUSTOM_DELETE_COMMAND
  list_command: ""             # CUSTOM_LIST_COMMAND
  command_timeout: "4h"        # CUSTOM_COMMAND_TIMEOUT
api:
  listen: "localhost:7171"     # API_LISTEN
  enable_metrics: true         # API_ENABLE_METRICS
  enable_pprof: false          # API_ENABLE_PPROF
  username: ""                 # API_USERNAME, basic authorization for API endpoint
  password: ""                 # API_PASSWORD
  secure: false                # API_SECURE, use TLS for listen API socket
  ca_cert_file: ""             # API_CA_CERT_FILE
                               # openssl genrsa -out /etc/clickhouse-backup/ca-key.pem 4096
                               # openssl req -subj "/O=altinity" -x509 -new -nodes -key /etc/clickhouse-backup/ca-key.pem -sha256 -days 365 -out /etc/clickhouse-backup/ca-cert.pem
  private_key_file: ""         # API_PRIVATE_KEY_FILE, openssl genrsa -out /etc/clickhouse-backup/server-key.pem 4096
  certificate_file: ""         # API_CERTIFICATE_FILE,
                               # openssl req -subj "/CN=localhost" -addext "subjectAltName = DNS:localhost,DNS:*.cluster.local" -new -key /etc/clickhouse-backup/server-key.pem -out /etc/clickhouse-backup/server-req.csr
                               # openssl x509 -req -days 365000 -extensions SAN -extfile <(printf "\n[SAN]\nsubjectAltName=DNS:localhost,DNS:*.cluster.local") -in /etc/clickhouse-backup/server-req.csr -out /etc/clickhouse-backup/server-cert.pem -CA /etc/clickhouse-backup/ca-cert.pem -CAkey /etc/clickhouse-backup/ca-key.pem -CAcreateserial
  integration_tables_host: ""  # API_INTEGRATION_TABLES_HOST, allow using DNS name to connect in `system.backup_list` and `system.backup_actions`
  allow_parallel: false        # API_ALLOW_PARALLEL, enable parallel operations, this allows for significant memory allocation and spawns go-routines, don't enable it if you are not sure
  create_integration_tables: false # API_CREATE_INTEGRATION_TABLES, create `system.backup_list` and `system.backup_actions`
  complete_resumable_after_restart: true # API_COMPLETE_RESUMABLE_AFTER_RESTART, after API server startup, if `/var/lib/clickhouse/backup/*/(upload|download).state2` present, then operation will continue in the background
  watch_is_main_process: false # WATCH_IS_MAIN_PROCESS, treats 'watch' command as a main api process, if it is stopped unexpectedly, api server is also stopped. Does not stop api server if 'watch' command canceled by the user. 

```

## Concurrency, CPU and Memory usage recommendation

`upload_concurrency` and `download_concurrency` define how many parallel download / upload go-routines will start independently of the remote storage type.
In 1.3.0+ it means how many parallel data parts will be uploaded, assuming `upload_by_part` and `download_by_part` are `true` (which is the default value).

`concurrency` in the `s3` section means how many concurrent `upload` streams will run during multipart upload in each upload go-routine.
A high value for `S3_CONCURRENCY` will allocate more memory for buffers inside the AWS golang SDK.

`concurrency` in the `sftp` section means how many concurrent request will be used for `upload` and `download` for each file.

For `compression_format`, a good default is `tar`, which uses less CPU. In most cases the data in clickhouse is already compressed, so you may not get a lot of space savings when compressing already-compressed data.

## remote_storage: custom

All custom commands use the go-template language. For example, you can use `{{ .cfg.* }}` `{{ .backupName }}` `{{ .diffFromRemote }}`.
A custom `list_command` returns JSON which is compatible with the `metadata.BackupMetadata` type with [JSONEachRow](https://clickhouse.com/docs/en/interfaces/formats/#jsoneachrow) format.
For examples, see [restic](https://github.com/Altinity/clickhouse-backup/tree/master/test/integration/restic/), [rsync](https://github.com/Altinity/clickhouse-backup/tree/master/test/integration/rsync/) and [kopia](https://github.com/Altinity/clickhouse-backup/tree/master/test/integration/kopia/). Feel free to add yours custom storage.

## ATTENTION!

**Never change file permissions in `/var/lib/clickhouse/backup`.**
This path contains hard links. Permissions on all hard links to the same data on disk are always identical.
That means that if you change the permissions/owner/attributes on a hard link in backup path, permissions on files with which ClickHouse works will be changed too.
That can lead to data corruption.

## API

Use the `clickhouse-backup server` command to run as a REST API server. In general, the API attempts to mirror the CLI commands.

### GET /

List all current applicable HTTP routes

### POST /

### POST /restart

Restart HTTP server, close all current connections, close listen socket, open listen socket again, all background go-routines breaks with contexts

### POST /backup/kill

Kill selected command from `GET /backup/actions` command list, kill process should be near immediate, but some go-routines (upload one data part) could continue to run.

- Optional query argument `command` may contain the command name to kill, or if it is omitted then kill the first "in progress" command.

### GET /backup/tables

Print list of tables: `curl -s localhost:7171/backup/tables | jq .`, exclude pattern matched tables from `skip_tables` configuration parameters

- Optional query argument `table` works the same as the `--table=pattern` CLI argument.
- Optional query argument `remote_backup` or `remote-backup` works the same as `--remote-backup=name` CLI argument.

### GET /backup/tables/all

Print list of tables: `curl -s localhost:7171/backup/tables/all | jq .`, ignore `skip_tables` configuration parameters.

- Optional query argument `table` works the same as the `--table=pattern` CLI argument.
- Optional query argument `remote_backup`or `remote-backup` works the same as `--remote-backup=name` CLI argument.

### POST /backup/create

Create new backup: `curl -s localhost:7171/backup/create -X POST | jq .`

- Optional string query argument `table` works the same as the `--table=pattern` CLI argument.
- Optional string query argument `partitions` works the same as the `--partitions=value` CLI argument.
- Optional string query argument `diff-from-remote` or `diff_from_remote` works the same as the `--diff-from-remote=backup_name` CLI argument (will calculate increment for object disks).
- Optional string query argument `name` works the same as specifying a backup name with the CLI.
- Optional boolean query argument `schema` works the same as the `--schema` CLI argument (backup schema only).
- Optional boolean query argument `rbac` works the same as the `--rbac` CLI argument (backup RBAC).
- Optional boolean query argument `rbac-only` or `rbac_only` works the same as the `--rbac-only` CLI argument (backup only RBAC).
- Optional boolean query argument `configs` works the same as the `--configs` CLI argument (backup configs).
- Optional boolean query argument `configs-only` or `configs_only` works the same as the `--configs-only` CLI argument (backup only configs).
- Optional boolean query argument `skip-check-parts-columns` or `skip_check_parts_columns` works the same as the `--skip-check-parts-columns` CLI argument (allow backup inconsistent column types for data parts).
- Optional boolean query argument `resume` works the same as the `--resume` CLI argument (resume upload for object disk data).
- Optional string query argument `callback` allow pass callback URL which will call with POST with `application/json` with payload `{"status":"error|success","error":"not empty when error happens", "operation_id" : "<random_uuid>"}`.

Additional example: `curl -s 'localhost:7171/backup/create?table=default.billing&name=billing_test' -X POST`

Note: this operation is asynchronous, so the API will return once the operation has started.

### POST /backup/watch

Run background watch process and create full+incremental backups sequence: `curl -s localhost:7171/backup/watch -X POST | jq .`
You can't run watch twice with the same parameters even when `allow_parallel: true`

- Optional string query argument `watch_interval` or `watch-interval` works the same as the `--watch-interval value` CLI argument.
- Optional string query argument `full_interval` or `full-interval` works the same as the `--full-interval value` CLI argument.
- Optional string query argument `watch_backup_name_template` or `watch-backup-name-template` works the same as the `--watch-backup-name-template value` CLI argument.
- Optional string query argument `table` works the same as the `--table value` CLI argument (backup only selected tables).
- Optional string query argument `partitions` works the same as the `--partitions value` CLI argument (backup only selected partitions).
- Optional boolean query argument `schema` works the same as the `--schema` CLI argument (backup schema only).
- Optional boolean query argument `rbac` works the same as the `--rbac` CLI argument (backup RBAC).
- Optional boolean query argument `configs` works the same as the `--configs` CLI argument (backup configs).
- Optional boolean query argument `skip-check-parts-columns` or `skip_check_parts_columns` works the same as the `--skip-check-parts-columns` CLI argument (allow backup inconsistent column types for data parts).
- Optional boolean query argument `delete-source` or `delete_source` works the same as the `--delete-source` CLI argument (delete source files during upload backup).
- Additional example: `curl -s 'localhost:7171/backup/watch?table=default.billing&watch_interval=1h&full_interval=24h' -X POST`

Note: this operation is asynchronous and can only be stopped with `kill -s SIGHUP $(pgrep -f clickhouse-backup)` or call `/restart`, `/backup/kill`. The API will return immediately once the operation has started.

### POST /backup/clean

Clean the `shadow` folders using all available paths from `system.disks`

### POST /backup/clean/remote_broken

Remove
Note: this operation is sync, and could take a lot of time, increase http timeouts during call

### POST /backup/upload

Upload backup to remote storage: `curl -s localhost:7171/backup/upload/<BACKUP_NAME> -X POST | jq .`

- Optional boolean query argument `delete-source` or `delete_source` works the same as the `--delete-source` CLI argument.
- Optional string query argument `diff-from` or `diff_from` works the same as the `--diff-from` CLI argument.
- Optional string query argument `diff-from-remote` or `diff_from_remote` works the same as the `--diff-from-remote` CLI argument.
- Optional string query argument `table` works the same as the `--table value` CLI argument.
- Optional string query argument `partitions` works the same as the `--partitions value` CLI argument.
- Optional boolean query argument `schema` works the same as the `--schema` CLI argument (upload schema only).
- Optional boolean query argument `rbac-only` works the same as the `--rbac-only` CLI argument (upload rbac only).
- Optional boolean query argument `configs-only` works the same as the `--configs-only` CLI argument (upload configs
  only).
- Optional boolean query argument `resumable` works the same as the `--resumable` CLI argument (save intermediate upload state and resume upload if data already exists on remote storage).
- Optional string query argument `callback` allow pass callback URL which will call with POST with `application/json` with payload `{"status":"error|success","error":"not empty when error happens", "operation_id" : "<random_uuid>"}`.

Note: this operation is asynchronous, so the API will return once the operation has started.

### GET /backup/list/{where}

Print a list of backups: `curl -s localhost:7171/backup/list | jq .`
Print a list of only local backups: `curl -s localhost:7171/backup/list/local | jq .`
Print a list of only remote backups: `curl -s localhost:7171/backup/list/remote | jq .`

Note: The `Size` field will not be set for the local backups that have just been created or are in progress.
Note: The `Size` field will not be set for the remote backups with upload status in progress.

### POST /backup/download

Download backup from remote storage: `curl -s localhost:7171/backup/download/<BACKUP_NAME> -X POST | jq .`

- Optional string query argument `table` works the same as the `--table value` CLI argument.
- Optional string query argument `partitions` works the same as the `--partitions value` CLI argument.
- Optional boolean query argument `schema` works the same as the `--schema` CLI argument (download schema only).
- Optional boolean query argument `rbac-only` works the same as the `--rbac-only` CLI argument (download rbac only).
- Optional boolean query argument `configs-only` works the same as the `--configs-only` CLI argument (download configs
  only).
- Optional boolean query argument `resumable` works the same as the `--resumable` CLI argument (save intermediate download state and resume download if it already exists on local storage).
- Optional string query argument `callback` allow pass callback URL which will call with POST with `application/json` with payload `{"status":"error|success","error":"not empty when error happens", "operation_id" : "<random_uuid>"}`.

Note: this operation is asynchronous, so the API will return once the operation has started.

### POST /backup/restore

Create schema and restore data from backup: `curl -s localhost:7171/backup/restore/<BACKUP_NAME> -X POST | jq .`

- Optional string query argument `table` works the same as the `--table value` CLI argument.
- Optional string query argument `partitions` works the same as the `--partitions value` CLI argument.
- Optional boolean query argument `schema` works the same as the `--schema` CLI argument (restore schema only).
- Optional boolean query argument `data` works the same as the `--data` CLI argument (restore data only).
- Optional boolean query argument `rm` works the same as the `--rm` CLI argument (drop tables before restore).
- Optional boolean query argument `ignore_dependencies` or `ignore-dependencies` works the as same the `--ignore-dependencies` CLI argument.
- Optional boolean query argument `rbac` works the same as the `--rbac` CLI argument (restore RBAC).
- Optional boolean query argument `rbac-only` works the same as the `--rbac` CLI argument (restore only RBAC).
- Optional boolean query argument `configs` works the same as the `--configs` CLI argument (restore configs).
- Optional boolean query argument `configs-only` works the same as the `--configs-only` CLI argument (restore configs).
- Optional string query argument `restore_database_mapping` or `restore-database-mapping` works the same as the `--restore-database-mapping=old_db:new_db` CLI argument.
- Optional string query argument `restore_table_mapping` or `restore-table-mapping` works the same as the `--restore-table-mapping=old_table:new_table` CLI argument.
- Optional string query argument `restore_schema_as_attach` or `restore-schema-as-attach` works the same as the `--restore-schema-as-attach` CLI argument.
- Optional boolean query argument `resume` works the same as the `--resume` CLI argument (resume download for object disk data).
- Optional string query argument `callback` allow pass callback URL which will call with POST with `application/json` with payload `{"status":"error|success","error":"not empty when error happens", "operation_id" : "<random_uuid>"}`.

### POST /backup/delete

Delete specific remote backup: `curl -s localhost:7171/backup/delete/remote/<BACKUP_NAME> -X POST | jq .`

Delete specific local backup: `curl -s localhost:7171/backup/delete/local/<BACKUP_NAME> -X POST | jq .`

### GET /backup/status

Display list of currently running asynchronous operations: `curl -s localhost:7171/backup/status | jq .`
Or latest command result if no backup operations executed.

### POST /backup/actions

Execute multiple backup actions: `curl -X POST -d '{"command":"create test_backup"}' -s localhost:7171/backup/actions`
You could pass multi line json each row in POST body
Will return result for each command as separate json string in each line.

### GET /backup/actions

Display a list of all operations from start of API server: `curl -s localhost:7171/backup/actions | jq .`

- Optional string query argument `filter` to filter actions on server side.
- Optional string query argument `last` to show only the last `N` actions.

## Examples

- [Simple cron script for daily backups and remote upload](Examples.md#simple-cron-script-for-daily-backups-and-remote-upload)
- [How to convert MergeTree to ReplicatedMergeTree](Examples.md#how-to-convert-mergetree-to-replicatedmergetree)
- [How to store backups on NFS or another server](Examples.md#how-to-store-backups-on-nfs-backup-drive-or-another-server-via-sftp)
- [How to move data to another clickhouse server](Examples.md#how-to-move-data-to-another-clickhouse-server)
- [How to monitor that backups created and uploaded correctly](Examples.md#how-to-monitor-that-backups-were-created-and-uploaded-correctly)
- [How to back up / restore a sharded cluster](Examples.md#how-to-back-up--restore-a-sharded-cluster)
- [How to back up a sharded cluster with Ansible](Examples.md#how-to-back-up-a-sharded-cluster-with-ansible)
- [How to back up a database with several terabytes of data](Examples.md#how-to-back-up-a-database-with-several-terabytes-of-data)
- [How to use clickhouse-backup in Kubernetes](Examples.md#how-to-use-clickhouse-backup-in-kubernetes)
- [How to back up object disks to s3 with s3:CopyObject](Examples.md#how-to-back-up-object-disks-to-s3-with-s3copyobject)
- [How to restore object disks to s3 with s3:CopyObject](Examples.md#how-to-restore-object-disks-to-s3-with-s3copyobject)
- [How to use AWS IRSA and IAM to allow S3 backup without Explicit credentials](Examples.md#how-to-use-aws-irsa-and-iam-to-allow-s3-backup-without-explicit-credentials)
- [How to do incremental backups work to remote storage](Examples.md#how-incremental-backups-work-with-remote-storage)
- [How to watch backups work](Examples.md#how-to-watch-backups-work)

## Original Author
Altinity wants to thank [@AlexAkulov](https://github.com/AlexAkulov) for creating this tool and for his valuable contributions.

## Common CLI Usage

### CLI command - tables
```
NAME:
   clickhouse-backup tables - List of tables, exclude skip_tables

USAGE:
   clickhouse-backup tables [--tables=<db>.<table>] [--remote-backup=<backup-name>] [--all]

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   --all, -a                                  Print table even when match with skip_tables pattern
   --table value, --tables value, -t value    List tables only match with table name patterns, separated by comma, allow ? and * as wildcard
   --remote-backup value                      List tables from remote backup
   
```
### CLI command - create
```
NAME:
   clickhouse-backup create - Create new backup

USAGE:
   clickhouse-backup create [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [--diff-from-remote=<backup-name>] [-s, --schema] [--rbac] [--configs] [--skip-check-parts-columns] [--resume] <backup_name>

DESCRIPTION:
   Create new backup

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   --table value, --tables value, -t value    Create backup only matched with table name patterns, separated by comma, allow ? and * as wildcard
   --diff-from-remote value                   Create incremental embedded backup or upload incremental object disk data based on other remote backup name
   --partitions partition_id                  Create backup only for selected partition names, separated by comma
If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
Values depends on field types in your table, use single quotes for String and Date/DateTime related types
Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, -s                                                                               Backup schemas only, will skip data
   --rbac, --backup-rbac, --do-backup-rbac                                                    Backup RBAC related objects
   --configs, --backup-configs, --do-backup-configs                                           Backup 'clickhouse-server' configuration files
   --rbac-only                                                                                Backup RBAC related objects only, will skip backup data, will backup schema only if --schema added
   --configs-only                                                                             Backup 'clickhouse-server' configuration files only, will skip backup data, will backup schema only if --schema added
   --skip-check-parts-columns                                                                 Skip check system.parts_columns to allow backup inconsistent column types for data parts
   --skip-projections db_pattern.table_pattern:projections_pattern                            Skip make hardlinks to *.proj/* files during backup creation, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --resume use_embedded_backup_restore: true, --resumable use_embedded_backup_restore: true  Will resume upload for object disk data, hard links on local disk still continue to recreate, not work when use_embedded_backup_restore: true
   
```
### CLI command - create_remote
```
NAME:
   clickhouse-backup create_remote - Create and upload new backup

USAGE:
   clickhouse-backup create_remote [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [--diff-from=<local_backup_name>] [--diff-from-remote=<local_backup_name>] [--schema] [--rbac] [--configs] [--resumable] [--skip-check-parts-columns] <backup_name>

DESCRIPTION:
   Create and upload

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   --table value, --tables value, -t value    Create and upload backup only matched with table name patterns, separated by comma, allow ? and * as wildcard
   --partitions partition_id                  Create and upload backup only for selected partition names, separated by comma
If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
Values depends on field types in your table, use single quotes for String and Date/DateTime related types
Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --diff-from value                                                Local backup name which used to upload current backup as incremental
   --diff-from-remote value                                         Remote backup name which used to upload current backup as incremental
   --schema, -s                                                     Backup and upload metadata schema only, will skip data backup
   --rbac, --backup-rbac, --do-backup-rbac                          Backup and upload RBAC related objects
   --configs, --backup-configs, --do-backup-configs                 Backup and upload 'clickhouse-server' configuration files
   --rbac-only                                                      Backup RBAC related objects only, will skip backup data, will backup schema only if --schema added
   --configs-only                                                   Backup 'clickhouse-server' configuration files only, will skip backup data, will backup schema only if --schema added
   --resume, --resumable                                            Save intermediate upload state and resume upload if backup exists on remote storage, ignore when 'remote_storage: custom' or 'use_embedded_backup_restore: true'
   --skip-check-parts-columns                                       Skip check system.parts_columns to allow backup inconsistent column types for data parts
   --skip-projections db_pattern.table_pattern:projections_pattern  Skip make and upload hardlinks to *.proj/* files during backup creation, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --delete, --delete-source, --delete-local                        explicitly delete local backup during upload
   
```
### CLI command - upload
```
NAME:
   clickhouse-backup upload - Upload backup to remote storage

USAGE:
   clickhouse-backup upload [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--diff-from=<local_backup_name>] [--diff-from-remote=<remote_backup_name>] [--resumable] <backup_name>

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   --diff-from value                          Local backup name which used to upload current backup as incremental
   --diff-from-remote value                   Remote backup name which used to upload current backup as incremental
   --table value, --tables value, -t value    Upload data only for matched table name patterns, separated by comma, allow ? and * as wildcard
   --partitions partition_id                  Upload backup only for selected partition names, separated by comma
If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
Values depends on field types in your table, use single quotes for String and Date/DateTime related types
Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, -s                                                     Upload schemas only
   --rbac-only, --rbac                                              Upload RBAC related objects only, will skip upload data, will backup schema only if --schema added
   --configs-only, --configs                                        Upload 'clickhouse-server' configuration files only, will skip upload data, will backup schema only if --schema added
   --skip-projections db_pattern.table_pattern:projections_pattern  Skip make and upload hardlinks to *.proj/* files during backup creation, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --resume, --resumable                                            Save intermediate upload state and resume upload if backup exists on remote storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'
   --delete, --delete-source, --delete-local                        explicitly delete local backup during upload
   
```
### CLI command - list
```
NAME:
   clickhouse-backup list - List of backups

USAGE:
   clickhouse-backup list [all|local|remote] [latest|previous]

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   
```
### CLI command - download
```
NAME:
   clickhouse-backup download - Download backup from remote storage

USAGE:
   clickhouse-backup download [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--resumable] <backup_name>

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   --table value, --tables value, -t value    Download objects which matched with table name patterns, separated by comma, allow ? and * as wildcard
   --partitions partition_id                  Download backup data only for selected partition names, separated by comma
If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
Values depends on field types in your table, use single quotes for String and Date/DateTime related types
Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, --schema-only, -s  Download schema only
   --rbac-only, --rbac          Download RBAC related objects only, will skip download data, will backup schema only if --schema added
   --configs-only, --configs    Download 'clickhouse-server' configuration files only, will skip download data, will backup schema only if --schema added
   --resume, --resumable        Save intermediate download state and resume download if backup exists on local storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'
   
```
### CLI command - restore
```
NAME:
   clickhouse-backup restore - Create schema and restore data from backup

USAGE:
   clickhouse-backup restore  [-t, --tables=<db>.<table>] [-m, --restore-database-mapping=<originDB>:<targetDB>[,<...>]] [--tm, --restore-table-mapping=<originTable>:<targetTable>[,<...>]] [--partitions=<partitions_names>] [-s, --schema] [-d, --data] [--rm, --drop] [-i, --ignore-dependencies] [--rbac] [--configs] [--resume] <backup_name>

OPTIONS:
   --config value, -c value                    Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value   override any environment variable via CLI parameter
   --table value, --tables value, -t value     Restore only database and objects which matched with table name patterns, separated by comma, allow ? and * as wildcard
   --restore-database-mapping value, -m value  Define the rule to restore data. For the database not defined in this struct, the program will not deal with it.
   --restore-table-mapping value, --tm value   Define the rule to restore data. For the table not defined in this struct, the program will not deal with it.
   --partitions partition_id                   Restore backup only for selected partition names, separated by comma
If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
Values depends on field types in your table, use single quotes for String and Date/DateTime related types
Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, -s                                                     Restore schema only
   --data, -d                                                       Restore data only
   --rm, --drop                                                     Drop exists schema objects before restore
   -i, --ignore-dependencies                                        Ignore dependencies when drop exists schema objects
   --rbac, --restore-rbac, --do-restore-rbac                        Restore RBAC related objects
   --configs, --restore-configs, --do-restore-configs               Restore 'clickhouse-server' CONFIG related files
   --rbac-only                                                      Restore RBAC related objects only, will skip backup data, will backup schema only if --schema added
   --configs-only                                                   Restore 'clickhouse-server' configuration files only, will skip backup data, will backup schema only if --schema added
   --skip-projections db_pattern.table_pattern:projections_pattern  Skip make hardlinks to *.proj/* files during backup restoring, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --resume, --resumable                                            Will resume download for object disk data
   
```
### CLI command - restore_remote
```
NAME:
   clickhouse-backup restore_remote - Download and restore

USAGE:
   clickhouse-backup restore_remote [--schema] [--data] [-t, --tables=<db>.<table>] [-m, --restore-database-mapping=<originDB>:<targetDB>[,<...>]] [--tm, --restore-table-mapping=<originTable>:<targetTable>[,<...>]] [--partitions=<partitions_names>] [--rm, --drop] [-i, --ignore-dependencies] [--rbac] [--configs] [--skip-rbac] [--skip-configs] [--resumable] <backup_name>

OPTIONS:
   --config value, -c value                    Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value   override any environment variable via CLI parameter
   --table value, --tables value, -t value     Download and restore objects which matched with table name patterns, separated by comma, allow ? and * as wildcard
   --restore-database-mapping value, -m value  Define the rule to restore data. For the database not defined in this struct, the program will not deal with it.
   --restore-table-mapping value, --tm value   Define the rule to restore data. For the database not defined in this struct, the program will not deal with it.
   --partitions partition_id                   Download and restore backup only for selected partition names, separated by comma
If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
Values depends on field types in your table, use single quotes for String and Date/DateTime related types
Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, -s                                                     Download and Restore schema only
   --data, -d                                                       Download and Restore data only
   --rm, --drop                                                     Drop schema objects before restore
   -i, --ignore-dependencies                                        Ignore dependencies when drop exists schema objects
   --rbac, --restore-rbac, --do-restore-rbac                        Download and Restore RBAC related objects
   --configs, --restore-configs, --do-restore-configs               Download and Restore 'clickhouse-server' CONFIG related files
   --rbac-only                                                      Restore RBAC related objects only, will skip backup data, will backup schema only if --schema added
   --configs-only                                                   Restore 'clickhouse-server' configuration files only, will skip backup data, will backup schema only if --schema added
   --skip-projections db_pattern.table_pattern:projections_pattern  Skip make hardlinks to *.proj/* files during backup restoring, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --resume, --resumable                                            Save intermediate download state and resume download if backup exists on remote storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'
   
```
### CLI command - delete
```
NAME:
   clickhouse-backup delete - Delete specific backup

USAGE:
   clickhouse-backup delete <local|remote> <backup_name>

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   
```
### CLI command - default-config
```
NAME:
   clickhouse-backup default-config - Print default config

USAGE:
   clickhouse-backup default-config [command options] [arguments...]

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   
```
### CLI command - print-config
```
NAME:
   clickhouse-backup print-config - Print current config merged with environment variables

USAGE:
   clickhouse-backup print-config [command options] [arguments...]

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   
```
### CLI command - clean
```
NAME:
   clickhouse-backup clean - Remove data in 'shadow' folder from all 'path' folders available from 'system.disks'

USAGE:
   clickhouse-backup clean [command options] [arguments...]

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   
```
### CLI command - clean_remote_broken
```
NAME:
   clickhouse-backup clean_remote_broken - Remove all broken remote backups

USAGE:
   clickhouse-backup clean_remote_broken [command options] [arguments...]

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   
```
### CLI command - watch
```
NAME:
   clickhouse-backup watch - Run infinite loop which create full + incremental backup sequence to allow efficient backup sequences

USAGE:
   clickhouse-backup watch [--watch-interval=1h] [--full-interval=24h] [--watch-backup-name-template=shard{shard}-{type}-{time:20060102150405}] [-t, --tables=<db>.<table>] [--partitions=<partitions_names>] [--schema] [--rbac] [--configs] [--skip-check-parts-columns]

DESCRIPTION:
   Execute create_remote + delete local, create full backup every `--full-interval`, create and upload incremental backup every `--watch-interval` use previous backup as base with `--diff-from-remote` option, use `backups_to_keep_remote` config option for properly deletion remote backups, will delete old backups which not have references from other backups

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   --watch-interval value                     Interval for run 'create_remote' + 'delete local' for incremental backup, look format https://pkg.go.dev/time#ParseDuration
   --full-interval value                      Interval for run 'create_remote'+'delete local' when stop create incremental backup sequence and create full backup, look format https://pkg.go.dev/time#ParseDuration
   --watch-backup-name-template value         Template for new backup name, could contain names from system.macros, {type} - full or incremental and {time:LAYOUT}, look to https://go.dev/src/time/format.go for layout examples
   --table value, --tables value, -t value    Create and upload only objects which matched with table name patterns, separated by comma, allow ? and * as wildcard
   --partitions partition_id                  Partitions names, separated by comma
If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
Values depends on field types in your table, use single quotes for String and Date/DateTime related types
Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, -s                                                     Schemas only
   --rbac, --backup-rbac, --do-backup-rbac                          Backup RBAC related objects only
   --configs, --backup-configs, --do-backup-configs                 Backup `clickhouse-server' configuration files only
   --skip-check-parts-columns                                       Skip check system.parts_columns to allow backup inconsistent column types for data parts
   --skip-projections db_pattern.table_pattern:projections_pattern  Skip make and upload hardlinks to *.proj/* files during backup creation, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   
```
### CLI command - server
```
NAME:
   clickhouse-backup server - Run API server

USAGE:
   clickhouse-backup server [command options] [arguments...]

OPTIONS:
   --config value, -c value                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override value, --env value  override any environment variable via CLI parameter
   --watch                                    Run watch go-routine for 'create_remote' + 'delete local', after API server startup
   --watch-interval value                     Interval for run 'create_remote' + 'delete local' for incremental backup, look format https://pkg.go.dev/time#ParseDuration
   --full-interval value                      Interval for run 'create_remote'+'delete local' when stop create incremental backup sequence and create full backup, look format https://pkg.go.dev/time#ParseDuration
   --watch-backup-name-template value         Template for new backup name, could contain names from system.macros, {type} - full or incremental and {time:LAYOUT}, look to https://go.dev/src/time/format.go for layout examples
   
```
