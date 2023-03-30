
# clickhouse-backup

[![Build](https://github.com/AlexAkulov/clickhouse-backup/actions/workflows/build.yaml/badge.svg?branch=dev)](https://github.com/AlexAkulov/clickhouse-backup/actions/workflows/build.yaml)
[![GoDoc](https://godoc.org/github.com/AlexAkulov/clickhouse-backup?status.svg)](http://godoc.org/github.com/AlexAkulov/clickhouse-backup)
[![Telegram](https://img.shields.io/badge/telegram-join%20chat-3796cd.svg)](https://t.me/clickhousebackup)
[![Docker Image](https://img.shields.io/docker/pulls/alexakulov/clickhouse-backup.svg)](https://hub.docker.com/r/alexakulov/clickhouse-backup)

Tool for easy ClickHouse backup and restore with cloud storage support

## Features

- Easy creating and restoring backups of all or specific tables
- Efficient storing of multiple backups on the file system
- Uploading and downloading with streaming compression
- Works with AWS, GCS, Azure, Tencent COS, FTP, SFTP
- **Support of Atomic Database Engine**
- **Support of multi disks installations**
- **Support for many custom remote storage like `rclone`, `kopia`, `restic`**
- Support of incremental backups on remote storage

## Limitations

- ClickHouse above 1.1.54390 is supported
- Only MergeTree family tables engines (more table types for `clickhouse-server` 22.7+ and `USE_EMBEDDED_BACKUP_RESTORE=true`)

## Installation

Download the latest binary from the [releases](https://github.com/AlexAkulov/clickhouse-backup/releases) page and decompress with:

```shell
tar -zxvf clickhouse-backup.tar.gz
```

Use the official tiny Docker image and run it on host with `clickhouse-server` installed:

```shell
docker run -u $(id -u clickhouse) --rm -it --network host -v "/var/lib/clickhouse:/var/lib/clickhouse" \
   -e CLICKHOUSE_PASSWORD="password" \
   -e S3_BUCKET="clickhouse-backup" \
   -e S3_ACCESS_KEY="access_key" \
   -e S3_SECRET_KEY="secret" \
   alexakulov/clickhouse-backup --help
```

Build from the sources:

```shell
GO111MODULE=on go get github.com/AlexAkulov/clickhouse-backup/cmd/clickhouse-backup
```

## Common CLI Usage

For CLI usage please see [Manual.md](Manual.md).

## Default Config

By default, the config file is located at `/etc/clickhouse-backup/config.yml`, but it can be redefined via `CLICKHOUSE_BACKUP_CONFIG` environment variable.
All options can be overwritten via environment variables.
Use `clickhouse-backup print-config` to print current config.

```yaml
general:
  remote_storage: none           # REMOTE_STORAGE, if `none` then `upload` and  `download` command will fail
  max_file_size: 1073741824      # MAX_FILE_SIZE, 1G by default, useless when upload_by_part is true, use to split data into archives of defined size
  disable_progress_bar: true     # DISABLE_PROGRESS_BAR, show progress bar during upload and download, makes sense only when `upload_concurrency` and `download_concurrency` is 1
  backups_to_keep_local: 0       # BACKUPS_TO_KEEP_LOCAL, how many latest local backup should be kept, 0 means all created backups will be stored on local disk
                                 # You shall run `clickhouse-backup delete local <backup_name>` command to remove temporary backup files from the local disk
  backups_to_keep_remote: 0      # BACKUPS_TO_KEEP_REMOTE, how many latest backup should be kept on remote storage, 0 means all uploaded backups will be stored on remote storage. 
                                 # If old backups are required for newer incremental backup then it won't be deleted. Be careful with long incremental backup sequences.
  log_level: info                # LOG_LEVEL, a choice from `debug`, `info`, `warn`, `error`
  allow_empty_backups: false     # ALLOW_EMPTY_BACKUPS
  # concurrency means parallel tables and parallel parts inside tables
  # for example 4 means max 4 parallel tables and 4 parallel parts inside one table, so equals 16 concurrent streams
  download_concurrency: 1        # DOWNLOAD_CONCURRENCY, max 255, by default, the value is round(sqrt(AVAILABLE_CPU_CORES / 2))  
  upload_concurrency: 1          # UPLOAD_CONCURRENCY, max 255, by default, the value is round(sqrt(AVAILABLE_CPU_CORES / 2))

  # RESTORE_SCHEMA_ON_CLUSTER, execute all schema related SQL queries with `ON CLUSTER` clause as Distributed DDL. 
  # Check `system.clusters` table for the correct cluster name, also `system.macros` can be used.
  # This isn't applicable when `use_embedded_backup_restore: true`
  restore_schema_on_cluster: ""   
  upload_by_part: true           # UPLOAD_BY_PART
  download_by_part: true         # DOWNLOAD_BY_PART
  use_resumable_state: true      # USE_RESUMABLE_STATE, allow resume upload and download according to the <backup_name>.resumable file

  # RESTORE_DATABASE_MAPPING, restore rules from backup databases to target databases, which is useful when changing destination database, all atomic tables will be created with new UUIDs.
  # Use the following format "src_db1:target_db1,src_db2:target_db2" 
  restore_database_mapping: {}   
  retries_on_failure: 3          # RETRIES_ON_FAILURE, how many times to retry after a failure during upload or download
  retries_pause: 30s             # RETRIES_PAUSE, duration time to pause after each download or upload failure 
clickhouse:
  username: default                # CLICKHOUSE_USERNAME
  password: ""                     # CLICKHOUSE_PASSWORD
  host: localhost                  # CLICKHOUSE_HOST
  port: 9000                       # CLICKHOUSE_PORT, don't use 8123, clickhouse-backup doesn't support HTTP protocol
  # CLICKHOUSE_DISK_MAPPING, use this mapping when your `system.disks` are different between the source and destination clusters during backup and restore process
  # Use the following format "disk_name1:disk_path1,disk_name2:disk_path2" 
  disk_mapping: {}
  # CLICKHOUSE_SKIP_TABLES, the list of tables (pattern are allowed) which are ignored during backup and restore process
  # Use the following format "pattern1,pattern2,pattern3" 
  skip_tables:                     
    - system.*
    - INFORMATION_SCHEMA.*
    - information_schema.*
  timeout: 5m                  # CLICKHOUSE_TIMEOUT
  freeze_by_part: false        # CLICKHOUSE_FREEZE_BY_PART, allow freeze by part instead of freeze the whole table
  freeze_by_part_where: ""     # CLICKHOUSE_FREEZE_BY_PART_WHERE, allow parts filtering during freeze when freeze_by_part: true
  secure: false                # CLICKHOUSE_SECURE, use TLS encryption for connection
  skip_verify: false           # CLICKHOUSE_SKIP_VERIFY, skip certificate verification and allow potential certificate warnings
  sync_replicated_tables: true # CLICKHOUSE_SYNC_REPLICATED_TABLES
  tls_key: ""                  # CLICKHOUSE_TLS_KEY, filename with TLS key file 
  tls_cert: ""                 # CLICKHOUSE_TLS_CERT, filename with TLS certificate file 
  tls_ca: ""                   # CLICKHOUSE_TLS_CA, filename with TLS custom authority file 
  log_sql_queries: true        # CLICKHOUSE_LOG_SQL_QUERIES, enable logging `clickhouse-backup` SQL queries on `system.query_log` table inside clickhouse-server
  debug: false                 # CLICKHOUSE_DEBUG
  config_dir:      "/etc/clickhouse-server"              # CLICKHOUSE_CONFIG_DIR
  restart_command: "systemctl restart clickhouse-server" # CLICKHOUSE_RESTART_COMMAND, use this command when restoring with --rbac or --config options
  ignore_not_exists_error_during_freeze: true # CLICKHOUSE_IGNORE_NOT_EXISTS_ERROR_DURING_FREEZE, helps to avoid backup failures when running frequent CREATE / DROP tables and databases during backup, `clickhouse-backup` will ignore `code: 60` and `code: 81` errors during execution of `ALTER TABLE ... FREEZE`
  check_replicas_before_attach: true # CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH, helps avoiding concurrent ATTACH PART execution when restoring ReplicatedMergeTree tables
  use_embedded_backup_restore: false # CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE, use BACKUP / RESTORE SQL statements instead of regular SQL queries to use features of modern ClickHouse server versions
azblob:
  endpoint_suffix: "core.windows.net" # AZBLOB_ENDPOINT_SUFFIX
  account_name: ""             # AZBLOB_ACCOUNT_NAME
  account_key: ""              # AZBLOB_ACCOUNT_KEY
  sas: ""                      # AZBLOB_SAS
  use_managed_identity: false  # AZBLOB_USE_MANAGED_IDENTITY
  container: ""                # AZBLOB_CONTAINER
  path: ""                     # AZBLOB_PATH
  compression_level: 1         # AZBLOB_COMPRESSION_LEVEL
  compression_format: tar      # AZBLOB_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  sse_key: ""                  # AZBLOB_SSE_KEY
  buffer_size: 0               # AZBLOB_BUFFER_SIZE, if less or eq 0 then calculated as max_file_size / max_parts_count, between 2Mb and 4Mb
  max_parts_count: 10000       # AZBLOB_MAX_PARTS_COUNT, number of parts for AZBLOB uploads, for properly calculate buffer size
  max_buffers: 3               # AZBLOB_MAX_BUFFERS
s3:
  access_key: ""                   # S3_ACCESS_KEY
  secret_key: ""                   # S3_SECRET_KEY
  bucket: ""                       # S3_BUCKET
  endpoint: ""                     # S3_ENDPOINT
  region: us-east-1                # S3_REGION
  acl: private                     # S3_ACL
  assume_role_arn: ""              # S3_ASSUME_ROLE_ARN
  force_path_style: false          # S3_FORCE_PATH_STYLE
  path: ""                         # S3_PATH
  disable_ssl: false               # S3_DISABLE_SSL
  compression_level: 1             # S3_COMPRESSION_LEVEL
  compression_format: tar          # S3_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  # look details in https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html
  sse: ""                          # S3_SSE, empty (default), AES256, or aws:kms
  sse_kms_key_id: ""               # S3_SSE_KMS_KEY_ID, if S3_SSE is aws:kms then specifies the ID of the Amazon Web Services Key Management Service
  sse_customer_algorithm: ""       # S3_SSE_CUSTOMER_ALGORITHM, Specifies the algorithm to use to when encrypting the object (for example, AES256)
  sse_customer_key: ""             # S3_SSE_CUSTOMER_KEY, Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data
  sse_customer_key_md5: ""         # S3_SSE_CUSTOMER_KEY_MD5, Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
  sse_kms_encryption_context: ""   # S3_SSE_KMS_ENCRYPTION_CONTEXT, The value of this header is a base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs.
                                   # Specifies the Amazon Web Services KMS Encryption Context to use for object
                                   # encryption. 
  disable_cert_verification: false # S3_DISABLE_CERT_VERIFICATION
  use_custom_storage_class: false  # S3_USE_CUSTOM_STORAGE_CLASS
  storage_class: STANDARD          # S3_STORAGE_CLASS, by default allow only from list https://github.com/aws/aws-sdk-go-v2/blob/main/service/s3/types/enums.go#L787-L799
  concurrency: 1                   # S3_CONCURRENCY
  part_size: 0                     # S3_PART_SIZE, if less or eq 0 then calculated as max_file_size / max_parts_count, between 5MB and 5Gb
  max_parts_count: 10000           # S3_MAX_PARTS_COUNT, number of parts for S3 multipart uploads
  allow_multipart_download: false  # S3_ALLOW_MULTIPART_DOWNLOAD, allow us fast download speed (same as upload), but will require additional disk space, download_concurrency * part size in worst case

  # S3_OBJECT_LABELS, allow setup metadata for each object during upload, use {macro_name} from system.macros and {backupName} for current backup name
  # Use the following format "key1:value1,key2:value2" 
  object_labels: {}
  # S3_CUSTOM_STORAGE_CLASS_MAP, allow setup  storage class depends on backup name regexp pattern, format nameRegexp > className  
  custom_storage_class_map: {}
  debug: false                     # S3_DEBUG
gcs:
  credentials_file: ""         # GCS_CREDENTIALS_FILE
  credentials_json: ""         # GCS_CREDENTIALS_JSON
  credentials_json_encoded: "" # GCS_CREDENTIALS_JSON_ENCODED
  bucket: ""                   # GCS_BUCKET
  path: ""                     # GCS_PATH
  compression_level: 1         # GCS_COMPRESSION_LEVEL
  compression_format: tar      # GCS_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  storage_class: STANDARD      # GCS_STORAGE_CLASS

  # GCS_OBJECT_LABELS, allow setup metadata for each object during upload, use {macro_name} from system.macros and {backupName} for current backup name
  # Use the following format "key1:value1,key2:value2" 
  object_labels: {}
  # GCS_CUSTOM_STORAGE_CLASS_MAP, allow setup storage class depends on backup name regexp pattern, format nameRegexp > className  
  custom_storage_class_map: {}  
  debug: false                 # GCS_DEBUG
cos:
  url: ""                      # COS_URL
  timeout: 2m                  # COS_TIMEOUT
  secret_id: ""                # COS_SECRET_ID
  secret_key: ""               # COS_SECRET_KEY
  path: ""                     # COS_PATH
  compression_format: tar      # COS_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  compression_level: 1         # COS_COMPRESSION_LEVEL
ftp:
  address: ""                  # FTP_ADDRESS
  timeout: 2m                  # FTP_TIMEOUT
  username: ""                 # FTP_USERNAME
  password: ""                 # FTP_PASSWORD
  tls: false                   # FTP_TLS
  path: ""                     # FTP_PATH
  compression_format: tar      # FTP_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  compression_level: 1         # FTP_COMPRESSION_LEVEL
  debug: false                 # FTP_DEBUG
sftp:
  address: ""                  # SFTP_ADDRESS
  username: ""                 # SFTP_USERNAME
  password: ""                 # SFTP_PASSWORD
  key: ""                      # SFTP_KEY
  path: ""                     # SFTP_PATH
  concurrency: 1               # SFTP_CONCURRENCY     
  compression_format: tar      # SFTP_COMPRESSION_FORMAT, allowed values tar, lz4, bzip2, gzip, sz, xz, brortli, zstd, `none` for upload data part folders as is
  compression_level: 1         # SFTP_COMPRESSION_LEVEL
  debug: false                 # SFTP_DEBUG
custom:  
  upload_command: ""           # CUSTOM_UPLOAD_COMMAND
  download_command: ""         # CUSTOM_DOWNLOAD_COMMAND
  delete_command: ""           # CUSTOM_DELETE_COMMAND
  list_command: ""             # CUSTOM_LIST_COMMAND
  command_timeout: "4h"          # CUSTOM_COMMAND_TIMEOUT
api:
  listen: "localhost:7171"     # API_LISTEN
  enable_metrics: true         # API_ENABLE_METRICS
  enable_pprof: false          # API_ENABLE_PPROF
  username: ""                 # API_USERNAME, basic authorization for API endpoint
  password: ""                 # API_PASSWORD
  secure: false                # API_SECURE, use TLS for listen API socket
  certificate_file: ""         # API_CERTIFICATE_FILE
  private_key_file: ""         # API_PRIVATE_KEY_FILE
  integration_tables_host: ""  # API_INTEGRATION_TABLES_HOST, allow use DNS name to connect in `system.backup_list` and `system.backup_actions`
  allow_parallel: false        # API_ALLOW_PARALLEL, could allocate much memory and spawn go-routines, don't enable it if you not sure
  create_integration_tables: false # API_CREATE_INTEGRATION_TABLES, create `system.backup_list` and `system.backup_actions` 
  complete_resumable_after_restart: true # API_COMPLETE_RESUMABLE_AFTER_RESTART, after API server startup, if `/var/lib/clickhouse/backup/*/(upload|download).state` present, then operation will continue in background

```

## Concurrency, CPU and Memory usage recommendation 

`upload_concurrency` and `download concurrency` define how much parallel download / upload go-routines will start independent of remote storage type.
In 1.3.0+ it means how much parallel data parts will upload, cause by default `upload_by_part` and `download_by_part` is true.

`concurrency` in `s3` section mean how much concurrent `upload` streams will run during multipart upload in each upload go-routine
High value for `S3_CONCURRENCY` and high value for `S3_PART_SIZE` will allocate high memory for buffers inside AWS golang SDK.

`concurrency` in `sftp` section mean how much concurrent request will use for `upload` and `download` for each file. 

`compression_format`, better use `tar` for less CPU usage, cause for most of cases data on clickhouse-backup already compressed.

## remote_storage: custom

All custom commands could use go-template language for evaluate you can use `{{ .cfg.* }}` `{{ .backupName }}` `{{ .diffFromRemote }}`
Custom `list_command` shall return JSON which compatible with `metadata.Backup` type with [JSONEachRow](https://clickhouse.com/docs/en/interfaces/formats/#jsoneachrow) format. 
Look examples for adoption [restic](https://github.com/AlexAkulov/clickhouse-backup/tree/master/test/integration/restic/), [rsync](https://github.com/AlexAkulov/clickhouse-backup/tree/master/test/integration/rsync/) and [kopia](https://github.com/AlexAkulov/clickhouse-backup/tree/master/test/integration/kopia/). 

## ATTENTION!

Never change files permissions in `/var/lib/clickhouse/backup`.
This path contains hard links. Permissions on all hard links to the same data on disk are always identical.
That means that if you change the permissions/owner/attributes on a hard link in backup path, permissions on files with which ClickHouse works will be changed too.
That might lead to data corruption.

## API
Use the `clickhouse-backup server` command to run as a REST API server. In general, the API attempts to mirror the CLI commands.

> **GET /**

List all current applicable HTTP routes

> **POST /**
> **POST /restart**

Restart HTTP server, close all current connections, close listen socket, open listen socket again, all background go-routines breaks with contexts

> **GET /backup/kill**

Kill selected command from `GET /backup/actions` command list, kill process should be near immediately, but some go-routines (upload one data part) could continue run  
* Optional query argument `command` should contain command string which will kill, if omit then will kill last "in progress" command  

> **GET /backup/tables**

Print list of tables: `curl -s localhost:7171/backup/tables | jq .`, exclude pattern matched table from `skip_tables` configuration parameters
* Optional query argument `table` works the same as the `--table value` CLI argument.

> **GET /backup/tables/all**

Print list of tables: `curl -s localhost:7171/backup/tables/all | jq .`, ignore `skip_tables` configuration parameters
* Optional query argument `table` works the same as the `--table value` CLI argument.

> **POST /backup/create**

Create new backup: `curl -s localhost:7171/backup/create -X POST | jq .`
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `partitions` works the same as the `--partitions value` CLI argument.
* Optional query argument `name` works the same as specifying a backup name with the CLI.
* Optional query argument `schema` works the same the `--schema` CLI argument (backup schema only).
* Optional query argument `rbac` works the same the `--rbac` CLI argument (backup RBAC).
* Optional query argument `configs` works the same the `--configs` CLI argument (backup configs).
* Additional example: `curl -s 'localhost:7171/backup/create?table=default.billing&name=billing_test' -X POST`

Note: this operation is async, so the API will return once the operation has been started.

> **POST /backup/watch**

Run background watch process and create full+incremental backups sequence: `curl -s localhost:7171/backup/watch -X POST | jq .`
You can't run watch twice with the same parameters even when `allow_parallel: true`
* Optional query argument `watch_interval` works the same as the `--watch-interval value` CLI argument.
* Optional query argument `full_interval` works the same as the `--full-interval value` CLI argument.
* Optional query argument `watch_backup_name_template` works the same as the `--watch-backup-name-template value` CLI argument.
* Optional query argument `table` works the same as the `--table value` CLI argument (backup only selected tables).
* Optional query argument `partitions` works the same as the `--partitions value` CLI argument (backup only selected partitions).
* Optional query argument `schema` works the same the `--schema` CLI argument (backup schema only).
* Optional query argument `rbac` works the same the `--rbac` CLI argument (backup RBAC).
* Optional query argument `configs` works the same the `--configs` CLI argument (backup configs).
* Additional example: `curl -s 'localhost:7171/backup/watch?table=default.billing&watch_interval=1h&full_interval=24h' -X POST`

Note: this operation is async and can stop only with `kill -s SIGHUP $(pgrep -f clickhouse-backup)` or call `/restart`, `/backup/kill`, so the API will return once the operation has been started.

> **POST /backup/clean**

Clean `shadow` folder on all available path from `system.disks`

> **POST /backup/clean/remote_broken**

Remove
Note: this operation is sync, and could take a lot of time, increase http timeouts during call


> **POST /backup/upload**

Upload backup to remote storage: `curl -s localhost:7171/backup/upload/<BACKUP_NAME> -X POST | jq .`
* Optional query argument `diff-from` works the same as the `--diff-from` CLI argument.
* Optional query argument `diff-from-remote` works the same as the `--diff-from-remote` CLI argument.
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `partitions` works the same as the `--partitions value` CLI argument.
* Optional query argument `schema` works the same as the `--schema` CLI argument (upload schema only).
* Optional query argument `resumable` works the same as the `--resumable` CLI argument (save intermediate upload state and resume upload if already exists on remote storage).

Note: this operation is async, so the API will return once the operation has been started.

> **GET /backup/list/{where}**

Print list of backups: `curl -s localhost:7171/backup/list | jq .`
Print list only local backups: `curl -s localhost:7171/backup/list/local | jq .`
Print list only remote backups: `curl -s localhost:7171/backup/list/remote | jq .`

Note: The `Size` field could not populate for local backups, which recently or in progress created.
Note: The `Size` field could not populate for remote backups, which upload status in progress.

> **POST /backup/download**

Download backup from remote storage: `curl -s localhost:7171/backup/download/<BACKUP_NAME> -X POST | jq .`
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `partitions` works the same as the `--partitions value` CLI argument.
* Optional query argument `schema` works the same the `--schema` CLI argument (download schema only).
* Optional query argument `resumable` works the same as the `--resumable` CLI argument (save intermediate download state and resume download if already exists on local storage).


Note: this operation is async, so the API will return once the operation has been started.

> **POST /backup/restore**

Create schema and restore data from backup: `curl -s localhost:7171/backup/restore/<BACKUP_NAME> -X POST | jq .`
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `partitions` works the same as the `--partitions value` CLI argument.
* Optional query argument `schema` works the same the `--schema` CLI argument (restore schema only).
* Optional query argument `data` works the same the `--data` CLI argument (restore data only).
* Optional query argument `rm` works the same the `--rm` CLI argument (drop tables before restore).
* Optional query argument `ignore_dependencies` works the same the `--ignore-dependencies` CLI argument.
* Optional query argument `rbac` works the same the `--rbac` CLI argument (restore RBAC).
* Optional query argument `configs` works the same the `--configs` CLI argument (restore configs).
* Optional query argument `restore_database_mapping` works the same the `--restore-database-mapping` CLI argument.

> **POST /backup/delete**

Delete specific remote backup: `curl -s localhost:7171/backup/delete/remote/<BACKUP_NAME> -X POST | jq .`

Delete specific local backup: `curl -s localhost:7171/backup/delete/local/<BACKUP_NAME> -X POST | jq .`

> **GET /backup/status**

Display list of current running async operation: `curl -s localhost:7171/backup/status | jq .`

> **POST /backup/actions**

Execute multiple backup actions: `curl -X POST -d '{"command":"create test_backup"}' -s localhost:7171/backup/actions`

> **GET /backup/actions**

Display list of all operations from start of API server: `curl -s localhost:7171/backup/actions | jq .`
* Optional query argument `filter` could filter actions on server side.
* Optional query argument `last` could filter show only last `XX` actions.

## Storage

### S3

In order to make backups to S3, the following permissions shall be set:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "clickhouse-backup-s3-access-to-files",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::BUCKET_NAME/*"
        },
        {
            "Sid": "clickhouse-backup-s3-access-to-bucket",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::BUCKET_NAME"
        }
    ]
}
```

## Examples

### Simple cron script for daily backup and uploading
```bash
#!/bin/bash
BACKUP_NAME=my_backup_$(date -u +%Y-%m-%dT%H-%M-%S)
clickhouse-backup create $BACKUP_NAME >> /var/log/clickhouse-backup.log
if [[ $? != 0 ]]; then
  echo "clickhouse-backup create $BACKUP_NAME FAILED and return $? exit code"
fi

clickhouse-backup upload $BACKUP_NAME >> /var/log/clickhouse-backup.log
if [[ $? != 0 ]]; then
  echo "clickhouse-backup upload $BACKUP_NAME FAILED and return $? exit code"
fi
```

### More use cases of clickhouse-backup
- [How to convert MergeTree to ReplicatedMergeTree](Examples.md#how-to-convert-mergetree-to-replicatedmergetree)
- [How to store backups on NFS or another server](Examples.md#how-to-store-backups-on-nfs-backup-drive-or-another-server-via-sftp)
- [How to move data to another clickhouse server](Examples.md#how-to-move-data-to-another-clickhouse-server)
- [How to reduce number of partitions](Examples.md#How-to-reduce-number-of-partitions)
- [How to monitor that backups created and uploaded correctly](Examples.md#how-to-monitor-that-backups-created-and-uploaded-correctly)
- [How to make backup / restore sharded cluster](Examples.md#how-to-make-backup--restore-sharded-cluster)
- [How to make backup sharded cluster with Ansible](Examples.md#how-to-make-backup-sharded-cluster-with-ansible)
- [How to make back up database with several terabytes of data](Examples.md#how-to-make-backup-database-with-several-terabytes-of-data)
- [How to use clickhouse-backup in Kubernetes](Examples.md#how-to-use-clickhouse-backup-in-kubernetes)
- [How do incremental backups work to remote storage](Examples.md#how-do-incremental-backups-work-to-remote-storage)
- [How to watch backups work](Examples.md#how-to-work-watch-command)
