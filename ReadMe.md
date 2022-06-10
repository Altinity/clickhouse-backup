
# clickhouse-backup

[![Build](https://github.com/AlexAkulov/clickhouse-backup/actions/workflows/build.yaml/badge.svg?branch=dev)](https://github.com/AlexAkulov/clickhouse-backup/actions/workflows/build.yaml)
[![GoDoc](https://godoc.org/github.com/AlexAkulov/clickhouse-backup?status.svg)](http://godoc.org/github.com/AlexAkulov/clickhouse-backup)
[![Telegram](https://img.shields.io/badge/telegram-join%20chat-3796cd.svg)](https://t.me/clickhousebackup)
[![Docker Image](https://img.shields.io/docker/pulls/alexakulov/clickhouse-backup.svg)](https://hub.docker.com/r/alexakulov/clickhouse-backup)

Tool for easy ClickHouse backup and restore with cloud storages support

## Features

- Easy creating and restoring backups of all or specific tables
- Efficient storing of multiple backups on the file system
- Uploading and downloading with streaming compression
- Works with AWS, GCS, Azure, Tencent COS, FTP, SFTP
- **Support of Atomic Database Engine**
- **Support of multi disks installations**
- Support of incremental backups on remote storages

TODO:
- Smart restore for replicated tables

## Limitations

- ClickHouse above 1.1.54390 is supported
- Only MergeTree family tables engines

## Installation

- Download the latest binary from the [releases](https://github.com/AlexAkulov/clickhouse-backup/releases) page and decompress with:

```shell
tar -zxvf clickhouse-backup.tar.gz
```

- Use the official tiny Docker image and run it on host where installed `clickhouse-server`:

```shell
docker run -u $(id -u clickhouse) --rm -it --network host -v "/var/lib/clickhouse:/var/lib/clickhouse" \
   -e CLICKHOUSE_PASSWORD="password" \
   -e S3_BUCKET="clickhouse-backup" \
   -e S3_ACCESS_KEY="access_key" \
   -e S3_SECRET_KEY="secret" \
   alexakulov/clickhouse-backup --help
```

- Build from the sources:

```shell
GO111MODULE=on go get github.com/AlexAkulov/clickhouse-backup/cmd/clickhouse-backup
```

## Usage

```
NAME:
   clickhouse-backup - Tool for easy backup of ClickHouse with cloud support

USAGE:
   clickhouse-backup <command> [-t, --tables=<db>.<table>] <backup_name>

VERSION:
   1.0.0

DESCRIPTION:
   Run as 'root' or 'clickhouse' user

COMMANDS:
   tables          Print list of tables
   create          Create new backup
   create_remote   Create and upload
   upload          Upload backup to remote storage
   list            Print list of backups
   download        Download backup from remote storage
   restore         Create schema and restore data from backup
   restore_remote  Download and restore
   delete          Delete specific backup
   default-config  Print default config
   print-config    Print current config
   clean           Remove data in 'shadow' folder from all `path` folders available from `system.disks`
   server          Run API server
   help, h         Shows a list of commands or help for one command
GLOBAL OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --help, -h              show help
   --version, -v           print the version
```

### Default Config

Config file location can be defined by ```$CLICKHOUSE_BACKUP_CONFIG```

All options can be overwritten via environment variables

```yaml
general:
  remote_storage: none           # REMOTE_STORAGE, if `none` then `upload` and  `download` command will fail
  max_file_size: 1073741824      # MAX_FILE_SIZE, 1G by default, useless when upload_by_part is true, use for split data parts files by archives  
  disable_progress_bar: true     # DISABLE_PROGRESS_BAR, show progress bar during upload and download, have sense only when `upload_concurrency` and `download_concurrency` equal 1
  backups_to_keep_local: 0       # BACKUPS_TO_KEEP_LOCAL, how much newest local backup should keep, 0 mean all created backups will keep on local disk
                                 # you shall to run `clickhouse-backup delete local <backup_name>` command to avoid useless disk space allocations
  backups_to_keep_remote: 0      # BACKUPS_TO_KEEP_REMOTE, how much newest backup should keep on remote storage, 0 mean all uploaded backups will keep on remote storage. 
                                 # if old backup is required for newer incremental backup, then it will don't delete. Be careful with long incremental backup sequences.
  log_level: info                # LOG_LEVEL
  allow_empty_backups: false     # ALLOW_EMPTY_BACKUPS
  download_concurrency: 1        # DOWNLOAD_CONCURRENCY, max 255
  upload_concurrency: 1          # UPLOAD_CONCURRENCY, max 255
  restore_schema_on_cluster: ""  # RESTORE_SCHEMA_ON_CLUSTER, execute all schema related SQL queryes with `ON CLUSTER` clause as Distributed DDL, look to `system.clusters` table for proper cluster name
  upload_by_part: true           # UPLOAD_BY_PART
  download_by_part: true         # DOWNLOAD_BY_PART
clickhouse:
  username: default                # CLICKHOUSE_USERNAME
  password: ""                     # CLICKHOUSE_PASSWORD
  host: localhost                  # CLICKHOUSE_HOST
  port: 9000                       # CLICKHOUSE_PORT, don't use 8123, clickhouse-backup doesn't support HTTP protocol
  disk_mapping: {}                 # CLICKHOUSE_DISK_MAPPING, use it if your system.disks on restored servers not the same with system.disks on server where backup was created
  skip_tables:                     # CLICKHOUSE_SKIP_TABLES
    - system.*
    - INFORMATION_SCHEMA.*
    - information_schema.*
  timeout: 5m                  # CLICKHOUSE_TIMEOUT
  freeze_by_part: false        # CLICKHOUSE_FREEZE_BY_PART
  secure: false                # CLICKHOUSE_SECURE, use SSL encryption for connect
  skip_verify: false           # CLICKHOUSE_SKIP_VERIFY
  sync_replicated_tables: true # CLICKHOUSE_SYNC_REPLICATED_TABLES
  tls_key: ""                  # CLICKHOUSE_TLS_KEY, filename with TLS key file 
  tls_cert: ""                 # CLICKHOUSE_TLS_CERT, filename with TLS certificate file 
  tls_ca: ""                   # CLICKHOUSE_TLS_CA, filename with TLS custom authority file 
  log_sql_queries: true        # CLICKHOUSE_LOG_SQL_QUERIES, enable log clickhouse-backup SQL queries on `system.query_log` table inside clickhouse-server
  debug: false                 # CLICKHOUSE_DEBUG
  config_dir:      "/etc/clickhouse-server"              # CLICKHOUSE_CONFIG_DIR
  restart_command: "systemctl restart clickhouse-server" # CLICKHOUSE_RESTART_COMMAND, this command use when you try to restore with --rbac or --config options
  ignore_not_exists_error_during_freeze: true # CLICKHOUSE_IGNORE_NOT_EXISTS_ERROR_DURING_FREEZE, allow avoiding backup failures when you often CREATE / DROP tables and databases during backup creation, clickhouse-backup will ignore `code: 60` and `code: 81` errors during execute `ALTER TABLE ... FREEZE` 
azblob:
  endpoint_suffix: "core.windows.net" # AZBLOB_ENDPOINT_SUFFIX
  account_name: ""             # AZBLOB_ACCOUNT_NAME
  account_key: ""              # AZBLOB_ACCOUNT_KEY
  sas: ""                      # AZBLOB_SAS
  use_managed_identity: false  # AZBLOB_USE_MANAGED_IDENTITY
  container: ""                # AZBLOB_CONTAINER
  path: ""                     # AZBLOB_PATH
  compression_level: 1         # AZBLOB_COMPRESSION_LEVEL
  compression_format: tar      # AZBLOB_COMPRESSION_FORMAT
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
  compression_format: tar          # S3_COMPRESSION_FORMAT
  sse: ""                          # S3_SSE, empty (default), AES256, or aws:kms
  disable_cert_verification: false # S3_DISABLE_CERT_VERIFICATION
  storage_class: STANDARD          # S3_STORAGE_CLASS
  concurrency: 1                   # S3_CONCURRENCY
  part_size: 0                     # S3_PART_SIZE, if less or eq 0 then calculated as max_file_size / max_parts_count, between 5MB and 5Gb
  max_parts_count: 10000           # S3_MAX_PARTS_COUNT, number of parts for S3 multipart uploads
  allow_multipart_download: false  # S3_ALLOW_MULTIPART_DOWNLOAD, allow us fast download speed (same as upload), but will require additional disk space, download_concurrency * part size in worst case   
  debug: false                     # S3_DEBUG
gcs:
  credentials_file: ""         # GCS_CREDENTIALS_FILE
  credentials_json: ""         # GCS_CREDENTIALS_JSON
  bucket: ""                   # GCS_BUCKET
  path: ""                     # GCS_PATH
  compression_level: 1         # GCS_COMPRESSION_LEVEL
  compression_format: tar      # GCS_COMPRESSION_FORMAT
  debug: false                 # GCS_DEBUG
cos:
  url: ""                      # COS_URL
  timeout: 2m                  # COS_TIMEOUT
  secret_id: ""                # COS_SECRET_ID
  secret_key: ""               # COS_SECRET_KEY
  path: ""                     # COS_PATH
  compression_format: tar      # COS_COMPRESSION_FORMAT
  compression_level: 1         # COS_COMPRESSION_LEVEL
ftp:
  address: ""                  # FTP_ADDRESS
  timeout: 2m                  # FTP_TIMEOUT
  username: ""                 # FTP_USERNAME
  password: ""                 # FTP_PASSWORD
  tls: false                   # FTP_TLS
  path: ""                     # FTP_PATH
  compression_format: tar      # FTP_COMPRESSION_FORMAT
  compression_level: 1         # FTP_COMPRESSION_LEVEL
  debug: false                 # FTP_DEBUG
sftp:
  address: ""                  # SFTP_ADDRESS
  username: ""                 # SFTP_USERNAME
  password: ""                 # SFTP_PASSWORD
  key: ""                      # SFTP_KEY
  path: ""                     # SFTP_PATH
  concurrency: 1               # SFTP_CONCURRENCY     
  compression_format: tar      # SFTP_COMPRESSION_FORMAT
  compression_level: 1         # SFTP_COMPRESSION_LEVEL
  debug: false                 # SFTP_DEBUG
api:
  listen: "localhost:7171"     # API_LISTEN
  enable_metrics: true         # API_ENABLE_METRICS
  enable_pprof: false          # API_ENABLE_PPROF
  username: ""                 # API_USERNAME, basic authorization for API endpoint
  password: ""                 # API_PASSWORD
  secure: false                # API_SECURE, use TLS for listen API socket
  certificate_file: ""         # API_CERTIFICATE_FILE
  private_key_file: ""         # API_PRIVATE_KEY_FILE
  create_integration_tables: false # API_CREATE_INTEGRATION_TABLES
  integration_tables_host: "" # API_INTEGRATION_TABLES_HOST, allow use DNS name to connect in `system.backup_list` and `system.backup_actions`
  allow_parallel: false        # API_ALLOW_PARALLEL, could allocate much memory and spawn go-routines, don't enable it if you not sure
```

## Concurrency, CPU and Memory usage recommendation 

`upload_concurrency` and `download concurrency` define how much parallel download / upload go-routines will start independent of remote storage type.
In 1.3.0+ it means how much parallel data parts will upload, cause by default `upload_by_part` and `download_by_part` is true.

`concurrency` in `s3` section mean how much concurrent `upload` streams will run during multipart upload in each upload go-routine
High value for `S3_CONCURRENCY` and high value for `S3_PART_SIZE` will allocate high memory for buffers inside AWS golang SDK.

`concurrency` in `sftp` section mean how much concurrent request will use for `upload` and `download` for each file. 

`compression_format`, better use `tar` for less CPU usage, cause for most of cases data on clickhouse-backup already compressed.

## ATTENTION!

Never change files permissions in `/var/lib/clickhouse/backup`.
This path contains hard links. Permissions on all hard links to the same data on disk are always identical.
That means that if you change the permissions/owner/attributes on a hard link in backup path, permissions on files with which ClickHouse works will be changed too.
That might lead to data corruption.

## API
Use the `clickhouse-backup server` command to run as a REST API server. In general, the API attempts to mirror the CLI commands.

> **GET /**

List all current applicable HTTP routes

> **POST /restart**

Restart HTTP server, close all current connections, close listen socket, open listen socket again, all background go-routines with upload / download not breaks (maybe will in future)

> **GET /backup/tables**

Print list of tables: `curl -s localhost:7171/backup/tables | jq .`

> **POST /backup/create**

Create new backup: `curl -s localhost:7171/backup/create -X POST | jq .`
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `partitions` works the same as the `--partitions value` CLI argument.
* Optional query argument `name` works the same as specifying a backup name with the CLI.
* Optional query argument `schema` works the same the `--schema` CLI argument (backup schema only).
* Optional query argument `rbac` works the same the `--rbac` CLI argument (backup RBAC).
* Optional query argument `configs` works the same the `--configs` CLI argument (backup configs).
* Full example: `curl -s 'localhost:7171/backup/create?table=default.billing&name=billing_test' -X POST`

Note: this operation is async, so the API will return once the operation has been started.

> **POST /backup/clean**

Clean `shadow` folder on all available path from `system.disks`


> **POST /backup/upload**

Upload backup to remote storage: `curl -s localhost:7171/backup/upload/<BACKUP_NAME> -X POST | jq .`
* Optional query argument `diff-from` works the same as the `--diff-from` CLI argument.
* Optional query argument `diff-from-remote` works the same as the `--diff-from-remote` CLI argument.
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `partitions` works the same as the `--partitions value` CLI argument.
* Optional query argument `schema` works the same as the `--schema` CLI argument (upload schema only).

Note: this operation is async, so the API will return once the operation has been started.

> **GET /backup/list/{where}**

Print list of backups: `curl -s localhost:7171/backup/list | jq .`
Print list only local backups: `curl -s localhost:7171/backup/list/local | jq .`
Print list only remote backups: `curl -s localhost:7171/backup/list/remote | jq .`

Note: The `Size` field is not populated for local backups.

> **POST /backup/download**

Download backup from remote storage: `curl -s localhost:7171/backup/download/<BACKUP_NAME> -X POST | jq .`
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `partitions` works the same as the `--partitions value` CLI argument.
* Optional query argument `schema` works the same the `--schema` CLI argument (download schema only).


Note: this operation is async, so the API will return once the operation has been started.

> **POST /backup/restore**

Create schema and restore data from backup: `curl -s localhost:7171/backup/restore/<BACKUP_NAME> -X POST | jq .`
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `partitions` works the same as the `--partitions value` CLI argument.
* Optional query argument `schema` works the same the `--schema` CLI argument (restore schema only).
* Optional query argument `data` works the same the `--data` CLI argument (restore data only).
* Optional query argument `rm` works the same the `--rm` CLI argument (drop tables before restore).
* Optional query argument `rbac` works the same the `--rbac` CLI argument (restore RBAC).
* Optional query argument `configs` works the same the `--configs` CLI argument (restore configs).

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

## Storages

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
- [How to backup sharded cluster with Ansible](Examples.md#how-to-backup-sharded-cluster-with-ansible)
- [How to backup database with several terabytes of data](Examples.md#how-to-backup-database-with-several-terabytes-of-data)
- [How to use clickhouse-backup in Kubernetes](Examples.md#how-to-use-clickhouse-backup-in-kubernetes)
- [How do incremental backups work to remote storage](Examples.md#how-do-incremental-backups-work-to-remote-storage)
