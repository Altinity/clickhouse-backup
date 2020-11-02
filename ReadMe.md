
# clickhouse-backup

[![Build Status](https://travis-ci.org/AlexAkulov/clickhouse-backup.svg?branch=master)](https://travis-ci.org/AlexAkulov/clickhouse-backup)
[![GoDoc](https://godoc.org/github.com/AlexAkulov/clickhouse-backup?status.svg)](http://godoc.org/github.com/AlexAkulov/clickhouse-backup)
[![Telegram](https://img.shields.io/badge/telegram-join%20chat-3796cd.svg)](https://t.me/clickhousebackup)
[![Docker Image](https://img.shields.io/docker/pulls/alexakulov/clickhouse-backup.svg)](https://hub.docker.com/r/alexakulov/clickhouse-backup)

Tool for easy ClickHouse backup and restore with cloud storages support

## Features

- Easy creating and restoring backups of all or specific tables
- Efficient storing of multiple backups on the file system
- Uploading and downloading with streaming compression
- Support of incremental backups on remote storages
- Works with AWS, Azure, GCS, Tencent COS, FTP

## Limitations

- ClickHouse above 1.1.54390 and before 20.10 is supported
- Only MergeTree family tables engines
- Backup of 'Tiered storage' or `storage_policy` IS NOT SUPPORTED!
- 'Atomic' database engine enabled by default in ClickHouse 20.10 IS NOT SUPPORTED!
- Maximum backup size on cloud storages is 5TB
- Maximum number of parts on AWS S3 is 10,000 (increase part_size if your database is more than 1TB)

## Download

- Download the latest binary from the [releases](https://github.com/AlexAkulov/clickhouse-backup/releases) page and decompress with:

```shell
tar -zxvf clickhouse-backup.tar.gz
```

- Use the official tiny Docker image and run it like:

```shell
docker run --rm -it --network host -v "/var/lib/clickhouse:/var/lib/clickhouse" \
   -e CLICKHOUSE_PASSWORD="password" \
   -e S3_BUCKET="clickhouse-backup" \
   -e S3_ACCESS_KEY="access_key" \
   -e S3_SECRET_KEY="secret" \
   alexakulov/clickhouse-backup --help
```

- Build from the sources:

```shell
GO111MODULE=on go get github.com/AlexAkulov/clickhouse-backup
```

## Usage

```
NAME:
   clickhouse-backup - Tool for easy backup of ClickHouse with cloud support

USAGE:
   clickhouse-backup <command> [-t, --tables=<db>.<table>] <backup_name>

VERSION:
   unknown

DESCRIPTION:
   Run as 'root' or 'clickhouse' user

COMMANDS:
     tables          Print list of tables
     create          Create new backup
     upload          Upload backup to remote storage
     list            Print list of backups
     download        Download backup from remote storage
     restore         Create schema and restore data from backup
     delete          Delete specific backup
     default-config  Print default config
     freeze          Freeze tables
     clean           Remove data in 'shadow' folder
     server          Run API server
     help, h         Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml")
   --help, -h              show help
   --version, -v           print the version
```

### Default Config

Config file location can be defined by ```$CLICKHOUSE_BACKUP_CONFIG```

All options can be overwritten via environment variables

```yaml
general:
  remote_storage: s3           # REMOTE_STORAGE
  disable_progress_bar: false  # DISABLE_PROGRESS_BAR
  backups_to_keep_local: 0     # BACKUPS_TO_KEEP_LOCAL
  backups_to_keep_remote: 0    # BACKUPS_TO_KEEP_REMOTE
clickhouse:
  username: default            # CLICKHOUSE_USERNAME
  password: ""                 # CLICKHOUSE_PASSWORD
  host: localhost              # CLICKHOUSE_HOST
  port: 9000                   # CLICKHOUSE_PORT
  timeout: 5m                  # CLICKHOUSE_TIMEOUT
  data_path: ""                # CLICKHOUSE_DATA_PATH
  skip_tables:                 # CLICKHOUSE_SKIP_TABLES
    - system.*
  timeout: 5m                  # CLICKHOUSE_TIMEOUT
  freeze_by_part: false        # CLICKHOUSE_FREEZE_BY_PART
azblob:
  endpoint_suffix: "core.windows.net" # AZBLOB_ENDPOINT_SUFFIX
  account_name: ""             # AZBLOB_ACCOUNT_NAME
  account_key: ""              # AZBLOB_ACCOUNT_KEY
  sas: ""                      # AZBLOB_SAS
  container: ""                # AZBLOB_CONTAINER
  path: ""                     # AZBLOB_PATH
  compression_level: 1         # AZBLOB_COMPRESSION_LEVEL
  compression_format: gzip     # AZBLOB_COMPRESSION_FORMAT
  sse_key: ""                  # AZBLOB_SSE_KEY
s3:
  access_key: ""                   # S3_ACCESS_KEY
  secret_key: ""                   # S3_SECRET_KEY
  bucket: ""                       # S3_BUCKET
  endpoint: ""                     # S3_ENDPOINT
  region: us-east-1                # S3_REGION
  acl: private                     # S3_ACL
  force_path_style: false          # S3_FORCE_PATH_STYLE
  path: ""                         # S3_PATH
  disable_ssl: false               # S3_DISABLE_SSL
  part_size: 104857600             # S3_PART_SIZE
  compression_level: 1             # S3_COMPRESSION_LEVEL
  # supports 'tar', 'lz4', 'bzip2', 'gzip', 'sz', 'xz'
  compression_format: gzip         # S3_COMPRESSION_FORMAT
  # empty (default), AES256, or aws:kms
  sse: AES256                      # S3_SSE
  disable_cert_verification: false # S3_DISABLE_CERT_VERIFICATION
  debug: false                     # S3_DEBUG
gcs:
  credentials_file: ""         # GCS_CREDENTIALS_FILE
  credentials_json: ""         # GCS_CREDENTIALS_JSON
  bucket: ""                   # GCS_BUCKET
  path: ""                     # GCS_PATH
  compression_level: 1         # GCS_COMPRESSION_LEVEL
  compression_format: gzip     # GCS_COMPRESSION_FORMAT
cos:
  url: ""                      # COS_URL
  timeout: 2m                  # COS_TIMEOUT
  secret_id: ""                # COS_SECRET_ID
  secret_key: ""               # COS_SECRET_KEY
  path: ""                     # COS_PATH
  compression_format: gzip     # COS_COMPRESSION_FORMAT
  compression_level: 1         # COS_COMPRESSION_LEVEL
  debug: false                 # COS_DEBUG
api:
  listen: "localhost:7171"     # API_LISTEN
  enable_metrics: false        # API_ENABLE_METRICS
  enable_pprof: false          # API_ENABLE_PPROF
  username: ""                 # API_USERNAME
  password: ""                 # API_PASSWORD
ftp:
  address: ""                  # FTP_ADDRESS
  timeout: 2m                  # FTP_TIMEOUT
  username: ""                 # FTP_USERNAME
  password: ""                 # FTP_PASSWORD
  tls: false                   # FTP_TLS
  path: ""                     # FTP_PATH
  compression_format: gzip     # FTP_COMPRESSION_FORMAT
  compression_level: 1         # FTP_COMPRESSION_LEVEL
  debug: false                 # FTP_DEBUG
```

## ATTENTION!

Never change files permissions in `/var/lib/clickhouse/backup`.
This path contains hard links. Permissions on all hard links to the same data on disk are always identical.
That means that if you change the permissions/owner/attributes on a hard link in backup path, permissions on files with which ClickHouse works will be changed too.
That might lead to data corruption.

## API
Use the `clickhouse-backup server` command to run as a REST API server. In general, the API attempts to mirror the CLI commands.

> **GET /backup/tables**

Print list of tables: `curl -s localhost:7171/backup/tables | jq .`

> **POST /backup/create**

Create new backup: `curl -s localhost:7171/backup/create -X POST | jq .`
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `name` works the same as specifying a backup name with the CLI.
* Full example: `curl -s 'localhost:7171/backup/create?table=default.billing&name=billing_test' -X POST`

Note: this operation is async, so the API will return once the operation has been started.

> **POST /backup/upload**

Upload backup to remote storage: `curl -s localhost:7171/backup/upload/<BACKUP_NAME> -X POST | jq .`
* Optional query argument `diff-from` works the same as the `--diff-from` CLI argument.

Note: this operation is async, so the API will return once the operation has been started.

> **GET /backup/list**

Print list of backups: `curl -s localhost:7171/backup/list | jq .`

Note: The `Size` field is not populated for local backups.

> **POST /backup/download**

Download backup from remote storage: `curl -s localhost:7171/backup/download/<BACKUP_NAME> -X POST | jq .`

Note: this operation is async, so the API will return once the operation has been started.

> **POST /backup/restore**

Create schema and restore data from backup: `curl -s localhost:7171/backup/restore/<BACKUP_NAME> -X POST | jq .`
* Optional query argument `table` works the same as the `--table value` CLI argument.
* Optional query argument `schema` works the same the `--schema` CLI argument (restore schema only).
* Optional query argument `data` works the same the `--data` CLI argument (restore data only).

> **POST /backup/delete**

Delete specific remote backup: `curl -s localhost:7171/backup/delete/remote/<BACKUP_NAME> -X POST | jq .`

Delete specific local backup: `curl -s localhost:7171/backup/delete/local/<BACKUP_NAME> -X POST | jq .`

> **POST /backup/freeze**

Freeze tables: `curl -s localhost:7171/backup/freeze -X POST | jq .`

> **POST /backup/clean**

Remove data in 'shadow' folder: `curl -s localhost:7171/backup/clean -X POST | jq .`

> **GET /backup/status**

Display list of current async operations: `curl -s localhost:7171/backup/status | jq .`

### API Configuration

> **GET /backup/config**

Get the current running configuration: `curl -s localhost:7171/backup/config | jq -r .Result > current_config.yml`

> **GET /backup/config/default**

Get the default configuration: `curl -s localhost:7171/backup/config/default | jq -r .Result > default_config.yml`

> **POST /backup/config**

Update the current running configuration: `curl -v localhost:7171/backup/config -X POST --data-binary '@new_config.yml'`

Be sure to check return code for config parsing/validation errors.

## Examples

### Simple cron script for daily backup and uploading
```bash
#!/bin/bash
BACKUP_NAME=my_backup_$(date -u +%Y-%m-%dT%H-%M-%S)
clickhouse-backup create $BACKUP_NAME
clickhouse-backup upload $BACKUP_NAME
```

### More use cases of clickhouse-backup
- [How to convert MergeTree to ReplicatedMergeTree](Examples.md#how-to-convert-mergetree-to-replicatedmegretree)
- [How to store backups on NFS or another server](Examples.md#how-to-store-backups-on-nfs-or-another-server)
- [How to move data to another clickhouse server](Examples.md#how-to-move-data-to-another-clickhouse-server)
- [How to reduce number of partitions](Examples.md#How-to-reduce-number-of-partitions)
- [How to monitor that backups created and uploaded correctly](Examples.md#how-to-monitor-that-backups-created-and-uploaded-correctly)
- [How to backup sharded cluster with Ansible](Examples.md#how-to-backup-sharded-cluster-with-ansible)
- [How to backup database with several terabytes of data](Examples.md#how-to-backup-database-with-several-terabytes-of-data)
- [How to use clickhouse-backup in Kubernetes](Examples.md#how-to-use-clickhouse-backup-in-kubernetes)
- [How do incremental backups work to remote storage](Examples.md#how_do_incremental_backups_work_to_remote_storage)
