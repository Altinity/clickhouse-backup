
# clickhouse-backup

[![Build Status](https://travis-ci.org/AlexAkulov/clickhouse-backup.svg?branch=master)](https://travis-ci.org/AlexAkulov/clickhouse-backup)
[![Telegram](https://img.shields.io/badge/telegram-join%20chat-3796cd.svg)](https://t.me/clickhousebackup)
[![Docker Image](https://img.shields.io/docker/pulls/alexakulov/clickhouse-backup.svg)](https://hub.docker.com/r/alexakulov/clickhouse-backup)

Tool for easy ClickHouse backup and restore with S3 support

## Usage

```
NAME:
   clickhouse-backup - Tool for easy backup of ClickHouse with S3 support

USAGE:
   clickhouse-backup <command> [--dry-run] [--table=<db>.<table>] <backup_name>

VERSION:
   unknown

DESCRIPTION:
   Run as root or clickhouse user

COMMANDS:
     tables          Print list of tables and exit
     list            Print list of backups and exit
     freeze          Freeze all or specific tables
     create          Create new backup of all or specific tables
     upload          Upload backup to s3
     download        Download backup from s3 to backup folder
     restore-schema  Create databases and tables from backup metadata
     restore-data    Copy data to 'detached' folder and execute ATTACH
     default-config  Print default config and exit
     clean           Clean backup data from shadow folder
     help, h         Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml")
   --dry-run               Only show what should be uploaded or downloaded but don't actually do it. May still perform S3 requests to get bucket listings and other information though (only for file transfer commands)
   --help, -h              show help
   --version, -v           print the version
```

## Usage in Docker

```
docker run --rm -it --network host -v "/var/lib/clickhouse:/var/lib/clickhouse" -e CLICKHOUSE_PASSWORD=password -e S3_ACCESS_KEY=access_key -e S3_SECRET_KEY=secret alexakulov/clickhouse-backup --help
```

### Default Config
```
clickhouse:
  username: default
  password: ""
  host: localhost
  port: 9000
  data_path: ""
  skip_tables:
    - system.*
s3:
  access_key: ""
  secret_key: ""
  bucket: ""
  endpoint: ""
  region: us-east-1
  acl: private
  force_path_style: false
  path: ""
  disable_ssl: false
  disable_progress_bar: false
  # Define behavior for rewrite exists files with the same size. Must set to "skip", "etag" or "always"
  # "skip" - the fastest but can make backup inconsistently
  # "etag" - calculate etag for local files, set this if your network is very slow
  overwrite_strategy: always
  part_size: 5242880
  delete_extra_files: true
  strategy: archive
  backups_to_keep_local: 0
  backups_to_keep_s3: 0
  compression_level: 1
  # supported: 'tar', 'lz4', 'bzip2', 'gzip', 'sz', 'xz'
  compression_format: lz4
```
