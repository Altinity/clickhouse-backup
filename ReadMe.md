
# clickhouse-backup

[![Build Status](https://travis-ci.org/AlexAkulov/clickhouse-backup.svg?branch=master)](https://travis-ci.org/AlexAkulov/clickhouse-backup)
[![Telegram](https://img.shields.io/badge/telegram-join%20chat-3796cd.svg)](https://t.me/clickhousebackup)

Tool for backup ClickHouse to s3


## Usage

```
NAME:
   clickhouse-backup - Backup ClickHouse to s3

USAGE:
   clickhouse-backup [global options] command [command options] [arguments...]

VERSION:
   unknown

COMMANDS:
     tables          Print all tables and exit
     list            Print backups list and exit
     freeze          Freeze all or specific tables. You can specify tables via flag -t db.[table]
     create          Create new backup of all or specific tables. You can specify tables via flag -t [db].[table]
     upload          Upload backup to s3
     download        Download backup from s3 to backup folder
     restore-schema  Create databases and tables from backup metadata
     restore-data    Copy data from 'backup' to 'detached' folder and execute ATTACH. You can specify tables like 'db.[table]' via flag -t and increments via -i flag
     default-config  Print default config and exit
     clean           Clean backup data from shadow folder
     help, h         Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml")
   --dry-run               Only show what should be uploaded or downloaded but don't actually do it. May still perform S3 requests to get bucket listings and other information though (only for file transfer commands)
   --help, -h              show help
   --version, -v           print the version
```

### Default Config
```
clickhouse:
  username: default
  password: ""
  host: localhost
  port: 9000
  data_path: ""
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
  compression_format: lz4
```
