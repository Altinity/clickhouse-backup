
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
   0.0.2

COMMANDS:
     tables          Print all tables and exit
     freeze          Freeze all or specific tables. You may use this syntax for specify tables [db].[table]
     upload          Upload 'metadata' and 'shadows' directories to s3. Extra files on s3 will be deleted
     download        Download 'metadata' and 'shadows' from s3 to backup folder. Pass filename for archive strategy
     create-tables   Create databases and tables from backup metadata
     restore         Copy data from 'backup' to 'detached' folder and execute ATTACH.
                     You can specify tables [db].[table] and increments via -i flag. -d flag
                     to use legacy partitioning key. -m flag to move files instead of copy.
     default-config  Print default config and exit
     clean           Remove contents from 'shadow' directory
     help, h         Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml")
   --dry-run               Only show what should be uploaded or downloaded but don't actually do it.
                           May still perform S3 requests to get bucket listings and other information
                           though (only for file transfer commands)
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
  overwrite_strategy: "always"
  part_size: 5242880
  delete_extra_files: true
backup:
  strategy: tree
  backups_to_keep: 0
```
