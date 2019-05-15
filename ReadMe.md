
# clickhouse-backup

[![Build Status](https://travis-ci.org/AlexAkulov/clickhouse-backup.svg?branch=master)](https://travis-ci.org/AlexAkulov/clickhouse-backup)
[![Telegram](https://img.shields.io/badge/telegram-join%20chat-3796cd.svg)](https://t.me/clickhousebackup)
[![Docker Image](https://img.shields.io/docker/pulls/alexakulov/clickhouse-backup.svg)](https://hub.docker.com/r/alexakulov/clickhouse-backup)

Tool for easy ClickHouse backup and restore with S3 support

## Features

- Easy creating and restoring backups of all or specific tables
- Most efficient AWS S3 uploading and downloading with streaming archiving and extracting
- Support of incremental backups on S3

## Download

- Grab the latest binary from the [releases](https://github.com/AlexAkulov/clickhouse-backup/releases) page and decompress with:

```shell
tar -zxvf clickhouse-backup.tar.gz
```


- Or use the official tiny Docker image and run it like:

```shell
docker run --rm -it --network host -v "/var/lib/clickhouse:/var/lib/clickhouse" \
   -e CLICKHOUSE_PASSWORD=password -e S3_ACCESS_KEY=access_key -e S3_SECRET_KEY=secret \
   alexakulov/clickhouse-backup --help
```

- Or get from the sources:

```shell
go get github.com/AlexAkulov/clickhouse-backup
```

## Usage

```
NAME:
   clickhouse-backup - Tool for easy backup of ClickHouse with S3 support

USAGE:
   clickhouse-backup <command> [--dry-run] [-t, --tables=<db>.<table>] <backup_name>

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
   --dry-run               [DEPRECATED] Only show what should be uploaded or downloaded but don't actually do it. May still perform S3 requests to get bucket listings and other information though (only for file transfer commands)
   --help, -h              show help
   --version, -v           print the version
```

### Default Config

```yaml
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

## Examples

### Simple cron script for daily backup and uploading
```bash
#!/bin/bash
BACKUP_NAME=my_backup_$(date -u +%Y-%m-%dT%H-%M-%S)
clickhouse-backup create $BACKUP_NAME
clickhouse-backup upload $BACKUP_NAME
```

### Ansible script for backup sharded cluster
You can use this playbook for daily backup of sharded cluster. 
On the first day of month full backup will be uploaded and increment on the other days.
Use https://healthchecks.io for monitoring creating and uploading of backups. 

```yaml
- hosts: clickhouse-cluster
  become: yes
  vars:
    healthchecksio_clickhouse_backup_id: "get on https://healthchecks.io"
    healthchecksio_clickhouse_upload_id: "..."
  roles:
    - clickhouse-backup
  tasks:
    - block:
        - uri: url="https://hc-ping.com/{{ healthchecksio_clickhouse_backup_id }}/start"
        - set_fact: backup_name="{{ lookup('pipe','date -u +%Y-%m-%d') }}-{{ clickhouse_shard }}"
        - set_fact: yesterday_backup_name="{{ lookup('pipe','date --date=yesterday -u +%Y-%m-%d') }}-{{ clickhouse_shard }}"
        - set_fact: current_day="{{ lookup('pipe','date -u +%d') }}"
        - name: create new backup
          shell: "clickhouse-backup create {{ backup_name }}"
          register: out
        - debug: var=out.stdout_lines
        - uri: url="https://hc-ping.com/{{ healthchecksio_clickhouse_backup_id }}"
      rescue:
        - uri: url="https://hc-ping.com/{{ healthchecksio_clickhouse_backup_id }}/fail"
    - block:
        - uri: url="https://hc-ping.com/{{ healthchecksio_clickhouse_upload_id }}/start"
        - name: upload full backup
          shell: "clickhouse-backup upload {{ backup_name }}"
          register: out
          when: current_day == '01'
        - name: upload diff backup
          shell: "clickhouse-backup upload {{ backup_name }} --diff-from {{ yesterday_backup_name }}"
          register: out
          when: current_day != '01'
        - debug: var=out.stdout_lines
        - uri: url="https://hc-ping.com/{{ healthchecksio_clickhouse_upload_id }}"
      rescue:
        - uri: url="https://hc-ping.com/{{ healthchecksio_clickhouse_upload_id }}/fail"
```