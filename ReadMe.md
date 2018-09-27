# clickhouse-backup

Tool for backup ClickHouse to s3

**IN PROGRESS TOO :)**

## Usage

```
NAME:
   clickhouse-backup - Backup ClickHouse to s3

USAGE:
   clickhouse-backup [global options] command [command options] [arguments...]

VERSION:
   0.0.1

COMMANDS:
     backup          Freeze tables
     upload          Upload freezed tables to s3
     download        NOT IMPLEMENTED! Download tables from s3 to rigth path
     restore         NOT IMPLEMENTED! Restore downloaded data
     default-config  Print default config and exit
     help, h         Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "config.yml")
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
```
