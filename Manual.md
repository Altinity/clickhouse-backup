### CLI command - tables
```
NAME:
   clickhouse-backup tables - List of tables, exclude skip_tables

USAGE:
   clickhouse-backup tables [--tables=<db>.<table>] [--remote-backup=<backup-name>] [--local-backup=<backup-name>] [-f, --format=<text|json|yaml|csv|tsv>] [--all] [--parts] [--partitions]

OPTIONS:
   --all, -a                                        Print table even when match with skip_tables pattern
   --table string, --tables string, -t string       List tables only match with table name patterns, separated by comma, allow ? and * as wildcard
   --remote-backup string                           List tables from a remote backup, including per-table size and parts count
   --local-backup string                            List tables from a local backup (read from disk, no live ClickHouse query), including per-table size and parts count
   --format string, -f string                       Output format (text|json|yaml|csv|tsv)
   --parts system.parts, --list-parts system.parts  Also list every physical part for each table (name, partition_id, size)
      Against the live server, reads name/partition_id/bytes_on_disk from system.parts
      Against --local-backup/--remote-backup, reads part names from backup metadata (partition_id derived from the name, no size available)
   --partitions system.parts, --list-partitions system.parts  Also list the distinct partitions for each table (partition_id, partition, parts count, size), aggregated from parts
      Against the live server, reads partition_id/partition/parts/size from system.parts
      Against --local-backup/--remote-backup, derives partition_id and parts count from part names (no partition value or per-partition size available)
   --help, -h  show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - create
```
NAME:
   clickhouse-backup create - Create new backup

USAGE:
   clickhouse-backup create [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [--diff-from-remote=<backup-name>] [-s, --schema] [--rbac] [--configs] [--named-collections] [--skip-check-parts-columns] [--resume] <backup_name>

DESCRIPTION:
   Create new backup

OPTIONS:
   --table string, --tables string, -t string               Create backup only matched with table name patterns, separated by comma, allow ? and * as wildcard
   --diff-from-remote string                                Create incremental embedded backup or upload incremental object disk data based on other remote backup name
   --partitions partition_id [ --partitions partition_id ]  Create backup only for selected partition names, separated by comma
      If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
      If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
      If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
      If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
      Values depends on field types in your table, use single quotes for String and Date/DateTime related types
      Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, -s                                                                                                                         Backup schemas only, will skip data
   --rbac, --backup-rbac, --do-backup-rbac                                                                                              Backup RBAC related objects
   --configs, --backup-configs, --do-backup-configs                                                                                     Backup 'clickhouse-server' configuration files
   --named-collections, --backup-named-collections, --do-backup-named-collections                                                       Backup named collections
   --rbac-only                                                                                                                          Backup RBAC related objects only, will skip backup data, will backup schema only if --schema added
   --configs-only                                                                                                                       Backup 'clickhouse-server' configuration files only, will skip backup data, will backup schema only if --schema added
   --named-collections-only                                                                                                             Backup named collections only, will skip backup data, will backup schema only if --schema added
   --skip-check-parts-columns                                                                                                           Skip check system.parts_columns to allow backup inconsistent column types for data parts
   --skip-projections db_pattern.table_pattern:projections_pattern [ --skip-projections db_pattern.table_pattern:projections_pattern ]  Skip make hardlinks to *.proj/* files during backup creation, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --resume use_embedded_backup_restore: true, --resumable use_embedded_backup_restore: true                                            Will resume upload for object disk data, hard links on local disk still continue to recreate, not work when use_embedded_backup_restore: true
   --dry-run                                                                                                                            Show tables count and data size which would be created, without creating
   --help, -h                                                                                                                           show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - create_remote
```
NAME:
   clickhouse-backup create_remote - Create and upload new backup

USAGE:
   clickhouse-backup create_remote [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [--diff-from=<local_backup_name>] [--diff-from-remote=<local_backup_name>] [--schema] [--rbac] [--configs] [--named-collections] [--resumable] [--skip-check-parts-columns] <backup_name>

DESCRIPTION:
   Create and upload

OPTIONS:
   --table string, --tables string, -t string               Create and upload backup only matched with table name patterns, separated by comma, allow ? and * as wildcard
   --partitions partition_id [ --partitions partition_id ]  Create and upload backup only for selected partition names, separated by comma
      If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
      If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
      If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
      If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
      Values depends on field types in your table, use single quotes for String and Date/DateTime related types
      Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --diff-from string                                                                                                                   Local backup name which used to upload current backup as incremental
   --diff-from-remote string                                                                                                            Remote backup name which used to upload current backup as incremental
   --schema, -s                                                                                                                         Backup and upload metadata schema only, will skip data backup
   --rbac, --backup-rbac, --do-backup-rbac                                                                                              Backup and upload RBAC related objects
   --configs, --backup-configs, --do-backup-configs                                                                                     Backup and upload 'clickhouse-server' configuration files
   --named-collections, --backup-named-collections, --do-backup-named-collections                                                       Backup and upload named collections and settings
   --rbac-only                                                                                                                          Backup RBAC related objects only, will skip backup data, will backup schema only if --schema added
   --configs-only                                                                                                                       Backup 'clickhouse-server' configuration files only, will skip backup data, will backup schema only if --schema added
   --named-collections-only                                                                                                             Backup named collections only, will skip backup data, will backup schema only if --schema added
   --resume, --resumable                                                                                                                Save intermediate upload state and resume upload if backup exists on remote storage, ignore when 'remote_storage: custom' or 'use_embedded_backup_restore: true'
   --skip-check-parts-columns                                                                                                           Skip check system.parts_columns to allow backup inconsistent column types for data parts
   --skip-projections db_pattern.table_pattern:projections_pattern [ --skip-projections db_pattern.table_pattern:projections_pattern ]  Skip make and upload hardlinks to *.proj/* files during backup creation, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --delete, --delete-source, --delete-local                                                                                            explicitly delete local backup during upload
   --dry-run                                                                                                                            Show tables count and data size which would be created and uploaded, without creating and uploading
   --help, -h                                                                                                                           show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - upload
```
NAME:
   clickhouse-backup upload - Upload backup to remote storage

USAGE:
   clickhouse-backup upload [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--diff-from=<local_backup_name>] [--diff-from-remote=<remote_backup_name>] [--resumable] <backup_name>

OPTIONS:
   --diff-from string                                       Local backup name which used to upload current backup as incremental
   --diff-from-remote string                                Remote backup name which used to upload current backup as incremental
   --table string, --tables string, -t string               Upload data only for matched table name patterns, separated by comma, allow ? and * as wildcard
   --partitions partition_id [ --partitions partition_id ]  Upload backup only for selected partition names, separated by comma
      If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
      If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
      If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
      If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
      Values depends on field types in your table, use single quotes for String and Date/DateTime related types
      Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, -s                                                                                                                         Upload schemas only
   --rbac-only, --rbac                                                                                                                  Upload RBAC related objects only, will skip upload data, will backup schema only if --schema added
   --configs-only, --configs                                                                                                            Upload 'clickhouse-server' configuration files only, will skip upload data, will backup schema only if --schema added
   --named-collections-only, --named-collections                                                                                        Upload named collections and settings only, will skip upload data, will backup schema only if --schema added
   --skip-projections db_pattern.table_pattern:projections_pattern [ --skip-projections db_pattern.table_pattern:projections_pattern ]  Skip make and upload hardlinks to *.proj/* files during backup creation, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --resume, --resumable                                                                                                                Save intermediate upload state and resume upload if backup exists on remote storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'
   --delete, --delete-source, --delete-local                                                                                            explicitly delete local backup during upload
   --dry-run                                                                                                                            Show tables count and data size which would be uploaded, without uploading
   --help, -h                                                                                                                           show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - list
```
NAME:
   clickhouse-backup list - List of backups

USAGE:
   clickhouse-backup list [all|local|remote] [latest|previous]

OPTIONS:
   --format string, -f string  Output format (text|json|yaml|csv|tsv)
   --help, -h                  show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - download
```
NAME:
   clickhouse-backup download - Download backup from remote storage

USAGE:
   clickhouse-backup download [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [-s, --schema] [--resumable] <backup_name>

OPTIONS:
   --table string, --tables string, -t string               Download objects which matched with table name patterns, separated by comma, allow ? and * as wildcard
   --partitions partition_id [ --partitions partition_id ]  Download backup data only for selected partition names, separated by comma
      If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
      If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
      If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
      If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
      Values depends on field types in your table, use single quotes for String and Date/DateTime related types
      Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, --schema-only, -s                    Download schema only
   --rbac-only, --rbac                            Download RBAC related objects only, will skip download data, will download schema only if --schema added
   --configs-only, --configs                      Download 'clickhouse-server' configuration files only, will skip download data, will download schema only if --schema added
   --named-collections-only, --named-collections  Download named collections and settings only, will skip download data, will download schema only if --schema added
   --resume, --resumable                          Save intermediate download state and resume download if backup exists on local storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'
   --hardlink-exists-files                        Create hardlinks for existing files instead of downloading
   --dry-run                                      Show tables count and data size which would be downloaded, without downloading
   --help, -h                                     show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - rebase
```
NAME:
   clickhouse-backup rebase - Copy required parts from `required_backup` chain into remote backup and remove `required_backup` dependency, so backup becomes full

USAGE:
   clickhouse-backup rebase <backup_name>

OPTIONS:
   --help, -h  show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - rebalance
```
NAME:
   clickhouse-backup rebalance - Move data parts inside local backup between disks to match current system.parts layout and storage policy, skip parts on object disks

USAGE:
   clickhouse-backup rebalance [-t, --tables=<db>.<table>] [--dry-run] <backup_name>

OPTIONS:
   --table string, --tables string, -t string  Rebalance only database and objects which matched with table name patterns, separated by comma, allow ? and * as wildcard
   --dry-run                                   Only log which parts would move between disks, change nothing
   --help, -h                                  show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - restore
```
NAME:
   clickhouse-backup restore - Create schema and restore data from backup

USAGE:
   clickhouse-backup restore  [-t, --tables=<db>.<table>] [-m, --restore-database-mapping=<originDB>:<targetDB>[,<...>]] [--tm, --restore-table-mapping=<originTable>:<targetTable>[,<...>]] [--partitions=<partitions_names>] [-s, --schema] [-d, --data] [--rm, --drop] [-i, --ignore-dependencies] [--rbac] [--configs] [--named-collections] [--resume] [--skip-empty-tables] <backup_name>

OPTIONS:
   --table string, --tables string, -t string                                                     Restore only database and objects which matched with table name patterns, separated by comma, allow ? and * as wildcard
   --restore-database-mapping string, -m string [ --restore-database-mapping string, -m string ]  Define the rule to restore data. For the database not defined in this struct, the program will not deal with it.
   --restore-table-mapping string, --tm string [ --restore-table-mapping string, --tm string ]    Define the rule to restore data. For the table not defined in this struct, the program will not deal with it.
   --partitions partition_id [ --partitions partition_id ]                                        Restore backup only for selected partition names, separated by comma
      If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
      If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
      If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
      If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
      Values depends on field types in your table, use single quotes for String and Date/DateTime related types
      Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, -s                                                                                                                         Restore schema only
   --data, -d                                                                                                                           Restore data only
   --rm, --drop                                                                                                                         Drop exists schema objects before restore
   -i, --ignore-dependencies                                                                                                            Ignore dependencies when drop exists schema objects
   --rbac, --restore-rbac, --do-restore-rbac                                                                                            Restore RBAC related objects
   --configs, --restore-configs, --do-restore-configs                                                                                   Restore 'clickhouse-server' CONFIG related files
   --named-collections, --restore-named-collections, --do-restore-named-collections                                                     Restore named collections and settings
   --rbac-only                                                                                                                          Restore RBAC related objects only, will skip restore data, will restore schema only if --schema added
   --configs-only                                                                                                                       Restore 'clickhouse-server' configuration files only, will skip restore data, will restore schema only if --schema added
   --named-collections-only                                                                                                             Restore named collections only, will skip restore data, will restore schema only if --schema added
   --skip-projections db_pattern.table_pattern:projections_pattern [ --skip-projections db_pattern.table_pattern:projections_pattern ]  Skip make hardlinks to *.proj/* files during backup restoring, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --resume, --resumable                                                                                                                Will resume download for object disk data
   --restore-schema-as-attach                                                                                                           Use DETACH/ATTACH instead of DROP/CREATE for schema restoration
   --replicated-copy-to-detached                                                                                                        Copy data to detached folder for Replicated*MergeTree tables but skip ATTACH PART step
   --skip-empty-tables                                                                                                                  Skip restoring tables that have no data (empty tables with only schema)
   --rebind-replica-path-if-exists                                                                                                      Override clickhouse.rebind_replica_path_if_exists, rebind a restored ReplicatedMergeTree to default_replica_path when the original ZK path still has leftover state but our replica entry is absent
   --dry-run                                                                                                                            Show tables count and data size which would be restored, without restoring
   --help, -h                                                                                                                           show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - restore_remote
```
NAME:
   clickhouse-backup restore_remote - Download and restore

USAGE:
   clickhouse-backup restore_remote [--schema] [--data] [-t, --tables=<db>.<table>] [-m, --restore-database-mapping=<originDB>:<targetDB>[,<...>]] [--tm, --restore-table-mapping=<originTable>:<targetTable>[,<...>]] [--partitions=<partitions_names>] [--rm, --drop] [-i, --ignore-dependencies] [--rbac] [--configs] [--named-collections] [--resumable] [--skip-empty-tables] <backup_name>

OPTIONS:
   --table string, --tables string, -t string                                                     Download and restore objects which matched with table name patterns, separated by comma, allow ? and * as wildcard
   --restore-database-mapping string, -m string [ --restore-database-mapping string, -m string ]  Define the rule to restore data. For the database not defined in this struct, the program will not deal with it.
   --restore-table-mapping string, --tm string [ --restore-table-mapping string, --tm string ]    Define the rule to restore data. For the database not defined in this struct, the program will not deal with it.
   --partitions partition_id [ --partitions partition_id ]                                        Download and restore backup only for selected partition names, separated by comma
      If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
      If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
      If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
      If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
      Values depends on field types in your table, use single quotes for String and Date/DateTime related types
      Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, -s                                                                                                                         Download and Restore schema only
   --data, -d                                                                                                                           Download and Restore data only
   --rm, --drop                                                                                                                         Drop schema objects before restore
   -i, --ignore-dependencies                                                                                                            Ignore dependencies when drop exists schema objects
   --rbac, --restore-rbac, --do-restore-rbac                                                                                            Download and Restore RBAC related objects
   --configs, --restore-configs, --do-restore-configs                                                                                   Download and Restore 'clickhouse-server' CONFIG related files
   --named-collections, --restore-named-collections, --do-restore-named-collections                                                     Download and Restore named collections and settings
   --rbac-only                                                                                                                          Restore RBAC related objects only, will skip backup data, will backup schema only if --schema added
   --configs-only                                                                                                                       Restore 'clickhouse-server' configuration files only, will skip backup data, will backup schema only if --schema added
   --named-collections-only                                                                                                             Restore named collections only, will skip restore data, will restore schema only if --schema added
   --skip-projections db_pattern.table_pattern:projections_pattern [ --skip-projections db_pattern.table_pattern:projections_pattern ]  Skip make hardlinks to *.proj/* files during backup restoring, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --resume, --resumable                                                                                                                Save intermediate download state and resume download if backup exists on remote storage, ignored with 'remote_storage: custom' or 'use_embedded_backup_restore: true'
   --restore-schema-as-attach                                                                                                           Use DETACH/ATTACH instead of DROP/CREATE for schema restoration
   --hardlink-exists-files                                                                                                              Create hardlinks for existing files instead of downloading
   --skip-empty-tables                                                                                                                  Skip restoring tables that have no data (empty tables with only schema)
   --rebind-replica-path-if-exists                                                                                                      Override clickhouse.rebind_replica_path_if_exists, rebind a restored ReplicatedMergeTree to default_replica_path when the original ZK path still has leftover state but our replica entry is absent
   --dry-run                                                                                                                            Show tables count and data size which would be downloaded and restored, without downloading and restoring
   --help, -h                                                                                                                           show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - restore_cloud
```
NAME:
   clickhouse-backup restore_cloud - Restore ClickHouse Cloud native S3 backup (Shared engines) as Atomic databases and Replicated*MergeTree tables on the current server

USAGE:
   clickhouse-backup restore_cloud [--bucket=<bucket>] [--region=<region>] [--endpoint=<url>] [--container=<container>] [--base-prefix=<prefix>] [--s3-restore-url=<url>] [--azblob-restore-url=<url>] [-t, --tables=<db>.<table>] [--partitions=<partition_names>] [--restore-on-cluster=<cluster>] [--replicated-zk-path=<path>] [--replicated-replica=<replica>] [--skip-empty-tables] [--continue-on-error] [--drop] [--parallel=<n>] [--dry-run] <backup_prefix>

DESCRIPTION:
   Read the .backup manifest from S3 or AzureBlobStorage, rewrite ClickHouse Cloud DDL (database ENGINE=Shared to Atomic, Shared*MergeTree to Replicated*MergeTree) and run RESTORE TABLE ... FROM S3(...) / AzureBlobStorage(...) with allow_different_database_def/allow_different_table_def
      Credentials and defaults are taken from the s3 config section (also works for GCS via s3->endpoint=https://storage.googleapis.com with HMAC keys), or from the azblob config section when --container / --azblob-restore-url is passed or general->remote_storage is azblob
      When s3->assume_role_arn is set, the manifest is read and RESTORE ... FROM S3(..., extra_credentials(role_arn='...')) is executed with the assumed AWS IAM role, the static keys only sign the STS AssumeRole call (requires ClickHouse 25.8+);
      without any static keys the STS AssumeRole call is signed by the ambient AWS identity instead: shared credentials file / IRSA / EC2-ECS instance profile for the manifest reads, and the ClickHouse server's own environment for the RESTORE statement
      https://github.com/Altinity/clickhouse-backup/issues/1508

OPTIONS:
   --bucket string                                          S3 bucket with the ClickHouse Cloud backup, overrides s3->bucket from config
   --region string                                          AWS region of the bucket, overrides s3->region from config
   --endpoint string                                        Custom S3 endpoint (MinIO, etc.), overrides s3->endpoint from config
   --base-prefix string                                     S3 key prefix of the base backup, for incremental backups with use_base files
   --s3-restore-url string                                  URL passed to RESTORE ... FROM S3('...'), default https://s3.<region>.amazonaws.com/<bucket>/<prefix>
   --container string                                       AzureBlobStorage container with the ClickHouse Cloud backup, overrides azblob->container from config and switches the source to AzureBlobStorage
   --azblob-restore-url string                              Blob endpoint passed to RESTORE ... FROM AzureBlobStorage(...), e.g. http://azurite:10000/devstoreaccount1, when it differs from azblob config section, switches the source to AzureBlobStorage
   --table string, --tables string, -t string               Restore only objects matched with table name patterns, separated by comma, allow ? and * as wildcard
   --partitions partition_id [ --partitions partition_id ]  Restore backup only for selected partition names, separated by comma
      If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
      If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
      If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
      If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
      Values depends on field types in your table, use single quotes for String and Date/DateTime related types
      Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --restore-on-cluster string  Execute CREATE and RESTORE with ON CLUSTER '<cluster>', macros like {cluster} are resolved via system.macros; requires the same shard count in the backup and in the cluster, replica counts may differ (ReplicatedMergeTree replicates the restored data)
   --replicated-zk-path string  First Replicated*MergeTree engine argument when Cloud DDL has none, default '/clickhouse/tables/{uuid}/{shard}'
   --replicated-replica string  Second Replicated*MergeTree engine argument when Cloud DDL has none, default '{replica}'
   --skip-empty-tables          Skip objects with no data/<db>/<table>/ files in the backup, also skips views and dictionaries
   --continue-on-error          Continue with the next object after an error, exit code is still non-zero
   --drop                       Execute DROP TABLE / DICTIONARY IF EXISTS ... SYNC before CREATE, to re-run a failed or interrupted restore into non-empty tables
   --parallel int               How many tables of one database restore concurrently (dictionaries and tables first, then views), default is the number of CPU cores (default: 0)
   --dry-run                    Only log DDL and RESTORE statements which would be executed, without executing
   --help, -h                   show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - delete
```
NAME:
   clickhouse-backup delete - Delete specific backup

USAGE:
   clickhouse-backup delete [--force] <local|remote> <backup_name>

OPTIONS:
   --force, -f  Delete the backup even when other backups depend on it via required_backup, breaks the incremental backups chain, also skips general.rebase_during_delete
   --dry-run    Show tables count and data size which would be deleted, without deleting
   --help, -h   show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - default-config
```
NAME:
   clickhouse-backup default-config - Print default config

USAGE:
   clickhouse-backup default-config [options]

OPTIONS:
   --help, -h  show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - print-config
```
NAME:
   clickhouse-backup print-config - Print current config merged with environment variables

USAGE:
   clickhouse-backup print-config [options]

OPTIONS:
   --help, -h  show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - clean
```
NAME:
   clickhouse-backup clean - Remove data in 'shadow' folder from all 'path' folders available from 'system.disks'

USAGE:
   clickhouse-backup clean [options]

OPTIONS:
   --help, -h  show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - clean_remote_broken
```
NAME:
   clickhouse-backup clean_remote_broken - Remove all broken remote backups

USAGE:
   clickhouse-backup clean_remote_broken [--include=glob ...]

OPTIONS:
   --include string [ --include string ]  Glob (path.Match syntax) to scope cleanup only to broken backup names matching these patterns; can be passed multiple times; if omitted, all broken backups are deleted
   --help, -h                             show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - clean_local_broken
```
NAME:
   clickhouse-backup clean_local_broken - Remove all broken local backups

USAGE:
   clickhouse-backup clean_local_broken [options]

OPTIONS:
   --help, -h  show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - clean_broken_retention
```
NAME:
   clickhouse-backup clean_broken_retention - Remove orphan entries under remote `path` and `object_disks_path` that are not in the live backup list

USAGE:
   clickhouse-backup clean_broken_retention [--commit] [--include=glob ...] [--exclude=glob ...]

DESCRIPTION:
   Walks top-level of remote `path` and `object_disks_path`, batch-deletes (with retry) every entry that is not a live backup and is not excluded by --exclude globs and is matched by --include globs (if provided). Object disk orphans are deleted in parallel with progress tracking. Pass --commit to actually delete; without it the command only logs what would be deleted.

OPTIONS:
   --include string [ --include string ]  Glob (path.Match syntax) to scope cleanup only to backup names matching these patterns; can be passed multiple times; if omitted, all orphans are candidates
   --exclude string [ --exclude string ]  Glob (path.Match syntax) of backup names to preserve even if they appear as orphans; can be passed multiple times
   --commit                               Actually delete orphans; without this flag the command only logs what would be deleted
   --help, -h                             show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - watch
```
NAME:
   clickhouse-backup watch - Run infinite loop which create full + incremental backup sequence to allow efficient backup sequences

USAGE:
   clickhouse-backup watch [--watch-interval=1h] [--full-interval=24h] [--watch-backup-name-template=shard{shard}-{type}-{time:20060102150405}] [--schedule=name=<name>,full=<cron>,increment=<cron>] [-t, --tables=<db>.<table>] [--partitions=<partitions_names>] [--schema] [--rbac] [--configs] [--skip-check-parts-columns]

DESCRIPTION:
   Execute create_remote + delete local, create full backup every `--full-interval`, create and upload incremental backup every `--watch-interval` use previous backup as base with `--diff-from-remote` option, use `backups_to_keep_remote` config option for properly deletion remote backups, will delete old backups which not have references from other backups. Use `--schedule` instead of intervals to run backups on cron expressions

OPTIONS:
   --watch-interval string                                  Interval for run 'create_remote' + 'delete local' for incremental backup, look format https://pkg.go.dev/time#ParseDuration
   --full-interval string                                   Interval for run 'create_remote'+'delete local' when stop create incremental backup sequence and create full backup, look format https://pkg.go.dev/time#ParseDuration
   --watch-backup-name-template string                      Template for new backup name, could contain names from system.macros, {type} - full or incremental and {time:LAYOUT}, look to https://go.dev/src/time/format.go for layout examples
   --schedule string [ --schedule string ]                  Named cron driven backup chain in name=<name>,full=<cron>[,increment=<cron>][,full_type=create|rebase][,delete_previous_cycle=true|false] format, can be specified multiple times, mutually exclusive with --watch-interval and --full-interval
                                                            cron expression contains standard 5 fields, optional leading seconds field and @every/@daily descriptors, see https://pkg.go.dev/github.com/robfig/cron/v3#hdr-CRON_Expression_Format
                                                            name added as prefix to --watch-backup-name-template to isolate backup chains
                                                            full_type=rebase creates scheduled full backup as increment + rebase command, server-side copy of previous chain instead of full re-upload
                                                            delete_previous_cycle=true deletes all older backups of the chain after successful full backup
   --table string, --tables string, -t string               Create and upload only objects which matched with table name patterns, separated by comma, allow ? and * as wildcard
   --partitions partition_id [ --partitions partition_id ]  Partitions names, separated by comma
      If PARTITION BY clause returns numeric not hashed values for partition_id field in system.parts table, then use --partitions=partition_id1,partition_id2 format
      If PARTITION BY clause returns hashed string values, then use --partitions=('non_numeric_field_value_for_part1'),('non_numeric_field_value_for_part2') format
      If PARTITION BY clause returns tuple with multiple fields, then use --partitions=(numeric_value1,'string_value1','date_or_datetime_value'),(...) format
      If you need different partitions for different tables, then use --partitions=db.table1:part1,part2 --partitions=db.table?:*
      Values depends on field types in your table, use single quotes for String and Date/DateTime related types
      Look at the system.parts partition and partition_id fields for details https://clickhouse.com/docs/en/operations/system-tables/parts/
   --schema, -s                                                                                                                         Schemas only
   --rbac, --backup-rbac, --do-backup-rbac                                                                                              Backup RBAC related objects
   --configs, --backup-configs, --do-backup-configs                                                                                     Backup `clickhouse-server' configuration files
   --named-collections, --backup-named-collections, --do-backup-named-collections                                                       Backup named collections and settings
   --skip-check-parts-columns                                                                                                           Skip check system.parts_columns to allow backup inconsistent column types for data parts
   --skip-projections db_pattern.table_pattern:projections_pattern [ --skip-projections db_pattern.table_pattern:projections_pattern ]  Skip make and upload hardlinks to *.proj/* files during backup creation, format db_pattern.table_pattern:projections_pattern, use https://pkg.go.dev/path/filepath#Match syntax
   --delete, --delete-source, --delete-local                                                                                            explicitly delete local backup during upload
   --help, -h                                                                                                                           show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - acvp
```
NAME:
   clickhouse-backup acvp - Run ACVP wrapper protocol over stdin/stdout

USAGE:
   clickhouse-backup acvp

OPTIONS:
   --help, -h  show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
### CLI command - server
```
NAME:
   clickhouse-backup server - Run API server

USAGE:
   clickhouse-backup server [options]

OPTIONS:
   --watch                                                                         Run watch go-routine for 'create_remote' + 'delete local', after API server startup
   --watch-interval string                                                         Interval for run 'create_remote' + 'delete local' for incremental backup, look format https://pkg.go.dev/time#ParseDuration
   --full-interval string                                                          Interval for run 'create_remote'+'delete local' when stop create incremental backup sequence and create full backup, look format https://pkg.go.dev/time#ParseDuration
   --watch-backup-name-template string                                             Template for new backup name, could contain names from system.macros, {type} - full or incremental and {time:LAYOUT}, look to https://go.dev/src/time/format.go for layout examples
   --schedule string [ --schedule string ]                                         Named cron driven backup chain for watch in name=<name>,full=<cron>[,increment=<cron>][,full_type=create|rebase][,delete_previous_cycle=true|false] format, can be specified multiple times, mutually exclusive with --watch-interval and --full-interval
   --rbac, --backup-rbac, --do-backup-rbac                                         Backup RBAC related objects during --watch
   --configs, --backup-configs, --do-backup-configs                                Backup `clickhouse-server' configuration files during --watch
   --named-collections, --backup-named-collections, --do-backup-named-collections  Backup named collections and settings during --watch
   --watch-delete-source, --watch-delete-local                                     explicitly delete local backup during upload in watch
   --help, -h                                                                      show help

GLOBAL OPTIONS:
   --config string, -c string                                                                   Config 'FILE' name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --environment-override string, --env string [ --environment-override string, --env string ]  override any environment variable via CLI parameter
```
