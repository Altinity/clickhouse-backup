# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.7.211116.1144005.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Specification
from testflows.core import Requirement

Heading = Specification.Heading

RQ_SRS_013_ClickHouse_BackupUtility_Generic_AllDataTypes = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.AllDataTypes',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support backing up and restoring tables containing any existing data type.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.1')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_RestorePartiallyDropped = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.RestorePartiallyDropped',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support restoring table that has lost some piece of data.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.2')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_BackupDuringMutation = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.BackupDuringMutation',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL be able to create a backup if there is an ongoing mutation. Restoring from such backups SHALL also be possible.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.3')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_AllTables = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.AllTables',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support easy backup and restore of all [ClickHouse] tables that SHALL result in table having identical state as at the time of backup.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.4')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_SpecificTables = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support easy backup and restore of specific [ClickHouse] tables that SHALL result in table having identical state as at the time of backup.\n'
        '\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.5')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_SpecificTables_Performance = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables.Performance',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL create backups for a particular table with high performance due to avoiding scanning whole `system.tables` when set `table` query string parameter or `--tables` CLI parameter.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.6')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_SpecificTables_ManyColumns = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables.ManyColumns',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support creating backups and restoring from them for a table with high number of columns.\n'
        '\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.7')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedTable = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.ReplicatedTable',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support easy backup and restore of [ClickHouse] replicated tables from the backup\n'
        'that SHALL result in table having identical state as at the time of backup.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.8')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_MultipleBackups_EfficientStorageByUsingHardLinks = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.MultipleBackups.EfficientStorageByUsingHardLinks',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support efficient storage of multiple backups on the file system by\n'
        'manipulating hardlinks used by [ClickHouse] parts.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.9')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_CompressionLevel = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.CompressionLevel',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support uploading to/downloading from remote storage with specified compression level.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.10')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_ClickHouseVersions = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.ClickHouseVersions',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support [ClickHouse] versions above `1.1.54390`.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.11')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_TieredStorage = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.TieredStorage',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `tiered storage` that was added in ClickHouse `19.15.2.2`.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.12')

RQ_SRS_013_ClickHouse_BackupUtility_Generic_EncryptedStorage = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.EncryptedStorage',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support creating backups from [encrypted storages](https://clickhouse.tech/docs/en/operations/storing-data/#encrypted-virtual-file-system) and restoring them.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.13')

RQ_SRS_013_ClickHouse_BackupUtility_Views_View = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Views.View',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support proper handling of `View` over `MergeTree` table engines.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.2.1')

RQ_SRS_013_ClickHouse_BackupUtility_Views_MaterializedView = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Views.MaterializedView',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support proper handling of `MaterializedView` over `MergeTree` table engines.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.2.2')

RQ_SRS_013_ClickHouse_BackupUtility_Views_LiveView = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Views.LiveView',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support proper handling of `LiveView` over `MergeTree` table engines.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.2.3')

RQ_SRS_013_ClickHouse_BackupUtility_Views_WindowView = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Views.WindowView',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support proper handling of `WindowView` over `MergeTree` table engines.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.2.4')

RQ_SRS_013_ClickHouse_BackupUtility_Views_NestedViews = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Views.NestedViews',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support proper handling of nested views, e.g. `View` over `View`.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.2.5')

RQ_SRS_013_ClickHouse_BackupUtility_TableEngines_MergeTree = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.MergeTree',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support only `MergeTree` family table engines.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.3.1')

RQ_SRS_013_ClickHouse_BackupUtility_TableEngines_OtherEngines = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.OtherEngines',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility workflow SHALL NOT be broken by presence of tables with engines other that listed above.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.3.2')

RQ_SRS_013_ClickHouse_BackupUtility_TableEngines_OtherEngines_Kafka = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.OtherEngines.Kafka',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL only support creating table schema backup and restore for `Kafka` table engine.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.3.3')

RQ_SRS_013_ClickHouse_BackupUtility_DatabaseEngines_Atomic = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.Atomic',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `MergeTree` family table engines with `Atomic` database engine.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.4.1')

RQ_SRS_013_ClickHouse_BackupUtility_DatabaseEngines_MaterializedMySQL = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.MaterializedMySQL',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `MergeTree` family table engines with `MaterializedMySQL` database engine.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.4.2')

RQ_SRS_013_ClickHouse_BackupUtility_DatabaseEngines_MaterializedPostgreSQL = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.MaterializedPostgreSQL',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `MergeTree` family table engines with `MaterializedPostgreSQL` database engine.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.4.3')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support creating and uploading backups on cloud storage.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.1')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_IncrementalBackups = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.IncrementalBackups',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support uploading an incremental backup to cloud storage.\n'
        'This means a data chunk will be uploaded only in case it is not contained in original backup.\n'
        'Incremental backup can be based on a full backup or on another incremental backup.\n'
        '\n'
        'The [clickhouse-backup] utility SHALL also support incremental backups after\n'
        'one or more mutations have been performed on a table using the `ALTER` statement.\n'
        'For example,\n'
        '\n'
        '* `ALTER TABLE <table> DELETE`\n'
        '* `ALTER TABLE <table> UPDATE`\n'
        '* `ALTER TABLE <table> ADD COLUMN`\n'
        '* `ALTER TABLE <table> CLEAR COLUMN`\n'
        '* `ALTER TABLE <table> DROP COLUMN`\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.2')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_CompressionFormats = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.CompressionFormats',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support storing backup on a cloud storage in following compression formats: `tar`, `bzip2`, `gzip`, `sz`, `xz`, `br/brotli`, `zstd`. In case of other value, error shall be thrown and no backup uploaded.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.3')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_S3_Minio = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.S3.Minio',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support storing backups on a Minio S3 storage. It also SHALL be possible to restore from such backups.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.4')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_S3_AWS = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.S3.AWS',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support storing backups on [AWS](https://aws.amazon.com/products/storage/) cloud servers. It also SHALL be possible to restore from such backups.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.5')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_GCS = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.GCS',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support storing backups on [GCS](https://cloud.google.com/storage) cloud servers. It also SHALL be possible to restore from such backups.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.6')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_Azure = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.Azure',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support storing backups on [Azure](https://docs.microsoft.com/en-us/azure/storage/common/storage-introduction) cloud servers. It also SHALL be possible to restore from such backups.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.7')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_TencentCOS = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.TencentCOS',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support storing backups on [Tencent COS](https://intl.cloud.tencent.com/product/cos) cloud servers. It also SHALL be possible to restore from such backups.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.8')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_FTP = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.FTP',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support storing backups on a remote FTP server. It also SHALL be possible to restore from such backup.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.9')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_SFTP = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.SFTP',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support storing backups on a remote SFTP (also known as Secure FTP) server. It also SHALL be possible to restore from such backup.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.10')

RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_GreaterThan5TB = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.GreaterThan5TB',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support uploading backups greater than `5TB` using partitioning by 5TB chunks.\n'
        'More info can be found [here](https://stackoverflow.com/questions/54012602/is-there-a-file-size-limit-for-amazon-s3#:~:text=While%20the%20maximum%20file%20size,any%20files%20larger%20than%20100MB)\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.11')

RQ_SRS_013_ClickHouse_BackupUtility_Configs_Backup = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Configs.Backup',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support backing up ClickHouse configs.\n'
        '\n'
        'By configs we mean the following files:\n'
        '    * `/etc/clickhouse-server/config.xml`\n'
        '    * `/etc/clickhouse-server/config.d/*.xml`\n'
        '    * `/etc/clickhouse-server/users.xml`\n'
        '    * `/etc/clickhouse-server/users.d/*.xml`\n'
        '\n'
        'Listed above are default config files. However, these values may be overriden on clickhouse-server launch.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.6.1')

RQ_SRS_013_ClickHouse_BackupUtility_Configs_Restore = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Configs.Restore',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support restoring ClickHouse configs from backup. This should be done automatically in case configs backup is present. If configs are found in backup, ClickHouse server SHALL restart automatically.\n'
        '\n'
        'By configs we mean the following files:\n'
        '    * `/etc/clickhouse-server/config.xml`\n'
        '    * `/etc/clickhouse-server/config.d/*.xml`\n'
        '    * `/etc/clickhouse-server/users.xml`\n'
        '    * `/etc/clickhouse-server/users.d/*.xml`\n'
        '\n'
        'Listed above are default config files. However, these values may be overriden on clickhouse-server launch.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.6.2')

RQ_SRS_013_ClickHouse_BackupUtility_RBAC_Backup = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.RBAC.Backup',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support backing up ClickHouse RBAC objects. By RBAC objects we mean the following:\n'
        '    * `USER`\n'
        '    * `ROLE`\n'
        '    * `ROW POLICY`\n'
        '    * `SETTINGS PROFILE`\n'
        '    * `QUOTA`\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.7.1')

RQ_SRS_013_ClickHouse_BackupUtility_RBAC_Restore = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.RBAC.Restore',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support restoring ClickHouse RBAC objects from backup. This should be done automatically in case RBAC objects backup is present. If RBAC objects are found in backup, ClickHouse server SHALL restart automatically. By RBAC objects we mean the following:\n'
        '    * `USER`\n'
        '    * `ROLE`\n'
        '    * `ROW POLICY`\n'
        '    * `SETTINGS PROFILE`\n'
        '    * `QUOTA`\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.7.2')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_Usage = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Usage',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL have CLI with the following usage\n'
        '\n'
        '```bash\n'
        'USAGE:\n'
        '   clickhouse-backup <command> [-t, --tables=<db>.<table>] <backup_name>\n'
        '\n'
        'DESCRIPTION:\n'
        "   Run as 'root' or 'clickhouse' user\n"
        '\n'
        'COMMANDS:\n'
        '   tables          Print list of tables\n'
        '   create          Create new backup\n'
        '   create_remote   Create and upload\n'
        '   upload          Upload backup to remote storage\n'
        '   list            Print list of backups\n'
        '   download        Download backup from remote storage\n'
        '   restore         Create schema and restore data from backup\n'
        '   restore_remote  Download and restore\n'
        '   delete          Delete specific backup\n'
        '   default-config  Print default config\n'
        '   server          Run API server\n'
        '   help, h         Shows a list of commands or help for one command\n'
        '\n'
        'GLOBAL OPTIONS:\n'
        '   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '   --help, -h              show help\n'
        '   --version, -v           print the version\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.1')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_Tables = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Tables',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `tables` option to list of tables that are present in the system.\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup tables\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.2')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_Create = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Create',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `create` option to create new backup.\n'
        '\n'
        '* `-t, --table, --tables` option SHALL specify table(s) for backup. The option SHALL\n'
        '   support match expression syntax and SHALL support more than one expression separated by\n'
        '   a comma.\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup create [-t, --tables=<db>.<table>] <backup_name>\n'
        '\n'
        'DESCRIPTION:\n'
        '   Create new backup\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE                   Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '   --table value, --tables value, -t value  \n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.3')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_CreateRemote = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.CreateRemote',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `create_remote` option to create new backup.\n'
        '\n'
        '* `-t, --table, --tables` option SHALL specify table(s) for backup. The option SHALL\n'
        '   support match expression syntax and SHALL support more than one expression separated by\n'
        '   a comma.\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup create_remote [-t, --tables=<db>.<table>] [--diff-from=<backup_name>] [--delete] <backup_name>\n'
        '\n'
        'DESCRIPTION:\n'
        '   Create and upload\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE                   Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '   --table value, --tables value, -t value  \n'
        '   --diff-from value                        \n'
        '   --schema, -s                             Schemas only\n'
        '\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.4')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_MatchExpression = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.MatchExpression',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support match expression syntax for specifying `skip_tables` option in configuration and for use with `--table` flag.\n'
        'Syntax must be based on\n'
        'the https://golang.org/pkg/path/filepath/#Match that SHALL have the following\n'
        'pattern syntax:\n'
        '\n'
        '```\n'
        'pattern:\n'
        '\t{ term }\n'
        'term:\n'
        "\t'*'         matches any sequence of non-Separator characters\n"
        "\t'?'         matches any single non-Separator character\n"
        "\t'[' [ '^' ] { character-range } ']'\n"
        '\t            character class (must be non-empty)\n'
        "\tc           matches character c (c != '*', '?', '\\\\', '[')\n"
        "\t'\\\\' c      matches character c\n"
        '\n'
        'character-range:\n'
        "\tc           matches character c (c != '\\\\', '-', ']')\n"
        "\t'\\\\' c      matches character c\n"
        "\tlo '-' hi   matches character c for lo <= c <= hi\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.5')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_Upload = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Upload',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `upload` option to upload backup to remote storage. This command SHALL automatically upload ClickHouse configs and RBAC objects backups if they are present (in case backup was created with `--configs` and/or `--rbac`).\n'
        '\n'
        '* `--diff-from` option SHALL specify backup to be used for difference\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup upload [--diff-from=<backup_name>] <backup_name>\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '   --diff-from value   \n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.6')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_List = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.List',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `list` option to print the list of backups.\n'
        '\n'
        '* type `all|local|remote` option SHALL specify backup type\n'
        '* version `latest|penult` option SHALL specify backup for difference with `latest` selecting the latest backup\n'
        '  and `penult` selecting the backup right before the latest.\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup list [all|local|remote] [latest|penult]\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.7')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_Download = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Download',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `download` option to download backup from remote storage. This command SHALL automatically download ClickHouse configs and RBAC objects backups if they are present (in case backup was created with `--configs` and/or `--rbac`).\n'
        '\n'
        '* `backup_name` SHALL specify the name of the backup\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup download <backup_name>\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.8')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_Restore = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Restore',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `restore` option to create schema and restore data from backup.\n'
        '\n'
        '* `--table, --tables, -t` SHALL specify table(s) name\n'
        '* `-s, --schema` SHALL specify to restore schema only\n'
        '* `-d, --data` SHALL specify to restore data only\n'
        '* `--rm, --drop` SHALL specify to drop table before restore\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup restore [--schema] [--data] [-t, --tables=<db>.<table>] <backup_name>\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE                   Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '   --table value, --tables value, -t value  \n'
        '   --schema, -s                             Restore schema only\n'
        '   --data, -d                               Restore data only\n'
        '   --rm, --drop                             Drop table before restore\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.9')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_RestoreRemote = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.RestoreRemote',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `restore_remote` option to create schema and restore data from remote backup.\n'
        '\n'
        '* `--table, --tables, -t` SHALL specify table(s) name\n'
        '* `-s, --schema` SHALL specify to restore schema only\n'
        '* `-d, --data` SHALL specify to restore data only\n'
        '* `--rm, --drop` SHALL specify to drop table before restore\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup create_remote [-t, --tables=<db>.<table>] [--diff-from=<backup_name>] [--delete] <backup_name>\n'
        '\n'
        'DESCRIPTION:\n'
        '   Create and upload\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE                   Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '   --table value, --tables value, -t value  \n'
        '   --diff-from value                        \n'
        '   --schema, -s                             Schemas only\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.10')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_Delete = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Delete',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `delete` option to delete specific backup.\n'
        '\n'
        '* type `local|remote` SHALL specify backup type\n'
        '* `backup_name` SHALL specify the name of the backup\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup delete <local|remote> <backup_name>\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.11')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_DefaultConfig = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.DefaultConfig',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `default-config` option to print default config.\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup default-config [command options] [arguments...]\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.12')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_Server = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Server',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `server` option to run REST API server.\n'
        '\n'
        '```\n'
        'USAGE:\n'
        '   clickhouse-backup server [command options] [arguments...]\n'
        '\n'
        'OPTIONS:\n'
        '   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.13')

RQ_SRS_013_ClickHouse_BackupUtility_CLI_Help = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Help',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `help` option to show the list of commands or help for one command.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.8.14')

RQ_SRS_013_ClickHouse_BackupUtility_Configuration = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.Configuration',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support the following options in the configuration file.\n'
        'All options SHALL be overwritable via environment variables.\n'
        '\n'
        '```bash\n'
        'general:\n'
        '  remote_storage: none           # REMOTE_STORAGE\n'
        '  max_file_size: 107374182400    # MAX_FILE_SIZE\n'
        '  disable_progress_bar: false    # DISABLE_PROGRESS_BAR\n'
        '  backups_to_keep_local: 0       # BACKUPS_TO_KEEP_LOCAL\n'
        '  backups_to_keep_remote: 0      # BACKUPS_TO_KEEP_REMOTE\n'
        '  log_level: info                # LOG_LEVEL\n'
        '  allow_empty_backups: false     # ALLOW_EMPTY_BACKUPS\n'
        'clickhouse:\n'
        '  username: default                # CLICKHOUSE_USERNAME\n'
        '  password: ""                     # CLICKHOUSE_PASSWORD\n'
        '  host: localhost                  # CLICKHOUSE_HOST\n'
        '  port: 9000                       # CLICKHOUSE_PORT\n'
        '  disk_mapping: {}                 # CLICKHOUSE_DISK_MAPPING\n'
        '  skip_tables:                     # CLICKHOUSE_SKIP_TABLES\n'
        '    - system.*\n'
        '  timeout: 5m                      # CLICKHOUSE_TIMEOUT\n'
        '  freeze_by_part: false            # CLICKHOUSE_FREEZE_BY_PART\n'
        '  secure: false                    # CLICKHOUSE_SECURE\n'
        '  skip_verify: false               # CLICKHOUSE_SKIP_VERIFY\n'
        '  sync_replicated_tables: true     # CLICKHOUSE_SYNC_REPLICATED_TABLES\n'
        '  log_sql_queries: true            # CLICKHOUSE_LOG_SQL_QUERIES\n'
        '\n'
        '  config_dir:      "/etc/clickhouse-server"              # CLICKHOUSE_CONFIG_DIR\n'
        '  restart_command: "systemctl restart clickhouse-server" # CLICKHOUSE_RESTART_COMMAND\n'
        '\n'
        'azblob:\n'
        '  endpoint_suffix: "core.windows.net" # AZBLOB_ENDPOINT_SUFFIX\n'
        '  account_name: ""             # AZBLOB_ACCOUNT_NAME\n'
        '  account_key: ""              # AZBLOB_ACCOUNT_KEY\n'
        '  sas: ""                      # AZBLOB_SAS\n'
        '  container: ""                # AZBLOB_CONTAINER\n'
        '  path: ""                     # AZBLOB_PATH\n'
        '  compression_level: 1         # AZBLOB_COMPRESSION_LEVEL\n'
        '  compression_format: tar      # AZBLOB_COMPRESSION_FORMAT\n'
        '  sse_key: ""                  # AZBLOB_SSE_KEY\n'
        's3:\n'
        '  access_key: ""                   # S3_ACCESS_KEY\n'
        '  secret_key: ""                   # S3_SECRET_KEY\n'
        '  bucket: ""                       # S3_BUCKET\n'
        '  endpoint: ""                     # S3_ENDPOINT\n'
        '  region: us-east-1                # S3_REGION\n'
        '  acl: private                     # S3_ACL\n'
        '  force_path_style: false          # S3_FORCE_PATH_STYLE\n'
        '  path: ""                         # S3_PATH\n'
        '  disable_ssl: false               # S3_DISABLE_SSL\n'
        '  compression_level: 1             # S3_COMPRESSION_LEVEL\n'
        "  # supports 'tar', 'gzip', 'zstd', 'brotli'\n"
        '  compression_format: tar          # S3_COMPRESSION_FORMAT\n'
        '  # empty (default), AES256, or aws:kms\n'
        '  sse: AES256                      # S3_SSE\n'
        '  disable_cert_verification: false # S3_DISABLE_CERT_VERIFICATION\n'
        '  storage_class: STANDARD          # S3_STORAGE_CLASS\n'
        '  debug: false                     # S3_DEBUG\n'
        'gcs:\n'
        '  credentials_file: ""         # GCS_CREDENTIALS_FILE\n'
        '  credentials_json: ""         # GCS_CREDENTIALS_JSON\n'
        '  bucket: ""                   # GCS_BUCKET\n'
        '  path: ""                     # GCS_PATH\n'
        '  compression_level: 1         # GCS_COMPRESSION_LEVEL\n'
        '  compression_format: tar      # GCS_COMPRESSION_FORMAT\n'
        'cos:\n'
        '  url: ""                      # COS_URL\n'
        '  timeout: 2m                  # COS_TIMEOUT\n'
        '  secret_id: ""                # COS_SECRET_ID\n'
        '  secret_key: ""               # COS_SECRET_KEY\n'
        '  path: ""                     # COS_PATH\n'
        '  compression_format: tar      # COS_COMPRESSION_FORMAT\n'
        '  compression_level: 1         # COS_COMPRESSION_LEVEL\n'
        'api:\n'
        '  listen: "localhost:7171"     # API_LISTEN\n'
        '  enable_metrics: true         # API_ENABLE_METRICS\n'
        '  enable_pprof: false          # API_ENABLE_PPROF\n'
        '  username: ""                 # API_USERNAME\n'
        '  password: ""                 # API_PASSWORD\n'
        '  secure: false                # API_SECURE\n'
        '  certificate_file: ""         # API_CERTIFICATE_FILE\n'
        '  private_key_file: ""         # API_PRIVATE_KEY_FILE\n'
        '  create_integration_tables: false # API_CREATE_INTEGRATION_TABLES\n'
        'ftp:\n'
        '  address: ""                  # FTP_ADDRESS\n'
        '  timeout: 2m                  # FTP_TIMEOUT\n'
        '  username: ""                 # FTP_USERNAME\n'
        '  password: ""                 # FTP_PASSWORD\n'
        '  tls: false                   # FTP_TLS\n'
        '  path: ""                     # FTP_PATH\n'
        '  compression_format: tar      # FTP_COMPRESSION_FORMAT\n'
        '  compression_level: 1         # FTP_COMPRESSION_LEVEL\n'
        '  debug: false                 # FTP_DEBUG\n'
        'sftp:\n'
        '  address: ""                  # SFTP_ADDRESS\n'
        '  username: ""                 # SFTP_USERNAME\n'
        '  password: ""                 # SFTP_PASSWORD\n'
        '  key: ""                      # SFTP_KEY\n'
        '  path: ""                     # SFTP_PATH\n'
        '  compression_format: tar      # SFTP_COMPRESSION_FORMAT\n'
        '  compression_level: 1         # SFTP_COMPRESSION_LEVEL\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.9.1')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL support `clickhouse-backup server`\n'
        'command to start REST API server.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.1')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_AsyncPostQuery = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.AsyncPostQuery',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility SHALL return `code 423` if a POST request is received while previous operation is not finished yet. The request SHALL NOT be executed in such case.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.2')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_OutputFormat_Default = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.OutputFormat.Default',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL return results\n'
        "in a format compatible with [ClickHouse]'s `JSONEachRow` format\n"
        'for all the the endpoints.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.3')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetTables = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetTables',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'GET /backup/tables\n'
        '```\n'
        '\n'
        'to print the list of tables. For example, `curl -s localhost:7171/backup/tables | jq`.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.4')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostCreate = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostCreate',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'POST /backup/create\n'
        '```\n'
        '\n'
        'to create new backup. For example, `curl -s localhost:7171/backup/create -X POST | jq`.\n'
        '\n'
        'Also, the following optional arguments SHALL be supported:\n'
        '* Optional query argument `table` SHALL work the same as the `--table <value>` CLI argument.\n'
        '* Optional query argument `name` SHALL work the same as specifying a backup name with the CLI.\n'
        '* Optional query argument `schema` SHALL work the same as the `--schema` CLI argument (backup schema only).\n'
        '* Optional query argument `rbac` SHALL work the same as the `--rbac` CLI argument (backup RBAC only).\n'
        '* Optional query argument `configs` SHALL work the same as the `--configs` CLI argument (backup configs only).\n'
        '\n'
        "Optional arguments may be defined as following: `curl -s 'localhost:7171/backup/create?table=default.billing&name=billing_test' -X POST`.\n"
        '\n'
        'This operation SHALL be async, so the API will return once the operation has been started.\n'
        '\n'
        'Response contents shall have the following structure:\n'
        '```\n'
        '{"status":"acknowledged","operation":"create","backup_name":"<backup_name>"}\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.5')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostCreate_Remote = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostCreate.Remote',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'POST /backup/actions -H "Content-Type: application/json" -d \'{"command":"create_remote <backup_name>"}\'\n'
        '```\n'
        '\n'
        'to create new remote backup.\n'
        '\n'
        'This operation SHALL be async, so the API will return once the operation has been started.\n'
        '\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.6')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostUpload = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostUpload',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'POST /backup/upload\n'
        '```\n'
        '\n'
        'to upload backup to remote storage. This command SHALL automatically upload ClickHouse configs and RBAC objects backups if they are present (in case backup was created with `--configs` and/or `--rbac`).\n'
        '\n'
        'For example, `curl -s localhost:7171/backup/upload/<BACKUP_NAME> -X POST | jq`.\n'
        '\n'
        '* optional query argument `diff-from` SHALL work the same as the `--diff-from` CLI argument.\n'
        '\n'
        'This operation SHALL be async, so the API will return once the operation has been started.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.7')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetList = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'GET /backup/list\n'
        '```\n'
        '\n'
        'to print list of backups. For example, `curl -s localhost:7171/backup/list | jq`.\n'
        '\n'
        'The `size` field SHALL not be populated for local backups.\n'
        '\n'
        'Response SHALL have the following format:\n'
        '```\n'
        "{'name': 'backup_1', 'created': '<backup_1_creation_time>', 'size': <size>, 'location': '<local|remote>', 'required': '', 'desc': ''}\n"
        "{'name': 'backup_2', 'created': '<backup_2_creation_time>', 'size': <size>, 'location': '<local|remote>', 'required': '', 'desc': ''}\n"
        '...\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.8')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetList_Local = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList.Local',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'GET /backup/list/local\n'
        '```\n'
        '\n'
        'to print list of Remote backups. For example, `curl -s localhost:7171/backup/list/local | jq`.\n'
        '\n'
        'The `size` field SHALL not be populated.\n'
        '\n'
        'Response SHALL have the following format:\n'
        '```\n'
        "{'name': 'backup_1', 'created': '<backup_1_creation_time>', 'size': <size>, 'location': 'local', 'required': '', 'desc': ''}\n"
        "{'name': 'backup_2', 'created': '<backup_2_creation_time>', 'size': <size>, 'location': 'local', 'required': '', 'desc': ''}\n"
        '...\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.9')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetList_Remote = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList.Remote',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'GET /backup/list/remote\n'
        '```\n'
        '\n'
        'to print list of remote backups. For example, `curl -s localhost:7171/backup/list/remote | jq`.\n'
        '\n'
        'Response SHALL have the following format:\n'
        '```\n'
        "{'name': 'backup_1', 'created': '<backup_1_creation_time>', 'size': <size>, 'location': 'remote', 'required': '', 'desc': ''}\n"
        "{'name': 'backup_2', 'created': '<backup_2_creation_time>', 'size': <size>, 'location': 'remote', 'required': '', 'desc': ''}\n"
        '...\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.10')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostDownload = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDownload',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'POST /backup/download\n'
        '```\n'
        '\n'
        'to download backup from remote storage. This command SHALL automatically download ClickHouse configs and RBAC objects backups if they are present (in case backup was created with `--configs` and/or `--rbac`).\n'
        '\n'
        'For example, `curl -s localhost:7171/backup/download/<BACKUP_NAME> -X POST | jq`.\n'
        '\n'
        'This operation SHALL be async, so the API will return once the operation has been started.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.11')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostRestore = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostRestore',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'POST /backup/restore\n'
        '```\n'
        '\n'
        'to create schema and restore data from backup.\n'
        '\n'
        'For example, `curl -s localhost:7171/backup/restore/<BACKUP_NAME> -X POST | jq`.\n'
        '\n'
        '* optional query argument `table` SHALL work the same as the `--table` value CLI argument\n'
        '* optional query argument `schema` SHALL work the same as the `--schema` CLI argument (restore schema only)\n'
        '* Optional query argument `data` SHALL work the same as the `--data` CLI argument (restore data only).\n'
        '* Optional query argument `rbac` SHALL work the same as the `--rbac` CLI argument (restore RBAC only).\n'
        '* Optional query argument `configs` SHALL work the same as the `--configs` CLI argument (restore configs only).\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.12')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostRestore_Remote = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostRestore.Remote',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'POST /backup/actions -H "Content-Type: application/json" -d \'{"command":"restore_remote <backup_name>"}\'\n'
        '```\n'
        '\n'
        'to restore from an existing remote backup.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.13')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostDelete_Remote = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDelete.Remote',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'POST /backup/delete/remote\n'
        '```\n'
        '\n'
        'to delete specific remote backup.\n'
        '\n'
        'For example, `curl -s localhost:7171/backup/delete/remote/<BACKUP_NAME> -X POST | jq`.\n'
        '\n'
        'Response contents shall have the following structure:\n'
        '```\n'
        '{"status":"success","operation":"delete","backup_name":"<backup_name>","location":"remote"}\n'
        '\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.14')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostDelete_Local = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDelete.Local',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'POST /backup/delete/local\n'
        '```\n'
        '\n'
        'to delete specific local backup.\n'
        '\n'
        '\n'
        'For example, `curl -s localhost:7171/backup/delete/local/<BACKUP_NAME> -X POST | jq`.\n'
        '\n'
        'Response contents shall have the following structure:\n'
        '```\n'
        '{"status":"success","operation":"delete","backup_name":"<backup_name>","location":"local"}\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.15')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetStatus = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetStatus',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'GET /backup/status\n'
        '```\n'
        '\n'
        'to display list of current async operations.\n'
        'For example, `curl -s localhost:7171/backup/status | jq`.\n'
        '\n'
        'Response contents shall have the following structure:\n'
        '```\n'
        "{'command': '<command name>', 'status': 'in progress', 'start': '2021-10-26 12:11:34'}\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.16')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetActions = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetActions',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'GET /backup/actions\n'
        '```\n'
        '\n'
        'to display a list of current async operations.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.17')

RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostActions = Requirement(
    name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostActions',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'The [clickhouse-backup] utility REST API server SHALL support\n'
        '\n'
        '```\n'
        'POST /backup/actions\n'
        '```\n'
        '\n'
        'to create a new action with parameters being the same as for the CLI commands.\n'
        'All CLI actions such as `create`, `upload` etc. SHALL be supported.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.10.18')

QA_SRS013_ClickHouse_Backup_Utility = Specification(
    name='QA-SRS013 ClickHouse Backup Utility', 
    description=None,
    author=None,
    date=None, 
    status=None, 
    approved_by=None,
    approved_date=None,
    approved_version=None,
    version=None,
    group=None,
    type=None,
    link=None,
    uid=None,
    parent=None,
    children=None,
    headings=(
        Heading(name='Revision History', level=1, num='1'),
        Heading(name='Introduction', level=1, num='2'),
        Heading(name='Terminology', level=1, num='3'),
        Heading(name='SRS', level=2, num='3.1'),
        Heading(name='Supported engine families', level=2, num='3.2'),
        Heading(name='Requirements', level=1, num='4'),
        Heading(name='Generic', level=2, num='4.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.AllDataTypes', level=3, num='4.1.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.RestorePartiallyDropped', level=3, num='4.1.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.BackupDuringMutation', level=3, num='4.1.3'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.AllTables', level=3, num='4.1.4'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables', level=3, num='4.1.5'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables.Performance', level=3, num='4.1.6'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables.ManyColumns', level=3, num='4.1.7'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.ReplicatedTable', level=3, num='4.1.8'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.MultipleBackups.EfficientStorageByUsingHardLinks', level=3, num='4.1.9'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.CompressionLevel', level=3, num='4.1.10'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.ClickHouseVersions', level=3, num='4.1.11'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.TieredStorage', level=3, num='4.1.12'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Generic.EncryptedStorage', level=3, num='4.1.13'),
        Heading(name='Views', level=2, num='4.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Views.View', level=3, num='4.2.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Views.MaterializedView', level=3, num='4.2.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Views.LiveView', level=3, num='4.2.3'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Views.WindowView', level=3, num='4.2.4'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Views.NestedViews', level=3, num='4.2.5'),
        Heading(name='Table Engines', level=2, num='4.3'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.MergeTree', level=3, num='4.3.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.OtherEngines', level=3, num='4.3.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.OtherEngines.Kafka', level=3, num='4.3.3'),
        Heading(name='Database Engines', level=2, num='4.4'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.Atomic', level=3, num='4.4.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.MaterializedMySQL', level=3, num='4.4.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.MaterializedPostgreSQL', level=3, num='4.4.3'),
        Heading(name='Cloud Storage', level=2, num='4.5'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage', level=3, num='4.5.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.IncrementalBackups', level=3, num='4.5.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.CompressionFormats', level=3, num='4.5.3'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.S3.Minio', level=3, num='4.5.4'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.S3.AWS', level=3, num='4.5.5'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.GCS', level=3, num='4.5.6'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.Azure', level=3, num='4.5.7'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.TencentCOS', level=3, num='4.5.8'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.FTP', level=3, num='4.5.9'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.SFTP', level=3, num='4.5.10'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.GreaterThan5TB', level=3, num='4.5.11'),
        Heading(name='Backup and restore configs', level=2, num='4.6'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Configs.Backup', level=3, num='4.6.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Configs.Restore', level=3, num='4.6.2'),
        Heading(name='Backup and restore RBAC', level=2, num='4.7'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.RBAC.Backup', level=3, num='4.7.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.RBAC.Restore', level=3, num='4.7.2'),
        Heading(name='Command Line Interface', level=2, num='4.8'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Usage', level=3, num='4.8.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Tables', level=3, num='4.8.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Create', level=3, num='4.8.3'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.CreateRemote', level=3, num='4.8.4'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.MatchExpression', level=3, num='4.8.5'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Upload', level=3, num='4.8.6'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.List', level=3, num='4.8.7'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Download', level=3, num='4.8.8'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Restore', level=3, num='4.8.9'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.RestoreRemote', level=3, num='4.8.10'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Delete', level=3, num='4.8.11'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.DefaultConfig', level=3, num='4.8.12'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Server', level=3, num='4.8.13'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.CLI.Help', level=3, num='4.8.14'),
        Heading(name='Configuration', level=2, num='4.9'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.Configuration', level=3, num='4.9.1'),
        Heading(name='REST API Server', level=2, num='4.10'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server', level=3, num='4.10.1'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.AsyncPostQuery', level=3, num='4.10.2'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.OutputFormat.Default', level=3, num='4.10.3'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetTables', level=3, num='4.10.4'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostCreate', level=3, num='4.10.5'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostCreate.Remote', level=3, num='4.10.6'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostUpload', level=3, num='4.10.7'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList', level=3, num='4.10.8'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList.Local', level=3, num='4.10.9'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList.Remote', level=3, num='4.10.10'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDownload', level=3, num='4.10.11'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostRestore', level=3, num='4.10.12'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostRestore.Remote', level=3, num='4.10.13'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDelete.Remote', level=3, num='4.10.14'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDelete.Local', level=3, num='4.10.15'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetStatus', level=3, num='4.10.16'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetActions', level=3, num='4.10.17'),
        Heading(name='RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostActions', level=3, num='4.10.18'),
        Heading(name='References', level=1, num='5'),
        ),
    requirements=(
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_AllDataTypes,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_RestorePartiallyDropped,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_BackupDuringMutation,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_AllTables,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_SpecificTables,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_SpecificTables_Performance,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_SpecificTables_ManyColumns,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_ReplicatedTable,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_MultipleBackups_EfficientStorageByUsingHardLinks,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_CompressionLevel,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_ClickHouseVersions,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_TieredStorage,
        RQ_SRS_013_ClickHouse_BackupUtility_Generic_EncryptedStorage,
        RQ_SRS_013_ClickHouse_BackupUtility_Views_View,
        RQ_SRS_013_ClickHouse_BackupUtility_Views_MaterializedView,
        RQ_SRS_013_ClickHouse_BackupUtility_Views_LiveView,
        RQ_SRS_013_ClickHouse_BackupUtility_Views_WindowView,
        RQ_SRS_013_ClickHouse_BackupUtility_Views_NestedViews,
        RQ_SRS_013_ClickHouse_BackupUtility_TableEngines_MergeTree,
        RQ_SRS_013_ClickHouse_BackupUtility_TableEngines_OtherEngines,
        RQ_SRS_013_ClickHouse_BackupUtility_TableEngines_OtherEngines_Kafka,
        RQ_SRS_013_ClickHouse_BackupUtility_DatabaseEngines_Atomic,
        RQ_SRS_013_ClickHouse_BackupUtility_DatabaseEngines_MaterializedMySQL,
        RQ_SRS_013_ClickHouse_BackupUtility_DatabaseEngines_MaterializedPostgreSQL,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_IncrementalBackups,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_CompressionFormats,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_S3_Minio,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_S3_AWS,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_GCS,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_Azure,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_TencentCOS,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_FTP,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_SFTP,
        RQ_SRS_013_ClickHouse_BackupUtility_CloudStorage_GreaterThan5TB,
        RQ_SRS_013_ClickHouse_BackupUtility_Configs_Backup,
        RQ_SRS_013_ClickHouse_BackupUtility_Configs_Restore,
        RQ_SRS_013_ClickHouse_BackupUtility_RBAC_Backup,
        RQ_SRS_013_ClickHouse_BackupUtility_RBAC_Restore,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_Usage,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_Tables,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_Create,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_CreateRemote,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_MatchExpression,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_Upload,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_List,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_Download,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_Restore,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_RestoreRemote,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_Delete,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_DefaultConfig,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_Server,
        RQ_SRS_013_ClickHouse_BackupUtility_CLI_Help,
        RQ_SRS_013_ClickHouse_BackupUtility_Configuration,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_AsyncPostQuery,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_OutputFormat_Default,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetTables,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostCreate,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostCreate_Remote,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostUpload,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetList,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetList_Local,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetList_Remote,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostDownload,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostRestore,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostRestore_Remote,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostDelete_Remote,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostDelete_Local,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetStatus,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_GetActions,
        RQ_SRS_013_ClickHouse_BackupUtility_REST_API_Server_PostActions,
        ),
    content='''
# QA-SRS013 ClickHouse Backup Utility
# Software Requirements Specification


## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Terminology](#terminology)
  * 3.1 [SRS](#srs)
  * 3.2 [Supported engine families](#supported-engine-families)
* 4 [Requirements](#requirements)
  * 4.1 [Generic](#generic)
    * 4.1.1 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.AllDataTypes](#rqsrs-013clickhousebackuputilitygenericalldatatypes)
    * 4.1.2 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.RestorePartiallyDropped](#rqsrs-013clickhousebackuputilitygenericrestorepartiallydropped)
    * 4.1.3 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.BackupDuringMutation](#rqsrs-013clickhousebackuputilitygenericbackupduringmutation)
    * 4.1.4 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.AllTables](#rqsrs-013clickhousebackuputilitygenericalltables)
    * 4.1.5 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables](#rqsrs-013clickhousebackuputilitygenericspecifictables)
    * 4.1.6 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables.Performance](#rqsrs-013clickhousebackuputilitygenericspecifictablesperformance)
    * 4.1.7 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables.ManyColumns](#rqsrs-013clickhousebackuputilitygenericspecifictablesmanycolumns)
    * 4.1.8 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.ReplicatedTable](#rqsrs-013clickhousebackuputilitygenericreplicatedtable)
    * 4.1.9 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.MultipleBackups.EfficientStorageByUsingHardLinks](#rqsrs-013clickhousebackuputilitygenericmultiplebackupsefficientstoragebyusinghardlinks)
    * 4.1.10 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.CompressionLevel](#rqsrs-013clickhousebackuputilitygenericcompressionlevel)
    * 4.1.11 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.ClickHouseVersions](#rqsrs-013clickhousebackuputilitygenericclickhouseversions)
    * 4.1.12 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.TieredStorage](#rqsrs-013clickhousebackuputilitygenerictieredstorage)
    * 4.1.13 [RQ.SRS-013.ClickHouse.BackupUtility.Generic.EncryptedStorage](#rqsrs-013clickhousebackuputilitygenericencryptedstorage)
  * 4.2 [Views](#views)
    * 4.2.1 [RQ.SRS-013.ClickHouse.BackupUtility.Views.View](#rqsrs-013clickhousebackuputilityviewsview)
    * 4.2.2 [RQ.SRS-013.ClickHouse.BackupUtility.Views.MaterializedView](#rqsrs-013clickhousebackuputilityviewsmaterializedview)
    * 4.2.3 [RQ.SRS-013.ClickHouse.BackupUtility.Views.LiveView](#rqsrs-013clickhousebackuputilityviewsliveview)
    * 4.2.4 [RQ.SRS-013.ClickHouse.BackupUtility.Views.WindowView](#rqsrs-013clickhousebackuputilityviewswindowview)
    * 4.2.5 [RQ.SRS-013.ClickHouse.BackupUtility.Views.NestedViews](#rqsrs-013clickhousebackuputilityviewsnestedviews)
  * 4.3 [Table Engines](#table-engines)
    * 4.3.1 [RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.MergeTree](#rqsrs-013clickhousebackuputilitytableenginesmergetree)
    * 4.3.2 [RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.OtherEngines](#rqsrs-013clickhousebackuputilitytableenginesotherengines)
    * 4.3.3 [RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.OtherEngines.Kafka](#rqsrs-013clickhousebackuputilitytableenginesotherengineskafka)
  * 4.4 [Database Engines](#database-engines)
    * 4.4.1 [RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.Atomic](#rqsrs-013clickhousebackuputilitydatabaseenginesatomic)
    * 4.4.2 [RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.MaterializedMySQL](#rqsrs-013clickhousebackuputilitydatabaseenginesmaterializedmysql)
    * 4.4.3 [RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.MaterializedPostgreSQL](#rqsrs-013clickhousebackuputilitydatabaseenginesmaterializedpostgresql)
  * 4.5 [Cloud Storage](#cloud-storage)
    * 4.5.1 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage](#rqsrs-013clickhousebackuputilitycloudstorage)
    * 4.5.2 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.IncrementalBackups](#rqsrs-013clickhousebackuputilitycloudstorageincrementalbackups)
    * 4.5.3 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.CompressionFormats](#rqsrs-013clickhousebackuputilitycloudstoragecompressionformats)
    * 4.5.4 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.S3.Minio](#rqsrs-013clickhousebackuputilitycloudstorages3minio)
    * 4.5.5 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.S3.AWS](#rqsrs-013clickhousebackuputilitycloudstorages3aws)
    * 4.5.6 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.GCS](#rqsrs-013clickhousebackuputilitycloudstoragegcs)
    * 4.5.7 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.Azure](#rqsrs-013clickhousebackuputilitycloudstorageazure)
    * 4.5.8 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.TencentCOS](#rqsrs-013clickhousebackuputilitycloudstoragetencentcos)
    * 4.5.9 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.FTP](#rqsrs-013clickhousebackuputilitycloudstorageftp)
    * 4.5.10 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.SFTP](#rqsrs-013clickhousebackuputilitycloudstoragesftp)
    * 4.5.11 [RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.GreaterThan5TB](#rqsrs-013clickhousebackuputilitycloudstoragegreaterthan5tb)
  * 4.6 [Backup and restore configs](#backup-and-restore-configs)
    * 4.6.1 [RQ.SRS-013.ClickHouse.BackupUtility.Configs.Backup](#rqsrs-013clickhousebackuputilityconfigsbackup)
    * 4.6.2 [RQ.SRS-013.ClickHouse.BackupUtility.Configs.Restore](#rqsrs-013clickhousebackuputilityconfigsrestore)
  * 4.7 [Backup and restore RBAC](#backup-and-restore-rbac)
    * 4.7.1 [RQ.SRS-013.ClickHouse.BackupUtility.RBAC.Backup](#rqsrs-013clickhousebackuputilityrbacbackup)
    * 4.7.2 [RQ.SRS-013.ClickHouse.BackupUtility.RBAC.Restore](#rqsrs-013clickhousebackuputilityrbacrestore)
  * 4.8 [Command Line Interface](#command-line-interface)
    * 4.8.1 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.Usage](#rqsrs-013clickhousebackuputilitycliusage)
    * 4.8.2 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.Tables](#rqsrs-013clickhousebackuputilityclitables)
    * 4.8.3 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.Create](#rqsrs-013clickhousebackuputilityclicreate)
    * 4.8.4 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.CreateRemote](#rqsrs-013clickhousebackuputilityclicreateremote)
    * 4.8.5 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.MatchExpression](#rqsrs-013clickhousebackuputilityclimatchexpression)
    * 4.8.6 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.Upload](#rqsrs-013clickhousebackuputilitycliupload)
    * 4.8.7 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.List](#rqsrs-013clickhousebackuputilityclilist)
    * 4.8.8 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.Download](#rqsrs-013clickhousebackuputilityclidownload)
    * 4.8.9 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.Restore](#rqsrs-013clickhousebackuputilityclirestore)
    * 4.8.10 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.RestoreRemote](#rqsrs-013clickhousebackuputilityclirestoreremote)
    * 4.8.11 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.Delete](#rqsrs-013clickhousebackuputilityclidelete)
    * 4.8.12 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.DefaultConfig](#rqsrs-013clickhousebackuputilityclidefaultconfig)
    * 4.8.13 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.Server](#rqsrs-013clickhousebackuputilitycliserver)
    * 4.8.14 [RQ.SRS-013.ClickHouse.BackupUtility.CLI.Help](#rqsrs-013clickhousebackuputilityclihelp)
  * 4.9 [Configuration](#configuration)
    * 4.9.1 [RQ.SRS-013.ClickHouse.BackupUtility.Configuration](#rqsrs-013clickhousebackuputilityconfiguration)
  * 4.10 [REST API Server](#rest-api-server)
    * 4.10.1 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server](#rqsrs-013clickhousebackuputilityrestapiserver)
    * 4.10.2 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.AsyncPostQuery](#rqsrs-013clickhousebackuputilityrestapiasyncpostquery)
    * 4.10.3 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.OutputFormat.Default](#rqsrs-013clickhousebackuputilityrestapiserveroutputformatdefault)
    * 4.10.4 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetTables](#rqsrs-013clickhousebackuputilityrestapiservergettables)
    * 4.10.5 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostCreate](#rqsrs-013clickhousebackuputilityrestapiserverpostcreate)
    * 4.10.6 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostCreate.Remote](#rqsrs-013clickhousebackuputilityrestapiserverpostcreateremote)
    * 4.10.7 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostUpload](#rqsrs-013clickhousebackuputilityrestapiserverpostupload)
    * 4.10.8 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList](#rqsrs-013clickhousebackuputilityrestapiservergetlist)
    * 4.10.9 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList.Local](#rqsrs-013clickhousebackuputilityrestapiservergetlistlocal)
    * 4.10.10 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList.Remote](#rqsrs-013clickhousebackuputilityrestapiservergetlistremote)
    * 4.10.11 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDownload](#rqsrs-013clickhousebackuputilityrestapiserverpostdownload)
    * 4.10.12 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostRestore](#rqsrs-013clickhousebackuputilityrestapiserverpostrestore)
    * 4.10.13 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostRestore.Remote](#rqsrs-013clickhousebackuputilityrestapiserverpostrestoreremote)
    * 4.10.14 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDelete.Remote](#rqsrs-013clickhousebackuputilityrestapiserverpostdeleteremote)
    * 4.10.15 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDelete.Local](#rqsrs-013clickhousebackuputilityrestapiserverpostdeletelocal)
    * 4.10.16 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetStatus](#rqsrs-013clickhousebackuputilityrestapiservergetstatus)
    * 4.10.17 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetActions](#rqsrs-013clickhousebackuputilityrestapiservergetactions)
    * 4.10.18 [RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostActions](#rqsrs-013clickhousebackuputilityrestapiserverpostactions)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a [Github Repository].
All the updates are tracked using the [Revision History].

## Introduction

[ClickHouse] does not support any convenient method to backup and restore databases or tables.
This [SRS] covers the requirements for the [clickhouse-backup] utility that supports
easy [ClickHouse] backup and restore with cloud storages support.

## Terminology

### SRS

Software Requirements Specification

### Supported engine families

Currently, [clickhouse-backup] utility only supports following table engines:

* [MergeTree family](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/)
* [MergeTree](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/)
* [ReplacingMergeTree](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/replacingmergetree/)
* [SummingMergeTree](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/summingmergetree/)
* [CollapsingMergeTree](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/collapsingmergetree/)
* [VersionedCollapsingMergeTree](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/versionedcollapsingmergetree/)
* [GraphiteMergeTree](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/graphitemergetree/)
* [Replicated MergeTree Tables](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/replication/)

## Requirements

### Generic

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.AllDataTypes
version: 1.0

The [clickhouse-backup] utility SHALL support backing up and restoring tables containing any existing data type.

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.RestorePartiallyDropped
version: 1.0

The [clickhouse-backup] utility SHALL support restoring table that has lost some piece of data.

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.BackupDuringMutation
version: 1.0

The [clickhouse-backup] utility SHALL be able to create a backup if there is an ongoing mutation. Restoring from such backups SHALL also be possible.

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.AllTables
version: 1.0

The [clickhouse-backup] utility SHALL support easy backup and restore of all [ClickHouse] tables that SHALL result in table having identical state as at the time of backup.

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables
version: 1.0

The [clickhouse-backup] utility SHALL support easy backup and restore of specific [ClickHouse] tables that SHALL result in table having identical state as at the time of backup.


#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables.Performance
version: 1.0

The [clickhouse-backup] utility SHALL create backups for a particular table with high performance due to avoiding scanning whole `system.tables` when set `table` query string parameter or `--tables` CLI parameter.

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.SpecificTables.ManyColumns
version: 1.0

The [clickhouse-backup] utility SHALL support creating backups and restoring from them for a table with high number of columns.


#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.ReplicatedTable
version: 1.0

The [clickhouse-backup] utility SHALL support easy backup and restore of [ClickHouse] replicated tables from the backup
that SHALL result in table having identical state as at the time of backup.

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.MultipleBackups.EfficientStorageByUsingHardLinks
version: 1.0

The [clickhouse-backup] utility SHALL support efficient storage of multiple backups on the file system by
manipulating hardlinks used by [ClickHouse] parts.

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.CompressionLevel
version: 1.0

The [clickhouse-backup] utility SHALL support uploading to/downloading from remote storage with specified compression level.

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.ClickHouseVersions
version: 1.0

The [clickhouse-backup] utility SHALL support [ClickHouse] versions above `1.1.54390`.

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.TieredStorage
version: 1.0

The [clickhouse-backup] utility SHALL support `tiered storage` that was added in ClickHouse `19.15.2.2`.

#### RQ.SRS-013.ClickHouse.BackupUtility.Generic.EncryptedStorage
version: 1.0

The [clickhouse-backup] utility SHALL support creating backups from [encrypted storages](https://clickhouse.tech/docs/en/operations/storing-data/#encrypted-virtual-file-system) and restoring them.

### Views

#### RQ.SRS-013.ClickHouse.BackupUtility.Views.View
version: 1.0

The [clickhouse-backup] utility SHALL support proper handling of `View` over `MergeTree` table engines.

#### RQ.SRS-013.ClickHouse.BackupUtility.Views.MaterializedView
version: 1.0

The [clickhouse-backup] utility SHALL support proper handling of `MaterializedView` over `MergeTree` table engines.

#### RQ.SRS-013.ClickHouse.BackupUtility.Views.LiveView
version: 1.0

The [clickhouse-backup] utility SHALL support proper handling of `LiveView` over `MergeTree` table engines.

#### RQ.SRS-013.ClickHouse.BackupUtility.Views.WindowView
version: 1.0

The [clickhouse-backup] utility SHALL support proper handling of `WindowView` over `MergeTree` table engines.

#### RQ.SRS-013.ClickHouse.BackupUtility.Views.NestedViews
version: 1.0

The [clickhouse-backup] utility SHALL support proper handling of nested views, e.g. `View` over `View`.

### Table Engines

#### RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.MergeTree
version: 1.0

The [clickhouse-backup] utility SHALL support only `MergeTree` family table engines.

#### RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.OtherEngines
version: 1.0

The [clickhouse-backup] utility workflow SHALL NOT be broken by presence of tables with engines other that listed above.

#### RQ.SRS-013.ClickHouse.BackupUtility.TableEngines.OtherEngines.Kafka
version: 1.0

The [clickhouse-backup] utility SHALL only support creating table schema backup and restore for `Kafka` table engine.

### Database Engines

#### RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.Atomic
version: 1.0

The [clickhouse-backup] utility SHALL support `MergeTree` family table engines with `Atomic` database engine.

#### RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.MaterializedMySQL
version: 1.0

The [clickhouse-backup] utility SHALL support `MergeTree` family table engines with `MaterializedMySQL` database engine.

#### RQ.SRS-013.ClickHouse.BackupUtility.DatabaseEngines.MaterializedPostgreSQL
version: 1.0

The [clickhouse-backup] utility SHALL support `MergeTree` family table engines with `MaterializedPostgreSQL` database engine.

### Cloud Storage

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage
version: 1.0

The [clickhouse-backup] utility SHALL support creating and uploading backups on cloud storage.

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.IncrementalBackups
version: 1.0

The [clickhouse-backup] utility SHALL support uploading an incremental backup to cloud storage.
This means a data chunk will be uploaded only in case it is not contained in original backup.
Incremental backup can be based on a full backup or on another incremental backup.

The [clickhouse-backup] utility SHALL also support incremental backups after
one or more mutations have been performed on a table using the `ALTER` statement.
For example,

* `ALTER TABLE <table> DELETE`
* `ALTER TABLE <table> UPDATE`
* `ALTER TABLE <table> ADD COLUMN`
* `ALTER TABLE <table> CLEAR COLUMN`
* `ALTER TABLE <table> DROP COLUMN`

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.CompressionFormats
version: 1.0

The [clickhouse-backup] utility SHALL support storing backup on a cloud storage in following compression formats: `tar`, `bzip2`, `gzip`, `sz`, `xz`, `br/brotli`, `zstd`. In case of other value, error shall be thrown and no backup uploaded.

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.S3.Minio
version: 1.0

The [clickhouse-backup] utility SHALL support storing backups on a Minio S3 storage. It also SHALL be possible to restore from such backups.

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.S3.AWS
version: 1.0

The [clickhouse-backup] utility SHALL support storing backups on [AWS](https://aws.amazon.com/products/storage/) cloud servers. It also SHALL be possible to restore from such backups.

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.GCS
version: 1.0

The [clickhouse-backup] utility SHALL support storing backups on [GCS](https://cloud.google.com/storage) cloud servers. It also SHALL be possible to restore from such backups.

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.Azure
version: 1.0

The [clickhouse-backup] utility SHALL support storing backups on [Azure](https://docs.microsoft.com/en-us/azure/storage/common/storage-introduction) cloud servers. It also SHALL be possible to restore from such backups.

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.TencentCOS
version: 1.0

The [clickhouse-backup] utility SHALL support storing backups on [Tencent COS](https://intl.cloud.tencent.com/product/cos) cloud servers. It also SHALL be possible to restore from such backups.

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.FTP
version: 1.0

The [clickhouse-backup] utility SHALL support storing backups on a remote FTP server. It also SHALL be possible to restore from such backup.

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.SFTP
version: 1.0

The [clickhouse-backup] utility SHALL support storing backups on a remote SFTP (also known as Secure FTP) server. It also SHALL be possible to restore from such backup.

#### RQ.SRS-013.ClickHouse.BackupUtility.CloudStorage.GreaterThan5TB
version: 1.0

The [clickhouse-backup] utility SHALL support uploading backups greater than `5TB` using partitioning by 5TB chunks.
More info can be found [here](https://stackoverflow.com/questions/54012602/is-there-a-file-size-limit-for-amazon-s3#:~:text=While%20the%20maximum%20file%20size,any%20files%20larger%20than%20100MB)

### Backup and restore configs

#### RQ.SRS-013.ClickHouse.BackupUtility.Configs.Backup
version: 1.0

The [clickhouse-backup] utility SHALL support backing up ClickHouse configs.

By configs we mean the following files:
    * `/etc/clickhouse-server/config.xml`
    * `/etc/clickhouse-server/config.d/*.xml`
    * `/etc/clickhouse-server/users.xml`
    * `/etc/clickhouse-server/users.d/*.xml`

Listed above are default config files. However, these values may be overriden on clickhouse-server launch.

#### RQ.SRS-013.ClickHouse.BackupUtility.Configs.Restore
version: 1.0

The [clickhouse-backup] utility SHALL support restoring ClickHouse configs from backup. This should be done automatically in case configs backup is present. If configs are found in backup, ClickHouse server SHALL restart automatically.

By configs we mean the following files:
    * `/etc/clickhouse-server/config.xml`
    * `/etc/clickhouse-server/config.d/*.xml`
    * `/etc/clickhouse-server/users.xml`
    * `/etc/clickhouse-server/users.d/*.xml`

Listed above are default config files. However, these values may be overriden on clickhouse-server launch.

### Backup and restore RBAC

#### RQ.SRS-013.ClickHouse.BackupUtility.RBAC.Backup
version: 1.0

The [clickhouse-backup] utility SHALL support backing up ClickHouse RBAC objects. By RBAC objects we mean the following:
    * `USER`
    * `ROLE`
    * `ROW POLICY`
    * `SETTINGS PROFILE`
    * `QUOTA`

#### RQ.SRS-013.ClickHouse.BackupUtility.RBAC.Restore
version: 1.0

The [clickhouse-backup] utility SHALL support restoring ClickHouse RBAC objects from backup. This should be done automatically in case RBAC objects backup is present. If RBAC objects are found in backup, ClickHouse server SHALL restart automatically. By RBAC objects we mean the following:
    * `USER`
    * `ROLE`
    * `ROW POLICY`
    * `SETTINGS PROFILE`
    * `QUOTA`

### Command Line Interface

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.Usage
version: 1.0

The [clickhouse-backup] utility SHALL have CLI with the following usage

```bash
USAGE:
   clickhouse-backup <command> [-t, --tables=<db>.<table>] <backup_name>

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
   server          Run API server
   help, h         Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --help, -h              show help
   --version, -v           print the version
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.Tables
version: 1.0

The [clickhouse-backup] utility SHALL support `tables` option to list of tables that are present in the system.

```
USAGE:
   clickhouse-backup tables

OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]

```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.Create
version: 1.0

The [clickhouse-backup] utility SHALL support `create` option to create new backup.

* `-t, --table, --tables` option SHALL specify table(s) for backup. The option SHALL
   support match expression syntax and SHALL support more than one expression separated by
   a comma.

```
USAGE:
   clickhouse-backup create [-t, --tables=<db>.<table>] <backup_name>

DESCRIPTION:
   Create new backup

OPTIONS:
   --config FILE, -c FILE                   Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --table value, --tables value, -t value  
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.CreateRemote
version: 1.0

The [clickhouse-backup] utility SHALL support `create_remote` option to create new backup.

* `-t, --table, --tables` option SHALL specify table(s) for backup. The option SHALL
   support match expression syntax and SHALL support more than one expression separated by
   a comma.

```
USAGE:
   clickhouse-backup create_remote [-t, --tables=<db>.<table>] [--diff-from=<backup_name>] [--delete] <backup_name>

DESCRIPTION:
   Create and upload

OPTIONS:
   --config FILE, -c FILE                   Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --table value, --tables value, -t value  
   --diff-from value                        
   --schema, -s                             Schemas only

```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.MatchExpression
version: 1.0

The [clickhouse-backup] utility SHALL support match expression syntax for specifying `skip_tables` option in configuration and for use with `--table` flag.
Syntax must be based on
the https://golang.org/pkg/path/filepath/#Match that SHALL have the following
pattern syntax:

```
pattern:
	{ term }
term:
	'*'         matches any sequence of non-Separator characters
	'?'         matches any single non-Separator character
	'[' [ '^' ] { character-range } ']'
	            character class (must be non-empty)
	c           matches character c (c != '*', '?', '\\', '[')
	'\\' c      matches character c

character-range:
	c           matches character c (c != '\\', '-', ']')
	'\\' c      matches character c
	lo '-' hi   matches character c for lo <= c <= hi
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.Upload
version: 1.0

The [clickhouse-backup] utility SHALL support `upload` option to upload backup to remote storage. This command SHALL automatically upload ClickHouse configs and RBAC objects backups if they are present (in case backup was created with `--configs` and/or `--rbac`).

* `--diff-from` option SHALL specify backup to be used for difference

```
USAGE:
   clickhouse-backup upload [--diff-from=<backup_name>] <backup_name>

OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --diff-from value   
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.List
version: 1.0

The [clickhouse-backup] utility SHALL support `list` option to print the list of backups.

* type `all|local|remote` option SHALL specify backup type
* version `latest|penult` option SHALL specify backup for difference with `latest` selecting the latest backup
  and `penult` selecting the backup right before the latest.

```
USAGE:
   clickhouse-backup list [all|local|remote] [latest|penult]

OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.Download
version: 1.0

The [clickhouse-backup] utility SHALL support `download` option to download backup from remote storage. This command SHALL automatically download ClickHouse configs and RBAC objects backups if they are present (in case backup was created with `--configs` and/or `--rbac`).

* `backup_name` SHALL specify the name of the backup

```
USAGE:
   clickhouse-backup download <backup_name>

OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.Restore
version: 1.0

The [clickhouse-backup] utility SHALL support `restore` option to create schema and restore data from backup.

* `--table, --tables, -t` SHALL specify table(s) name
* `-s, --schema` SHALL specify to restore schema only
* `-d, --data` SHALL specify to restore data only
* `--rm, --drop` SHALL specify to drop table before restore

```
USAGE:
   clickhouse-backup restore [--schema] [--data] [-t, --tables=<db>.<table>] <backup_name>

OPTIONS:
   --config FILE, -c FILE                   Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --table value, --tables value, -t value  
   --schema, -s                             Restore schema only
   --data, -d                               Restore data only
   --rm, --drop                             Drop table before restore
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.RestoreRemote
version: 1.0

The [clickhouse-backup] utility SHALL support `restore_remote` option to create schema and restore data from remote backup.

* `--table, --tables, -t` SHALL specify table(s) name
* `-s, --schema` SHALL specify to restore schema only
* `-d, --data` SHALL specify to restore data only
* `--rm, --drop` SHALL specify to drop table before restore

```
USAGE:
   clickhouse-backup create_remote [-t, --tables=<db>.<table>] [--diff-from=<backup_name>] [--delete] <backup_name>

DESCRIPTION:
   Create and upload

OPTIONS:
   --config FILE, -c FILE                   Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
   --table value, --tables value, -t value  
   --diff-from value                        
   --schema, -s                             Schemas only
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.Delete
version: 1.0

The [clickhouse-backup] utility SHALL support `delete` option to delete specific backup.

* type `local|remote` SHALL specify backup type
* `backup_name` SHALL specify the name of the backup

```
USAGE:
   clickhouse-backup delete <local|remote> <backup_name>

OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.DefaultConfig
version: 1.0

The [clickhouse-backup] utility SHALL support `default-config` option to print default config.

```
USAGE:
   clickhouse-backup default-config [command options] [arguments...]

OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.Server
version: 1.0

The [clickhouse-backup] utility SHALL support `server` option to run REST API server.

```
USAGE:
   clickhouse-backup server [command options] [arguments...]

OPTIONS:
   --config FILE, -c FILE  Config FILE name. (default: "/etc/clickhouse-backup/config.yml") [$CLICKHOUSE_BACKUP_CONFIG]
```

#### RQ.SRS-013.ClickHouse.BackupUtility.CLI.Help
version: 1.0

The [clickhouse-backup] utility SHALL support `help` option to show the list of commands or help for one command.

### Configuration

#### RQ.SRS-013.ClickHouse.BackupUtility.Configuration
version: 1.0

The [clickhouse-backup] utility SHALL support the following options in the configuration file.
All options SHALL be overwritable via environment variables.

```bash
general:
  remote_storage: none           # REMOTE_STORAGE
  max_file_size: 107374182400    # MAX_FILE_SIZE
  disable_progress_bar: false    # DISABLE_PROGRESS_BAR
  backups_to_keep_local: 0       # BACKUPS_TO_KEEP_LOCAL
  backups_to_keep_remote: 0      # BACKUPS_TO_KEEP_REMOTE
  log_level: info                # LOG_LEVEL
  allow_empty_backups: false     # ALLOW_EMPTY_BACKUPS
clickhouse:
  username: default                # CLICKHOUSE_USERNAME
  password: ""                     # CLICKHOUSE_PASSWORD
  host: localhost                  # CLICKHOUSE_HOST
  port: 9000                       # CLICKHOUSE_PORT
  disk_mapping: {}                 # CLICKHOUSE_DISK_MAPPING
  skip_tables:                     # CLICKHOUSE_SKIP_TABLES
    - system.*
  timeout: 5m                      # CLICKHOUSE_TIMEOUT
  freeze_by_part: false            # CLICKHOUSE_FREEZE_BY_PART
  secure: false                    # CLICKHOUSE_SECURE
  skip_verify: false               # CLICKHOUSE_SKIP_VERIFY
  sync_replicated_tables: true     # CLICKHOUSE_SYNC_REPLICATED_TABLES
  log_sql_queries: true            # CLICKHOUSE_LOG_SQL_QUERIES

  config_dir:      "/etc/clickhouse-server"              # CLICKHOUSE_CONFIG_DIR
  restart_command: "systemctl restart clickhouse-server" # CLICKHOUSE_RESTART_COMMAND

azblob:
  endpoint_suffix: "core.windows.net" # AZBLOB_ENDPOINT_SUFFIX
  account_name: ""             # AZBLOB_ACCOUNT_NAME
  account_key: ""              # AZBLOB_ACCOUNT_KEY
  sas: ""                      # AZBLOB_SAS
  container: ""                # AZBLOB_CONTAINER
  path: ""                     # AZBLOB_PATH
  compression_level: 1         # AZBLOB_COMPRESSION_LEVEL
  compression_format: tar      # AZBLOB_COMPRESSION_FORMAT
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
  compression_level: 1             # S3_COMPRESSION_LEVEL
  # supports 'tar', 'gzip', 'zstd', 'brotli'
  compression_format: tar          # S3_COMPRESSION_FORMAT
  # empty (default), AES256, or aws:kms
  sse: AES256                      # S3_SSE
  disable_cert_verification: false # S3_DISABLE_CERT_VERIFICATION
  storage_class: STANDARD          # S3_STORAGE_CLASS
  debug: false                     # S3_DEBUG
gcs:
  credentials_file: ""         # GCS_CREDENTIALS_FILE
  credentials_json: ""         # GCS_CREDENTIALS_JSON
  bucket: ""                   # GCS_BUCKET
  path: ""                     # GCS_PATH
  compression_level: 1         # GCS_COMPRESSION_LEVEL
  compression_format: tar      # GCS_COMPRESSION_FORMAT
cos:
  url: ""                      # COS_URL
  timeout: 2m                  # COS_TIMEOUT
  secret_id: ""                # COS_SECRET_ID
  secret_key: ""               # COS_SECRET_KEY
  path: ""                     # COS_PATH
  compression_format: tar      # COS_COMPRESSION_FORMAT
  compression_level: 1         # COS_COMPRESSION_LEVEL
api:
  listen: "localhost:7171"     # API_LISTEN
  enable_metrics: true         # API_ENABLE_METRICS
  enable_pprof: false          # API_ENABLE_PPROF
  username: ""                 # API_USERNAME
  password: ""                 # API_PASSWORD
  secure: false                # API_SECURE
  certificate_file: ""         # API_CERTIFICATE_FILE
  private_key_file: ""         # API_PRIVATE_KEY_FILE
  create_integration_tables: false # API_CREATE_INTEGRATION_TABLES
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
  compression_format: tar      # SFTP_COMPRESSION_FORMAT
  compression_level: 1         # SFTP_COMPRESSION_LEVEL
```

### REST API Server

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server
version: 1.0

The [clickhouse-backup] utility SHALL support `clickhouse-backup server`
command to start REST API server.

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.AsyncPostQuery
version: 1.0

The [clickhouse-backup] utility SHALL return `code 423` if a POST request is received while previous operation is not finished yet. The request SHALL NOT be executed in such case.

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.OutputFormat.Default
version: 1.0

The [clickhouse-backup] utility REST API server SHALL return results
in a format compatible with [ClickHouse]'s `JSONEachRow` format
for all the the endpoints.

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetTables
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
GET /backup/tables
```

to print the list of tables. For example, `curl -s localhost:7171/backup/tables | jq`.

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostCreate
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
POST /backup/create
```

to create new backup. For example, `curl -s localhost:7171/backup/create -X POST | jq`.

Also, the following optional arguments SHALL be supported:
* Optional query argument `table` SHALL work the same as the `--table <value>` CLI argument.
* Optional query argument `name` SHALL work the same as specifying a backup name with the CLI.
* Optional query argument `schema` SHALL work the same as the `--schema` CLI argument (backup schema only).
* Optional query argument `rbac` SHALL work the same as the `--rbac` CLI argument (backup RBAC only).
* Optional query argument `configs` SHALL work the same as the `--configs` CLI argument (backup configs only).

Optional arguments may be defined as following: `curl -s 'localhost:7171/backup/create?table=default.billing&name=billing_test' -X POST`.

This operation SHALL be async, so the API will return once the operation has been started.

Response contents shall have the following structure:
```
{"status":"acknowledged","operation":"create","backup_name":"<backup_name>"}
```

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostCreate.Remote
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
POST /backup/actions -H "Content-Type: application/json" -d '{"command":"create_remote <backup_name>"}'
```

to create new remote backup.

This operation SHALL be async, so the API will return once the operation has been started.


#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostUpload
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
POST /backup/upload
```

to upload backup to remote storage. This command SHALL automatically upload ClickHouse configs and RBAC objects backups if they are present (in case backup was created with `--configs` and/or `--rbac`).

For example, `curl -s localhost:7171/backup/upload/<BACKUP_NAME> -X POST | jq`.

* optional query argument `diff-from` SHALL work the same as the `--diff-from` CLI argument.

This operation SHALL be async, so the API will return once the operation has been started.

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
GET /backup/list
```

to print list of backups. For example, `curl -s localhost:7171/backup/list | jq`.

The `size` field SHALL not be populated for local backups.

Response SHALL have the following format:
```
{'name': 'backup_1', 'created': '<backup_1_creation_time>', 'size': <size>, 'location': '<local|remote>', 'required': '', 'desc': ''}
{'name': 'backup_2', 'created': '<backup_2_creation_time>', 'size': <size>, 'location': '<local|remote>', 'required': '', 'desc': ''}
...
```

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList.Local
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
GET /backup/list/local
```

to print list of Remote backups. For example, `curl -s localhost:7171/backup/list/local | jq`.

The `size` field SHALL not be populated.

Response SHALL have the following format:
```
{'name': 'backup_1', 'created': '<backup_1_creation_time>', 'size': <size>, 'location': 'local', 'required': '', 'desc': ''}
{'name': 'backup_2', 'created': '<backup_2_creation_time>', 'size': <size>, 'location': 'local', 'required': '', 'desc': ''}
...
```

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetList.Remote
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
GET /backup/list/remote
```

to print list of remote backups. For example, `curl -s localhost:7171/backup/list/remote | jq`.

Response SHALL have the following format:
```
{'name': 'backup_1', 'created': '<backup_1_creation_time>', 'size': <size>, 'location': 'remote', 'required': '', 'desc': ''}
{'name': 'backup_2', 'created': '<backup_2_creation_time>', 'size': <size>, 'location': 'remote', 'required': '', 'desc': ''}
...
```

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDownload
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
POST /backup/download
```

to download backup from remote storage. This command SHALL automatically download ClickHouse configs and RBAC objects backups if they are present (in case backup was created with `--configs` and/or `--rbac`).

For example, `curl -s localhost:7171/backup/download/<BACKUP_NAME> -X POST | jq`.

This operation SHALL be async, so the API will return once the operation has been started.

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostRestore
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
POST /backup/restore
```

to create schema and restore data from backup.

For example, `curl -s localhost:7171/backup/restore/<BACKUP_NAME> -X POST | jq`.

* optional query argument `table` SHALL work the same as the `--table` value CLI argument
* optional query argument `schema` SHALL work the same as the `--schema` CLI argument (restore schema only)
* Optional query argument `data` SHALL work the same as the `--data` CLI argument (restore data only).
* Optional query argument `rbac` SHALL work the same as the `--rbac` CLI argument (restore RBAC only).
* Optional query argument `configs` SHALL work the same as the `--configs` CLI argument (restore configs only).

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostRestore.Remote
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
POST /backup/actions -H "Content-Type: application/json" -d '{"command":"restore_remote <backup_name>"}'
```

to restore from an existing remote backup.

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDelete.Remote
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
POST /backup/delete/remote
```

to delete specific remote backup.

For example, `curl -s localhost:7171/backup/delete/remote/<BACKUP_NAME> -X POST | jq`.

Response contents shall have the following structure:
```
{"status":"success","operation":"delete","backup_name":"<backup_name>","location":"remote"}

```

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostDelete.Local
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
POST /backup/delete/local
```

to delete specific local backup.


For example, `curl -s localhost:7171/backup/delete/local/<BACKUP_NAME> -X POST | jq`.

Response contents shall have the following structure:
```
{"status":"success","operation":"delete","backup_name":"<backup_name>","location":"local"}
```

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetStatus
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
GET /backup/status
```

to display list of current async operations.
For example, `curl -s localhost:7171/backup/status | jq`.

Response contents shall have the following structure:
```
{'command': '<command name>', 'status': 'in progress', 'start': '2021-10-26 12:11:34'}
```

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.GetActions
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
GET /backup/actions
```

to display a list of current async operations.

#### RQ.SRS-013.ClickHouse.BackupUtility.REST.API.Server.PostActions
version: 1.0

The [clickhouse-backup] utility REST API server SHALL support

```
POST /backup/actions
```

to create a new action with parameters being the same as for the CLI commands.
All CLI actions such as `create`, `upload` etc. SHALL be supported.

## References

* **clickhouse-backup**: https://github.com/AlexAkulov/clickhouse-backup
* **ClickHouse:** https://clickhouse.tech
* **GitHub Repository:** https://github.com/AlexAkulov/clickhouse-backup/blob/master/test/testflows/clickhouse_backup/requirements/requirements.md
* **Revision History:** https://github.com/AlexAkulov/clickhouse-backup/commits/master/test/testflows/clickhouse_backup/requirements/requirements.md
* **Git:** https://git-scm.com/

[SRS]: #srs
[clickhouse-backup]: https://github.com/AlexAkulov/clickhouse-backup
[ClickHouse]: https://clickhouse.tech
[GitHub Repository]: https://github.com/AlexAkulov/clickhouse-backup/blob/master/test/testflows/clickhouse_backup/requirements/requirements.md
[Revision History]: https://github.com/AlexAkulov/clickhouse-backup/commits/master/test/testflows/clickhouse_backup/requirements/requirements.md
[Git]: https://git-scm.com/
[GitHub]: https://github.com/
''')
