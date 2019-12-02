# Use cases of clickhouse-backup

## How to convert MergeTree to ReplicatedMegreTree
1. Create backup
   ```
   clickhouse-backup create --table='my_db.my_table' my_backup
   ```
2. Edit '/var/lib/clickhouse/backup/my_backup/metadata/my_db/my_table.sql'
3. Drop table in Clickhouse
   ```
   clickhouse -c "DROP TABLE my_db.my.table
   ```
4. Restore backup
   ```
   clickhouse-backup restore my_backup
   ```

## How to store backups on NFS, backup drive or another server via SFTP
Use 'rsync'
'rsync' supports hard links with means that backup on remote server or mounted fs will be stored as efficiently as in the '/var/lib/clickhouse/backup'.
You can create daily backup by clickhouse-backup and sync backup folder to mounted fs with this command:
`rsync -a -H --delete --progress --numeric-ids --update /var/lib/clickhouse/backup/ /mnt/data/clickhouse-backup/` or similar for sync over ssh. In this case rsync will copy only difference between backups.

## How to move data to another clickhouse server
See abowe

## How to reduce number of partitions
...

## How to monitor that backups created and uploaded correctly
Use services like https://healthchecks.io or https://deadmanssnitch.com.

## How to backup sharded cluster with Ansible
On the first day of month full backup will be uploaded and increments on the others days.

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

## How to backup database with several terabytes of data
You can use clickhouse-backup for creating periodical backups and keep it local. It protect you from destructive operations.
In addition you may create instance of ClickHouse on another DC and have it fresh by clickhouse-copier it protect you from hardware or DC failures.

## How to use clickhouse-backup in Kubernetes
...
