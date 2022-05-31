# Use cases of clickhouse-backup

## How to convert MergeTree to ReplicatedMergeTree
don't work for tables which created in `MergeTree(date_column, (primary keys columns), 8192)` format
1. Create backup
   ```
   clickhouse-backup create --table='my_db.my_table' my_backup
   ```
2. Edit `/var/lib/clickhouse/backup/my_backup/metadata/my_db/my_table.json`, change `query` field, 
   replace MergeTree() to ReplicatedMergeTree() with parameters according to https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication/#creating-replicated-tables
   

3. Drop table in Clickhouse
   ```
   clickhouse-client -q "DROP TABLE my_db.my.table NO DELAY"
   ```
4. Restore backup
   ```
   clickhouse-backup restore --rm my_backup
   ```

## How to store backups on NFS, backup drive or another server via SFTP
Use 'rsync'
'rsync' supports hard links with means that backup on remote server or mounted fs will be stored as efficiently as in the '/var/lib/clickhouse/backup'.
You can create daily backup by clickhouse-backup and sync backup folder to mounted fs with this command:
`rsync -a -H --delete --progress --numeric-ids --update /var/lib/clickhouse/backup/ /mnt/data/clickhouse-backup/` or similar for sync over ssh. In this case rsync will copy only difference between backups.

## How to move data to another clickhouse server
See above

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
Install [clickhouse kubernetes operator](https://github.com/Altinity/clickhouse-operator/) and use following manifest


```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
   name: test-backups
spec:
   defaults:
      templates:
         podTemplate: clickhouse-backup
   configuration:
      users:
         # use cluster Pod CIDR for more security
         backup/networks/ip: 0.0.0.0/0
         # PASSWORD=backup_password; echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
         backup/password_sha256_hex: eb94c11d77f46a0290ba8c4fca1a7fd315b72e1e6c83146e42117c568cc3ea4d
      settings:
         # to allow scrape metrics via embedded prometheus protocol
         prometheus/endpoint: /metrics
         prometheus/port: 8888
         prometheus/metrics: true
         prometheus/events: true
         prometheus/asynchronous_metrics: true
      # need install zookeeper separately, look to https://github.com/Altinity/clickhouse-operator/tree/master/deploy/zookeeper/ for details
      zookeeper:
         nodes:
            - host: zookeeper
              port: 2181
         session_timeout_ms: 5000
         operation_timeout_ms: 5000
      clusters:
         - name: default
           layout:
              # 2 shards one replica in each
              shardsCount: 2
              replicasCount: 1
   templates:
      podTemplates:
         - name: clickhouse-backup
           metadata:
              annotations:
                 prometheus.io/scrape: 'true'
                 prometheus.io/port: '8888'
                 prometheus.io/path: '/metrics'
                 # need separate prometheus scrape config, look to https://github.com/prometheus/prometheus/issues/3756
                 clickhouse.backup/scrape: 'true'
                 clickhouse.backup/port: '7171'
                 clickhouse.backup/path: '/metrics'
           spec:
              securityContext:
                 runAsUser: 101
                 runAsGroup: 101
                 fsGroup: 101
              containers:
                 - name: clickhouse-pod
                   image: clickhouse/clickhouse-server:22.3
                   command:
                      - clickhouse-server
                      - --config-file=/etc/clickhouse-server/config.xml
                 - name: clickhouse-backup
                   image: altinity/clickhouse-backup:master
                   imagePullPolicy: Always
                   command:
                      - bash
                      - -xc
                      - "/bin/clickhouse-backup server"
                   env:
                      - name: LOG_LEVEL
                        value: "debug"
                      - name: ALLOW_EMPTY_BACKUPS
                        value: "true"
                      - name: API_LISTEN
                        value: "0.0.0.0:7171"
                      # INSERT INTO system.backup_actions to execute backup
                      - name: API_CREATE_INTEGRATION_TABLES
                        value: "true"
                      - name: BACKUPS_TO_KEEP_REMOTE
                        value: "3"
                      # change it for production S3
                      - name: REMOTE_STORAGE
                        value: "s3"
                      - name: S3_ACL
                        value: "private"
                      - name: S3_ENDPOINT
                        value: http://s3-backup-minio:9000
                      - name: S3_BUCKET
                        value: clickhouse
                      - name: S3_PATH
                        value: backup
                      - name: S3_ACCESS_KEY
                        value: backup-access-key
                      - name: S3_SECRET_KEY
                        value: backup-secret-key
                      - name: S3_FORCE_PATH_STYLE
                        value: "true"
                      # remove it for production S3
                      - name: S3_DISABLE_SSL
                        value: "true"
                      - name: S3_DEBUG
                        value: "true"
                    # require to avoid double scraping clickhouse and clickhouse-backup containers
                   ports:
                      - name: backup-rest
                        containerPort: 7171
```

You need to prepare remote storage, for test only
```yaml
---
apiVersion: "apps/v1"
kind: Deployment
metadata:
  name: s3-backup-minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: s3-backup-minio
  template:
    metadata:
      labels:
        app: s3-backup-minio
    spec:
      containers:
        - name: minio
          image: minio/minio:latest
          env:
            - name: MINIO_ACCESS_KEY
              value: backup-access-key
            - name: MINIO_SECRET_KEY
              value: backup-secret-key
          command:
            - sh
            - -xc
            - mkdir -p doc_gen_minio/export/clickhouse && minio server doc_gen_minio/export
          ports:
              - name: minio
                containerPort: 9000
---
apiVersion: v1
kind: Service
metadata:
  name: s3-backup-minio
spec:
  type: ClusterIP
  selector:
    app: s3-backup-minio
  ports:
    - name: s3
      port: 9000
      protocol: TCP
      targetPort: minio
```

Also, you can apply CronJob to run clickouse-backup actions by schedule
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: clickhouse-backup-cron
spec:
  # every day at 00:00
  schedule: "0 0 * * *"
  concurrencyPolicy: "Forbid"
  jobTemplate:
    spec:
      backoffLimit: 1
      completions: 1
      parallelism: 1
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: run-backup-cron
              image: yandex/clickhouse-client:latest
              imagePullPolicy: IfNotPresent
              env:
                # use first replica in each shard, use `kubectl get svc | grep test-backups`
                - name: CLICKHOUSE_SERVICES
                  value: chi-test-backups-default-0-0,chi-test-backups-default-1-0
                - name: CLICKHOUSE_PORT
                  value: "9000"
                - name: BACKUP_USER
                  value: backup
                - name: BACKUP_PASSWORD
                  value: "backup_password"
              command:
                - bash
                - -ec
                - CLICKHOUSE_SERVICES=$(echo $CLICKHOUSE_SERVICES | tr "," " ");
                  BACKUP_DATE=$(date +%Y-%m-%d-%H-%M-%S);
                  BACKUP_NAME="full-$BACKUP_DATE";
                  if [[ "" != "$BACKUP_PASSWORD" ]]; then
                    BACKUP_PASSWORD="--password=$BACKUP_PASSWORD";
                  fi;
                  for SERVER in $CLICKHOUSE_SERVICES; do
                    echo "create $BACKUP_NAME on $SERVER";
                    clickhouse-client --echo -mn -q "INSERT INTO system.backup_actions(command) VALUES('create ${SERVER}-${BACKUP_NAME}')" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
                  done;
                  for SERVER in $CLICKHOUSE_SERVICES; do
                    while [[ "in progress" == $(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='create ${SERVER}-${BACKUP_NAME}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD) ]]; do
                      echo "still in progress $BACKUP_NAME on $SERVER";
                      sleep 1;
                    done;
                    if [[ "success" != $(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='create ${SERVER}-${BACKUP_NAME}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD) ]]; then
                      echo "error create $BACKUP_NAME on $SERVER";
                      clickhouse-client -mn --echo -q "SELECT status,error FROM system.backup_actions WHERE command='create ${SERVER}-${BACKUP_NAME}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
                      exit 1;
                    fi;
                  done;
                  for SERVER in $CLICKHOUSE_SERVICES; do
                    echo "upload $BACKUP_NAME on $SERVER";
                    clickhouse-client --echo -mn -q "INSERT INTO system.backup_actions(command) VALUES('upload ${SERVER}-${BACKUP_NAME}')" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
                  done;
                  for SERVER in $CLICKHOUSE_SERVICES; do
                    while [[ "in progress" == $(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='upload ${SERVER}-${BACKUP_NAME}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD) ]]; do
                      echo "upload still in progress $BACKUP_NAME on $SERVER";
                      sleep 5;
                    done;
                    if [[ "success" != $(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='upload ${SERVER}-${BACKUP_NAME}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD) ]]; then
                      echo "error $BACKUP_NAME on $SERVER";
                      clickhouse-client -mn --echo -q "SELECT status,error FROM system.backup_actions WHERE command='upload ${SERVER}-${BACKUP_NAME}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
                      exit 1;
                    fi;
                  done;
                  echo "BACKUP CREATED"
```


## How do incremental backups work to remote storage
- Incremental backup calculate increment only during execute `upload` or `create_remote` command or similar REST API request.
- Currently, incremental backup calculate increment only on table parts level, look to ClicHouse documentation to fill the difference between [data parts](https://clickhouse.tech/docs/en/operations/system-tables/parts/) and [table partitions](https://clickhouse.tech/docs/en/operations/system-tables/partitions/).  
- To calculate increment, backup which listed on `--diff-from` parameter is required to be present as local backup, look to `clickhouse-backup list` command results for ensure.
- Currently, during execute `clickhouse-backup upload --diff-from=base_backup` don't check `base_backup` exits on remote storage, be carefull.
- During upload operation `base_backup` added to current backup metadata as required. All data parts which exists in `base_backup` also mark in backup metadata table level with `required` flag and skip data uploading. 
- During download, if backup contains link to `required` backup it will try to fully download first. This action apply recursively. If you have a chain of incremental backups, all incremental backups in the chain and first "full" will download to local storage. 
- Size of increment depends not only on the intensity your data ingestion and also depends on the intensity background merges for data parts in your tables. Please increase how much rows you will ingest during one INSERT query and don't apply often [table data mutations](https://clickhouse.tech/docs/en/operations/system-tables/mutations/).
- Look to [ClicHouse documentation](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/) and try to understand how exactly `*MergeTree` table engine works.
