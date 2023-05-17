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
destination server
```bash
mkdir -p /var/lib/clickhouse/backups/backup_name
```
source server
```bash
clickhouse-backup create backup_name
rsync --rsh=ssh /var/lib/clickhouse/backups/backup_name/ user@dst_server:/var/lib/clickhouse/backups/backup_name
```

destination server
```bash
clickhouse-backup restore --rm backup_name
```

## How to reduce number of partitions
...

## How to monitor that backups created and uploaded correctly
Use services like https://healthchecks.io or https://deadmanssnitch.com.
Or use `clickhouse-backup server` and prometheus endpoint :7171/metrics, look alerts examples on https://github.com/Altinity/clickhouse-operator/blob/master/deploy/prometheus/prometheus-alert-rules-backup.yaml

## How to make backup / restore sharded cluster 
### BACKUP
run only on the first replica for each shard
```bash
shard_number=$(clickhouse-client -q "SELECT getMacro('shard')")
clickhouse-backup create_remote shard${shard_number}-backup
clickhouse-backup delete local shard${shard_number}-backup
```

### RESTORE
run on all replicas
```bash
shard_number=$(clickhouse-client -q "SELECT getMacro('shard')")
clickhouse-backup restore_remote --rm --schema shard${shard_number}-backup
clickhouse-backup delete local shard${shard_number}-backup
```
after it, run only on the first replica for each shard
```bash
shard_number=$(clickhouse-client -q "SELECT getMacro('shard')")
clickhouse-backup restore_remote --rm shard${shard_number}-backup
clickhouse-backup delete local shard${shard_number}-backup
```

## How to make backup sharded cluster with Ansible
On the first day of month full backup will be uploaded and increments on the others days.
`hosts: clickhouse-cluster` shall be only first replica on each shard

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

## How to make backup database with several terabytes of data
You can use clickhouse-backup for creating periodical backups and keep it local. It protects you from destructive operations.
In addition, you may create instance of ClickHouse on another DC and have it fresh by clickhouse-copier it protects you from hardware or DC failures.

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
         dataVolumeClaimTemplate: data-volume
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
              replicasCount: 2
   templates:
      volumeClaimTemplates:
         - name: data-volume
           spec:
              accessModes:
                 - ReadWriteOnce
              resources:
                 requests:
                    storage: 10Gi
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
                   image: clickhouse/clickhouse-server:22.8
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
                      # {shard} macro defined by clickhouse-operator
                      - name: S3_PATH
                        value: backup/shard-{shard}
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

Also, you can apply CronJob to run `clickhouse-backup` actions by schedule
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
        metadata:
          labels:
            app: clickhouse-backup-cron
        spec:
          restartPolicy: Never
          containers:
            - name: run-backup-cron
              image: clickhouse/clickhouse-client:latest
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
                # change to 1, if you want make full backup only in $FULL_BACKUP_WEEKDAY (1 - Mon, 7 - Sun)
                - name: MAKE_INCREMENT_BACKUP
                  value: "1"
                - name: FULL_BACKUP_WEEKDAY
                  value: "1"
              command:
                - bash
                - -ec
                - CLICKHOUSE_SERVICES=$(echo $CLICKHOUSE_SERVICES | tr "," " ");
                  BACKUP_DATE=$(date +%Y-%m-%d-%H-%M-%S);
                  declare -A BACKUP_NAMES;
                  declare -A DIFF_FROM;
                  if [[ "" != "$BACKUP_PASSWORD" ]]; then
                    BACKUP_PASSWORD="--password=$BACKUP_PASSWORD";
                  fi;
                  for SERVER in $CLICKHOUSE_SERVICES; do
                    if [[ "1" == "$MAKE_INCREMENT_BACKUP" ]]; then
                      LAST_FULL_BACKUP=$(clickhouse-client -q "SELECT name FROM system.backup_list WHERE location='remote' AND name LIKE '%full%' AND desc NOT LIKE 'broken%' ORDER BY created DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD);
                      TODAY_FULL_BACKUP=$(clickhouse-client -q "SELECT name FROM system.backup_list WHERE location='remote' AND name LIKE '%full%' AND desc NOT LIKE 'broken%' AND toDate(created) = today() ORDER BY created DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD)
                      PREV_BACKUP_NAME=$(clickhouse-client -q "SELECT name FROM system.backup_list WHERE location='remote' AND desc NOT LIKE 'broken%' ORDER BY created DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD);
                      DIFF_FROM[$SERVER]="";
                      if [[ ("$FULL_BACKUP_WEEKDAY" == "$(date +%u)" && "" == "$TODAY_FULL_BACKUP") || "" == "$PREV_BACKUP_NAME" || "" == "$LAST_FULL_BACKUP" ]]; then
                        BACKUP_NAMES[$SERVER]="full-$BACKUP_DATE";
                      else
                        BACKUP_NAMES[$SERVER]="increment-$BACKUP_DATE";
                        DIFF_FROM[$SERVER]="--diff-from-remote=$PREV_BACKUP_NAME";
                      fi
                    else
                      BACKUP_NAMES[$SERVER]="full-$BACKUP_DATE";
                    fi;
                    echo "set backup name on $SERVER = ${BACKUP_NAMES[$SERVER]}";
                  done;
                  for SERVER in $CLICKHOUSE_SERVICES; do
                    echo "create ${BACKUP_NAMES[$SERVER]} on $SERVER";
                    clickhouse-client --echo -mn -q "INSERT INTO system.backup_actions(command) VALUES('create ${SERVER}-${BACKUP_NAMES[$SERVER]}')" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
                  done;
                  for SERVER in $CLICKHOUSE_SERVICES; do
                    while [[ "in progress" == $(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='create ${SERVER}-${BACKUP_NAMES[$SERVER]}' FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD) ]]; do
                      echo "still in progress ${BACKUP_NAMES[$SERVER]} on $SERVER";
                      sleep 1;
                    done;
                    if [[ "success" != $(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='create ${SERVER}-${BACKUP_NAMES[$SERVER]}' FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD) ]]; then
                      echo "error create ${BACKUP_NAMES[$SERVER]} on $SERVER";
                      clickhouse-client -mn --echo -q "SELECT status,error FROM system.backup_actions WHERE command='create ${SERVER}-${BACKUP_NAMES[$SERVER]}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
                      exit 1;
                    fi;
                  done;
                  for SERVER in $CLICKHOUSE_SERVICES; do
                    echo "upload ${DIFF_FROM[$SERVER]} ${BACKUP_NAMES[$SERVER]} on $SERVER";
                    clickhouse-client --echo -mn -q "INSERT INTO system.backup_actions(command) VALUES('upload ${DIFF_FROM[$SERVER]} ${SERVER}-${BACKUP_NAMES[$SERVER]}')" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
                  done;
                  for SERVER in $CLICKHOUSE_SERVICES; do
                    while [[ "in progress" == $(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='upload ${DIFF_FROM[$SERVER]} ${SERVER}-${BACKUP_NAMES[$SERVER]}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD) ]]; do
                      echo "upload still in progress ${BACKUP_NAMES[$SERVER]} on $SERVER";
                      sleep 5;
                    done;
                    if [[ "success" != $(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='upload ${DIFF_FROM[$SERVER]} ${SERVER}-${BACKUP_NAMES[$SERVER]}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD) ]]; then
                      echo "error ${BACKUP_NAMES[$SERVER]} on $SERVER";
                      clickhouse-client -mn --echo -q "SELECT status,error FROM system.backup_actions WHERE command='upload ${DIFF_FROM[$SERVER]} ${SERVER}-${BACKUP_NAMES[$SERVER]}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
                      exit 1;
                    fi;
                    clickhouse-client --echo -mn -q "INSERT INTO system.backup_actions(command) VALUES('delete local ${SERVER}-${BACKUP_NAMES[$SERVER]}')" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
                  done;
                  echo "BACKUP CREATED"
```

For one time restore data you could use `Job`
```yaml
# example to restore latest backup
apiVersion: batch/v1
kind: Job
metadata:
  name: clickhouse-backup-restore
spec:
  backoffLimit: 0
  template:
    metadata:
      name: clickhouse-backup-restore
      labels:
        app: clickhouse-backup-restore
    spec:
      restartPolicy: Never
      containers:
      - name: clickhouse-backup-restore
        image: clickhouse/clickhouse-client:latest
        imagePullPolicy: IfNotPresent
        env:
          # use all replicas in each shard to restore schema
          - name: CLICKHOUSE_SCHEMA_RESTORE_SERVICES
            value: chi-test-backups-default-0-0,chi-test-backups-default-0-1,chi-test-backups-default-1-0,chi-test-backups-default-1-1
          # use only first replica in each shard to restore data
          - name: CLICKHOUSE_DATA_RESTORE_SERVICES
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
        - if [[ "" != "$BACKUP_PASSWORD" ]]; then
            BACKUP_PASSWORD="--password=$BACKUP_PASSWORD";
          fi;
          declare -A BACKUP_NAMES;
          CLICKHOUSE_SCHEMA_RESTORE_SERVICES=$(echo $CLICKHOUSE_SCHEMA_RESTORE_SERVICES | tr "," " ");
          CLICKHOUSE_DATA_RESTORE_SERVICES=$(echo $CLICKHOUSE_DATA_RESTORE_SERVICES | tr "," " ");
          for SERVER in $CLICKHOUSE_SCHEMA_RESTORE_SERVICES; do
            LATEST_BACKUP_NAME=$(clickhouse-client -q "SELECT name FROM system.backup_list WHERE location='remote' AND desc NOT LIKE 'broken%' ORDER BY created DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD);
            if [[ "" == "$LATEST_BACKUP_NAME" ]]; then
              echo "Remote backup not found for $SERVER";
              exit 1;
            fi;
            BACKUP_NAMES[$SERVER]="$LATEST_BACKUP_NAME";
            clickhouse-client -mn --echo -q "INSERT INTO system.backup_actions(command) VALUES('restore_remote --schema --rm ${BACKUP_NAMES[$SERVER]}')" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
          done;
          for SERVER in $CLICKHOUSE_SCHEMA_RESTORE_SERVICES; do
            while [[ "in progress" == $(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='restore_remote --schema --rm ${BACKUP_NAMES[$SERVER]}' ORDER BY start DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD) ]]; do
              echo "still in progress ${BACKUP_NAMES[$SERVER]} on $SERVER";
              sleep 1;
            done;
            RESTORE_STATUS=$(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='restore_remote --schema --rm ${BACKUP_NAMES[$SERVER]}'  ORDER BY start DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD);
            if [[ "success" != "${RESTORE_STATUS}" ]]; then
              echo "error restore_remote --schema --rm ${BACKUP_NAMES[$SERVER]} on $SERVER";
              clickhouse-client -mn --echo -q "SELECT start,finish,status,error FROM system.backup_actions WHERE command='restore_remote --schema --rm ${BACKUP_NAMES[$SERVER]}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
              exit 1;
            fi;
            if [[ "success" == "${RESTORE_STATUS}" ]]; then
              echo "schema ${BACKUP_NAMES[$SERVER]} on $SERVER RESTORED";
              clickhouse-client -q "INSERT INTO system.backup_actions(command) VALUES('delete local ${BACKUP_NAMES[$SERVER]}')" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
            fi;
          done;
          for SERVER in $CLICKHOUSE_DATA_RESTORE_SERVICES; do
            clickhouse-client -mn --echo -q "INSERT INTO system.backup_actions(command) VALUES('restore_remote --data ${BACKUP_NAMES[$SERVER]}')" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
          done;
          for SERVER in $CLICKHOUSE_DATA_RESTORE_SERVICES; do
            while [[ "in progress" == $(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='restore_remote --data ${BACKUP_NAMES[$SERVER]}' ORDER BY start DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD) ]]; do
              echo "still in progress ${BACKUP_NAMES[$SERVER]} on $SERVER";
              sleep 1;
            done;
            RESTORE_STATUS=$(clickhouse-client -mn -q "SELECT status FROM system.backup_actions WHERE command='restore_remote --data ${BACKUP_NAMES[$SERVER]}'  ORDER BY start DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD);
            if [[ "success" != "${RESTORE_STATUS}" ]]; then
              echo "error restore_remote --data ${BACKUP_NAMES[$SERVER]} on $SERVER";
              clickhouse-client -mn --echo -q "SELECT start,finish,status,error FROM system.backup_actions WHERE command='restore_remote --data ${BACKUP_NAMES[$SERVER]}'" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
              exit 1;
            fi;
            echo "data ${BACKUP_NAMES[$SERVER]} on $SERVER RESTORED";
            if [[ "success" == "${RESTORE_STATUS}" ]]; then
              clickhouse-client -q "INSERT INTO system.backup_actions(command) VALUES('delete local ${BACKUP_NAMES[$SERVER]}')" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
            fi;
          done
```

## How do incremental backups work to remote storage
- Incremental backup calculate increment only during execute `upload` or `create_remote` command or similar REST API request.
- Currently, incremental backup calculate increment only on table parts level, look to ClickHouse documentation to fill the difference between [data parts](https://clickhouse.tech/docs/en/operations/system-tables/parts/) and [table partitions](https://clickhouse.tech/docs/en/operations/system-tables/partitions/).  
- To calculate increment, backup which listed on `--diff-from` parameter is required to be present as local backup, look to `clickhouse-backup list` command results for ensure.
- Currently, during execute `clickhouse-backup upload --diff-from=base_backup` don't check `base_backup` exits on remote storage, be careful.
- During upload operation `base_backup` added to current backup metadata as required. All data parts which exists in `base_backup` also mark in backup metadata table level with `required` flag and skip data uploading. 
- During download, if backup contains link to `required` backup it will try to fully download first. This action apply recursively. If you have a chain of incremental backups, all incremental backups in the chain and first "full" will download to local storage. 
- Size of increment depends not only on the intensity your data ingestion and also depends on the intensity background merges for data parts in your tables. Please increase how much rows you will ingest during one INSERT query and don't apply often [table data mutations](https://clickhouse.tech/docs/en/operations/system-tables/mutations/).
- Look to [ClickHouse documentation](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/) and try to understand how exactly `*MergeTree` table engine works.

## How to work `watch` command
Current implementation simple and will improve in next releases
- When `watch` command start, it call create_remote+delete command sequence to make `full` backup
- Then it wait `watch-interval` time period and call create_remote+delete command sequence again, type of backup will `full` if `full-interval` expired after last full backup created and `incremental`, if not.
