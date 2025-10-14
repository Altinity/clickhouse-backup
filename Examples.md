# Use cases of clickhouse-backup

## Simple cron script for daily backups and remote upload

```bash
#!/bin/bash
BACKUP_NAME=my_backup_$(date -u +%Y-%m-%dT%H-%M-%S)
clickhouse-backup create $BACKUP_NAME >> /var/log/clickhouse-backup.log 2>&1
exit_code=$?
if [[ $exit_code != 0 ]]; then
  echo "clickhouse-backup create $BACKUP_NAME FAILED and return $exit_code exit code"
  exit $exit_code
fi

clickhouse-backup upload $BACKUP_NAME >> /var/log/clickhouse-backup.log 2>&1
exit_code=$?
if [[ $exit_code != 0 ]]; then
  echo "clickhouse-backup upload $BACKUP_NAME FAILED and return $exit_code exit code"
  exit $exit_code
fi
```

## How to convert MergeTree to ReplicatedMergeTree

This doesn't work for tables created in `MergeTree(date_column, (primary keys columns), 8192)` format

1. Create backup
   ```
   clickhouse-backup create --table='my_db.my_table' my_backup
   ```
2. Edit `/var/lib/clickhouse/backup/my_backup/metadata/my_db/my_table.json`, change `query` field,
   replace MergeTree() with ReplicatedMergeTree() with parameters according
   to https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication/#creating-replicated-tables
3. Drop table in Clickhouse
   ```
   clickhouse-client -q "DROP TABLE my_db.my.table NO DELAY"
   ```
4. Restore backup
   ```
   clickhouse-backup restore --rm my_backup
   ```

## How to store backups on NFS, backup drive or another server via SFTP

Use `rsync`.
`rsync` supports hard links, which means that a backup on a remote server or mounted fs will be stored as efficiently as
in `/var/lib/clickhouse/backup`.
You can create a daily backup by clickhouse-backup and a sync backup folder to mounted fs with this command:
`rsync -a -H --delete --progress --numeric-ids --update /var/lib/clickhouse/backup/ /mnt/data/clickhouse-backup/` or
similar for sync over ssh. In this case `rsync` will copy only difference between backups.

## How to move data to another ClickHouse server

On the destination server:

```bash
mkdir -p /var/lib/clickhouse/backup/backup_name
```

On the source server:

```bash
clickhouse-backup create backup_name
rsync --rsh=ssh /var/lib/clickhouse/backup/backup_name/ user@dst_server:/var/lib/clickhouse/backup/backup_name
```

On the destination server:

```bash
clickhouse-backup restore --rm backup_name
```

## How to monitor that backups were created and uploaded correctly

Use services like https://healthchecks.io or https://deadmanssnitch.com.
Or use `clickhouse-backup server` and prometheus endpoint `:7171/metrics`. For an example of setting up Prometheus
alerts,
see https://github.com/Altinity/clickhouse-operator/blob/master/deploy/prometheus/prometheus-alert-rules-backup.yaml.

## How to back up / restore a sharded cluster

### BACKUP

Run only on the first replica for each shard:

```bash
shard_number=$(clickhouse-client -q "SELECT getMacro('shard')")
clickhouse-backup create_remote shard${shard_number}-backup
clickhouse-backup delete local shard${shard_number}-backup
```

### RESTORE

Run on all replicas:

```bash
shard_number=$(clickhouse-client -q "SELECT getMacro('shard')")
clickhouse-backup restore_remote --rm --schema shard${shard_number}-backup
clickhouse-backup delete local shard${shard_number}-backup
```

After that, run only on the first replica for each shard:

```bash
shard_number=$(clickhouse-client -q "SELECT getMacro('shard')")
clickhouse-backup restore_remote --rm shard${shard_number}-backup
clickhouse-backup delete local shard${shard_number}-backup
```

## How to back up a sharded cluster with Ansible

On the first day of month a full backup will be uploaded and increments on the other days.
`hosts: clickhouse-cluster` shall be only the first replica on each shard

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

## How to back up a database with several terabytes of data

You can use clickhouse-backup for creating periodical backups and keep it local. It protects you from destructive
operations.
In addition, you may create instance of ClickHouse on another DC and have it fresh by clickhouse-copier to protect you
from hardware or DC failures.

## How to use clickhouse-backup in Kubernetes

Install the [clickhouse kubernetes operator](https://github.com/Altinity/clickhouse-operator/) and use the following
manifest.

This particular example creates a `ClickHouseInstallation` which creates a 2-replica 2-shard cluster with two
containers - the `clickhouse-server` container and a `clickhouse-backup` container, running in server mode for creating
backups both with access to `data-volume` volume. The `clickhouse-backup` container runs `clickhouse-backup` in server
mode which provides the backup API (`system.backup_list` and `system.backup_actions`, URL engine tables to the backup
API). The S3-compatible object storage credentials and endpoint values are provided via environment variables to the
`clickhouse-backup` container, this object storage being where the backup will be uploaded to:

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
              image: altinity/clickhouse-backup:latest
              imagePullPolicy: Always
              args: [ "server" ]
              env:
                - name: LOG_LEVEL
                  value: "info"
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
                - name: S3_REGION
                  value: us-east-1
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
                  value: "false"
                # require to avoid double scraping clickhouse and clickhouse-backup containers
              ports:
                - name: backup-rest
                  containerPort: 7171
```

If you want to test with a minio setup, as the example above provides environment variables for, you will need to
prepare remote storage using Minio for test only as this also works perfectly with AWS S3 for production:

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

To run scheduled backups, you can also use CronJob to run `clickhouse-backup` actions.

Note: This cron job uses the `clickhouse-server` container which also contains `clickhouse-client` via a script that
loops through each server/node in the cluster, querying `system.backup_list` and inserting into `system.backup_actions`
resulting in creating backups per `schedule` specified. Previously, it used the `clickhouse-client` image which isn't
supported on Kubernetes clusters on arm64 architecture.

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
              # https://hub.docker.com/r/clickhouse/clickhouse-client/tags, doesn't support arm64 platform
              # `clickhouse-server` this is the same symlink with `clickhouse-client` to `clickhouse` binary 
              image: clickhouse/clickhouse-server:latest
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
                # change to 1, if you want to make full backup only in $FULL_BACKUP_WEEKDAY (1 - Mon, 7 - Sun)
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
                  LAST_FULL_BACKUP=$(clickhouse-client -q "SELECT name FROM system.backup_list WHERE location='remote' AND name LIKE '%${SERVER}%' AND name LIKE '%full%' AND desc NOT LIKE 'broken%' ORDER BY created DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD);
                  TODAY_FULL_BACKUP=$(clickhouse-client -q "SELECT name FROM system.backup_list WHERE location='remote' AND name LIKE '%${SERVER}%' AND name LIKE '%full%' AND desc NOT LIKE 'broken%' AND toDate(created) = today() ORDER BY created DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD)
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

For one time restore data, you can use `Job`:

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
              SHARDED_PREFIX=${SERVER%-*}
              LATEST_BACKUP_NAME=$(clickhouse-client -q "SELECT name FROM system.backup_list WHERE location='remote' AND desc NOT LIKE 'broken%' AND name LIKE '%${SHARDED_PREFIX}%' ORDER BY created DESC LIMIT 1 FORMAT TabSeparatedRaw" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD);
              if [[ "" == "$LATEST_BACKUP_NAME" ]]; then
              echo "Remote backup not found for $SERVER";
              exit 1;
              fi;
              BACKUP_NAMES[$SERVER]="$LATEST_BACKUP_NAME";
              clickhouse-client -mn --echo -q "INSERT INTO system.backup_actions(command) VALUES('restore_remote --schema --rm ${BACKUP_NAMES[$SERVER]}')" --host="$SERVER" --port="$CLICKHOUSE_PORT" --user="$BACKUP_USER" $BACKUP_PASSWORD;
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

## How to back up object disks to s3 with s3:CopyObject

To properly make backup your object s3 disks to s3 backup bucket you need to have minimal access rights via IAM

```json
{
  "Id": "altinity-clickhouse-backup-for-s3-iam-your-uniq-name",
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "altinity-clickhouse-backup-for-s3-iam-your-uniq-name",
      "Action": [
        "s3:GetBucketVersioning",
        "s3:ListBucket"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::<your-object-disks-bucket>",
      "Principal": {
        "AWS": [
          "arn:aws:iam::<your-aws-acount-id-for-backup>:user/<your-backup-user>"
        ]
      }
    },
    {
      "Sid": "altinity-clickhouse-backup-for-s3-iam-your-uniq-name",
      "Action": [
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::<your-object-disks-bucket>/*",
      "Principal": {
        "AWS": [
          "arn:aws:iam::<your-aws-acount-id-for-backup>:user/<your-backup-user>"
        ]
      }
    }
  ]
}
```

Store this content into `backup.json`

Use following command to detect `Principal` field value

```
AWS_ACCESS_KEY_ID=<backup-cretentials-access-key-id> AWS_SECRET_ACCESS_KEY=<backup-cretentials-access-secret-key> aws sts get-caller-identity
```

Use following command to put IAM policy to s3 object disks bucket

```
aws s3api put-bucket-policy --bucket <your-object-disk-bucket> --policy="$(cat backup.json)"  
```

## How to restore object disks to s3 with s3:CopyObject

To properly restore your object s3 disks from s3 backup bucket you need to have minimal access rights via IAM

```json
{
  "Id": "altinity-clickhouse-restore-for-s3-iam-your-uniq-name",
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "altinity-clickhouse-restore-for-s3-iam-your-uniq-name",
      "Action": [
        "s3:GetBucketVersioning",
        "s3:ListBucket"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::<your-backup-bucket>",
      "Principal": {
        "AWS": [
          "arn:aws:iam::<your-aws-acount-id-for-object-disks-user>:user/<your-object-disks-user>"
        ]
      }
    },
    {
      "Sid": "altinity-clickhouse-restore-for-s3-iam-your-uniq-name",
      "Action": [
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::<your-backup-bucket>/*",
      "Principal": {
        "AWS": [
          "arn:aws:iam::<your-aws-acount-id-for-object-disks-user>:user/<your-object-disks-user>"
        ]
      }
    }
  ]
}
```

Store this content into `backup.json`

Use following command to detect `Principal` field value

```
AWS_ACCESS_KEY_ID=<object-disks-cretentials-access-key-id> AWS_SECRET_ACCESS_KEY=<object-disks-cretentials-secret-access-key> aws sts get-caller-identity
```

Use following command to attach IAM policy to s3 object disks bucket

```
aws s3api put-bucket-policy --bucket <your-object-disk-bucket> --policy="$(cat backup.json)"  
```

## How to use AWS IRSA and IAM to allow S3 backup without Explicit credentials

Create Role <ROLE NAME> and IAM Policy. This field typically looks like this:
`arn:aws:iam::1393332413596:role/rolename-clickhouse-backup`,
where `1393332413596` is the ID of the role and
`rolename-clickhouse-backup` is the name of the role.
See [the AWS documentation](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/setting-up-enable-IAM.html)
for all the details.

Create a service account with annotations:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: <SERVICE ACCOUNT NAME>
  namespace: <NAMESPACE>
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::<ACCOUNT_NAME>:role/<ROLE_NAME>
```

Link the service account to a podTemplate to create `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` environment
variables:

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: <NAME>
  namespace: <NAMESPACE>
spec:
  defaults:
    templates:
      podTemplate: <POD_TEMPLATE_NAME>
  templates:
    podTemplates:
      - name: <POD_TEMPLATE_NAME>
        spec:
          serviceAccountName: <SERVICE ACCOUNT NAME>
          containers:
            - name: clickhouse-backup
```

### How to use clickhouse-backup + clickhouse-operator in FIPS compatible mode in Kubernetes for S3

Use the image `altinity/clickhouse-backup:X.X.X-fips` (where X.X.X is the version number).
Run the following commands to generate self-signed TLS keys for secure clickhouse-backup API endpoint:
(You need to renew these certs periodically; use https://github.com/cert-manager/cert-manager for it in kubernetes.)

```bash
   openssl genrsa -out ca-key.pem 4096
   openssl req -subj "/O=altinity" -x509 -new -nodes -key ca-key.pem -sha256 -days 365000 -out ca-cert.pem
   openssl genrsa -out server-key.pem 4096
   openssl req -subj "/CN=localhost" -addext "subjectAltName = DNS:localhost,DNS:*.cluster.local" -new -key server-key.pem -out server-req.csr
   openssl x509 -req -days 365 -extensions SAN -extfile <(printf "\n[SAN]\nsubjectAltName=DNS:localhost,DNS:*.cluster.local") -in server-req.csr -out server-cert.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial
```

Create the following `ConfigMap` + `ClickHouseInstallation` kubernetes manifest:

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: backup-tls-certs
data:
  ca-key.pem: |-
    -----BEGIN PRIVATE KEY-----
    data from openssl related command described above
    -----END PRIVATE KEY-----
  ca-cert.pem: |-
    -----BEGIN PRIVATE KEY-----
    data from openssl related command described above
    -----END PRIVATE KEY-----
  server-key.pem: |-
    -----BEGIN PRIVATE KEY-----
    data from openssl related command described above
    -----END PRIVATE KEY-----
  server-cert.pem: |-
    -----BEGIN CERTIFICATE-----
    data from openssl related command described above
    -----END CERTIFICATE-----      
---
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: fips-example
spec:
  defaults:
    templates:
      podTemplate: clickhouse-backup-fips
      dataVolumeClaimTemplate: data-volume
  configuration:
    clusters:
      - name: default
        layout:
          shardsCount: 1
          replicasCount: 1
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
      - name: clickhouse-backup-fips
        spec:
          securityContext:
            runAsUser: 101
            runAsGroup: 101
            fsGroup: 101
          containers:
            - name: clickhouse
              image: clickhouse/clickhouse-server:latest
              command:
                - clickhouse-server
                - --config-file=/etc/clickhouse-server/config.xml
            - name: clickhouse-backup
              image: altinity/clickhouse-backup:latest-fips
              imagePullPolicy: Always
              command:
                - bash
                - -xc
                - "/bin/clickhouse-backup server"
              env:
                - name: AWS_USE_FIPS_ENDPOINT
                  value: "true"
                # use properly value  
                - name: AWS_REGION
                  value: us-east-2
                - name: S3_REGION
                  value: us-east-2
                - name: API_SECURE
                  value: "true"
                - name: API_PRIVATE_KEY_FILE
                  value: "/etc/ssl/clickhouse-backup/server-key.pem"
                - name: API_CERTIFICATE_FILE
                  value: "/etc/ssl/clickhouse-backup/server-cert.pem"
                - name: API_LISTEN
                  value: "0.0.0.0:7171"
                # INSERT INTO system.backup_actions to execute backup
                - name: API_CREATE_INTEGRATION_TABLES
                  value: "true"
                - name: BACKUPS_TO_KEEP_REMOTE
                  value: "3"
                - name: REMOTE_STORAGE
                  value: "s3"
                # change it to production bucket name  
                - name: S3_BUCKET
                  value: bucket-name
                # {shard} macro defined by clickhouse-operator
                - name: S3_PATH
                  value: backup/shard-{shard}
                - name: S3_ACCESS_KEY
                  value: backup-access-key
                - name: S3_SECRET_KEY
                  value: backup-secret-key
              # require to avoid double scraping clickhouse and clickhouse-backup containers
              ports:
                - name: backup-rest
                  containerPort: 7171
```

## How incremental backups work with remote storage

- Incremental backup calculates the increment only while executing `upload` or `create_remote` commands or similar REST
  API requests.
- When `use_embedded_backup_restore: false`, then incremental backup calculates the increment only on the table parts
  level.
- When `use_embedded_backup_restore: true`, then incremental backup calculates by the checksums on file level, this
  approach more effective.
- For ClickHouse version 23.3+, see the ClickHouse documentation to find the difference
  between [data parts](https://clickhouse.com/docs/en/operations/system-tables/parts/)
  and [table partitions](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key).
- To calculate the increment, the backup listed on the `--diff-from` parameter is required to be present as a local
  backup. Check the `clickhouse-backup list` command results for errors.
- During upload, `base_backup` is added to current backup metadata as `required_backup` in `backup_name/metadata.json`.
  All data parts that exist in `base_backup` also mark in the backup metadata table level with `required` flag in
  `backup_name/metadata/database/table.json` and skip data uploading.
- During download, if a backup contains link to a `required_backup`, each table which contains parts marked as
  `required` will download these parts to local storage after complete downloading for non `required` parts. If you have
  a chain of incremental backups and required parts exist in this chain, then this action applies recursively.
- The size of the increment depends not only on the intensity of your data ingestion but also on the intensity of
  background merges for data parts in your tables. Please increase how many rows you will ingest during one INSERT query
  and don't do frequent [table data mutations](https://clickhouse.com/docs/en/operations/system-tables/mutations/).
- See the [ClickHouse documentation](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/)
  for information on how the `*MergeTree` table engine works.

## How backups_to_keep_remote works

- get list remote backup
- select oldest backups to stay only last backups equal `backups_to_keep_remote`
- before deleting old backup check backup dependencies, we can't delete `dead` remote backup if any `live` backup have
  direct or recursive reference to `dead` backup

## How to watch backups work

The current implementation is simple and will improve in next releases.

- When the `watch` command starts, it calls the `create_remote+delete command` sequence to make a `full` backup
- Then it waits `watch-interval` time period and calls the `create_remote+delete` command sequence again. The type of
  backup will be `full` if `full-interval` expired after last full backup created and `incremental` if not.

## How to restore a database or table to a different name

This example will restore a remote backup of database, and restore it to another differently named database. As well, the example will also demonstrate restoring a backed up table restored to a different table name. NOTE: this process will also work for local backups

Assumptions are that s3 credentials are already set up in a file called `s3.xml`:

```
<clickhouse>
  <s3>
    <endpoint-name>
      <endpoint>https://backup-bucket.s3.amazonaws.com</endpoint>
      <access_key_id>AKREDACTED</access_key_id>
      <secret_access_key>12345abcdefg</secret_access_key>
    </endpoint-name>
  </s3>
</clickhouse>
```

### Backup and restore a database to a different database name

A remote database backup of the database `test` is created, first for an entire database with the backup name of `test`:

`clickhouse-backup create test`

Confirmation of the backup once completed:

```
clickhouse-backup list

test                                                             21/04/2025 18:24:42   remote      all:60.86MiB,data:60.70MiB,arch:60.85MiB,obj:2.50KiB,meta:0B,rbac:0B,conf:0B      tar, regular
```

The user wants to restore this database as `testnew`, using the remote backup named `test`:

```
$ clickhouse-backup restore_remote --restore-database-mapping test:testnew test
```

Confirmation on Clickhouse:

```
:) show databases;

SHOW DATABASES

Query id: 5d42ae97-a521-4b60-8dc2-706b94416d78

   ┌─name───────────────┐
1. │ INFORMATION_SCHEMA │
2. │ default            │
3. │ information_schema │
4. │ system             │
5. │ test               │
6. │ testnew            │
   └────────────────────┘

```


### Backup and restore a table to a different table name

In this example, a backup is made only of a table because in order to do this, only a table backup will work. In this example, the table name is `trips`, and is being restored using the name `trips2` to the same database the database-level backup was made, `test`,  

First, the backup is made:

```$ clickhouse-backup create_remote test.trips```

Then confirmed:

```
$ clickhouse-backup list

iB,meta:0B,rbac:0B,conf:0B      tar, regular
test                                                             21/04/2025 18:24:42   remote      all:60.86MiB,data:60.70MiB,arch:60.85MiB,obj:2.50KiB,meta:0B,rbac:0B,conf:0B      tar, regular
test.trips                                                       21/04/2025 18:47:39   remote      all:121.71MiB,data:121.41MiB,arch:121.71MiB,obj:5.81KiB,meta:0B,rbac:0B,conf:0B   tar, regular
```

Then restored using the table backup named `test.trips`:

```
$ clickhouse-backup restore_remote --restore-table-mapping test:trips2 test.trips
```

Then confirmed:

```
:) show tables from test

SHOW TABLES FROM test

Query id: 9e8ed231-dc4b-499d-b02b-57a555a3d309

   ┌─name────────┐
1. │ trips       │
2. │ trips2      │
   └─────────────┘

```

## How to track operation status with operation_id

All asynchronous API operations now return an `operation_id` that can be used to track the specific operation status:

```bash
# 1. Create backup and capture operation_id
response=$(curl -s -X POST 'localhost:7171/backup/create?name=my-backup')
operation_id=$(echo $response | jq -r '.operation_id')

# 2. Check status of specific operation
curl -s "localhost:7171/backup/status?operationid=$operation_id" | jq .

# 3. Monitor until completion
while true; do
  status=$(curl -s "localhost:7171/backup/status?operationid=$operation_id" | jq -r '.[0].status // "not_found"')
  if [ "$status" != "in_progress" ]; then
    echo "Operation completed with status: $status"
    break
  fi
  sleep 5
done
```
