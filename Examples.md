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

## Tuning for high-bandwidth (10Gbit) networks

The default buffer sizes and HTTP transport settings are tuned for low-bandwidth environments. On 10Gbit+ links to
S3/GCS-compatible object storage they limit throughput, see https://github.com/Altinity/clickhouse-backup/issues/1376.

The config below shows **only the parameters that change** from their defaults; keep the rest of your config as is.
Start from these values and adjust to your hardware and network. The single most impactful knob is
`s3.http_max_idle_conns_per_host` — Go's default of 2 forces most parallel streams to open a fresh TCP+TLS connection
per request when `upload_concurrency`/`download_concurrency` are high.

```yaml
general:
  # let compression run ahead of uploads, and copy files in larger chunks
  pipe_buffer_size: 8388608          # 8MB (default 128KB)
  download_copy_buffer_size: 1048576 # 1MB (default 0 = Go's 32KB io.Copy buffer)
  # raise concurrency to actually saturate a 10Gbit link (tune to CPU cores and storage)
  upload_concurrency: 16
  download_concurrency: 16
s3:
  buffer_size: 1048576               # 1MB per-part s3manager buffer (default 64KB)
  http_max_idle_conns_per_host: 128  # default 2 (!), critical for parallel streams to the same endpoint
  http_max_idle_conns: 512           # default AWS SDK value
  http_write_buffer_size: 1048576    # 1MB (default 4KB)
  http_read_buffer_size: 1048576     # 1MB (default 4KB)
  http_idle_conn_timeout: 120s       # default 90s
  chunk_size: 67108864               # 64MB multipart part size (default 0 = remoteSize / max_parts_count, min 5MB), fewer parts for large files
# when remote_storage: gcs
gcs:
  upload_buffer_size: 1048576        # 1MB (default 128KB)
# when remote_storage: sftp
sftp:
  concurrency: 16                    # parallel requests per file
  max_packet_size: 262144            # 256KB SFTP payload per packet (default 32KB), only works with servers that accept >32KB packets
# when remote_storage: azblob
azblob:
  buffer_count: 16                   # AZBLOB_MAX_BUFFERS, parallel block buffers per upload (default 3); the per-block size auto-scales from max_parts_count
  max_parts_count: 1024              # default 256, larger backups need more blocks
```

Notes for the other backends:
- **azblob**: there is no separate buffer-size knob — the upload block size auto-scales (2–10MB) from `max_parts_count`, and `buffer_count` controls how many blocks upload in parallel. Raise both for fast networks.
- **cos**: the SDK uses `cos.DNSScatterTransport` for internal Tencent endpoints, which already spreads connections across IPs; raise `concurrency` rather than touching the HTTP pool.
- **ftp**: the FTP library has no transfer-buffer knob; throughput is governed by `concurrency` (connection pool size).

## Multi-threaded zstd/gzip compression

By default `compression_use_multi_thread: false`, each compression stream is single-threaded. clickhouse-backup already
parallelizes compression across tables via `upload_concurrency`/`download_concurrency`, so per-stream multi-threading
mainly over-subscribes the CPU and reduces total throughput, see https://github.com/Altinity/clickhouse-backup/issues/1378.

```yaml
general:
  remote_storage: s3
  compression_use_multi_thread: true # COMPRESSION_USE_MULTI_THREAD, zstd WithEncoderConcurrency/WithDecoderConcurrency, gzip via pgzip
  compression_threads: 8             # COMPRESSION_THREADS, per-stream threads (zstd concurrency / pgzip block workers); 0 = auto (GOMAXPROCS)
  compression_buffer_size: 4194304   # COMPRESSION_BUFFER_SIZE, 4MB; zstd encoder window / pgzip block size (multi-threaded gzip)
s3:
  compression_format: zstd           # or gzip; the three settings above only affect zstd and gzip
  compression_level: 3
```

Notes:
- `compression_use_multi_thread` enables parallel zstd encode/decode (`WithEncoderConcurrency`/`WithDecoderConcurrency`)
  and switches gzip to the parallel `pgzip` implementation.
- `compression_threads` sets how many threads each stream uses when `compression_use_multi_thread` is enabled (zstd
  concurrency / pgzip block workers); `0` means auto (`GOMAXPROCS`). It must be left at `0` when multi-thread is off.
- `compression_buffer_size` meaning depends on the format and on `compression_use_multi_thread`:
  - **zstd**: encoder window size (`WithWindowSize`), must be a power of two between 1024 and 536870912; larger windows
    improve the compression ratio at the cost of more memory and CPU.
  - **gzip, single-threaded**: DEFLATE window size (`gzip.NewWriterWindow`), 32..32768 — the gzip format caps the window
    at 32KB, so values above that are rejected.
  - **gzip, multi-threaded**: `pgzip` block size (`SetConcurrency`), must be greater than 16384 (e.g. 1–4MB).
- The other formats (`lz4`, `bzip2`, `sz`, `xz`, `brotli`) ignore both settings.

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

## How to use Azure AD Workload Identity to allow AZBLOB backup without Explicit credentials

This is the Azure equivalent of AWS IRSA. On AKS with
[Workload Identity](https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview)
enabled, a Kubernetes `ServiceAccount` is federated to an Azure user-assigned managed identity,
so `clickhouse-backup` authenticates to Azure Blob Storage without an `account_key` or `sas`.

A resource group is just a named container for Azure resources; it must already exist before you
put resources into it. There are three groups in play here and they are NOT necessarily the same
(role assignment and workload-identity federation work across resource groups and subscriptions):

```bash
# 1) resource group of your AKS cluster — look it up in the ResourceGroup column:
az aks list -o table
AKS_RESOURCE_GROUP=<AKS_RESOURCE_GROUP_FROM_LIST>

# 2) the storage account that will hold the backups — pick an existing one (az storage account
az storage account list -o table
STORAGE_ACCOUNT=<STORAGE_ACCOUNT_FROM_LIST>
# or create it (name must be globally unique, 3-24 lowercase alphanumeric chars):
# only if it does not exist yet
az storage account create --name "your-account-name" --resource-group "${AKS_RESOURCE_GROUP}" --location <REGION>   
STORAGE_ACCOUNT=your-account-name

# full resource id of the account (includes subscription + resource group) — used as the role scope:
STORAGE_ACCOUNT_ID=$(az storage account show --name "${STORAGE_ACCOUNT}" --query id -o tsv)

# 3) resource group for the managed identity — reuse an existing one
az group list -o table
IDENTITY_RESOURCE_GROUP=<IDENTITY_RESOURCE_GROUP_FROM_LIST>
#    or create a dedicated one:
az group create --name "your-identity-resource-group" --location <REGION>   # 
IDENTITY_RESOURCE_GROUP=your-identity-resource-group
```

Create a user-assigned managed identity and grant it access to the storage account
(`Storage Blob Data Contributor` on the account or container):

```bash
IDENTITY_NAME=your-new-idenity-name
az identity create --name ${IDENTITY_NAME} --resource-group "${IDENTITY_RESOURCE_GROUP}"
# capture the client id used below as ${CLIENT_ID} in the ServiceAccount annotation
CLIENT_ID=$(az identity show --name ${IDENTITY_NAME} --resource-group "${IDENTITY_RESOURCE_GROUP}" --query clientId -o tsv)
PRINCIPAL_ID=$(az identity show --name ${IDENTITY_NAME} --resource-group "${IDENTITY_RESOURCE_GROUP}" --query principalId -o tsv)
# tenant id of the managed identity, used below as ${TENANT_ID} in the ServiceAccount annotation
TENANT_ID=$(az identity show --name ${IDENTITY_NAME} --resource-group "${IDENTITY_RESOURCE_GROUP}" --query tenantId -o tsv)
az role assignment create \
  --assignee-object-id "${PRINCIPAL_ID}" --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope "${STORAGE_ACCOUNT_ID}"
```

Federate the managed identity with the AKS Kubernetes `ServiceAccount` via the cluster OIDC issuer:

```bash
NAMESPACE=your-kubernetes-namespace
kubectl create ns ${NAMESPACE}
# will create later in kubernetes
SERVICE_ACCOUNT_NAME=your-kubernetes-service-account
 
OIDC_ISSUER=$(az aks show --name <CLUSTER_NAME> --resource-group "${AKS_RESOURCE_GROUP}" --query oidcIssuerProfile.issuerUrl -o tsv)
FEDERATED_CREDENTIAL_NAME=your-federated-cretential-name
az identity federated-credential create \
  --name ${FEDERATED_CREDENTIAL_NAME} \
  --identity-name <IDENTITY_NAME> --resource-group "${IDENTITY_RESOURCE_GROUP}" \
  --issuer "${OIDC_ISSUER}" \
  --subject "system:serviceaccount:${NAMESPACE}:${SERVICE_ACCOUNT_NAME}" \
  --audience api://AzureADTokenExchange
```

Create a service account with annotations:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${NAMESPACE}
  annotations:
    azure.workload.identity/client-id: "${CLIENT_ID}"
    azure.workload.identity/tenant-id: "${TENANT_ID}"
```

Put the `clickhouse-backup` config into a `ConfigMap` (no `account_key`/`sas` needed). With
`use_managed_identity: true`, `clickhouse-backup` uses `DefaultAzureCredential`, which
automatically consumes the federated token injected by the webhook. Mount this `ConfigMap` into
`/etc/clickhouse-backup/`, add the `azure.workload.identity/use: "true"` label so the mutating
webhook injects `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_AUTHORITY_HOST` and
`AZURE_FEDERATED_TOKEN_FILE`, and link the service account to the podTemplate:

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: clickhouse-backup-config
  namespace: ${NAMESPACE}
data:
  config.yml: |
    azblob:
      use_managed_identity: true
      # ${STORAGE_ACCOUNT} — the storage account created/selected in the bash steps above,
      # the one you granted "Storage Blob Data Contributor" on in the role assignment
      account_name: ${STORAGE_ACCOUNT}
      # blob container that holds the backups (e.g. `clickhouse-backup`);
      # create it with: az storage container create --name <CONTAINER> --account-name "${STORAGE_ACCOUNT}" --auth-mode login
      # clickhouse-backup also auto-creates it on first upload (default assume_container_exists: false)
      container: <CONTAINER>
---
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: <NAME>
  namespace: ${NAMESPACE}
spec:
  defaults:
    templates:
      podTemplate: <POD_TEMPLATE_NAME>
  templates:
    podTemplates:
      - name: <POD_TEMPLATE_NAME>
        # the azure.workload.identity/use: "true" label together with serviceAccountName makes the
        # mutating webhook inject AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_AUTHORITY_HOST /
        # AZURE_FEDERATED_TOKEN_FILE into every container of this pod
        metadata:
          labels:
            azure.workload.identity/use: "true"
        spec:
          serviceAccountName: ${SERVICE_ACCOUNT_NAME}
          containers:
            - name: clickhouse
              image: clickhouse/clickhouse-server:latest
            - name: clickhouse-backup
              image: altinity/clickhouse-backup:latest
              command:
                - bash
                - -xc
                - "/bin/clickhouse-backup server"
              volumeMounts:
                - name: clickhouse-backup-config
                  mountPath: /etc/clickhouse-backup/
          volumes:
            - name: clickhouse-backup-config
              configMap:
                name: clickhouse-backup-config
```

## How to use GCP Workload Identity to allow GCS backup without Explicit credentials

This is the Google Cloud equivalent of AWS IRSA. On GKE with
[Workload Identity Federation for GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
enabled, a Kubernetes `ServiceAccount` (KSA) is bound to a Google Cloud IAM service account (GSA),
so `clickhouse-backup` authenticates to Google Cloud Storage without a `credentials_file` or
`credentials_json`.

`clickhouse-backup` supports two GCS auth modes under Workload Identity:

- **Direct binding** — annotate the KSA with the target GSA. The pod's Application Default
  Credentials (ADC) already *are* that GSA, so leave the whole `gcs.credentials_*`/`gcs.sa_email`
  block empty and `clickhouse-backup` uses ADC automatically.
- **Impersonation via `gcs.sa_email`** — the pod runs as one identity (a "source" GSA bound to the
  KSA, or even the cluster default) and `clickhouse-backup` mints a short-lived token for a separate
  "target" GSA that owns the bucket permissions. Set `gcs.sa_email` to the target GSA email; the
  source identity needs `roles/iam.serviceAccountTokenCreator` on the target. This is the GCS
  `sa_email` flow and is what the steps below configure.

First set the variables used throughout (look the values up if you don't know them):

```bash
# project that owns the GCS bucket and the service accounts
gcloud projects list
PROJECT_ID=<YOUR_PROJECT_ID>
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')

# GKE cluster that runs clickhouse — Workload Identity must be enabled on it:
gcloud container clusters list
CLUSTER_NAME=<YOUR_CLUSTER_NAME>
CLUSTER_LOCATION=<YOUR_CLUSTER_REGION_OR_ZONE>
# enable Workload Identity if it is not already (no-op if already enabled):
gcloud container clusters update "${CLUSTER_NAME}" --location "${CLUSTER_LOCATION}" \
  --workload-pool="${PROJECT_ID}.svc.id.goog"

# GCS bucket that will hold the backups — pick an existing one:
gcloud storage buckets list --format='value(name)'
GCS_BUCKET=<YOUR_BUCKET>
# or create it (bucket names are globally unique):
gcloud storage buckets create gs://your-bucket-name --project "${PROJECT_ID}" --location <REGION>
GCS_BUCKET=your-bucket-name

# kubernetes namespace and service account name (created later):
NAMESPACE=your-kubernetes-namespace
SERVICE_ACCOUNT_NAME=your-kubernetes-service-account
```

Create the **target** Google Cloud service account (its email becomes `gcs.sa_email`) and grant it
access to the bucket (`roles/storage.objectAdmin` scoped to the bucket is enough for
backup/restore; use `roles/storage.admin` if `clickhouse-backup` must also create the bucket):

```bash
TARGET_GSA_NAME=clickhouse-backup-gcs-sa-name
gcloud iam service-accounts create "${TARGET_GSA_NAME}" --project "${PROJECT_ID}" \
  --display-name "clickhouse-backup GCS access"
TARGET_GSA_EMAIL="${TARGET_GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# grant bucket access to the target GSA (scoped to the single bucket):
gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
  --member "serviceAccount:${TARGET_GSA_EMAIL}" \
  --role "roles/storage.objectAdmin"
```

Bind the Kubernetes `ServiceAccount` to a **source** identity via Workload Identity, and allow that
source identity to impersonate the target GSA. The simplest source identity is the target GSA
itself bound directly to the KSA — then the source impersonates itself, which keeps a single GSA in
play while still exercising the `gcs.sa_email` flow:

```bash
# allow the KSA to act as the source GSA (here: the same target GSA):
gcloud iam service-accounts add-iam-policy-binding "${TARGET_GSA_EMAIL}" \
  --project "${PROJECT_ID}" \
  --role "roles/iam.workloadIdentityUser" \
  --member "serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${SERVICE_ACCOUNT_NAME}]"

# allow the source identity to mint impersonated tokens for the target GSA
# (required because gcs.sa_email goes through impersonate.CredentialsTokenSource):
gcloud iam service-accounts add-iam-policy-binding "${TARGET_GSA_EMAIL}" \
  --project "${PROJECT_ID}" \
  --role "roles/iam.serviceAccountTokenCreator" \
  --member "serviceAccount:${TARGET_GSA_EMAIL}"
```

Create the namespace and a service account annotated with the source GSA so the GKE webhook injects
the Workload Identity credentials into the pod:

```bash
kubectl create ns "${NAMESPACE}"
```

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${NAMESPACE}
  annotations:
    # the source GSA the pod runs as; clickhouse-backup then impersonates gcs.sa_email
    iam.gke.io/gcp-service-account: ${TARGET_GSA_EMAIL}
```

Put the `clickhouse-backup` config into a `ConfigMap` (no `credentials_file`/`credentials_json`
needed). With `gcs.sa_email` set, `clickhouse-backup` uses the pod's ambient Workload Identity
credentials to impersonate the target service account. Mount this `ConfigMap` into
`/etc/clickhouse-backup/` and link the service account to the podTemplate:

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: clickhouse-backup-config
  namespace: ${NAMESPACE}
data:
  config.yml: |
    general:
      remote_storage: gcs
    gcs:
      # ${TARGET_GSA_EMAIL} — the target service account that owns the bucket permissions;
      # the pod's Workload Identity credentials impersonate it via impersonate.CredentialsTokenSource
      sa_email: ${TARGET_GSA_EMAIL}
      # ${GCS_BUCKET} — the bucket granted roles/storage.objectAdmin above
      bucket: ${GCS_BUCKET}
      path: backup
---
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: <NAME>
  namespace: ${NAMESPACE}
spec:
  defaults:
    templates:
      podTemplate: <POD_TEMPLATE_NAME>
  templates:
    podTemplates:
      - name: <POD_TEMPLATE_NAME>
        spec:
          serviceAccountName: ${SERVICE_ACCOUNT_NAME}
          containers:
            - name: clickhouse
              image: clickhouse/clickhouse-server:latest
            - name: clickhouse-backup
              image: altinity/clickhouse-backup:latest
              command:
                - bash
                - -xc
                - "/bin/clickhouse-backup server"
              volumeMounts:
                - name: clickhouse-backup-config
                  mountPath: /etc/clickhouse-backup/
          volumes:
            - name: clickhouse-backup-config
              configMap:
                name: clickhouse-backup-config
```

> If you prefer the **direct binding** mode instead, omit `gcs.sa_email` from the `ConfigMap`,
> keep the `iam.gke.io/gcp-service-account` annotation pointing at the GSA that owns the bucket,
> and skip the `roles/iam.serviceAccountTokenCreator` self-binding — `clickhouse-backup` will use
> Application Default Credentials directly.

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

## Minimal grants for backup user

Better use maximum grants, but minimal grants is here (could fail with restore RBAC objects)

```sql
CREATE ROLE IF NOT EXISTS backup_role;

GRANT SELECT ON system.* TO backup_role;
GRANT INSERT ON system.backup_actions TO backup_role;

GRANT ALTER FREEZE PARTITION ON *.* TO backup_role;
GRANT ALTER FETCH PARTITION ON *.* TO backup_role;

GRANT CREATE TABLE ON *.* TO backup_role;
GRANT DROP TABLE   ON *.* TO backup_role;

GRANT DROP DATABASE ON *.* TO backup_role;
GRANT CREATE DATABASE ON *.* TO backup_role;

CREATE USER IF NOT EXISTS backup_user IDENTIFIED WITH sha256_password BY 'YourStrongP@ssw0rd!';             
CREATE GRANT backup_role TO backup_user;
```
