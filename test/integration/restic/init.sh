CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export RESTIC_PASSWORD_FILE="${CUR_DIR}/password"
export RESTIC_REPOSITORY=s3:https://minio:9000/clickhouse/restic/cluster_name/shard_number
export AWS_ACCESS_KEY_ID=access_key
export AWS_SECRET_ACCESS_KEY=it_is_my_super_secret_key
export RESTIC_KEEP_LAST=7
export CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-custom-restic.yml
export CLICKHOUSE_PARAMS="--host '$(yq '.clickhouse.host' ${CLICKHOUSE_BACKUP_CONFIG})' --port '$(yq '.clickhouse.port' ${CLICKHOUSE_BACKUP_CONFIG})' --user '$(yq '.clickhouse.username' ${CLICKHOUSE_BACKUP_CONFIG})' --password '$(yq '.clickhouse.password' ${CLICKHOUSE_BACKUP_CONFIG})'"
restic cat config --insecure-tls > /dev/null || restic init --insecure-tls
