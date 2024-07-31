CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export KOPIA_PASSWORD_FILE="${CUR_DIR}/password"
export KOPIA_S3_BUCKET=clickhouse
export KOPIA_S3_PATH=/kopia/cluster_name/shard_number/
export KOPIA_S3_ENDPOINT=minio:9000
export AWS_ACCESS_KEY_ID=access_key
export AWS_SECRET_ACCESS_KEY=it_is_my_super_secret_key
export KOPIA_KEEP_LAST=7
export KOPIA_PASSWORD=kopia-repo-password
export KOPIA_CHECK_FOR_UPDATES=false
export CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-custom-kopia.yml
export CLICKHOUSE_PARAMS="--host '$(yq '.clickhouse.host' ${CLICKHOUSE_BACKUP_CONFIG})' --port '$(yq '.clickhouse.port' ${CLICKHOUSE_BACKUP_CONFIG})' --user '$(yq '.clickhouse.username' ${CLICKHOUSE_BACKUP_CONFIG})' --password '$(yq '.clickhouse.password' ${CLICKHOUSE_BACKUP_CONFIG})'"
kopia repository connect s3 --endpoint=${KOPIA_S3_ENDPOINT} --disable-tls --bucket=${KOPIA_S3_BUCKET} --prefix=${KOPIA_S3_PATH} --access-key=${AWS_ACCESS_KEY_ID} --secret-access-key=${AWS_SECRET_ACCESS_KEY} || kopia repository create s3 --endpoint=${KOPIA_S3_ENDPOINT} --disable-tls --bucket=${KOPIA_S3_BUCKET} --prefix=${KOPIA_S3_PATH} --access-key=${AWS_ACCESS_KEY_ID} --secret-access-key=${AWS_SECRET_ACCESS_KEY}
kopia policy set --global  --keep-latest=${KOPIA_KEEP_LAST}
