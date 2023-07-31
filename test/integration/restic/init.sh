CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export RESTIC_PASSWORD_FILE="${CUR_DIR}/password"
export RESTIC_REPOSITORY=s3:http://minio:9000/clickhouse/restic/cluster_name/shard_number
export AWS_ACCESS_KEY_ID=access-key
export AWS_SECRET_ACCESS_KEY=it-is-my-super-secret-key
export RESTIC_KEEP_LAST=7
export CLICKHOUSE_PARAMS="--host '$(yq '.clickhouse.host' /etc/clickhouse-backup/config.yml)' --port '$(yq '.clickhouse.port' /etc/clickhouse-backup/config.yml)' --user '$(yq '.clickhouse.username' /etc/clickhouse-backup/config.yml)' --password '$(yq '.clickhouse.password' /etc/clickhouse-backup/config.yml)'"
restic cat config > /dev/null || restic init