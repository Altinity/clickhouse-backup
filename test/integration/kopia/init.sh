CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export KOPIA_PASSWORD_FILE="${CUR_DIR}/password"
export KOPIA_S3_BUCKET=clickhouse
export KOPIA_S3_ENDPOINT=minio:9000
export AWS_ACCESS_KEY_ID=access-key
export AWS_SECRET_ACCESS_KEY=it-is-my-super-secret-key
export KOPIA_KEEP_LAST=7
export KOPIA_PASSWORD=kopia-repo-password
export KOPIA_CHECK_FOR_UPDATES=false
kopia repository connect s3 --endpoint=${KOPIA_S3_ENDPOINT} --disable-tls --bucket=${KOPIA_S3_BUCKET} --access-key=${AWS_ACCESS_KEY_ID} --secret-access-key=${AWS_SECRET_ACCESS_KEY} || kopia repository create s3 --endpoint=${KOPIA_S3_ENDPOINT} --disable-tls --bucket=${KOPIA_S3_BUCKET} --access-key=${AWS_ACCESS_KEY_ID} --secret-access-key=${AWS_SECRET_ACCESS_KEY}
kopia policy set --global  --keep-latest=${KOPIA_KEEP_LAST}