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
kopia repository connect s3 --endpoint=${KOPIA_S3_ENDPOINT} --disable-tls-verification --bucket=${KOPIA_S3_BUCKET} --prefix=${KOPIA_S3_PATH} --access-key=${AWS_ACCESS_KEY_ID} --secret-access-key=${AWS_SECRET_ACCESS_KEY} || kopia repository create s3 --endpoint=${KOPIA_S3_ENDPOINT} --disable-tls-verification --bucket=${KOPIA_S3_BUCKET} --prefix=${KOPIA_S3_PATH} --access-key=${AWS_ACCESS_KEY_ID} --secret-access-key=${AWS_SECRET_ACCESS_KEY}
kopia policy set --global  --keep-latest=${KOPIA_KEEP_LAST}

# kopia_diag dumps repository state when a kopia script fails, to diagnose the
# flaky "content not found" inconsistency (which snapshot is broken, whether
# maintenance/GC actually ran, full content/snapshot verification). Output is
# appended to /tmp/kopia_diag.log (persisted across the 4 download retries so it
# survives even when the captured stderr stream is truncated) and mirrored to
# stderr (never to stdout, so it can't corrupt the JSON that list.sh prints).
# TestCustomKopia dumps /tmp/kopia_diag.log on failure.
# Wire it up in callers with: trap kopia_diag ERR
kopia_diag() {
  local rc=$?
  trap - ERR
  set +e
  {
    echo "===== KOPIA DIAGNOSTICS (exit=${rc}) ====="

    echo "----- repository status -----"
    kopia repository status

    echo "----- maintenance info -----"
    kopia maintenance info

    echo "----- snapshot list (all) -----"
    kopia snapshot list --all --json | jq -c -M '.[] | {id, path: .source.path, backup_name: .tags["tag:backup_name"], start: .startTime}'

    echo "----- content stats -----"
    kopia content stats

    echo "----- snapshot verify -----"

    kopia snapshot verify --verify-files-percent=0
    echo "===== END KOPIA DIAGNOSTICS ====="
  } 2>&1 | tee -a /tmp/kopia_diag.log >&2
  return "${rc}"
}
