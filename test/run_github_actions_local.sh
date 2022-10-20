# require curl -sSLf https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash -x
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
mkdir -p /tmp/act/artifacts
source "${CUR_DIR}/testflows/.env"
act "pull_request" -r -b -C "${CUR_DIR}/../" \
  --artifact-server-path /tmp/act/artifacts \
  -s VAULT_PASSWORD="${VAULT_PASSWORD}" \
  -s QA_AWS_ACCESS_KEY="${QA_AWS_ACCESS_KEY}" \
  -s QA_AWS_ENDPOINT="${QA_AWS_ENDPOINT}" \
  -s QA_AWS_SECRET_KEY="${QA_AWS_SECRET_KEY}" \
  -s QA_AWS_REGION="${QA_AWS_REGION}" \
  -s QA_AWS_BUCKET="${QA_AWS_BUCKET}" \
  -s QA_GCS_CRED_JSON="${QA_GCS_CRED_JSON}"
