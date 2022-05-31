# ClickHouse Backup Test Suite

This folder contains TestFlows tests for ClickHouse Backup Utility. This file describes how to launch those tests.

## Requirements

To execute tests, you will need:

* Python 3.8 or higher (`python3`)
* The latest version of Go (`https://go.dev/doc/install`)
* TestFlows Python library (`https://testflows.com/`)
* `docker` and `docker-compose`

To install all necessary Python packages, execute:
```bash
pip3 install -r ./test/testflows/requirements.txt
```

## Execution
Make clickhouse-backup binary
```bash
make build-race
```

Some environment variables required to be set up before test execution:
* `export CLICKHOUSE_TESTS_DIR=/home/username/clickhouse-backup/test/testflows/clickhouse_backup`
  - (this variable must point to the folder containing `regression.py`)
* In order to test cloud platforms (AWS S3 and GCS), you will need the following variables to contain valid credentials (otherwise, the corresponding tests will fail):
  - `QA_AWS_ACCESS_KEY`
  - `QA_AWS_ENDPOINT`
  - `QA_AWS_SECRET_KEY`
  - `QA_AWS_REGION`
  - `QA_AWS_BUCKET`
  - `QA_GCS_CRED_JSON`

You can do it with something like that:

```bash
cat > /home/username/clickhouse-backup/test/testflows/.env <<EOT
export CLICKHOUSE_TESTS_DIR=/home/username/clickhouse-backup/test/testflows/clickhouse_backup
export QA_AWS_ACCESS_KEY=XXXXXX
export QA_AWS_ENDPOINT=XXXXXX
export QA_AWS_SECRET_KEY=XXXXXX
export QA_AWS_REGION=XXXXXX
export QA_AWS_BUCKET=XXXXXX
export QA_GCS_CRED_JSON=XXXXXX
EOT
source /home/username/clickhouse-backup/test/testflows/.env
```


### To execute the test suite, execute the following commands:

```bash
python3 ./test/testflows/clickhouse_backup/regression.py
```

If you need only one certain test, you may execute

```bash
python3 ./test/testflows/clickhouse_backup/regression.py --only "/clickhouse backup/path to test/"
```
