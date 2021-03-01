# v0.6.5

IMPROVEMENTS
- add Atomic Database Engine support
- add Multi Volumes support
- improve prometheus metrics

# v0.6.4

IMPROVEMENTS

- Added CLICKHOUSE_AUTO_CLEAN_SHADOW option for cleaning shadow folder before backup. Enabled by default.
- Added CLICKHOUSE_SYNC_REPLICATED_TABLES option for sync replicated tables before backup. Enabled by default.
- Improved statuses of operations in server mode

BUG FIXES

- Fixed bug with semaphores in server mode
