# CAS Operator Runbook

This runbook covers day-to-day operation of the content-addressable backup
mode (`cas-*` commands). For the design rationale see
[docs/cas-design.md](cas-design.md). For end-user usage see the README.

## ⚠️ Binary rollback procedure (READ FIRST IF DOWNGRADING)

> **🛑 STOP. Read this section before downgrading the clickhouse-backup binary if CAS data exists in your bucket.**

Pre-CAS binaries (any release that does not include the `cas-*` commands) have **no knowledge of the `cas/` skip prefix**. When such a binary runs `clean remote_broken` — or when the scheduled `BackupsToKeepRemote` retention logic fires — it sees `cas/<cluster>/…` as a malformed v1 backup tree and **deletes the entire CAS namespace**, including all blob data and metadata. This is irrecoverable without an independent copy.

**You have three safe options. Choose one before downgrading:**

1. **Pin the new binary in place — do not downgrade.** The safest and simplest option. If the reason for downgrading is a bug in the new binary, fix the bug instead.

2. **Move CAS data out of the bucket first.** Using your cloud console or CLI, rename (copy + delete) the `cas/` prefix to a different name that won't be touched by v1 retention (e.g. `cas-archived/`). The old binary will not see it. Restore the rename when the binary is upgraded again.

   ```sh
   # Example with mc (MinIO Client):
   mc cp --recursive myminio/mybucket/cas/ myminio/mybucket/cas-archived/
   mc rm --recursive --force myminio/mybucket/cas/

   # Example with AWS CLI:
   aws s3 cp s3://mybucket/cas/ s3://mybucket/cas-archived/ --recursive
   aws s3 rm s3://mybucket/cas/ --recursive
   ```

3. **Disable v1 retention/cleanup jobs before downgrading, and keep them disabled until upgraded again.**
   - Set `BackupsToKeepRemote: 0` in every config that touches this bucket.
   - Remove `clean remote_broken` from all cron entries.
   - Do **not** re-enable either until the binary is upgraded back to a CAS-aware release.
   - Document this as a temporary state so it isn't forgotten.

> **Warning:** There is no partial protection. A single `clean remote_broken` call from any pre-CAS host with access to the bucket is enough to destroy all CAS data. If you operate multiple hosts or automation pipelines, all of them must be updated or disabled before downgrading any one host.

---

## First production deployment (start here)

> ⚠️ **CAS is experimental.** The on-disk layout may change incompatibly
> before the feature is marked stable. Validate on non-critical workloads
> first and keep parallel v1 backups (or copies outside the CAS namespace)
> until you've gained confidence. See `docs/cas-design.md` for stability notes.

This section walks an operator from zero to a first scheduled prune. Each
subsection is a gate — don't advance until the current step is clean.

### 1. Validate config

Open your config file (default `/etc/clickhouse-backup/config.yml`) and
confirm the following fields are set:

| Field | Requirement |
|---|---|
| `cas.enabled` | `true` |
| `cas.cluster_id` | Non-empty, **unique per source cluster** |
| `cas.root_prefix` | Set (default `cas/`; leave unless you have a reason to change) |
| `cas.grace_blob` | `24h` default; increase if prune windows are infrequent |
| `cas.abandon_threshold` | `168h` default; lower only if you have noisy uploader crashes |

```sh
clickhouse-backup print-config 2>/dev/null | grep -A15 "^cas:"
```

### 2. First test backup (low-risk table)

Pick a small, non-critical table. Do **not** run the first CAS upload
against production-critical data until step 4 completes.

```sh
clickhouse-backup create test-cas-bk1 --tables=mydb.small_table
clickhouse-backup cas-upload test-cas-bk1
```

The upload summary reports bytes uploaded vs. reused. On a fresh cluster
expect 100 % uploaded / 0 % reused. Dedup gains appear from backup 2 onward.

### 3. Validate via cas-verify

```sh
clickhouse-backup cas-verify test-cas-bk1
```

Zero failures is the bar. If `missing` or `size_mismatch` failures appear,
see [Recovering from cas-verify failures](#recovering-from-cas-verify-failures)
below.

### 4. Round-trip restore + count check

Drop the test table, restore from CAS, and confirm row counts match the
pre-backup baseline:

```sh
clickhouse-client -q "SELECT count() FROM mydb.small_table"   # record N
clickhouse-backup cas-restore test-cas-bk1 --rm
clickhouse-client -q "SELECT count() FROM mydb.small_table"   # must equal N
```

A mismatched count indicates a data or config problem; investigate before
proceeding to production backups.

### 5. Set up scheduled prune

`cas-prune` is the garbage collector; run it regularly (weekly is a safe
default, daily for high-churn deployments). Schedule it in a quiet window
when no concurrent uploads are expected. For the prune's behavior and flags
see [When to run cas-prune](#when-to-run-cas-prune) below.

```cron
# Example: daily at 03:00 UTC
0 3 * * * /usr/bin/clickhouse-backup cas-prune
```

If cron timing cannot guarantee no overlap with scheduled uploads, set
`cas.wait_for_prune` so uploads poll and retry instead of failing immediately:

```yaml
cas:
  wait_for_prune: "10m"
```

### 6. Monitoring

`cas-status` is LIST-only (never writes) and cheap to run frequently. Pipe
its output into your log pipeline and alert on:

- Prune marker present for more than 2× expected prune duration → stranded
  marker.
- Abandoned in-progress markers accumulating → failed uploads or dying hosts.
- Total blob bytes growing linearly despite stable backup count → `cas-prune`
  is not running.

See [Monitoring suggestions](#monitoring-suggestions) below for the full
alert catalogue.

### 7. Recovery procedures

For step-by-step recovery instructions see the dedicated sections below:

- Stranded prune marker → [Recovering from a stranded cas/\<cluster\>/prune.marker](#recovering-from-a-stranded-casclusterprunemarker)
- Stranded upload marker → [Recovering from a stranded inprogress marker](#recovering-from-a-stranded-inprogress-marker)
- `cas-upload` refusal due to concurrent marker → [Recovering from a concurrent cas-upload refusal](#recovering-from-a-concurrent-cas-upload-refusal)
- Corrupt backup found by `cas-verify` → [Recovering from cas-verify failures](#recovering-from-cas-verify-failures)

### 8. REST API

In daemon mode all CAS commands are available via HTTP at the same port as
the v1 API. See [REST API endpoints](#rest-api-endpoints) below for the full
route table, async polling pattern, and example `curl` calls.

---

## Known limitations (v1)

The `cas-*` commands ship as **experimental** in v1. Things v1 explicitly does
not do; expect them to land in later releases:

- **`--tables` patterns are glob-only, not regex.** `--tables=db.*` or
  `--tables=db.tab[12]` work (filepath.Match semantics, parity with v1).
  Regex-style filters (`^db\..*_temp$`) do not.
- **Object-disk tables are refused.** Tables on disks of type `s3`,
  `s3_plain`, `azure_blob_storage`, `azure`, `hdfs`, `web`, or `encrypted`
  layered on any of those are blocked by the `cas-upload` preflight. Use
  `--skip-object-disks` to exclude or v1 `upload` for those tables. Lifted
  in a future release once content-addressing of already-remote object
  stubs is designed.
- **Multi-host concurrent upload to the same backup name is unsupported.**
  Two hosts running `cas-upload mybackup` simultaneously can race past the
  same-name check and last-writer wins on `metadata.json`. Use unique names
  per writer (e.g. `<cluster>__<shard>__<timestamp>`).
- **Hash verification on download is HEAD + size only.** `cas-verify` and
  `cas-download` confirm each blob's *size* against the value in
  `checksums.txt`; they do NOT re-hash blob bytes. Silent corruption from a
  buggy GC is caught; an attacker who replaces a blob with same-sized
  garbage at the same key is not (CityHash128 is non-cryptographic; the
  threat model assumes a trusted bucket).
- **No per-blob resumable uploads.** Existing `pkg/resumable` operates at
  per-archive granularity; CAS uploads at blob granularity have no resume
  protocol yet. A killed `cas-upload` re-uploads everything that wasn't
  already in the blob store on the next attempt (cold-list dedup limits
  the cost).
- **FTP is best-effort.** With `cas.allow_unsafe_markers=true` FTP markers
  use a STAT+STOR+RNFR/RNTO sequence with a small race window. Without the
  flag, CAS refuses on FTP. SFTP, S3, GCS, Azure, COS all have native
  atomic primitives.
- **Old MinIO is rejected.** The conditional-put startup probe refuses
  MinIO releases pre-`RELEASE.2024-11-07T00-52-20Z` because they silently
  ignore `If-None-Match: "*"`. Update MinIO, switch to a different
  backend, or set `cas.skip_conditional_put_probe=true` after independent
  validation of the precondition.
- **Cross-cluster blob sharing is not supported.** Each cluster has its
  own namespace under `cas.root_prefix + cas.cluster_id + "/"`. Two
  clusters writing to the same bucket cannot dedup against each other.

A consolidated v2 backlog with rationale lives in `docs/cas-design.md` §9.

### Changing `cas.root_prefix`

> **Warning:** Changing `cas.root_prefix` while CAS data exists at the old prefix (e.g. renaming `"cas/"` to `"snapshots/"`) silently exposes the old data to v1 retention and `clean remote_broken`. The old binary — and even the new binary running with the updated config — no longer skips `cas/` because the configured skip prefix has changed to `snapshots/`. Any scheduled `BackupsToKeepRemote` or `clean remote_broken` job that runs during or after the config flip will see the old `cas/` subtree as broken v1 backups and delete it.

To migrate safely, do one of the following **before** flipping the config:

- **Copy/move the old prefix to the new one first**, then update `cas.root_prefix`:
  ```sh
  # Move cas/ → snapshots/ before changing any config file.
  mc cp --recursive myminio/mybucket/cas/ myminio/mybucket/snapshots/
  mc rm --recursive --force myminio/mybucket/cas/
  # Only now update cas.root_prefix: "snapshots/"
  ```
- **Disable v1 retention and `clean remote_broken` for the duration of the transition**, perform the copy/move, update the config, verify with `cas-status`, then re-enable retention.

---

## When to run `cas-prune`

`cas-prune` is the garbage collector. After every `cas-delete` (and after
crashed `cas-upload` runs), orphan blobs accumulate in remote storage; they
are reclaimed only by `cas-prune`.

- **Cadence:** weekly is a safe default; daily for high-churn deployments
  (lots of mutations + frequent `cas-delete`).
- **Quiet window:** while `cas-prune` runs it holds an advisory marker
  (`cas/<cluster>/prune.marker`) that causes concurrent `cas-upload` and
  `cas-delete` to refuse. Run during a window when no scheduled backups
  start or expire. The integration test on a 3-backup workload completes
  in well under a minute; a real 100-backup catalog typically runs in a
  few minutes plus the LIST round-trips.
- **Concurrency:** only one host at a time. `cas-prune` does not implement
  distributed locking; two hosts that race the marker race-write will
  abort one of them via run-id read-back, but operators must serialize
  manually across replicas (no overlapping cron entries).

```sh
clickhouse-backup cas-prune                          # use configured grace/abandon
clickhouse-backup cas-prune --dry-run                # preview candidates, no writes
clickhouse-backup cas-prune --grace-blob=1h          # tighter grace for cleanup runs
clickhouse-backup cas-prune --grace-blob=0s          # zero grace (immediate reclaim)
```

## Reading `cas-status`

`cas-status` is a LIST-only health summary; safe to run at any time
(it never writes). Sample output:

```
CAS status (cluster=prod-1):
  Backups: 12 (newest: bk_2026_05_06, oldest: bk_2026_05_01)
  Blobs:   42,318 objects, 5.2 TiB

  Prune marker: NONE
  In-progress markers: 1 fresh, 0 abandoned
    fresh: bk_pending (5m ago)
```

Field meanings:
- **Backups**: count of `cas/<cluster>/metadata/<name>/metadata.json` entries.
- **Blobs**: count + total bytes under `cas/<cluster>/blob/`.
- **Prune marker**: shows `NONE`, or `<host> (run_id=..., age=Xm)` if held.
  An age much larger than your typical prune duration suggests a stranded
  marker; see "Recovering from a stranded prune marker" below.
- **In-progress markers**: counts and lists per-backup upload markers.
  - `fresh`: younger than `cas.abandon_threshold`. Treat as a real upload
    in flight; don't act on it until it ages out.
  - `abandoned`: older than `cas.abandon_threshold`. Reclaimed automatically
    by the next `cas-prune`. You can also delete the marker manually if
    the upload host is confirmed dead.

## Recovering from a stranded `cas/<cluster>/prune.marker`

A stranded marker happens when `cas-prune` is killed by SIGKILL or OOM-kill
before its deferred release fires. Symptoms:

- `cas-status` shows `Prune marker: <host> (age=2h+)` long after the
  expected prune duration.
- `cas-upload` and `cas-delete` refuse with `cas: prune in progress`.

Recovery:

1. **Verify no prune is actually running.** Check `ps`/`systemctl` on the
   host listed in the marker. If something IS running, do not interrupt it.
2. If confirmed dead, clear the marker:

   ```sh
   clickhouse-backup cas-prune --unlock
   ```

   `--unlock` deletes the marker and exits. It refuses if no marker is
   present (safety).

3. Re-run `cas-prune` normally to reclaim any orphans the killed run
   would have caught.

## Recovering from a stranded inprogress marker

A stranded `cas/<cluster>/inprogress/<name>.marker` (without the matching
`metadata.json`) happens when `cas-upload` crashes mid-run. The next
`cas-prune` reclaims any markers older than `cas.abandon_threshold`
(default 168h = 7 days). To accelerate:

```sh
# Override threshold for this run only:
clickhouse-backup cas-prune --abandon-threshold=24h
```

Or, if you're confident the upload is dead and don't want to wait:

```sh
# Manual marker delete (operator authority required to reach the bucket):
mc rm <minio-alias>/<bucket>/<root_prefix><cluster>/inprogress/<name>.marker
# or via gsutil/aws s3 rm for the corresponding backend.
```

## Recovering from a concurrent cas-upload refusal

If `cas-upload` is killed (SIGKILL, OOM-kill, host crash) before its
deferred cleanup fires, the `cas/<cluster>/inprogress/<name>.marker`
remains in remote storage. The next `cas-upload` for the same backup
name refuses with:

    cas: another cas-upload is in progress for "<name>" on host=<host>
    started=<rfc3339>; wait for it to finish or run cas-prune
    --abandon-threshold=0s if confirmed dead

Recovery:

1. **Verify nothing is actually running.** Check `ps`/`systemctl` on the
   host listed in the error message. If something IS running, do not
   interrupt it.

2. If confirmed dead, sweep the marker:

   ```sh
   clickhouse-backup cas-prune --abandon-threshold=0s
   ```

   This treats every inprogress marker as abandoned regardless of age and
   reclaims it. Then retry `cas-upload`.

## Backend support for atomic markers

`cas-upload` and `cas-prune` rely on atomic create-only-if-absent writes
to their respective markers. Backend support:

| Backend | Atomic markers | Notes |
|---|---|---|
| s3 | yes | Requires MinIO ≥ RELEASE.2024-11 or AWS S3 (always supported) |
| azblob | yes | Native If-None-Match |
| gcs | yes | Native generation-match |
| cos | yes | Native If-None-Match |
| sftp | yes | Server-side via SSH_FXF_EXCL |
| ftp | NO by default | Set `cas.allow_unsafe_markers: true` to enable best-effort with documented race window |

If your backend is FTP and you have not set `cas.allow_unsafe_markers`,
`cas-upload` and `cas-prune` will refuse with an `ErrConditionalPutNotSupported`-derived
message at marker-write time.

### CI smoke-test coverage

The atomic-marker primitive is exercised end-to-end against a real-or-
emulator server in CI for these backends:

| Backend | Integration test | Emulator |
|---|---|---|
| s3 | `TestCAS*` (11 tests covering upload, restore, prune, projections, empty tables, concurrency) | MinIO `latest` |
| gcs | `TestCASSmokeGCS` (full upload → restore → delete → prune cycle) | fake-gcs-server `latest` |
| azblob | `TestCASSmokeAzure` (same cycle) | Azurite `latest` |
| sftp | `TestCASSmokeSFTP` (same cycle) | OpenSSH-server (panubo/sshd `latest`) |
| ftp | `TestCASSmokeFTPRefusesByDefault` + `TestCASSmokeFTPOptIn` | proftpd `latest` |
| cos | none — no Tencent COS emulator available | rely on SDK correctness; report regressions to maintainers |

S3 has the most thorough coverage (11 tests covering concurrency,
partial restore, projections, etc.). The other backends have a single
smoke test each that proves the core upload/restore path works through
that backend's atomic-marker primitive. The smoke tests catch SDK-level
wiring bugs (e.g., the `casstorage` adapter calling the wrong method
that Phase 4 T12 caught) but do not cover concurrency edge cases on
non-S3 backends; if a real-world race is suspected on Azure / GCS / SFTP /
FTP, request a follow-up.

## Recovering from `cas-verify` failures

`cas-verify` reports three failure kinds:

- **`missing`** — the blob isn't in remote storage. Either truly lost or
  reclaimed by an over-eager `cas-prune` (rare; would indicate a bug).
- **`size_mismatch`** — the blob exists but its size differs from what
  `checksums.txt` recorded. Truncated upload or external mutation.
- **`stat_error`** — transient backend error during the HEAD probe. Re-run
  `cas-verify` before assuming the blob is bad.

For `missing`/`size_mismatch`, the affected backup is unrestorable. Phase 1
has no automated repair (`cas-fsck` is a Phase-3 candidate). Workflow:

```sh
clickhouse-backup cas-delete <broken_backup>          # remove broken metadata
clickhouse-backup create     <new_name>               # fresh local snapshot
clickhouse-backup cas-upload <new_name>               # re-upload
```

CAS backups are independent: losing one doesn't affect any other.

## Backend assumptions

`cas-prune` and `cas-status` assume the configured object store provides:

1. **Read-your-writes consistency for individual objects.** Get/Stat
   immediately after Put returns the new content. Standard on AWS S3,
   GCS, Azure Blob, MinIO ≥ 2020.
2. **Meaningful `LastModified`** that reflects the actual write time
   (not a quirky monotonic clock or a clamped fixed value). The grace
   window is enforced via this field.

On-prem MinIO sandboxes occasionally have skewed clocks; if `cas-status`
reports an "abandoned" marker that's actually fresh, check NTP sync on
the MinIO host first.

## Monitoring suggestions

Alerts to consider:

- **Prune marker stuck**: `cas-status` reports a `prune marker` older
  than your typical prune duration (e.g., > 30 min for typical
  catalogs). Likely a stranded marker — page on-call.
- **Abandoned-marker accumulation**: more than N abandoned in-progress
  markers indicates either a buggy uploader or a dying host. N=3
  triggers a warning; N=10 a page.
- **CAS bucket growth**: track total blob bytes over time. After the
  first warm-up week the curve should asymptote. Continued linear
  growth despite stable backup count suggests `cas-prune` is not
  running (is it scheduled?).

A simple cron entry to dump `cas-status` to a log every 15 minutes makes
all of the above trivially monitorable via your existing log pipeline.

## REST API endpoints

In daemon mode (`clickhouse-backup server`), the CAS commands are available
via HTTP on the same port as the v1 API endpoints (default `:7171`):

| Method | Path | Maps to CLI |
|--------|------|-------------|
| POST | `/backup/cas-upload/{name}` | `cas-upload` |
| POST | `/backup/cas-download/{name}` | `cas-download` |
| POST | `/backup/cas-restore/{name}` | `cas-restore` |
| POST | `/backup/cas-delete/{name}` | `cas-delete` |
| POST | `/backup/cas-verify/{name}` | `cas-verify` |
| POST | `/backup/cas-prune` | `cas-prune` |
| GET  | `/backup/cas-status` | `cas-status` |

Async commands (`cas-upload`, `cas-download`, `cas-restore`, `cas-verify`,
`cas-prune`) return an `acknowledged` JSON envelope with an `operation_id`;
poll `GET /backup/status?operationid=<id>` for completion. `cas-delete` and
`cas-status` are synchronous and return the result directly.

CLI flags map to query parameters of the same name, e.g.:

```sh
# async upload
curl -XPOST 'http://localhost:7171/backup/cas-upload/my_backup?skip-object-disks&wait-for-prune=5m'

# async restore with drop-and-recreate
curl -XPOST 'http://localhost:7171/backup/cas-restore/my_backup?rm'

# async prune — dry run
curl -XPOST 'http://localhost:7171/backup/cas-prune?dry-run'

# sync delete
curl -XPOST 'http://localhost:7171/backup/cas-delete/my_backup'

# poll completion
curl -s 'http://localhost:7171/backup/status?operationid=<id>' | jq .
```

`GET /backup/list[/remote]` now includes CAS backups alongside v1 entries.
Each entry carries a `"kind"` field (`"v1"` or `"cas"`), and CAS entries
include a `"cas"` sub-object with `unique_blobs`, `blob_bytes`, and
`cluster_id`.

`POST /backup/actions` recognizes the same `cas-*` verbs in the command
body, e.g. `{"command": "cas-upload mybk --skip-object-disks"}`.

The `cas-prune --unlock` flag is also available via `?unlock=true`. It
overrides a stranded prune marker; use with the same operator confidence
required when running the CLI form.
