# CAS Operator Runbook

This runbook covers day-to-day operation of the content-addressable backup
mode (`cas-*` commands). For the design rationale see
[docs/cas-design.md](cas-design.md). For end-user usage see the README.

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
