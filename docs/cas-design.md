# Content-Addressable Storage (CAS) Layout for clickhouse-backup

> ⚠️ **EXPERIMENTAL.** CAS commands and the on-disk layout are still under active development. The `LayoutVersion` may change in a way that requires re-uploading existing CAS backups before adopting a newer release. Do not treat CAS as the sole copy of production data yet — keep a parallel v1 backup (or a copy outside the CAS namespace) until the feature is marked stable. Operators are encouraged to evaluate it on non-critical workloads, report issues, and watch the changelog for compatibility notes.

**Status**: Phases 1–8 shipped on branch `cas-phase1`. Commands implemented: `cas-{upload,download,restore,delete,verify,status,prune}` available both via CLI and REST API in daemon mode. Phase 3 added projection-aware planner + cross-mode guards; Phase 4 added atomic markers (S3 IfNoneMatch, SFTP O_EXCL, native conditional create on Azure/GCS/COS, refuse-by-default on FTP); Phase 5 added per-backend integration smoke tests across MinIO/Azurite/fake-gcs/SFTP; Phase 6 closed the P1 defects from the second external review wave; Phase 7 (cleanup round) closed the ColdList TOCTOU window, populated `PruneReport` counters, added defensive `cfg.Validate()` at Prune entry, and added focused per-backend not-found tests; Phase 8 added `wait_for_prune` (poll-and-wait for CAS upload/delete when prune is in flight) and wired all CAS commands through the REST API (dedicated routes + `/backup/actions` verbs + list-merge with `kind` field).
**Author**: Mikhail Filimonov, drafted with design-interview support
**Last updated**: 2026-05-08

## 1. Summary

A new set of commands — `cas-upload`, `cas-download`, `cas-restore`, `cas-delete`, `cas-prune`, `cas-verify` — that store backups to remote object storage in a **content-addressable layout**. Files are keyed by their content hash (CityHash128, sourced from ClickHouse's per-part `checksums.txt`); identical content is stored once and referenced from any number of backups. Garbage collection is a separate, eventual mark-and-sweep step.

The new commands run side-by-side with the existing `upload` / `download` / `delete` commands and use a separate top-level prefix in the target bucket, so v1 and CAS backups never share namespace.

### 1.1 When to use CAS vs. v1

Pick CAS when:
- Tables are mutation-heavy (CAS deduplicates the unchanged columns of mutated parts; v1 re-uploads).
- You want every backup to be independently restorable (CAS has no incremental chain; removing one backup never affects another).
- You expect many backups over time and want storage to grow with *new* data, not with the number of backups.

Stick with v1 when:
- Tables include **object-disk** disks (s3/azure/hdfs object disks) — CAS does not support these in v1.
- You currently use **client-side encryption** — CAS v1 supports bucket-level encryption only (see §3). Operators using v1's client-side encryption cannot move to CAS until convergent encryption ships in a later version.
- You're already happy with v1's incremental chain and don't want to change.
- You need a feature CAS hasn't implemented yet (see §3 non-goals).

CAS backups and v1 backups can coexist in the same bucket under different prefixes; they never see each other's data. There is no migration tool — opt in by writing new backups with `cas-upload`.

### 1.2 Mental model

CAS backups are **independent**. There is no `RequiredBackup` chain. Removing backup A never affects backup B's restorability. A blob remains in the store as long as any backup references it (refcounting is implicit via mark-and-sweep). This is the most important difference from v1; surface it everywhere user-facing (README, `--help` text).

## 2. Goals

- **Deduplicate across mutations**: ClickHouse mutations create new part names whose underlying files are mostly identical (often hardlinked) to the source part. Today's tool re-uploads them. CAS reuses the existing blob on the target.
- **Eliminate the incremental chain dependency**: every CAS backup is independently restorable. No `RequiredBackup` pointer; no chain unrolling.
- **Reduce full-backup wall-clock and cost** by uploading only blobs not already in the target.
- **Reuse existing infrastructure**: the storage abstraction (`BackupDestination`), table walker, multipart upload, retry, throttling, `BackupMetadata` / `TableMetadata` structs are reused as-is.
- **Don't break v1**: separate commands, separate prefix, no behavioral changes to existing code paths.
- **Preserve the relevant restore CLI surface**: `--tables`, `--partitions`, `--schema-only`, `--data-only`, `--restore-database-mapping`, `--restore-table-mapping`, `--rm`, `--restore-schema-as-attach` all work unchanged for CAS backups. `--ignore-dependencies` is rejected with an error (CAS backups have no chain — see §6.10).

## 3. Non-goals (v1 of CAS)

- Distributed locking. Operators serialize commands externally, matching today's PID-file model.
- Hash verification on download (full content re-hash). Deferred to v2 of CAS. **Size verification on download is in v1**; see §6.8.
- Refcount-delta files / incremental garbage collection. Deferred; v1 uses full mark-and-sweep, which is sufficient at the target scale.
- Convergent encryption. v1 of CAS uses bucket-level encryption only. **TODO (v2)**: design and ship convergent encryption so existing v1 client-side-encryption users can migrate to CAS without losing client-side encryption. Known weaknesses (confirmation-of-file attacks) need threat-modeling per the deployment context.
- Migration of existing v1 backups into CAS layout. Out of scope; users opt in by writing new backups with `cas-upload`.
- Garbage collection of metadata across replicas/clusters beyond what mark-and-sweep already handles.
- Object Lock / immutability features beyond what's intrinsic to content addressing.
- **Object-disk parts (s3/azure/hdfs object disks) are NOT supported by `cas-*` commands in v1.** The existing `pkg/backup/create.go:1031` and `pkg/backup/restore.go:2227` paths (~1000 LOC of dual-direction object-reference handling, including cross-storage key rewriting) are non-trivial to fold into CAS, and content-addressing semantics for already-remote object stubs need their own design pass. `cas-upload` will refuse to back up tables on object disks in v1; operators must use v1 `upload` for those tables. Lifted in v2 of CAS.

## 4. Background

Today's `clickhouse-backup` upload pipeline (see `pkg/backup/upload.go:38–295`):

- A backup is a directory `{backup_name}/` on the remote target containing a top-level `metadata.json` (`BackupMetadata` struct, `pkg/metadata/backup_metadata.go:12`), per-table JSONs at `metadata/{db}/{table}.json` (`TableMetadata`, `pkg/metadata/table_metadata.go:10`), and per-part archives (compressed tar streams) under each table.
- Dedup happens **only against a base backup** (`RequiredBackup` field), at part-name granularity, via the `Required=true` flag set in `TableMetadata.Parts` (`pkg/backup/upload.go:756–784`).
- Concurrency is bounded by a per-backup PID file (`pkg/pidlock/pidlock.go:15`); there are no S3 conditional writes or distributed locks.
- ClickHouse's `checksums.txt` files are **not parsed** by the tool today. Hashes are computed by `pkg/common/common.go:131` (CRC64 of whole files/archives).

## 5. Problems being solved

1. **Mutation explosion**: a mutation can rewrite one column and rename the part, while hardlinking ~all other column files. Today this triggers a full re-upload of the new part. CAS reuses the existing blobs.
2. **Incremental chain fragility**: incremental backups depend on a base backup. Restore unrolls the chain. With CAS, every backup is independent.
3. **Time/cost of full backups**: with CAS the second backup of a mostly-unchanged dataset uploads only the diff in blobs.
4. **Reuse**: as much existing code as possible — orchestration, retry, multipart, encryption, metadata structs.
5. **Containment**: separate commands (`cas-*`) so the new path can't regress v1.

## 6. Proposed design

### 6.1 Object layout

A CAS deployment uses a single configurable root prefix (default: `cas/`) under the existing remote target. Inside that root:

```
cas/
  blob/<aa>/<rest_of_hash>                       # immutable, content-addressed; <aa> = first 2 hex
                                                 # chars of CityHash128 hex; <rest> = remaining 30 chars
  metadata/<backup>/                             # per-backup directory
    metadata.json                                # BackupMetadata (reused struct; RequiredBackup
                                                 #   unused; new CAS sub-struct populated — see §6.2.1)
    metadata/<enc_db>/<enc_table>.json           # TableMetadata (reused struct). enc_* = TablePathEncode
    parts/<disk>/<enc_db>/<enc_table>.tar.zstd    # per-disk per-(db,table) archive of small files only.
                                                 #   contents: <part_name>/<filename> for every part.
                                                 #   Path components encoded with common.TablePathEncode
                                                 #   (pkg/common/common.go:18) to handle non-ASCII /
                                                 #   special chars and avoid db/table name collisions.
    rbac/, configs/, named_collections/          # unchanged from existing layout
  inprogress/<backup>.marker                     # timestamp + host; written at upload start, deleted
                                                 #   at commit. Used by `cas-prune` for abandoned-upload
                                                 #   cleanup; also blocks `cas-delete` of same name
  prune.marker                                   # the GC lock; written when `cas-prune` runs.
                                                 #   While present, `cas-upload` and `cas-delete` refuse
```

**Sharding**: 256-way prefix sharding by first 2 hex chars of the hash. Sufficient for S3 per-prefix rate limits at the target scale; intuitive layout for human inspection.

**Hash source**: each part's `checksums.txt`, parsed per the on-disk format (versions 2/3/4 supported; spec in `docs/checksumstxt/format.md`, reference parser in `docs/checksumstxt/checksumstxt.go`). The `Checksum.FileHash` field (CityHash128 of the file's on-disk bytes — not `UncompressedHash`) is what CAS keys blobs by. This gives byte-identity dedup, which is exactly what we want: identical column files produced by hardlink-mutations dedup; logically-equal files with different on-disk encodings (e.g., recompressed) do not, and that's correct because their stored bytes differ. 128 bits gives negligible accidental-collision probability across ~10⁹ blobs; reusing ClickHouse's already-computed hash avoids ~50+ CPU-hours of re-hashing per 100TB backup.

**Path layout decisions**:
- No `by-extension` dimension. Same content under different filenames must dedupe.
- No `first/last` double-byte split. Single 2-byte prefix is sufficient (see §10.1 for rate-limit math).

### 6.2 Inline-vs-blob threshold

Files with `size > inline_threshold` go to the blob store. Files with `size ≤ inline_threshold` are packed into the per-disk-per-(db,table) archive. Default: **256 KiB**, configurable.

Rationale: ClickHouse parts contain many small metadata files (`columns.txt`, `primary.idx`, `partition.dat`, `minmax_*.idx`, `count.txt`, `default_compression_codec.txt`, `serialization.json`, `checksums.txt` itself). Per-PUT cost on S3 ≈ $0.005/1K. At 50 small files × 10⁴ parts × 1 backup = 500K extra PUTs ≈ $2.50/backup just in API charges, with worse tail-latency. Packing them into a per-table tar.zstd is dramatically more efficient.

The threshold should be tuned against an actual file-size distribution from a representative ClickHouse instance before final commit; 256 KiB is the starting point.

### 6.2.1 CAS layout parameters MUST be persisted with the backup

Restore behavior depends on parameters chosen at upload time. If a backup is uploaded with `inline_threshold = 256 KiB` and later restored after the operator has reconfigured the tool to `inline_threshold = 1 MB`, restore would look in the inline archive for files that were actually stored as blobs — silent corruption.

Persist the following per-backup, embedded in `BackupMetadata` as a new `CAS *CASBackupParams` field (`omitempty`, populated only by `cas-upload`):

```go
type CASBackupParams struct {
    LayoutVersion   uint8   // schema version of the CAS layout itself; v1 = 1
    InlineThreshold uint64  // bytes; ValidateBackup MUST reject if 0 or > 1 GiB
    ClusterID       string  // required (§6.11); identifies the source cluster for namespace isolation
}
```

**LayoutVersion evolution policy** (decided): a tool encountering `LayoutVersion > supported_max` refuses with a clear error. Operators MUST keep the oldest tool capable of reading their oldest backup. LayoutVersion bumps are major-version BREAKING CHANGE entries.

**No `HashAlgorithm` field.** The hash is sourced from each part's `checksums.txt` — its value, encoding, and meaning are part-local properties defined by ClickHouse's on-disk format (always CityHash128 for `Checksum.FileHash` across format versions 2/3/4; see `docs/checksumstxt/format.md`). If ClickHouse ever changed the hash, the change would be visible per-part in the format version of `checksums.txt`, not as a CAS-wide policy. CAS does not pick a hash; it adopts whatever the part wrote.

**No `RootPrefix`, `BlobShardWidth`, or `ArchiveCodec` fields.** A field whose only purpose is "documentation" is rot-by-construction (you can't read it without already knowing where to look). Hardcode v1 to: root prefix configurable via config but not persisted, shard width = 2, archive codec = `zstd` with `.tar.zstd` extension matching `pkg/config/config.go:310`. Any change to these is a `LayoutVersion` bump.

Restore reads `BackupMetadata.CAS` and uses those values exclusively. CLI / config values for these parameters apply only at upload time. Restore ignores them.

If `BackupMetadata.CAS == nil`, the backup is a v1 backup; CAS commands refuse to operate on it (and v1 commands refuse to operate on a backup with `CAS != nil`). See §6.2.2 for exact code locations of the cross-mode guards.

If `BackupMetadata.CAS.LayoutVersion` is unrecognized (newer than the running tool supports), CAS commands refuse with a clear error.

### 6.2.2 Isolation from v1

CAS and v1 must not see each other's data. Two requirements, both Phase-1 ship-gates:

**1. v1 commands MUST exclude the configured CAS root prefix from listing and retention.** v1's `BackupList` (`pkg/storage/general.go`) walks the bucket root; `RemoveOldBackupsRemote` and `CleanRemoteBroken` then operate on those entries. Without an explicit exclusion, the CAS root would appear as a broken v1 backup and could be reclaimed. Modify `BackupList` to accept a skip-prefixes set populated from `cas.root_prefix`; the consequence flows to `RemoveBackupRemote`, `RemoveOldBackupsRemote`, and `CleanRemoteBroken`.

**2. v1 and CAS commands MUST refuse on the wrong type.** v1 commands (`Download`, `Restore`, `RestoreFromRemote`, `RemoveBackupRemote`, watch mode — anywhere remote `BackupMetadata` is loaded) refuse with a clear error if `BackupMetadata.CAS != nil`. CAS commands refuse with the inverse check via `ValidateBackup` (§7).

Test: `TestCompatibilityMixedBucket` (mixed bucket; v1 retention/list/clean don't touch CAS objects regardless of config) plus `TestV1RefusesCASBackup` / `TestCASRefusesV1Backup` per entry point.

### 6.3 Metadata archive packing

One **tar.zstd per disk per (db, table)**. Path inside the archive: `<part_name>/<filename>`. Contains every file of every part where `size ≤ inline_threshold`. Extension `.tar.zstd` matches `pkg/config/config.go:310`'s convention so existing readers can be reused.

**`checksums.txt` is always inlined**, regardless of size. It is treated as a special case (not as a parsed-checksum entry) because:
- Restore needs `checksums.txt` on disk *before* it can decide which blobs to fetch (§6.5 step 6 reads the local file).
- It's tiny in practice (KB-range).
- Putting it in the blob store would chicken-and-egg the restore protocol.

**Files on disk but not listed in `checksums.txt`** (an edge case ClickHouse should not produce, but the parser may encounter from future or experimental part formats): **always inline into the per-table archive**, regardless of size. They go into the metadata archive alongside the small files; never into the blob store. This avoids any local hashing in the `cas-upload` data path and gives a single rule for the corner case. The lost-dedup cost is negligible because such files are rare by construction; the simplicity is worth it. No "skip" mode — silently skipping files corrupts backups.

Rationale:
- Matches existing per-disk per-table structure (`TableMetadata.Files: map[string][]string`, `pkg/metadata/table_metadata.go`).
- Natural partial-restore granularity: `--tables=db.t1` downloads one archive per disk.
- Reasonable file count: hundreds of tables × few disks → low thousands of archives, not 10⁴+ per-part archives.
- Small files of disparate types don't benefit from per-type clustering; the win from cross-type compression is small once the big homogeneous files are in the blob store.

### 6.4 Upload — `cas-upload`

**Pre-condition**: `cas-upload` operates on a **pre-existing local backup** produced by the existing `clickhouse-backup create` command (which freezes parts into the local backup directory). This mirrors the v1 `create` + `upload` split: separation of concerns, reuses `pkg/backup/create.go` unchanged, and lets operators inspect the local backup before pushing. `cas-upload` does NOT internally freeze — operators run `clickhouse-backup create <name>` first, then `clickhouse-backup cas-upload <name>`.

1. PID-lock as today (`pkg/pidlock`).
2. **Refuse if `cas/prune.marker` exists** (the GC lock — see §6.7). Surface the marker's age in the error. If `cas.wait_for_prune` (or `--wait-for-prune` flag) is > 0, poll the marker every 2 seconds for up to that duration before refusing.
3. **Pre-flight check for object disks**: scan in-scope tables. If any are on object disks (s3/azure/hdfs) and `--skip-object-disks` is not set, refuse with a list of `(db, table, disk)` triples. With `--skip-object-disks`, log them and exclude from the upload set.
4. **Best-effort same-name check**: refuse if `cas/metadata/<backup>/metadata.json` already exists. Best-effort only — two hosts can both pass and both PUT (last writer wins). Multi-host concurrent uploads to the same name are **unsupported** (§3); operators must use unique names per shard.
5. Write `cas/inprogress/<backup>.marker` with timestamp + host identifier (used by prune for abandoned-upload cleanup; not for race protection).
6. Walk parts. For each part, parse `checksums.txt` to obtain `(filename, size, hash)` triples. Apply the inline threshold.
7. Build the set of unique blob paths.
8. **Cold-list** `cas/blob/<aa>/` prefixes in parallel → in-memory existence set.
9. Upload missing blobs via the existing `BackupDestination` abstraction.
10. For each `(disk, db, table)`: build and upload `cas/metadata/<backup>/parts/<disk>/<enc_db>/<enc_table>.tar.zstd` (path components encoded via `common.TablePathEncode`).
11. Upload per-table JSONs at `cas/metadata/<backup>/metadata/<enc_db>/<enc_table>.json`.
12. Upload RBAC, configs, named_collections (unchanged from v1).
13. **Pre-commit safety re-checks** (closes the old-orphan-reuse and long-upload-vs-abandon-sweep races — Blockers B4, B5):
    a. HEAD `cas/prune.marker`. If present, abort: "concurrent prune detected; aborting before commit." (Single HEAD; cheap; closes the window where prune ran past our step 2 lock check.)
    b. HEAD `cas/inprogress/<backup>.marker`. If absent, abort: "our in-progress marker was swept (upload exceeded abandon_threshold); aborting." (Closes the long-upload-past-abandon-sweep race.)
14. **Commit (LAST, in this order)**:
    a. Upload `cas/metadata/<backup>/metadata.json` — populates `BackupMetadata.CAS` per §6.2.1. Until this exists, the backup is not in the catalog.
    b. Delete `cas/inprogress/<backup>.marker`. (If this fails — 5xx, OOM, Ctrl-C — the marker becomes stale; `cas-delete` is required to treat it as stale when `metadata.json` exists, see §6.6.)

The presence of `cas/metadata/<backup>/metadata.json` is the catalog truth.

### 6.5 Restore — `cas-restore`

CAS restore is implemented as **`cas-download`** (downloads + materializes a complete v1-shaped backup directory on local disk) followed by the **existing v1 restore flow** (which reads from that local directory).

The existing restore reads metadata **from disk**, not in-memory:
- Root `metadata.json` is read by `pkg/backup/restore.go:114`.
- Per-table JSONs are read from the local `metadata/` directory by `pkg/backup/restore.go:1936`.

So `cas-download` MUST write the complete v1 backup directory layout before `cas-restore` invokes the existing restore. "Synthesize in memory and call restore" is **not** sufficient; existing restore won't see synthesized structures.

#### What `cas-download` writes locally

For backup `<name>` rooted at `<local_backups_dir>/<name>/`:

```
<name>/
  metadata.json                                # full BackupMetadata (DataFormat="directory")
  metadata/<enc_db>/<enc_table>.json           # full TableMetadata per table (Parts populated,
                                               #   Files empty, schema fields preserved as in v1)
  shadow/<enc_db>/<enc_table>/<disk>/<part>/   # part directories with all files reconstructed:
                                               #   - small files extracted from per-table archive
                                               #   - large files downloaded from cas/blob/...
  rbac/, configs/, named_collections/          # downloaded as today
```

Every file the existing restore expects must exist on disk before handoff.

#### Local staging contract

- `BackupMetadata.DataFormat = "directory"` (`pkg/metadata/backup_metadata.go:30`; constant `pkg/backup/backuper.go:28` `DirectoryFormat`). Branches existing restore code into the no-archive path (`pkg/backup/download.go:615, 627, 670`).
- `TableMetadata.Parts` populated; `TableMetadata.Files` empty (only consumed when `DataFormat != "directory"`; `pkg/backup/download.go:673`).
- `TableMetadata.Checksums` is **not populated** by CAS (the per-archive CRC64 the v1 path uses is irrelevant — checksums.txt inside each part directory is the source of truth for blob content).
- Reuses `filesystemhelper.HardlinkBackupPartsToStorage` (`pkg/filesystemhelper/filesystemhelper.go:119`) for the staging-to-detached step.

#### `cas-download` steps

1. Resolve the backup name. Read `cas/metadata/<backup>/metadata.json`. **Read `BackupMetadata.CAS` to get the persisted parameters** (`LayoutVersion` and `InlineThreshold` — those are the only fields per §6.2.1); restore uses these — never values from the current config.
2. Refuse if `BackupMetadata.CAS == nil` (v1 backup) or `LayoutVersion` is unsupported.
3. Apply CLI filters (`--tables`, `--partitions`, `--schema-only`, `--data-only`, mappings, etc.) to determine the working set of `(db, table, parts)`.
4. Write the local `metadata.json` and per-table `metadata/<enc_db>/<enc_table>.json` files to disk first (the existing restore flow reads them from disk).
5. For each in-scope `(disk, db, table)`: download `parts/<disk>/<enc_db>/<enc_table>.tar.zstd`, extract into the local shadow directory at the canonical layout path. **Path containment** for every tar entry, assert `strings.HasPrefix(filepath.Clean(extractPath)+sep, filepath.Clean(rootDir)+sep)` before write; reject `..` and absolute paths. **`checksums.txt` filename validation**: when parsing, reject any filename with leading `/`, embedded `..` components, or NUL bytes; allow single `/` separators only for projection paths matching `<name>.proj/<file>`. Each part directory now contains all small files including `checksums.txt`.
6. For each part in scope: parse the local `checksums.txt`, identify files with `size > BackupMetadata.CAS.InlineThreshold` (i.e. files NOT in the archive), download each from `cas/blob/<aa>/<rest>` into the part directory. The full part directory is now reconstructed locally.

`cas-download` exits here. The local layout is exactly what `restore` consumes.

#### `cas-restore`

1. Run `cas-download` (steps above) with the same flags.
2. Invoke the existing `restore` flow on the materialized local directory. **Object-disk handling MUST be skipped**: `pkg/backup/restore.go:196-204` checks live ClickHouse disks, not metadata; CAS restore must short-circuit `downloadObjectDiskParts` when `BackupMetadata.CAS != nil`. The pre-flight in `cas-upload` ensures CAS backups never include object-disk parts, so no object-disk processing is needed at restore.

This split also matches v1's `download` + `restore` verb pair and lets operators inspect the staged directory before applying.

Per-partition restore is per-part filtering: intersect `TableMetadata.Parts` with `--partitions`, then proceed only with selected parts. The per-table archive is downloaded whole even for one partition (acceptable overhead).

`--schema-only` skips steps 4–5 entirely; very fast for CAS.

### 6.6 Delete — `cas-delete`

**Order matters.** The catalog truth is `metadata.json`.

1. **Refuse if `cas/prune.marker` exists** (the GC lock). If `cas.wait_for_prune` (or `--wait-for-prune` flag) is > 0, poll the marker every 2 seconds for up to that duration before refusing.
2. **Stale-marker-aware inprogress check**: if `cas/inprogress/<backup>.marker` exists AND `cas/metadata/<backup>/metadata.json` does NOT exist → upload in flight; refuse. If both exist → the upload committed but failed to delete its marker; treat as **stale** and proceed (log a warning). If only `metadata.json` exists → normal case; proceed.
3. Delete `cas/metadata/<backup>/metadata.json` **first**. Backup is no longer in the catalog.
4. Delete the rest of `cas/metadata/<backup>/`.
5. Orphan blobs reclaimed by the next prune run.

If interrupted between steps 3 and 4: the backup is gone from the catalog; remaining files become metadata-orphans. Prune handles them lazily.

### 6.7 Prune — `cas-prune`

Mark-and-sweep GC. **Single rule**: `cas-prune` takes an exclusive lock; while held, no `cas-upload` or `cas-delete` may run. Operators schedule pruning during a quiet window. There is no automatic protection — the operator must ensure no CAS writes are happening.

#### Algorithm

1. **Sanity check** (operator-courtesy): list `cas/inprogress/*.marker`. If any is younger than `abandon_threshold` (default 7 days), refuse with a clear error listing the markers (name, host, age) and exit. The operator either waits for the upload to finish or, if confident the upload is dead, deletes the marker manually before retrying.
2. Write `cas/prune.marker` with timestamp + host id + a random run-id. **Read it back** and compare run-id to ours; if it differs, another `cas-prune` raced us — abort with "concurrent prune detected; aborting". **Defer the marker delete to the end of the function** so step 12 always runs even if intermediate steps fail or panic. The defer-release MUST run on every exit path: success, fail-closed abort, panic, signal cancellation, error returns.
3. Record `T_0 = now()`.
4. **Abandoned-upload sweep**: any `cas/inprogress/<backup>.marker` older than `abandon_threshold` → delete it. Any blobs from the abandoned run become orphans handled by step 9.
5. List `cas/metadata/*/metadata.json` → live backup set.
6. For each live backup, walk per-table archives, extract `checksums.txt` files, accumulate referenced blob paths into a sorted on-disk file (streaming).
7. **Fail-closed**: if any live backup's per-table archives or JSONs cannot be read, abort without deleting; surface error.
8. List `cas/blob/<aa>/` in parallel; stream-compare against the referenced set to identify orphan candidates.
9. Filter deletion candidates: orphan AND `LastModified < T_0 - grace_blob` (default 24h).
10. Sweep metadata-orphan subtrees: `cas/metadata/<backup>/` with no `metadata.json` → delete.
11. Delete confirmed blob candidates.
12. Release `cas/prune.marker`. (Implemented as a deferred call from step 2 — runs unconditionally.)

**Stale-marker recovery**: defer-release covers panics, signals, and error returns. Only `kill -9` or kernel OOM-kill leaves a stranded marker. When that happens, the operator inspects and clears it explicitly:

- `cas-status` displays `cas/prune.marker` if present (timestamp, host, run-id).
- `cas-prune --unlock` deletes the marker (after operator confirms no prune is actually running). Refuses if a marker isn't present (avoid silently doing nothing).

No timeout-based auto-bypass; operator owns the call. Documented in the operator runbook.

`cas-prune --dry-run` runs steps 1, 3–10 and prints what would be deleted; does not write the lock or perform deletes.

#### Why this works

The single load-bearing rule: **don't run `cas-upload` or `cas-delete` while `cas-prune` is running.** `cas-upload` and `cas-delete` enforce this by refusing to start when `cas/prune.marker` exists.

The grace period (`grace_blob`, default 24h) is defense-in-depth against:
- The TOCTOU window between `cas-upload`'s marker check and the prune lock write (small).
- Operator misuse (running prune during uploads anyway, or ignoring marker errors).
- Object-store eventual-consistency oddities.

**This is not a distributed mutex.** Two operators racing `cas-prune` on different hosts can both pass step 1 and both PUT step 2. Operators must serialize prune across hosts the same way they serialize v1 commands today (no overlapping cron, etc.). Distributed locking is a non-goal (§3); v2 may add it via S3 conditional-create.

#### Race scenarios

| Scenario | Outcome |
|---|---|
| Operator starts `cas-upload` while prune holds the lock | Upload refuses; clear error naming the prune marker's age and host. |
| Operator starts `cas-prune` while uploads are in flight | Prune refuses (step 1) with a list of fresh inprogress markers. |
| Operator forces the issue (deletes markers manually) | Grace period limits damage to blobs younger than `grace_blob`. Beyond that, on their own. |
| Upload crashes mid-flight | Inprogress marker persists. Next prune blocks until `abandon_threshold`; then sweeps marker; orphan blobs reclaimed. |
| Two uploaders race on same blob | Idempotent (content-keyed). |
| Crashed remove between deleting metadata.json and rest of subtree | Backup gone from catalog; remaining files become metadata-orphans; step 10 sweeps. |
| Backend with weak `LastModified` semantics | Grace degrades; rely harder on operator scheduling. Document. |

### 6.8 Verify — `cas-verify`

Integrity check, ships with v1:

1. Read `cas/metadata/<backup>/metadata.json` and `metadata/<enc_db>/<enc_table>.json` for all tables.
2. Download per-table archives, extract `checksums.txt` files into memory. Build the set of `(blob_path, expected_size)` pairs.
3. **HEAD each blob in parallel**. Report:
   - Missing blobs (HEAD 404).
   - **Size mismatches**: HEAD-returned `Content-Length` vs. expected size from `checksums.txt`. Catches truncated, replaced, or partially-written blobs at zero CPU cost.
4. Exit non-zero if any failures.

`cas-verify --json` emits machine-readable output (one JSON object per failure) so operators can pipe into tooling for triage / alerts.

Does NOT verify blob *content* hashes against `checksums.txt` — that's a separate v2 mode (full re-hash on download, ~minutes-to-hours wall-clock at 100TB scale). HEAD + size verification catches the silent-corruption-from-buggy-GC class of failures, which is the most likely failure mode in v1, at near-zero cost.

#### Recovery from `cas-verify` failures

If `cas-verify` reports missing or wrong-sized blobs, the backup is unrestorable. v1 has no automated repair — `cas-delete` the broken backup and create a fresh one (`clickhouse-backup create <new_name>` + `cas-upload <new_name>`). Because every CAS backup is independent (no chain), losing one doesn't affect any other.

`cas-fsck` (v2) will automate repair when local parts are still available.

### 6.9 Multi-shard concurrent upload to a shared bucket

Supported natively, with one convention: backup names must be unique across writers. Recommended naming: `<cluster>__<shard>__<timestamp>` or similar.

Mechanics:
- Different shards write to different `cas/metadata/<backup>/` directories. No collision.
- Different shards may upload identical blobs concurrently. Idempotent (content-keyed). Worst case: a small amount of wasted bandwidth.
- Prune is single-writer (marker file); operators must ensure only one prune runs at a time across all shards.

This is a strict improvement over v1, which requires per-shard separate prefixes.

### 6.10 CLI surface

Six new top-level subcommands, plus extension of the existing `list` verb:

| Command | Purpose |
|---|---|
| `cas-upload <name> [--skip-object-disks] [--dry-run]` | Build and push a CAS backup. `--skip-object-disks` excludes object-disk tables; `--dry-run` reports what would be uploaded without writing. |
| `cas-download <name> [--tables ...] [--partitions ...]` | Materialize a CAS backup into the local shadow directory in v1 layout. **Stops there** — does not load into ClickHouse. Mirrors the existing `download` verb. **Disk-space pre-flight**: estimate bytes from per-table archive sizes + sum of blob sizes from `checksums.txt`; refuse early if local free space < estimate × 1.1. Re-running over a partial directory is safe (idempotent overwrites). |
| `cas-restore <name> [...all existing restore flags...]` | Convenience: `cas-download` followed by the existing `restore` flow. Identical flag set to `restore`. |
| `cas-delete <name>` | Delete the per-backup metadata subtree (refuses if upload or prune in flight; see §6.6). Blobs are reclaimed by the next prune run. |
| `cas-prune [--dry-run] [--grace-blob DUR] [--abandon-threshold DUR] [--unlock]` | Mark-and-sweep GC. `DUR` is a Go duration string (e.g. `24h`, `30m`, `0s`). `--grace-blob` overrides config `cas.grace_blob`; `--abandon-threshold` overrides `cas.abandon_threshold`. `--dry-run` prints candidates without deleting and never touches the prune marker (so combining `--dry-run --unlock` is a no-op rather than the destructive double-meaning the first cut shipped with — see Phase 6 A3). `--unlock` deletes a stranded `cas/prune.marker` (operator escape hatch when prune was killed by SIGKILL/OOM). Explicit `--grace-blob=0s` / `--abandon-threshold=0s` are honored as "no grace" / "sweep all stale markers now" — distinguished from unset via `*Set` bools in `PruneOptions`. |
| `cas-verify <name> [--json]` | HEAD + size check on referenced blobs. `--json` outputs structured failures for tooling. |
| `cas-status` | Bucket-level health summary: backup count, blob count, total bytes, freshest/oldest backup, in-progress markers (with age + host), prune marker state, abandoned-marker candidates. Cheap (LIST only). |

**Existing `list` extended**: `clickhouse-backup list remote` enumerates v1 *and* CAS backups, with a `[CAS]` tag. `clickhouse-backup list local` unchanged. No new `cas-list` verb — symmetry beats command proliferation.

**Help-text discoverability**:
- The existing `upload --help` gains a closing line: *"For mutation-heavy tables or chain-free incrementals, see `cas-upload`."*
- The README gains a short "CAS layout" section pointing to this design doc.

**Rejected flags**: `cas-restore` does NOT accept `--ignore-dependencies`. CAS backups have no chain, so the flag is meaningless; passing it produces an error ("CAS backups have no dependencies; flag not applicable") rather than silently being a no-op.

**Retention behavior**: `cas-upload` MUST NOT call `RemoveOldBackupsRemote`. CAS retention is exclusively managed by `cas-prune`. The v1 `backups_to_keep_remote` config knob applies only to v1 backups (and the §6.2.0 prefix exclusion ensures CAS backups don't accidentally count toward it).

#### 6.10.1 Output-format convention

`cas-verify --json` is a boolean flag that emits line-delimited JSON failures. The existing v1 `list` command uses `--format text|json|yaml|csv|tsv` for tabular listings.

These two patterns are kept distinct on purpose:

- **Tabular listings** use `--format` because operators may want csv/tsv for spreadsheet ingest.
- **Diagnostic pass/fail commands** (`cas-verify`, future `cas-fsck`) use `--json` because failures are line-delimited streams, not tables; the only useful alternatives are "human" or "machine".

When new CAS commands need machine-readable output, follow this rule: tabular → `--format`; line-delimited diagnostic → `--json`. Don't introduce a third convention without an explicit decision recorded here.

### 6.11 Configuration surface

CAS-specific parameters live under a `cas:` block in `config.yml`. Existing config file paths and env-var conventions are unchanged.

```yaml
cas:
  enabled: false             # gate; set true to allow cas-* commands against this config
  cluster_id: ""             # REQUIRED, no default. Identifies the source cluster;
                             #   persisted in BackupMetadata.CAS.ClusterID.
  root_prefix: "cas/"        # top-level prefix in the bucket. Effective per-cluster prefix
                             #   is <root_prefix><cluster_id>/  (e.g. "cas/prod-shard-1/")
  inline_threshold: 262144   # bytes (256 KiB); ValidateBackup MUST reject 0 or > 1 GiB
  grace_blob: "24h"          # prune won't delete a blob younger than this. Go duration string.
  abandon_threshold: "168h"  # 7 days; in-progress markers older than this are auto-cleaned. Go duration string.
  allow_unsafe_markers: false # opt-in for backends that lack atomic conditional create. Phase 4
                              #   implements PutFileIfAbsent natively on S3 / Azure / GCS / COS / SFTP.
                              #   FTP has no portable atomic primitive: with this flag false (default)
                              #   cas-upload and cas-prune refuse on FTP; with true, FTP falls back to
                              #   a STAT+STOR+RNFR/RNTO best-effort sequence with a per-call WARN log.
  wait_for_prune: "0s"        # if > 0, cas-upload and cas-delete poll the prune
                              #   marker for up to this duration before refusing.
                              #   Useful for cron deployments where prune may overlap
                              #   with scheduled uploads. Go duration string.
```

**Per-cluster prefix is mandatory.** Operators MUST configure `cluster_id`. Cross-cluster blob sharing is out of scope for v1; if anyone needs it, it's a v2 conversation with its own threat model.

**Env vars** (override config; prefix `CAS_*` for symmetry with `S3_*`/`GCS_*`/`AZBLOB_*`):
- `CAS_ENABLED`, `CAS_CLUSTER_ID`, `CAS_ROOT_PREFIX`
- `CAS_INLINE_THRESHOLD`, `CAS_GRACE_BLOB`, `CAS_ABANDON_THRESHOLD`
- `CAS_ALLOW_UNSAFE_MARKERS`, `CAS_WAIT_FOR_PRUNE`

**CLI flags** (override config + env):
- `cas-prune --grace-blob DUR --abandon-threshold DUR --dry-run --unlock`
- `cas-upload --skip-object-disks --dry-run [--wait-for-prune=DUR]`
- `cas-delete [--wait-for-prune=DUR]`
- `cas-verify --json`

`inline_threshold` is read from config at upload time and **persisted** in `BackupMetadata.CAS.InlineThreshold`. Restore uses the persisted value, never the current config (§6.2.1).

### 6.12 Compatibility notes

**Breaking interface change** (Phase 4). The CAS work added two new methods and one sentinel error to the public `pkg/storage.RemoteStorage` interface:

```go
type RemoteStorage interface {
    // ... existing methods ...
    PutFileAbsoluteIfAbsent(ctx context.Context, key string, r io.ReadCloser, localSize int64) (created bool, err error)
    PutFileIfAbsent(ctx context.Context, key string, r io.ReadCloser, localSize int64) (created bool, err error)
}

var ErrConditionalPutNotSupported = errors.New("conditional PutFile not supported by this backend")
```

External code that implements `RemoteStorage` directly (private forks with custom backends, third-party plugins) will fail to build until they add the two methods. Implementations that lack a native atomic-create primitive should return `ErrConditionalPutNotSupported`; CAS commands then refuse on those backends unless `cas.allow_unsafe_markers=true`.

**Backend version requirements.** S3-compatible stores must honor `If-None-Match: "*"` on `PutObject` for marker locks to be safe. AWS S3 supports it natively. MinIO requires release `RELEASE.2024-11-07T00-52-20Z` or newer; older versions silently ignore the header. CAS performs a one-shot startup probe on the first command (writes a sentinel twice and asserts the second write reports not-created); operators on confirmed-good backends can skip it via `cas.skip_conditional_put_probe=true`. Ceph RGW and other S3-compatible stores have not been validated against the probe; prefer one of the natively-supported backends in production.

**LayoutVersion downgrade.** Operators downgrading clickhouse-backup to a release that doesn't recognize the persisted `BackupMetadata.CAS.LayoutVersion` will see a refusal at restore time with a clear error. Upgrade-then-downgrade-then-restore is the failure mode; document the build matrix you support.

## 7. Reuse vs. new code

### Reused as-is
- `pkg/storage/general.go` — `BackupDestination`, multipart, retry, throttling, `CopyObject`
- `pkg/metadata/backup_metadata.go:12` — `BackupMetadata` struct (don't populate `RequiredBackup`; new `CAS` field added — see §6.2.1)
- `pkg/metadata/table_metadata.go:10` — `TableMetadata` struct (write `Parts` populated, `Files` empty, `DataFormat = "directory"`)
- `pkg/backup/backuper.go:28` — `DirectoryFormat` constant
- `pkg/backup/backuper.go:145` — shadow-directory layout for local staging
- `pkg/common/common.go:18` — `TablePathEncode` for db/table path components
- `pkg/pidlock/pidlock.go:15` — per-backup PID locking
- `pkg/backup/upload.go:114` — `prepareTableListToUpload` table iteration
- `pkg/filesystemhelper/filesystemhelper.go:119` — `HardlinkBackupPartsToStorage` for staging-to-detached
- `pkg/resumable/state.go` — progress tracking (BoltDB) usable for blob-level resume
- All `pkg/clickhouse/*` query helpers
- All restore-side schema/RBAC/configs handling

### New code (estimate: ~1500-2500 LOC)
- **`pkg/checksumstxt/`** — parser for ClickHouse's `checksums.txt` format (versions 2/3/4 on-disk; v5 minimalistic for completeness). Reference implementation already drafted at `docs/checksumstxt/checksumstxt.go` (300 LOC) with tests at `docs/checksumstxt/checksumstxt_test.go` (271 LOC) and full format spec at `docs/checksumstxt/format.md`. Move into `pkg/checksumstxt/` during Phase 1 — this is a ClickHouse part-format concept, not a CAS concept; namespace it accordingly. Keep tests against real ClickHouse part fixtures spanning compact, wide, encrypted, and projection parts.
- **`pkg/cas/validate.go`** — single `ValidateBackup(ctx, name) error` function used as a precondition by every CAS command. Enforces:
  1. Backup name is well-formed (printable ASCII only, no NUL or control chars, len ≤ 128, no `..` or path separators).
  2. `metadata.json` exists and parses.
  3. `BackupMetadata.CAS != nil` and `LayoutVersion ≤ supported_max` (refuse newer; §6.2.1).
  4. `InlineThreshold > 0 AND ≤ 1 GiB`.
  5. `ClusterID` is non-empty and matches the configured cluster.
  6. All referenced per-table archives can be HEADed.
  7. Inprogress / prune marker state is consistent with the catalog (used by `cas-delete`'s stale-marker logic, §6.6 step 2).
**Backend assumptions** (no probe in v1): CAS assumes the configured backend provides read-your-writes consistency for individual objects and a meaningful `LastModified`. AWS S3, MinIO, GCS, and Azure Blob all qualify. Quirky on-prem backends are the operator's risk to validate; document the assumption in the operator runbook.
- **`pkg/cas/blobpath.go`** — derives blob paths from hashes. Trivial.
- **`pkg/cas/upload.go`** — orchestrates the upload protocol in §6.4 (object-disk pre-flight, prune-lock check, marker management). Calls into existing `BackupDestination`.
- **`pkg/cas/download.go`** — implements `cas-download`: materializes a backup into the shadow directory.
- **`pkg/cas/restore.go`** — thin: invokes `cas-download` then hands off to existing restore flow.
- **`pkg/cas/delete.go`** — implements §6.6 (prune-lock check, ordered delete).
- **`pkg/cas/prune.go`** — implements §6.7. Streaming mergesort, parallel listings, lock-and-sweep.
- **`pkg/cas/verify.go`** — implements §6.8 (HEAD + size; `--json` output).
- **`pkg/cas/cache.go`** — cold-list and in-memory existence set. (Spill-to-disk only if a real workload exhausts memory; ship in-memory first.)
- **`pkg/cas/list.go`** — thin helpers used by the existing `list remote` to surface CAS backups with a `[CAS]` tag.
- **`cmd/clickhouse-backup/cas_*.go`** — command bindings.
- **`pkg/cas/config.go`** — CAS-specific config: root prefix, inline threshold, grace period, abandon threshold (the actual persisted parameters and the configurable knobs).

See §6.10 for the full CLI surface.

## 8. Risk register

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|-----------|
| R1 | `checksums.txt` parser bug (format version edge case, multi-block compressed v4, projection paths, etc.) producing wrong hashes → blob mis-keyed → silent corruption at restore | Low-Medium | High | Reference parser already drafted at `docs/checksumstxt/` with format spec and unit tests covering v2/v3/v4 paths. Add fixture-based tests against real ClickHouse part directories spanning compact, wide, encrypted, projection, and multi-disk parts before Phase 1 ships. `cas-verify` size check catches some manifestations. |
| R2 | GC race: in-flight upload's blob deleted before commit, OR old-orphan-reuse during concurrent prune | Low (with operator discipline) | High | `cas-prune` takes an exclusive lock; `cas-upload` and `cas-delete` refuse while it's held. `grace_blob` is defense-in-depth. Operator must serialize prune across hosts (no overlapping cron). |
| R4 | Hash collision (CityHash128) | Negligible | High | 128 bits → ~10¹⁸ blobs before 10⁻⁶ collision probability. Practically impossible at any plausible scale. Documented. |
| R5 | Memory blowup at upload (cold-list set of 10⁷ hashes) or at GC (live set of 10⁸+ hashes) | Medium | Medium | Spill cold-list to sorted on-disk file at >N entries. GC uses streaming mergesort with bounded memory. |
| R6 | Object store backend doesn't honor `LastModified` semantics needed for grace check (e.g., quirky on-prem MinIO) | Medium | High | Document: grace mechanism assumes `LastModified` reflects actual write time. Fall back to `abandon_threshold`-based stricter mode for non-conforming backends. |
| R7 | Per-table archive becomes huge (table with many parts) → restore must download whole archive even for partial-partition restore | Medium | Low | Acceptable v1; if it becomes a problem, switch to per-part archives or multi-archive splitting (matches existing `splitPartFiles` infrastructure). |
| R9 | Bucket cost surprise: per-PUT charges from many small blobs if inline threshold misconfigured | Low | Medium | Inline threshold default 256 KiB. Document the cost trade-off. |
| R10 | First CAS upload after migration is huge because nothing is shared with v1 backups | Certain | Low | Expected. Document. CAS dedup compounds across subsequent CAS backups. |
| R11 | Crashed upload leaves orphan blobs that aren't reclaimed for `grace_blob` | Certain | Low | Expected; tolerable per design. The orphan-cleanup latency is bounded by `grace_blob`. |
| R13 | Object-disk tables encountered during `cas-upload` cause silent skip or partial backup | Certain (if user has them) | High | `cas-upload` does pre-flight pass and refuses with a list of offending `(db, table, disk)` triples. `--skip-object-disks` excludes them. Operator must use v1 `upload` for those tables. v2 lifts. |
| R17 | Same-name concurrent `cas-upload` from two hosts: both pass the metadata.json existence check, both PUT, last writer wins on root metadata | Low (if naming convention followed) | High | v1 of CAS does not protect against this; documented as unsupported (§6.4 step 3). Operators MUST use unique backup names per shard. v2 may add S3 conditional-create-based "claim" for multi-host coordination. |
| R14 | Layout-parameter mismatch between upload-time config and restore-time config (e.g., `inline_threshold` changed) → restore reads wrong location → silent corruption | Medium | High | Persist all layout parameters in `BackupMetadata.CAS` (§6.2.1); restore reads from there exclusively, ignoring config. CAS commands refuse to operate on backups whose `CAS` block is missing or has unknown `LayoutVersion`. |
| R15 | Adversarial CityHash128 collision (attacker crafts a colliding blob to corrupt restore) | Negligible-Low | High | CityHash128 is non-cryptographic; collisions are findable by motivated attackers. Backup-tool threat model assumes trusted bucket. **CAS cannot switch to a stronger hash without ClickHouse upstream changes** — the hash comes from each part's `checksums.txt`, written by ClickHouse. If adversarial-collision resistance becomes a real requirement, it's an upstream conversation, not a clickhouse-backup change. |
| R16 | `cas-delete` interrupted between deleting `metadata.json` and rest of subtree → metadata-orphans accumulate | Low | Low | Live-set computation ignores subtrees without `metadata.json` (§6.6). Prune does lazy cleanup of metadata-orphan directories. |

## 9. Deferred to v2 of CAS

This section is the consolidated backlog of items raised across the design-interview, brainstorming, and external-review waves and explicitly punted out of the v1 ship train. Each entry names a category and a one-line rationale for deferral. Feature-class items get a short "what it would do" line; correctness/operability items name the file or scenario that motivates them.

### 9.1 Major features

- **Hash verification on download** (full content re-hash). v1 ships HEAD + size verification only (`cas-verify`); v2 adds `cas-verify --deep` that downloads each blob and re-hashes against the value in `checksums.txt`. Wall-clock cost is minutes-to-hours at 100TB; size verification catches the realistic silent-corruption-from-buggy-GC class for free.
- **Object-disk parts** (s3 / azure / hdfs object disks). `cas-upload` refuses these in v1 (§3); v2 needs a design pass for content-addressing already-remote object stubs and the cross-storage key rewriting paths in `pkg/backup/create.go:1031` / `pkg/backup/restore.go:2227`.
- **Convergent encryption**. Required for v1 client-side-encryption users to migrate to CAS without losing client-side encryption. Known weaknesses (confirmation-of-file attacks) need threat-modeling per deployment.
- **`cas-fsck` repair tool**. Walks local part directories and re-uploads missing blobs in bulk; today the only recovery from a broken backup is `cas-delete` + fresh `create` + `cas-upload`.
- **Parallel `cas-verify`**. Today HEAD calls run sequentially; parallelizing across blobs gives a multi-x speedup at zero correctness cost. Deferred because v1 verify is fast enough at the target scale.
- **Per-blob resumable uploads**. Existing `pkg/resumable` is per-archive; CAS uploads at blob granularity. Either extend resumable state or maintain a separate per-blob completion log.
- **Migration tool from v1 to CAS**. Out of scope for v1; users opt in by writing new backups with `cas-upload`.
- **Distributed locking via S3 conditional create** (true multi-host coordination). Phase 4 added per-backend `PutFileIfAbsent` and Phase 6 wired it into both markers, which closes the local same-name race; cross-host coordination across many writers on the same backup name is still operator-policy.
- **Local-disk / NFS target for CAS**. Today `cas-*` commands run against object-store backends (S3/Azure/GCS/COS) and SFTP/FTP. A local filesystem target (plain `file://` path or NFS mount) is attractive for on-prem deployments and air-gapped backups. Most pieces port cleanly: blob layout is just files, atomic markers map to `O_CREAT|O_EXCL`, cold-list is `filepath.WalkDir`. Open questions: how `cas-prune`'s `LastModified`-based grace handles NFS clock skew between writer and pruner; whether to expose the existing `pkg/storage` filesystem backend (if any) or write a thin local backend specifically for CAS; concurrency semantics across multiple writers on the same NFS export.
- **Refcount-delta / blob-manifest optimization for prune**. Re-evaluate if catalog grows past several hundred backups or prune wall-clock becomes painful. Decide between post-commit manifest, per-backup blob-list sidecar, or delta files based on real measurements.

### 9.2 Performance / scalability

- **Prune mark-phase parallelism**. Today the live-set walk is single-threaded over `cas/metadata/*/`. With hundreds of backups this dominates prune wall-clock. Trivial to parallelize across backups with bounded concurrency.
- **`SweepOrphans` spill-to-disk**. Current implementation streams the merge but holds intermediate state in memory; at very large catalogs (>10⁸ blobs) spill the sorted intermediates to disk. Existing on-disk live-set already does this; mirror the pattern for the orphan side.
- **Streaming archive upload**. Per-table archives are built fully in memory before upload. Streaming the tar.zstd into the multipart upload pipe halves the peak RSS for tables with many small files.
- **Heap-merge for `shardIter`**. Cold-list merges 256 sorted shard streams via a flat sweep; a binary-heap merge is asymptotically tighter and matters when the per-shard stream count grows (e.g. wider sharding in v2).
- **`ExistenceSet` memory bound**. v1 ships in-memory only (per §10.2 estimate, ~600 MB at 10⁷ blobs). Add spill-to-disk only when a real workload exhausts memory.

### 9.3 Operability / observability

- **Structured prune logs**. Today prune emits human-readable status lines; for cron / observability pipelines, add a `--log-format=json` option emitting one structured event per phase (mark-start, mark-done with counts, sweep-start, sweep-done with bytes-reclaimed, marker-release).
- **Populate `BlobsTotal` and `OrphansHeldByGrace` in PruneReport**. Fields exist; values are zero in v1 because counting them adds a LIST pass. Cheap and useful for capacity planning.
- **`BytesReclaimed` formatting**. Report carries raw bytes; surface `FormatBytes` rendering in CLI output.
- **Upload / download progress logging**. v1 logs only at start/end of each archive. Per-blob progress (especially for download) helps operator confidence on large restores.
- **`cas-status` historical trend**. Today reports a snapshot; persisting a small JSON history file (last N runs) would let `cas-status --trend` surface growth/shrink rates without external infra.

### 9.4 Correctness defenses (low-likelihood, defense-in-depth)

- **S3 `IfNoneMatch` startup probe**. AWS S3 supports `IfNoneMatch: "*"` since Nov 2024; older MinIO releases (pre-RELEASE.2024-11) silently ignore the header and the PUT succeeds unconditionally, defeating the marker lock. v1 documents the minimum MinIO version in the runbook; v2 should run a small startup probe (PUT a sentinel twice, expect the second to 412) and refuse to start if the backend silently overwrites.
- **`RemoteStorage` interface compatibility note in changelog**. Phase 4 added `PutFileAbsoluteIfAbsent` and `ErrConditionalPutNotSupported` to `pkg/storage.RemoteStorage`. Any external downstream implementing this interface directly will fail to compile until they add the method. Flag in release notes.
- **Downgrade warning for `LayoutVersion`**. Operators downgrading to a tool that doesn't recognize the persisted `BackupMetadata.CAS.LayoutVersion` get a refusal at restore time. Document the upgrade-then-downgrade hazard explicitly in the runbook.

### 9.5 Test coverage (deferred — load-bearing tests already ship)

- **Real-production error-classification tests for GCS/COS/FTP backends**. Phase 7 added `pkg/storage/errors_test.go` with focused not-found tests, but the GCS/COS/FTP subtests call mirror-functions defined in the test file rather than the production code paths (S3 calls real production code via httptest; azblob and SFTP are explicit `t.Skip` with pointers to integration coverage). Tighten by extracting the production classifiers into named exported helpers and calling them from the test.

### 9.6 UX / docs polish

- **`--data` flag is a no-op on v1 commands when CAS is enabled**. Already hidden in the CLI; documented in operator runbook. Remove entirely when CAS becomes the default in a future major version.
- **`cas-delete --force`** to bypass stale-marker checks. Today operators clear stranded markers via `cas-prune --abandon-threshold=0s`; a direct `--force` flag on `cas-delete` is more discoverable.
- **Help-text examples for common flows**. README has the headline flows; per-command `--help` could carry one or two example invocations each.
- **Changelog entries**. The phased shipping has produced ~50 commits on `cas-phase1`; before merge, condense into a coherent CHANGELOG section that names the feature and references this design doc rather than the per-phase plan files (which are gitignored).

### 9.7 Out of scope (not on any roadmap)

- Garbage collection of metadata across replicas/clusters beyond what mark-and-sweep already handles.
- Object Lock / immutability features beyond what's intrinsic to content addressing.
- Cross-cluster blob sharing. Phase 1 mandates `cluster_id`; if cross-cluster dedup ever becomes a requirement, it's a v2 conversation with its own threat model (one cluster can poison another's blob store).
- Adversarial-collision resistance on the content hash. The hash is whatever ClickHouse writes in `checksums.txt` (CityHash128 today); switching to a stronger hash is an upstream conversation, not a clickhouse-backup change.

### 9.8 Implementation-time decisions

- **Inline threshold default**: 256 KiB is a starting point; profile against a representative ClickHouse part-file distribution before locking it in.

## 10. Appendix

### 10.1 Request-rate sanity check (justifies 256-prefix sharding)

S3 limits: ~3500 PUT/COPY/POST/DELETE per second per partition prefix; ~5500 GET/HEAD per second per partition prefix.

**Upload phase** (100 TB × 10⁷ files; assume 1 GB/s network; ~10 MB avg file):
- Aggregate ~100 PUT/s. Distributed evenly across 256 prefixes → ~0.4 PUT/s/prefix. Three orders of magnitude under the limit.
- Worst case (small-file-heavy, 1 MB avg): ~1000 PUT/s aggregate → ~4 PUT/s/prefix. Still trivial.

**Cold-list phase**:
- 10⁷ blobs / 1000 keys per page = 10⁴ LIST calls. With 256-way parallelism: ~40 LIST per prefix; <1 second wall-clock.
- Cost: ~$0.05.

**Garbage collection**:
- LIST `metadata/*` → ~100 entries; one call.
- Metadata archive download: ~10⁴ archives total at ~MB each; tens of GB total; same-region S3 egress is free; minutes wall-clock.
- LIST `blob/*` for orphan scan: same as cold-list; <1 second wall-clock.

Two-byte sharding gives ample headroom. One-byte (16 prefixes) would also work at this scale. Two-byte is git-familiar and provides headroom for users with much larger catalogs.

### 10.2 Memory budget

- **Upload-time existence cache**: ~10⁷ blobs × 32 bytes/hash + overhead ≈ 600 MB peak. v1 ships in-memory only; spill-to-disk added only if a real workload exhausts memory. (600 MB is acceptable on any host already running clickhouse-backup against 100TB.)
- **GC-time live-set**: ~10⁸ refs aggregate across 100 backups; held as a sorted on-disk file (streaming mergesort over per-backup contributions). Bounded RAM regardless of catalog size.
- **GC-time orphan-scan**: streaming compare against the on-disk live-set; bounded RAM.

### 10.3 Implementation phasing

**Phase 1** — MVP upload + restore round-trip (the smallest shippable thing):
- Move `docs/checksumstxt/` into `pkg/checksumstxt/`; extend tests with real ClickHouse part fixtures (compact, wide, encrypted, projection, multi-disk)
- `pkg/cas/config.go` with the §6.11 schema; `BackupMetadata.CAS` struct + persistence (§6.2.1)
- Blob path derivation, encoded db/table path components
- Object-disk detection (pre-flight + `--skip-object-disks`)
- `cas-upload`: prune-lock check, `metadata.json` collision check, cold-list cache, blob upload, per-table `.tar.zstd`, ordered commit (§6.4 step 13)
- `cas-download` and `cas-restore`: shadow-directory staging with `DataFormat="directory"`, filter support (`--tables`, `--partitions`, `--schema-only`, `--data-only`, `--restore-database-mapping`, `--restore-table-mapping`, `--rm`); `--ignore-dependencies` rejected with explicit error
- `cas-delete` (prune-lock check, ordered delete §6.6)
- `cas-verify` (HEAD + size; `--json`)
- `cas-delete` (with §6.6 ordering: metadata.json first)
- `list remote` extended to surface CAS backups with `[CAS]` tag
- v1 `BackupList` / `RemoveBackupRemote` / `RemoveOldBackupsRemote` / `CleanRemoteBroken` exclude the configured CAS root prefix- Cross-mode guards in v1 `delete` / `download` / `restore` (§6.2.2)
- README + `--help` discoverability hooks (§6.10)

**Phase 1.5** — operational primitives (between MVP and prune):
- `cas-status` (bucket health summary; LIST-only, cheap; surfaces in-progress markers and prune-marker state)

**Phase 2** — prune:
- `cas-prune`: mark-and-sweep with exclusive lock (refuses while `cas-upload`/`cas-delete` are in flight; the symmetric refusal is enforced from the upload/remove side), abandoned-upload sweep, grace-period delete, fail-closed on unreadable live-backup metadata, metadata-orphan lazy cleanup
- `--dry-run` for sanity checks
- Operator runbook (when to run, what failures mean, manual recovery from `cas-verify` output)

**Phase 3 (shipped)** — planner correctness:
- Two-pass projection-aware part walker (extract-set + file walk) replacing the original recursive directory scan; closes the projection-path silent-skip class
- `ExcludedTables` plumbed through `UploadOptions` so `--skip-object-disks` excludes by decoded `(db, table)` pair, not by `DiskInfo.Path` (which was empty)
- Empty-`Parts` table guard in `uploadTableJSONs` (skips tables with `len(tp.parts) == 0` to avoid producing archives with no entries)
- `validateChecksumsTxtFilename` hoisted above the `.proj` recursion branch, closing the path-traversal corner

**Phase 4 (shipped)** — atomic markers:
- New `PutFileAbsoluteIfAbsent(ctx, key, r, size) (created bool, err error)` on `pkg/storage.RemoteStorage`, with `ErrConditionalPutNotSupported` sentinel
- Implementations: S3 `IfNoneMatch: "*"` on direct PutObject (bypasses `s3manager.Uploader` because markers are <1KB); Azure `If-None-Match: *`; GCS `Conditions{DoesNotExist: true}`; COS `If-None-Match: *`; SFTP `OpenFile(O_WRONLY|O_CREATE|O_EXCL)` mapping to `SSH_FXF_EXCL`; FTP refuses by default, opts into STAT+STOR+RNFR/RNTO best-effort with `cas.allow_unsafe_markers`
- Symmetric relative-key `PutFileIfAbsent` on the `cas.Backend` adapter (so casstorage marker writes go through the configured `<root>/<cluster>/cas/...` prefix instead of bucket-root)
- `WriteInProgressMarker` and `WritePruneMarker` return `(created, err)`; upload/prune branch on `!created` and surface a diagnostic naming the existing marker's host + start time

**Phase 5 (shipped)** — backend smoke tests:
- testcontainers-driven integration coverage for MinIO, Azurite, fake-gcs-server, and OpenSSH-server SFTP; FTP exercised via proftpd in the refusal path. 16 CAS integration tests pass (15 PASS, 1 SKIP), spanning 5 of 6 backends
- Surfaced and fixed 4 pre-existing storage-layer bugs (SFTP/FTP `WalkAbsolute` and `DeleteFile` not-found handling) that CAS exercises but v1 paths never hit

**Phase 6 (shipped)** — P1 defects from external review:
- Inprogress-marker cleanup on the StatFile-recheck error branch and on the metadata.json commit failure (steps 11b and 12) — previously leaked the marker, blocking the backup name for `abandon_threshold` (default 7 days)
- `cas-prune --dry-run --unlock` is now a no-op rather than deleting the real prune marker
- `cas-{download,restore} --data-only` returns `ErrNotImplemented` instead of silently doing a full download
- Zero-`ModTime` markers (FTP `LIST` without MLSD facts) are treated as fresh; zero-`ModTime` blobs are treated as inside grace — closes the data-loss path where prune sweeps every active marker on FTP-like backends
- Object-disk preflight now scans `metadata.json` rather than only the local shadow tree, catching fully-remote tables that have no local part directories
- `--skip-object-disks` exclusions are computed against decoded `(db, table)` names (matching planUpload's lookup) rather than the encoded shadow directory names

**Phase 7 (shipped)** — cleanup round:
- `ColdList` TOCTOU re-validation: after the pre-commit prune-marker re-check, HEAD every blob skipped via cold-list and abort if any disappeared (closes the narrow window where a concurrent prune past `grace_blob` could delete blobs the upload was about to commit a reference to)
- `PruneReport` counters populated: `BlobsTotal` and `OrphansHeldByGrace` now reflect actual scan counts, no extra LIST passes
- `BytesReclaimed` rendered via `utils.FormatBytes` in `PrintPruneReport` with raw count in parentheses
- Defensive `cfg.Validate()` at `Prune` entry to protect embedded callers from misconfigured input
- Explicit-zero `--grace-blob=0s` / `--abandon-threshold=0s` override semantics locked with focused unit tests
- Per-backend not-found classification tests in `pkg/storage/errors_test.go` (S3 against httptest is real production code; GCS/COS/FTP exercise mirror-functions documenting intent; azblob/SFTP `t.Skip` with integration-test pointers — see §9.5 deferred)
- `casstorage.Walk` key-reconstruction extracted into testable `reconstructAbsoluteKey` helper with table-driven coverage
- Cross-backup dedup integration test: third backup whose payload column files are byte-identical to an earlier backup's reuses 100% of those blobs (`bytesC/bytesA = 0%`)

**Phase 8 (shipped)** — wait_for_prune + REST API:
- `cas.wait_for_prune` config knob and `--wait-for-prune=DUR` CLI flag on `cas-upload` and `cas-delete`. When > 0, polls the prune marker every 2s for up to that duration before refusing. Explicit `0s` overrides non-zero config. The pre-commit prune-marker re-check (upload step 11a) deliberately does NOT wait — any prune that started after step 2 is racing in-flight blob uploads and the safe response is to abort.
- All seven CAS commands wired through the daemon-mode REST API: dedicated routes (`POST /backup/cas-upload/{name}` etc.), `/backup/actions` recognizes the same `cas-*` verb names, `GET /backup/list` merges CAS backups into the existing array with a `kind` field (`"v1"` or `"cas"`) and an optional `cas` sub-object on CAS rows. Async commands (upload, download, restore, verify, prune) return an `acknowledged` envelope with an `operation_id`; clients poll `GET /backup/status` for completion. `cas-delete` is sync; `cas-status` is sync GET. Backuper signatures unified to take a `commandId int` parameter so HTTP and CLI register identically in `status.Current`.

**Phase 9 (planned)** — performance and operability:
- See §9.2 (performance) and §9.3 (operability) for the consolidated backlog. None of these are correctness gates; they are response to real workload measurements.
- Performance benchmarks against representative datasets. **TODO**: pin concrete success targets before benchmarking. Suggested starting points (operator to confirm):
  - **Mutation dedup**: post-mutation backup uploads ≤ 5% of unmutated backup size on a 100TB-with-one-mutated-column scenario (the headline value-prop).
  - **Cold full backup**: within 1.2× of v1's wall-clock for the same dataset (slight overhead acceptable due to per-file HEAD checks).
  - **Repeat-of-same-data backup**: < 5 min wall-clock for 100TB if all blobs are already present (cold-list dominates).
  - **Restore**: within 1.5× of v1's wall-clock (slower due to per-blob fetches; acceptable trade for chain-free).
- Stress tests for the prune-lock + grace-period correctness paths under sustained concurrent upload load.

### 10.4 Ship-gating tests

Implementer fills in normal coverage during code review. These are the load-bearing tests that must pass before each phase ships:

**Phase 1:**
- `TestCASRoundtrip` — cas-upload → cas-download → byte-compare every file.
- `TestMutationDedup` — the headline value-prop. Backup, ALTER UPDATE one column, OPTIMIZE, backup again; assert the second backup uploads roughly the mutated-column's blobs only.
- `TestCompatibilityMixedBucket` — v1 + CAS backups same bucket; v1 commands refuse CAS targets; v1 retention/list/clean-broken don't touch CAS prefix.
- `TestV1RefusesCASBackup` / `TestCASRefusesV1Backup` — cross-mode guards.
- `TestUploadCommitChecksPruneMarker` — pre-commit re-check closes the old-orphan-reuse race.
- `TestParseV4_MultiBlock` / `TestParseFilenameTraversal` — parser hardening.
- `TestTarExtractionContainment` — path-traversal defense (also patch the v1 path).

**Phase 2 (prune):**
- `TestPruneGracePeriodRespected` — fresh blob younger than `grace_blob` is never deleted.
- `TestPruneMarkerReleasedOnError` — defer-release runs on every exit path.
- `TestPruneSweepsAbandonedMarker` — markers older than `abandon_threshold` are cleaned up.

### 10.5 Glossary

- **Blob**: an immutable file in `cas/blob/<aa>/<rest>`, content-keyed by the CityHash128 of its contents.
- **Live set / referenced set**: union of blob paths referenced by any backup whose `metadata.json` exists.
- **Orphan**: a blob in the blob store with no live references.
- **Grace period (`grace_blob`)**: the minimum age a blob must have before prune may delete it.
- **Abandon threshold**: how long an `inprogress` marker must persist before being treated as a crashed upload.
- **Cold-list**: parallel `LIST` of all `cas/blob/<aa>/` prefixes at the start of an upload, to seed the existence cache.
- **In-progress marker**: a small sentinel file at `cas/inprogress/<backup>.marker` written when an upload starts and deleted at commit.
- **Prune marker**: `cas/prune.marker`. The advisory exclusive lock for GC. While present, `cas-upload` and `cas-delete` refuse to start.
