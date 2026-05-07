# CAS Layout — Phase 2 (Prune) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add mark-and-sweep garbage collection to the CAS layout via a new `cas-prune` command. This reclaims orphan blobs (no live backup references them) and metadata-orphan subtrees (deletion left behind), and sweeps abandoned in-progress markers.

**Architecture:** Single-writer, advisory-locked. `cas-prune` writes `cas/<cluster>/prune.marker` with a random run-id; reads it back to detect concurrent racers; runs mark (walk every live backup's per-table archives, extract `checksums.txt`, accumulate referenced blobs into a sorted on-disk file via streaming mergesort) and sweep (list `cas/<cluster>/blob/<aa>/` in parallel, stream-compare against the live set, delete orphans older than `grace_blob`). Releases the marker via deferred call. The lock is enforced from the **upload** and **delete** sides (already done in Plan A) — `cas-upload` and `cas-delete` refuse to start when `prune.marker` exists.

**Tech Stack:** Same as Plan A. New: streaming external-sort over blob references (10⁸ ref scale → bounded RAM).

**Spec:** `docs/cas-design.md` §6.7. Risk references: R2, R6, R11.

**Pre-requisite:** Plan A merged. The types, paths, marker primitives, fake backend, archive helpers, and `ValidateBackup` already exist.

---

## File structure

### New files
| Path | Responsibility |
|---|---|
| `pkg/cas/prune.go` | `Prune(ctx, b, cfg, opts)` — orchestrator for §6.7. |
| `pkg/cas/prune_test.go` | Unit tests against the fake backend. |
| `pkg/cas/markset.go` | Streaming on-disk sorted set: writer, reader, merger. |
| `pkg/cas/markset_test.go` | Unit tests. |
| `pkg/cas/sweep.go` | Parallel listing of `cas/<cluster>/blob/<aa>/`, stream-diff against the live set. |
| `pkg/cas/sweep_test.go` | |
| `cmd/clickhouse-backup/cas_commands.go` (modified) | Add `cas-prune` subcommand with flags. |
| `pkg/backup/cas_methods.go` (modified) | Add `Backuper.CASPrune(...)`. |
| `test/integration/cas_prune_test.go` | Integration tests (grace, abandoned-marker sweep, lock release on panic). |

### Modified files (none beyond Plan A's tree)

---

## Conventions

Same as Plan A. Branch: `cas-phase2-prune`.

---

## Task 1: `pkg/cas/markset.go` — streaming on-disk sorted set

**Files:**
- Create: `pkg/cas/markset.go`
- Create: `pkg/cas/markset_test.go`

**Why a new component**: at ~10⁸ blob references aggregated across 100 backups, the live set doesn't fit in memory. The mark phase appends references to a sorted-on-disk file (16-byte hashes, sorted), then sweep streams it alongside the LIST output of the blob store.

- [ ] **Step 1: Tests**

```go
func TestMarkSet_WriteSortRead(t *testing.T) {
    tmp := t.TempDir()
    w, err := NewMarkSetWriter(filepath.Join(tmp, "marks"), 1024)
    if err != nil { t.Fatal(err) }
    refs := []Hash128{
        {High: 0xff, Low: 1},
        {High: 0x00, Low: 5},
        {High: 0x80, Low: 3},
        {High: 0x00, Low: 5}, // duplicate (different parts referencing same blob)
        {High: 0x00, Low: 1},
    }
    for _, h := range refs { _ = w.Write(h) }
    if err := w.Close(); err != nil { t.Fatal(err) }
    r, err := OpenMarkSetReader(filepath.Join(tmp, "marks"))
    if err != nil { t.Fatal(err) }
    defer r.Close()
    var got []Hash128
    for {
        h, ok, err := r.Next()
        if err != nil { t.Fatal(err) }
        if !ok { break }
        got = append(got, h)
    }
    want := []Hash128{
        {High: 0x00, Low: 1},
        {High: 0x00, Low: 5},
        {High: 0x80, Low: 3},
        {High: 0xff, Low: 1},
    }
    if !reflect.DeepEqual(got, want) { t.Fatalf("got %v want %v", got, want) }
}

func TestMarkSet_LargeExternalSort(t *testing.T) {
    // 1M random refs, small in-memory chunk size (1024) → forces multi-run mergesort
    // assert: output is sorted, deduplicated, length matches expected unique count
}
```

- [ ] **Step 2: Implement**

`pkg/cas/markset.go`:

```go
package cas

import (
    "bufio"
    "container/heap"
    "encoding/binary"
    "errors"
    "io"
    "os"
    "path/filepath"
    "sort"
)

// MarkSetWriter accumulates Hash128 references and produces a sorted, deduped
// on-disk file. Implementation: in-memory buffer of `chunk` entries; when full,
// sort and spill to a "run" file; on Close, k-way merge all runs into the
// final output, deduplicating in the process.
type MarkSetWriter struct {
    finalPath string
    runDir    string
    chunk     int
    buf       []Hash128
    runs      []string
}

func NewMarkSetWriter(finalPath string, chunk int) (*MarkSetWriter, error) {
    runDir, err := os.MkdirTemp(filepath.Dir(finalPath), "markset-runs-*")
    if err != nil { return nil, err }
    return &MarkSetWriter{finalPath: finalPath, runDir: runDir, chunk: chunk, buf: make([]Hash128, 0, chunk)}, nil
}

func (w *MarkSetWriter) Write(h Hash128) error {
    w.buf = append(w.buf, h)
    if len(w.buf) >= w.chunk { return w.spill() }
    return nil
}

func (w *MarkSetWriter) spill() error {
    if len(w.buf) == 0 { return nil }
    sort.Slice(w.buf, func(i, j int) bool { return less(w.buf[i], w.buf[j]) })
    p := filepath.Join(w.runDir, fmt.Sprintf("run-%05d", len(w.runs)))
    f, err := os.Create(p); if err != nil { return err }
    bw := bufio.NewWriter(f)
    var prev Hash128; first := true
    for _, h := range w.buf {
        if !first && h == prev { continue } // dedup within run
        if err := writeHash(bw, h); err != nil { f.Close(); return err }
        prev = h; first = false
    }
    if err := bw.Flush(); err != nil { f.Close(); return err }
    if err := f.Close(); err != nil { return err }
    w.buf = w.buf[:0]
    w.runs = append(w.runs, p)
    return nil
}

func (w *MarkSetWriter) Close() error {
    if err := w.spill(); err != nil { return err }
    return mergeRuns(w.runs, w.finalPath)
}

// MarkSetReader streams sorted hashes from disk.
type MarkSetReader struct { f *os.File; br *bufio.Reader }
func OpenMarkSetReader(p string) (*MarkSetReader, error) { /* ... */ }
func (r *MarkSetReader) Next() (Hash128, bool, error)    { /* read 16 bytes */ }
func (r *MarkSetReader) Close() error                    { return r.f.Close() }

func less(a, b Hash128) bool {
    if a.High != b.High { return a.High < b.High }
    return a.Low < b.Low
}
func writeHash(w io.Writer, h Hash128) error {
    var b [16]byte
    binary.LittleEndian.PutUint64(b[0:8], h.Low)
    binary.LittleEndian.PutUint64(b[8:16], h.High)
    _, err := w.Write(b[:]); return err
}
func mergeRuns(runs []string, dst string) error {
    // k-way heap merge across runs, deduping at the boundary.
    // Output: a single sorted file with no duplicates.
}
```

- [ ] **Step 3: Iterate to green**

Run: `go test ./pkg/cas/ -run TestMarkSet -race -count=1 -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/cas/markset.go pkg/cas/markset_test.go
git commit -m "feat(cas): streaming on-disk mark set with external mergesort"
```

---

## Task 2: `pkg/cas/sweep.go` — parallel orphan scan

**Files:**
- Create: `pkg/cas/sweep.go`
- Create: `pkg/cas/sweep_test.go`

The sweep phase: for each of 256 prefixes in parallel, list blobs and stream-compare against the sorted mark set. Anything not in the mark set AND older than `grace` is an orphan candidate.

- [ ] **Step 1: Tests**

```go
func TestSweep_ReturnsOnlyUnreferencedAndOldEnough(t *testing.T) {
    f := fakedst.New(); ctx := context.Background()
    // populate fake bucket: 5 blobs total
    //   b1, b2 referenced; b3 unreferenced and old; b4 unreferenced and fresh; b5 referenced
    //   PutFile + manually set ModTime (fake supports SetModTime helper for tests)
    marks := buildMarkSet(t, []Hash128{h1, h2, h5})
    cands, err := SweepOrphans(ctx, f, "cas/c1/", marks, time.Hour, time.Now())
    if err != nil { t.Fatal(err) }
    // expect only b3
    assertHashes(t, cands, []Hash128{h3})
}

func TestSweep_RespectsGracePeriodPrecisely(t *testing.T) {
    // blob with ModTime exactly grace ago → must NOT be deleted (strict < cutoff)
}
```

- [ ] **Step 2: Implement**

```go
func SweepOrphans(ctx context.Context, b Backend, clusterPrefix string, marks *MarkSetReader, grace time.Duration, t0 time.Time) ([]OrphanCandidate, error) {
    cutoff := t0.Add(-grace)
    type shardOut struct{ blobs []remoteBlob; err error }
    shards := make([]shardOut, 256)
    var wg sync.WaitGroup
    sem := make(chan struct{}, 32)
    for i := 0; i < 256; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            sem <- struct{}{}; defer func(){ <-sem }()
            prefix := fmt.Sprintf("%sblob/%02x/", clusterPrefix, i)
            var blobs []remoteBlob
            err := b.Walk(ctx, prefix, true, func(rf RemoteFile) error {
                h, ok := parseHashFromKey(rf.Key, prefix); if !ok { return nil }
                blobs = append(blobs, remoteBlob{hash: h, modTime: rf.ModTime, size: rf.Size, key: rf.Key})
                return nil
            })
            sort.Slice(blobs, func(a, c int) bool { return less(blobs[a].hash, blobs[c].hash) })
            shards[i] = shardOut{blobs: blobs, err: err}
        }(i)
    }
    wg.Wait()
    for _, s := range shards {
        if s.err != nil { return nil, s.err }
    }
    // streaming compare: walk shards in order ⊕ marks (single sorted pass)
    return streamCompareWithMarks(shards, marks, cutoff), nil
}
```

- [ ] **Step 3: Iterate to green, commit**

Run: `go test ./pkg/cas/ -run TestSweep -race -count=1 -v`

```bash
git add pkg/cas/sweep.go pkg/cas/sweep_test.go
git commit -m "feat(cas): parallel orphan sweep with grace cutoff"
```

---

## Task 3: `pkg/cas/prune.go` — orchestrator (§6.7)

**Files:**
- Create: `pkg/cas/prune.go`
- Create: `pkg/cas/prune_test.go`

- [ ] **Step 1: Tests covering every §6.7 rule**

```go
func TestPrune_HappyPath(t *testing.T) {
    // 2 backups, 100 blobs total, 90 referenced, 10 orphans (5 old / 5 young)
    // assert: 5 blobs deleted (only old orphans), prune.marker absent after
}

func TestPrune_RefusesIfFreshInProgressMarker(t *testing.T) {
    // inprogress marker younger than abandon_threshold → refuse with helpful error
}

func TestPrune_SweepsAbandonedMarker(t *testing.T) {
    // inprogress marker older than abandon_threshold → swept (deleted), prune continues
}

func TestPrune_FailClosedOnUnreadableLiveBackup(t *testing.T) {
    // make one live per-table archive return error from GetFile;
    // assert: prune aborts WITHOUT deleting any blob and WITHOUT releasing marker until defer
}

func TestPrune_ConcurrentRunIDDetection(t *testing.T) {
    // pre-populate a prune.marker with a different run-id;
    // call Prune; expect "concurrent prune detected" error
}

func TestPrune_DeferReleasesMarkerOnError(t *testing.T) {
    // inject an error after writing the marker but before the natural release;
    // assert: marker is gone after Prune returns
}

func TestPrune_DeferReleasesMarkerOnPanic(t *testing.T) {
    // inject a panic; recover at boundary; assert marker gone
}

func TestPrune_GracePeriodRespected(t *testing.T) {
    // identical to §10.4 Phase 2 test
}

func TestPrune_DryRun(t *testing.T) {
    // DryRun=true: assert no marker written, no blob deleted, but candidates printed
}

func TestPrune_MetadataOrphanSubtreeSwept(t *testing.T) {
    // pre-populate cas/<c>/metadata/halfdeleted/ with table JSONs but NO metadata.json;
    // assert: subtree deleted by step 10
}

func TestPrune_Unlock(t *testing.T) {
    // pre-populate cas/<c>/prune.marker; call Prune with Unlock=true; assert marker deleted, no other work performed
}
```

- [ ] **Step 2: Implement (§6.7 step-for-step)**

```go
type PruneOptions struct {
    DryRun           bool
    GraceBlob        time.Duration // overrides cfg.GraceBlob if set
    AbandonThreshold time.Duration // overrides cfg.AbandonThreshold if set
    Unlock           bool
}

type PruneReport struct {
    DryRun                  bool
    LiveBackups             int
    LiveBlobsReferenced     uint64
    BlobsTotal              uint64
    OrphanBlobsConsidered   uint64
    OrphansHeldByGrace      uint64
    OrphansDeleted          uint64
    AbandonedMarkersSwept   int
    MetadataOrphansSwept    int
    DurationSeconds         float64
}

func Prune(ctx context.Context, b Backend, cfg Config, opts PruneOptions) (*PruneReport, error) {
    if !cfg.Enabled { return nil, errors.New("cas: cas.enabled=false") }
    cp := cfg.ClusterPrefix()
    grace := cfg.GraceBlob; if opts.GraceBlob != 0 { grace = opts.GraceBlob }
    abandon := cfg.AbandonThreshold; if opts.AbandonThreshold != 0 { abandon = opts.AbandonThreshold }

    // --- --unlock escape hatch (§6.7 stale-marker recovery) ---
    if opts.Unlock {
        _, ok, _ := b.HeadFile(ctx, PruneMarkerPath(cp))
        if !ok { return nil, errors.New("cas: --unlock specified but no prune.marker present") }
        if err := b.DeleteFile(ctx, PruneMarkerPath(cp)); err != nil { return nil, err }
        log.Warn().Msg("cas-prune: prune marker manually unlocked by operator")
        return &PruneReport{}, nil
    }

    rep := &PruneReport{DryRun: opts.DryRun}
    start := time.Now()

    // step 1: sanity check on inprogress markers.
    fresh, _, err := classifyInProgress(ctx, b, cp, abandon)
    if err != nil { return nil, err }
    if len(fresh) > 0 {
        return nil, freshInProgressError(fresh) // includes name, host, age
    }

    // step 2: write marker, read back, validate run-id.
    if !opts.DryRun {
        runID, err := WritePruneMarker(ctx, b, cp, hostname())
        if err != nil { return nil, err }
        defer func() { _ = b.DeleteFile(ctx, PruneMarkerPath(cp)) }() // §6.7 step 12
        m, err := ReadPruneMarker(ctx, b, cp); if err != nil { return nil, err }
        if m.RunID != runID { return nil, errors.New("cas: concurrent prune detected; aborting") }
    }

    // step 3: T_0 = now() (use start above)
    T0 := start

    // step 4: abandoned-upload sweep
    _, abandoned, _ := classifyInProgress(ctx, b, cp, abandon)
    if !opts.DryRun {
        for _, m := range abandoned { _ = b.DeleteFile(ctx, InProgressMarkerPath(cp, m.Backup)) }
    }
    rep.AbandonedMarkersSwept = len(abandoned)

    // step 5: list live backups
    backups, err := listLiveBackups(ctx, b, cp); if err != nil { return rep, err }
    rep.LiveBackups = len(backups)

    // step 6: build mark set by walking each live backup's per-table archives
    tmp := os.TempDir()
    marksPath := filepath.Join(tmp, fmt.Sprintf("cas-marks-%d", os.Getpid()))
    defer os.Remove(marksPath)
    mw, err := NewMarkSetWriter(marksPath, 1<<20)
    if err != nil { return rep, err }
    for _, bk := range backups {
        // step 7 fail-closed: if any GetFile/parse fails, return without deleting
        if err := accumulateRefsForBackup(ctx, b, cp, bk, mw); err != nil {
            return rep, fmt.Errorf("cas-prune: cannot read live backup %q: %w", bk, err)
        }
    }
    if err := mw.Close(); err != nil { return rep, err }

    // step 8 + 9: stream compare against blob store, filter by grace
    mr, err := OpenMarkSetReader(marksPath); if err != nil { return rep, err }
    defer mr.Close()
    cands, err := SweepOrphans(ctx, b, cp, mr, grace, T0)
    if err != nil { return rep, err }
    rep.OrphanBlobsConsidered = uint64(len(cands))

    // step 10: metadata-orphan subtree sweep
    orphans, err := findMetadataOrphans(ctx, b, cp); if err != nil { return rep, err }
    if !opts.DryRun {
        for _, p := range orphans { _ = walkAndDelete(ctx, b, p) }
    }
    rep.MetadataOrphansSwept = len(orphans)

    // step 11: delete orphan blobs (parallel; skip if DryRun)
    if !opts.DryRun {
        n, err := deleteBlobs(ctx, b, cands, 32); if err != nil { return rep, err }
        rep.OrphansDeleted = uint64(n)
    } else {
        for _, c := range cands { fmt.Printf("would delete %s (modTime=%s)\n", c.Key, c.ModTime) }
    }

    // step 12 (deferred above): release marker
    rep.DurationSeconds = time.Since(start).Seconds()
    return rep, nil
}
```

- [ ] **Step 3: Iterate to green**

Run: `go test ./pkg/cas/ -run TestPrune -race -count=1 -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/cas/prune.go pkg/cas/prune_test.go
git commit -m "feat(cas): cas-prune mark-and-sweep with deferred marker release (§6.7)"
```

---

## Task 4: CLI binding for `cas-prune`

**Files:**
- Modify: `cmd/clickhouse-backup/cas_commands.go`
- Modify: `pkg/backup/cas_methods.go`

- [ ] **Step 1: Add the command**

In `cas_commands.go`, append:

```go
{
    Name:  "cas-prune",
    Usage: "Mark-and-sweep GC for the CAS layout (see docs/cas-design.md §6.7)",
    UsageText: "clickhouse-backup cas-prune [--dry-run] [--grace-hours N] [--abandon-days N] [--unlock]",
    Action: func(c *cli.Context) error {
        cfg := config.GetConfigFromCli(c)
        b := backup.NewBackuper(cfg)
        return b.CASPrune(c.Bool("dry-run"), c.Int("grace-hours"), c.Int("abandon-days"), c.Bool("unlock"))
    },
    Flags: append(rootFlags,
        cli.BoolFlag{Name: "dry-run", Usage: "Print candidates without deleting"},
        cli.IntFlag{Name: "grace-hours", Value: 0, Usage: "Override cas.grace_blob (hours)"},
        cli.IntFlag{Name: "abandon-days", Value: 0, Usage: "Override cas.abandon_threshold (days)"},
        cli.BoolFlag{Name: "unlock", Usage: "Delete a stranded prune.marker (operator escape hatch)"},
    ),
},
```

- [ ] **Step 2: `pkg/backup/cas_methods.go` `CASPrune` adapter**

```go
func (b *Backuper) CASPrune(dryRun bool, graceHours, abandonDays int, unlock bool) error {
    opts := cas.PruneOptions{DryRun: dryRun, Unlock: unlock}
    if graceHours > 0 { opts.GraceBlob = time.Duration(graceHours) * time.Hour }
    if abandonDays > 0 { opts.AbandonThreshold = time.Duration(abandonDays) * 24 * time.Hour }
    backend := newCASBackend(b.dst) // adapter created in Plan A
    rep, err := cas.Prune(b.ctx, backend, b.cfg.CAS, opts)
    if err != nil { return err }
    return cas.PrintPruneReport(rep, os.Stdout)
}
```

- [ ] **Step 3: Build, smoke-test**

Run: `go build ./cmd/clickhouse-backup && ./clickhouse-backup help cas-prune`
Expected: help text prints.

- [ ] **Step 4: Commit**

```bash
git add cmd/clickhouse-backup/cas_commands.go pkg/backup/cas_methods.go
git commit -m "feat(cas): cas-prune CLI binding"
```

---

## Task 5: Integration tests

**Files:**
- Create: `test/integration/cas_prune_test.go`

These are the §10.4 Phase 2 ship-gating tests against MinIO.

- [ ] **Step 1: `TestPruneGracePeriodRespected`**

1. Configure CAS with `grace_blob=1h`.
2. Upload `bk1`. Delete `bk1` (`cas-delete`). Some blobs are now orphans.
3. Immediately run `cas-prune`. Assert: no blobs deleted (all orphans younger than 1h).
4. Manipulate the bucket modtime to age them by 2h (or sleep, or use MinIO's ability to set object timestamps via admin API).
5. Re-run `cas-prune`. Assert: orphans deleted; total blob count = 0.

- [ ] **Step 2: `TestPruneMarkerReleasedOnError`**

1. Upload `bk1`.
2. Inject failure: delete one of `bk1`'s per-table archives between step 5 and step 6 of `cas-prune` (use a custom test wrapper around the backend that fails GetFile on a specific key after the run starts).
3. Run `cas-prune`. Expect non-zero exit + "cannot read live backup" error.
4. Assert: `cas/<c>/prune.marker` is GONE (deferred release ran on error path).

- [ ] **Step 3: `TestPruneSweepsAbandonedMarker`**

1. PutFile a fake `inprogress/bk_dead.marker` with a backdated timestamp (older than `abandon_threshold`).
2. Run `cas-prune`. Assert: marker swept; prune proceeds normally.

- [ ] **Step 4: `TestUploadAndDeleteRefuseDuringPrune`**

1. Manually PutFile `cas/<c>/prune.marker`.
2. Run `cas-upload bk2` → expect ErrPruneInProgress.
3. Run `cas-delete bk1` → expect ErrPruneInProgress.
4. Run `cas-prune --unlock`. Assert: marker gone.
5. Re-run upload/delete. Expect success.

- [ ] **Step 5: `TestPruneEndToEndDedupeReclaim`**

The most realistic scenario:
1. Upload three backups that share most blobs (mutation-heavy workload).
2. Delete the middle one.
3. `cas-prune` (with grace=0 for the test).
4. Assert: only the *unique* blobs of the middle backup are reclaimed; the shared ones survive because they're still referenced by the other two.

- [ ] **Step 6: Run integration suite**

Run: `go test ./test/integration/ -tags=integration -run TestPrune -v -timeout 30m`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add test/integration/cas_prune_test.go
git commit -m "test(cas): prune integration — grace, abandoned-marker, defer-release, end-to-end reclaim"
```

---

## Task 6: Operator runbook

**Files:**
- Create: `docs/cas-operator-runbook.md`

- [ ] **Step 1: Write the runbook**

Sections:
1. **When to run `cas-prune`** — quiet window, no concurrent CAS writes, weekly/daily depending on churn.
2. **What `cas-status` shows and how to read it** — backup count, blob count, in-progress markers, prune-marker state.
3. **Recovering from a stranded prune.marker** — `cas-status` shows it; verify no actual prune is running on any host; `cas-prune --unlock`.
4. **Recovering from a stranded inprogress marker** — `cas-status` shows abandoned candidates; either wait `abandon_threshold` (auto-swept by next prune) or delete manually.
5. **Recovering from `cas-verify` failures** — `cas-delete` the broken backup, recreate via `clickhouse-backup create + cas-upload`.
6. **Backend assumptions** — needs read-your-writes for objects + meaningful `LastModified`. AWS S3 / GCS / Azure / MinIO all qualify. Document the on-prem MinIO sandbox quirk if any.
7. **Monitoring suggestions** — alert if `cas-status` shows a prune.marker older than the expected prune duration; alert on accumulating abandoned markers.

- [ ] **Step 2: Commit**

```bash
git add docs/cas-operator-runbook.md
git commit -m "docs(cas): operator runbook for prune, status, recovery"
```

---

## Task 7: Update README + spec status

**Files:**
- Modify: `README.md`
- Modify: `docs/cas-design.md` (status line)

- [ ] **Step 1: README**

Update the "CAS layout" section added in Plan A: add `cas-prune` to the command list and link to `docs/cas-operator-runbook.md`.

- [ ] **Step 2: Spec status**

Change the spec's top status from "Design draft, pending implementation" to "Phase 1 + Phase 2 shipped" with the version-tag.

- [ ] **Step 3: Final test sweep**

Run: `go test ./... -race -count=1 && go vet ./...`
Run: `go test ./test/integration/ -tags=integration -run TestCAS -v`
Expected: green.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/cas-design.md
git commit -m "docs(cas): mark Phase 1 + 2 shipped; add prune runbook link"
```

---

## Spec coverage check

| Spec section / risk | Covered by task |
|---|---|
| §6.7 Algorithm | Task 3 |
| §6.7 Stale-marker recovery (`--unlock`) | Task 3 (Unlock branch) + Task 4 (CLI flag) + Task 5 step 4 (test) |
| §6.7 Race scenarios table | Task 3 covers each row via tests; Task 5 reproduces at integration level |
| §10.4 Phase 2 ship-gating tests | Task 5 |
| Risk R2 (GC race) | Task 5 step 4 (refusal proof) + Task 3 step 2 grace test |
| Risk R6 (LastModified semantics) | Task 6 (documented assumptions) |
| Risk R11 (orphan-blob latency) | Task 3 step 2 (grace test) |

Coverage gaps acknowledged: none. Plan B is small; Phase 3 hardening (per-blob resumable, performance benchmarks) is not in this plan and ships only if real workloads demand it.
