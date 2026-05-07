# CAS Phase 1 Follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the correctness gaps and verification debt accumulated during the cas-phase1 implementation, plus the three CLI/output consistency findings from the codebase consistency review.

**Architecture:** Two strands of work. (a) **Correctness/verification**: populate `TableMetadata.Query`/`UUID`/`Size`/`TotalBytes` from the local v1 metadata that `clickhouse-backup create` already writes (the headline fresh-host restore depends on these); add the missing `TestMutationDedup` integration test (the headline value-prop is currently unmeasured); run the integration suite end-to-end against MinIO. (b) **Hygiene/coverage**: tighten `cas-verify` failure classification, the `list remote` size column for CAS entries, the object-disk pre-flight, plus unit tests for the previously-untested wiring layer (`pkg/backup/cas_methods.go`, `pkg/cas/casstorage`).

**Tech Stack:** Go 1.26, urfave/cli v1, klauspost/compress/zstd, testify, the existing `pkg/cas/internal/{fakedst,testfixtures}` test infrastructure, the existing `test/integration` testcontainers harness.

**Spec inputs:**
- Final-review findings from the cas-phase1 close-out (numbered #4 through #14 in the conversation that produced this plan).
- Codebase consistency review against `master..cas-phase1` (Findings 1, 2, 3).
- Original spec: `docs/cas-design.md`.

---

## File structure

### Files modified
| Path | Why |
|---|---|
| `pkg/cas/upload.go` | Read local v1 `metadata/<db>/<table>.json` and merge `Query`/`UUID`/`Size`/`TotalBytes` into the uploaded TableMetadata. |
| `pkg/cas/upload_test.go` | New tests for the merge logic. |
| `pkg/cas/verify.go` | Distinguish "blob missing" from "stat-error". |
| `pkg/cas/verify_test.go` | New test for transient stat-error case. |
| `pkg/cas/list.go` | Replace `"???"` sentinel with `"(unknown)"`; tag the size column so mixed lists are operator-readable. |
| `pkg/cas/list_test.go` | Adjust assertions to the new sentinel string. |
| `pkg/cas/upload.go` (object-disk pre-flight) | Read disk types from the local backup snapshot, not live ClickHouse. |
| `pkg/cas/markers.go` | Decide markerTool fate (wire from `version` or delete `SetMarkerTool`). |
| `cmd/clickhouse-backup/cas_commands.go` | Hide `--data/-d` on `cas-download` (Finding 3). |
| `cmd/clickhouse-backup/main.go` | Wire `cas.SetMarkerTool` to the version string at startup (if D4 = wire). |
| `pkg/backup/cas_methods.go` | (No changes; new test file referenced below.) |
| `.gitignore` | Add `docs/clickhouse-backup-v2-design-state.md` (working artifact, not part of release). |

### Files created
| Path | Responsibility |
|---|---|
| `pkg/backup/cas_methods_test.go` | Unit tests for the `Backuper.CAS*` wiring layer using a stubbed Backuper + the existing `fakedst`. |
| `pkg/cas/casstorage/backend_storage_test.go` | Adapter tests proving `storage.ErrNotFound` becomes `(0, _, false, nil)` from `StatFile`. |
| `test/integration/cas_mutation_dedup_test.go` | The missing `TestMutationDedup` (value-prop). |

---

## Conventions

- Branch: `cas-phase1-followups`. Off the current `cas-phase1` HEAD.
- Commit prefix: `fix(cas)`, `feat(cas)`, `test(cas)`, `docs(cas)` per Conventional Commits.
- Test commands: `go test ./pkg/cas/... ./pkg/backup/... -race -count=1 -short` for unit; `go test -tags=integration ./test/integration/ -run TestCAS -v -timeout 30m` for integration.
- Open the PR via `gh pr create` after Task 1.

---

## Task 1: Open the PR for cas-phase1 (D1: actual final-step from the original Plan A)

**Files:**
- (None — pure git operation.)

- [ ] **Step 1: Push the branch and inspect ahead-of-master state**

```bash
git status                              # confirm clean
git push -u origin cas-phase1
git log --oneline master..cas-phase1 | wc -l   # expect ~27
```

- [ ] **Step 2: Open PR**

```bash
gh pr create --title "feat(cas): content-addressable backup layout (Phase 1)" --body "$(cat <<'EOF'
## Summary

Phase 1 of the content-addressable storage layout for `clickhouse-backup`. Adds six new commands (`cas-upload`, `cas-download`, `cas-restore`, `cas-delete`, `cas-verify`, `cas-status`) that use a content-addressed remote layout. Files are keyed by the CityHash128 already in each part's `checksums.txt`; identical content is stored once and reused across mutations and across backups; every backup is independently restorable (no `RequiredBackup` chain).

CAS commands run side-by-side with the existing `upload`/`download`/`restore` and use a separate top-level prefix in the bucket. The default is opt-in: `cas.enabled: false`.

Garbage collection (`cas-prune`) is Phase 2 (separate plan: `docs/superpowers/plans/2026-05-07-cas-phase2-prune.md`).

## Test plan

- [ ] `go test ./pkg/cas/... ./pkg/backup/... ./pkg/storage/... -race -count=1` passes
- [ ] `go vet ./...` clean
- [ ] `go vet -tags=integration ./test/integration/...` clean
- [ ] Manual smoke: `clickhouse-backup help cas-upload` (and the other five) prints help
- [ ] Integration: `TestCASRoundtrip` and `TestMutationDedup` against MinIO (see follow-up plan)

## Known follow-ups (separate plan)

See `docs/superpowers/plans/2026-05-07-cas-phase1-followups.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Capture the PR URL printed by `gh pr create`.

- [ ] **Step 3: Verify PR is up**

```bash
gh pr view --web   # or just: gh pr view
```

Expected: title and body render; CI starts (or queues).

---

## Task 2: Verify SkipPrefixes reconciliation between cherry-pick and manual edit (D2)

**Files:**
- Inspect: `pkg/cas/config.go`

- [ ] **Step 1: Inspect the current `SkipPrefixes` body**

```bash
grep -n -A 15 "func .* SkipPrefixes" pkg/cas/config.go
```

Expected: ONE definition. If two definitions appear → conflict slipped through; remove the older one and re-run tests.

- [ ] **Step 2: Compare against the cherry-picked version**

```bash
git show 5bb0a356 -- pkg/cas/config.go | head -40
```

Confirm the function body on disk matches what 5bb0a356 added (one definition, returns `nil` when disabled, returns `[]string{rp}` when enabled with non-empty `rp`, returns `nil` when `rp` ends up empty after normalization).

- [ ] **Step 3: If they drifted, reconcile**

If on-disk version is missing the empty-`rp` guard, restore it:

```go
func (c Config) SkipPrefixes() []string {
    if !c.Enabled {
        return nil
    }
    rp := c.RootPrefix
    if rp != "" && !strings.HasSuffix(rp, "/") {
        rp += "/"
    }
    if rp == "" {
        return nil
    }
    return []string{rp}
}
```

Run `go test ./pkg/cas/... -count=1`. Commit only if changes were needed:

```bash
git add pkg/cas/config.go
git commit -m "fix(cas): reconcile SkipPrefixes after cherry-pick + manual edit"
```

If no changes needed: skip the commit; document the no-op verification by checking off the task.

---

## Task 3: Populate `TableMetadata.Query`/`UUID`/`Size`/`TotalBytes` in cas-upload

This is the load-bearing correctness fix. Without it, `cas-restore` on a fresh host can't recreate tables — the v1 restore reads `Query` from the per-table JSON to issue `CREATE TABLE`, and CAS uploads currently leave it empty.

The local backup directory `clickhouse-backup create` produces already contains `<root>/<name>/metadata/<db>/<table>.json` with a fully-populated v1 `TableMetadata`. cas-upload just needs to read those files and merge the schema fields into what it uploads.

**Files:**
- Modify: `pkg/cas/upload.go` — `uploadTableJSONs` and the `tablePlan` struct.
- Modify: `pkg/cas/upload_test.go` — new tests verifying merged fields.
- Modify: `pkg/cas/internal/testfixtures/localbackup.go` — write a synthetic `metadata/<db>/<table>.json` so tests have something to merge from.

- [ ] **Step 1: Extend the test fixture builder to write a v1 per-table JSON**

Add to `pkg/cas/internal/testfixtures/localbackup.go`:

```go
import (
    "encoding/json"
    "github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

// PartSpec gains a TableMeta field so callers can specify the per-table
// JSON content that 'clickhouse-backup create' would have produced.
type PartSpec struct {
    Disk, DB, Table, Name string
    Files                 []FileSpec
    // TableMeta is optional; if zero-value, Build still writes a minimal
    // v1 metadata/<db>/<table>.json so that cas-upload's merge logic has
    // something to read.
    TableMeta metadata.TableMetadata
}
```

In `Build`, after writing the part files for each `(disk, db, table)` group, write the per-table JSON ONCE per `(db, table)` (deduped across disks):

```go
seen := map[string]bool{}
for _, p := range parts {
    key := p.DB + "." + p.Table
    if seen[key] { continue }
    seen[key] = true
    tm := p.TableMeta
    if tm.Database == "" { tm.Database = p.DB }
    if tm.Table == ""    { tm.Table = p.Table }
    if tm.Query == "" {
        tm.Query = "CREATE TABLE " + p.DB + "." + p.Table + " (id UInt64) ENGINE=MergeTree ORDER BY id"
    }
    if tm.UUID == "" {
        tm.UUID = "00000000-0000-0000-0000-000000000000"
    }
    metaDir := filepath.Join(root, "metadata", p.DB)
    if err := os.MkdirAll(metaDir, 0o755); err != nil { t.Fatal(err) }
    body, err := json.MarshalIndent(&tm, "", "\t")
    if err != nil { t.Fatal(err) }
    metaPath := filepath.Join(metaDir, p.Table+".json")
    if err := os.WriteFile(metaPath, body, 0o644); err != nil { t.Fatal(err) }
}
```

- [ ] **Step 2: Write the failing test**

In `pkg/cas/upload_test.go`, add:

```go
func TestUpload_MergesSchemaFieldsFromLocalV1Metadata(t *testing.T) {
    parts := []testfixtures.PartSpec{
        {
            Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
            Files: []testfixtures.FileSpec{
                {Name: "columns.txt", Size: 8, HashLow: 1, HashHigh: 0},
            },
            TableMeta: metadata.TableMetadata{
                Database: "db1", Table: "t1",
                Query:      "CREATE TABLE db1.t1 (id UInt64) ENGINE=MergeTree ORDER BY id",
                UUID:       "deadbeef-0000-0000-0000-000000000001",
                TotalBytes: 12345,
            },
        },
    }
    src := testfixtures.Build(t, parts)
    f := fakedst.New()
    cfg := testCfg(100)
    if _, err := cas.Upload(context.Background(), f, cfg, "bk1", cas.UploadOptions{
        LocalBackupDir: src.Root,
    }); err != nil {
        t.Fatal(err)
    }

    // Read the uploaded per-table metadata.json from the fake backend.
    rc, err := f.GetFile(context.Background(), cas.TableMetaPath(cfg.ClusterPrefix(), "bk1", "db1", "t1"))
    if err != nil { t.Fatal(err) }
    body, _ := io.ReadAll(rc); rc.Close()
    var got metadata.TableMetadata
    if err := json.Unmarshal(body, &got); err != nil { t.Fatal(err) }

    if got.Query == "" {
        t.Error("uploaded TableMetadata.Query is empty — fresh-host restore would fail")
    }
    if got.UUID != "deadbeef-0000-0000-0000-000000000001" {
        t.Errorf("UUID: got %q want %q", got.UUID, "deadbeef-0000-0000-0000-000000000001")
    }
    if got.TotalBytes != 12345 {
        t.Errorf("TotalBytes: got %d want 12345", got.TotalBytes)
    }
}
```

- [ ] **Step 3: Run the test to confirm it fails**

```bash
go test ./pkg/cas/ -run TestUpload_MergesSchemaFieldsFromLocalV1Metadata -v
```

Expected: FAIL with `Query` empty.

- [ ] **Step 4: Implement the merge in `pkg/cas/upload.go`**

Add a helper near `planUpload`:

```go
// readLocalTableMetadata reads <root>/metadata/<db>/<table>.json that
// 'clickhouse-backup create' wrote. Returns a zero-value TableMetadata
// + nil error if the file is missing (older create flow or malformed
// local layout); cas-upload then ships an empty schema, which is fine
// for tests but degrades fresh-host restore — log a warning.
func readLocalTableMetadata(root, db, table string) (metadata.TableMetadata, error) {
    p := filepath.Join(root, "metadata", db, table+".json")
    f, err := os.Open(p)
    if err != nil {
        if os.IsNotExist(err) {
            log.Warn().Str("path", p).Msg("cas: local v1 per-table metadata missing; uploaded schema fields will be empty")
            return metadata.TableMetadata{}, nil
        }
        return metadata.TableMetadata{}, fmt.Errorf("cas: open %s: %w", p, err)
    }
    defer f.Close()
    var tm metadata.TableMetadata
    if err := json.NewDecoder(f).Decode(&tm); err != nil {
        return metadata.TableMetadata{}, fmt.Errorf("cas: parse %s: %w", p, err)
    }
    return tm, nil
}
```

In `uploadTableJSONs` (around `pkg/cas/upload.go:560-605`), before writing the JSON for each `(db, table)`, merge the schema fields:

```go
// Existing code builds a TableMetadata from plan with Database, Table, Parts.
// After that block:
local, err := readLocalTableMetadata(plan.localRoot, dt.DB, dt.Table)
if err != nil {
    return fmt.Errorf("cas: read local table metadata for %s.%s: %w", dt.DB, dt.Table, err)
}
tm.Query = local.Query
tm.UUID = local.UUID
tm.TotalBytes = local.TotalBytes
tm.Size = local.Size
tm.DependenciesTable = local.DependenciesTable
tm.DependenciesDatabase = local.DependenciesDatabase
tm.Mutations = local.Mutations
```

For this you'll need `plan.localRoot` populated. Add a `localRoot string` field to `uploadPlan` and set it from `planUpload(root, ...)` (the existing `root` parameter).

- [ ] **Step 5: Run the test to confirm it passes**

```bash
go test ./pkg/cas/ -run TestUpload_MergesSchemaFieldsFromLocalV1Metadata -v
```

Expected: PASS. Also run the full `pkg/cas/...` suite to confirm no regressions.

- [ ] **Step 6: Verify the corresponding download path consumes the merged fields correctly**

`cas.Download` already reads the per-table JSON straight from remote and writes it to disk; v1 restore reads that. No download-side change needed. But add a regression test:

```go
func TestDownload_PreservesSchemaFields(t *testing.T) {
    parts := []testfixtures.PartSpec{{
        Disk: "default", DB: "db1", Table: "t1", Name: "all_1_1_0",
        Files: []testfixtures.FileSpec{{Name: "columns.txt", Size: 8, HashLow: 1, HashHigh: 0}},
        TableMeta: metadata.TableMetadata{
            Database: "db1", Table: "t1",
            Query: "CREATE TABLE db1.t1 ENGINE=Memory",
            UUID:  "abc",
        },
    }}
    _, _, _, root := uploadAndDownload(t, parts, "b1", cas.DownloadOptions{})
    body, _ := os.ReadFile(filepath.Join(root, "b1", "metadata", "db1", "t1.json"))
    var got metadata.TableMetadata
    if err := json.Unmarshal(body, &got); err != nil { t.Fatal(err) }
    if got.Query == "" || got.UUID == "" {
        t.Errorf("downloaded JSON lost schema fields: %+v", got)
    }
}
```

- [ ] **Step 7: Commit**

```bash
git add pkg/cas/upload.go pkg/cas/upload_test.go pkg/cas/download_test.go pkg/cas/internal/testfixtures/localbackup.go
git commit -m "fix(cas): merge Query/UUID/Size/TotalBytes from local v1 metadata into uploaded TableMetadata

Without these fields the v1 restore handoff cannot recreate tables on a
fresh host (CREATE TABLE statement is empty). Read the per-table JSON
that 'clickhouse-backup create' already wrote to disk and merge into the
uploaded TableMetadata. Add a download-side regression test that the
schema fields survive the round-trip."
```

---

## Task 4: Add `TestMutationDedup` integration test (the headline value-prop)

**Files:**
- Create: `test/integration/cas_mutation_dedup_test.go`

- [ ] **Step 1: Write the test**

```go
//go:build integration

package main

import (
    "fmt"
    "strings"
    "testing"

    "github.com/stretchr/testify/require"
)

// TestCASMutationDedup verifies the headline value-prop:
// after an ALTER TABLE ... UPDATE that rewrites a single column,
// the second cas-upload should transfer dramatically fewer bytes than
// the first because all unmutated column files are byte-identical and
// dedup against the existing blob store.
func TestCASMutationDedup(t *testing.T) {
    env := NewTestEnvironment(t)
    defer env.Cleanup(t, require.New(t))
    r := require.New(t)
    env.casBootstrap(t, r) // helper from cas_test.go

    // Wide table with two columns so we can mutate one and leave the other
    // unchanged.
    env.runChQuery(t, r, "CREATE DATABASE IF NOT EXISTS dedup")
    env.runChQuery(t, r, `CREATE TABLE dedup.t (id UInt64, payload String, marker String)
        ENGINE=MergeTree ORDER BY id
        SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0`)
    // 100k rows; payload is the "big" column; marker is the "small" column we'll mutate.
    env.runChQuery(t, r, `INSERT INTO dedup.t SELECT number, repeat('x', 1024), 'orig'
        FROM numbers(100000)`)
    env.runChQuery(t, r, "OPTIMIZE TABLE dedup.t FINAL")

    // First backup — uploads everything fresh.
    env.dockerExec(t, r, "clickhouse-backup", "create", "bk1")
    out1 := env.dockerExec(t, r, "clickhouse-backup", "--config", "/tmp/config-cas.yml", "cas-upload", "bk1")
    bytes1 := parseBytesUploaded(t, out1)

    // Mutate ONLY the marker column; payload is hardlinked unchanged.
    env.runChQuery(t, r, "ALTER TABLE dedup.t UPDATE marker = 'after' WHERE 1 SETTINGS mutations_sync=2")
    env.runChQuery(t, r, "OPTIMIZE TABLE dedup.t FINAL")

    env.dockerExec(t, r, "clickhouse-backup", "create", "bk2")
    out2 := env.dockerExec(t, r, "clickhouse-backup", "--config", "/tmp/config-cas.yml", "cas-upload", "bk2")
    bytes2 := parseBytesUploaded(t, out2)

    // Headline assertion: second upload is at most 25% of the first.
    // (Real-world ratio for a single mutated column out of N is ~1/N, but we
    // pick a loose bound to absorb compression-blob overhead and avoid flake.)
    if bytes2 >= bytes1/4 {
        t.Fatalf("mutation dedup failed: bk1 uploaded %d bytes, bk2 uploaded %d bytes (expected bk2 << bk1)", bytes1, bytes2)
    }

    // Clean up:
    env.dockerExec(t, r, "clickhouse-backup", "--config", "/tmp/config-cas.yml", "cas-delete", "bk1")
    env.dockerExec(t, r, "clickhouse-backup", "--config", "/tmp/config-cas.yml", "cas-delete", "bk2")
    env.runChQuery(t, r, "DROP DATABASE dedup SYNC")
}

// parseBytesUploaded extracts the bytes-uploaded value from cas-upload's
// printed summary. Format (from pkg/backup/cas_methods.go):
//
//   cas-upload: bk1
//     ...
//     uploaded now : N blobs, X.Y MiB
//     ...
//
// We parse the "uploaded now" line.
func parseBytesUploaded(t *testing.T, out string) int64 {
    t.Helper()
    for _, line := range strings.Split(out, "\n") {
        if !strings.Contains(line, "uploaded now") { continue }
        // Form: "    uploaded now : 1234 blobs, 5.6 MiB"
        idx := strings.Index(line, ", ")
        if idx < 0 { continue }
        rest := strings.TrimSpace(line[idx+2:])
        return humanBytesToInt64(t, rest)
    }
    t.Fatalf("could not parse 'uploaded now' line from cas-upload output:\n%s", out)
    return 0
}

func humanBytesToInt64(t *testing.T, s string) int64 {
    t.Helper()
    var v float64
    var unit string
    if _, err := fmt.Sscanf(s, "%f %s", &v, &unit); err != nil {
        t.Fatalf("parse human bytes %q: %v", s, err)
    }
    mult := int64(1)
    switch strings.ToUpper(unit) {
    case "B":   mult = 1
    case "KIB": mult = 1024
    case "MIB": mult = 1024 * 1024
    case "GIB": mult = 1024 * 1024 * 1024
    default:    t.Fatalf("unknown unit %q in %q", unit, s)
    }
    return int64(v * float64(mult))
}
```

(`runChQuery`, `dockerExec`, `casBootstrap` already exist in `test/integration/cas_test.go`. Reuse them; if any signature differs, follow the local convention.)

- [ ] **Step 2: Verify the test compiles**

```bash
go test -c -tags=integration -o /dev/null ./test/integration/
```

Expected: clean.

- [ ] **Step 3: Verify go vet is clean with the integration tag**

```bash
go vet -tags=integration ./test/integration/...
```

- [ ] **Step 4: Commit**

```bash
git add test/integration/cas_mutation_dedup_test.go
git commit -m "test(cas): mutation-dedup integration test (the headline value-prop)"
```

---

## Task 5: Run the integration suite end-to-end against MinIO

**Files:**
- (None — pure execution.)

This task validates Tasks 3 and 4 together. It produces the first real signal that cas-upload + cas-restore work as a system. Budget: 30–60 minutes wall-clock for the harness to spin up + run.

- [ ] **Step 1: Run integration tests, capture the log**

```bash
RUN_PARALLEL=1 go test -tags=integration ./test/integration/ -run 'TestCAS' -v -timeout 60m 2>&1 | tee /tmp/cas-integration.log
```

Expected: `TestCASRoundtrip`, `TestCASCrossModeGuards`, `TestCASVerify`, `TestCASMutationDedup` all PASS.

- [ ] **Step 2: If any test fails, diagnose**

For `TestCASRoundtrip` failure:
- Most likely cause: schema fields still missing → re-verify Task 3 landed.
- If Task 3 landed correctly: read the failure log; the most common remaining issue is `ATTACH PART` failing because part metadata diverges (sort order, projection sub-parts).

For `TestCASMutationDedup` failure:
- If `bytes2 >= bytes1/4`: the dedup is happening at less than expected. Print the per-blob diff (cold-list size before vs after the second upload). The threshold `1/4` may be too tight for a small dataset; loosen to `1/2` and document.

For `TestCASCrossModeGuards` failure:
- Check `pkg/storage/general.go` BackupList signature; if regressed, Task 18 cherry-pick may have unwound.

- [ ] **Step 3: Save the log as a PR comment**

```bash
gh pr comment cas-phase1 --body "$(cat <<'EOF'
Integration run on cas-phase1-followups:

\`\`\`
$(tail -50 /tmp/cas-integration.log)
\`\`\`
EOF
)"
```

- [ ] **Step 4: Commit the log capture as a record (optional)**

If a log artifact is useful for the PR record, attach it as `docs/superpowers/runs/2026-05-07-cas-integration.log` and commit. Otherwise just reference the comment.

---

## Task 6: cas-verify — distinguish missing blob from stat-error

**Files:**
- Modify: `pkg/cas/verify.go` — `headAllInParallel` and `VerifyFailure`.
- Modify: `pkg/cas/errors.go` — add `VerifyFailureKindStatError` constant or extend `VerifyFailure.Kind`.
- Modify: `pkg/cas/verify_test.go` — new test for stat-error case.

- [ ] **Step 1: Write the failing test**

```go
// stallingBackend wraps fakedst and forces StatFile to return a non-nil
// error for one specific key — simulating a transient network hiccup.
type stallingBackend struct {
    cas.Backend
    failKey string
}

func (s *stallingBackend) StatFile(ctx context.Context, key string) (int64, time.Time, bool, error) {
    if key == s.failKey {
        return 0, time.Time{}, false, errors.New("simulated network error")
    }
    return s.Backend.StatFile(ctx, key)
}

func TestVerify_StatErrorIsNotMissing(t *testing.T) {
    f := fakedst.New()
    cfg := testCfg(100)
    ctx := context.Background()
    parts := []testfixtures.PartSpec{{
        Disk: "default", DB: "db", Table: "t", Name: "all_1_1_0",
        Files: []testfixtures.FileSpec{
            {Name: "data.bin", Size: 2048, HashLow: 7, HashHigh: 7},
            {Name: "columns.txt", Size: 8, HashLow: 8, HashHigh: 8},
        },
    }}
    src := testfixtures.Build(t, parts)
    if _, err := cas.Upload(ctx, f, cfg, "bk", cas.UploadOptions{LocalBackupDir: src.Root}); err != nil {
        t.Fatal(err)
    }
    // Force StatFile failure on the data.bin blob's path.
    target := cas.BlobPath(cfg.ClusterPrefix(), cas.Hash128{Low: 7, High: 7})
    sb := &stallingBackend{Backend: f, failKey: target}

    var out bytes.Buffer
    res, err := cas.Verify(ctx, sb, cfg, "bk", cas.VerifyOptions{}, &out)
    if !errors.Is(err, cas.ErrVerifyFailures) {
        t.Fatalf("expected ErrVerifyFailures, got %v", err)
    }
    if len(res.Failures) != 1 {
        t.Fatalf("got %d failures, want 1", len(res.Failures))
    }
    if res.Failures[0].Kind != "stat_error" {
        t.Errorf("Kind: got %q want \"stat_error\" (NOT \"missing\" — that would mislead operators into recreating a healthy backup)", res.Failures[0].Kind)
    }
}
```

- [ ] **Step 2: Run, confirm it fails**

```bash
go test ./pkg/cas/ -run TestVerify_StatErrorIsNotMissing -v
```

Expected: FAIL with `Kind: got "missing" want "stat_error"`.

- [ ] **Step 3: Implement the fix in `pkg/cas/verify.go`**

Find `headAllInParallel`. The existing branch is roughly:

```go
size, _, exists, err := b.StatFile(ctx, blob.Path)
if err != nil || !exists {
    failures = append(failures, VerifyFailure{Kind: "missing", Path: blob.Path, Want: blob.Size})
    return
}
```

Change to:

```go
size, _, exists, err := b.StatFile(ctx, blob.Path)
if err != nil {
    failures = append(failures, VerifyFailure{Kind: "stat_error", Path: blob.Path, Want: blob.Size, Err: err.Error()})
    return
}
if !exists {
    failures = append(failures, VerifyFailure{Kind: "missing", Path: blob.Path, Want: blob.Size})
    return
}
if int64(blob.Size) != size {
    failures = append(failures, VerifyFailure{Kind: "size_mismatch", Path: blob.Path, Want: blob.Size, Got: size})
}
```

Add `Err string \`json:"err,omitempty"\`` to `VerifyFailure`.

- [ ] **Step 4: Confirm test passes**

```bash
go test ./pkg/cas/ -run TestVerify -race -count=1 -v
```

Expected: all verify tests pass; the new test reports kind `"stat_error"`.

- [ ] **Step 5: Commit**

```bash
git add pkg/cas/verify.go pkg/cas/verify_test.go
git commit -m "fix(cas): cas-verify distinguishes stat-error from missing-blob

A transient StatFile error was being reported as 'missing', tempting
operators to discard a healthy backup. Surface the underlying error
under a new failure Kind 'stat_error' so monitoring can react
differently from a true missing-blob signal."
```

---

## Task 7: list remote — replace `"???"` sentinel; tag CAS sizes (Finding 1)

**Files:**
- Modify: `pkg/cas/list.go` (or `pkg/backup/list.go` `collectRemoteCASBackups`)
- Modify: `pkg/cas/list_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestCollectRemoteCASBackups_UnknownSizeRendersClearly(t *testing.T) {
    // Construct a CAS metadata.json on disk that doesn't include sizing,
    // call collectRemoteCASBackups (via cas.ListRemoteCAS + the helper),
    // assert the resulting Description / Size column does NOT contain "???".

    f := fakedst.New()
    cfg := testCfg(100)
    cfg.Enabled = true
    cfg.ClusterID = "c1"
    ctx := context.Background()
    // Put a minimal metadata.json with no size fields.
    body := []byte(`{"backup_name":"empty","data_format":"directory","cas":{"layout_version":1,"inline_threshold":1024,"cluster_id":"c1"}}`)
    err := f.PutFile(ctx, cas.MetadataJSONPath(cfg.ClusterPrefix(), "empty"), io.NopCloser(bytes.NewReader(body)), int64(len(body)))
    if err != nil { t.Fatal(err) }

    entries, err := cas.ListRemoteCAS(ctx, f, cfg)
    if err != nil { t.Fatal(err) }
    if len(entries) != 1 { t.Fatalf("got %d entries, want 1", len(entries)) }
    if strings.Contains(entries[0].Description, "???") || strings.Contains(entries[0].Size, "???") {
        t.Errorf("entry uses '???' sentinel; want '(unknown)' or omitted: %+v", entries[0])
    }
}
```

(Adjust to match the actual `CASListEntry` shape.)

- [ ] **Step 2: Run, confirm it fails**

```bash
go test ./pkg/cas/ -run TestCollectRemoteCASBackups_UnknownSizeRendersClearly -v
```

Expected: FAIL because the current code uses `"???"`.

- [ ] **Step 3: Fix in `pkg/cas/list.go` (or the renderer)**

Find the line that produces `"???"`. Replace with `"(unknown)"`. While there, prefix the size with `[CAS] total:` so a mixed list reads cleanly:

```go
size := "(unknown)"
if e.SizeBytes > 0 {
    size = utils.FormatBytes(uint64(e.SizeBytes))
}
description := "[CAS] total:" + size
```

- [ ] **Step 4: Update the test so the assertion matches**

Adjust the test from Step 1 to assert `description` contains `"(unknown)"` and `"[CAS]"`.

- [ ] **Step 5: Confirm tests pass**

```bash
go test ./pkg/cas/ -race -count=1 -v
```

- [ ] **Step 6: Commit**

```bash
git add pkg/cas/list.go pkg/cas/list_test.go
git commit -m "fix(cas): replace '???' sentinel with '(unknown)' in list remote; tag CAS sizes

Operators reading a mixed v1+CAS list output couldn't distinguish
'data missing' from 'display broken'. '(unknown)' is self-evident.
Prefix CAS sizes with '[CAS] total:' so the format difference vs v1's
8-category breakdown is operator-explained, not surprising."
```

---

## Task 8: cas-upload — object-disk pre-flight reads the snapshot, not live ClickHouse

**Files:**
- Modify: `pkg/backup/cas_methods.go` `chTablesAndDisks` — read the local backup's `metadata/<db>/<table>.json` files instead of querying ClickHouse for the table list.

- [ ] **Step 1: Write the failing test (in `pkg/backup/cas_methods_test.go` — created here)**

This test exercises the wiring layer; it requires a mock Backuper. If standing up that infrastructure in this task is too costly, skip the unit test and rely on the integration test below — but document the gap.

Simpler: write an integration-style test in `test/integration/`:

```go
//go:build integration

func TestCASPreflight_UsesSnapshotNotLiveDisks(t *testing.T) {
    env := NewTestEnvironment(t)
    defer env.Cleanup(t, require.New(t))
    r := require.New(t)
    env.casBootstrap(t, r)

    // Create a table on a local disk; create a backup; then ALTER it onto
    // an object disk (this requires an s3-backed disk being configured in
    // the harness). cas-upload of the original backup should SUCCEED:
    // the pre-flight should reject only what was on object disk AT BACKUP
    // TIME, not what's there now.

    env.runChQuery(t, r, "CREATE DATABASE preflight")
    env.runChQuery(t, r, `CREATE TABLE preflight.t (id UInt64) ENGINE=MergeTree
        ORDER BY id SETTINGS storage_policy='default'`)
    env.runChQuery(t, r, "INSERT INTO preflight.t SELECT number FROM numbers(100)")
    env.dockerExec(t, r, "clickhouse-backup", "create", "snap")

    // Now move the table onto the object-disk policy (assumes 's3' policy
    // exists in the harness CH config — if not, document this test as
    // skipped under 'requires s3 storage policy').
    env.runChQuery(t, r, "ALTER TABLE preflight.t MODIFY SETTING storage_policy='s3'")
    // Wait for parts to migrate (best-effort; harness-specific).

    out := env.dockerExec(t, r, "clickhouse-backup", "--config", "/tmp/config-cas.yml", "cas-upload", "snap")
    r.NotContains(out, "object-disk", "pre-flight should not refuse a backup taken before the table moved to object disk")
}
```

If the harness doesn't have an s3-backed storage policy, gate the test with `t.Skip("requires s3 storage policy in test harness")`.

- [ ] **Step 2: Implement the snapshot-reading pre-flight**

In `pkg/backup/cas_methods.go` `chTablesAndDisks` (or wherever pre-flight inputs are gathered for `cas.Upload`):

Today (paraphrased):
```go
// Live ClickHouse:
tables, _ := b.ch.GetTables(ctx, "")
disks, _ := b.ch.GetDisks(ctx, true)
```

Change to read from the local backup's metadata directory:

```go
// Snapshot from the local backup directory: each metadata/<db>/<table>.json
// already records the DataPaths the table had at create-time. The disks
// information still has to come from ClickHouse (system.disks), since the
// disk *types* don't change with the table — but match by path prefix
// against the snapshot's DataPaths, not against live tables.

snapshotTables, err := readSnapshotTables(localBackupDir)
if err != nil { return nil, nil, err }
disks, err := b.ch.GetDisks(ctx, true)
if err != nil { return nil, nil, err }

return snapshotTables, mapDisksToCASDiskInfo(disks), nil
```

`readSnapshotTables` walks `localBackupDir/metadata/*/*.json`, parses each `metadata.TableMetadata`, returns `[]cas.TableInfo`.

- [ ] **Step 3: Run unit tests**

```bash
go test ./pkg/cas/... ./pkg/backup/... -race -count=1
```

- [ ] **Step 4: Run the integration test (best-effort)**

```bash
go test -tags=integration ./test/integration/ -run TestCASPreflight -v -timeout 30m
```

Expected: PASS (or SKIP with documented reason).

- [ ] **Step 5: Commit**

```bash
git add pkg/backup/cas_methods.go test/integration/cas_preflight_test.go
git commit -m "fix(cas): object-disk pre-flight reads the backup snapshot, not live ClickHouse

A user who ALTERs a table onto an object disk between 'create' and
'cas-upload' would get a false refusal under the old logic; one who
moves a table OFF object disk would get a false acceptance. Read the
table list from the local backup's metadata/<db>/<table>.json files
(written by 'create') so the pre-flight reflects what's actually in
the backup."
```

---

## Task 9: Unit tests for `pkg/backup/cas_methods.go`

**Files:**
- Create: `pkg/backup/cas_methods_test.go`

The 410-LOC wiring layer has no direct coverage. Without a mock Backuper, full unit testing is heavy. **Aim for narrow tests that exercise the pure-logic helpers** (`splitTablePattern`, `chTablesAndDisks` snapshot mode after Task 8, the failure paths in `ensureCAS`).

- [ ] **Step 1: Test `splitTablePattern` directly**

```go
package backup

import (
    "reflect"
    "testing"
)

func TestSplitTablePattern(t *testing.T) {
    cases := []struct {
        in   string
        want []string
    }{
        {"", nil},
        {"db.t", []string{"db.t"}},
        {"db1.t1, db2.t2", []string{"db1.t1", "db2.t2"}},
        {"  db.t  ", []string{"db.t"}}, // whitespace
    }
    for _, c := range cases {
        got := splitTablePattern(c.in)
        if !reflect.DeepEqual(got, c.want) {
            t.Errorf("splitTablePattern(%q) = %v, want %v", c.in, got, c.want)
        }
    }
}
```

- [ ] **Step 2: Test `ensureCAS` refusal when CAS disabled**

```go
func TestEnsureCAS_RefusesWhenDisabled(t *testing.T) {
    cfg := config.DefaultConfig()
    cfg.CAS.Enabled = false
    b := &Backuper{cfg: cfg}
    _, _, err := b.ensureCAS(context.Background(), "anyname")
    if err == nil || !strings.Contains(err.Error(), "cas.enabled=false") {
        t.Fatalf("got %v", err)
    }
}
```

(`Backuper` may have unexported fields that resist construction in a `_test.go`. If so, place this test in `package backup` (white-box) and zero-value the struct fields it doesn't touch.)

- [ ] **Step 3: Test `chTablesAndDisks` snapshot reader (after Task 8)**

```go
func TestChTablesAndDisks_FromSnapshot(t *testing.T) {
    tmp := t.TempDir()
    metaDir := filepath.Join(tmp, "metadata", "db1")
    if err := os.MkdirAll(metaDir, 0o755); err != nil { t.Fatal(err) }
    body := `{"database":"db1","table":"t1","query":"CREATE TABLE db1.t1 (id UInt64) ENGINE=MergeTree ORDER BY id"}`
    if err := os.WriteFile(filepath.Join(metaDir, "t1.json"), []byte(body), 0o644); err != nil { t.Fatal(err) }

    tables, err := readSnapshotTables(tmp)
    if err != nil { t.Fatal(err) }
    if len(tables) != 1 || tables[0].Database != "db1" || tables[0].Name != "t1" {
        t.Fatalf("got %+v", tables)
    }
}
```

- [ ] **Step 4: Run all three**

```bash
go test ./pkg/backup/ -run "TestSplitTablePattern|TestEnsureCAS|TestChTablesAndDisks" -race -count=1 -v
```

- [ ] **Step 5: Commit**

```bash
git add pkg/backup/cas_methods_test.go
git commit -m "test(cas): unit tests for cas_methods helpers (splitTablePattern, ensureCAS, snapshot reader)"
```

---

## Task 10: Unit tests for `pkg/cas/casstorage` adapter

**Files:**
- Create: `pkg/cas/casstorage/backend_storage_test.go`

The adapter maps `storage.ErrNotFound` → `(0, _, false, nil)` from `StatFile`. If that mapping is wrong, every cas-* command silently misbehaves.

- [ ] **Step 1: Write a fake `*storage.BackupDestination`**

(Or a minimal stub that exposes the methods the adapter calls.)

```go
package casstorage

import (
    "context"
    "errors"
    "io"
    "testing"
    "time"

    "github.com/Altinity/clickhouse-backup/v2/pkg/storage"
)

type fakeBD struct {
    statErr error
    size    int64
    modTime time.Time
}

func (f *fakeBD) StatFile(ctx context.Context, key string) (storage.RemoteFile, error) {
    if f.statErr != nil { return nil, f.statErr }
    return fakeRemoteFile{size: f.size, modTime: f.modTime}, nil
}
func (f *fakeBD) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) { return nil, nil }
func (f *fakeBD) PutFile(ctx context.Context, key string, r io.ReadCloser, sz int64) error { return nil }
func (f *fakeBD) DeleteFile(ctx context.Context, key string) error { return nil }
func (f *fakeBD) Walk(ctx context.Context, prefix string, recursive bool, fn func(context.Context, storage.RemoteFile) error) error { return nil }

type fakeRemoteFile struct {
    size    int64
    modTime time.Time
}
func (f fakeRemoteFile) Size() int64            { return f.size }
func (f fakeRemoteFile) Name() string           { return "x" }
func (f fakeRemoteFile) LastModified() time.Time { return f.modTime }
```

(If the real `*storage.BackupDestination` is not subclassable via interface, refactor `casstorage` to take an interface — small change, big test win. Defer if it requires opening up `pkg/storage`'s API.)

- [ ] **Step 2: Test the not-found mapping**

```go
func TestStorageBackend_StatFile_NotFoundMapsToExistsFalse(t *testing.T) {
    fbd := &fakeBD{statErr: storage.ErrNotFound}
    sb := &storageBackend{bd: anyBackupDestinationFrom(fbd)}
    sz, _, exists, err := sb.StatFile(context.Background(), "anykey")
    if err != nil { t.Fatalf("err: %v", err) }
    if exists { t.Error("exists should be false on ErrNotFound") }
    if sz != 0 { t.Errorf("size: %d", sz) }
}

func TestStorageBackend_StatFile_OtherErrorPropagates(t *testing.T) {
    fbd := &fakeBD{statErr: errors.New("network broken")}
    sb := &storageBackend{bd: anyBackupDestinationFrom(fbd)}
    _, _, _, err := sb.StatFile(context.Background(), "anykey")
    if err == nil { t.Error("non-not-found error must propagate") }
}
```

If wrapping `fakeBD` into a `*storage.BackupDestination` is hard, refactor `storageBackend` to take an interface field instead of a concrete pointer:

```go
type bdInterface interface {
    StatFile(ctx context.Context, key string) (storage.RemoteFile, error)
    GetFileReader(ctx context.Context, key string) (io.ReadCloser, error)
    PutFile(ctx context.Context, key string, r io.ReadCloser, sz int64) error
    DeleteFile(ctx context.Context, key string) error
    Walk(ctx context.Context, prefix string, recursive bool, fn func(context.Context, storage.RemoteFile) error) error
}
type storageBackend struct{ bd bdInterface }
```

`*storage.BackupDestination` already satisfies this implicitly.

- [ ] **Step 3: Run, commit**

```bash
go test ./pkg/cas/casstorage/ -race -count=1 -v
git add pkg/cas/casstorage/
git commit -m "test(cas): unit tests for casstorage adapter (ErrNotFound mapping, error propagation)"
```

---

## Task 11: Decide the fate of `markerTool` (D4)

**Files:**
- Modify: `pkg/cas/markers.go` and `cmd/clickhouse-backup/main.go` — choose ONE branch below.

The current `markerTool` global has `SetMarkerTool` defined but never called. Markers in production therefore always say `Tool: "clickhouse-backup"` (no version).

**Two options. Pick one and apply.**

### Option A: Wire it (preferred)

- [ ] **Step 1: Modify `cmd/clickhouse-backup/main.go`**

After `cliapp.Version = version`:

```go
cas.SetMarkerTool(fmt.Sprintf("clickhouse-backup/%s", version))
```

(Add `cas` import if not already present. The `version` global is set at link time.)

- [ ] **Step 2: Run, commit**

```bash
go build ./cmd/clickhouse-backup
git add cmd/clickhouse-backup/main.go
git commit -m "feat(cas): wire SetMarkerTool to the build version string"
```

### Option B: Delete it

- [ ] **Step 1: Remove `SetMarkerTool` from `pkg/cas/markers.go`**

Delete the function and its referencing test. Hard-code `markerTool = "clickhouse-backup"`.

- [ ] **Step 2: Run, commit**

```bash
go test ./pkg/cas/... -count=1
git add pkg/cas/markers.go pkg/cas/markers_test.go
git commit -m "refactor(cas): drop unused SetMarkerTool (YAGNI)"
```

**Pick Option A** unless the wiring proves complicated. The version string is genuinely useful in marker JSON for forensics.

---

## Task 12: Resolve `docs/clickhouse-backup-v2-design-state.md` (D3)

**Files:**
- Modify: `.gitignore`

The file is a brainstorming-state artifact left behind by the design interview. Three options:
1. Add to `.gitignore` (treat as never committed; user can keep locally).
2. Commit it (preserves the trail).
3. Delete it.

Without strong reasons either way, **option 1** is the conservative default — it doesn't lose the file, just stops `git status` from nagging.

- [ ] **Step 1: Append to `.gitignore`**

```bash
echo "docs/clickhouse-backup-v2-design-state.md" >> .gitignore
```

- [ ] **Step 2: Verify `git status` is clean**

```bash
git status --short    # should NOT list the file
```

- [ ] **Step 3: Commit**

```bash
git add .gitignore
git commit -m "chore: ignore docs/clickhouse-backup-v2-design-state.md (working artifact)"
```

---

## Task 13: Hide `cas-download --data/-d` flag (Finding 3)

**Files:**
- Modify: `cmd/clickhouse-backup/cas_commands.go`

The flag is documented as "reserved; no behavioral effect". Hide it with `Hidden: true` so it doesn't show in `--help` output but still parses (preserves CLI compatibility for any future `--data` semantics).

- [ ] **Step 1: Edit the flag definition**

Find the `cli.BoolFlag` for `--data` on `cas-download` and add `Hidden: true`:

```go
cli.BoolFlag{
    Name:   "data, d",
    Hidden: true,
    Usage:  "Reserved (currently a no-op); will gate data-only download in a future version",
},
```

- [ ] **Step 2: Verify help output**

```bash
go build ./cmd/clickhouse-backup
./clickhouse-backup help cas-download | grep -- "--data" || echo "OK: --data hidden"
rm -f clickhouse-backup
```

- [ ] **Step 3: Commit**

```bash
git add cmd/clickhouse-backup/cas_commands.go
git commit -m "fix(cas): hide cas-download --data/-d (reserved, no behavioral effect)

Visible no-op flags mislead operators familiar with v1 restore's --data
into expecting filtering. Mark hidden until CAS v2 implements the
behavior. Parsing remains compatible for forward-migration."
```

---

## Task 14: Document `--json` vs `--format` decision for future CAS commands (Finding 2)

**Files:**
- Modify: `docs/cas-design.md` — add a short subsection under §6.10 "CLI surface" documenting the convention.

- [ ] **Step 1: Add the subsection**

Append to `docs/cas-design.md` §6.10:

```markdown
### 6.10.1 Output-format convention

`cas-verify --json` is a boolean flag that emits line-delimited JSON failures. Existing v1 commands use `--format text|json|yaml|csv|tsv` for tabular listings (`list remote`).

These two patterns are kept distinct on purpose:
- **Tabular listings** use `--format` because operators may want csv/tsv for spreadsheet ingest.
- **Diagnostic pass/fail commands** (cas-verify, future cas-fsck) use `--json` because failures are line-delimited streams, not tables; the only useful alternatives are "human" or "machine".

When new CAS commands need machine-readable output, follow this rule: tabular → `--format`; line-delimited diagnostic → `--json`. Don't introduce a third convention.
```

- [ ] **Step 2: Commit**

```bash
git add docs/cas-design.md
git commit -m "docs(cas): document --json vs --format CLI convention (consistency review F2)"
```

---

## Task 15: Final verification + PR comment

- [ ] **Step 1: Full test sweep**

```bash
go test ./pkg/cas/... ./pkg/backup/... ./pkg/storage/... ./pkg/checksumstxt/... -race -count=1
go vet ./...
go vet -tags=integration ./test/integration/...
go build ./cmd/clickhouse-backup
```

All green.

- [ ] **Step 2: Push and update the PR**

```bash
git push origin cas-phase1-followups
gh pr comment cas-phase1 --body "Follow-up branch \`cas-phase1-followups\` addresses correctness gaps + 3 findings from the consistency review. See plan: docs/superpowers/plans/2026-05-07-cas-phase1-followups.md"
```

If the user wants the follow-ups merged into the same PR (rather than a chained PR), instead:

```bash
git checkout cas-phase1
git merge --no-ff cas-phase1-followups -m "merge follow-up fixes"
git push
```

(Decide with the user which they prefer.)

- [ ] **Step 3: Mark plan complete**

Edit this plan's checkboxes in-place, commit:

```bash
git add docs/superpowers/plans/2026-05-07-cas-phase1-followups.md
git commit -m "docs(cas): mark follow-up plan complete"
```

---

## Spec coverage check

| Source | Item | Task |
|---|---|---|
| Self-review #4 / #7 | Empty `Query`/`UUID`/`Size` in TableMetadata | Task 3 |
| Self-review #5 | cas-verify StatFile error misclassified as missing | Task 6 |
| Self-review #8 | `pkg/backup/cas_methods.go` untested | Task 9 |
| Self-review #8 | `pkg/cas/casstorage` adapter untested | Task 10 |
| Self-review #9 | Object-disk pre-flight reads live state | Task 8 |
| Self-review #10 | PR never opened | Task 1 |
| Self-review #11 | TestMutationDedup never written | Task 4 |
| Self-review #12 | SkipPrefixes reconciliation unverified | Task 2 |
| Self-review (general) | End-to-end never run | Task 5 |
| Earlier review of Task 7 | markerTool unused | Task 11 |
| Earlier (untracked file) | docs/...design-state.md | Task 12 |
| Consistency F1 | `"???"` sentinel in list remote | Task 7 |
| Consistency F2 | `--json` vs `--format` convention | Task 14 |
| Consistency F3 | `--data/-d` no-op flag on cas-download | Task 13 |
| (Final) | Full sweep + PR update | Task 15 |

Coverage gaps acknowledged: **B1 brittleness** (the bm.CAS-stripping approach to v1 restore handoff) is left as-is unless Task 5 reveals it failing end-to-end. If end-to-end fails, add a follow-up task: thread an explicit "this is a CAS handoff" flag through `Backuper.Restore` and revert the strip. Document the decision in the §6.5 of cas-design.md.
