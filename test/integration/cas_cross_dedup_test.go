//go:build integration

package main

import (
	"fmt"
	"testing"
	"time"
)

// TestCASCrossBackupDedup verifies the catalog-level dedup invariant:
// a third backup that produces parts byte-identical to data already
// uploaded in two earlier independent backups should reuse those blobs
// instead of re-uploading them.
func TestCASCrossBackupDedup(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "cross_dedup")

	const (
		dbA  = "cas_xdedup_a"
		dbB  = "cas_xdedup_b"
		dbC  = "cas_xdedup_c"
		tbl  = "t"
		bkA  = "cas_xdedup_bkA"
		bkB  = "cas_xdedup_bkB"
		bkC  = "cas_xdedup_bkC"
		rows = 50000
	)

	// Setup a deterministic-payload schema that gives reproducible byte content
	// (so blobs in C match A's exactly).
	setup := func(db string, seed int) {
		r.NoError(env.dropDatabase(db, true))
		env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", db))
		env.queryWithNoError(r, fmt.Sprintf(`CREATE TABLE `+"`%s`.`%s`"+` (id UInt64, payload String)
            ENGINE=MergeTree ORDER BY id
            SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0`, db, tbl))
		env.queryWithNoError(r, fmt.Sprintf(
			"INSERT INTO `%s`.`%s` SELECT number + %d, repeat('x', 1024) FROM numbers(%d)",
			db, tbl, seed, rows))
		env.queryWithNoError(r, fmt.Sprintf("OPTIMIZE TABLE `%s`.`%s` FINAL", db, tbl))
	}

	// Backup A: db dbA, seed 0
	setup(dbA, 0)
	env.casBackupNoError(r, "create", "--tables", dbA+".*", bkA)
	outA := env.casBackupNoError(r, "cas-upload", bkA)
	bytesA := parseBytesUploaded(t, outA)
	r.True(bytesA > 0, "bkA: bytes uploaded must be > 0; out=%s", outA)

	// Backup B: db dbB, seed 100000 (disjoint from A) so B has no shared content with A
	setup(dbB, 100000)
	env.casBackupNoError(r, "create", "--tables", dbB+".*", bkB)
	outB := env.casBackupNoError(r, "cas-upload", bkB)
	bytesB := parseBytesUploaded(t, outB)
	r.True(bytesB > 0, "bkB: bytes uploaded must be > 0; out=%s", outB)

	// Backup C: db dbC = dbA's data verbatim. Setup with same seed.
	// Expectation: C's payload column files are byte-identical to A's,
	// so cas-upload C should reuse blobs and upload near-zero new bytes.
	setup(dbC, 0)
	env.casBackupNoError(r, "create", "--tables", dbC+".*", bkC)
	outC := env.casBackupNoError(r, "cas-upload", bkC)
	bytesC := parseBytesUploaded(t, outC)
	t.Logf("bkA=%d bytes uploaded, bkB=%d, bkC=%d", bytesA, bytesB, bytesC)

	// Headline assertion: C's upload is dramatically smaller than A's
	// because C's content already lives in the blob store (uploaded as part of A).
	// NOTE: The (db, table) name differs (dbA vs dbC), so per-table archives
	// (containing tiny metadata files like checksums.txt, primary.idx) won't
	// dedupe — they go into the table-archive .tar.zstd, not the blob store.
	// Only large column files (payload.bin, payload.mrk) live in the blob store
	// and these dedupe. Choose a loose threshold to absorb the inline-archive
	// overhead and any small-file leak.
	if bytesC >= bytesA/4 {
		t.Fatalf("cross-backup dedup failed: bkA uploaded %d bytes, bkC uploaded %d bytes (expected bkC << bkA; ratio = %.2f)",
			bytesA, bytesC, float64(bytesC)/float64(bytesA))
	}
	t.Logf("cross-backup dedup OK: bkC=%d B is %.1f%% of bkA=%d B",
		bytesC, 100*float64(bytesC)/float64(bytesA), bytesA)

	// Cleanup
	env.casBackupNoError(r, "cas-delete", bkA)
	env.casBackupNoError(r, "cas-delete", bkB)
	env.casBackupNoError(r, "cas-delete", bkC)
	env.queryWithNoError(r, fmt.Sprintf("DROP DATABASE `%s` SYNC", dbA))
	env.queryWithNoError(r, fmt.Sprintf("DROP DATABASE `%s` SYNC", dbB))
	env.queryWithNoError(r, fmt.Sprintf("DROP DATABASE `%s` SYNC", dbC))
}
