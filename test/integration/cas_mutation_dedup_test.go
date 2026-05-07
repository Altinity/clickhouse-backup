//go:build integration

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TestCASMutationDedup verifies the headline value-prop:
// after an ALTER TABLE ... UPDATE that rewrites a single column,
// the second cas-upload should transfer dramatically fewer bytes than
// the first because all unmutated column files are byte-identical and
// dedup against the existing blob store.
func TestCASMutationDedup(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 500*time.Millisecond, 1*time.Second, 1*time.Minute)
	defer env.Cleanup(t, r)

	env.casBootstrap(r, "mutation_dedup")

	const (
		dbName  = "cas_mutdedup_db"
		tblName = "cas_mutdedup_t"
		bk1     = "cas_mutdedup_bk1"
		bk2     = "cas_mutdedup_bk2"
		rows    = 100000
	)

	// Schema: wide table with a "big" payload column and a "small" marker
	// column we'll mutate. force-wide so each column has its own .bin file.
	r.NoError(env.dropDatabase(dbName, true))
	env.queryWithNoError(r, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	env.queryWithNoError(r, fmt.Sprintf(`CREATE TABLE `+"`%s`.`%s`"+` (id UInt64, payload String, marker String)
        ENGINE=MergeTree ORDER BY id
        SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0`, dbName, tblName))
	env.queryWithNoError(r, fmt.Sprintf(
		"INSERT INTO `%s`.`%s` SELECT number, repeat('x', 1024), 'orig' FROM numbers(%d)",
		dbName, tblName, rows))
	env.queryWithNoError(r, fmt.Sprintf("OPTIMIZE TABLE `%s`.`%s` FINAL", dbName, tblName))

	// First backup — uploads everything fresh.
	env.casBackupNoError(r, "create", "--tables", dbName+".*", bk1)
	out1 := env.casBackupNoError(r, "cas-upload", bk1)
	log.Debug().Str("bk1_out", out1).Msg("first cas-upload")
	bytes1 := parseBytesUploaded(t, out1)
	if bytes1 == 0 {
		t.Fatalf("could not parse bytes uploaded for bk1; output:\n%s", out1)
	}

	// Mutate ONLY the marker column; payload is hardlinked unchanged.
	env.queryWithNoError(r, fmt.Sprintf(
		"ALTER TABLE `%s`.`%s` UPDATE marker = 'after' WHERE 1 SETTINGS mutations_sync=2",
		dbName, tblName))
	env.queryWithNoError(r, fmt.Sprintf("OPTIMIZE TABLE `%s`.`%s` FINAL", dbName, tblName))

	env.casBackupNoError(r, "create", "--tables", dbName+".*", bk2)
	out2 := env.casBackupNoError(r, "cas-upload", bk2)
	log.Debug().Str("bk2_out", out2).Msg("second cas-upload")
	bytes2 := parseBytesUploaded(t, out2)
	if bytes2 == 0 && !strings.Contains(out2, "uploaded now") {
		t.Fatalf("could not parse bytes uploaded for bk2; output:\n%s", out2)
	}

	// Headline assertion: second upload is at most 1/4 of the first.
	// Real-world ratio is ~1/N where N is the number of columns; we pick a
	// loose 1/4 bound to absorb compression-blob overhead (one compressed
	// marker column, plus the ALTER's bookkeeping files) and avoid flake.
	if bytes2 >= bytes1/4 {
		t.Fatalf("mutation dedup failed: bk1 uploaded %d bytes, bk2 uploaded %d bytes (expected bk2 << bk1; ratio = %.2f)",
			bytes1, bytes2, float64(bytes2)/float64(bytes1))
	}
	t.Logf("mutation dedup OK: bk1=%d B, bk2=%d B (%.1f%% of bk1)",
		bytes1, bytes2, 100*float64(bytes2)/float64(bytes1))

	// Cleanup.
	env.casBackupNoError(r, "cas-delete", bk1)
	env.casBackupNoError(r, "cas-delete", bk2)
	env.queryWithNoError(r, fmt.Sprintf("DROP DATABASE `%s` SYNC", dbName))
}

// parseBytesUploaded extracts the bytes-uploaded value from cas-upload's
// printed summary. Format (from pkg/backup/cas_methods.go's stats output):
//
//	cas-upload: bk1
//	  Backup content : 100 files, 1.5 MiB total
//	  Inlined        : 30 files, 12.3 KiB (packed into 1 archive, 8.4 KiB compressed)
//	  Blob store     : 50 unique blobs, 1.4 MiB
//	    uploaded now : 50 blobs, 1.4 MiB
//	    reused       : 0 blobs, 0 B (already in remote — saved by content-addressing)
//	  Wall clock     : 1.234s
//
// Returns 0 if the line can't be parsed (caller decides how strict to be).
func parseBytesUploaded(t *testing.T, out string) int64 {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "uploaded now") {
			continue
		}
		// Form: "    uploaded now : N blobs, X.Y UNIT"
		idx := strings.Index(line, ", ")
		if idx < 0 {
			continue
		}
		rest := strings.TrimSpace(line[idx+2:])
		return humanBytesToInt64(t, rest)
	}
	return 0
}

// humanBytesToInt64 parses outputs like "5.6 MiB" / "1024 B" / "0 B" into
// int64 bytes. Uses utils.FormatBytes-compatible suffixes.
func humanBytesToInt64(t *testing.T, s string) int64 {
	t.Helper()
	var v float64
	var unit string
	if _, err := fmt.Sscanf(s, "%f %s", &v, &unit); err != nil {
		t.Fatalf("parse human bytes %q: %v", s, err)
	}
	mult := int64(1)
	switch strings.ToUpper(unit) {
	case "B":
		mult = 1
	case "KIB":
		mult = 1024
	case "MIB":
		mult = 1024 * 1024
	case "GIB":
		mult = 1024 * 1024 * 1024
	case "TIB":
		mult = 1024 * 1024 * 1024 * 1024
	default:
		t.Fatalf("unknown unit %q in %q", unit, s)
	}
	return int64(v * float64(mult))
}
