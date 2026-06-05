package resumable

import (
	"os"
	"path/filepath"
	"testing"
)

// newTestState creates a State backed by a real bolt DB in a temp dir.
func newTestState(t *testing.T) *State {
	t.Helper()
	dir := t.TempDir()
	const backupName = "test-backup"
	// NewState writes to <dir>/backup/<backupName>/<command>.state2 and bolt
	// does not create intermediate directories, so create them up front.
	if err := os.MkdirAll(filepath.Join(dir, "backup", backupName), 0755); err != nil {
		t.Fatalf("failed to create state dir: %v", err)
	}
	s := NewState(dir, backupName, "upload", nil)
	if s.db == nil {
		t.Fatal("expected an open resumable state DB")
	}
	return s
}

// TestNewStateReturnsNilDBOnOpenError verifies that when bolt.Open fails (here
// because the intermediate <dir>/backup/<backupName> directory does not exist
// and bolt does not create it), NewState logs a warning and returns a State
// with a nil db instead of panicking. Every method then short-circuits on the
// nil db guard, see issue #1172.
func TestNewStateReturnsNilDBOnOpenError(t *testing.T) {
	dir := t.TempDir()
	const backupName = "test-backup"
	// Deliberately do NOT create <dir>/backup/<backupName> so bolt.Open returns
	// an error for <dir>/backup/<backupName>/upload.state2.
	s := NewState(dir, backupName, "upload", nil)
	if s == nil {
		t.Fatal("NewState must never return a nil *State")
	}
	if s.db != nil {
		t.Fatal("expected s.db to be nil after bolt.Open failed")
	}
	// Methods must be safe to call on a nil-db state.
	if processed, size, err := s.IsAlreadyProcessed("shard1/part-0"); err != nil || processed || size != 0 {
		t.Errorf("expected (false, 0, nil) on nil-db state, got (%v, %d, %v)", processed, size, err)
	}
	if err := s.AppendToState("shard1/part-0", 1024); err != nil {
		t.Errorf("expected nil error on nil-db state AppendToState, got %v", err)
	}
}

// TestAppendToStateReturnsWriteError verifies that a failure to write the
// resumable state (here forced by closing the underlying DB) is returned to the
// caller instead of aborting the whole process via log.Fatal/os.Exit, see issue
// #1172. The running server can surface the error and the CLI exits non-zero,
// but the process must still be alive after the call.
func TestAppendToStateReturnsWriteError(t *testing.T) {
	s := newTestState(t)
	// Close the DB so subsequent writes return an error instead of succeeding.
	if err := s.db.Close(); err != nil {
		t.Fatalf("unexpected error closing db: %v", err)
	}
	// Must not call os.Exit; if it did, the test binary would die here.
	if err := s.AppendToState("shard1/part-0", 1024); err == nil {
		t.Error("expected a write error after closing the DB, got nil")
	}
}

// TestIsAlreadyProcessedReturnsReadError verifies that a failure to read the
// resumable state is returned to the caller (with processed=false, size=0)
// rather than aborting via log.Fatal/os.Exit, see issue #1172.
func TestIsAlreadyProcessedReturnsReadError(t *testing.T) {
	s := newTestState(t)
	if err := s.db.Close(); err != nil {
		t.Fatalf("unexpected error closing db: %v", err)
	}
	processed, size, err := s.IsAlreadyProcessed("shard1/part-0")
	if err == nil {
		t.Error("expected a read error after closing the DB, got nil")
	}
	if processed {
		t.Errorf("expected processed=false on read error, got true")
	}
	if size != 0 {
		t.Errorf("expected size=0 on read error, got %d", size)
	}
}

// TestAppendThenIsAlreadyProcessed exercises the happy path to ensure the
// guard changes did not break normal operation.
func TestAppendThenIsAlreadyProcessed(t *testing.T) {
	s := newTestState(t)
	defer s.Close()

	const p = "shard1/part-0"
	if processed, _, err := s.IsAlreadyProcessed(p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if processed {
		t.Fatalf("part should not be processed before AppendToState")
	}
	if err := s.AppendToState(p, 4096); err != nil {
		t.Fatalf("unexpected error from AppendToState: %v", err)
	}
	processed, size, err := s.IsAlreadyProcessed(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !processed {
		t.Errorf("expected part to be processed after AppendToState")
	}
	if size != 4096 {
		t.Errorf("expected size=4096, got %d", size)
	}
}
