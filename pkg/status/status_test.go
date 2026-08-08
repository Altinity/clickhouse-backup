package status

import (
	"sync/atomic"
	"testing"
	"time"

	stderrors "errors"

	"github.com/stretchr/testify/require"
)

// TestCancelWaitsForGoroutineStop verifies that AsyncStatus.Cancel blocks
// until the command goroutine calls Stop (sync-wait behavior added for
// https://github.com/Altinity/clickhouse-backup/issues/1365).
func TestCancelWaitsForGoroutineStop(t *testing.T) {
	r := require.New(t)
	s := &AsyncStatus{}
	commandId, ctx := s.Start("upload my_backup")

	// Simulate the worker goroutine: blocks until ctx cancel, then waits a
	// known interval before calling Stop. Cancel must observe both the
	// ctx-cancel signal AND the Stop "Done close" — otherwise it would
	// return early.
	stopDelay := 200 * time.Millisecond
	stopCalled := atomic.Bool{}
	go func() {
		<-ctx.Done()
		time.Sleep(stopDelay)
		stopCalled.Store(true)
		s.Stop(commandId, ctx.Err())
	}()

	cancelStart := time.Now()
	cmd, err := s.Cancel("upload my_backup", stderrors.New("test cancel"))
	cancelElapsed := time.Since(cancelStart)
	r.NoError(err)
	r.Equal("upload my_backup", cmd)
	r.True(stopCalled.Load(), "Cancel returned before the worker goroutine had a chance to call Stop")
	r.GreaterOrEqual(cancelElapsed, stopDelay,
		"Cancel returned after only %s, expected to block ≥ %s waiting for Stop", cancelElapsed, stopDelay)
}

// TestCancelAllWaitsForAllGoroutines verifies that CancelAll waits for
// every in-progress goroutine to call Stop before returning.
func TestCancelAllWaitsForAllGoroutines(t *testing.T) {
	r := require.New(t)
	s := &AsyncStatus{}

	cmd1Id, ctx1 := s.Start("upload backup1")
	cmd2Id, ctx2 := s.Start("download backup2")

	stopDelay1 := 150 * time.Millisecond
	stopDelay2 := 250 * time.Millisecond
	var stopped1, stopped2 atomic.Bool

	go func() {
		<-ctx1.Done()
		time.Sleep(stopDelay1)
		stopped1.Store(true)
		s.Stop(cmd1Id, ctx1.Err())
	}()
	go func() {
		<-ctx2.Done()
		time.Sleep(stopDelay2)
		stopped2.Store(true)
		s.Stop(cmd2Id, ctx2.Err())
	}()

	cancelStart := time.Now()
	canceled := s.CancelAll("test cancelAll")
	cancelElapsed := time.Since(cancelStart)

	r.ElementsMatch([]string{"upload backup1", "download backup2"}, canceled)
	r.True(stopped1.Load(), "CancelAll returned before goroutine #1 called Stop")
	r.True(stopped2.Load(), "CancelAll returned before goroutine #2 called Stop")
	// CancelAll waits sequentially per Done, so total >= max(delays).
	r.GreaterOrEqual(cancelElapsed, stopDelay2,
		"CancelAll returned after only %s, expected to block ≥ %s", cancelElapsed, stopDelay2)
}

// TestCancelTimeout verifies the CancelWaitTimeout safety net: if a worker
// goroutine never calls Stop (e.g. stuck on IO with no ctx awareness),
// Cancel must still return after the configured timeout.
func TestCancelTimeout(t *testing.T) {
	r := require.New(t)
	s := &AsyncStatus{}
	s.Start("upload stuck_backup")

	originalTimeout := CancelWaitTimeout
	SetCancelWaitTimeout(150 * time.Millisecond)
	defer SetCancelWaitTimeout(originalTimeout)

	// No goroutine ever calls Stop — Cancel must hit the timeout and return.
	cancelStart := time.Now()
	cmd, err := s.Cancel("upload stuck_backup", stderrors.New("test cancel"))
	cancelElapsed := time.Since(cancelStart)
	r.NoError(err)
	r.Equal("upload stuck_backup", cmd)
	r.GreaterOrEqual(cancelElapsed, 150*time.Millisecond,
		"Cancel returned after only %s, expected to wait the timeout", cancelElapsed)
	// Allow generous upper bound (timeout + some slack) — should not block forever.
	r.Less(cancelElapsed, 2*time.Second,
		"Cancel did not honor timeout: blocked for %s", cancelElapsed)
}

// TestCancelOfFinishedCommandReturnsError verifies the error path when no
// in-progress command matches.
func TestCancelOfFinishedCommandReturnsError(t *testing.T) {
	r := require.New(t)
	s := &AsyncStatus{}
	commandId, _ := s.Start("upload done_backup")
	s.Stop(commandId, nil) // command finishes naturally

	_, err := s.Cancel("upload done_backup", stderrors.New("test"))
	r.Error(err, "Cancel of a finished command must return an error, not block")
}

// TestSetCancelWaitTimeout sanity-checks the setter.
func TestSetCancelWaitTimeout(t *testing.T) {
	r := require.New(t)
	saved := CancelWaitTimeout
	defer func() { CancelWaitTimeout = saved }()

	SetCancelWaitTimeout(42 * time.Second)
	r.Equal(42*time.Second, CancelWaitTimeout)
	// Zero/negative durations must be ignored.
	SetCancelWaitTimeout(0)
	r.Equal(42*time.Second, CancelWaitTimeout)
	SetCancelWaitTimeout(-1 * time.Second)
	r.Equal(42*time.Second, CancelWaitTimeout)
}

// Long running `watch` registers one row per iteration, so finished rows must be
// dropped while already handed out commandIds keep resolving to their own row.
func TestTrimFinishedRows_KeepsCommandIdStable(t *testing.T) {
	r := require.New(t)
	defer func(old int) { maxFinishedRows = old }(maxFinishedRows)
	maxFinishedRows = 5

	s := &AsyncStatus{}
	// a `watch`/`server` row stays in progress for the whole process lifetime and
	// must not keep the finished iterations recorded after it alive
	stuckId, _ := s.Start("watch")
	firstIterationId, _ := s.Start("create_remote iteration-0")
	s.Stop(firstIterationId, nil)
	var lastId int
	for i := 1; i < 50; i++ {
		lastId, _ = s.Start("create_remote iteration")
		s.Stop(lastId, nil)
	}

	s.RLock()
	kept := len(s.commands)
	s.RUnlock()
	// trimming runs on Start, so the row finished after the last Start may linger,
	// plus the one still in progress
	r.LessOrEqual(kept, maxFinishedRows+2, "finished rows must be trimmed")

	// the in progress row is never dropped, and its id still resolves
	_, _, err := s.GetContextWithCancel(stuckId)
	r.NoError(err)

	// the newest row still resolves to itself, not to a shifted neighbour
	_, _, err = s.GetContextWithCancel(lastId)
	r.Error(err, "finished rows have no context")
	r.Equal(SuccessStatus, s.GetStatus(false, "iteration", 1)[0].Status)

	// a trimmed id resolves to nothing instead of hitting the wrong row
	_, _, err = s.GetContextWithCancel(firstIterationId)
	r.Error(err)
	r.Empty(s.GetStatus(false, "iteration-0", 1), "trimmed row must be gone from history")
}

// A row canceled via /backup/kill is terminal but its command goroutine may still
// be running, and that goroutine must still reach Stop to close Done and release
// the Cancel waiter. Trimming it early would strand Cancel for CancelWaitTimeout.
func TestTrimKeepsCanceledRowUntilGoroutineReturns(t *testing.T) {
	r := require.New(t)
	defer func(old int) { maxFinishedRows = old }(maxFinishedRows)
	maxFinishedRows = 1
	// Cancel waits for the goroutine, which in this test only returns later
	defer func(old time.Duration) { CancelWaitTimeout = old }(CancelWaitTimeout)
	CancelWaitTimeout = 100 * time.Millisecond

	s := &AsyncStatus{}
	canceledId, _ := s.Start("restore my_backup")
	_, err := s.Cancel("restore my_backup", stderrors.New("canceled by user"))
	r.NoError(err)

	// push far past the limit while the canceled goroutine has not returned yet
	for i := 0; i < 20; i++ {
		id, _ := s.Start("create iteration")
		s.Stop(id, nil)
	}

	s.RLock()
	row := s.byId[canceledId]
	s.RUnlock()
	r.NotNil(row, "canceled row must survive trimming until its goroutine returns")

	// once the goroutine reaches Stop the row becomes trimmable
	s.Stop(canceledId, nil)
	for i := 0; i < 20; i++ {
		id, _ := s.Start("create iteration")
		s.Stop(id, nil)
	}
	s.RLock()
	_, stillThere := s.byId[canceledId]
	s.RUnlock()
	r.False(stillThere, "canceled row must be trimmed after its goroutine returned")
}
