package cas_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
	"github.com/Altinity/clickhouse-backup/v2/pkg/cas/internal/fakedst"
	"github.com/stretchr/testify/require"
)

func TestWaitForPrune_NoMarkerProceedsImmediately(t *testing.T) {
	d := 10 * time.Millisecond
	cas.SetPollIntervalForTesting(&d)
	defer cas.SetPollIntervalForTesting(nil)

	b := fakedst.New()
	start := time.Now()
	err := cas.WaitForPrune(context.Background(), b, "cas/c1/", 5*time.Second)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 100*time.Millisecond)
}

func TestWaitForPrune_MarkerClearsBeforeDeadline(t *testing.T) {
	d := 10 * time.Millisecond
	cas.SetPollIntervalForTesting(&d)
	defer cas.SetPollIntervalForTesting(nil)

	b := fakedst.New()
	cp := "cas/c1/"
	require.NoError(t, b.PutFile(context.Background(), cp+"prune.marker", io.NopCloser(strings.NewReader("{}")), 2))

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = b.DeleteFile(context.Background(), cp+"prune.marker")
	}()

	start := time.Now()
	err := cas.WaitForPrune(context.Background(), b, cp, 5*time.Second)
	require.NoError(t, err)
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	require.Less(t, elapsed, 1*time.Second)
}

func TestWaitForPrune_TimeoutReturnsErrPruneInProgress(t *testing.T) {
	d := 10 * time.Millisecond
	cas.SetPollIntervalForTesting(&d)
	defer cas.SetPollIntervalForTesting(nil)

	b := fakedst.New()
	cp := "cas/c1/"
	body := `{"host":"h1","started_at":"2026-05-08T10:30:12Z","run_id":"abc123","tool":"cas-prune"}`
	require.NoError(t, b.PutFile(context.Background(), cp+"prune.marker", io.NopCloser(strings.NewReader(body)), int64(len(body))))

	err := cas.WaitForPrune(context.Background(), b, cp, 100*time.Millisecond)
	require.Error(t, err)
	require.True(t, errors.Is(err, cas.ErrPruneInProgress))
	require.Contains(t, err.Error(), "h1")
	require.Contains(t, err.Error(), "abc123")
}

func TestWaitForPrune_ZeroWaitMatchesImmediateRefusal(t *testing.T) {
	b := fakedst.New()
	cp := "cas/c1/"
	require.NoError(t, b.PutFile(context.Background(), cp+"prune.marker", io.NopCloser(strings.NewReader("{}")), 2))

	err := cas.WaitForPrune(context.Background(), b, cp, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, cas.ErrPruneInProgress))
}

func TestWaitForPrune_RespectsContextCancel(t *testing.T) {
	d := 10 * time.Millisecond
	cas.SetPollIntervalForTesting(&d)
	defer cas.SetPollIntervalForTesting(nil)

	b := fakedst.New()
	cp := "cas/c1/"
	require.NoError(t, b.PutFile(context.Background(), cp+"prune.marker", io.NopCloser(strings.NewReader("{}")), 2))

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()

	start := time.Now()
	err := cas.WaitForPrune(ctx, b, cp, 5*time.Second)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Less(t, time.Since(start), 500*time.Millisecond)
}
