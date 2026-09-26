package clickhouse

import (
	"bytes"
	"context"
	"net"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestConnectClosesFailedPools(t *testing.T) {
	for _, stop := range []string{"break on error", "shutdown", "retry then shutdown"} {
		t.Run(stop, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var attempts atomic.Int32
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					// Reject the native handshake so Open succeeds but Ping fails.
					if attempts.Add(1) == 2 && stop == "retry then shutdown" {
						cancel()
					}
					_ = conn.Close()
				}
			}()
			t.Cleanup(func() {
				_ = listener.Close()
				<-serverDone
			})

			ch := NewClickHouse(&config.ClickHouseConfig{
				Host:           "127.0.0.1",
				Port:           uint(listener.Addr().(*net.TCPAddr).Port),
				Timeout:        "1s",
				MaxConnections: 1,
			})
			ch.ShutdownCtx = ctx
			ch.BreakConnectOnError = stop == "break on error"
			if stop == "shutdown" {
				cancel()
			}

			before := countDrainPoolGoroutines(t)
			// Bound the retry test even if it regresses and stops observing shutdown.
			connectDone := make(chan error, 1)
			go func() { connectDone <- ch.Connect() }()
			select {
			case err := <-connectDone:
				require.Error(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("Connect did not stop after a failed Ping")
			}
			// Clean up the last pool on failure, without hiding the leak assertion.
			if ch.conn != nil {
				t.Cleanup(func() { _ = ch.conn.Close() })
			}
			wantAttempts := int32(1)
			if stop == "retry then shutdown" {
				wantAttempts = 2
			}
			require.Equal(t, wantAttempts, attempts.Load())
			require.False(t, ch.IsOpen)
			require.Eventually(t, func() bool {
				return countDrainPoolGoroutines(t) == before
			}, time.Second, 10*time.Millisecond, "failed Ping left a connection pool running")
			require.Nil(t, ch.conn)
		})
	}
}

// Each native driver pool owns a drain goroutine, including pools with no open
// sockets. Checking these catches a leak that connection statistics cannot see.
func countDrainPoolGoroutines(t *testing.T) int {
	t.Helper()
	var stacks bytes.Buffer
	require.NoError(t, pprof.Lookup("goroutine").WriteTo(&stacks, 2))
	return strings.Count(stacks.String(), "github.com/ClickHouse/clickhouse-go/v2.(*connPool).runDrainPool(")
}
