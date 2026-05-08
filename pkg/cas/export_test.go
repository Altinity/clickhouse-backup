// export_test.go exposes unexported symbols to the cas_test package.
// This file is compiled only during testing.
package cas

import (
	"context"
	"time"
)

// WaitForPrune is the exported test shim for the unexported waitForPrune.
func WaitForPrune(ctx context.Context, b Backend, clusterPrefix string, wait time.Duration) error {
	return waitForPrune(ctx, b, clusterPrefix, wait)
}

// SetPollIntervalForTesting sets the package-level testing override for the
// poll interval. Pass nil to restore production behaviour.
func SetPollIntervalForTesting(d *time.Duration) {
	pollIntervalForTesting = d
}

// ProbeKey is the exported test shim for the unexported probeKey constant.
// Used by probe_test.go to assert sentinel cleanup.
const ProbeKey = probeKey
