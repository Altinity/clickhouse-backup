//go:build integration

package main

import (
	"context"
	"os"
	"testing"

	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	if os.Getenv("USE_TESTCONTAINERS") == "1" {
		cleanupStaleTestContainers(ctx)
	}
	code := m.Run()
	if len(allTestContainers) > 0 {
		for _, tc := range allTestContainers {
			log.Info().Msgf("stopping testcontainers env %d", tc.envID)
			tc.StopAll(ctx)
		}
	}
	os.Exit(code)
}
