//go:build integration

package main

import (
	"context"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	cleanupStaleTestContainers(context.Background())
	os.Exit(m.Run())
}
