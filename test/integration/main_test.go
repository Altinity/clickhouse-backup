//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	cleanupStaleTestContainers(ctx)
	prePullImages()

	runParallel := 1
	if v, err := strconv.Atoi(os.Getenv("RUN_PARALLEL")); err == nil && v > 0 {
		runParallel = v
	}

	envPool = make(chan *TestEnvironment, runParallel)
	allContainers := make([]*TestContainers, runParallel)

	// Start all envs in parallel
	var startWg sync.WaitGroup
	var startErr atomic.Value
	for i := 1; i <= runParallel; i++ {
		startWg.Add(1)
		go func(idx int) {
			defer startWg.Done()
			tc, err := NewTestContainers(idx)
			if err != nil {
				startErr.Store(fmt.Errorf("NewTestContainers(%d): %w", idx, err))
				return
			}
			if err = tc.StartAll(ctx); err != nil {
				tc.StopAll(ctx)
				startErr.Store(fmt.Errorf("TestContainers(%d).StartAll: %w", idx, err))
				return
			}
			allContainers[idx-1] = tc
			envPool <- &TestEnvironment{
				ProjectName: fmt.Sprintf("project%d", idx),
				tc:          tc,
			}
			log.Info().Msgf("started testcontainers env %d", idx)
		}(i)
	}
	startWg.Wait()
	if v := startErr.Load(); v != nil {
		// Stop any envs that did start before failing
		for _, tc := range allContainers {
			if tc != nil {
				tc.StopAll(ctx)
			}
		}
		log.Fatal().Err(v.(error)).Msg("failed to start test environments")
	}

	code := m.Run()

	// Stop all envs in parallel
	var stopWg sync.WaitGroup
	for _, tc := range allContainers {
		stopWg.Add(1)
		go func(tc *TestContainers) {
			defer stopWg.Done()
			tc.StopAll(ctx)
		}(tc)
	}
	stopWg.Wait()
	os.Exit(code)
}

// prePullImages pulls all Docker images once before tests start,
// so parallel tests don't race to pull the same images.
func prePullImages() {
	chImage := fmt.Sprintf("docker.io/%s:%s",
		getEnvDefault("CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server"),
		getEnvDefault("CLICKHOUSE_VERSION", "25.8"))
	keeperImage := fmt.Sprintf("docker.io/clickhouse/clickhouse-keeper:%s",
		getEnvDefault("CLICKHOUSE_KEEPER_VERSION", "latest-alpine"))
	zkImage := fmt.Sprintf("%s:%s",
		getEnvDefault("ZOOKEEPER_IMAGE", "docker.io/zookeeper"),
		getEnvDefault("ZOOKEEPER_VERSION", "3.8.4"))

	images := []string{
		"docker.io/panubo/sshd:latest",
		fmt.Sprintf("docker.io/minio/minio:%s", getEnvDefault("MINIO_VERSION", "latest")),
		"fsouza/fake-gcs-server:latest",
		"mcr.microsoft.com/azure-storage/azurite:latest",
		chImage,
	}

	if isAdvancedMode() {
		images = append(images,
			keeperImage,
			fmt.Sprintf("docker.io/mysql:%s", getEnvDefault("MYSQL_VERSION", "latest")),
			fmt.Sprintf("docker.io/postgres:%s", getEnvDefault("PGSQL_VERSION", "latest")),
		)
		images = append(images, "docker.io/iradu/proftpd:latest")
	} else {
		images = append(images, zkImage)
		images = append(images, "docker.io/instantlinux/vsftpd:latest")
	}

	ctx := context.Background()
	tc, err := NewTestContainers(0)
	if err != nil {
		log.Warn().Err(err).Msg("prePullImages: can't create docker client, skipping pre-pull")
		return
	}
	defer func() {
		_ = tc.client.Close()
	}()

	for _, img := range images {
		log.Info().Msgf("pre-pulling image %s", img)
		tc.pullImageIfNeeded(ctx, img)
	}

	curDir := os.Getenv("CUR_DIR")
	if curDir == "" {
		curDir, _ = os.Getwd()
		curDir = filepath.Join(curDir, "test", "integration")
	}
	log.Info().Msgf("pre-pull complete, CUR_DIR=%s", curDir)
}
