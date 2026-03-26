//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
	var allContainers []*TestContainers
	for i := 1; i <= runParallel; i++ {
		tc, err := NewTestContainers(i)
		if err != nil {
			log.Fatal().Err(err).Msgf("NewTestContainers(%d) failed", i)
		}
		if err = tc.StartAll(ctx); err != nil {
			tc.StopAll(ctx)
			log.Fatal().Err(err).Msgf("TestContainers(%d).StartAll failed", i)
		}
		allContainers = append(allContainers, tc)
		envPool <- &TestEnvironment{
			ProjectName: fmt.Sprintf("project%d", i),
			tc:          tc,
		}
		log.Info().Msgf("started testcontainers env %d", i)
	}

	code := m.Run()

	for _, tc := range allContainers {
		tc.StopAll(ctx)
	}
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
