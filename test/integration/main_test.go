//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {
	cleanupStaleTestContainers(context.Background())
	prePullImages()
	os.Exit(m.Run())
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
		chImage, // used for both clickhouse and clickhouse-backup containers
	}

	if isAdvancedMode() {
		images = append(images,
			keeperImage,
			fmt.Sprintf("docker.io/mysql:%s", getEnvDefault("MYSQL_VERSION", "latest")),
			fmt.Sprintf("docker.io/postgres:%s", getEnvDefault("PGSQL_VERSION", "latest")),
		)
		// FTP image for advanced mode
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

	// Also pre-pull the clickhouse-backup binary config dir to verify paths
	curDir := os.Getenv("CUR_DIR")
	if curDir == "" {
		curDir, _ = os.Getwd()
		curDir = filepath.Join(curDir, "test", "integration")
	}
	log.Info().Msgf("pre-pull complete, CUR_DIR=%s", curDir)
}
