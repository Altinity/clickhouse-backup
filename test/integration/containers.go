//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	osExec "os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	dockerImage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/volume"
	dockerClient "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/rs/zerolog/log"
)

// cleanupStaleTestContainers removes any leftover tc_ containers, networks, and volumes
// from a previous interrupted test run.
func cleanupStaleTestContainers(ctx context.Context) {
	cli, err := dockerClient.NewClientWithOpts(dockerClient.FromEnv, dockerClient.WithAPIVersionNegotiation())
	if err != nil {
		log.Warn().Err(err).Msg("cleanup: can't create docker client")
		return
	}
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			log.Warn().Err(err).Msg("can't close cli")
		}
	}()

	// Remove containers with name prefix "tc_"
	containers, err := cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("name", "tc_")),
	})
	if err == nil {
		timeout := 1
		for _, cn := range containers {
			log.Info().Msgf("cleanup: removing stale container %s (%s)", cn.Names, cn.ID[:12])
			_ = cli.ContainerStop(ctx, cn.ID, container.StopOptions{Timeout: &timeout})
			_ = cli.ContainerRemove(ctx, cn.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
		}
	}

	// Remove networks with name prefix "tc_"
	networks, err := cli.NetworkList(ctx, network.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", "tc_")),
	})
	if err == nil {
		for _, n := range networks {
			log.Info().Msgf("cleanup: removing stale network %s", n.Name)
			_ = cli.NetworkRemove(ctx, n.ID)
		}
	}

	// Remove volumes with name prefix "tc_"
	volList, err := cli.VolumeList(ctx, volume.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", "tc_")),
	})
	if err == nil {
		for _, v := range volList.Volumes {
			log.Info().Msgf("cleanup: removing stale volume %s", v.Name)
			_ = cli.VolumeRemove(ctx, v.Name, true)
		}
	}
}

// ContainerInfo holds runtime info for a started container.
type ContainerInfo struct {
	ID       string
	Name     string
	Hostname string
}

// TestContainers manages all Docker containers for integration tests.
type TestContainers struct {
	client        *dockerClient.Client
	networkID     string
	networkName   string
	mu            sync.Mutex
	containers    map[string]*ContainerInfo // service name -> info
	sharedVolumes []string                  // named volume names for cleanup
	isAdvanced    bool
	envID         int
}

// NewTestContainers creates a new container manager.
func NewTestContainers(envID int) (*TestContainers, error) {
	cli, err := dockerClient.NewClientWithOpts(dockerClient.FromEnv, dockerClient.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	tc := &TestContainers{
		client:     cli,
		containers: make(map[string]*ContainerInfo),
		isAdvanced: isAdvancedMode(),
		envID:      envID,
	}
	return tc, nil
}

func isAdvancedMode() bool {
	v := os.Getenv("CLICKHOUSE_VERSION")
	return v == "" || v == "head" || compareVersion(v, "20.0") >= 0
}

// isModernSSHD reports whether the OpenSSH 10 linuxserver/openssh-server sshd
// (advertises the sftp `copy-data` extension) is used instead of the OpenSSH 8.6
// panubo image; gated to CH>=23.3 so the copy-data path gets coverage on newer
// matrix entries while older ones keep exercising the hardlink fallback.
func isModernSSHD() bool {
	v := os.Getenv("CLICKHOUSE_VERSION")
	return v == "" || v == "head" || compareVersion(v, "23.3") >= 0
}

// StartAll creates the network and starts all containers.
// Independent support services start in parallel to reduce startup time.
func (tc *TestContainers) StartAll(ctx context.Context) error {
	var err error

	tc.networkName = fmt.Sprintf("tc_integration_%d", tc.envID)
	resp, err := tc.client.NetworkCreate(ctx, tc.networkName, network.CreateOptions{Driver: "bridge"})
	if err != nil {
		return fmt.Errorf("create network: %w", err)
	}
	tc.networkID = resp.ID

	curDir := os.Getenv("CUR_DIR")
	if curDir == "" {
		curDir, _ = os.Getwd()
	}
	configsDir := filepath.Join(curDir, "configs")

	// Shared named volumes for clickhouse <-> clickhouse-backup
	prefix := fmt.Sprintf("tc_%d_", tc.envID)
	tc.sharedVolumes = []string{
		prefix + "ch_data",
		prefix + "hdd1",
		prefix + "hdd2",
		prefix + "hdd3",
	}
	for _, vol := range tc.sharedVolumes {
		if _, err = tc.client.VolumeCreate(ctx, volume.CreateOptions{Name: vol}); err != nil {
			return fmt.Errorf("create volume %s: %w", vol, err)
		}
	}

	// Start all independent support services in parallel
	type startResult struct {
		name string
		err  error
	}
	var wg sync.WaitGroup
	resultCh := make(chan startResult, 10)

	startAsync := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resultCh <- startResult{name: name, err: fn()}
		}()
	}

	startAsync("sshd", func() error { return tc.startSSHD(ctx) })
	startAsync("ftp", func() error { return tc.startFTP(ctx, curDir) })
	startAsync("minio", func() error { return tc.startMinio(ctx, configsDir) })
	startAsync("gcs", func() error { return tc.startGCS(ctx) })
	startAsync("azure", func() error { return tc.startAzure(ctx) })
	startAsync("zookeeper", func() error { return tc.startZookeeper(ctx, configsDir) })
	if tc.isAdvanced {
		startAsync("mysql", func() error { return tc.startMySQL(ctx) })
		startAsync("pgsql", func() error { return tc.startPgSQL(ctx) })
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for res := range resultCh {
		if res.err != nil {
			return fmt.Errorf("start %s: %w", res.name, res.err)
		}
	}

	// Wait for all support services health in parallel
	healthServices := []struct {
		name    string
		timeout time.Duration
	}{
		{"sshd", 90 * time.Second},
		{"ftp", 90 * time.Second},
		{"minio", 90 * time.Second},
		{"gcs", 90 * time.Second},
		{"azure", 90 * time.Second},
		{"zookeeper", 90 * time.Second},
	}
	if tc.isAdvanced {
		healthServices = append(healthServices,
			struct {
				name    string
				timeout time.Duration
			}{"mysql", 120 * time.Second},
			struct {
				name    string
				timeout time.Duration
			}{"pgsql", 120 * time.Second},
		)
	}

	healthCh := make(chan startResult, len(healthServices))
	for _, svc := range healthServices {
		go func(name string, timeout time.Duration) {
			healthCh <- startResult{name: name, err: tc.waitHealthy(ctx, name, timeout, "")}
		}(svc.name, svc.timeout)
	}
	for range healthServices {
		res := <-healthCh
		if res.err != nil {
			return fmt.Errorf("wait %s: %w", res.name, res.err)
		}
	}

	// ClickHouse depends on ZooKeeper, so start after support services are healthy
	if err = tc.startClickHouse(ctx, curDir, configsDir); err != nil {
		return err
	}
	if err = tc.waitHealthy(ctx, "clickhouse", 300*time.Second, ""); err != nil {
		return fmt.Errorf("wait clickhouse: %w", err)
	}

	// clickhouse-backup depends on ClickHouse
	if err = tc.startClickHouseBackup(ctx, curDir, configsDir); err != nil {
		return err
	}
	if err = tc.waitHealthy(ctx, "clickhouse-backup", 60*time.Second, ""); err != nil {
		return fmt.Errorf("wait clickhouse-backup: %w", err)
	}

	return nil
}

// StopAll stops and removes all containers, volumes, and network.
func (tc *TestContainers) StopAll(ctx context.Context) {
	timeout := 1
	var stopWg sync.WaitGroup
	for name, info := range tc.containers {
		stopWg.Add(1)
		go func(name string, id string) {
			defer stopWg.Done()
			if err := tc.client.ContainerStop(ctx, id, container.StopOptions{Timeout: &timeout}); err != nil {
				log.Debug().Err(err).Msgf("stop %s", name)
			}
			if err := tc.client.ContainerRemove(ctx, id, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
				log.Debug().Err(err).Msgf("remove %s", name)
			}
		}(name, info.ID)
	}
	stopWg.Wait()
	tc.containers = make(map[string]*ContainerInfo)

	for _, vol := range tc.sharedVolumes {
		if err := tc.client.VolumeRemove(ctx, vol, true); err != nil {
			log.Debug().Err(err).Msgf("remove volume %s", vol)
		}
	}
	if tc.networkID != "" {
		if err := tc.client.NetworkRemove(ctx, tc.networkID); err != nil {
			log.Debug().Err(err).Msgf("remove network %s", tc.networkName)
		}
		tc.networkID = ""
	}
}

// GetContainerID returns the container ID for a service name.
func (tc *TestContainers) GetContainerID(name string) string {
	if info, ok := tc.containers[name]; ok {
		return info.ID
	}
	return ""
}

// GetMappedPort returns the host-mapped port for a container's internal port.
func (tc *TestContainers) GetMappedPort(ctx context.Context, name string, containerPort string) (string, uint16, error) {
	info := tc.containers[name]
	if info == nil {
		return "", 0, fmt.Errorf("no container %s", name)
	}
	inspect, err := tc.client.ContainerInspect(ctx, info.ID)
	if err != nil {
		return "", 0, err
	}
	portKey := nat.Port(containerPort + "/tcp")
	bindings := inspect.NetworkSettings.Ports[portKey]
	if len(bindings) == 0 {
		return "", 0, fmt.Errorf("no binding for %s on %s", containerPort, name)
	}
	host := bindings[0].HostIP
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	var port uint16
	_, _ = fmt.Sscanf(bindings[0].HostPort, "%d", &port)
	return host, port, nil
}

// RestartContainer restarts a container by name and waits for it to become healthy.
// Waiting for healthy avoids racing with the entrypoint's init-time clickhouse-server,
// which entrypoint.sh SIGTERMs before exec'ing the real server when
// CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS=true.
func (tc *TestContainers) RestartContainer(t *testing.T, name string) error {
	ctx := t.Context()
	info := tc.containers[name]
	if info == nil {
		return fmt.Errorf("no container %s", name)
	}
	timeout := 30
	if err := tc.client.ContainerRestart(ctx, info.ID, container.StopOptions{Timeout: &timeout}); err != nil {
		return err
	}
	return tc.waitHealthy(ctx, name, 12*time.Minute, t.Name())
}

func (tc *TestContainers) waitHealthy(ctx context.Context, name string, timeout time.Duration, testName string) error {
	info := tc.containers[name]
	if info == nil {
		return fmt.Errorf("no container %s", name)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		inspect, err := tc.client.ContainerInspect(ctx, info.ID)
		if err == nil && inspect.State != nil && inspect.State.Health != nil {
			if inspect.State.Health.Status == "healthy" {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	tc.dumpContainerInfo(ctx, name, testName)
	if name == "clickhouse" {
		// ClickHouse depends on Keeper; a "not healthy" clickhouse is frequently a
		// symptom of an unresponsive/restarted keeper, so dump it too.
		tc.dumpContainerInfo(ctx, "zookeeper", testName)
		// clickhouse always mounts Azure-backed disks (disk_azblob/backups_azure) from the shared
		// config, so ANY test that (re)starts clickhouse - not only TestAzure* - can fail to become
		// healthy when azurite is unresponsive: clickhouse reads the disk format version over the
		// network at table-load time and aborts startup on an Azure timeout. When the failure looks
		// Azure-related, dump the azurite logs scoped to clickhouse's last (re)start so the azurite
		// side of the timeout is visible (the 12m healthcheck window means "recent" logs would miss it).
		if _, ok := tc.containers["azure"]; ok && (strings.HasPrefix(testName, "TestAzure") || tc.clickhouseFailedOnAzure(ctx)) {
			tc.DumpContainerLogsSince(ctx, "azure", tc.containerStartedAt(ctx, "clickhouse"), testName)
		}
	}
	return fmt.Errorf("container %s not healthy after %v", name, timeout)
}

// testLogPrefix returns a "[TestName] " prefix for dump banners so that, under
// parallel execution, interleaved container dumps can be attributed to the test
// that triggered them. Empty when the dump happens outside a test (e.g. startup).
func testLogPrefix(testName string) string {
	if testName == "" {
		return ""
	}
	return "[" + testName + "] "
}

// containerStateSummary builds a one-line human-readable summary of a container's
// state for dump banners.
//
// RestartCount is included because it is the only signal that survives a Docker
// auto-restart: when a container crashes (e.g. OOM under high RUN_PARALLEL load)
// Docker restarts it and inspect reports status=running, exitCode=0, OOMKilled=false
// again, so restartCount>0 (or a startedAt later than the test start) is often the
// only remaining evidence that the container died mid-test.
func containerStateSummary(inspect container.InspectResponse) string {
	if inspect.State == nil {
		return "unknown"
	}
	state := inspect.State.Status
	if inspect.State.Health != nil {
		state += ", health=" + inspect.State.Health.Status
	}
	if inspect.State.ExitCode != 0 {
		state += fmt.Sprintf(", exitCode=%d", inspect.State.ExitCode)
	}
	if inspect.State.OOMKilled {
		state += ", OOMKilled"
	}
	if inspect.RestartCount != 0 {
		state += fmt.Sprintf(", restartCount=%d", inspect.RestartCount)
	}
	if inspect.State.StartedAt != "" {
		state += ", startedAt=" + inspect.State.StartedAt
	}
	if inspect.State.FinishedAt != "" && inspect.State.FinishedAt != "0001-01-01T00:00:00Z" {
		state += ", finishedAt=" + inspect.State.FinishedAt
	}
	return state
}

// clickhouseFailedOnAzure reports whether the clickhouse container logs contain an Azure SDK
// failure signature, meaning the unhealthy state was caused by an unresponsive azurite rather
// than clickhouse itself. The signatures only appear in exception stack traces, never during
// normal startup/shutdown, so this stays quiet for genuine clickhouse-only failures.
func (tc *TestContainers) clickhouseFailedOnAzure(ctx context.Context) bool {
	info := tc.containers["clickhouse"]
	if info == nil {
		return false
	}
	reader, err := tc.client.ContainerLogs(ctx, info.ID, container.LogsOptions{
		ShowStdout: true, ShowStderr: true, Tail: "500",
	})
	if err != nil {
		return false
	}
	defer func() { _ = reader.Close() }()
	logBytes, _ := io.ReadAll(reader)
	logs := string(logBytes)
	for _, sig := range []string{"Azure::Core::Http", "TransportException"} {
		if strings.Contains(logs, sig) {
			return true
		}
	}
	return false
}

// containerStartedAt returns the container's last start time from docker inspect,
// or the zero time if it can't be determined.
func (tc *TestContainers) containerStartedAt(ctx context.Context, name string) time.Time {
	info := tc.containers[name]
	if info == nil {
		return time.Time{}
	}
	inspect, err := tc.client.ContainerInspect(ctx, info.ID)
	if err != nil || inspect.State == nil || inspect.State.StartedAt == "" {
		return time.Time{}
	}
	started, err := time.Parse(time.RFC3339Nano, inspect.State.StartedAt)
	if err != nil {
		return time.Time{}
	}
	return started
}

// DumpAllContainerLogs dumps state and last 50 log lines for all containers.
// Called when a test fails to aid debugging.
func (tc *TestContainers) DumpAllContainerLogs(ctx context.Context, testName string) {
	tc.mu.Lock()
	names := make([]string, 0, len(tc.containers))
	for name := range tc.containers {
		names = append(names, name)
	}
	tc.mu.Unlock()
	for _, name := range names {
		tc.dumpContainerInfo(ctx, name, testName)
	}
}

// DumpContainerLogsSince dumps state and logs for a single container limited to a time window.
// Used to provide focused diagnostics when a query fails — we only want logs from the moment the
// query started, not the entire test history. A small look-back buffer is added to catch
// shutdown/restart messages that may precede the failure. testName prefixes every emitted line so
// the dump is attributable to the failing test in interleaved parallel CI output (pass "" when the
// caller has no test name to hand).
func (tc *TestContainers) DumpContainerLogsSince(ctx context.Context, name string, since time.Time, testName string) {
	info := tc.containers[name]
	if info == nil {
		return
	}
	inspect, err := tc.client.ContainerInspect(ctx, info.ID)
	if err != nil {
		log.Error().Err(err).Msgf("can't inspect container %s (%s)", name, info.ID[:12])
		return
	}
	if since.IsZero() {
		since = time.Now()
	}
	since = since.Add(-30 * time.Second)
	log.Error().Msgf("=== %scontainer %s (%s) state: %s ===", testLogPrefix(testName), name, info.ID[:12], containerStateSummary(inspect))

	logOpts := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Timestamps: true,
		Since:      fmt.Sprintf("%d", since.Unix()),
	}
	reader, logErr := tc.client.ContainerLogs(ctx, info.ID, logOpts)
	if logErr != nil {
		log.Error().Err(logErr).Msgf("can't get logs for %s", name)
		return
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("can't close DumpContainerLogsSince reader")
		}
	}()
	logBytes, _ := io.ReadAll(reader)
	logText := string(logBytes)
	if name == "azure" {
		logText = filterAzuriteNoise(logText)
	}
	log.Error().Msgf("=== %s%s logs since %s ===\n%s", testLogPrefix(testName), name, since.Format(time.RFC3339), logText)
}

// filterAzuriteNoise drops azurite's idle background-maintenance chatter (blob/queue GC mark-sweep
// loops, per-extent skip lines, account re-init) so the request/error lines that explain a
// clickhouse-to-azurite timeout are not buried under thousands of debug lines. Non-azurite logs
// never contain these markers, so this is a no-op for them.
func filterAzuriteNoise(logText string) string {
	noise := []string{
		"GCManager",
		"FSExtentStore:deleteExtents",
		"AccountDataStore:init",
	}
	lines := strings.Split(logText, "\n")
	kept := lines[:0]
	for _, line := range lines {
		drop := false
		for _, marker := range noise {
			if strings.Contains(line, marker) {
				drop = true
				break
			}
		}
		if !drop {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

func (tc *TestContainers) dumpContainerInfo(ctx context.Context, name string, testName string) {
	info := tc.containers[name]
	if info == nil {
		return
	}
	inspect, err := tc.client.ContainerInspect(ctx, info.ID)
	if err != nil {
		log.Error().Err(err).Msgf("can't inspect container %s (%s)", name, info.ID[:12])
		return
	}
	log.Error().Msgf("=== %scontainer %s (%s) state: %s ===", testLogPrefix(testName), name, info.ID[:12], containerStateSummary(inspect))

	logOpts := container.LogsOptions{ShowStdout: true, ShowStderr: true}
	reader, logErr := tc.client.ContainerLogs(ctx, info.ID, logOpts)
	if logErr != nil {
		log.Error().Err(logErr).Msgf("can't get logs for %s", name)
		return
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("can't close dumpContainerInfo reader")
		}
	}()
	logBytes, _ := io.ReadAll(reader)
	log.Error().Msgf("=== %sfull %s logs ===\n%s", testLogPrefix(testName), name, string(logBytes))

	// For the clickhouse-server container, healthcheck failures may leave
	// nothing in stdout/stderr because clickhouse-server writes auth/config
	// errors only to clickhouse-server.err.log. Dump it so silent 3-minute
	// "not healthy" failures are diagnosable.
	if name == "clickhouse" {
		for _, logPath := range []string{
			"/var/log/clickhouse-server/clickhouse-server.err.log",
			"/var/log/clickhouse-server/clickhouse-server.log",
		} {
			execCmd := osExec.CommandContext(ctx, "docker", "exec", info.ID, "cat", logPath)
			errOut, execErr := execCmd.CombinedOutput()
			if execErr != nil {
				log.Error().Err(execErr).Msgf("can't cat %s in %s: %s", logPath, name, string(errOut))
			} else {
				log.Error().Msgf("=== %sfull %s:%s ===\n%s", testLogPrefix(testName), name, logPath, string(errOut))
			}
		}
	}

	// For the clickhouse-backup container, the server is launched as a
	// background process from the entrypoint and its stdout/stderr is
	// redirected to /tmp/clickhouse-backup-server.log. Dump it so we can see
	// what the API server actually did on test failure.
	if name == "clickhouse-backup" {
		serverLogPath := "/tmp/clickhouse-backup-server.log"
		execCmd := osExec.CommandContext(ctx, "docker", "exec", info.ID, "cat", serverLogPath)
		serverOut, execErr := execCmd.CombinedOutput()
		if execErr != nil {
			log.Error().Err(execErr).Msgf("can't cat %s in %s: %s", serverLogPath, name, string(serverOut))
		} else {
			log.Error().Msgf("=== %sfull %s:%s ===\n%s", testLogPrefix(testName), name, serverLogPath, string(serverOut))
		}
	}
}

func (tc *TestContainers) startContainer(ctx context.Context, name string, cfg *container.Config, hostCfg *container.HostConfig, hostname string, extraAliases ...string) error {
	// Connect to network
	aliases := append([]string{hostname}, extraAliases...)
	networkCfg := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			tc.networkName: {
				Aliases: aliases,
			},
		},
	}
	cfg.Hostname = hostname

	tc.pullImageIfNeeded(ctx, cfg.Image)

	resp, err := tc.client.ContainerCreate(ctx, cfg, hostCfg, networkCfg, nil, fmt.Sprintf("tc_%d_%s", tc.envID, name))
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	if err = tc.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("start %s: %w", name, err)
	}
	tc.mu.Lock()
	tc.containers[name] = &ContainerInfo{ID: resp.ID, Name: name, Hostname: hostname}
	tc.mu.Unlock()
	return nil
}

func (tc *TestContainers) pullImageIfNeeded(ctx context.Context, imageName string) {
	// Check if image already exists locally to avoid unnecessary pull overhead
	_, inspectErr := tc.client.ImageInspect(ctx, imageName)
	if inspectErr == nil {
		log.Debug().Msgf("image %s already exists locally, skipping pull", imageName)
		return
	}
	reader, err := tc.client.ImagePull(ctx, imageName, dockerImage.PullOptions{})
	if err != nil {
		log.Debug().Err(err).Msgf("pull %s (may already exist)", imageName)
		return
	}
	if reader != nil {
		defer func() {
			if closeErr := reader.Close(); closeErr != nil {
				log.Warn().Err(closeErr).Msg("can't close ImagePull reader")
			}
		}()
		_, _ = io.Copy(io.Discard, reader)
	}
}

func envMap(m map[string]string) []string {
	var result []string
	for k, v := range m {
		result = append(result, k+"="+v)
	}
	return result
}

func getEnvDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// Container start methods

// startSSHD starts the shared sshd container used by the SFTP storage and the
// custom rsync backend. Both variants expose the identical connection surface -
// user `sftpuser`, port 2222, writable home /config - so a single set of sftp
// configs and rsync scripts works regardless of image. CH>=23.3 uses the
// OpenSSH 10 linuxserver/openssh-server image so the sftp `copy-data` extension
// is advertised and SFTP.CopyObject exercises the server-side copyDataServerSide
// path; older versions keep the OpenSSH 8.6 panubo image (hardlink fallback).
func (tc *TestContainers) startSSHD(ctx context.Context) error {
	if isModernSSHD() {
		return tc.startSSHDOpenSSH10(ctx)
	}
	return tc.startSSHDPanubo(ctx)
}

func (tc *TestContainers) startSSHDPanubo(ctx context.Context) error {
	return tc.startContainer(ctx, "sshd",
		&container.Config{
			Image: "docker.io/panubo/sshd:latest",
			Env: envMap(map[string]string{
				"SSH_ENABLE_PASSWORD_AUTH": "true",
			}),
			// create sftpuser (home /config), enable password auth, listen on 2222 to
			// match the linuxserver image; rsync-over-ssh reuses the same user/port.
			Cmd: []string{"sh", "-c", strings.Join([]string{
				`adduser -D -h /config -s /bin/sh sftpuser`,
				`echo "sftpuser:JFzMHfVpvTgEd74XXPq6wARA2Qg3AutJ" | chpasswd`,
				`mkdir -p /config && chown -R sftpuser:sftpuser /config`,
				`sed -i "s/^Port 22$/Port 2222/" /etc/ssh/sshd_config`,
				`sed -i "s/^#*PasswordAuthentication .*/PasswordAuthentication yes/" /etc/ssh/sshd_config`,
				`echo "LogLevel DEBUG3" >> /etc/ssh/sshd_config`,
				`/usr/sbin/sshd -D -e -f /etc/ssh/sshd_config`,
			}, " && ")},
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", "nc -z 127.0.0.1 2222"},
				Interval: 1 * time.Second,
				Retries:  30,
			},
		},
		&container.HostConfig{SecurityOpt: []string{"label:disable"}},
		"sshd",
	)
}

// startSSHDOpenSSH10 runs linuxserver/openssh-server (OpenSSH 10, copy-data
// capable). It logs in as non-root `sftpuser` on port 2222 with password + key
// auth (public key installed from PUBLIC_KEY at start), home /config.
func (tc *TestContainers) startSSHDOpenSSH10(ctx context.Context) error {
	pubKey, err := os.ReadFile("sftp/clickhouse-backup_rsa.pub")
	if err != nil {
		return fmt.Errorf("startSSHDOpenSSH10 read sftp pubkey: %w", err)
	}
	return tc.startContainer(ctx, "sshd",
		&container.Config{
			Image: "docker.io/linuxserver/openssh-server:latest",
			Env: envMap(map[string]string{
				"USER_NAME":       "sftpuser",
				"USER_PASSWORD":   "JFzMHfVpvTgEd74XXPq6wARA2Qg3AutJ",
				"PASSWORD_ACCESS": "true",
				"SUDO_ACCESS":     "false",
				"PUBLIC_KEY":      strings.TrimSpace(string(pubKey)),
			}),
			// gate readiness on the full s6 init, not just the sshd bind: under parallel
			// container startup the `chown /config` and PUBLIC_KEY install steps can lag
			// behind the port opening, and connecting early hits a root-owned /config
			// (MkdirAll -> permission denied).
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", `nc -z 127.0.0.1 2222 && [ "$(stat -c %U /config)" = sftpuser ] && [ -s /config/.ssh/authorized_keys ]`},
				Interval: 1 * time.Second,
				Retries:  60,
			},
		},
		&container.HostConfig{SecurityOpt: []string{"label:disable"}},
		"sshd",
	)
}

func (tc *TestContainers) startFTP(ctx context.Context, curDir string) error {
	if tc.isAdvanced {
		proftpdEnv := map[string]string{
			"FTP_USER_NAME":         "test_backup",
			"FTP_USER_PASS":         "test_backup",
			"FTP_MASQUERADEADDRESS": "yes",
			"FTP_PASSIVE_PORTS":     "21100 31100",
			"FTP_MAX_CONNECTIONS":   "255",
		}
		// 23.x+ runs ProFTPD with mod_copy (SITE CPFR/CPTO CopyObject path),
		// 20.0 <= CLICKHOUSE_VERSION <= 22.12 disables mod_copy to cover the FXP CopyObject path
		if v := os.Getenv("CLICKHOUSE_VERSION"); v != "" && v != "head" && compareVersion(v, "23.0") < 0 {
			proftpdEnv["FTP_DISABLE_MOD_COPY"] = "yes"
		}
		return tc.startContainer(ctx, "ftp",
			&container.Config{
				Image: "docker.io/iradu/proftpd:latest",
				Env:   envMap(proftpdEnv),
				Healthcheck: &container.HealthConfig{
					Test:     []string{"CMD-SHELL", "echo 1"},
					Interval: 1 * time.Second,
					Retries:  30,
				},
			},
			&container.HostConfig{
				Binds:       []string{filepath.Join(curDir, "configs/proftpd_arm64_fix.sh") + ":/run.sh"},
				SecurityOpt: []string{"label:disable"},
			},
			"ftp",
		)
	}
	return tc.startContainer(ctx, "ftp",
		&container.Config{
			Image: "docker.io/instantlinux/vsftpd:latest",
			Env: envMap(map[string]string{
				"FTPUSER_NAME":            "test_backup",
				"FTPUSER_PASSWORD_SECRET": "test_backup",
				"PASV_ENABLE":             "YES",
				"PASV_ADDRESS":            "ftp",
				"PASV_ADDR_RESOLVE":       "YES",
				"PASV_MIN_PORT":           "20000",
				"PASV_MAX_PORT":           "21000",
			}),
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", "echo 1"},
				Interval: 1 * time.Second,
				Retries:  30,
			},
		},
		&container.HostConfig{
			Binds: []string{
				filepath.Join(curDir, "configs/vsftpd_secret") + ":/run/secrets/test_backup",
				filepath.Join(curDir, "configs/vsftpd_chroot.conf") + ":/etc/vsftpd.d/chroot.conf",
			},
			SecurityOpt: []string{"label:disable"},
		},
		"ftp",
	)
}

func (tc *TestContainers) startMinio(ctx context.Context, configsDir string) error {
	return tc.startContainer(ctx, "minio",
		&container.Config{
			Image:      fmt.Sprintf("docker.io/minio/minio:%s", getEnvDefault("MINIO_VERSION", "latest")),
			Entrypoint: []string{"/bin/bash"},
			Cmd:        []string{"-c", "mkdir -p /minio/data/clickhouse && minio server /minio/data"},
			Env: envMap(map[string]string{
				"MINIO_ROOT_USER":     "access_key",
				"MINIO_ROOT_PASSWORD": "it_is_my_super_secret_key",
				"MC_CONFIG_DIR":       "/root/.mc",
			}),
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", "ls -lah /minio/data/clickhouse/ && curl -skL https://localhost:9000/"},
				Interval: 1 * time.Second,
				Retries:  60,
			},
		},
		&container.HostConfig{
			Binds: []string{
				filepath.Join(configsDir, "minio_nodelete.sh") + ":/bin/minio_nodelete.sh",
				filepath.Join(configsDir, "minio.crt") + ":/root/.minio/certs/CAs/public.crt",
				filepath.Join(configsDir, "minio.crt") + ":/root/.mc/certs/CAs/public.crt",
				filepath.Join(configsDir, "minio.crt") + ":/root/.minio/certs/public.crt",
				filepath.Join(configsDir, "minio.key") + ":/root/.minio/certs/private.key",
			},
			SecurityOpt: []string{"label:disable"},
		},
		"minio",
	)
}

func (tc *TestContainers) startGCS(ctx context.Context) error {
	gcsBucket := getEnvDefault("QA_GCS_OVER_S3_BUCKET", "")
	cmd := fmt.Sprintf("mkdir -p /data/altinity-qa-test && mkdir -p /data/%s && fake-gcs-server -data /data -scheme http -port 8080 -public-host gcs:8080", gcsBucket)
	return tc.startContainer(ctx, "gcs",
		&container.Config{
			Image:      "fsouza/fake-gcs-server:latest",
			Entrypoint: []string{"/bin/sh"},
			Cmd:        []string{"-c", cmd},
			Env: envMap(map[string]string{
				"QA_GCS_OVER_S3_BUCKET": gcsBucket,
			}),
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", "nc 127.0.0.1 8080 -z"},
				Interval: 1 * time.Second,
				Retries:  30,
			},
		},
		&container.HostConfig{SecurityOpt: []string{"label:disable"}},
		"gcs",
	)
}

func (tc *TestContainers) startAzure(ctx context.Context) error {
	return tc.startContainer(ctx, "azure",
		&container.Config{
			Image: "mcr.microsoft.com/azure-storage/azurite:latest",
			// --skipApiVersionCheck: azure-cli 2.84+ (w/ Azure SDK v12.27+) sends x-ms-version 2026-02-06
			// which Azurite 3.35.0 does not recognise.  Tracked upstream:
			// https://github.com/Azure/Azurite/issues/2623
			Cmd: []string{"azurite", "--debug", "/dev/stderr", "-l", "/data", "--blobHost", "0.0.0.0", "--blobKeepAliveTimeout", "600", "--disableTelemetry", "--skipApiVersionCheck"},
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", "nc 127.0.0.1 10000 -z"},
				Interval: 1 * time.Second,
				Retries:  30,
			},
		},
		&container.HostConfig{
			Mounts: []mount.Mount{
				{Type: mount.TypeTmpfs, Target: "/data", TmpfsOptions: &mount.TmpfsOptions{SizeBytes: 60 * 1024 * 1024}},
			},
			SecurityOpt: []string{"label:disable"},
		},
		"devstoreaccount1.blob.azure",
		"azure",
	)
}

func (tc *TestContainers) startZookeeper(ctx context.Context, configsDir string) error {
	if tc.isAdvanced {
		return tc.startContainer(ctx, "zookeeper",
			&container.Config{
				Image: fmt.Sprintf("docker.io/clickhouse/clickhouse-keeper:%s", getEnvDefault("CLICKHOUSE_KEEPER_VERSION", "latest-alpine")),
				Env: envMap(map[string]string{
					"CLICKHOUSE_RUN_AS_ROOT": "1",
				}),
				Healthcheck: &container.HealthConfig{
					Test:        []string{"CMD-SHELL", `echo ruok | nc 127.0.0.1 2181 | grep imok`},
					Interval:    1 * time.Second,
					Timeout:     2 * time.Second,
					Retries:     10,
					StartPeriod: 1 * time.Second,
				},
			},
			&container.HostConfig{
				Binds: []string{
					filepath.Join(configsDir, "clickhouse-keeper.xml") + ":/etc/clickhouse-keeper/conf.d/clickhouse-keeper.xml",
					filepath.Join(configsDir, "keeper.crt") + ":/etc/clickhouse-keeper/keeper.crt",
					filepath.Join(configsDir, "keeper.key") + ":/etc/clickhouse-keeper/keeper.key",
					filepath.Join(configsDir, "keeper.crt") + ":/etc/clickhouse-keeper/rootCA.crt",
				},
				SecurityOpt: []string{"label:disable"},
			},
			"zookeeper",
		)
	}
	return tc.startContainer(ctx, "zookeeper",
		&container.Config{
			Image: fmt.Sprintf("%s:%s", getEnvDefault("ZOOKEEPER_IMAGE", "docker.io/zookeeper"), getEnvDefault("ZOOKEEPER_VERSION", "3.9.5")),
			Env: envMap(map[string]string{
				"ZOO_4LW_COMMANDS_WHITELIST": "*",
			}),
			Healthcheck: &container.HealthConfig{
				Test:        []string{"CMD-SHELL", `echo ruok | nc 127.0.0.1 2181 | grep imok`},
				Interval:    1 * time.Second,
				Timeout:     2 * time.Second,
				Retries:     10,
				StartPeriod: 1 * time.Second,
			},
		},
		&container.HostConfig{SecurityOpt: []string{"label:disable"}},
		"zookeeper",
	)
}

func (tc *TestContainers) startMySQL(ctx context.Context) error {
	return tc.startContainer(ctx, "mysql",
		&container.Config{
			Image: fmt.Sprintf("docker.io/mysql:%s", getEnvDefault("MYSQL_VERSION", "latest")),
			Cmd:   []string{"--gtid_mode=on", "--enforce_gtid_consistency=ON"},
			Env: envMap(map[string]string{
				"MYSQL_ROOT_PASSWORD": "root",
			}),
			ExposedPorts: nat.PortSet{"3306/tcp": {}},
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", "mysqladmin -p=root ping -h localhost"},
				Timeout:  10 * time.Second,
				Interval: 1 * time.Second,
				Retries:  100,
			},
		},
		&container.HostConfig{
			Mounts: []mount.Mount{
				{Type: mount.TypeTmpfs, Target: "/var/lib/mysql", TmpfsOptions: &mount.TmpfsOptions{SizeBytes: 250 * 1024 * 1024}},
			},
			SecurityOpt: []string{"label:disable"},
		},
		"mysql",
	)
}

func (tc *TestContainers) startPgSQL(ctx context.Context) error {
	return tc.startContainer(ctx, "pgsql",
		&container.Config{
			Image: fmt.Sprintf("docker.io/postgres:%s", getEnvDefault("PGSQL_VERSION", "latest")),
			Cmd:   []string{"postgres", "-c", "wal_level=logical"},
			Env: envMap(map[string]string{
				"POSTGRES_USER":             "root",
				"POSTGRES_PASSWORD":         "root",
				"POSTGRES_HOST_AUTH_METHOD": "md5",
			}),
			ExposedPorts: nat.PortSet{"5432/tcp": {}},
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", "pg_isready"},
				Timeout:  10 * time.Second,
				Interval: 1 * time.Second,
				Retries:  60,
			},
		},
		&container.HostConfig{
			Mounts: []mount.Mount{
				{Type: mount.TypeTmpfs, Target: "/var/lib/postgresql", TmpfsOptions: &mount.TmpfsOptions{SizeBytes: 60 * 1024 * 1024}},
			},
			SecurityOpt: []string{"label:disable"},
		},
		"pgsql",
	)
}

func (tc *TestContainers) commonClickHouseEnv() map[string]string {
	return map[string]string{
		"CLICKHOUSE_VERSION":                   getEnvDefault("CLICKHOUSE_VERSION", "26.3"),
		"CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS": "true",
		"CLICKHOUSE_SKIP_USER_SETUP":           "1",
		"TZ":                                   "UTC",
		"LOG_LEVEL":                            getEnvDefault("LOG_LEVEL", "info"),
		"S3_DEBUG":                             getEnvDefault("S3_DEBUG", "false"),
		"GCS_DEBUG":                            getEnvDefault("GCS_DEBUG", "false"),
		"FTP_DEBUG":                            getEnvDefault("FTP_DEBUG", "false"),
		"SFTP_DEBUG":                           getEnvDefault("SFTP_DEBUG", "false"),
		"AZBLOB_DEBUG":                         getEnvDefault("AZBLOB_DEBUG", "false"),
		"COS_DEBUG":                            getEnvDefault("COS_DEBUG", "false"),
		"CLICKHOUSE_DEBUG":                     getEnvDefault("CLICKHOUSE_DEBUG", "false"),
		"GOCOVERDIR":                           "/tmp/_coverage_/",
		"QA_AWS_ACCESS_KEY":                    os.Getenv("QA_AWS_ACCESS_KEY"),
		"QA_AWS_SECRET_KEY":                    os.Getenv("QA_AWS_SECRET_KEY"),
		"QA_AWS_BUCKET":                        os.Getenv("QA_AWS_BUCKET"),
		"QA_AWS_REGION":                        os.Getenv("QA_AWS_REGION"),
		"AWS_ACCESS_KEY_ID":                    "access_key",
		"AWS_SECRET_ACCESS_KEY":                "it_is_my_super_secret_key",
		"QA_GCS_OVER_S3_ACCESS_KEY":            os.Getenv("QA_GCS_OVER_S3_ACCESS_KEY"),
		"QA_GCS_OVER_S3_SECRET_KEY":            os.Getenv("QA_GCS_OVER_S3_SECRET_KEY"),
		"QA_GCS_OVER_S3_BUCKET":                os.Getenv("QA_GCS_OVER_S3_BUCKET"),
		"QA_ALIBABA_ACCESS_KEY":                os.Getenv("QA_ALIBABA_ACCESS_KEY"),
		"QA_ALIBABA_SECRET_KEY":                os.Getenv("QA_ALIBABA_SECRET_KEY"),
		"QA_TENCENT_SECRET_ID":                 os.Getenv("QA_TENCENT_SECRET_ID"),
		"QA_TENCENT_SECRET_KEY":                os.Getenv("QA_TENCENT_SECRET_KEY"),
		"GCS_ENCRYPTION_KEY":                   os.Getenv("GCS_ENCRYPTION_KEY"),
		"AWS_EC2_METADATA_DISABLED":            "true",
	}
}

func (tc *TestContainers) clickHouseBinds(curDir, configsDir string) []string {
	backupBin := getEnvDefault("CLICKHOUSE_BACKUP_BIN", filepath.Join(curDir, "../../clickhouse-backup/clickhouse-backup-race"))
	backupBinFips := getEnvDefault("CLICKHOUSE_BACKUP_BIN_FIPS", filepath.Join(curDir, "../../clickhouse-backup/clickhouse-backup-race-fips"))
	coverageDir := filepath.Join(curDir, "_coverage_")
	_ = os.MkdirAll(coverageDir, 0o755)

	binds := []string{
		backupBin + ":/usr/bin/clickhouse-backup",
		backupBinFips + ":/usr/bin/clickhouse-backup-fips",
		filepath.Join(curDir, "credentials.json") + ":/etc/clickhouse-backup/credentials.json",
		coverageDir + ":/tmp/_coverage_/",
		filepath.Join(configsDir, "install_delve.sh") + ":/tmp/install_delve.sh",
	}

	// backup config files
	configFiles := []string{
		"config-azblob.yml", "config-azblob-embedded.yml", "config-azblob-embedded-url.yml",
		"config-custom-kopia.yml", "config-custom-restic.yml", "config-custom-rsync.yml",
		"config-database-mapping.yml",
		"config-ftp.yaml", "config-ftp-old.yaml",
		"config-gcs.yml", "config-gcs-custom-endpoint.yml", "config-gcs-parallel.yml",
		"config-s3.yml", "config-s3-embedded.yml", "config-s3-embedded-url.yml",
		"config-s3-embedded-local.yml", "config-s3-nodelete.yml", "config-s3-plain-embedded.yml",
		"config-sftp-auth-key.yaml", "config-sftp-auth-password.yaml",
	}
	// template files (copied with .template suffix)
	templateFiles := []string{
		"config-azblob-sas.yml",
		"config-cos.yml",
		"config-gcs-embedded-url.yml",
		"config-s3-fips.yml",
		"config-s3-alibabacloud.yml",
		"config-s3-glacier.yml",
	}

	for _, f := range configFiles {
		binds = append(binds, filepath.Join(configsDir, f)+":/etc/clickhouse-backup/"+f)
	}
	for _, f := range templateFiles {
		binds = append(binds, filepath.Join(configsDir, f)+":/etc/clickhouse-backup/"+f+".template")
	}

	// ClickHouse server configs
	serverConfigs := map[string]string{
		"enable-access_management.xml": "/etc/clickhouse-server/users.d/enable-access_management.xml",
		"backup-user.xml":              "/etc/clickhouse-server/users.d/backup-user.xml",
		"server.crt":                   "/etc/clickhouse-server/server.crt",
		"server.key":                   "/etc/clickhouse-server/server.key",
		"dhparam.pem":                  "/etc/clickhouse-server/dhparam.pem",
		"ssl.xml":                      "/etc/clickhouse-server/config.d/ssl.xml",
		"clickhouse-config.xml":        "/etc/clickhouse-server/config.d/clickhouse-config.xml",
		"minio.crt":                    "/etc/clickhouse-server/minio.crt",
		"keeper.crt":                   "/etc/clickhouse-server/keeper.crt",
		"keeper.key":                   "/etc/clickhouse-server/keeper.key",
	}
	for src, dst := range serverConfigs {
		binds = append(binds, filepath.Join(configsDir, src)+":"+dst)
	}

	// rootCA cert
	binds = append(binds, filepath.Join(configsDir, "server.crt")+":/etc/clickhouse-server/rootCA.crt")

	if tc.isAdvanced {
		binds = append(binds,
			filepath.Join(configsDir, "custom_entrypoint.sh")+":/custom_entrypoint.sh",
			filepath.Join(configsDir, "dynamic_settings.sh")+":/docker-entrypoint-initdb.d/dynamic_settings.sh",
		)
	}

	return binds
}

// writeMacrosVersionXML - generate the `{version}` macro for the clickhouse-backup config
// path/object_disk_path, it isolates parallel CI matrix jobs sharing the same remote bucket
// (real GCS, real S3, COS). Generated on the host and bind-mounted unconditionally because
// dynamic_settings.sh runs only in advanced mode, while old versions (1.x, 19.x) need the
// isolation too: with the macro missing `{version}` stays literal, so all simple-mode matrix
// jobs collide on the same remote path and delete each other's fixed-name backups.
// GITHUB_RUN_ID is appended because the version alone doesn't isolate CONCURRENT workflow
// runs (master push + PR) executing the same matrix job: they share the remote path and
// delete/overwrite each other's fixed-name backups mid-test.
func (tc *TestContainers) writeMacrosVersionXML() (string, error) {
	version := strings.ReplaceAll(getEnvDefault("CLICKHOUSE_VERSION", "26.3"), ".", "_")
	if runID := os.Getenv("GITHUB_RUN_ID"); runID != "" {
		version += "-" + runID
	}
	content := fmt.Sprintf("<yandex>\n  <macros>\n    <version>%s</version>\n  </macros>\n</yandex>\n", version)
	fPath := filepath.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup-macros-version-env%d.xml", tc.envID))
	if err := os.WriteFile(fPath, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("writeMacrosVersionXML %s: %w", fPath, err)
	}
	return fPath, nil
}

func (tc *TestContainers) startClickHouse(ctx context.Context, curDir, configsDir string) error {
	chImage := fmt.Sprintf("docker.io/%s:%s",
		getEnvDefault("CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server"),
		getEnvDefault("CLICKHOUSE_VERSION", "26.3"))

	env := tc.commonClickHouseEnv()
	if tc.isAdvanced {
		env["KEEPER_TLS_ENABLED"] = getEnvDefault("KEEPER_TLS_ENABLED", "false")
	}

	binds := tc.clickHouseBinds(curDir, configsDir)
	macrosVersionPath, err := tc.writeMacrosVersionXML()
	if err != nil {
		return err
	}
	binds = append(binds, macrosVersionPath+":/etc/clickhouse-server/config.d/macros_version.xml")

	// Add shared volume mounts
	for i, vol := range tc.sharedVolumes {
		targets := []string{"/var/lib/clickhouse", "/hdd1_data", "/hdd2_data", "/hdd3_data"}
		binds = append(binds, vol+":"+targets[i])
	}

	cfg := &container.Config{
		Image:        chImage,
		User:         "root",
		Env:          envMap(env),
		ExposedPorts: nat.PortSet{"8123/tcp": {}, "9000/tcp": {}},
		Healthcheck: &container.HealthConfig{
			Test:        []string{"CMD-SHELL", "clickhouse client -q 'SELECT 1'"},
			Interval:    10 * time.Second,
			Retries:     6,
			StartPeriod: 60 * time.Second,
		},
	}
	if tc.isAdvanced {
		cfg.Entrypoint = []string{"/custom_entrypoint.sh"}
	}

	hostCfg := &container.HostConfig{
		Binds: binds,
		PortBindings: nat.PortMap{
			"9000/tcp": {nat.PortBinding{HostIP: "0.0.0.0"}},
			"8123/tcp": {nat.PortBinding{HostIP: "0.0.0.0"}},
		},
		CapAdd:        []string{"SYS_PTRACE", "SYS_NICE"},
		SecurityOpt:   []string{"label:disable"},
		RestartPolicy: container.RestartPolicy{Name: "always"},
	}

	return tc.startContainer(ctx, "clickhouse", cfg, hostCfg, "clickhouse")
}

func (tc *TestContainers) startClickHouseBackup(ctx context.Context, curDir, configsDir string) error {
	chImage := fmt.Sprintf("docker.io/%s:%s",
		getEnvDefault("CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server"),
		getEnvDefault("CLICKHOUSE_VERSION", "26.3"))

	env := tc.commonClickHouseEnv()

	// Mount shared volumes from clickhouse
	var binds []string
	for i, vol := range tc.sharedVolumes {
		targets := []string{"/var/lib/clickhouse", "/hdd1_data", "/hdd2_data", "/hdd3_data"}
		binds = append(binds, vol+":"+targets[i])
	}

	// Also mount backup binary and configs (same as clickhouse)
	binds = append(binds, tc.clickHouseBinds(curDir, configsDir)...)

	cfg := &container.Config{
		Image:      chImage,
		User:       "root",
		Entrypoint: []string{"/bin/bash", "-xce", "sleep infinity"},
		Env:        envMap(env),
		ExposedPorts: nat.PortSet{
			"7171/tcp":  {},
			"40001/tcp": {},
		},
		Healthcheck: &container.HealthConfig{
			Test:        []string{"CMD-SHELL", `bash -c "exit 0"`},
			Interval:    1 * time.Second,
			Timeout:     1 * time.Second,
			Retries:     5,
			StartPeriod: 1 * time.Second,
		},
	}

	hostCfg := &container.HostConfig{
		Binds: binds,
		PortBindings: nat.PortMap{
			"7171/tcp": {nat.PortBinding{HostIP: "0.0.0.0"}},
		},
		CapAdd:      []string{"SYS_PTRACE", "SYS_NICE"},
		SecurityOpt: []string{"label:disable"},
	}

	return tc.startContainer(ctx, "clickhouse-backup", cfg, hostCfg, "clickhouse-backup")
}

// CopyToContainer copies a file from the host into a container.
func (tc *TestContainers) CopyToContainer(ctx context.Context, containerName, srcPath, dstPath string) error {
	info := tc.containers[containerName]
	if info == nil {
		return fmt.Errorf("no container %s", containerName)
	}
	// Use docker cp via exec since the API is complex with tar archives
	args := []string{"cp", srcPath, info.ID + ":" + dstPath}
	cmd := strings.Join(append([]string{"docker"}, args...), " ")
	log.Debug().Msg(cmd)
	return execDockerCmd(ctx, 180*time.Second, args...)
}

// CopyFromContainer copies a file from a container to the host.
func (tc *TestContainers) CopyFromContainer(ctx context.Context, containerName, srcPath, dstPath string) error {
	info := tc.containers[containerName]
	if info == nil {
		return fmt.Errorf("no container %s", containerName)
	}
	args := []string{"cp", info.ID + ":" + srcPath, dstPath}
	return execDockerCmd(ctx, 180*time.Second, args...)
}

func execDockerCmd(ctx context.Context, timeout time.Duration, args ...string) error {
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	log.Debug().Msgf("docker %s", strings.Join(args, " "))
	cmd := osExec.CommandContext(ctx2, "docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Debug().Msgf("docker %s: %s %v", strings.Join(args, " "), string(output), err)
	}
	return err
}
