package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/server/metrics"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// newServerCliContext parses the same global flags as main.go, as `server` gets them
func newServerCliContext(t *testing.T, args ...string) *cli.Command {
	t.Helper()
	var parsed *cli.Command
	cmd := &cli.Command{
		Name:                      "clickhouse-backup",
		DisableSliceFlagSeparator: true,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config", Aliases: []string{"c"}, Value: config.DefaultConfigPath},
			&cli.StringSliceFlag{Name: "environment-override", Aliases: []string{"env"}},
		},
		Action: func(_ context.Context, c *cli.Command) error {
			parsed = c
			return nil
		},
	}
	require.NoError(t, cmd.Run(context.Background(), append([]string{"clickhouse-backup"}, args...)))
	return parsed
}

// TestServerKeepsEnvOverride - `server --env` shall apply to every config reload and to every cliApp re-entry
func TestServerKeepsEnvOverride(t *testing.T) {
	r := require.New(t)
	configPath := filepath.Join(t.TempDir(), "config.yml")
	r.NoError(os.WriteFile(configPath, []byte("general:\n  remote_storage: none\n  backups_to_keep_remote: 3\n"), 0644))
	t.Setenv("BACKUPS_TO_KEEP_REMOTE", "")
	r.NoError(os.Unsetenv("BACKUPS_TO_KEEP_REMOTE"))

	cliCtx := newServerCliContext(t, "-c", configPath, "--env", "BACKUPS_TO_KEEP_REMOTE=7")
	api := &APIServer{cliCtx: cliCtx, configPath: configPath, metrics: metrics.NewAPIMetrics()}
	api.metrics.RegisterMetrics()

	for i := 0; i < 2; i++ {
		cfg, err := api.ReloadConfig(nil, "test")
		r.NoError(err)
		r.Equal(7, cfg.General.BackupsToKeepRemote, "reload %d shall apply server --env", i)
		r.Equal(7, api.GetConfig().General.BackupsToKeepRemote)
	}

	r.Equal([]string{"clickhouse-backup", "-c", configPath, "--command-id", "5", "--env", "BACKUPS_TO_KEEP_REMOTE=7", "upload", "--env", "BACKUPS_TO_KEEP_REMOTE=1", "backup1"},
		api.cliArgs(5, []string{"upload", "--env", "BACKUPS_TO_KEEP_REMOTE=1", "backup1"}),
		"server --env shall go before the command args, so --env of the command itself wins")
}
