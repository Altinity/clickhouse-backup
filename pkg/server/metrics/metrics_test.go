package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// RegisterMetrics registers into the global prometheus registry, so it can run only once per test binary
func TestRegisterMetrics(t *testing.T) {
	r := require.New(t)
	NewAPIMetrics().RegisterMetrics()

	families, err := prometheus.DefaultGatherer.Gather()
	r.NoError(err)
	registered := make(map[string]bool, len(families))
	for _, family := range families {
		registered[family.GetName()] = true
	}
	// every gauge created by RegisterMetrics shall be exposed on /metrics
	for _, name := range []string{
		"clickhouse_backup_failed_parts_count",
		"clickhouse_backup_last_backup_size_local",
		"clickhouse_backup_last_backup_size_remote",
		"clickhouse_backup_number_backups_remote",
		"clickhouse_backup_number_backups_remote_broken",
		"clickhouse_backup_number_backups_local",
		"clickhouse_backup_number_backups_remote_expected",
		"clickhouse_backup_number_backups_local_expected",
		"clickhouse_backup_in_progress_commands",
		"clickhouse_backup_local_data_size",
	} {
		r.True(registered[name], "%s is not registered", name)
	}
}
