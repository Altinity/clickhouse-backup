//go:build integration

package main

import (
	"os"
	"testing"
	"time"
)

func TestSkipTablesAndSkipTableEngines(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)
	version, err := env.ch.GetVersion(t.Context())
	r.NoError(err)
	env.queryWithNoError(r, "CREATE DATABASE test_skip_tables")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_skip_tables.test_merge_tree (id UInt64, s String) ENGINE=MergeTree() ORDER BY id")
	env.queryWithNoError(r, "CREATE TABLE IF NOT EXISTS test_skip_tables.test_memory (id UInt64) ENGINE=Memory")
	env.queryWithNoError(r, "CREATE MATERIALIZED VIEW IF NOT EXISTS test_skip_tables.test_mv (id UInt64) ENGINE=MergeTree() ORDER BY id AS SELECT id FROM test_skip_tables.test_merge_tree")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		query := "CREATE LIVE VIEW IF NOT EXISTS test_skip_tables.test_live_view AS SELECT count() FROM test_skip_tables.test_merge_tree"
		allowExperimentalAnalyzer, err := env.ch.TurnAnalyzerOffIfNecessary(version, query, "")
		r.NoError(err)
		env.queryWithNoError(r, query)
		r.NoError(env.ch.TurnAnalyzerOnIfNecessary(version, query, allowExperimentalAnalyzer))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		query := "CREATE WINDOW VIEW IF NOT EXISTS test_skip_tables.test_window_view ENGINE=MergeTree() ORDER BY s AS SELECT count(), s, tumbleStart(w_id) as w_start FROM test_skip_tables.test_merge_tree GROUP BY s, tumble(now(), INTERVAL '5' SECOND) AS w_id"
		allowExperimentalAnalyzer, err := env.ch.TurnAnalyzerOffIfNecessary(version, query, "")
		r.NoError(err)
		env.queryWithNoError(r, query)
		r.NoError(env.ch.TurnAnalyzerOnIfNecessary(version, query, allowExperimentalAnalyzer))
	}
	// create
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_TABLES=*.test_merge_tree clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create skip_table_pattern")
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/test_merge_tree.json"))
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/test_memory.json")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/test_mv.json")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ls -la /var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/test_live_view.json")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_table_pattern/metadata/test_skip_tables/test_window_view.json")
	}

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_TABLE_ENGINES=memory,materializedview,windowview,liveview clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create skip_engines")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/test_merge_tree.json")
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/test_memory.json"))
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/test_mv.json"))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ls -la /var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/test_live_view.json"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/skip_engines/metadata/test_skip_tables/test_window_view.json"))
	}

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_table_pattern")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local skip_engines")

	//upload
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 CLICKHOUSE_SKIP_TABLES=*.test_memory clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload test_skip_full_backup")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json")
	r.Error(env.DockerExec("minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_memory.json"))
	env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_mv.json")
	env.DockerExecNoError(r, "minio", "bash", "-ce", "ls -la /minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json")
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 CLICKHOUSE_SKIP_TABLE_ENGINES=memory,materializedview,liveview,windowview clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload test_skip_full_backup")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json")
	r.Error(env.DockerExec("minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_memory.json"))
	r.Error(env.DockerExec("minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_mv.json"))
	env.DockerExecNoError(r, "minio", "bash", "-ce", "ls -la /minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		r.Error(env.DockerExec("minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		r.Error(env.DockerExec("minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json"))
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload test_skip_full_backup")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_memory.json")
	env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_mv.json")
	env.DockerExecNoError(r, "minio", "bash", "-ce", "ls -la /minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		env.DockerExecNoError(r, "minio", "ls", "-la", "/minio/data/clickhouse/backup/cluster/0/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json")
	}

	//download
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml upload test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 CLICKHOUSE_SKIP_TABLES=*.test_merge_tree clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download test_skip_full_backup")
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json"))
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_memory.json")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_mv.json")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ls -la /var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json")
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 CLICKHOUSE_SKIP_TABLE_ENGINES=memory,materializedview,liveview,windowview clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json")
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_memory.json"))
	r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_mv.json"))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ls -la /var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json"))
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		r.Error(env.DockerExec("clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json"))
	}
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local test_skip_full_backup")

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "USE_RESUMABLE_STATE=0 clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml download test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_merge_tree.json")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_memory.json")
	env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_mv.json")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ls -la /var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/*inner*.json")
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_live_view.json")
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		env.DockerExecNoError(r, "clickhouse-backup", "ls", "-la", "/var/lib/clickhouse/backup/test_skip_full_backup/metadata/test_skip_tables/test_window_view.json")
	}

	//restore
	r.NoError(env.dropDatabase("test_skip_tables", false))

	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_TABLES=*.test_memory clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore test_skip_full_backup")

	result := make([]struct {
		Name string `ch:"name"`
	}, 0)
	r.NoError(env.ch.SelectContext(t.Context(), &result, "SELECT name FROM system.tables WHERE database='test_skip_tables'"))
	expectedTables := 3
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		expectedTables = 4
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		expectedTables = 6
	}
	//*.inner.target.* for WINDOW VIEW created only after 22.6
	//LIVE VIEW removed in 25.11+
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.6") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		expectedTables = 7
	}

	found := false
	for _, item := range result {
		if item.Name == "test_memory" {
			found = true
			break
		}
	}
	r.False(found, "invalid tables in test_skip_tables, test_memory shall not present %#v", result)
	r.Equal(expectedTables, len(result), "invalid tables length in test_skip_tables %#v", result)

	r.NoError(env.dropDatabase("test_skip_tables", false))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_TABLE_ENGINES=memory,materializedview,liveview,windowview clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore --schema test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "CLICKHOUSE_SKIP_TABLE_ENGINES=memory,materializedview,liveview,windowview clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore --data test_skip_full_backup")

	result = make([]struct {
		Name string `ch:"name"`
	}, 0)
	expectedTables = 2
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		expectedTables = 3
	}
	r.NoError(env.ch.Select(&result, "SELECT name FROM system.tables WHERE database='test_skip_tables' AND engine='MergeTree'"))
	r.Equal(expectedTables, len(result), "invalid tables engines length in test_skip_tables %#v", result)

	result = make([]struct {
		Name string `ch:"name"`
	}, 0)
	r.NoError(env.ch.Select(&result, "SELECT name FROM system.tables WHERE database='test_skip_tables' AND engine IN ('Memory','MaterializedView','LiveView','WindowView')"))
	r.Equal(0, len(result), "unexpected tables engines in test_skip_tables %#v", result)

	r.NoError(env.dropDatabase("test_skip_tables", false))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml restore test_skip_full_backup")
	result = make([]struct {
		Name string `ch:"name"`
	}, 0)
	r.NoError(env.ch.SelectContext(t.Context(), &result, "SELECT name FROM system.tables WHERE database='test_skip_tables'"))
	expectedTables = 4
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.3") >= 0 {
		expectedTables = 5
	}
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "21.12") >= 0 {
		expectedTables = 7
	}
	//*.inner.target.* for WINDOW VIEW created only after 22.6
	//LIVE VIEW removed in 25.11+
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "22.6") >= 0 && compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "25.11") < 0 {
		expectedTables = 8
	}
	r.Equal(expectedTables, len(result), "unexpected tables after full restore in test_skip_tables %#v", result)

	r.NoError(env.dropDatabase("test_skip_tables", false))
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete local test_skip_full_backup")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml delete remote test_skip_full_backup")
	env.Cleanup(t, r)
}
