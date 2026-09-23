package backup

import (
	"path"
	"testing"

	"github.com/google/shlex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

func TestEmbeddedClusterCreateWorkerCommand(t *testing.T) {
	command := embeddedClusterCreateWorkerCommand("create_remote", "backup 1", "base", "db.t*,db2.x", []string{"2024-01", "(1,'a b')"}, true, true, true)
	assert.Equal(t, `create_remote --tables="db.t*,db2.x" --partitions="2024-01" --partitions="(1,'a b')" --diff-from-remote="base" --schema --skip-check-parts-columns --delete-source --embedded-on-cluster-worker "backup 1"`, command)
	// the API server parses the command back with shlex, quoted values must survive as single arguments
	args, err := shlex.Split(command)
	require.NoError(t, err)
	assert.Equal(t, []string{"create_remote", "--tables=db.t*,db2.x", "--partitions=2024-01", "--partitions=(1,'a b')", "--diff-from-remote=base", "--schema", "--skip-check-parts-columns", "--delete-source", "--embedded-on-cluster-worker", "backup 1"}, args)

	// empty values and unset flags are omitted
	assert.Equal(t, `create --embedded-on-cluster-worker "b"`, embeddedClusterCreateWorkerCommand("create", "b", "", "", nil, false, false, false))
}

func TestEmbeddedClusterRestoreWorkerCommand(t *testing.T) {
	command := embeddedClusterRestoreWorkerCommand("restore", "b", "db.*", []string{"db:db2"}, []string{"t:t2"}, []string{"p1"}, false, true, true, true, true, true)
	assert.Equal(t, `restore --tables="db.*" --restore-database-mapping="db:db2" --restore-table-mapping="t:t2" --partitions="p1" --data --rm --ignore-dependencies --restore-schema-as-attach --skip-empty-tables --embedded-on-cluster-worker "b"`, command)
	assert.Equal(t, `restore_remote --schema --embedded-on-cluster-worker "b"`, embeddedClusterRestoreWorkerCommand("restore_remote", "b", "", nil, nil, nil, true, false, false, false, false, false))
}

func TestShlexQuote(t *testing.T) {
	for _, v := range []string{`plain`, `with space`, `with "quotes"`, `back\slash`, `mixed "a\b" c`} {
		args, err := shlex.Split("cmd " + shlexQuote(v))
		require.NoError(t, err, v)
		assert.Equal(t, []string{"cmd", v}, args, v)
	}
}

func TestChQuote(t *testing.T) {
	assert.Equal(t, `'plain'`, chQuote("plain"))
	assert.Equal(t, `'it\'s'`, chQuote("it's"))
	assert.Equal(t, `'back\\slash\'q'`, chQuote(`back\slash'q`))
}

func TestEmbeddedClusterHostRemoteBackupActions(t *testing.T) {
	host := embeddedClusterHost{HostName: "clickhouse-2", Port: 9000, ShardNum: 1, ReplicaNum: 2}
	assert.Equal(t, `remote('clickhouse-2:9000', 'system', 'backup_actions', 'default', 'p\'w')`, host.remoteBackupActions("default", "p'w", false))
	// system.clusters has no `secure` column, the transport follows clickhouse.secure of the initiator
	host.Port = 9440
	assert.Equal(t, `remoteSecure('clickhouse-2:9440', 'system', 'backup_actions', 'backup', '')`, host.remoteBackupActions("backup", "", true))
	assert.Equal(t, "clickhouse-2:9440 (shard 1, replica 2)", host.String())
}

func TestEmbeddedMetadataDir(t *testing.T) {
	existing := map[string]bool{path.Join("/var/lib/backups/b1", "shards/1/replicas/2", "metadata"): true}
	exists := func(p string) bool { return existing[p] }
	// cluster prefixed layout wins when present
	assert.Equal(t, "/var/lib/backups/b1/shards/1/replicas/2/metadata", embeddedMetadataDir("/var/lib/backups/b1", "shards/1/replicas/2", exists))
	// legacy flat layout of backups made before ON CLUSTER support
	assert.Equal(t, "/var/lib/backups/b2/metadata", embeddedMetadataDir("/var/lib/backups/b2", "shards/1/replicas/2", exists))
	// no cluster mode, never probes
	assert.Equal(t, "/var/lib/backups/b1/metadata", embeddedMetadataDir("/var/lib/backups/b1", "", func(string) bool { t.Fatal("must not probe"); return false }))
}

func TestRemoteTableMetadataJSON(t *testing.T) {
	assert.Equal(t, "b1/shards/1/replicas/2/metadata/db/t%20x.json", remoteTableMetadataJSON("b1", "shards/1/replicas/2", "db", "t x"))
	assert.Equal(t, "b1/metadata/db/t.json", remoteTableMetadataJSON("b1", "", "db", "t"))
}

func TestEmbeddedOnClusterWorkerValidation(t *testing.T) {
	cfg := &config.Config{}
	b := NewBackuper(cfg)
	b.ch.BreakConnectOnError = true
	// the flag is a no-op when unset, whatever the config is
	require.NoError(t, b.validateEmbeddedOnClusterWorker())
	assert.False(t, b.embeddedClusterInitiator())

	b.EmbeddedOnClusterWorker = true
	require.Error(t, b.validateEmbeddedOnClusterWorker())
	cfg.ClickHouse.UseEmbeddedBackupRestore = true
	require.Error(t, b.validateEmbeddedOnClusterWorker())
	cfg.ClickHouse.UseEmbeddedBackupRestoreCluster = "{cluster}"
	require.NoError(t, b.validateEmbeddedOnClusterWorker())
	// a worker never drives other nodes
	assert.False(t, b.embeddedClusterInitiator())
	assert.False(t, b.embeddedClusterWorkerSkipsDDL())
	cfg.General.RestoreSchemaOnCluster = "{cluster}"
	assert.True(t, b.embeddedClusterWorkerSkipsDDL())

	b.EmbeddedOnClusterWorker = false
	assert.True(t, b.embeddedClusterInitiator())
	assert.False(t, b.embeddedClusterWorkerSkipsDDL())
	b.DryRun = true
	assert.False(t, b.embeddedClusterInitiator())
}

func TestEmbeddedClusterTagsAndLayout(t *testing.T) {
	b := NewBackuper(&config.Config{})
	b.ch.BreakConnectOnError = true
	assert.Equal(t, "embedded", b.embeddedBackupTags())
	b.embeddedClusterName = "sharded_cluster"
	b.embeddedClusterPrefix = "shards/1/replicas/2"
	assert.Equal(t, "embedded,cluster=sharded_cluster", b.embeddedBackupTags())

	// the remote layout of per table .json and shadow data follows the backup tags, not the current config
	b.applyEmbeddedClusterLayout(&metadata.BackupMetadata{Tags: "embedded"})
	assert.Equal(t, "", b.embeddedClusterFilesPrefix)
	b.applyEmbeddedClusterLayout(&metadata.BackupMetadata{Tags: b.embeddedBackupTags()})
	assert.Equal(t, "shards/1/replicas/2", b.embeddedClusterFilesPrefix)
}
