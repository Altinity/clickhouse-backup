package backup

import (
	"os"
	"path"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/stretchr/testify/assert"
)

// TestFindDependentBackups - `delete local|remote` must detect backups which reference the deleted
// backup via `required_backup`, see https://github.com/Altinity/clickhouse-backup/issues/1493
func TestFindDependentBackups(t *testing.T) {
	// full <- inc1 <- inc2, plus an unrelated full backup
	localBackups := []LocalBackup{
		{BackupMetadata: metadata.BackupMetadata{BackupName: "full"}},
		{BackupMetadata: metadata.BackupMetadata{BackupName: "inc1", RequiredBackup: "full"}},
		{BackupMetadata: metadata.BackupMetadata{BackupName: "inc2", RequiredBackup: "inc1"}},
		{BackupMetadata: metadata.BackupMetadata{BackupName: "other_full"}},
	}
	remoteBackups := make([]storage.Backup, len(localBackups))
	for i := range localBackups {
		remoteBackups[i] = storage.Backup{BackupMetadata: localBackups[i].BackupMetadata}
	}
	// two increments of the same full backup, both must be reported
	localBackups = append(localBackups, LocalBackup{BackupMetadata: metadata.BackupMetadata{BackupName: "inc1_alt", RequiredBackup: "full"}})

	assert.Equal(t, []string{"inc1", "inc1_alt"}, findDependentBackups("full", localBackupsChainLinks(localBackups)))
	assert.Equal(t, []string{"inc2"}, findDependentBackups("inc1", localBackupsChainLinks(localBackups)))
	assert.Empty(t, findDependentBackups("inc2", localBackupsChainLinks(localBackups)))
	assert.Empty(t, findDependentBackups("other_full", localBackupsChainLinks(localBackups)))
	assert.Empty(t, findDependentBackups("not_exists", localBackupsChainLinks(localBackups)))

	assert.Equal(t, []string{"inc1"}, findDependentBackups("full", remoteBackupsChainLinks(remoteBackups)))
	assert.Equal(t, []string{"inc2"}, findDependentBackups("inc1", remoteBackupsChainLinks(remoteBackups)))
	assert.Empty(t, findDependentBackups("inc2", remoteBackupsChainLinks(remoteBackups)))

	// broken metadata with a self-reference must not report the backup as its own dependent
	assert.Empty(t, findDependentBackups("self", localBackupsChainLinks([]LocalBackup{
		{BackupMetadata: metadata.BackupMetadata{BackupName: "self", RequiredBackup: "self"}},
	})))
}

func TestCleanDir(t *testing.T) {
	t.Run("Test deletion of nonexistent directory",
		func(t *testing.T) {
			b := &Backuper{}

			dir := path.Join(t.TempDir(), t.Name(), "does-not-exist")
			if err := b.cleanDir(dir); err != nil {
				t.Fatalf("unexpected error when deleting nonexistent dir: %v", err)
			}
		},
	)

	t.Run("Test deletion of existing directory",
		func(t *testing.T) {
			b := &Backuper{}

			dir := t.TempDir()
			if err := os.MkdirAll(dir, 0644); err != nil {
				t.Fatalf("unexpected error while creating temporary directory: %v", err)
			}
			if err := b.cleanDir(dir); err != nil {
				t.Fatalf("unexpected error while deleting existing dir: %v", err)
			}
			if err := b.cleanDir(dir); err != nil {
				t.Fatalf("unexpected error during back to back invocation of delete: %v", err)
			}
		},
	)
}
