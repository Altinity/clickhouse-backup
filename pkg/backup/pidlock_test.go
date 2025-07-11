package backup

import (
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPidLockFlow(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	backuper := &Backuper{
		DefaultDataPath: tmpDir,
		cfg: &config.Config{
			ClickHouse: config.ClickHouseConfig{
				UseEmbeddedBackupRestore: false,
				EmbeddedBackupDisk:       "",
			},
		},
		DiskToPathMap: make(map[string]string),
	}
	backupName := "test_backup"

	// Test basic pid file creation
	t.Run("CreatePidFileSuccess", func(t *testing.T) {
		err := backuper.checkAndCreatePidFile(backupName, "create")
		require.NoError(t, err)

		pidPath := path.Join(tmpDir, "backup", backupName, "clickhouse-backup.pid")
		data, err := os.ReadFile(pidPath)
		require.NoError(t, err)

		parts := strings.Split(string(data), "|")
		require.Len(t, parts, 3)
		_, err = strconv.Atoi(parts[0])
		require.NoError(t, err)
		require.Equal(t, "create", parts[1])
	})

	// Test pid check with running process
	t.Run("CheckRunningProcess", func(t *testing.T) {
		// Use current process PID to simulate running process
		currentPid := os.Getpid()
		pidContent := fmt.Sprintf("%d|create|%s", currentPid, time.Now().Format(time.RFC3339))
		backupPath := path.Join(tmpDir, "backup", backupName)
		err := os.MkdirAll(backupPath, 0750)
		require.NoError(t, err)

		pidPath := path.Join(backupPath, "clickhouse-backup.pid")
		err = os.WriteFile(pidPath, []byte(pidContent), 0644)
		require.NoError(t, err)

		err = backuper.checkAndCreatePidFile(backupName, "create")
		require.Error(t, err)
		require.Contains(t, err.Error(), "already running")
	})

	// Test pid creation with empty backup name
	t.Run("EmptyBackupName", func(t *testing.T) {
		err := backuper.checkAndCreatePidFile("", "create")
		require.Error(t, err)
		require.Contains(t, err.Error(), "backupName is required")
	})

	// Test invalid pid file format
	t.Run("InvalidPidFile", func(t *testing.T) {
		backupPath := path.Join(tmpDir, "backup", backupName)
		pidPath := path.Join(backupPath, "clickhouse-backup.pid")

		// Write invalid content
		err := os.WriteFile(pidPath, []byte("invalid-pid-content"), 0644)
		require.NoError(t, err)

		err = backuper.checkAndCreatePidFile(backupName, "create")
		require.NoError(t, err) // Invalid format should be ignored
	})

	// Test pid file cleanup
	t.Run("RemovePidFile", func(t *testing.T) {
		backuper.removePidFile(backupName)
		backupPath := path.Join(tmpDir, "backup", backupName)
		pidPath := path.Join(backupPath, "clickhouse-backup.pid")
		_, err := os.Stat(pidPath)
		require.True(t, os.IsNotExist(err))
	})

	// Test with embedded backup disk configuration
	t.Run("EmbeddedBackupPath", func(t *testing.T) {
		backuper.cfg.ClickHouse.UseEmbeddedBackupRestore = true
		backuper.cfg.ClickHouse.EmbeddedBackupDisk = "custom_disk"
		backuper.DiskToPathMap = map[string]string{"custom_disk": path.Join(tmpDir, "custom_disk")}

		err := backuper.checkAndCreatePidFile(backupName, "create")
		require.NoError(t, err)

		expectedPath := path.Join(tmpDir, "custom_disk", "clickhouse-backup.pid")
		_, err = os.Stat(expectedPath)
		require.NoError(t, err)

		// Cleanup
		backuper.cfg.ClickHouse.UseEmbeddedBackupRestore = false
		backuper.removePidFile(backupName)
	})

	// Test non-existent process PID
	t.Run("NonExistingProcess", func(t *testing.T) {
		// Use an invalid PID number
		pidContent := fmt.Sprintf("%d|create|%s", 999999, time.Now().Format(time.RFC3339))
		backupPath := path.Join(tmpDir, "backup", backupName)
		pidPath := path.Join(backupPath, "clickhouse-backup.pid")
		err := os.WriteFile(pidPath, []byte(pidContent), 0644)
		require.NoError(t, err)

		err = backuper.checkAndCreatePidFile(backupName, "create")
		require.NoError(t, err) // Should pass since process doesn't exist
	})

	// Test directory creation handling
	t.Run("DirectoryCreation", func(t *testing.T) {
		nonExistingDir := path.Join(tmpDir, "new_backup_dir")
		backuper.DefaultDataPath = nonExistingDir

		err := backuper.checkAndCreatePidFile(backupName, "create")
		require.NoError(t, err)
		_, err = os.Stat(path.Join(nonExistingDir, "backup", backupName))
		require.NoError(t, err)
	})
}
