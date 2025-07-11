package pidlock

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCheckAndCreatePidFile(t *testing.T) {
	t.Run("CreatesValidPidFile", func(t *testing.T) {
		backupName := "test_backup"
		command := "create"

		err := CheckAndCreatePidFile(backupName, command)
		require.NoError(t, err)

		pidPath := path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))
		data, err := os.ReadFile(pidPath)
		require.NoError(t, err)

		parts := strings.Split(string(data), "|")
		require.Len(t, parts, 3)
		pid, err := strconv.Atoi(parts[0])
		require.NoError(t, err)
		require.Equal(t, os.Getpid(), pid)
		require.Equal(t, command, parts[1])
		_, err = time.Parse(time.RFC3339, parts[2])
		require.NoError(t, err)

		// Cleanup
		RemovePidFile(backupName)
	})

	t.Run("DetectsRunningProcess", func(t *testing.T) {
		backupName := "running_test"
		command := "create"

		// Create initial pid file
		err := CheckAndCreatePidFile(backupName, command)
		require.NoError(t, err)

		// Try to create again - should detect running process
		err = CheckAndCreatePidFile(backupName, command)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already running")

		// Cleanup
		RemovePidFile(backupName)
	})

	t.Run("OverwritesInvalidPidFile", func(t *testing.T) {
		backupName := "invalid_pid_test"
		pidPath := path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))

		// Create invalid pid file
		err := os.WriteFile(pidPath, []byte("invalid-content"), 0644)
		require.NoError(t, err)

		// Should overwrite invalid file
		err = CheckAndCreatePidFile(backupName, "create")
		require.NoError(t, err)

		// Verify new content is valid
		data, err := os.ReadFile(pidPath)
		require.NoError(t, err)
		parts := strings.Split(string(data), "|")
		require.Len(t, parts, 3)

		// Cleanup
		RemovePidFile(backupName)
	})

	t.Run("HandlesNonExistentProcess", func(t *testing.T) {
		backupName := "nonexistent_test"
		pidPath := path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))

		// Create pid file with non-existent PID
		nonExistentPid := 999999
		pidContent := fmt.Sprintf("%d|create|%s", nonExistentPid, time.Now().Format(time.RFC3339))
		err := os.WriteFile(pidPath, []byte(pidContent), 0644)
		require.NoError(t, err)

		// Should overwrite since process doesn't exist
		err = CheckAndCreatePidFile(backupName, "create")
		require.NoError(t, err)

		// Cleanup
		RemovePidFile(backupName)
	})

	t.Run("FailsOnEmptyBackupName", func(t *testing.T) {
		err := CheckAndCreatePidFile("", "create")
		require.Error(t, err)
		require.Contains(t, err.Error(), "backupName is required")
	})
}

func TestRemovePidFile(t *testing.T) {
	t.Run("RemovesExistingPidFile", func(t *testing.T) {
		backupName := "remove_test"

		// Create pid file first
		err := CheckAndCreatePidFile(backupName, "create")
		require.NoError(t, err)

		pidPath := path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))
		_, err = os.Stat(pidPath)
		require.NoError(t, err)

		// Remove it
		RemovePidFile(backupName)

		// Verify removed
		_, err = os.Stat(pidPath)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("SilentlyHandlesMissingPidFile", func(t *testing.T) {
		backupName := "nonexistent_remove_test"
		pidPath := path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))

		// Ensure file doesn't exist
		_, err := os.Stat(pidPath)
		require.True(t, os.IsNotExist(err))

		// Should not error
		RemovePidFile(backupName)
	})
}

func TestSignalHandling(t *testing.T) {
	t.Run("DetectsRunningProcessViaSignal", func(t *testing.T) {
		backupName := "signal_test"
		pidPath := path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))

		// Create pid file with current process
		pidContent := fmt.Sprintf("%d|create|%s", os.Getpid(), time.Now().Format(time.RFC3339))
		err := os.WriteFile(pidPath, []byte(pidContent), 0644)
		require.NoError(t, err)

		// Should detect running process via signal check
		err = CheckAndCreatePidFile(backupName, "create")
		require.Error(t, err)
		require.Contains(t, err.Error(), "already running")

		// Cleanup
		RemovePidFile(backupName)
	})
}
