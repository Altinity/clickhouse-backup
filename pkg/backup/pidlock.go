package backup

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"syscall"
)

func (b *Backuper) createPidFile(backupName, command string) error {
	if backupName == "" {
		return fmt.Errorf("backupName is empty")
	}
	backupPath := path.Join(b.DefaultDataPath, "backup", backupName)
	if b.cfg.ClickHouse.UseEmbeddedBackupRestore && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		if diskPath := b.DiskToPathMap[b.cfg.ClickHouse.EmbeddedBackupDisk]; diskPath != "" {
			backupPath = diskPath
		}
	}
	pidPath := path.Join(backupPath, "clickhouse-backup.pid")

	// Create backup directory if not exists
	if err := os.MkdirAll(backupPath, 0750); err != nil {
		return fmt.Errorf("can't create backup directory %s: %v", backupPath, err)
	}

	// Write our PID, command and timestamp to file
	pid := fmt.Sprintf("%d|%s|%s", os.Getpid(), command, time.Now().UTC().Format(time.RFC3339))
	return os.WriteFile(pidPath, []byte(strconv.Itoa(pid)), 0644)
}

func (b *Backuper) checkPidFile(backupName, command string) error {
	if backupName == "" {
		return fmt.Errorf("backupName is empty")
	}
	backupPath := path.Join(b.DefaultDataPath, "backup", backupName)
	if b.cfg.ClickHouse.UseEmbeddedBackupRestore && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		if diskPath := b.DiskToPathMap[b.cfg.ClickHouse.EmbeddedBackupDisk]; diskPath != "" {
			backupPath = diskPath
		}
	}
	pidPath := path.Join(backupPath, command+pidFileSuffix)

	// Read existing PID file if exists
	pidData, err := os.ReadFile(pidPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No PID file exists
		}
		return err
	}

	pid, err := strconv.Atoi(string(pidData))
	if err != nil {
		return nil // Invalid PID file - we'll overwrite it
	}

	// Check if process exists
	process, err := os.FindProcess(pid)
	if err != nil {
		return nil // Process doesn't exist
	}

	// Check if process is still running
	if err := process.Signal(syscall.Signal(0)); err == nil {
		return fmt.Errorf("another clickhouse-backup instance (PID %d) is already working with backup %s", pid, backupName)
	}

	return nil
}

func (b *Backuper) removePidFile(backupName, command string) {
	if backupName == "" {
		return
	}
	backupPath := path.Join(b.DefaultDataPath, "backup", backupName)
	if b.cfg.ClickHouse.UseEmbeddedBackupRestore && b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		if diskPath := b.DiskToPathMap[b.cfg.ClickHouse.EmbeddedBackupDisk]; diskPath != "" {
			backupPath = diskPath
		}
	}
	pidPath := path.Join(backupPath, "clickhouse-backup.pid")
	_ = os.Remove(pidPath)
}
