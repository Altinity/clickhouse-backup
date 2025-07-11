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
	pidPath := path.Join(backupPath, "clickhouse-backup.pid")

	// Read existing PID file if exists
	pidData, err := os.ReadFile(pidPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No PID file exists
		}
		return err
	}

	// Parse PID, command and timestamp from file
	parts := strings.SplitN(strings.TrimSpace(string(pidData)), "|", 3)
	if len(parts) < 3 {
		return nil // Invalid PID file - we'll overwrite it
	}

	pid, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil // Invalid PID - we'll overwrite it
	}

	// Check if process exists
	process, err := os.FindProcess(pid)
	if err != nil {
		return nil // Process doesn't exist
	}

	// Check if process is still running
	if err := process.Signal(syscall.Signal(); err == nil {
		exeCommand := command
		if len(parts) > 1 {
			exeCommand = parts[1]
		}
		return fmt.Errorf("another clickhouse-backup %s command is already running (PID %d)", exeCommand, pid)
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
