package backup

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"
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
	return os.WriteFile(pidPath, []byte(pid), 0644)
}

func (b *Backuper) checkAndCreatePidFile(backupName string, command string) error {
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

	process, err := os.FindProcess(pid)
	if err != nil {
		return nil
	}

	if err := process.Signal(syscall.Signal(0)); err == nil {
		return fmt.Errorf("another clickhouse-backup %s command is already running %s (PID %d)", parts[1], parts[2], pid)
	}

	return nil
}

func (b *Backuper) removePidFile(backupName string) {
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
