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
		return fmt.Errorf("backupName is required")
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

	// Check existing PID file
	existingPidData, err := os.ReadFile(pidPath)
	if err == nil {
		// Parse existing PID data
		parts := strings.SplitN(strings.TrimSpace(string(existingPidData)), "|", 3)
		if len(parts) < 3 {
			log.Warn().Msgf("Invalid PID file format in %s - will be overwritten", pidPath)
		} else if pid, err := strconv.Atoi(parts[0]); err == nil {
			if process, err := os.FindProcess(pid); err == nil {
				if err := process.Signal(syscall.Signal(0)); err == nil {
					return fmt.Errorf("another clickhouse-backup %s command is already running %s (PID %d)", 
						parts[1], parts[2], pid)
				}
			}
		}
	}

	// Write new PID file
	pid := fmt.Sprintf("%d|%s|%s", os.Getpid(), command, time.Now().UTC().Format(time.RFC3339))
	return os.WriteFile(pidPath, []byte(pid), 0644)
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
