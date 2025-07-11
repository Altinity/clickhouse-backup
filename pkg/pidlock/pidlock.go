package pidlock

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func CheckAndCreatePidFile(backupName string, command string) error {
	pidPath := path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))
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

func RemovePidFile(backupName string) {
	pidPath := path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))
	_ = os.Remove(pidPath)
}
