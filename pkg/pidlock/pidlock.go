package pidlock

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/shirou/gopsutil/v3/process"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func CheckAndCreatePidFile(backupName string, command string) error {
	if backupName == "" {
		return fmt.Errorf("backupName is required")
	}
	pidPath := path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))
	// Check existing PID file
	existingPidData, err := os.ReadFile(pidPath)
	if err == nil {
		// Parse existing PID data
		parts := strings.SplitN(strings.TrimSpace(string(existingPidData)), "|", 3)
		if len(parts) < 3 {
			log.Warn().Msgf("Invalid PID file format in %s - will be overwritten", pidPath)
		} else if pid, err := strconv.Atoi(parts[0]); err == nil {
			if proc, err := os.FindProcess(pid); err == nil {
				if err := proc.Signal(syscall.Signal(0)); err == nil {
					if procInfo, infoErr := process.NewProcess(int32(pid)); infoErr == nil {
						if cmdLine, cmdLineErr := procInfo.Cmdline(); cmdLineErr == nil {
							return fmt.Errorf(
								"another clickhouse-backup `%s` command is already running %s (pid=%d, pidPath=%s, cmdLine=%s)",
								parts[1], parts[2], pid, pidPath, cmdLine,
							)
						} else {
							log.Warn().Err(cmdLineErr).Str("pidPath", pidPath).Int("pid", pid).Msg("can't get cmdLine")
						}
					} else {
						log.Warn().Err(infoErr).Str("pidPath", pidPath).Int("pid", pid).Msg("can't get process info")
					}
				}
			}
		}
	}

	// Write new PID file
	pid := fmt.Sprintf("%d|%s|%s", os.Getpid(), command, time.Now().Format(time.RFC3339))
	return os.WriteFile(pidPath, []byte(pid), 0644)
}

func RemovePidFile(backupName string) {
	pidPath := path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))
	_ = os.Remove(pidPath)
}
