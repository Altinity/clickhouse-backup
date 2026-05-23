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

// ExtractBackupNameFromCommand parses a backup_actions command string (e.g.
// `upload --resumable=1 my_backup`, `delete local my_backup`,
// `restore_remote --rm my_backup`) and returns the backup name (last
// positional token), or empty string if the command does not target a
// single backup (watch, clean, list, kill, ...).
func ExtractBackupNameFromCommand(command string) string {
	fields := strings.Fields(command)
	if len(fields) < 2 {
		return ""
	}
	cmd := fields[0]
	switch cmd {
	case "create", "upload", "download", "restore", "delete",
		"create_remote", "restore_remote":
		// proceed
	default:
		return ""
	}
	// pick the last token that doesn't start with "-" or "--"
	for i := len(fields) - 1; i >= 1; i-- {
		tok := fields[i]
		if strings.HasPrefix(tok, "-") {
			continue
		}
		// for `delete local NAME` / `delete remote NAME` skip the
		// "local"/"remote" subcommand token
		if cmd == "delete" && (tok == "local" || tok == "remote") {
			continue
		}
		return strings.Trim(tok, `"'`)
	}
	return ""
}

// RemovePidFileForCommand extracts the backup name from a backup_actions
// command string and removes the corresponding pid file, if any. Safe to
// call for commands that don't have a pid file (no-op).
func RemovePidFileForCommand(command string) {
	backupName := ExtractBackupNameFromCommand(command)
	if backupName == "" {
		return
	}
	RemovePidFile(backupName)
}
