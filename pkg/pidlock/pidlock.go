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

func pidPath(backupName string) string {
	return path.Join(os.TempDir(), fmt.Sprintf("clickhouse-backup.%s.pid", backupName))
}

// runningProcess returns the pid, command and start time recorded in the pid file of backupName
// when that process is still alive, ok=false when there is no pid file or the process is gone
func runningProcess(backupName string) (pid int, command string, since string, ok bool) {
	pidFile := pidPath(backupName)
	existingPidData, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, "", "", false
	}
	parts := strings.SplitN(strings.TrimSpace(string(existingPidData)), "|", 3)
	if len(parts) < 3 {
		log.Warn().Msgf("Invalid PID file format in %s - will be overwritten", pidFile)
		return 0, "", "", false
	}
	pid, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", "", false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return 0, "", "", false
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		return 0, "", "", false
	}
	return pid, parts[1], parts[2], true
}

// IsRunning reports whether the pid file of backupName points to a live process,
// used to skip cleanup of shadow freezes which belong to a parallel or the current operation
func IsRunning(backupName string) bool {
	_, _, _, ok := runningProcess(backupName)
	return ok
}

func CheckAndCreatePidFile(backupName string, command string) error {
	if backupName == "" {
		return fmt.Errorf("backupName is required")
	}
	pidFile := pidPath(backupName)
	// Check existing PID file
	if pid, runningCommand, since, ok := runningProcess(backupName); ok {
		if procInfo, infoErr := process.NewProcess(int32(pid)); infoErr == nil {
			if cmdLine, cmdLineErr := procInfo.Cmdline(); cmdLineErr == nil {
				return fmt.Errorf(
					"another clickhouse-backup `%s` command is already running %s (pid=%d, pidPath=%s, cmdLine=%s)",
					runningCommand, since, pid, pidFile, cmdLine,
				)
			} else {
				log.Warn().Err(cmdLineErr).Str("pidPath", pidFile).Int("pid", pid).Msg("can't get cmdLine")
			}
		} else {
			log.Warn().Err(infoErr).Str("pidPath", pidFile).Int("pid", pid).Msg("can't get process info")
		}
	}

	// Write new PID file
	pidData := fmt.Sprintf("%d|%s|%s", os.Getpid(), command, time.Now().Format(time.RFC3339))
	return os.WriteFile(pidFile, []byte(pidData), 0644)
}

func RemovePidFile(backupName string) {
	_ = os.Remove(pidPath(backupName))
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
