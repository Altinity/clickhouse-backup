package config

import (
	"syscall"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// iopolicysys(2) constants, see sys/resource.h; setiopolicy_np(3) is a thin libc wrapper around this syscall
const (
	iopolCmdSet       = 2
	iopolTypeDisk     = 0
	iopolScopeProcess = 0
	iopolDefault      = 0
	iopolImportant    = 1
	iopolThrottle     = 3
	iopolStandard     = 5
)

type iopolParam struct {
	scope  int32
	ioType int32
	policy int32
}

func (cfg *Config) SetPriority() error {
	if cfg.General.IONicePriority != "" {
		nicePriority, err := parseIONicePriority(cfg.General.IONicePriority)
		if err != nil {
			return err
		}
		param := iopolParam{scope: iopolScopeProcess, ioType: iopolTypeDisk, policy: iopolDefault}
		switch nicePriority {
		case ioNiceNone:
			param.policy = iopolDefault
		case ioNiceRealtime:
			param.policy = iopolImportant
		case ioNiceBestEffort:
			param.policy = iopolStandard
		case ioNiceIdle:
			param.policy = iopolThrottle
		default:
			return errors.Errorf("unknown %v nice priority", nicePriority)
		}
		if _, _, errno := syscall.Syscall(syscall.SYS_IOPOLICYSYS, iopolCmdSet, uintptr(unsafe.Pointer(&param)), 0); errno != 0 {
			log.Warn().Msgf("can't set i/o priority %s, error: %v", cfg.General.IONicePriority, errno)
		}
	}
	if err := syscall.Setpriority(syscall.PRIO_PROCESS, 0, cfg.General.CPUNicePriority); err != nil {
		log.Warn().Msgf("can't set CPU priority %d, error: %v, raising priority requires root on macOS, run with sudo or set the `Nice` key in the launchd plist", cfg.General.CPUNicePriority, err)
	}
	return nil
}
