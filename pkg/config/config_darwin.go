package config

import (
	"os"
	"syscall"
	"unsafe"

	"github.com/go-faster/errors"
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
	var err error
	var executable string
	if cfg.General.IONicePriority != "" {
		var nicePriority ioNiceClass
		executable, err = os.Executable()
		if err != nil {
			log.Warn().Msgf("can't get current executable path: %v", err)
		}
		if nicePriority, err = parseIONicePriority(cfg.General.IONicePriority); err != nil {
			return err
		}
		param := iopolParam{scope: iopolScopeProcess, ioType: iopolTypeDisk, policy: iopolDefault}
		switch nicePriority {
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
	if err = syscall.Setpriority(syscall.PRIO_PROCESS, 0, cfg.General.CPUNicePriority); err != nil {
		log.Warn().Msgf("can't set CPU priority %d, error: %v, use `sudo setcap cap_sys_nice+ep %s` to fix it", cfg.General.CPUNicePriority, err, executable)
	}
	return nil
}
