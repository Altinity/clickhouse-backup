package config

import (
	"os"
	"syscall"

	"github.com/rs/zerolog/log"
)

// ioprio_set(2) constants, see linux/ioprio.h
const (
	ioprioWhoPgrp     = 2
	ioprioClassShift  = 13
	ioprioBestEffort7 = 7
)

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
		ioprio := uintptr(nicePriority)<<ioprioClassShift | ioprioBestEffort7
		if _, _, errno := syscall.Syscall(syscall.SYS_IOPRIO_SET, ioprioWhoPgrp, 0, ioprio); errno != 0 {
			log.Warn().Msgf("can't set i/o priority %s, error: %v, use `sudo setcap cap_sys_nice+ep %s` to fix it", cfg.General.IONicePriority, errno, executable)
		}
	}
	if err = syscall.Setpriority(syscall.PRIO_PROCESS, 0, cfg.General.CPUNicePriority); err != nil {
		log.Warn().Msgf("can't set CPU priority %v, error: %v, use `sudo setcap cap_sys_nice+ep %s` to fix it", cfg.General.CPUNicePriority, err, executable)
	}
	return nil
}
