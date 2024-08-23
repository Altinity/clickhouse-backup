package config

import (
	"github.com/rs/zerolog/log"
	"github.com/xyproto/gionice"
	"os"
)

func (cfg *Config) SetPriority() error {
	var err error
	var executable string
	if cfg.General.IONicePriority != "" {
		var nicePriority gionice.PriClass
		executable, err = os.Executable()
		if err != nil {
			log.Warn().Msgf("can't get current executable path: %v", err)
		}
		if nicePriority, err = gionice.Parse(cfg.General.IONicePriority); err != nil {
			return err
		}
		if err = gionice.SetIDPri(0, nicePriority, 7, gionice.IOPRIO_WHO_PGRP); err != nil {
			log.Warn().Msgf("can't set i/o priority %s, error: %v, use `sudo setcap cap_sys_nice+ep %s` to fix it", cfg.General.IONicePriority, err, executable)
		}
	}
	if err = gionice.SetNicePri(0, gionice.PRIO_PROCESS, cfg.General.CPUNicePriority); err != nil {
		log.Warn().Msgf("can't set CPU priority %v, error: %v, use `sudo setcap cap_sys_nice+ep %s` to fix it", cfg.General.CPUNicePriority, err, executable)
	}
	return nil
}
