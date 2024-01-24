package config

import (
	"github.com/apex/log"
	"github.com/xyproto/gionice"
)

func (cfg *Config) SetPriority() error {
	var err error
	if cfg.General.IONicePriority != "" {
		var nicePriority gionice.PriClass
		if nicePriority, err = gionice.Parse(cfg.General.IONicePriority); err != nil {
			return err
		}
		if err = gionice.SetIDPri(0, nicePriority, 7, gionice.IOPRIO_WHO_PGRP); err != nil {
			log.Warnf("can't set i/o priority %s, error: %v", cfg.General.IONicePriority, err)
		}
	}
	if err = gionice.SetNicePri(0, gionice.PRIO_PROCESS, cfg.General.CPUNicePriority); err != nil {
		log.Warnf("can't set CPU priority %v, error: %v", cfg.General.CPUNicePriority, err)
	}
	return nil
}
