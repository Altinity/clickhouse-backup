package config

import (
	"github.com/apex/log"
	"syscall"
)

func (cfg *Config) SetPriority() error {
	if err := syscall.Setpriority(0, 0, cfg.General.CPUNicePriority); err != nil {
		log.Warnf("can't set CPU priority %s, error: %v", cfg.General.CPUNicePriority, err)
	}
	return nil
}
