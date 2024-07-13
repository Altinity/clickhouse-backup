package config

import (
	"github.com/apex/log"
	"syscall"
	"os"
)

func (cfg *Config) SetPriority() error {
	var executable string
	if err := syscall.Setpriority(0, 0, cfg.General.CPUNicePriority); err != nil {
		executable, err = os.Executable()
		if err != nil {
			log.Warnf("can't get current executable path: %v", err)
		}
		log.Warnf("can't set CPU priority %s, error: %v, use `sudo setcap cap_sys_nice+ep %s` to fix it", cfg.General.CPUNicePriority, err, executable)
	}
	return nil
}
