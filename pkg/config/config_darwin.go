package config

import (
	"github.com/rs/zerolog/log"
	"os"
	"syscall"
)

func (cfg *Config) SetPriority() error {
	var executable string
	if err := syscall.Setpriority(0, 0, cfg.General.CPUNicePriority); err != nil {
		executable, err = os.Executable()
		if err != nil {
			log.Warn().Msgf("can't get current executable path: %v", err)
		}
		log.Warn().Msgf("can't set CPU priority %s, error: %v, use `sudo setcap cap_sys_nice+ep %s` to fix it", cfg.General.CPUNicePriority, err, executable)
	}
	return nil
}
