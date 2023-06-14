package log_helper

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func SetLogLevelFromString(logLevel string) {
	allLogLevels := map[string]zerolog.Level{
		"error":   zerolog.ErrorLevel,
		"warning": zerolog.WarnLevel,
		"info":    zerolog.InfoLevel,
		"debug":   zerolog.DebugLevel,
	}
	level := zerolog.InfoLevel
	var ok bool
	if level, ok = allLogLevels[logLevel]; !ok {
		log.Warn().Msgf("unexpected log_level=%v, will apply `info`", logLevel)
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
}
