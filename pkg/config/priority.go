package config

import (
	"fmt"
	"strings"
)

// ioNiceClass mirrors the Linux ionice(1) scheduling classes; other platforms map them to the closest equivalent.
type ioNiceClass int

const (
	ioNiceNone ioNiceClass = iota
	ioNiceRealtime
	ioNiceBestEffort
	ioNiceIdle
)

func parseIONicePriority(ioprio string) (ioNiceClass, error) {
	switch strings.ToLower(ioprio) {
	case "0", "none":
		return ioNiceNone, nil
	case "1", "realtime":
		return ioNiceRealtime, nil
	case "2", "best-effort":
		return ioNiceBestEffort, nil
	case "3", "idle":
		return ioNiceIdle, nil
	}
	return 0, fmt.Errorf("could not parse %s as an IOPRIO_CLASS constant", ioprio)
}
