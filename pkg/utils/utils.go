package utils

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

const (
	day  = time.Minute * 60 * 24
	year = 365 * day
)

var CleanBackupNameRE = regexp.MustCompile(`[\t\r\n ]+|\.{2,}|[\\/]+`)

// FormatBytes - Convert bytes to human-readable string
func FormatBytes(i uint64) string {
	const (
		KiB = 1024
		MiB = 1048576
		GiB = 1073741824
		TiB = 1099511627776
	)
	switch {
	case i >= TiB:
		return fmt.Sprintf("%.02fTiB", float64(i)/TiB)
	case i >= GiB:
		return fmt.Sprintf("%.02fGiB", float64(i)/GiB)
	case i >= MiB:
		return fmt.Sprintf("%.02fMiB", float64(i)/MiB)
	case i >= KiB:
		return fmt.Sprintf("%.02fKiB", float64(i)/KiB)
	default:
		return fmt.Sprintf("%dB", i)
	}
}

func HumanizeDuration(d time.Duration) string {
	if d < day {
		return d.Round(time.Millisecond).String()
	}
	var b strings.Builder
	if d >= year {
		years := d / year
		if _, err := fmt.Fprintf(&b, "%dy", years); err != nil {
			log.Warn().Msgf("HumanizeDuration error: %v", err)
		}
		d -= years * year
	}
	days := d / day
	d -= days * day
	if _, err := fmt.Fprintf(&b, "%dd%s", days, d); err != nil {
		log.Warn().Msgf("HumanizeDuration error: %v", err)
	}
	return b.String()
}

func ExecCmd(ctx context.Context, timeout time.Duration, cmd string, args ...string) error {
	out, err := ExecCmdOut(ctx, timeout, cmd, args...)
	log.Info().Msg(out)
	return err
}

func ExecCmdOut(ctx context.Context, timeout time.Duration, cmd string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	log.Info().Msgf("%s %s", cmd, strings.Join(args, " "))
	out, err := exec.CommandContext(ctx, cmd, args...).CombinedOutput()
	cancel()
	return string(out), err
}
