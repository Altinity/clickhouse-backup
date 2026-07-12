package backup

import (
	"regexp"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestWatchScheduleState(t *testing.T, name, full, increment string) *watchScheduleState {
	st := &watchScheduleState{schedule: config.WatchSchedule{Name: name, Full: full, Increment: increment}}
	var err error
	st.fullCron, err = config.WatchCronParser.Parse(full)
	require.NoError(t, err)
	if increment != "" {
		st.incrementCron, err = config.WatchCronParser.Parse(increment)
		require.NoError(t, err)
	}
	return st
}

func TestComputeNextWatchScheduleEvents(t *testing.T) {
	// full every day at 00:00, increment every 15 minutes
	st := newTestWatchScheduleState(t, "daily", "0 0 * * *", "*/15 * * * *")
	now := time.Date(2026, 7, 12, 12, 7, 0, 0, time.UTC)
	earliest := computeNextWatchScheduleEvents([]*watchScheduleState{st}, now)
	assert.Equal(t, time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC), st.nextFull)
	assert.Equal(t, time.Date(2026, 7, 12, 12, 15, 0, 0, time.UTC), st.nextIncrement)
	assert.Equal(t, st.nextIncrement, earliest)

	// schedule without increment
	st2 := newTestWatchScheduleState(t, "weekly", "0 0 * * 0", "")
	earliest = computeNextWatchScheduleEvents([]*watchScheduleState{st2}, now)
	assert.True(t, st2.nextIncrement.IsZero())
	assert.Equal(t, st2.nextFull, earliest)

	// earliest across multiple schedules
	earliest = computeNextWatchScheduleEvents([]*watchScheduleState{st2, st}, now)
	assert.Equal(t, st.nextIncrement, earliest)
}

func TestDueBackupType(t *testing.T) {
	st := newTestWatchScheduleState(t, "daily", "0 0 * * *", "*/15 * * * *")
	st.prevBackupName = "daily-shard0-full-20260712000000"
	now := time.Date(2026, 7, 12, 12, 7, 0, 0, time.UTC)
	computeNextWatchScheduleEvents([]*watchScheduleState{st}, now)

	assert.Equal(t, "", st.dueBackupType(now), "nothing due before next tick")
	assert.Equal(t, "increment", st.dueBackupType(st.nextIncrement))

	// full and increment on the same tick, full wins
	now = time.Date(2026, 7, 12, 23, 59, 0, 0, time.UTC)
	computeNextWatchScheduleEvents([]*watchScheduleState{st}, now)
	assert.Equal(t, st.nextFull, st.nextIncrement)
	assert.Equal(t, "full", st.dueBackupType(st.nextFull))

	// increment without an existing chain promotes to full
	st.prevBackupName = ""
	now = time.Date(2026, 7, 12, 12, 7, 0, 0, time.UTC)
	computeNextWatchScheduleEvents([]*watchScheduleState{st}, now)
	assert.Equal(t, "full", st.dueBackupType(st.nextIncrement))
}

func TestValidateWatchParamsSchedules(t *testing.T) {
	// --schedule is mutually exclusive with --watch-interval / --full-interval
	b := NewBackuper(config.DefaultConfig())
	err := b.ValidateWatchParams("1h", "", "", []string{"name=x,full=0 0 * * *"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")

	b = NewBackuper(config.DefaultConfig())
	err = b.ValidateWatchParams("", "24h", "", []string{"name=x,full=0 0 * * *"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")

	// CLI --schedule populates cfg, default watch_interval/full_interval from config don't conflict
	b = NewBackuper(config.DefaultConfig())
	require.NoError(t, b.ValidateWatchParams("", "", "", []string{"name=x,full=0 0 * * *"}))
	require.Len(t, b.cfg.General.WatchSchedules, 1)
	assert.Equal(t, "x", b.cfg.General.WatchSchedules[0].Name)

	// invalid schedule from CLI propagates parse error
	b = NewBackuper(config.DefaultConfig())
	require.Error(t, b.ValidateWatchParams("", "", "", []string{"garbage"}))
}

func TestAvgCronPeriod(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 7, 0, 0, time.UTC)
	every15min, err := config.WatchCronParser.Parse("*/15 * * * *")
	require.NoError(t, err)
	hourly, err := config.WatchCronParser.Parse("0 * * * *")
	require.NoError(t, err)
	assert.Equal(t, 15*time.Minute, avgCronPeriod(every15min, now, 4))
	assert.Equal(t, time.Hour, avgCronPeriod(hourly, now, 4))
	// the warning condition in newWatchScheduleStates: full fires as often or more often than increment
	assert.LessOrEqual(t, avgCronPeriod(every15min, now, 4), avgCronPeriod(hourly, now, 4))
}

func TestFindPreviousWatchBackup(t *testing.T) {
	templateRE := regexp.MustCompile(`^daily-shard0-\S+-\S+$`)
	newRemoteBackup := func(name, broken string) storage.Backup {
		return storage.Backup{BackupMetadata: metadata.BackupMetadata{BackupName: name}, Broken: broken}
	}
	remoteBackups := []storage.Backup{
		newRemoteBackup("daily-shard0-full-20260710000000", ""),
		newRemoteBackup("weekly-shard0-full-20260711000000", ""),
		newRemoteBackup("daily-shard0-increment-20260711000000", ""),
		newRemoteBackup("daily-shard0-increment-20260712000000", "broken"),
		newRemoteBackup("other-backup", ""),
	}
	prevName, prevType := findPreviousWatchBackup(remoteBackups, templateRE)
	assert.Equal(t, "daily-shard0-increment-20260711000000", prevName, "shall skip broken and non-matching backups")
	assert.Equal(t, "increment", prevType)

	prevName, prevType = findPreviousWatchBackup(remoteBackups, regexp.MustCompile(`^weekly-shard0-\S+-\S+$`))
	assert.Equal(t, "weekly-shard0-full-20260711000000", prevName)
	assert.Equal(t, "full", prevType)

	prevName, prevType = findPreviousWatchBackup(remoteBackups, regexp.MustCompile(`^missing-\S+$`))
	assert.Equal(t, "", prevName)
	assert.Equal(t, "", prevType)
}
