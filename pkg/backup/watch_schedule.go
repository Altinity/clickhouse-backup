package backup

import (
	"context"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/server/metrics"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"

	"github.com/pkg/errors"
	cron "github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"
)

// watchScheduleState - runtime state of one named cron driven backup chain, see https://github.com/Altinity/clickhouse-backup/issues/1354
type watchScheduleState struct {
	schedule       config.WatchSchedule
	fullCron       cron.Schedule
	incrementCron  cron.Schedule
	template       string
	templateRE     *regexp.Regexp
	prevBackupName string
	prevBackupType string
	nextFull       time.Time
	nextIncrement  time.Time
}

var watchScheduleTemplatePrepareRE = regexp.MustCompile(`{type}|{time:([^}]+)}`)

// avgCronPeriod - average duration between `samples` successive firings after `from`,
// exact "fires more often" comparison of two arbitrary cron expressions is not computable, sampling is a heuristic for warnings only
func avgCronPeriod(sched cron.Schedule, from time.Time, samples int) time.Duration {
	first := sched.Next(from)
	last := first
	for range samples {
		last = sched.Next(last)
	}
	return last.Sub(first) / time.Duration(samples)
}

func (b *Backuper) watchScheduleTemplateRE(ctx context.Context, template string) (*regexp.Regexp, error) {
	backupTemplateName, err := b.ch.ApplyMacros(ctx, template)
	if err != nil {
		return nil, errors.Wrap(err, "watchScheduleTemplateRE ApplyMacros")
	}
	templateRE, err := regexp.Compile("^" + watchScheduleTemplatePrepareRE.ReplaceAllString(backupTemplateName, `\S+`) + "$")
	if err != nil {
		return nil, errors.Wrapf(err, "can't compile regexp for watch template `%s`", template)
	}
	return templateRE, nil
}

func findPreviousWatchBackup(remoteBackups []storage.Backup, templateRE *regexp.Regexp) (string, string) {
	prevBackupName, prevBackupType := "", ""
	for _, remoteBackup := range remoteBackups {
		if remoteBackup.Broken == "" && templateRE.MatchString(remoteBackup.BackupName) {
			prevBackupName = remoteBackup.BackupName
			if strings.Contains(remoteBackup.BackupName, "increment") {
				prevBackupType = "increment"
			} else {
				prevBackupType = "full"
			}
		}
	}
	return prevBackupName, prevBackupType
}

func (b *Backuper) newWatchScheduleStates(ctx context.Context) ([]*watchScheduleState, error) {
	remoteBackups, err := b.GetRemoteBackups(ctx, true)
	if err != nil {
		return nil, errors.Wrap(err, "newWatchScheduleStates GetRemoteBackups")
	}
	states := make([]*watchScheduleState, 0, len(b.cfg.General.WatchSchedules))
	for _, schedule := range b.cfg.General.WatchSchedules {
		st := &watchScheduleState{schedule: schedule}
		if st.fullCron, err = config.WatchCronParser.Parse(schedule.Full); err != nil {
			return nil, errors.Wrapf(err, "invalid `full` cron expression for schedule `%s`", schedule.Name)
		}
		if schedule.Increment != "" {
			if st.incrementCron, err = config.WatchCronParser.Parse(schedule.Increment); err != nil {
				return nil, errors.Wrapf(err, "invalid `increment` cron expression for schedule `%s`", schedule.Name)
			}
			// full wins over increment on the same tick, so a too frequent `full` degenerates the chain to mostly full backups
			now := time.Now()
			if fullPeriod, incrementPeriod := avgCronPeriod(st.fullCron, now, 4), avgCronPeriod(st.incrementCron, now, 4); fullPeriod <= incrementPeriod {
				log.Warn().Str("schedule", schedule.Name).Msgf("`full` cron `%s` fires as often or more often than `increment` cron `%s`, increment backups will rarely or never run", schedule.Full, schedule.Increment)
			}
		}
		st.template = schedule.Name + "-" + b.cfg.General.WatchBackupNameTemplate
		if st.templateRE, err = b.watchScheduleTemplateRE(ctx, st.template); err != nil {
			return nil, err
		}
		st.prevBackupName, st.prevBackupType = findPreviousWatchBackup(remoteBackups, st.templateRE)
		if st.prevBackupName != "" {
			log.Info().Str("schedule", schedule.Name).Msgf("continue backup chain from previous %s backup `%s`", st.prevBackupType, st.prevBackupName)
		}
		states = append(states, st)
	}
	return states, nil
}

// computeNextWatchScheduleEvents - refresh nextFull/nextIncrement for each schedule and return the earliest fire time
func computeNextWatchScheduleEvents(states []*watchScheduleState, now time.Time) time.Time {
	earliest := time.Time{}
	for _, st := range states {
		st.nextFull = st.fullCron.Next(now)
		st.nextIncrement = time.Time{}
		if st.incrementCron != nil {
			st.nextIncrement = st.incrementCron.Next(now)
		}
		for _, t := range []time.Time{st.nextFull, st.nextIncrement} {
			if !t.IsZero() && (earliest.IsZero() || t.Before(earliest)) {
				earliest = t
			}
		}
	}
	return earliest
}

// dueBackupType - which backup type shall run now, full wins over increment on the same tick,
// increment without an existing chain promotes to full
func (st *watchScheduleState) dueBackupType(now time.Time) string {
	if !st.nextFull.IsZero() && !st.nextFull.After(now) {
		return "full"
	}
	if !st.nextIncrement.IsZero() && !st.nextIncrement.After(now) {
		if st.prevBackupName == "" {
			return "full"
		}
		return "increment"
	}
	return ""
}

// watchWithSchedules - cron driven watch mode, each schedule keeps its own full+increment chain with
// schedule name as backup name prefix; failed backups are retried on the next cron tick instead of aborting,
// see https://github.com/Altinity/clickhouse-backup/issues/1354
func (b *Backuper) watchWithSchedules(ctx context.Context, watchInterval, fullInterval, watchBackupNameTemplate string, schedules []string, tablePattern string, partitions, skipProjections []string, schemaOnly, backupRBAC, backupConfigs, backupNamedCollections, skipCheckPartsColumns, deleteSource bool, version string, commandId int, metrics *metrics.APIMetrics, cliCtx *cli.Context) error {
	states, err := b.newWatchScheduleStates(ctx)
	if err != nil {
		return errors.Wrap(err, "watchWithSchedules newWatchScheduleStates")
	}
	if b.ch.IsOpen {
		b.ch.Close()
	}
	createRemoteErrCount, deleteLocalErrCount := 0, 0
	for {
		now := time.Now()
		nextAt := computeNextWatchScheduleEvents(states, now)
		if nextAt.IsZero() {
			return errors.New("watchWithSchedules can't calculate next scheduled event")
		}
		for _, st := range states {
			log.Info().Str("schedule", st.schedule.Name).Msgf("next full backup at %s, next increment backup at %s", st.nextFull.Format(time.RFC3339), st.nextIncrement.Format(time.RFC3339))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextAt.Sub(now)):
		}
		if cliCtx != nil {
			prevSchedules := b.cfg.General.WatchSchedules
			if cfg, loadErr := config.LoadConfig(config.GetConfigPath(cliCtx)); loadErr == nil {
				b.cfg = cfg
			} else {
				log.Warn().Msgf("watch config.LoadConfig error: %v", loadErr)
			}
			if err = b.ValidateWatchParams(watchInterval, fullInterval, watchBackupNameTemplate, schedules); err != nil {
				return errors.Wrap(err, "watchWithSchedules ValidateWatchParams in loop")
			}
			if len(b.cfg.General.WatchSchedules) == 0 {
				log.Warn().Msg("general->watch_schedules removed from config, keep previous schedules, restart watch to apply")
				b.cfg.General.WatchSchedules = prevSchedules
			} else if !reflect.DeepEqual(prevSchedules, b.cfg.General.WatchSchedules) {
				log.Info().Msg("watch schedules changed, rebuild schedule states")
				// keep connection open for the whole rebuild, newWatchScheduleStates needs ApplyMacros after GetRemoteBackups
				if !b.ch.IsOpen {
					if err = b.ch.Connect(); err != nil {
						return errors.Wrap(err, "watchWithSchedules rebuild ch.Connect")
					}
				}
				if states, err = b.newWatchScheduleStates(ctx); err != nil {
					return errors.Wrap(err, "watchWithSchedules rebuild newWatchScheduleStates")
				}
				if b.ch.IsOpen {
					b.ch.Close()
				}
				continue
			}
		}
		now = time.Now()
		for _, st := range states {
			backupType := st.dueBackupType(now)
			if backupType == "" {
				continue
			}
			if !b.ch.IsOpen {
				if err = b.ch.Connect(); err != nil {
					return errors.Wrap(err, "watchWithSchedules ch.Connect")
				}
			}
			b.executeScheduledBackup(ctx, st, backupType, tablePattern, partitions, skipProjections, schemaOnly, backupRBAC, backupConfigs, backupNamedCollections, skipCheckPartsColumns, deleteSource, version, commandId, metrics, &createRemoteErrCount, &deleteLocalErrCount)
		}
		// https://github.com/Altinity/clickhouse-backup/issues/1152
		// https://github.com/Altinity/clickhouse-backup/issues/1166
		// https://github.com/Altinity/clickhouse-backup/issues/1177
		if metrics != nil {
			remoteBackups, listRemoteErr := b.GetRemoteBackups(ctx, false)
			if listRemoteErr == nil && len(remoteBackups) > 0 {
				numberBackupsRemote := len(remoteBackups)
				lastBackupInstance := remoteBackups[numberBackupsRemote-1]
				metrics.LastBackupSizeRemote.Set(float64(lastBackupInstance.GetFullSize()))
				metrics.NumberBackupsRemote.Set(float64(numberBackupsRemote))
			} else {
				metrics.LastBackupSizeRemote.Set(0)
				metrics.NumberBackupsRemote.Set(0)
			}
		}
		if b.ch.IsOpen {
			b.ch.Close()
		}
	}
}

// executeScheduledBackup - create_remote (+ optional rebase for full_type=rebase) + delete local + optional delete previous cycle
func (b *Backuper) executeScheduledBackup(ctx context.Context, st *watchScheduleState, backupType, tablePattern string, partitions, skipProjections []string, schemaOnly, backupRBAC, backupConfigs, backupNamedCollections, skipCheckPartsColumns, deleteSource bool, version string, commandId int, metrics *metrics.APIMetrics, createRemoteErrCount, deleteLocalErrCount *int) {
	backupName, err := b.newBackupWatchNameFromTemplate(ctx, st.template, backupType)
	if err != nil {
		log.Error().Str("schedule", st.schedule.Name).Msgf("newBackupWatchNameFromTemplate return error: %v", err)
		return
	}
	diffFromRemote := ""
	if backupType == "increment" {
		diffFromRemote = st.prevBackupName
	}
	// full_type=rebase creates the scheduled full backup as increment + `rebase`,
	// server-side copy of the previous chain instead of full re-upload
	rebaseRequired := backupType == "full" && st.schedule.FullType == "rebase" && st.prevBackupName != ""
	if rebaseRequired {
		diffFromRemote = st.prevBackupName
	}
	createRemote := func() error {
		return b.CreateToRemote(backupName, deleteSource, "", diffFromRemote, tablePattern, partitions, skipProjections, schemaOnly, backupRBAC, false, backupConfigs, false, backupNamedCollections, false, skipCheckPartsColumns, false, version, commandId)
	}
	var createRemoteErr error
	if metrics != nil {
		createRemoteErr, *createRemoteErrCount = metrics.ExecuteWithMetrics("create_remote", *createRemoteErrCount, createRemote)
		if createRemoteErr == nil && rebaseRequired {
			createRemoteErr, _ = metrics.ExecuteWithMetrics("rebase", 0, func() error {
				return b.Rebase(backupName, commandId)
			})
		}
	} else {
		createRemoteErr = createRemote()
		if createRemoteErr == nil && rebaseRequired {
			createRemoteErr = b.Rebase(backupName, commandId)
		}
	}
	if createRemoteErr != nil {
		log.Error().Str("schedule", st.schedule.Name).Msgf("scheduled %s backup `%s` return error: %v, will retry on next cron tick", backupType, backupName, createRemoteErr)
	}
	// If backups_to_keep_local=-1 then the local backup is deleted in the upload step when RemoveOldBackupsLocal is called
	if !deleteSource && b.cfg.General.BackupsToKeepLocal >= 0 {
		removeLocal := func() error {
			return b.RemoveBackupLocal(ctx, backupName, nil)
		}
		var deleteLocalErr error
		if metrics != nil {
			deleteLocalErr, *deleteLocalErrCount = metrics.ExecuteWithMetrics("delete", *deleteLocalErrCount, removeLocal)
		} else {
			deleteLocalErr = removeLocal()
		}
		if deleteLocalErr != nil {
			log.Error().Str("schedule", st.schedule.Name).Msgf("delete local `%s` return error: %v", backupName, deleteLocalErr)
		}
	}
	if createRemoteErr != nil {
		return
	}
	st.prevBackupName = backupName
	st.prevBackupType = backupType
	if backupType == "full" && st.schedule.DeletePreviousCycle {
		if deleteErr := b.deletePreviousWatchCycle(ctx, st, backupName); deleteErr != nil {
			log.Error().Str("schedule", st.schedule.Name).Msgf("delete previous cycle after `%s` return error: %v", backupName, deleteErr)
		}
	}
}

// deletePreviousWatchCycle - delete all remote backups of the schedule chain older than newFullBackup,
// newest first, so at no moment a remaining backup references a deleted one
func (b *Backuper) deletePreviousWatchCycle(ctx context.Context, st *watchScheduleState, newFullBackup string) error {
	remoteBackups, err := b.GetRemoteBackups(ctx, true)
	if err != nil {
		return errors.Wrap(err, "deletePreviousWatchCycle GetRemoteBackups")
	}
	newFullIdx := -1
	for i, remoteBackup := range remoteBackups {
		if remoteBackup.BackupName == newFullBackup {
			newFullIdx = i
			break
		}
	}
	if newFullIdx < 0 {
		return errors.Errorf("can't find new full backup `%s` on remote storage", newFullBackup)
	}
	for i := newFullIdx - 1; i >= 0; i-- {
		remoteBackup := remoteBackups[i]
		if !st.templateRE.MatchString(remoteBackup.BackupName) {
			continue
		}
		log.Info().Str("schedule", st.schedule.Name).Msgf("delete previous cycle backup `%s`", remoteBackup.BackupName)
		if err = b.RemoveBackupRemote(ctx, remoteBackup.BackupName); err != nil {
			return errors.Wrapf(err, "can't delete `%s`", remoteBackup.BackupName)
		}
	}
	return nil
}
