package backup

import (
	"context"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/server/metrics"
	"github.com/Altinity/clickhouse-backup/pkg/status"
	apexLog "github.com/apex/log"
	"github.com/urfave/cli"
	"regexp"
	"strings"
	"time"
)

var watchBackupTemplateTimeRE = regexp.MustCompile(`{time:([^}]+)}`)

func (b *Backuper) NewBackupWatchName(ctx context.Context, backupType string) (string, error) {
	backupName, err := b.ch.ApplyMacros(ctx, b.cfg.General.WatchBackupNameTemplate)
	if err != nil {
		return "", err
	}
	backupName = strings.Replace(backupName, "{type}", backupType, -1)
	if watchBackupTemplateTimeRE.MatchString(backupName) {
		for _, group := range watchBackupTemplateTimeRE.FindAllStringSubmatch(backupName, -1) {
			templateItem := group[0]
			layout := group[1]
			backupName = strings.ReplaceAll(backupName, templateItem, time.Now().UTC().Format(layout))
		}
	} else {
		return "", fmt.Errorf("watch_backup_name_template doesn't contain {time:layout}, backup name will non unique")
	}
	return backupName, nil
}

func (b *Backuper) ValidateWatchParams(watchInterval, fullInterval, watchBackupNameTemplate string) error {
	var err error
	if watchInterval != "" {
		b.cfg.General.WatchInterval = watchInterval
		if b.cfg.General.WatchDuration, err = time.ParseDuration(watchInterval); err != nil {
			return fmt.Errorf("watchInterval `%s` parsing error: %v", watchInterval, err)
		}
	}
	if fullInterval != "" {
		b.cfg.General.FullInterval = fullInterval
		if b.cfg.General.FullDuration, err = time.ParseDuration(fullInterval); err != nil {
			return fmt.Errorf("fullInterval `%s` parsing error: %v", fullInterval, err)
		}
	}
	if b.cfg.General.FullDuration <= b.cfg.General.WatchDuration {
		return fmt.Errorf("fullInterval `%s` should be more than watchInterval `%s`", b.cfg.General.FullInterval, b.cfg.General.WatchInterval)
	}
	if watchBackupNameTemplate != "" {
		b.cfg.General.WatchBackupNameTemplate = watchBackupNameTemplate
	}
	if b.cfg.General.BackupsToKeepRemote > 0 && b.cfg.General.WatchDuration.Seconds()*float64(b.cfg.General.BackupsToKeepRemote) < b.cfg.General.FullDuration.Seconds() {
		return fmt.Errorf("fullInterval `%s` is too long to keep %d remote backups with watchInterval `%s`", b.cfg.General.FullInterval, b.cfg.General.BackupsToKeepRemote, b.cfg.General.WatchInterval)
	}
	return nil
}

// Watch
// - run create_remote full + delete local full, even when upload failed
//   - if success save backup type full, next will increment, until reach full interval
//   - if fail save previous backup type empty, next try will also full
//
// - each watch-interval, run create_remote increment --diff-from=prev-name + delete local increment, even when upload failed
//   - save previous backup type incremental, next try will also incremental, until reach full interval
func (b *Backuper) Watch(watchInterval, fullInterval, watchBackupNameTemplate, tablePattern string, partitions []string, schemaOnly, backupRBAC, backupConfigs, skipCheckPartsColumns bool, version string, commandId int, metrics metrics.APIMetricsInterface, cliCtx *cli.Context) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	if err := b.ValidateWatchParams(watchInterval, fullInterval, watchBackupNameTemplate); err != nil {
		return err
	}
	backupType := "full"
	prevBackupName := ""
	prevBackupType := ""
	lastBackup := time.Now()
	lastFullBackup := time.Now()

	remoteBackups, err := b.GetRemoteBackups(ctx, true)
	if err != nil {
		return err
	}
	backupTemplateName, err := b.ch.ApplyMacros(ctx, b.cfg.General.WatchBackupNameTemplate)
	if err != nil {
		return err
	}
	backupTemplateNamePrepR := regexp.MustCompile(`{type}|{time:([^}]+)}`)
	backupTemplateNameR := regexp.MustCompile(backupTemplateNamePrepR.ReplaceAllString(backupTemplateName, `\S+`))

	for _, remoteBackup := range remoteBackups {
		if remoteBackup.Broken == "" && backupTemplateNameR.MatchString(remoteBackup.BackupName) {
			prevBackupName = remoteBackup.BackupName
			if strings.Contains(remoteBackup.BackupName, "increment") {
				prevBackupType = "increment"
				lastBackup = remoteBackup.CreationDate
			} else {
				prevBackupType = "full"
				lastBackup = remoteBackup.CreationDate
				lastFullBackup = remoteBackup.CreationDate
			}
		}
	}
	if prevBackupName != "" {
		now := time.Now()
		timeBeforeDoBackup := int(b.cfg.General.WatchDuration.Seconds() - now.Sub(lastBackup).Seconds())
		timeBeforeDoFullBackup := int(b.cfg.General.FullDuration.Seconds() - now.Sub(lastFullBackup).Seconds())
		b.log.Infof("Time before do backup %v", timeBeforeDoBackup)
		b.log.Infof("Time before do full backup %v", timeBeforeDoFullBackup)
		if timeBeforeDoBackup > 0 && timeBeforeDoFullBackup > 0 {
			b.log.Infof("Waiting %d seconds until continue doing backups due watch interval", timeBeforeDoBackup)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(b.cfg.General.WatchDuration - now.Sub(lastBackup)):
			}
		}
		now = time.Now()
		lastBackup = now
		if b.cfg.General.FullDuration.Seconds()-time.Now().Sub(lastFullBackup).Seconds() <= 0 {
			backupType = "full"
			lastFullBackup = now
		} else {
			backupType = "increment"
		}
	}

	createRemoteErrCount := 0
	deleteLocalErrCount := 0
	var createRemoteErr error
	var deleteLocalErr error
	for {
		if !b.ch.IsOpen {
			if err = b.ch.Connect(); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if cliCtx != nil {
				if cfg, err := config.LoadConfig(config.GetConfigPath(cliCtx)); err == nil {
					b.cfg = cfg
				} else {
					b.log.Warnf("watch config.LoadConfig error: %v", err)
				}
				if err := b.ValidateWatchParams(watchInterval, fullInterval, watchBackupNameTemplate); err != nil {
					return err
				}
			}
			backupName, err := b.NewBackupWatchName(ctx, backupType)
			log := b.log.WithFields(apexLog.Fields{
				"backup":    backupName,
				"operation": "watch",
			})
			if err != nil {
				return err
			}
			diffFromRemote := ""
			if backupType == "increment" {
				diffFromRemote = prevBackupName
			}
			if metrics != nil {
				createRemoteErr, createRemoteErrCount = metrics.ExecuteWithMetrics("create_remote", createRemoteErrCount, func() error {
					return b.CreateToRemote(backupName, "", diffFromRemote, tablePattern, partitions, schemaOnly, backupRBAC, false, backupConfigs, false, skipCheckPartsColumns, false, version, commandId)
				})
				deleteLocalErr, deleteLocalErrCount = metrics.ExecuteWithMetrics("delete", deleteLocalErrCount, func() error {
					return b.RemoveBackupLocal(ctx, backupName, nil)
				})

			} else {
				createRemoteErr = b.CreateToRemote(backupName, "", diffFromRemote, tablePattern, partitions, schemaOnly, backupRBAC, false, backupConfigs, false, skipCheckPartsColumns, false, version, commandId)
				if createRemoteErr != nil {
					log.Errorf("create_remote %s return error: %v", backupName, createRemoteErr)
					createRemoteErrCount += 1
				} else {
					createRemoteErrCount = 0
				}
				deleteLocalErr = b.RemoveBackupLocal(ctx, backupName, nil)
				if deleteLocalErr != nil {
					log.Errorf("delete local %s return error: %v", backupName, deleteLocalErr)
					deleteLocalErrCount += 1
				} else {
					deleteLocalErrCount = 0
				}

			}

			if createRemoteErrCount > b.cfg.General.BackupsToKeepRemote || deleteLocalErrCount > b.cfg.General.BackupsToKeepLocal {
				return fmt.Errorf("too many errors create_remote: %d, delete local: %d, during watch full_interval: %s, abort watching", createRemoteErrCount, deleteLocalErrCount, b.cfg.General.FullInterval)
			}
			if (createRemoteErr != nil || deleteLocalErr != nil) && time.Now().Sub(lastFullBackup) > b.cfg.General.FullDuration {
				return fmt.Errorf("too many errors during watch full_interval: %s, abort watching", b.cfg.General.FullInterval)
			}
			if createRemoteErr == nil {
				prevBackupName = backupName
				prevBackupType = backupType
				if prevBackupType == "full" {
					backupType = "increment"
				}
				now := time.Now()
				if b.cfg.General.WatchDuration.Seconds()-now.Sub(lastBackup).Seconds() > 0 {
					select {
					case <-ctx.Done(): //context cancelled
						return ctx.Err()
					case <-time.After(b.cfg.General.WatchDuration - now.Sub(lastBackup)): //timeout
					}
				}
				now = time.Now()
				lastBackup = now
				if b.cfg.General.FullDuration.Seconds()-now.Sub(lastFullBackup).Seconds() <= 0 {
					backupType = "full"
					lastFullBackup = now
				}
			}
		}
		if b.ch.IsOpen {
			b.ch.Close()
		}
	}
}
