package custom

import (
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/utils"
	"github.com/apex/log"
	"github.com/eapache/go-resiliency/retrier"
	"time"
)

func Download(cfg *config.Config, backupName string, tablePattern string, partitions []string, schemaOnly bool) error {
	startCustomDownload := time.Now()
	if cfg.Custom.DownloadCommand == "" {
		return fmt.Errorf("CUSTOM_DOWNLOAD_COMMAND is not defined")
	}
	templateData := map[string]interface{}{
		"BACKUP_NAME":   backupName,
		"backup_name":   backupName,
		"name":          backupName,
		"backupName":    backupName,
		"backup":        backupName,
		"cfg":           cfg,
		"TABLE_PATTERN": tablePattern,
		"t":             tablePattern,
		"tables":        tablePattern,
		"table":         tablePattern,
		"PARTITIONS":    partitions,
		"partitions":    partitions,
		"partition":     partitions,
		"SCHEMA_ONLY":   schemaOnly,
		"s":             schemaOnly,
		"schema":        schemaOnly,
	}
	args := ApplyCommandTemplate(cfg.Custom.DownloadCommand, templateData)
	retry := retrier.New(retrier.ConstantBackoff(cfg.General.RetriesOnFailure, cfg.General.RetriesDuration), nil)
	err := retry.Run(func() error {
		return utils.ExecCmd(cfg.Custom.CommandTimeoutDuration, args[0], args[1:]...)
	})
	if err == nil {
		log.
			WithField("operation", "download_custom").
			WithField("duration", utils.HumanizeDuration(time.Since(startCustomDownload))).
			Info("done")
		return nil
	} else {
		log.
			WithField("operation", "download_custom").
			Error(err.Error())
		return err
	}
}
