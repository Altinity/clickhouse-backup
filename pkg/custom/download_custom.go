package custom

import (
	"context"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/utils"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/rs/zerolog/log"
	"time"
)

func Download(ctx context.Context, cfg *config.Config, backupName string, tablePattern string, partitions []string, schemaOnly bool) error {
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
	err := retry.RunCtx(ctx, func(ctx context.Context) error {
		return utils.ExecCmd(ctx, cfg.Custom.CommandTimeoutDuration, args[0], args[1:]...)
	})
	if err == nil {
		log.Info().
			Str("operation", "download_custom").
			Str("duration", utils.HumanizeDuration(time.Since(startCustomDownload))).
			Msg("done")
		return nil
	} else {
		log.Error().
			Str("operation", "download_custom").
			Err(err).Send()
		return err
	}
}
