package custom

import (
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/utils"
	"github.com/apex/log"
	"time"
)

func Upload(cfg *config.Config, backupName, diffFrom, diffFromRemote, tablePattern string, partitions []string, schemaOnly bool) error {
	startCustomUpload := time.Now()
	if cfg.Custom.UploadCommand == "" {
		return fmt.Errorf("CUSTOM_UPLOAD_COMMAND is not defined")
	}
	templateData := map[string]interface{}{
		"BACKUP_NAME":      backupName,
		"backup_name":      backupName,
		"name":             backupName,
		"backupName":       backupName,
		"backup":           backupName,
		"cfg":              cfg,
		"DIFF_FROM":        diffFrom,
		"diff_from":        diffFrom,
		"diffFrom":         diffFrom,
		"DIFF_FROM_REMOTE": diffFromRemote,
		"diff_from_remote": diffFromRemote,
		"diffFromRemote":   diffFromRemote,
		"TABLE_PATTERN":    tablePattern,
		"t":                tablePattern,
		"tables":           tablePattern,
		"table":            tablePattern,
		"PARTITIONS":       partitions,
		"partitions":       partitions,
		"partition":        partitions,
		"SCHEMA_ONLY":      schemaOnly,
		"s":                schemaOnly,
		"schema":           schemaOnly,
	}
	args := ApplyCommandTemplate(cfg.Custom.UploadCommand, templateData)
	err := utils.ExecCmd(cfg.Custom.CommandTimeoutDuration, args[0], args[1:]...)
	if err == nil {
		log.
			WithField("operation", "upload_custom").
			WithField("duration", utils.HumanizeDuration(time.Since(startCustomUpload))).
			Info("done")
		return nil
	} else {
		log.
			WithField("operation", "upload_custom").
			Error(err.Error())
		return err
	}
}
