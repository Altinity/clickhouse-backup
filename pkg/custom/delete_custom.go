package custom

import (
	"context"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/utils"
	"github.com/apex/log"
	"time"
)

func DeleteRemote(ctx context.Context, cfg *config.Config, backupName string) error {
	if cfg.Custom.DeleteCommand == "" {
		return fmt.Errorf("CUSTOM_DELETE_COMMAND is not defined")
	}
	startCustomDelete := time.Now()
	templateData := map[string]interface{}{
		"BACKUP_NAME": backupName,
		"backup_name": backupName,
		"name":        backupName,
		"backupName":  backupName,
		"backup":      backupName,
		"cfg":         cfg,
	}
	args := ApplyCommandTemplate(cfg.Custom.DeleteCommand, templateData)
	err := utils.ExecCmd(ctx, cfg.Custom.CommandTimeoutDuration, args[0], args[1:]...)
	if err == nil {
		log.WithFields(log.Fields{
			"backup":    backupName,
			"operation": "delete_custom",
			"duration":  utils.HumanizeDuration(time.Since(startCustomDelete)),
		}).Info("done")
		return nil
	} else {
		log.WithFields(log.Fields{
			"backup":    backupName,
			"operation": "delete_custom",
		}).Error(err.Error())
		return err
	}

}
