package custom

import (
	"context"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

func DeleteRemote(ctx context.Context, cfg *config.Config, backupName string) error {
	if cfg.Custom.DeleteCommand == "" {
		return errors.New("CUSTOM_DELETE_COMMAND is not defined")
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
		log.Info().Fields(map[string]interface{}{
			"backup":    backupName,
			"operation": "delete_custom",
			"duration":  utils.HumanizeDuration(time.Since(startCustomDelete)),
		}).Msg("done")
		return nil
	} else {
		log.Error().Fields(map[string]interface{}{
			"backup":    backupName,
			"operation": "delete_custom",
		}).Msg(err.Error())
		return errors.WithMessage(err, "DeleteRemote custom")
	}

}
