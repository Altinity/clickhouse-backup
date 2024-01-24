package custom

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/apex/log"
	"strings"
	"time"
)

func List(ctx context.Context, cfg *config.Config) ([]storage.Backup, error) {
	if cfg.Custom.ListCommand == "" {
		return nil, fmt.Errorf("CUSTOM_LIST_COMMAND is not defined")
	}
	startCustomList := time.Now()
	templateData := map[string]interface{}{
		"cfg": cfg,
	}
	args := ApplyCommandTemplate(cfg.Custom.ListCommand, templateData)
	out, err := utils.ExecCmdOut(ctx, cfg.Custom.CommandTimeoutDuration, args[0], args[1:]...)
	if err == nil {
		outLines := strings.Split(strings.TrimRight(out, "\n"), "\n")
		backupList := make([]storage.Backup, len(outLines))
		for i, line := range outLines {
			if len(line) > 0 {
				if err = json.Unmarshal([]byte(line), &backupList[i]); err != nil {
					return nil, fmt.Errorf("JSON parsing '%s' error: %v ", line, err)
				}
			}
		}
		log.
			WithField("operation", "list_custom").
			WithField("duration", utils.HumanizeDuration(time.Since(startCustomList))).
			Info("done")
		return backupList, nil
	} else {
		log.
			WithField("operation", "list_custom").
			Error(err.Error())
		return nil, err
	}
}
