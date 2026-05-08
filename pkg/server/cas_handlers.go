package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"

	"github.com/Altinity/clickhouse-backup/v2/pkg/backup"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
)

// asyncAck is the standard 200-acknowledged JSON body returned by async CAS handlers.
type asyncAck struct {
	Status      string `json:"status"`
	Operation   string `json:"operation"`
	BackupName  string `json:"backup_name,omitempty"`
	OperationId string `json:"operation_id"`
}

func newAsyncAck(op, name, opID string) asyncAck {
	return asyncAck{Status: "acknowledged", Operation: op, BackupName: name, OperationId: opID}
}

// httpCASUploadHandler handles POST /backup/cas-upload/{name}
func (api *APIServer) httpCASUploadHandler(w http.ResponseWriter, r *http.Request) {
	if !api.GetConfig().API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "cas-upload", ErrAPILocked)
		return
	}
	cfg, err := api.ReloadConfig(w, "cas-upload")
	if err != nil {
		return
	}

	name := utils.CleanBackupNameRE.ReplaceAllString(mux.Vars(r)["name"], "")
	if name == "" {
		api.writeError(w, http.StatusBadRequest, "cas-upload", fmt.Errorf("name required"))
		return
	}
	query := r.URL.Query()
	_, skipObjectDisks := api.getQueryParameter(query, "skip-object-disks")
	_, dryRun := api.getQueryParameter(query, "dry-run")
	waitForPruneStr := query.Get("wait-for-prune")

	var waitForPrune time.Duration
	if waitForPruneStr != "" {
		waitForPrune, err = time.ParseDuration(waitForPruneStr)
		if err != nil {
			api.writeError(w, http.StatusBadRequest, "cas-upload",
				fmt.Errorf("wait-for-prune: %w", err))
			return
		}
	} else {
		waitForPrune = cfg.CAS.WaitForPruneDuration()
	}

	fullCommand := fmt.Sprintf("cas-upload %s", name)
	if skipObjectDisks {
		fullCommand += " --skip-object-disks"
	}
	if dryRun {
		fullCommand += " --dry-run"
	}
	if waitForPruneStr != "" {
		fullCommand += " --wait-for-prune=" + waitForPruneStr
	}

	operationId, _ := uuid.NewUUID()
	callback, err := parseCallback(query)
	if err != nil {
		log.Error().Err(err).Send()
		api.writeError(w, http.StatusBadRequest, "cas-upload", err)
		return
	}

	commandId, _ := status.Current.StartWithOperationId(fullCommand, operationId.String())
	go func() {
		err, _ := api.metrics.ExecuteWithMetrics("cas-upload", 0, func() error {
			b := backup.NewBackuper(cfg)
			return b.CASUpload(name, skipObjectDisks, dryRun, api.clickhouseBackupVersion, commandId, waitForPrune)
		})
		if err != nil {
			log.Error().Msgf("cas-upload error: %v", err)
			status.Current.Stop(commandId, err)
			api.errorCallback(context.Background(), err, operationId.String(), callback)
			return
		}
		status.Current.Stop(commandId, nil)
		api.successCallback(context.Background(), operationId.String(), callback)
	}()

	api.sendJSONEachRow(w, http.StatusOK, newAsyncAck("cas-upload", name, operationId.String()))
}
