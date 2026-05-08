package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
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

// httpCASDownloadHandler handles POST /backup/cas-download/{name}
func (api *APIServer) httpCASDownloadHandler(w http.ResponseWriter, r *http.Request) {
	if !api.GetConfig().API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "cas-download", ErrAPILocked)
		return
	}
	cfg, err := api.ReloadConfig(w, "cas-download")
	if err != nil {
		return
	}

	name := utils.CleanBackupNameRE.ReplaceAllString(mux.Vars(r)["name"], "")
	if name == "" {
		api.writeError(w, http.StatusBadRequest, "cas-download", fmt.Errorf("name required"))
		return
	}

	query := r.URL.Query()
	tablePattern := ""
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
	}
	partitions := query["partitions"]
	_, schemaOnly := api.getQueryParameter(query, "schema")
	_, dataOnly := api.getQueryParameter(query, "data")

	if dataOnly {
		api.writeError(w, http.StatusNotImplemented, "cas-download",
			fmt.Errorf("cas-download: data-only restore is not yet implemented"))
		return
	}

	fullCommand := fmt.Sprintf("cas-download %s", name)
	if tablePattern != "" {
		fullCommand += fmt.Sprintf(" --table=%q", tablePattern)
	}
	for _, p := range partitions {
		fullCommand += " --partitions=" + p
	}
	if schemaOnly {
		fullCommand += " --schema"
	}
	if dataOnly {
		fullCommand += " --data"
	}

	operationId, _ := uuid.NewUUID()
	callback, err := parseCallback(query)
	if err != nil {
		log.Error().Err(err).Send()
		api.writeError(w, http.StatusBadRequest, "cas-download", err)
		return
	}

	commandId, _ := status.Current.StartWithOperationId(fullCommand, operationId.String())
	go func() {
		err, _ := api.metrics.ExecuteWithMetrics("cas-download", 0, func() error {
			b := backup.NewBackuper(cfg)
			return b.CASDownload(name, tablePattern, partitions, schemaOnly, dataOnly, api.clickhouseBackupVersion, commandId)
		})
		if err != nil {
			log.Error().Msgf("cas-download error: %v", err)
			status.Current.Stop(commandId, err)
			api.errorCallback(context.Background(), err, operationId.String(), callback)
			return
		}
		status.Current.Stop(commandId, nil)
		api.successCallback(context.Background(), operationId.String(), callback)
	}()

	api.sendJSONEachRow(w, http.StatusOK, newAsyncAck("cas-download", name, operationId.String()))
}

// httpCASRestoreHandler handles POST /backup/cas-restore/{name}
func (api *APIServer) httpCASRestoreHandler(w http.ResponseWriter, r *http.Request) {
	if !api.GetConfig().API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "cas-restore", ErrAPILocked)
		return
	}
	cfg, err := api.ReloadConfig(w, "cas-restore")
	if err != nil {
		return
	}

	name := utils.CleanBackupNameRE.ReplaceAllString(mux.Vars(r)["name"], "")
	if name == "" {
		api.writeError(w, http.StatusBadRequest, "cas-restore", fmt.Errorf("name required"))
		return
	}

	query := r.URL.Query()
	tablePattern := ""
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
	}
	partitions := query["partitions"]
	_, schemaOnly := api.getQueryParameter(query, "schema")
	_, dataOnly := api.getQueryParameter(query, "data")

	if dataOnly {
		api.writeError(w, http.StatusNotImplemented, "cas-restore",
			fmt.Errorf("cas-restore: data-only restore is not yet implemented"))
		return
	}

	// Reject ignore-dependencies at the boundary — CASRestore passes false internally.
	if _, exists := api.getQueryParameter(query, "ignore-dependencies"); exists {
		api.writeError(w, http.StatusBadRequest, "cas-restore",
			fmt.Errorf("cas-restore: ignore-dependencies is not supported; CAS restore always respects table dependencies"))
		return
	}

	// Parse database mapping (same as v1 httpRestoreHandler).
	dbMapping := make([]string, 0)
	for _, qpName := range []string{"restore-database-mapping", "restore_database_mapping"} {
		if vals, exist := query[qpName]; exist {
			for _, v := range vals {
				for _, m := range strings.Split(v, ",") {
					m = strings.TrimSpace(m)
					if m != "" {
						dbMapping = append(dbMapping, m)
					}
				}
			}
		}
	}

	// Parse table mapping.
	tableMapping := make([]string, 0)
	for _, qpName := range []string{"restore-table-mapping", "restore_table_mapping"} {
		if vals, exist := query[qpName]; exist {
			for _, v := range vals {
				for _, m := range strings.Split(v, ",") {
					m = strings.TrimSpace(m)
					if m != "" {
						tableMapping = append(tableMapping, m)
					}
				}
			}
		}
	}

	// Parse skip-projections.
	skipProjections := make([]string, 0)
	if sp, exist := api.getQueryParameter(query, "skip-projections"); exist {
		skipProjections = append(skipProjections, sp)
	}

	dropExists := false
	if _, exist := query["drop"]; exist {
		dropExists = true
	}
	if _, exist := query["rm"]; exist {
		dropExists = true
	}

	_, restoreSchemaAsAttach := api.getQueryParameter(query, "restore-schema-as-attach")
	if !restoreSchemaAsAttach {
		_, restoreSchemaAsAttach = api.getQueryParameter(query, "restore_schema_as_attach")
	}

	_, replicatedCopyToDetached := api.getQueryParameter(query, "replicated-copy-to-detached")
	if !replicatedCopyToDetached {
		_, replicatedCopyToDetached = api.getQueryParameter(query, "replicated_copy_to_detached")
	}

	_, skipEmptyTables := api.getQueryParameter(query, "skip-empty-tables")
	if !skipEmptyTables {
		_, skipEmptyTables = api.getQueryParameter(query, "skip_empty_tables")
	}

	_, resume := api.getQueryParameter(query, "resume")
	if !resume {
		_, resume = query["resumable"]
	}

	fullCommand := fmt.Sprintf("cas-restore %s", name)
	if tablePattern != "" {
		fullCommand += fmt.Sprintf(" --table=%q", tablePattern)
	}
	for _, p := range partitions {
		fullCommand += " --partitions=" + p
	}
	if schemaOnly {
		fullCommand += " --schema"
	}
	if len(dbMapping) > 0 {
		fullCommand += fmt.Sprintf(" --restore-database-mapping=%q", strings.Join(dbMapping, ","))
	}
	if len(tableMapping) > 0 {
		fullCommand += fmt.Sprintf(" --restore-table-mapping=%q", strings.Join(tableMapping, ","))
	}
	if len(skipProjections) > 0 {
		fullCommand += " --skip-projections=" + strings.Join(skipProjections, ",")
	}
	if dropExists {
		fullCommand += " --drop"
	}
	if restoreSchemaAsAttach {
		fullCommand += " --restore-schema-as-attach"
	}
	if replicatedCopyToDetached {
		fullCommand += " --replicated-copy-to-detached"
	}
	if skipEmptyTables {
		fullCommand += " --skip-empty-tables"
	}
	if resume {
		fullCommand += " --resume"
	}

	operationId, _ := uuid.NewUUID()
	callback, err := parseCallback(query)
	if err != nil {
		log.Error().Err(err).Send()
		api.writeError(w, http.StatusBadRequest, "cas-restore", err)
		return
	}

	commandId, _ := status.Current.StartWithOperationId(fullCommand, operationId.String())
	go func() {
		err, _ := api.metrics.ExecuteWithMetrics("cas-restore", 0, func() error {
			b := backup.NewBackuper(cfg)
			return b.CASRestore(
				name, tablePattern,
				dbMapping, tableMapping, partitions, skipProjections,
				schemaOnly, dataOnly,
				dropExists, false, // ignoreDependencies always false for CAS
				restoreSchemaAsAttach, replicatedCopyToDetached,
				skipEmptyTables, resume,
				api.clickhouseBackupVersion, commandId,
			)
		})
		if err != nil {
			log.Error().Msgf("cas-restore error: %v", err)
			status.Current.Stop(commandId, err)
			api.errorCallback(context.Background(), err, operationId.String(), callback)
			return
		}
		status.Current.Stop(commandId, nil)
		api.successCallback(context.Background(), operationId.String(), callback)
	}()

	api.sendJSONEachRow(w, http.StatusOK, newAsyncAck("cas-restore", name, operationId.String()))
}
